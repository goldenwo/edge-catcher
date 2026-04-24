"""SQLite-backed hypothesis tracker — prevents re-running identical hypotheses."""

from __future__ import annotations

import json
import logging
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .hypothesis import Hypothesis, HypothesisResult

logger = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS hypotheses (
    id          TEXT PRIMARY KEY,
    strategy    TEXT NOT NULL,
    series      TEXT NOT NULL,
    db_path     TEXT NOT NULL,
    start_date  TEXT NOT NULL,
    end_date    TEXT NOT NULL,
    fee_pct     REAL NOT NULL,
    parent_id   TEXT,
    tags        TEXT,       -- JSON array
    notes       TEXT,
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS results (
    hypothesis_id   TEXT PRIMARY KEY REFERENCES hypotheses(id),
    status          TEXT NOT NULL,
    total_trades    INTEGER NOT NULL,
    wins            INTEGER NOT NULL,
    losses          INTEGER NOT NULL,
    win_rate        REAL NOT NULL,
    net_pnl_cents   REAL NOT NULL,
    sharpe          REAL NOT NULL,
    max_drawdown_pct REAL NOT NULL,
    fees_paid_cents REAL NOT NULL,
    avg_win_cents   REAL NOT NULL,
    avg_loss_cents  REAL NOT NULL,
    per_strategy    TEXT,   -- JSON object
    verdict         TEXT NOT NULL,
    verdict_reason  TEXT NOT NULL,
    raw_json        TEXT,   -- full backtester output
    validation_details TEXT,  -- JSON array of gate results
    completed_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS kill_registry (
    strategy TEXT PRIMARY KEY,
    kill_count INTEGER NOT NULL,
    series_tested INTEGER NOT NULL,
    kill_rate REAL NOT NULL,
    reason_summary TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    permanent INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS strategy_fingerprints (
    fingerprint TEXT PRIMARY KEY,
    strategy_name TEXT NOT NULL,
    code_hash TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_hypothesis_dedup
    ON hypotheses(strategy, series, db_path, start_date, end_date, fee_pct);

CREATE TABLE IF NOT EXISTS hypothesis_results (
    id TEXT PRIMARY KEY,
    test_type TEXT NOT NULL,
    series TEXT NOT NULL,
    db TEXT NOT NULL,
    params TEXT NOT NULL,
    thresholds TEXT NOT NULL,
    verdict TEXT NOT NULL,
    z_stat REAL NOT NULL,
    fee_adjusted_edge REAL NOT NULL,
    detail TEXT NOT NULL,
    rationale TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hypothesis_kills (
    pattern_key TEXT PRIMARY KEY,
    kill_count INTEGER NOT NULL DEFAULT 0,
    last_params TEXT NOT NULL DEFAULT '{}',
    last_z_stat REAL NOT NULL DEFAULT 0.0,
    permanent INTEGER NOT NULL DEFAULT 0,
    reason_summary TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS ui_backtests (
    task_id         TEXT PRIMARY KEY,
    series          TEXT NOT NULL,
    strategies      TEXT NOT NULL,
    db_path         TEXT NOT NULL DEFAULT '',
    start_date      TEXT,
    end_date        TEXT,
    run_timestamp   TEXT NOT NULL,
    total_trades    INTEGER,
    wins            INTEGER,
    losses          INTEGER,
    net_pnl_cents   INTEGER,
    sharpe          REAL,
    max_drawdown_pct REAL,
    win_rate        REAL,
    result_path     TEXT,
    hypothesis_id   TEXT
);
"""


class Tracker:
    def __init__(self, db_path: str | Path = "data/research.db") -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init()

    def _connect(self, timeout: float = 30.0) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=timeout)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.row_factory = sqlite3.Row
        return conn

    def _init(self) -> None:
        conn = self._connect()
        try:
            conn.executescript(_SCHEMA)
            # Migrate existing databases: add columns that may not exist
            cursor = conn.execute("PRAGMA table_info(results)")
            columns = {row[1] for row in cursor.fetchall()}
            if "validation_details" not in columns:
                conn.execute("ALTER TABLE results ADD COLUMN validation_details TEXT")
            conn.commit()
        finally:
            conn.close()

    def is_tested(self, h: Hypothesis) -> Optional[str]:
        """Return existing hypothesis_id if this combo was already tested, else None."""
        conn = self._connect()
        try:
            row = conn.execute(
                """SELECT id FROM hypotheses
                   WHERE strategy=? AND series=? AND db_path=?
                     AND start_date=? AND end_date=? AND fee_pct=?""",
                (h.strategy, h.series, h.db_path, h.start_date, h.end_date, h.fee_pct),
            ).fetchone()
            return row["id"] if row else None
        finally:
            conn.close()

    def save_hypothesis(self, h: Hypothesis) -> None:
        """Insert hypothesis record (ignore if duplicate)."""
        conn = self._connect()
        # Normalize db_path to forward slashes for cross-platform consistency
        db_path = h.db_path.replace("\\", "/") if h.db_path else h.db_path
        start = h.start_date or ""
        end = h.end_date or ""
        try:
            conn.execute(
                """INSERT OR IGNORE INTO hypotheses
                   (id, strategy, series, db_path, start_date, end_date, fee_pct,
                    parent_id, tags, notes, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    h.id, h.strategy, h.series, db_path,
                    start, end, h.fee_pct,
                    h.parent_id,
                    json.dumps(h.tags),
                    h.notes,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def save_result(self, result: HypothesisResult, validation_details: list[dict] | None = None) -> None:
        """Upsert hypothesis + result records."""
        conn = self._connect()
        try:
            h = result.hypothesis
            # Normalize db_path to forward slashes for cross-platform consistency
            db_path = h.db_path.replace("\\", "/") if h.db_path else h.db_path
            start = h.start_date or ""
            end = h.end_date or ""
            conn.execute(
                """INSERT OR IGNORE INTO hypotheses
                   (id, strategy, series, db_path, start_date, end_date, fee_pct,
                    parent_id, tags, notes, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    h.id, h.strategy, h.series, db_path,
                    start, end, h.fee_pct,
                    h.parent_id,
                    json.dumps(h.tags),
                    h.notes,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            # Resolve the actual hypothesis ID — if the dedup index caused
            # INSERT OR IGNORE to skip, look up the existing row.
            # Normalize db_path separators to forward slashes for cross-platform matching.
            normalized_db = h.db_path.replace("\\", "/") if h.db_path else h.db_path
            row = conn.execute(
                """SELECT id FROM hypotheses
                   WHERE strategy=? AND series=? AND REPLACE(db_path, '\\', '/')=?
                     AND start_date=? AND end_date=? AND fee_pct=?""",
                (h.strategy, h.series, normalized_db, start, end, h.fee_pct),
            ).fetchone()
            if row:
                actual_id = row["id"]
            else:
                # Dedup lookup failed — force insert with INSERT OR REPLACE
                # to prevent orphaned results.
                conn.execute(
                    """INSERT OR REPLACE INTO hypotheses
                       (id, strategy, series, db_path, start_date, end_date, fee_pct,
                        parent_id, tags, notes, created_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        h.id, h.strategy, h.series, db_path,
                        start, end, h.fee_pct,
                        h.parent_id,
                        json.dumps(h.tags),
                        h.notes,
                        datetime.now(timezone.utc).isoformat(),
                    ),
                )
                actual_id = h.id
            conn.execute(
                """INSERT OR REPLACE INTO results
                   (hypothesis_id, status, total_trades, wins, losses, win_rate,
                    net_pnl_cents, sharpe, max_drawdown_pct, fees_paid_cents,
                    avg_win_cents, avg_loss_cents, per_strategy,
                    verdict, verdict_reason, raw_json, validation_details, completed_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    actual_id,
                    result.status,
                    result.total_trades,
                    result.wins,
                    result.losses,
                    result.win_rate,
                    result.net_pnl_cents,
                    result.sharpe,
                    result.max_drawdown_pct,
                    result.fees_paid_cents,
                    result.avg_win_cents,
                    result.avg_loss_cents,
                    json.dumps(result.per_strategy),
                    result.verdict,
                    result.verdict_reason,
                    json.dumps(result.raw_json),
                    json.dumps(validation_details) if validation_details is not None else None,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def list_results_for_strategy(self, strategy: str) -> list[dict]:
        """Return all results for a given strategy name."""
        conn = self._connect()
        try:
            rows = conn.execute(
                """SELECT h.id, h.strategy, h.series, h.db_path,
                          h.start_date, h.end_date, h.fee_pct, h.tags,
                          r.status, r.total_trades, r.wins, r.losses, r.win_rate,
                          r.net_pnl_cents, r.sharpe, r.max_drawdown_pct,
                          r.fees_paid_cents, r.avg_win_cents, r.avg_loss_cents,
                          r.verdict, r.verdict_reason, r.validation_details,
                          r.completed_at
                   FROM hypotheses h
                   JOIN results r ON h.id = r.hypothesis_id
                   WHERE h.strategy = ?
                   ORDER BY r.completed_at DESC""",
                (strategy,),
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def list_results(
        self,
        limit: int | None = None,
        offset: int | None = None,
        sort: str = "completed_at",
        verdict: str | None = None,
    ) -> list[dict]:
        """Return all results as plain dicts, joined with hypothesis metadata."""
        conn = self._connect()
        try:
            allowed_sorts = {"completed_at", "sharpe", "win_rate", "net_pnl_cents", "total_trades"}
            if sort not in allowed_sorts:
                sort = "completed_at"
            query = """
                SELECT h.id, h.strategy, h.series, h.db_path,
                          h.start_date, h.end_date, h.fee_pct, h.tags, h.created_at,
                          r.status, r.total_trades, r.wins, r.losses, r.win_rate,
                          r.net_pnl_cents, r.sharpe, r.max_drawdown_pct, r.fees_paid_cents,
                          r.avg_win_cents, r.avg_loss_cents,
                          r.verdict, r.verdict_reason, r.validation_details, r.completed_at
               FROM hypotheses h
               JOIN results r ON h.id = r.hypothesis_id
        """
            params: list = []
            if verdict is not None:
                query += " WHERE r.verdict = ?"
                params.append(verdict)
            query += f" ORDER BY r.{sort} DESC"
            if limit is not None:
                query += f" LIMIT {int(limit)}"
            if offset is not None:
                if limit is None:
                    query += " LIMIT -1"
                query += f" OFFSET {int(offset)}"
            rows = conn.execute(query, params).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def count_by_verdict(self) -> dict[str, int]:
        conn = self._connect()
        try:
            rows = conn.execute(
                """SELECT r.verdict, COUNT(*) as cnt
                   FROM hypotheses h
                   JOIN results r ON h.id = r.hypothesis_id
                   GROUP BY r.verdict"""
            ).fetchall()
            return {r["verdict"]: r["cnt"] for r in rows}
        finally:
            conn.close()

    def update_verdict(self, hypothesis_id: str, verdict: str) -> None:
        conn = self._connect()
        try:
            conn.execute(
                "UPDATE results SET verdict = ? WHERE hypothesis_id = ?",
                (verdict, hypothesis_id),
            )
            conn.commit()
        finally:
            conn.close()

    def get_result_by_id(self, hypothesis_id: str) -> Optional[dict]:
        """Return a single result dict or None."""
        conn = self._connect()
        try:
            row = conn.execute(
                """SELECT h.*, r.*
                   FROM hypotheses h
                   JOIN results r ON h.id = r.hypothesis_id
                   WHERE h.id = ?""",
                (hypothesis_id,),
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def delete_orphaned_results(self) -> int:
        """Delete result rows whose hypothesis_id has no matching hypothesis."""
        conn = self._connect()
        try:
            cursor = conn.execute(
                """DELETE FROM results
                   WHERE hypothesis_id NOT IN (SELECT id FROM hypotheses)"""
            )
            conn.commit()
            return cursor.rowcount
        finally:
            conn.close()

    def clear_unvalidated_promotes(self) -> int:
        """Delete result rows for promotes that lack validation_details.

        These are stale results from before the validation pipeline was added.
        Removing the result row (but keeping the hypothesis) causes the next
        sweep to re-run and re-evaluate them with validation gates.
        """
        conn = self._connect()
        try:
            cursor = conn.execute(
                """DELETE FROM results
                   WHERE verdict = 'promote'
                     AND (validation_details IS NULL OR validation_details = '')
                     AND hypothesis_id IN (SELECT id FROM hypotheses)"""
            )
            conn.commit()
            return cursor.rowcount
        finally:
            conn.close()

    def cleanup(self) -> dict[str, int]:
        """Run all cleanup operations. Returns counts of affected rows."""
        orphans = self.delete_orphaned_results()
        stale_promotes = self.clear_unvalidated_promotes()
        return {"orphaned_results_deleted": orphans, "unvalidated_promotes_cleared": stale_promotes}

    def list_pending(self) -> list[dict]:
        """Return hypotheses that have no result row (saved but not yet executed)."""
        conn = self._connect()
        try:
            rows = conn.execute(
                """SELECT h.*
                   FROM hypotheses h
                   LEFT JOIN results r ON h.id = r.hypothesis_id
                   WHERE r.hypothesis_id IS NULL
                   ORDER BY h.created_at ASC"""
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def stats(self) -> dict:
        """Return summary counts."""
        conn = self._connect()
        try:
            total = conn.execute(
                """SELECT COUNT(*) FROM hypotheses h
                   JOIN results r ON h.id = r.hypothesis_id"""
            ).fetchone()[0]
            by_verdict = {
                row["verdict"]: row["cnt"]
                for row in conn.execute(
                    """SELECT r.verdict, COUNT(*) AS cnt
                       FROM hypotheses h
                       JOIN results r ON h.id = r.hypothesis_id
                       GROUP BY r.verdict"""
                ).fetchall()
            }
            return {"total": total, "by_verdict": by_verdict}
        finally:
            conn.close()

    def upsert_kill_registry(
        self,
        strategy: str,
        kill_count: int,
        series_tested: int,
        kill_rate: float,
        reason_summary: str,
    ) -> None:
        """Upsert a strategy into the kill registry. Always sets permanent=TRUE."""
        conn = self._connect()
        try:
            conn.execute(
                """INSERT INTO kill_registry (strategy, kill_count, series_tested, kill_rate, reason_summary)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(strategy) DO UPDATE SET
                     kill_count=excluded.kill_count,
                     series_tested=excluded.series_tested,
                     kill_rate=excluded.kill_rate,
                     reason_summary=excluded.reason_summary,
                     permanent=1""",
                (strategy, kill_count, series_tested, kill_rate, reason_summary),
            )
            conn.commit()
        finally:
            conn.close()

    def reject_and_update_kill_registry(self, hypothesis_id: str, reason: str) -> None:
        """Mark hypothesis as killed and update the kill registry with recalculated stats."""
        result = self.get_result_by_id(hypothesis_id)
        if not result:
            raise ValueError(f"Hypothesis {hypothesis_id!r} not found")
        strategy = result["strategy"]
        all_results = self.list_results_for_strategy(strategy)
        kill_count = sum(1 for r in all_results if r.get("verdict") == "kill") + 1
        series_tested = len(set(r["series"] for r in all_results))
        kill_rate = kill_count / len(all_results) if all_results else 1.0
        self.update_verdict(hypothesis_id, "kill")
        self.upsert_kill_registry(
            strategy=strategy,
            kill_count=kill_count,
            series_tested=series_tested,
            kill_rate=kill_rate,
            reason_summary=reason,
        )

    def reset_kill_registry(self, strategy: str) -> None:
        """Mark a strategy as non-permanent, allowing re-proposal."""
        conn = self._connect()
        try:
            conn.execute(
                "UPDATE kill_registry SET permanent=0 WHERE strategy=?",
                (strategy,),
            )
            conn.commit()
        finally:
            conn.close()

    def list_kill_registry(self, permanent_only: bool = False) -> list[dict]:
        """Return all kill registry entries, optionally filtered to permanent only."""
        conn = self._connect()
        try:
            query = "SELECT * FROM kill_registry"
            if permanent_only:
                query += " WHERE permanent=1"
            query += " ORDER BY kill_rate DESC, kill_count DESC"
            rows = conn.execute(query).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def save_fingerprint(self, fingerprint: str, strategy_name: str, code_hash: str) -> None:
        """Record a strategy's AST fingerprint."""
        conn = self._connect()
        try:
            conn.execute(
                "INSERT OR IGNORE INTO strategy_fingerprints (fingerprint, strategy_name, code_hash) VALUES (?, ?, ?)",
                (fingerprint, strategy_name, code_hash),
            )
            conn.commit()
        finally:
            conn.close()

    def check_fingerprint(self, fingerprint: str) -> Optional[str]:
        """Return strategy name if fingerprint exists, else None."""
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT strategy_name FROM strategy_fingerprints WHERE fingerprint=?",
                (fingerprint,),
            ).fetchone()
            return row["strategy_name"] if row else None
        finally:
            conn.close()

    def check_code_hash(self, code_hash: str) -> Optional[str]:
        """Return strategy name if code hash exists, else None."""
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT strategy_name FROM strategy_fingerprints WHERE code_hash=?",
                (code_hash,),
            ).fetchone()
            return row["strategy_name"] if row else None
        finally:
            conn.close()

    # ── hypothesis_results & hypothesis_kills ────────────────────────

    def save_hypothesis_result(
        self,
        test_type: str,
        series: str,
        db: str,
        params: dict,
        thresholds: dict,
        verdict: str,
        z_stat: float,
        fee_adjusted_edge: float,
        detail: dict,
        rationale: str = "",
    ) -> str:
        """Save a statistical hypothesis test result. Returns the generated id."""
        row_id = str(uuid.uuid4())
        conn = self._connect()
        try:
            conn.execute(
                """INSERT INTO hypothesis_results
                   (id, test_type, series, db, params, thresholds, verdict,
                    z_stat, fee_adjusted_edge, detail, rationale, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    row_id,
                    test_type,
                    series,
                    db,
                    json.dumps(params),
                    json.dumps(thresholds),
                    verdict,
                    z_stat,
                    fee_adjusted_edge,
                    json.dumps(detail),
                    rationale,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            conn.commit()
        finally:
            conn.close()
        return row_id

    def count_hypothesis_results(
        self,
        test_type: str | None = None,
        series: str | None = None,
        verdict: str | None = None,
    ) -> int:
        """Count hypothesis results with optional filters."""
        conn = self._connect()
        try:
            query = "SELECT COUNT(*) FROM hypothesis_results"
            clauses: list[str] = []
            params: list = []
            if test_type is not None:
                clauses.append("test_type = ?")
                params.append(test_type)
            if series is not None:
                clauses.append("series = ?")
                params.append(series)
            if verdict is not None:
                clauses.append("verdict = ?")
                params.append(verdict)
            if clauses:
                query += " WHERE " + " AND ".join(clauses)
            return conn.execute(query, params).fetchone()[0]
        finally:
            conn.close()

    def list_hypothesis_results(
        self,
        test_type: str | None = None,
        series: str | None = None,
        verdict: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[dict]:
        """List hypothesis results with optional filters."""
        conn = self._connect()
        try:
            query = "SELECT * FROM hypothesis_results"
            clauses: list[str] = []
            params: list = []
            if test_type is not None:
                clauses.append("test_type = ?")
                params.append(test_type)
            if series is not None:
                clauses.append("series = ?")
                params.append(series)
            if verdict is not None:
                clauses.append("verdict = ?")
                params.append(verdict)
            if clauses:
                query += " WHERE " + " AND ".join(clauses)
            query += " ORDER BY created_at DESC"
            if limit is not None:
                query += f" LIMIT {int(limit)}"
            if offset is not None:
                if limit is None:
                    query += " LIMIT -1"
                query += f" OFFSET {int(offset)}"
            rows = conn.execute(query, params).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def record_hypothesis_kill(
        self,
        test_type: str,
        series: str,
        db: str,
        verdict: str,
        params: dict,
        z_stat: float,
    ) -> None:
        """Record a hypothesis test failure toward the kill counter.

        Kill logic:
        - INSUFFICIENT_DATA -> skip (don't count)
        - NO_EDGE -> increment kill_count; permanent at 3
        - EDGE_NOT_TRADEABLE -> increment kill_count; permanent at 5
        """
        if verdict == "INSUFFICIENT_DATA":
            return

        pattern_key = f"{test_type}:{series}:{db}"
        threshold = 3 if verdict == "NO_EDGE" else 5

        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT kill_count FROM hypothesis_kills WHERE pattern_key = ?",
                (pattern_key,),
            ).fetchone()

            if row is None:
                new_count = 1
                conn.execute(
                    """INSERT INTO hypothesis_kills
                       (pattern_key, kill_count, last_params, last_z_stat,
                        permanent, reason_summary)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (
                        pattern_key,
                        new_count,
                        json.dumps(params),
                        z_stat,
                        1 if new_count >= threshold else 0,
                        verdict,
                    ),
                )
            else:
                new_count = row["kill_count"] + 1
                conn.execute(
                    """UPDATE hypothesis_kills
                       SET kill_count = ?, last_params = ?, last_z_stat = ?,
                           permanent = ?, reason_summary = ?
                       WHERE pattern_key = ?""",
                    (
                        new_count,
                        json.dumps(params),
                        z_stat,
                        1 if new_count >= threshold else 0,
                        verdict,
                        pattern_key,
                    ),
                )
            conn.commit()
        finally:
            conn.close()

    def list_hypothesis_kills(self, permanent_only: bool = False) -> list[dict]:
        """List hypothesis kill registry entries."""
        conn = self._connect()
        try:
            query = "SELECT * FROM hypothesis_kills"
            if permanent_only:
                query += " WHERE permanent = 1"
            query += " ORDER BY kill_count DESC"
            rows = conn.execute(query).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def is_hypothesis_killed(self, test_type: str, series: str, db: str) -> bool:
        """Check if a hypothesis pattern is permanently killed."""
        pattern_key = f"{test_type}:{series}:{db}"
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT permanent FROM hypothesis_kills WHERE pattern_key = ?",
                (pattern_key,),
            ).fetchone()
            return bool(row and row["permanent"])
        finally:
            conn.close()

    # ── ui_backtests ────────────────────────────────────────────────────

    def save_ui_backtest(
        self,
        task_id: str,
        series: str,
        strategies: str,
        db_path: str = "",
        start_date: str | None = None,
        end_date: str | None = None,
        total_trades: int = 0,
        wins: int = 0,
        losses: int = 0,
        net_pnl_cents: int = 0,
        sharpe: float = 0.0,
        max_drawdown_pct: float = 0.0,
        win_rate: float = 0.0,
        result_path: str | None = None,
        hypothesis_id: str | None = None,
    ) -> None:
        """Save a UI-triggered backtest result."""
        conn = self._connect()
        try:
            conn.execute(
                """INSERT OR REPLACE INTO ui_backtests
                   (task_id, series, strategies, db_path, start_date, end_date,
                    run_timestamp, total_trades, wins, losses, net_pnl_cents,
                    sharpe, max_drawdown_pct, win_rate, result_path, hypothesis_id)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    task_id, series, strategies, db_path, start_date, end_date,
                    datetime.now(timezone.utc).isoformat(),
                    total_trades, wins, losses, net_pnl_cents,
                    sharpe, max_drawdown_pct, win_rate, result_path, hypothesis_id,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def list_ui_backtests(self, limit: int = 25, offset: int = 0) -> tuple[list[dict], int]:
        """Return (rows, total_count) for UI backtest history."""
        conn = self._connect()
        try:
            total = conn.execute("SELECT COUNT(*) FROM ui_backtests").fetchone()[0]
            rows = conn.execute(
                "SELECT * FROM ui_backtests ORDER BY run_timestamp DESC LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
            return [dict(r) for r in rows], total
        finally:
            conn.close()

    def delete_ui_backtest(self, task_id: str) -> bool:
        """Delete a UI backtest by task_id. Returns True if deleted."""
        conn = self._connect()
        try:
            cur = conn.execute("DELETE FROM ui_backtests WHERE task_id = ?", (task_id,))
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()

    def count_ui_backtests(self) -> int:
        """Return total number of UI backtests."""
        conn = self._connect()
        try:
            return conn.execute("SELECT COUNT(*) FROM ui_backtests").fetchone()[0]
        finally:
            conn.close()

    def latest_ui_backtest(self) -> dict | None:
        """Return the most recent UI backtest, or None."""
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM ui_backtests ORDER BY run_timestamp DESC LIMIT 1"
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def delete_result(self, result_id: str) -> bool:
        """Delete a result by ID. Checks both results and hypothesis_results tables. Returns True if deleted."""
        conn = self._connect()
        try:
            cur = conn.execute("DELETE FROM results WHERE hypothesis_id = ?", (result_id,))
            if cur.rowcount == 0:
                cur = conn.execute("DELETE FROM hypothesis_results WHERE id = ?", (result_id,))
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()

    def get_hypothesis_result_by_id(self, result_id: str) -> dict | None:
        """Return a single hypothesis_results row by id, or None."""
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM hypothesis_results WHERE id = ?",
                (result_id,),
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()
