"""SQLite-backed hypothesis tracker — prevents re-running identical hypotheses."""

from __future__ import annotations

import json
import logging
import sqlite3
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
    completed_at    TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_hypothesis_dedup
    ON hypotheses(strategy, series, db_path, start_date, end_date, fee_pct);
"""


class Tracker:
    def __init__(self, db_path: str | Path = "data/research.db") -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init()

    def _connect(self, timeout: float = 30.0) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=timeout)
        conn.row_factory = sqlite3.Row
        return conn

    def _init(self) -> None:
        conn = self._connect()
        try:
            conn.executescript(_SCHEMA)
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
        try:
            conn.execute(
                """INSERT OR IGNORE INTO hypotheses
                   (id, strategy, series, db_path, start_date, end_date, fee_pct,
                    parent_id, tags, notes, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    h.id, h.strategy, h.series, h.db_path,
                    h.start_date, h.end_date, h.fee_pct,
                    h.parent_id,
                    json.dumps(h.tags),
                    h.notes,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def save_result(self, result: HypothesisResult) -> None:
        """Upsert hypothesis + result records."""
        conn = self._connect()
        try:
            h = result.hypothesis
            conn.execute(
                """INSERT OR IGNORE INTO hypotheses
                   (id, strategy, series, db_path, start_date, end_date, fee_pct,
                    parent_id, tags, notes, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    h.id, h.strategy, h.series, h.db_path,
                    h.start_date, h.end_date, h.fee_pct,
                    h.parent_id,
                    json.dumps(h.tags),
                    h.notes,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            conn.execute(
                """INSERT OR REPLACE INTO results
                   (hypothesis_id, status, total_trades, wins, losses, win_rate,
                    net_pnl_cents, sharpe, max_drawdown_pct, fees_paid_cents,
                    avg_win_cents, avg_loss_cents, per_strategy,
                    verdict, verdict_reason, raw_json, completed_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    h.id,
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
                          r.verdict, r.verdict_reason, r.completed_at
                   FROM hypotheses h
                   JOIN results r ON h.id = r.hypothesis_id
                   WHERE h.strategy = ?
                   ORDER BY r.completed_at DESC""",
                (strategy,),
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def list_results(self) -> list[dict]:
        """Return all results as plain dicts, joined with hypothesis metadata."""
        conn = self._connect()
        try:
            rows = conn.execute(
                """SELECT h.id, h.strategy, h.series, h.db_path,
                          h.start_date, h.end_date, h.fee_pct, h.tags, h.created_at,
                          r.status, r.total_trades, r.wins, r.losses, r.win_rate,
                          r.net_pnl_cents, r.sharpe, r.max_drawdown_pct, r.fees_paid_cents,
                          r.verdict, r.verdict_reason, r.completed_at
                   FROM hypotheses h
                   JOIN results r ON h.id = r.hypothesis_id
                   ORDER BY r.completed_at DESC"""
            ).fetchall()
            return [dict(r) for r in rows]
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
            total = conn.execute("SELECT COUNT(*) FROM results").fetchone()[0]
            by_verdict = {
                row["verdict"]: row["cnt"]
                for row in conn.execute(
                    "SELECT verdict, COUNT(*) AS cnt FROM results GROUP BY verdict"
                ).fetchall()
            }
            return {"total": total, "by_verdict": by_verdict}
        finally:
            conn.close()
