"""ResearchAgent: orchestrates hypothesis testing via subprocess backtester calls."""

from __future__ import annotations

import json
import logging
import subprocess
import sys
from pathlib import Path

from .evaluator import Evaluator, Thresholds
from .hypothesis import Hypothesis, HypothesisResult
from .reporter import Reporter
from .tracker import Tracker

logger = logging.getLogger(__name__)

# Related strategy families for adjacent hypothesis generation.
# Each strategy maps to its "cousins" (filter variants of the same core idea).
_STRATEGY_FAMILY: dict[str, list[str]] = {
    "A":      ["Avol", "Amom"],
    "Avol":   ["A", "Amom"],
    "Amom":   ["A", "Avol"],
    "B":      [],
    "C":      ["Cvol", "Cmom", "Cstack"],
    "Cvol":   ["C", "Cmom", "Cstack"],
    "Cmom":   ["C", "Cvol", "Cstack"],
    "Cstack": ["C", "Cvol", "Cmom"],
    "D":      ["Dvol"],
    "Dvol":   ["D"],
    "TP":     [],
    "Fflow":  ["Ffvol"],
    "Ffvol":  ["Fflow"],
}


class ResearchAgent:
    def __init__(
        self,
        tracker: Tracker | None = None,
        evaluator: Evaluator | None = None,
        reporter: Reporter | None = None,
        thresholds: Thresholds | None = None,
        research_db: str = "data/research.db",
    ) -> None:
        self.tracker = tracker or Tracker(research_db)
        self.evaluator = evaluator or Evaluator()
        self.reporter = reporter or Reporter()
        self.thresholds = thresholds or Thresholds()

    # ------------------------------------------------------------------
    # Core: run a single hypothesis
    # ------------------------------------------------------------------

    def run_hypothesis(self, h: Hypothesis) -> HypothesisResult:
        """Run a single hypothesis via CLI subprocess, evaluate, track, return result."""
        # Dedup check: skip if already tested with identical parameters
        existing_id = self.tracker.is_tested(h)
        if existing_id:
            logger.info(
                "Skipping %s/%s (already tested, id=%s)", h.strategy, h.series, existing_id
            )
            # Return a synthetic result pointing at the existing record
            existing = self.tracker.get_result_by_id(existing_id)
            if existing:
                return self._row_to_result(existing, h)
            # Fallback: re-run (edge case if result row is missing)
            logger.warning("Existing record %s has no result row — re-running", existing_id)

        logger.info("Running hypothesis: strategy=%s series=%s db=%s [%s → %s] fee=%.2f",
                    h.strategy, h.series, h.db_path, h.start_date, h.end_date, h.fee_pct)

        cmd = [
            sys.executable, "-m", "edge_catcher", "backtest",
            "--series", h.series,
            "--strategy", h.strategy,
            "--db-path", h.db_path,
            "--fee-pct", str(h.fee_pct),
            "--json",
        ]
        if h.start_date:
            cmd += ["--start", h.start_date]
        if h.end_date:
            cmd += ["--end", h.end_date]

        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300,  # 5-minute timeout per hypothesis
            )
        except subprocess.TimeoutExpired:
            result = HypothesisResult.error(h, "subprocess timed out after 300s")
            self.tracker.save_result(result)
            return result
        except Exception as exc:
            result = HypothesisResult.error(h, f"subprocess failed: {exc}")
            self.tracker.save_result(result)
            return result

        # Log stderr for debugging (progress messages from backtester)
        if proc.stderr:
            for line in proc.stderr.strip().splitlines():
                logger.debug("[backtest stderr] %s", line)

        # Parse JSON from stdout
        try:
            data = json.loads(proc.stdout.strip())
        except json.JSONDecodeError as exc:
            msg = f"JSON parse error: {exc} | stdout={proc.stdout[:200]!r}"
            result = HypothesisResult.error(h, msg)
            self.tracker.save_result(result)
            return result

        if data.get("status") == "error":
            result = HypothesisResult.error(h, data.get("message", "unknown error"))
            self.tracker.save_result(result)
            return result

        # Build HypothesisResult from backtester JSON
        verdict, verdict_reason = self.evaluator.evaluate(
            HypothesisResult(
                hypothesis=h,
                status="ok",
                total_trades=data.get("total_trades", 0),
                wins=data.get("wins", 0),
                losses=data.get("losses", 0),
                win_rate=data.get("win_rate", 0.0),
                net_pnl_cents=data.get("net_pnl_cents", 0.0),
                sharpe=data.get("sharpe", 0.0),
                max_drawdown_pct=data.get("max_drawdown_pct", 0.0),
                fees_paid_cents=data.get("total_fees_paid", 0.0),
                avg_win_cents=data.get("avg_win_cents", 0.0),
                avg_loss_cents=data.get("avg_loss_cents", 0.0),
                per_strategy=data.get("per_strategy", {}),
                verdict="",          # filled below
                verdict_reason="",
                raw_json=data,
            ),
            self.thresholds,
        )

        result = HypothesisResult(
            hypothesis=h,
            status="ok",
            total_trades=data.get("total_trades", 0),
            wins=data.get("wins", 0),
            losses=data.get("losses", 0),
            win_rate=data.get("win_rate", 0.0),
            net_pnl_cents=data.get("net_pnl_cents", 0.0),
            sharpe=data.get("sharpe", 0.0),
            max_drawdown_pct=data.get("max_drawdown_pct", 0.0),
            fees_paid_cents=data.get("total_fees_paid", 0.0),
            avg_win_cents=data.get("avg_win_cents", 0.0),
            avg_loss_cents=data.get("avg_loss_cents", 0.0),
            per_strategy=data.get("per_strategy", {}),
            verdict=verdict,
            verdict_reason=verdict_reason,
            raw_json=data,
        )

        self.tracker.save_result(result)
        logger.info(
            "  → verdict=%s | trades=%d win_rate=%.1f%% sharpe=%.2f pnl=%.0f¢  [%s]",
            result.verdict, result.total_trades, result.win_rate * 100,
            result.sharpe, result.net_pnl_cents, verdict_reason,
        )
        return result

    # ------------------------------------------------------------------
    # Adjacent hypothesis generation
    # ------------------------------------------------------------------

    def generate_adjacent(self, result: HypothesisResult) -> list[Hypothesis]:
        """Given a result, generate adjacent hypotheses worth testing next.

        Promoted → try same strategy on other series/databases.
        Exploring → try related strategies (filter variants) on same series/db.
        Killed   → return empty list.
        """
        h = result.hypothesis
        adjacent: list[Hypothesis] = []

        if result.verdict == "kill":
            return adjacent

        if result.verdict == "promote":
            # Discover other (db, series) pairs and try the same winning strategy
            for db_path, series_list in self._discover_all_series().items():
                for series in series_list:
                    if series == h.series and db_path == h.db_path:
                        continue  # skip the one we just ran
                    adjacent.append(
                        Hypothesis(
                            strategy=h.strategy,
                            series=series,
                            db_path=db_path,
                            start_date=h.start_date,
                            end_date=h.end_date,
                            fee_pct=h.fee_pct,
                            parent_id=h.id,
                            tags=h.tags + ["adjacent-promoted"],
                        )
                    )

        elif result.verdict == "explore":
            # Try related strategies (same core idea, different filters) on same data
            cousins = _STRATEGY_FAMILY.get(h.strategy, [])
            for cousin in cousins:
                adjacent.append(
                    Hypothesis(
                        strategy=cousin,
                        series=h.series,
                        db_path=h.db_path,
                        start_date=h.start_date,
                        end_date=h.end_date,
                        fee_pct=h.fee_pct,
                        parent_id=h.id,
                        tags=h.tags + ["adjacent-explore"],
                    )
                )

        return adjacent

    # ------------------------------------------------------------------
    # Batch sweep
    # ------------------------------------------------------------------

    def sweep(
        self,
        hypotheses: list[Hypothesis],
        max_runs: int = 50,
    ) -> list[HypothesisResult]:
        """Run a batch of hypotheses, auto-generating adjacent ones from results.

        Stops at max_runs or when queue is exhausted.
        """
        queue = list(hypotheses)
        results: list[HypothesisResult] = []
        runs = 0

        while queue and runs < max_runs:
            h = queue.pop(0)
            result = self.run_hypothesis(h)
            results.append(result)
            runs += 1

            # Enqueue adjacent hypotheses if budget allows
            adjacent = self.generate_adjacent(result)
            if adjacent:
                remaining_budget = max_runs - runs
                queue.extend(adjacent[:remaining_budget])
                logger.info(
                    "  Enqueued %d adjacent hypotheses (%d in queue, %d budget remaining)",
                    len(adjacent), len(queue), remaining_budget,
                )

        if queue:
            logger.info("Sweep stopped at max_runs=%d; %d hypotheses remain in queue", max_runs, len(queue))

        return results

    def sweep_all_series(
        self,
        strategy: str,
        fee_pct: float = 1.0,
        start: str = "2025-01-01",
        end: str = "2025-12-31",
        max_runs: int = 50,
    ) -> list[HypothesisResult]:
        """Sweep one strategy across ALL available databases and series.

        Auto-discovers databases and series via the list-dbs CLI command.
        """
        hypotheses: list[Hypothesis] = []
        for db_path, series_list in self._discover_all_series().items():
            for series in series_list:
                hypotheses.append(
                    Hypothesis(
                        strategy=strategy,
                        series=series,
                        db_path=db_path,
                        start_date=start,
                        end_date=end,
                        fee_pct=fee_pct,
                        tags=["sweep-all-series"],
                    )
                )

        logger.info(
            "sweep_all_series: strategy=%s, %d hypotheses across %d databases",
            strategy, len(hypotheses), len(set(h.db_path for h in hypotheses)),
        )
        return self.sweep(hypotheses, max_runs=max_runs)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _discover_all_series(self) -> dict[str, list[str]]:
        """Return {db_path: [series, ...]} for all data/*.db files with series data.

        Uses the list-dbs CLI command for discovery.
        """
        try:
            proc = subprocess.run(
                [sys.executable, "-m", "edge_catcher", "list-dbs"],
                capture_output=True,
                text=True,
                timeout=30,
            )
            data = json.loads(proc.stdout.strip())
        except Exception as exc:
            logger.warning("list-dbs failed, falling back to glob: %s", exc)
            return self._discover_series_via_glob()

        result: dict[str, list[str]] = {}
        for db_info in data.get("databases", []):
            series = [s for s in db_info.get("series", []) if s]
            if series:
                result[db_info["path"]] = series
        return result

    def _discover_series_via_glob(self) -> dict[str, list[str]]:
        """Fallback: scan data/*.db files directly."""
        import sqlite3

        result: dict[str, list[str]] = {}
        for db_file in sorted(Path("data").glob("*.db")):
            # Skip tracker's own database
            if db_file.name == "research.db":
                continue
            try:
                conn = sqlite3.connect(str(db_file))
                rows = conn.execute(
                    "SELECT DISTINCT series_ticker FROM markets ORDER BY series_ticker"
                ).fetchall()
                conn.close()
                series = [r[0] for r in rows if r[0]]
                if series:
                    result[str(db_file)] = series
            except Exception:
                pass
        return result

    @staticmethod
    def _row_to_result(row: dict, h: Hypothesis) -> HypothesisResult:
        """Reconstruct a HypothesisResult from a tracker row dict."""
        return HypothesisResult(
            hypothesis=h,
            status=row.get("status", "ok"),
            total_trades=row.get("total_trades", 0),
            wins=row.get("wins", 0),
            losses=row.get("losses", 0),
            win_rate=row.get("win_rate", 0.0),
            net_pnl_cents=row.get("net_pnl_cents", 0.0),
            sharpe=row.get("sharpe", 0.0),
            max_drawdown_pct=row.get("max_drawdown_pct", 0.0),
            fees_paid_cents=row.get("fees_paid_cents", 0.0),
            avg_win_cents=row.get("avg_win_cents", 0.0),
            avg_loss_cents=row.get("avg_loss_cents", 0.0),
            per_strategy={},
            verdict=row.get("verdict", "kill"),
            verdict_reason=row.get("verdict_reason", ""),
            raw_json={},
        )
