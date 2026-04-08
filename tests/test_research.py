"""Tests for edge_catcher.research module."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from edge_catcher.research.agent import ResearchAgent
from edge_catcher.research.data_source_config import make_ds
from edge_catcher.research.evaluator import Evaluator, Thresholds
from edge_catcher.research.hypothesis import Hypothesis, HypothesisResult
from edge_catcher.research.reporter import Reporter
from edge_catcher.research.tracker import Tracker


def _ds(db="kalshi.db", series="KXBTCD"):
    return make_ds(db=db, series=series)


# ---------------------------------------------------------------------------
# Hypothesis
# ---------------------------------------------------------------------------

class TestHypothesis:
    def test_auto_uuid(self):
        h1 = Hypothesis(strategy="C", data_sources=_ds(),
                        start_date="2025-01-01", end_date="2025-12-31")
        h2 = Hypothesis(strategy="C", data_sources=_ds(),
                        start_date="2025-01-01", end_date="2025-12-31")
        assert h1.id != h2.id

    def test_dedup_key_ignores_id(self):
        h1 = Hypothesis(strategy="C", data_sources=_ds(),
                        start_date="2025-01-01", end_date="2025-12-31", fee_pct=1.0)
        h2 = Hypothesis(strategy="C", data_sources=_ds(),
                        start_date="2025-01-01", end_date="2025-12-31", fee_pct=1.0)
        assert h1.dedup_key() == h2.dedup_key()

    def test_dedup_key_differs_on_fee(self):
        h1 = Hypothesis(strategy="C", data_sources=_ds(),
                        start_date="2025-01-01", end_date="2025-12-31", fee_pct=1.0)
        h2 = Hypothesis(strategy="C", data_sources=_ds(),
                        start_date="2025-01-01", end_date="2025-12-31", fee_pct=0.25)
        assert h1.dedup_key() != h2.dedup_key()

    def test_error_constructor(self):
        h = Hypothesis(strategy="C", data_sources=_ds(db="x.db"),
                       start_date="2025-01-01", end_date="2025-12-31")
        result = HypothesisResult.error(h, "db not found")
        assert result.status == "error"
        assert result.verdict == "kill"
        assert "db not found" in result.verdict_reason


# ---------------------------------------------------------------------------
# Evaluator
# ---------------------------------------------------------------------------

def _make_result(
    strategy="C", series="KXBTCD", total_trades=100, wins=90, losses=10,
    win_rate=0.90, net_pnl_cents=500.0, sharpe=2.5, max_drawdown_pct=5.0,
    fees_paid_cents=100.0, avg_win_cents=10.0, avg_loss_cents=-5.0,
    status="ok", verdict="", verdict_reason="", per_strategy=None, raw_json=None,
) -> HypothesisResult:
    h = Hypothesis(strategy=strategy, data_sources=_ds(series=series),
                   start_date="2025-01-01", end_date="2025-12-31")
    return HypothesisResult(
        hypothesis=h, status=status, total_trades=total_trades,
        wins=wins, losses=losses, win_rate=win_rate, net_pnl_cents=net_pnl_cents,
        sharpe=sharpe, max_drawdown_pct=max_drawdown_pct,
        fees_paid_cents=fees_paid_cents, avg_win_cents=avg_win_cents,
        avg_loss_cents=avg_loss_cents, per_strategy=per_strategy or {},
        verdict=verdict, verdict_reason=verdict_reason, raw_json=raw_json or {},
    )


class TestEvaluator:
    def setup_method(self):
        self.ev = Evaluator()
        self.th = Thresholds()

    def test_candidate_high_sharpe(self):
        """Sharpe >= 2.0 with >= 100 trades → candidate (needs validation)."""
        r = _make_result(sharpe=2.5, win_rate=0.90, net_pnl_cents=500.0, total_trades=100)
        verdict, reason = self.ev.evaluate(r, self.th)
        assert verdict == "candidate"

    def test_explore_insufficient_trades_for_promote(self):
        """Sharpe >= 2.0 but < 100 trades → explore (not enough for candidate)."""
        r = _make_result(sharpe=2.5, win_rate=0.90, net_pnl_cents=500.0, total_trades=75)
        verdict, reason = self.ev.evaluate(r, self.th)
        assert verdict == "explore"
        assert "trades" in reason

    def test_kill_low_sharpe(self):
        r = _make_result(sharpe=0.5, win_rate=0.90, net_pnl_cents=500.0, total_trades=100)
        verdict, reason = self.ev.evaluate(r, self.th)
        assert verdict == "kill"
        assert "Sharpe" in reason

    def test_validate_mid_sharpe(self):
        """Sharpe between kill (1.0) and promote (2.0) with enough trades → validate."""
        r = _make_result(sharpe=1.5, win_rate=0.70, net_pnl_cents=500.0, total_trades=100)
        verdict, reason = self.ev.evaluate(r, self.th)
        assert verdict == "validate"

    def test_kill_negative_pnl(self):
        r = _make_result(sharpe=1.5, win_rate=0.90, net_pnl_cents=-100.0, total_trades=100)
        verdict, reason = self.ev.evaluate(r, self.th)
        assert verdict == "kill"
        assert "PnL" in reason

    def test_explore_few_trades(self):
        r = _make_result(sharpe=3.0, win_rate=0.95, net_pnl_cents=500.0, total_trades=10)
        verdict, reason = self.ev.evaluate(r, self.th)
        assert verdict == "explore"
        assert "trades" in reason

    def test_validate_between_thresholds(self):
        # Sharpe between 1.0 and 2.0, win rate between 0.85 and 0.87
        r = _make_result(sharpe=1.5, win_rate=0.86, net_pnl_cents=200.0, total_trades=100)
        verdict, reason = self.ev.evaluate(r, self.th)
        assert verdict == "validate"

    def test_error_result_is_killed(self):
        h = Hypothesis(strategy="C", data_sources=_ds(db="x.db"),
                       start_date="2025-01-01", end_date="2025-12-31")
        error_result = HypothesisResult.error(h, "db not found")
        verdict, reason = self.ev.evaluate(error_result, self.th)
        assert verdict == "kill"

    def test_custom_thresholds(self):
        th = Thresholds(min_sharpe=0.5, promote_sharpe=1.0)
        r = _make_result(sharpe=1.2, win_rate=0.65, net_pnl_cents=100.0, total_trades=100)
        verdict, _ = self.ev.evaluate(r, th)
        assert verdict == "candidate"


# ---------------------------------------------------------------------------
# Tracker
# ---------------------------------------------------------------------------

class TestTracker:
    def test_init_creates_tables(self, tmp_path):
        tracker = Tracker(tmp_path / "research.db")
        results = tracker.list_results()
        assert results == []

    def test_save_and_retrieve(self, tmp_path):
        tracker = Tracker(tmp_path / "research.db")
        r = _make_result(status="ok", verdict="promote", verdict_reason="good")
        tracker.save_result(r)

        rows = tracker.list_results()
        assert len(rows) == 1
        assert rows[0]["strategy"] == "C"
        assert rows[0]["verdict"] == "promote"

    def test_dedup_check(self, tmp_path):
        tracker = Tracker(tmp_path / "research.db")
        r = _make_result(verdict="promote", verdict_reason="test")
        tracker.save_result(r)

        # Same parameters → already tested
        h2 = Hypothesis(strategy="C", data_sources=_ds(),
                        start_date="2025-01-01", end_date="2025-12-31", fee_pct=1.0)
        existing_id = tracker.is_tested(h2)
        assert existing_id is not None

    def test_no_dedup_different_fee(self, tmp_path):
        tracker = Tracker(tmp_path / "research.db")
        r = _make_result(verdict="promote", verdict_reason="test")
        tracker.save_result(r)

        h2 = Hypothesis(strategy="C", data_sources=_ds(),
                        start_date="2025-01-01", end_date="2025-12-31", fee_pct=0.25)
        existing_id = tracker.is_tested(h2)
        assert existing_id is None

    def test_stats(self, tmp_path):
        tracker = Tracker(tmp_path / "research.db")
        tracker.save_result(_make_result(verdict="promote", verdict_reason="p"))
        tracker.save_result(_make_result(strategy="D", verdict="kill", verdict_reason="k"))
        tracker.save_result(_make_result(strategy="A", series="KXETH", verdict="explore", verdict_reason="e"))

        stats = tracker.stats()
        assert stats["total"] == 3
        assert stats["by_verdict"]["promote"] == 1
        assert stats["by_verdict"]["kill"] == 1
        assert stats["by_verdict"]["explore"] == 1

    def test_list_pending_returns_hypotheses_without_results(self, tmp_path):
        tracker = Tracker(tmp_path / "research.db")
        # Save a hypothesis with a result
        r = _make_result(strategy="C", verdict="promote", verdict_reason="p")
        tracker.save_result(r)
        # Save a hypothesis WITHOUT a result
        h_pending = Hypothesis(
            strategy="D", data_sources=_ds(series="KXETH"),
            start_date="2025-01-01", end_date="2025-12-31",
            tags=["source:llm_ideated"],
        )
        tracker.save_hypothesis(h_pending)
        pending = tracker.list_pending()
        assert len(pending) == 1
        assert pending[0]["strategy"] == "D"
        assert pending[0]["series"] == "KXETH"

    def test_list_pending_empty_when_all_have_results(self, tmp_path):
        tracker = Tracker(tmp_path / "research.db")
        r = _make_result(strategy="C", verdict="promote", verdict_reason="p")
        tracker.save_result(r)
        pending = tracker.list_pending()
        assert pending == []

    def test_save_result_with_validation_details(self, tmp_path):
        tracker = Tracker(tmp_path / "research.db")
        r = _make_result(verdict="promote", verdict_reason="passed all gates")
        validation_details = [
            {"gate_name": "deflated_sharpe", "passed": True, "reason": "DSR 0.97 > 0.95"},
        ]
        tracker.save_result(r, validation_details=validation_details)

        rows = tracker.list_results()
        assert len(rows) == 1
        # validation_details should be queryable
        import sqlite3
        conn = sqlite3.connect(str(tmp_path / "research.db"))
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT validation_details FROM results").fetchone()
        conn.close()
        import json
        assert json.loads(row["validation_details"])[0]["gate_name"] == "deflated_sharpe"

    def test_force_rerun_updates_existing_result_no_orphan(self, tmp_path):
        """When --force creates a new hypothesis ID for the same combo,
        save_result should update the existing result, not create an orphan."""
        tracker = Tracker(tmp_path / "research.db")

        # Save initial result
        r1 = _make_result(strategy="C", series="KXBTCD", verdict="kill", verdict_reason="bad")
        tracker.save_result(r1)
        original_id = r1.hypothesis.id

        # Simulate --force: new Hypothesis with different UUID, same dedup key
        h2 = Hypothesis(strategy="C", data_sources=_ds(),
                        start_date="2025-01-01", end_date="2025-12-31")
        assert h2.id != original_id  # different UUID
        r2 = HypothesisResult(
            hypothesis=h2, status="ok", total_trades=200, wins=180, losses=20,
            win_rate=0.90, net_pnl_cents=1000.0, sharpe=5.0, max_drawdown_pct=2.0,
            fees_paid_cents=50.0, avg_win_cents=10.0, avg_loss_cents=-5.0,
            per_strategy={}, verdict="promote", verdict_reason="strong edge",
            raw_json={},
        )
        tracker.save_result(r2)

        # Should have exactly 1 result, not 2 (no orphan)
        rows = tracker.list_results()
        assert len(rows) == 1
        assert rows[0]["verdict"] == "promote"
        assert rows[0]["sharpe"] == 5.0
        assert rows[0]["strategy"] == "C"

        # No orphaned results
        import sqlite3
        conn = sqlite3.connect(str(tmp_path / "research.db"))
        orphans = conn.execute(
            "SELECT COUNT(*) FROM results r LEFT JOIN hypotheses h ON r.hypothesis_id = h.id WHERE h.id IS NULL"
        ).fetchone()[0]
        conn.close()
        assert orphans == 0

    def test_delete_orphaned_results(self, tmp_path):
        """delete_orphaned_results removes result rows with no matching hypothesis."""
        tracker = Tracker(tmp_path / "research.db")

        # Save a normal result
        r = _make_result(verdict="promote", verdict_reason="good")
        tracker.save_result(r)

        # Manually insert an orphan
        import sqlite3
        conn = sqlite3.connect(str(tmp_path / "research.db"))
        conn.execute(
            """INSERT INTO results (hypothesis_id, status, total_trades, wins, losses,
               win_rate, net_pnl_cents, sharpe, max_drawdown_pct, fees_paid_cents,
               avg_win_cents, avg_loss_cents, verdict, verdict_reason, completed_at)
               VALUES ('orphan-id', 'ok', 50, 25, 25, 0.5, 100, 1.0, 5.0, 10, 5, -5,
                       'kill', 'orphan', '2026-01-01')"""
        )
        conn.commit()
        conn.close()

        deleted = tracker.delete_orphaned_results()
        assert deleted == 1
        assert len(tracker.list_results()) == 1


# ---------------------------------------------------------------------------
# Reporter
# ---------------------------------------------------------------------------

class TestReporter:
    def test_generate_report_structure(self):
        reporter = Reporter()
        results = [
            _make_result(strategy="C", verdict="promote", verdict_reason="great"),
            _make_result(strategy="D", verdict="kill", verdict_reason="bad sharpe"),
            _make_result(strategy="A", verdict="explore", verdict_reason="borderline"),
        ]
        report = reporter.generate_report(results)

        assert report["summary"]["total"] == 3
        assert report["summary"]["promoted"] == 1
        assert report["summary"]["killed"] == 1
        assert report["summary"]["explore"] == 1
        assert len(report["promoted"]) == 1
        assert len(report["killed"]) == 1
        assert len(report["explore"]) == 1

    def test_promoted_ranked_by_sharpe(self):
        reporter = Reporter()
        results = [
            _make_result(strategy="C", sharpe=1.5, verdict="promote", verdict_reason="p"),
            _make_result(strategy="D", sharpe=3.0, verdict="promote", verdict_reason="p"),
            _make_result(strategy="A", sharpe=2.0, verdict="promote", verdict_reason="p"),
        ]
        report = reporter.generate_report(results)
        sharpes = [r["sharpe"] for r in report["promoted"]]
        assert sharpes == sorted(sharpes, reverse=True)

    def test_to_markdown(self):
        reporter = Reporter()
        results = [
            _make_result(strategy="C", verdict="promote", verdict_reason="great"),
            _make_result(strategy="D", verdict="kill", verdict_reason="bad"),
        ]
        report = reporter.generate_report(results)
        md = reporter.to_markdown(report)
        assert "# Research Findings Report" in md
        assert "Promoted" in md
        assert "Killed" in md

    def test_save_creates_files(self, tmp_path):
        reporter = Reporter()
        results = [_make_result(verdict="promote", verdict_reason="p")]
        report = reporter.generate_report(results)
        output_base = str(tmp_path / "findings")
        reporter.save(report, output_base)
        assert Path(output_base + ".json").exists()
        assert Path(output_base + ".md").exists()

    def test_empty_results(self):
        reporter = Reporter()
        report = reporter.generate_report([])
        assert report["summary"]["total"] == 0
        assert report["summary"]["best_sharpe"] is None
        md = reporter.to_markdown(report)
        assert "# Research Findings Report" in md


# ---------------------------------------------------------------------------
# ResearchAgent — unit tests with mocked subprocess
# ---------------------------------------------------------------------------

class TestResearchAgent:
    def setup_method(self):
        self.tmp = tempfile.mkdtemp()

    def _make_agent(self, tmp_path=None):
        from edge_catcher.research.agent import ResearchAgent
        db = Path(tmp_path or self.tmp) / "research.db"
        return ResearchAgent(research_db=str(db))

    def _mock_backtest_output(self, **overrides) -> str:
        data = {
            "status": "ok",
            "total_trades": 100,
            "wins": 90,
            "losses": 10,
            "win_rate": 0.90,
            "net_pnl_cents": 500.0,
            "sharpe": 2.5,
            "max_drawdown_pct": 5.0,
            "total_fees_paid": 100.0,
            "avg_win_cents": 10.0,
            "avg_loss_cents": -5.0,
            "per_strategy": {},
        }
        data.update(overrides)
        return json.dumps(data)

    def test_run_hypothesis_success(self, tmp_path):
        agent = self._make_agent(tmp_path)
        h = Hypothesis(strategy="C", data_sources=_ds(),
                       start_date="2025-01-01", end_date="2025-12-31")

        mock_proc = MagicMock()
        mock_proc.stdout = self._mock_backtest_output()
        mock_proc.stderr = ""

        mock_pipeline = MagicMock()
        mock_pipeline.validate.return_value = ("promote", "passed all gates", [])

        with patch("subprocess.run", return_value=mock_proc), \
             patch("edge_catcher.research.agent.ValidationPipeline", return_value=mock_pipeline), \
             patch("edge_catcher.research.agent.default_gates", return_value=[]):
            result = agent.run_hypothesis(h)

        assert result.status == "ok"
        assert result.total_trades == 100
        assert result.win_rate == 0.90
        assert result.sharpe == 2.5
        assert result.verdict == "promote"

    def test_run_hypothesis_error_json(self, tmp_path):
        agent = self._make_agent(tmp_path)
        h = Hypothesis(strategy="C", data_sources=_ds(),
                       start_date="2025-01-01", end_date="2025-12-31")

        mock_proc = MagicMock()
        mock_proc.stdout = json.dumps({"status": "error", "message": "series not found"})
        mock_proc.stderr = ""

        with patch("subprocess.run", return_value=mock_proc):
            result = agent.run_hypothesis(h)

        assert result.status == "error"
        assert result.verdict == "kill"
        assert "series not found" in result.verdict_reason

    def test_run_hypothesis_deduplicates(self, tmp_path):
        agent = self._make_agent(tmp_path)
        h = Hypothesis(strategy="C", data_sources=_ds(),
                       start_date="2025-01-01", end_date="2025-12-31")

        mock_proc = MagicMock()
        mock_proc.stdout = self._mock_backtest_output()
        mock_proc.stderr = ""

        with patch("subprocess.run", return_value=mock_proc) as mock_sub:
            agent.run_hypothesis(h)

        # Second run with same params — should not call subprocess again
        with patch("subprocess.run", return_value=mock_proc) as mock_sub2:
            agent.run_hypothesis(h)
            mock_sub2.assert_not_called()

    def test_force_reruns_despite_existing_result(self, tmp_path):
        agent = self._make_agent(tmp_path)
        h = Hypothesis(strategy="C", data_sources=_ds(),
                       start_date="2025-01-01", end_date="2025-12-31")

        mock_proc = MagicMock()
        mock_proc.stdout = self._mock_backtest_output()
        mock_proc.stderr = ""

        with patch("subprocess.run", return_value=mock_proc):
            agent.run_hypothesis(h)

        # Without force — should skip (dedup)
        with patch("subprocess.run", return_value=mock_proc) as mock_sub:
            agent.run_hypothesis(h)
            mock_sub.assert_not_called()

        # With force — should re-run
        agent.force = True
        with patch("subprocess.run", return_value=mock_proc) as mock_sub:
            agent.run_hypothesis(h)
            mock_sub.assert_called_once()

    def test_generate_adjacent_killed_returns_empty(self, tmp_path):
        agent = self._make_agent(tmp_path)
        r = _make_result(strategy="C", verdict="kill", verdict_reason="bad")
        adjacent = agent.generate_adjacent(r)
        assert adjacent == []

    def test_generate_adjacent_explore_returns_cousins(self, tmp_path):
        agent = self._make_agent(tmp_path)
        r = _make_result(strategy="test-strategy-a", verdict="explore", verdict_reason="borderline")
        mock_families = {"test-strategy-a": ["test-strategy-a-vol", "test-strategy-a-mom", "test-strategy-a-stacked"]}
        with patch("edge_catcher.research.agent._build_strategy_families", return_value=mock_families):
            adjacent = agent.generate_adjacent(r)
        strategies = [h.strategy for h in adjacent]
        assert set(strategies) == {"test-strategy-a-vol", "test-strategy-a-mom", "test-strategy-a-stacked"}
        for h in adjacent:
            assert h.parent_id == r.hypothesis.id
            assert h.series == r.hypothesis.series

    def test_generate_adjacent_promoted_targets_other_series(self, tmp_path):
        agent = self._make_agent(tmp_path)
        r = _make_result(strategy="test-strategy-a", series="KXBTCD", verdict="promote", verdict_reason="great")

        mock_discovery = {"data/kalshi.db": ["KXBTCD", "KXETH", "KXNBA"]}
        with patch.object(agent, "_discover_all_series", return_value=mock_discovery):
            adjacent = agent.generate_adjacent(r)

        series = [h.series for h in adjacent]
        assert "KXBTCD" not in series   # skip the one already run
        assert "KXETH" in series
        assert "KXNBA" in series
        for h in adjacent:
            assert h.strategy == "test-strategy-a"
            assert h.parent_id == r.hypothesis.id

    def test_sweep_respects_max_runs(self, tmp_path):
        agent = self._make_agent(tmp_path)

        hypotheses = [
            Hypothesis(strategy="C", data_sources=_ds(series=f"SER{i}"),
                       start_date="2025-01-01", end_date="2025-12-31")
            for i in range(10)
        ]

        mock_proc = MagicMock()
        mock_proc.stdout = self._mock_backtest_output(sharpe=0.1, win_rate=0.5, net_pnl_cents=-10.0)
        mock_proc.stderr = ""

        with patch("subprocess.run", return_value=mock_proc):
            results = agent.sweep(hypotheses, max_runs=3)

        assert len(results) == 3

    def test_sweep_stops_without_hypotheses(self, tmp_path):
        agent = self._make_agent(tmp_path)

        mock_proc = MagicMock()
        mock_proc.stdout = self._mock_backtest_output(sharpe=0.1, win_rate=0.5, net_pnl_cents=-10.0)
        mock_proc.stderr = ""

        with patch("subprocess.run", return_value=mock_proc):
            results = agent.sweep([], max_runs=50)

        assert results == []


# ---------------------------------------------------------------------------
# ResearchAgent.run_backtest_only
# ---------------------------------------------------------------------------

class TestRunBacktestOnly:
    def test_returns_parsed_json(self, tmp_path):
        """run_backtest_only should return parsed JSON dict without saving."""
        import json

        tracker = Tracker(tmp_path / "research.db")
        agent = ResearchAgent.__new__(ResearchAgent)
        agent.tracker = tracker
        agent.evaluator = Evaluator()
        agent.thresholds = Thresholds()
        agent.force = False

        h = Hypothesis(
            strategy="C", data_sources=_ds(),
            start_date="2025-01-01", end_date="2025-12-31",
        )

        fake_output = json.dumps({
            "status": "ok", "total_trades": 50, "sharpe": 1.5,
            "pnl_values": [10, -5, 10],
        })
        mock_proc = MagicMock(stdout=fake_output, stderr="", returncode=0)

        with patch("subprocess.run", return_value=mock_proc):
            data = agent.run_backtest_only(h)

        assert data is not None
        assert data["total_trades"] == 50
        assert data["pnl_values"] == [10, -5, 10]
        # Should NOT have saved to tracker
        assert tracker.list_results() == []

    def test_returns_none_on_timeout(self, tmp_path):
        """run_backtest_only returns None on subprocess timeout."""
        import subprocess

        tracker = Tracker(tmp_path / "research.db")
        agent = ResearchAgent.__new__(ResearchAgent)
        agent.tracker = tracker
        agent.evaluator = Evaluator()
        agent.thresholds = Thresholds()
        agent.force = False

        h = Hypothesis(
            strategy="C", data_sources=_ds(),
            start_date="2025-01-01", end_date="2025-12-31",
        )

        with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("cmd", 300)):
            data = agent.run_backtest_only(h)

        assert data is None


# ---------------------------------------------------------------------------
# Validation pipeline integration
# ---------------------------------------------------------------------------

class TestValidationIntegration:
    """Test that the validation pipeline is wired into run_hypothesis."""

    def test_candidate_goes_through_validation(self, tmp_path):
        """When evaluator returns 'candidate', validation pipeline should run."""
        from unittest.mock import patch, MagicMock
        import json

        tracker = Tracker(tmp_path / "research.db")
        agent = ResearchAgent(tracker=tracker)

        h = Hypothesis(
            strategy="C", data_sources=_ds(),
            start_date="2025-01-01", end_date="2025-12-31",
        )

        fake_output = json.dumps({
            "status": "ok",
            "total_trades": 100, "wins": 90, "losses": 10,
            "win_rate": 0.90, "net_pnl_cents": 500.0,
            "sharpe": 2.5, "max_drawdown_pct": 5.0,
            "total_fees_paid": 100.0,
            "avg_win_cents": 10.0, "avg_loss_cents": -5.0,
            "per_strategy": {},
            "pnl_values": [10] * 90 + [-5] * 10,
        })
        mock_proc = MagicMock(stdout=fake_output, stderr="", returncode=0)

        # Mock validation pipeline to always promote
        mock_pipeline = MagicMock()
        mock_pipeline.validate.return_value = (
            "promote", "passed all gates", [],
        )

        with patch("subprocess.run", return_value=mock_proc), \
             patch("edge_catcher.research.agent.ValidationPipeline", return_value=mock_pipeline), \
             patch("edge_catcher.research.agent.default_gates", return_value=[]):
            result = agent.run_hypothesis(h)

        assert result.verdict == "promote"
        mock_pipeline.validate.assert_called_once()

    def test_candidate_never_persists_on_error(self, tmp_path):
        """If validation crashes, verdict should be 'explore', not 'candidate'."""
        from unittest.mock import patch, MagicMock
        import json

        tracker = Tracker(tmp_path / "research.db")
        agent = ResearchAgent(tracker=tracker)

        h = Hypothesis(
            strategy="C", data_sources=_ds(),
            start_date="2025-01-01", end_date="2025-12-31",
        )

        fake_output = json.dumps({
            "status": "ok",
            "total_trades": 100, "wins": 90, "losses": 10,
            "win_rate": 0.90, "net_pnl_cents": 500.0,
            "sharpe": 2.5, "max_drawdown_pct": 5.0,
            "total_fees_paid": 100.0,
            "avg_win_cents": 10.0, "avg_loss_cents": -5.0,
            "per_strategy": {},
            "pnl_values": [10] * 90 + [-5] * 10,
        })
        mock_proc = MagicMock(stdout=fake_output, stderr="", returncode=0)

        # Mock validation pipeline to raise
        mock_pipeline = MagicMock()
        mock_pipeline.validate.side_effect = RuntimeError("boom")

        with patch("subprocess.run", return_value=mock_proc), \
             patch("edge_catcher.research.agent.ValidationPipeline", return_value=mock_pipeline), \
             patch("edge_catcher.research.agent.default_gates", return_value=[]):
            result = agent.run_hypothesis(h)

        assert result.verdict == "explore"
        assert "validation pipeline error" in result.verdict_reason
        # Verify it was saved to tracker
        rows = tracker.list_results()
        assert len(rows) == 1
        assert rows[0]["verdict"] == "explore"


# ---------------------------------------------------------------------------
# ResearchJournal
# ---------------------------------------------------------------------------

class TestResearchJournal:
    def test_init_creates_table(self, tmp_path):
        from edge_catcher.research.journal import ResearchJournal
        journal = ResearchJournal(tmp_path / "research.db")
        # Should be able to read without error
        entries = journal.read_recent()
        assert entries == []

    def test_write_and_read_recent(self, tmp_path):
        from edge_catcher.research.journal import ResearchJournal
        journal = ResearchJournal(tmp_path / "research.db")
        journal.write_entry("run-001", "outcome", {"phase": "grid", "strategy": "Foo", "best_sharpe": 1.5})
        journal.write_entry("run-001", "observation", {"pattern": "X killed often", "evidence": "4/5 kills"})

        entries = journal.read_recent()
        assert len(entries) == 2
        # Newest first
        assert entries[0]["entry_type"] == "observation"
        assert entries[1]["entry_type"] == "outcome"
        # Content is deserialized
        assert entries[1]["content"]["strategy"] == "Foo"

    def test_write_invalid_entry_type_raises(self, tmp_path):
        from edge_catcher.research.journal import ResearchJournal
        journal = ResearchJournal(tmp_path / "research.db")
        with pytest.raises(ValueError, match="entry_type"):
            journal.write_entry("run-001", "bogus", {})

    def test_get_latest_trajectory_none_when_empty(self, tmp_path):
        from edge_catcher.research.journal import ResearchJournal
        journal = ResearchJournal(tmp_path / "research.db")
        assert journal.get_latest_trajectory() is None

    def test_get_latest_trajectory_returns_most_recent(self, tmp_path):
        from edge_catcher.research.journal import ResearchJournal
        journal = ResearchJournal(tmp_path / "research.db")
        journal.write_entry("run-001", "trajectory", {"status": "stuck", "total_sessions": 10})
        journal.write_entry("run-002", "trajectory", {"status": "improving", "total_sessions": 20})
        traj = journal.get_latest_trajectory()
        assert traj is not None
        assert traj["status"] == "improving"
        assert traj["total_sessions"] == 20

    def test_get_latest_trajectory_ignores_other_types(self, tmp_path):
        from edge_catcher.research.journal import ResearchJournal
        journal = ResearchJournal(tmp_path / "research.db")
        journal.write_entry("run-001", "outcome", {"phase": "grid", "strategy": "Foo"})
        assert journal.get_latest_trajectory() is None

    def test_build_context_trajectory_first(self, tmp_path):
        from edge_catcher.research.journal import ResearchJournal
        journal = ResearchJournal(tmp_path / "research.db")
        journal.write_entry("run-001", "outcome", {"phase": "grid", "strategy": "Bar", "best_sharpe": 1.2, "verdicts": {"promote": 0, "explore": 1, "kill": 2}})
        journal.write_entry("run-001", "trajectory", {"status": "plateauing", "total_sessions": 30, "promote_rate": 0.02})

        ctx = journal.build_context_for_prompt()
        # Trajectory header should appear before outcome
        traj_pos = ctx.find("## Research Trajectory")
        outcome_pos = ctx.find("- **[grid]**")
        assert traj_pos != -1
        assert outcome_pos != -1
        assert traj_pos < outcome_pos

    def test_build_context_character_budget(self, tmp_path):
        from edge_catcher.research.journal import ResearchJournal
        journal = ResearchJournal(tmp_path / "research.db")
        # Write many outcome entries to exceed budget
        for i in range(100):
            journal.write_entry(f"run-{i:03d}", "outcome", {
                "phase": "grid",
                "strategy": f"Strategy{i:04d}",
                "best_sharpe": 1.5,
                "verdicts": {"promote": 1, "explore": 0, "kill": 0},
            })
        ctx = journal.build_context_for_prompt(max_chars=500)
        assert len(ctx) <= 500

    def test_build_context_empty_journal(self, tmp_path):
        from edge_catcher.research.journal import ResearchJournal
        journal = ResearchJournal(tmp_path / "research.db")
        ctx = journal.build_context_for_prompt()
        assert ctx == ""

    def test_classify_trajectory_improving_with_promote(self, tmp_path):
        from edge_catcher.research.journal import ResearchJournal
        results = [
            {"run_id": "run-001", "verdict": "promote", "sharpe": 2.5},
            {"run_id": "run-001", "verdict": "kill", "sharpe": 0.3},
        ]
        status = ResearchJournal.classify_trajectory("run-001", results, None)
        assert status == "improving"

    def test_classify_trajectory_improving_exceeds_prev_best(self, tmp_path):
        from edge_catcher.research.journal import ResearchJournal
        results = [
            {"run_id": "run-002", "verdict": "explore", "sharpe": 2.6},
            {"run_id": "run-001", "verdict": "promote", "sharpe": 2.5},
        ]
        prev = {"status": "improving", "best_sharpe_overall": 2.5}
        # 2.6 > 2.5 → improving (new best Sharpe exceeds previous)
        status = ResearchJournal.classify_trajectory("run-002", results, prev)
        assert status == "improving"

    def test_classify_trajectory_plateauing(self, tmp_path):
        from edge_catcher.research.journal import ResearchJournal
        results = [
            {"run_id": "run-002", "verdict": "explore", "sharpe": 1.2},
            {"run_id": "run-002", "verdict": "kill", "sharpe": 0.5},
            {"run_id": "run-001", "verdict": "promote", "sharpe": 2.5},
        ]
        prev = {"status": "improving", "best_sharpe_overall": 2.5}
        # no promotes, best_this=1.2 < 2.375, but has explore → plateauing
        status = ResearchJournal.classify_trajectory("run-002", results, prev)
        assert status == "plateauing"

    def test_classify_trajectory_stuck(self, tmp_path):
        from edge_catcher.research.journal import ResearchJournal
        results = [
            {"run_id": "run-002", "verdict": "kill", "sharpe": 0.3},
            {"run_id": "run-002", "verdict": "kill", "sharpe": 0.2},
            {"run_id": "run-001", "verdict": "promote", "sharpe": 2.5},
        ]
        prev = {"status": "improving", "best_sharpe_overall": 2.5}
        # no promotes, best_this=0.3 < 2.375, all kills → stuck
        status = ResearchJournal.classify_trajectory("run-002", results, prev)
        assert status == "stuck"

    def test_classify_trajectory_empty_results(self, tmp_path):
        from edge_catcher.research.journal import ResearchJournal
        status = ResearchJournal.classify_trajectory("run-001", [], None)
        assert status == "stuck"

    def test_exported_from_package(self):
        from edge_catcher.research import ResearchJournal
        assert ResearchJournal is not None


# ---------------------------------------------------------------------------
# Helpers for self-performance / loop-integration tests
# ---------------------------------------------------------------------------

def _make_tagged_result(
    strategy: str,
    series: str,
    verdict: str,
    sharpe: float,
    total_trades: int,
    tags: list[str],
    tracker: "Tracker",
    validation_details: list[dict] | None = None,
) -> "HypothesisResult":
    """Create a HypothesisResult with specific tags and save it to the tracker."""
    wins = int(total_trades * 0.6)
    h = Hypothesis(
        strategy=strategy, data_sources=_ds(series=series),
        start_date="2025-01-01", end_date="2025-12-31",
        tags=tags,
    )
    result = HypothesisResult(
        hypothesis=h, status="ok", total_trades=total_trades,
        wins=wins, losses=total_trades - wins,
        win_rate=0.6, net_pnl_cents=100.0, sharpe=sharpe,
        max_drawdown_pct=5.0, fees_paid_cents=10.0,
        avg_win_cents=10.0, avg_loss_cents=-5.0,
        per_strategy={}, verdict=verdict, verdict_reason="test",
        raw_json={},
    )
    tracker.save_result(result, validation_details=validation_details)
    return result


# ---------------------------------------------------------------------------
# LLMIdeator — self-performance summary tests
# ---------------------------------------------------------------------------

class TestSelfPerformanceSummary:
    def _make_ideator(self, tmp_path):
        from unittest.mock import MagicMock
        from edge_catcher.research.llm_ideator import LLMIdeator
        tracker = Tracker(tmp_path / "research.db")
        return LLMIdeator(tracker=tracker, audit=MagicMock(), client=MagicMock()), tracker

    def test_self_performance_empty_no_llm_history(self, tmp_path):
        """No LLM-tagged results → _build_self_performance_summary returns ''."""
        ideator, tracker = self._make_ideator(tmp_path)
        # Save some plain results (no LLM tags)
        tracker.save_result(_make_result(strategy="C", verdict="kill", verdict_reason="bad"))
        result = ideator._build_self_performance_summary()
        assert result == ""

    def test_self_performance_novel_strategies(self, tmp_path):
        """Results tagged source:llm_novel_strategy are categorised; adjacent-tagged ones filtered out."""
        ideator, tracker = self._make_ideator(tmp_path)
        # A genuine novel result
        _make_tagged_result("NovelA", "KXBTCD", "promote", 2.5, 100,
                            ["source:llm_novel_strategy"], tracker)
        # An adjacent expansion — should be excluded from novel group
        _make_tagged_result("NovelA_adj", "KXBTCD", "explore", 1.5, 80,
                            ["source:llm_novel_strategy", "adjacent-promoted"], tracker)

        summary = ideator._build_self_performance_summary()
        assert "## Your Track Record" in summary
        assert "Novel Strategy Proposals" in summary
        # The track record should cover exactly 1 novel result
        assert "Total proposed: 1" in summary

    def test_self_performance_hit_rate_excludes_errors(self, tmp_path):
        """Hit rate counts only promote+explore, not errors."""
        ideator, tracker = self._make_ideator(tmp_path)
        # 1 promote, 1 explore, 1 kill, 1 error
        for verdict, strategy in [
            ("promote", "Nov1"),
            ("explore", "Nov2"),
            ("kill", "Nov3"),
        ]:
            _make_tagged_result(strategy, "KXBTCD", verdict, 1.5, 100,
                                ["source:llm_novel_strategy"], tracker)
        # Add an error result manually
        h_err = Hypothesis(
            strategy="Nov4", data_sources=_ds(),
            start_date="2025-01-01", end_date="2025-12-31",
            tags=["source:llm_novel_strategy"],
        )
        err_result = HypothesisResult.error(h_err, "simulated failure")
        tracker.save_result(err_result)

        summary = ideator._build_self_performance_summary()
        # 2 non-kill out of 4 total = 50%
        assert "Hit rate (non-kill): 50%" in summary

    def test_self_performance_validation_gates(self, tmp_path):
        """Results with validation_details produce gate stats in the summary."""
        ideator, tracker = self._make_ideator(tmp_path)
        gates = [
            {"gate_name": "deflated_sharpe", "passed": True, "reason": "DSR ok"},
            {"gate_name": "walk_forward", "passed": False, "reason": "WF failed"},
        ]
        _make_tagged_result("NovelB", "KXBTCD", "explore", 1.8, 100,
                            ["source:llm_novel_strategy"], tracker,
                            validation_details=gates)

        summary = ideator._build_self_performance_summary()
        assert "Validation Gate Performance" in summary
        assert "deflated_sharpe" in summary
        assert "walk_forward" in summary

    def test_self_performance_steering_trade_bottleneck(self, tmp_path):
        """More than 50% of novel proposals killed for <50 trades → directive appears."""
        ideator, tracker = self._make_ideator(tmp_path)
        # 3 novel results killed with low trade count
        for i in range(3):
            _make_tagged_result(f"ThinStrat{i}", "KXBTCD", "kill", 0.5, 10,
                                ["source:llm_novel_strategy"], tracker)
        # 1 promote with decent trade count
        _make_tagged_result("GoodStrat", "KXBTCD", "promote", 2.5, 120,
                            ["source:llm_novel_strategy"], tracker)

        summary = ideator._build_self_performance_summary()
        assert "Trade frequency bottleneck" in summary

    def test_self_performance_steering_hardest_gate(self, tmp_path):
        """A gate with <50% pass rate → directive naming that gate appears."""
        ideator, tracker = self._make_ideator(tmp_path)
        # Two candidates; walk_forward fails on both → 0% pass rate
        for strategy in ["CandA", "CandB"]:
            gates = [
                {"gate_name": "deflated_sharpe", "passed": True, "reason": "ok"},
                {"gate_name": "walk_forward", "passed": False, "reason": "failed"},
            ]
            _make_tagged_result(strategy, "KXBTCD", "explore", 1.5, 100,
                                ["source:llm_novel_strategy"], tracker,
                                validation_details=gates)

        summary = ideator._build_self_performance_summary()
        assert "walk_forward" in summary
        # The directive should cite the hard gate
        assert "hardest gate" in summary or "0%" in summary


# ---------------------------------------------------------------------------
# Tag helpers
# ---------------------------------------------------------------------------

class TestTagHelpers:
    def _row(self, tags) -> dict:
        return {"tags": tags}

    def test_has_tag_json_string(self):
        from edge_catcher.research.llm_ideator import LLMIdeator
        row = self._row(json.dumps(["source:llm_ideated", "foo"]))
        assert LLMIdeator._has_tag(row, "source:llm_ideated") is True
        assert LLMIdeator._has_tag(row, "bar") is False

    def test_has_tag_list(self):
        from edge_catcher.research.llm_ideator import LLMIdeator
        row = self._row(["source:llm_novel_strategy"])
        assert LLMIdeator._has_tag(row, "source:llm_novel_strategy") is True
        assert LLMIdeator._has_tag(row, "other") is False

    def test_has_tag_none(self):
        from edge_catcher.research.llm_ideator import LLMIdeator
        row = self._row(None)
        assert LLMIdeator._has_tag(row, "anything") is False

    def test_has_any_adjacent_tag_promoted(self):
        from edge_catcher.research.llm_ideator import LLMIdeator
        row = self._row(["source:llm_novel_strategy", "adjacent-promoted"])
        assert LLMIdeator._has_any_adjacent_tag(row) is True

    def test_has_any_adjacent_tag_explore(self):
        from edge_catcher.research.llm_ideator import LLMIdeator
        row = self._row(json.dumps(["source:llm_novel_strategy", "adjacent-explore"]))
        assert LLMIdeator._has_any_adjacent_tag(row) is True

    def test_has_any_adjacent_tag_absent(self):
        from edge_catcher.research.llm_ideator import LLMIdeator
        row = self._row(["source:llm_novel_strategy"])
        assert LLMIdeator._has_any_adjacent_tag(row) is False


# ---------------------------------------------------------------------------
# Validation gate analysis helpers
# ---------------------------------------------------------------------------

class TestGateHelpers:
    def _make_candidates(self) -> list[dict]:
        return [
            {
                "verdict": "promote",
                "validation_details": json.dumps([
                    {"gate_name": "dsr", "passed": True},
                    {"gate_name": "walk_forward", "passed": True},
                ]),
            },
            {
                "verdict": "explore",
                "validation_details": json.dumps([
                    {"gate_name": "dsr", "passed": True},
                    {"gate_name": "walk_forward", "passed": False},
                ]),
            },
            {
                "verdict": "kill",
                "validation_details": json.dumps([
                    {"gate_name": "dsr", "passed": False},
                    {"gate_name": "walk_forward", "passed": False},
                ]),
            },
        ]

    def test_compute_gate_stats_pass_rates(self):
        from edge_catcher.research.llm_ideator import LLMIdeator
        candidates = self._make_candidates()
        stats = LLMIdeator._compute_gate_stats(candidates)
        # dsr: 2 passed, 1 failed → 66.7%
        assert stats["dsr"]["passed"] == 2
        assert stats["dsr"]["failed"] == 1
        assert abs(stats["dsr"]["pass_rate"] - 2 / 3) < 0.01
        # walk_forward: 1 passed, 2 failed → 33.3%
        assert stats["walk_forward"]["passed"] == 1
        assert stats["walk_forward"]["failed"] == 2

    def test_compute_gate_stats_empty(self):
        from edge_catcher.research.llm_ideator import LLMIdeator
        assert LLMIdeator._compute_gate_stats([]) == {}

    def test_summarize_validation_gates_output(self, tmp_path):
        from unittest.mock import MagicMock
        from edge_catcher.research.llm_ideator import LLMIdeator
        tracker = Tracker(tmp_path / "research.db")
        ideator = LLMIdeator(tracker=tracker, audit=MagicMock(), client=MagicMock())
        candidates = self._make_candidates()
        summary = ideator._summarize_validation_gates(candidates)
        assert "Validation Gate Performance" in summary
        assert "Candidates reaching validation: 3" in summary
        assert "walk_forward" in summary
        assert "dsr" in summary

    def test_build_steering_directives_hardest_gate(self, tmp_path):
        from unittest.mock import MagicMock
        from edge_catcher.research.llm_ideator import LLMIdeator
        tracker = Tracker(tmp_path / "research.db")
        ideator = LLMIdeator(tracker=tracker, audit=MagicMock(), client=MagicMock())
        gate_stats = {
            "easy_gate": {"passed": 9, "failed": 1, "pass_rate": 0.9},
            "hard_gate": {"passed": 1, "failed": 9, "pass_rate": 0.1},
        }
        directives = ideator._build_steering_directives([], [], gate_stats)
        assert "hard_gate" in directives
        assert "10%" in directives

    def test_build_steering_directives_trade_bottleneck(self, tmp_path):
        from unittest.mock import MagicMock
        from edge_catcher.research.llm_ideator import LLMIdeator
        tracker = Tracker(tmp_path / "research.db")
        ideator = LLMIdeator(tracker=tracker, audit=MagicMock(), client=MagicMock())
        # Build 3 novel results with low trades and kill verdict
        novel = [
            {"strategy": f"S{i}", "series": "X", "total_trades": 5,
             "verdict": "kill", "sharpe": 0.1}
            for i in range(3)
        ]
        directives = ideator._build_steering_directives(novel, [], {})
        assert "Trade frequency bottleneck" in directives

    def test_build_steering_directives_empty(self, tmp_path):
        from unittest.mock import MagicMock
        from edge_catcher.research.llm_ideator import LLMIdeator
        tracker = Tracker(tmp_path / "research.db")
        ideator = LLMIdeator(tracker=tracker, audit=MagicMock(), client=MagicMock())
        result = ideator._build_steering_directives([], [], {})
        assert result == ""


# ---------------------------------------------------------------------------
# LoopOrchestrator — journal integration
# ---------------------------------------------------------------------------

class TestLoopJournalIntegration:
    def _make_orch(self, tmp_path):
        from edge_catcher.research.loop import LoopOrchestrator
        return LoopOrchestrator(research_db=str(tmp_path / "research.db"))

    def _make_journal(self, tmp_path):
        from edge_catcher.research.journal import ResearchJournal
        return ResearchJournal(db_path=str(tmp_path / "research.db"))

    def test_write_phase_outcomes(self, tmp_path):
        """_write_phase_outcomes writes one outcome entry per strategy."""
        orch = self._make_orch(tmp_path)
        journal = self._make_journal(tmp_path)

        results = [
            _make_result(strategy="Foo", series="KXBTCD", verdict="promote",
                         verdict_reason="great", sharpe=2.5, total_trades=100),
            _make_result(strategy="Foo", series="KXETH", verdict="explore",
                         verdict_reason="mid", sharpe=1.5, total_trades=80),
            _make_result(strategy="Bar", series="KXBTCD", verdict="kill",
                         verdict_reason="bad", sharpe=0.5, total_trades=50),
        ]

        orch._write_phase_outcomes(journal, results, "grid")

        entries = journal.read_recent()
        # Two strategies → two outcome entries (plus one near-miss observation)
        outcome_entries = [e for e in entries if e["entry_type"] == "outcome"]
        assert len(outcome_entries) == 2
        strategies = {e["content"]["strategy"] for e in outcome_entries}
        assert strategies == {"Foo", "Bar"}
        phases = {e["content"]["phase"] for e in outcome_entries}
        assert phases == {"grid"}

    def test_write_phase_outcomes_verdict_aggregation(self, tmp_path):
        """Verdicts for same strategy are summed across series."""
        orch = self._make_orch(tmp_path)
        journal = self._make_journal(tmp_path)

        results = [
            _make_result(strategy="Foo", series="KXBTCD", verdict="promote",
                         verdict_reason="good", sharpe=2.5, total_trades=100),
            _make_result(strategy="Foo", series="KXETH", verdict="kill",
                         verdict_reason="bad", sharpe=0.5, total_trades=50),
            _make_result(strategy="Foo", series="KXNBA", verdict="kill",
                         verdict_reason="bad2", sharpe=0.4, total_trades=40),
        ]

        orch._write_phase_outcomes(journal, results, "grid")

        entries = journal.read_recent()
        # One strategy → one outcome entry (plus one near-miss observation)
        outcome_entries = [e for e in entries if e["entry_type"] == "outcome"]
        assert len(outcome_entries) == 1
        verdicts = outcome_entries[0]["content"]["verdicts"]
        assert verdicts["promote"] == 1
        assert verdicts["kill"] == 2

    def test_write_phase_outcomes_empty(self, tmp_path):
        """Empty results list produces no journal entries."""
        orch = self._make_orch(tmp_path)
        journal = self._make_journal(tmp_path)
        orch._write_phase_outcomes(journal, [], "grid")
        assert journal.read_recent() == []

    def test_write_journal_summary_trajectory(self, tmp_path):
        """_write_journal_summary writes a trajectory entry with correct fields."""
        orch = self._make_orch(tmp_path)
        journal = self._make_journal(tmp_path)

        results = [
            _make_result(strategy="X", verdict="promote", verdict_reason="p",
                         sharpe=3.0, total_trades=100),
            _make_result(strategy="Y", verdict="kill", verdict_reason="k",
                         sharpe=0.5, total_trades=50),
        ]

        orch._write_journal_summary(journal, results)

        entries = journal.read_recent()
        trajectory_entries = [e for e in entries if e["entry_type"] == "trajectory"]
        assert len(trajectory_entries) == 1
        traj = trajectory_entries[0]["content"]
        assert traj["status"] in {"improving", "plateauing", "stuck"}
        assert traj["total_sessions"] == 1
        assert traj["new_promotes"] == 1
        assert traj["new_kills"] == 1
        assert traj["best_sharpe_this_run"] == pytest.approx(3.0)

    def test_write_journal_summary_observations(self, tmp_path):
        """Promoted results generate observation entries."""
        orch = self._make_orch(tmp_path)
        journal = self._make_journal(tmp_path)

        results = [
            _make_result(strategy="WinStrat", series="KXBTCD", verdict="promote",
                         verdict_reason="excellent", sharpe=3.5,
                         total_trades=120, win_rate=0.75, net_pnl_cents=800.0),
        ]

        orch._write_journal_summary(journal, results)

        entries = journal.read_recent()
        observation_entries = [e for e in entries if e["entry_type"] == "observation"]
        assert len(observation_entries) >= 1
        patterns = [e["content"]["pattern"] for e in observation_entries]
        assert any("WinStrat" in p and "PROMOTED" in p for p in patterns)

    def test_write_journal_summary_high_kill_rate_observation(self, tmp_path):
        """Strategy with >80% kill rate across >=3 series gets an observation entry."""
        orch = self._make_orch(tmp_path)
        journal = self._make_journal(tmp_path)

        # LoserStrat: 4 kills out of 4 series
        results = [
            _make_result(strategy="LoserStrat", series=f"KXSER{i}", verdict="kill",
                         verdict_reason="bad", sharpe=0.3, total_trades=30)
            for i in range(4)
        ]

        orch._write_journal_summary(journal, results)

        entries = journal.read_recent()
        observations = [e for e in entries if e["entry_type"] == "observation"]
        patterns = [e["content"]["pattern"] for e in observations]
        assert any("LoserStrat" in p for p in patterns)

    def test_write_journal_summary_low_trade_observation(self, tmp_path):
        """Strategy averaging <50 trades across >=2 series gets an observation entry."""
        orch = self._make_orch(tmp_path)
        journal = self._make_journal(tmp_path)

        results = [
            _make_result(strategy="ThinStrat", series="KXBTCD", verdict="kill",
                         verdict_reason="few trades", sharpe=0.5, total_trades=10),
            _make_result(strategy="ThinStrat", series="KXETH", verdict="kill",
                         verdict_reason="few trades", sharpe=0.4, total_trades=15),
        ]

        orch._write_journal_summary(journal, results)

        entries = journal.read_recent()
        observations = [e for e in entries if e["entry_type"] == "observation"]
        patterns = [e["content"]["pattern"] for e in observations]
        assert any("ThinStrat" in p for p in patterns)


# ---------------------------------------------------------------------------
# Tracker extensions
# ---------------------------------------------------------------------------

class TestTrackerExtensions:
    def test_list_results_with_limit(self, tmp_path):
        db = str(tmp_path / "research.db")
        tracker = Tracker(db)
        for i in range(5):
            result = _make_result(strategy=f"S{i}", verdict="kill")
            tracker.save_hypothesis(result.hypothesis)
            tracker.save_result(result)
        results = tracker.list_results(limit=3)
        assert len(results) == 3

    def test_list_results_with_offset(self, tmp_path):
        db = str(tmp_path / "research.db")
        tracker = Tracker(db)
        for i in range(5):
            result = _make_result(strategy=f"S{i}", verdict="kill")
            tracker.save_hypothesis(result.hypothesis)
            tracker.save_result(result)
        all_results = tracker.list_results()
        offset_results = tracker.list_results(limit=3, offset=2)
        assert len(offset_results) == 3
        assert offset_results[0]["strategy"] == all_results[2]["strategy"]

    def test_count_by_verdict(self, tmp_path):
        db = str(tmp_path / "research.db")
        tracker = Tracker(db)
        for verdict, count in [("promote", 2), ("kill", 3), ("review", 1)]:
            for i in range(count):
                result = _make_result(strategy=f"{verdict}_{i}", verdict=verdict)
                tracker.save_hypothesis(result.hypothesis)
                tracker.save_result(result)
        counts = tracker.count_by_verdict()
        assert counts["promote"] == 2
        assert counts["kill"] == 3
        assert counts["review"] == 1

    def test_update_verdict(self, tmp_path):
        db = str(tmp_path / "research.db")
        tracker = Tracker(db)
        result = _make_result(strategy="S1", verdict="promote")
        tracker.save_hypothesis(result.hypothesis)
        tracker.save_result(result)
        tracker.update_verdict(result.hypothesis.id, "accepted")
        updated = tracker.get_result_by_id(result.hypothesis.id)
        assert updated["verdict"] == "accepted"
