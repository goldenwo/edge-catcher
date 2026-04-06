"""Tests for edge_catcher.research module."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from edge_catcher.research.agent import ResearchAgent
from edge_catcher.research.evaluator import Evaluator, Thresholds
from edge_catcher.research.hypothesis import Hypothesis, HypothesisResult
from edge_catcher.research.reporter import Reporter
from edge_catcher.research.tracker import Tracker


# ---------------------------------------------------------------------------
# Hypothesis
# ---------------------------------------------------------------------------

class TestHypothesis:
    def test_auto_uuid(self):
        h1 = Hypothesis(strategy="C", series="KXBTCD", db_path="data/kalshi.db",
                        start_date="2025-01-01", end_date="2025-12-31")
        h2 = Hypothesis(strategy="C", series="KXBTCD", db_path="data/kalshi.db",
                        start_date="2025-01-01", end_date="2025-12-31")
        assert h1.id != h2.id

    def test_dedup_key_ignores_id(self):
        h1 = Hypothesis(strategy="C", series="KXBTCD", db_path="data/kalshi.db",
                        start_date="2025-01-01", end_date="2025-12-31", fee_pct=1.0)
        h2 = Hypothesis(strategy="C", series="KXBTCD", db_path="data/kalshi.db",
                        start_date="2025-01-01", end_date="2025-12-31", fee_pct=1.0)
        assert h1.dedup_key() == h2.dedup_key()

    def test_dedup_key_differs_on_fee(self):
        h1 = Hypothesis(strategy="C", series="KXBTCD", db_path="data/kalshi.db",
                        start_date="2025-01-01", end_date="2025-12-31", fee_pct=1.0)
        h2 = Hypothesis(strategy="C", series="KXBTCD", db_path="data/kalshi.db",
                        start_date="2025-01-01", end_date="2025-12-31", fee_pct=0.25)
        assert h1.dedup_key() != h2.dedup_key()

    def test_error_constructor(self):
        h = Hypothesis(strategy="C", series="KXBTCD", db_path="x.db",
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
    h = Hypothesis(strategy=strategy, series=series, db_path="data/kalshi.db",
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

    def test_explore_mid_sharpe(self):
        """Sharpe between kill (1.0) and promote (2.0) → explore, regardless of win rate."""
        r = _make_result(sharpe=1.5, win_rate=0.70, net_pnl_cents=500.0, total_trades=100)
        verdict, reason = self.ev.evaluate(r, self.th)
        assert verdict == "explore"

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

    def test_explore_between_thresholds(self):
        # Sharpe between 1.0 and 2.0, win rate between 0.85 and 0.87
        r = _make_result(sharpe=1.5, win_rate=0.86, net_pnl_cents=200.0, total_trades=100)
        verdict, reason = self.ev.evaluate(r, self.th)
        assert verdict == "explore"

    def test_error_result_is_killed(self):
        h = Hypothesis(strategy="C", series="KXBTCD", db_path="x.db",
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
        h2 = Hypothesis(strategy="C", series="KXBTCD", db_path="data/kalshi.db",
                        start_date="2025-01-01", end_date="2025-12-31", fee_pct=1.0)
        existing_id = tracker.is_tested(h2)
        assert existing_id is not None

    def test_no_dedup_different_fee(self, tmp_path):
        tracker = Tracker(tmp_path / "research.db")
        r = _make_result(verdict="promote", verdict_reason="test")
        tracker.save_result(r)

        h2 = Hypothesis(strategy="C", series="KXBTCD", db_path="data/kalshi.db",
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
            strategy="D", series="KXETH", db_path="data/kalshi.db",
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
        h = Hypothesis(strategy="C", series="KXBTCD", db_path="data/kalshi.db",
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
        h = Hypothesis(strategy="C", series="KXBTCD", db_path="data/kalshi.db",
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
        h = Hypothesis(strategy="C", series="KXBTCD", db_path="data/kalshi.db",
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
        h = Hypothesis(strategy="C", series="KXBTCD", db_path="data/kalshi.db",
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
        r = _make_result(strategy="C", verdict="explore", verdict_reason="borderline")
        adjacent = agent.generate_adjacent(r)
        strategies = [h.strategy for h in adjacent]
        assert set(strategies) == {"Cvol", "Cmom", "Cstack"}
        for h in adjacent:
            assert h.parent_id == r.hypothesis.id
            assert h.series == r.hypothesis.series

    def test_generate_adjacent_promoted_targets_other_series(self, tmp_path):
        agent = self._make_agent(tmp_path)
        r = _make_result(strategy="C", series="KXBTCD", verdict="promote", verdict_reason="great")

        mock_discovery = {"data/kalshi.db": ["KXBTCD", "KXETH", "KXNBA"]}
        with patch.object(agent, "_discover_all_series", return_value=mock_discovery):
            adjacent = agent.generate_adjacent(r)

        series = [h.series for h in adjacent]
        assert "KXBTCD" not in series   # skip the one already run
        assert "KXETH" in series
        assert "KXNBA" in series
        for h in adjacent:
            assert h.strategy == "C"
            assert h.parent_id == r.hypothesis.id

    def test_sweep_respects_max_runs(self, tmp_path):
        agent = self._make_agent(tmp_path)

        hypotheses = [
            Hypothesis(strategy="C", series=f"SER{i}", db_path="data/kalshi.db",
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
            strategy="C", series="KXBTCD", db_path="data/kalshi.db",
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
            strategy="C", series="KXBTCD", db_path="data/kalshi.db",
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
            strategy="C", series="KXBTCD", db_path="data/kalshi.db",
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
            strategy="C", series="KXBTCD", db_path="data/kalshi.db",
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
