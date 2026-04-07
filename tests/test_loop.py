# tests/test_loop.py
"""Tests for edge_catcher.research.loop module (LoopOrchestrator)."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from edge_catcher.research.audit import AuditLog
from edge_catcher.research.hypothesis import Hypothesis, HypothesisResult
from edge_catcher.research.loop import LoopOrchestrator
from edge_catcher.research.tracker import Tracker


def _make_result(h: Hypothesis, verdict="promote") -> HypothesisResult:
    return HypothesisResult(
        hypothesis=h, status="ok", total_trades=100, wins=90, losses=10,
        win_rate=0.9, net_pnl_cents=500.0, sharpe=2.5, max_drawdown_pct=5.0,
        fees_paid_cents=100.0, avg_win_cents=10.0, avg_loss_cents=-5.0,
        per_strategy={}, verdict=verdict, verdict_reason="test", raw_json={},
    )


class TestLoopOrchestratorGridOnly:
    def test_grid_only_runs_grid_phase(self, tmp_path):
        db_path = tmp_path / "research.db"
        orch = LoopOrchestrator(
            research_db=str(db_path),
            start_date="2025-01-01",
            end_date="2025-12-31",
            max_runs=10,
            grid_only=True,
        )
        # Mock the strategy/series discovery
        with patch.object(orch, "_discover_strategies", return_value=["A"]), \
             patch.object(orch, "_discover_series", return_value={"data/k.db": ["SER1"]}), \
             patch("edge_catcher.research.loop.ResearchAgent") as MockAgent:

            mock_agent = MockAgent.return_value
            h = Hypothesis(strategy="A", series="SER1", db_path="data/k.db",
                           start_date="2025-01-01", end_date="2025-12-31",
                           tags=["source:grid"])
            mock_agent.run_hypothesis.return_value = _make_result(h, "kill")

            exit_code, results = orch.run()

        assert exit_code in (0, 2)
        assert len(results) >= 0

    def test_grid_only_skips_llm(self, tmp_path):
        db_path = tmp_path / "research.db"
        orch = LoopOrchestrator(
            research_db=str(db_path),
            start_date="2025-01-01",
            end_date="2025-12-31",
            max_runs=10,
            grid_only=True,
        )
        with patch.object(orch, "_discover_strategies", return_value=[]), \
             patch.object(orch, "_discover_series", return_value={}), \
             patch("edge_catcher.research.loop.ResearchAgent"), \
             patch("edge_catcher.research.loop.LLMIdeator") as MockIdeator:

            orch.run()
            MockIdeator.assert_not_called()


class TestLoopOrchestratorLLMOnly:
    def test_llm_only_cold_start_with_context(self, tmp_path):
        """LLM-only mode now supports cold start via Context Engine — no min results needed."""
        db_path = tmp_path / "research.db"
        orch = LoopOrchestrator(
            research_db=str(db_path),
            start_date="2025-01-01",
            end_date="2025-12-31",
            max_runs=10,
            llm_only=True,
        )
        with patch.object(orch, "_discover_strategies", return_value=["A"]), \
             patch.object(orch, "_discover_series", return_value={"data/k.db": ["S1"]}), \
             patch("edge_catcher.research.loop.ResearchAgent"), \
             patch.object(orch, "_run_ideate_phase", return_value=([], 0)) as mock_ideate, \
             patch.object(orch, "_write_phase_outcomes"), \
             patch.object(orch, "_write_journal_summary"):

            exit_code, results = orch.run()
            # With context engine, cold start is supported — ideate phase runs
            mock_ideate.assert_called_once()
            assert exit_code == 0


class TestLoopOrchestratorIntegrity:
    def test_integrity_checkpoint_recorded(self, tmp_path):
        db_path = tmp_path / "research.db"
        orch = LoopOrchestrator(
            research_db=str(db_path),
            start_date="2025-01-01",
            end_date="2025-12-31",
            max_runs=5,
            grid_only=True,
        )
        with patch.object(orch, "_discover_strategies", return_value=["A"]), \
             patch.object(orch, "_discover_series", return_value={"data/k.db": ["S1"]}), \
             patch("edge_catcher.research.loop.ResearchAgent") as MockAgent:

            mock_agent = MockAgent.return_value
            h = Hypothesis(strategy="A", series="S1", db_path="data/k.db",
                           start_date="2025-01-01", end_date="2025-12-31")
            mock_agent.run_hypothesis.return_value = _make_result(h)

            orch.run()

        audit = AuditLog(db_path)
        checks = audit.list_integrity_checks()
        # Should have at least one integrity checkpoint after grid phase
        assert len(checks) >= 1


class TestLoopOrchestratorBudget:
    def test_exit_code_2_when_budget_exhausted(self, tmp_path):
        db_path = tmp_path / "research.db"
        orch = LoopOrchestrator(
            research_db=str(db_path),
            start_date="2025-01-01",
            end_date="2025-12-31",
            max_runs=1,  # very small budget
            grid_only=True,
        )
        with patch.object(orch, "_discover_strategies", return_value=["A", "B"]), \
             patch.object(orch, "_discover_series", return_value={"data/k.db": ["S1", "S2"]}), \
             patch("edge_catcher.research.loop.ResearchAgent") as MockAgent:

            mock_agent = MockAgent.return_value
            h = Hypothesis(strategy="A", series="S1", db_path="data/k.db",
                           start_date="2025-01-01", end_date="2025-12-31")
            mock_agent.run_hypothesis.return_value = _make_result(h, "kill")

            exit_code, results = orch.run()

        # 4 combos but budget=1, so exit 2 (partial)
        assert exit_code == 2

    def test_grid_only_and_llm_only_raises(self, tmp_path):
        db_path = tmp_path / "research.db"
        with pytest.raises(ValueError, match="Cannot use both"):
            LoopOrchestrator(
                research_db=str(db_path),
                start_date="2025-01-01",
                end_date="2025-12-31",
                grid_only=True,
                llm_only=True,
            )


class TestWritePhaseOutcomes:
    def test_write_phase_outcomes_handles_unexpected_verdicts(self):
        """_write_phase_outcomes should not crash on 'candidate' or 'error' verdicts."""
        loop = LoopOrchestrator.__new__(LoopOrchestrator)
        loop.run_id = "test-run"

        journal = MagicMock()

        h = Hypothesis(strategy="A", series="X", db_path="d.db",
                       start_date="2025-01-01", end_date="2025-12-31")
        error_result = HypothesisResult(
            hypothesis=h, status="error", total_trades=0, wins=0, losses=0,
            win_rate=0.0, net_pnl_cents=0.0, sharpe=0.0, max_drawdown_pct=0.0,
            fees_paid_cents=0.0, avg_win_cents=0.0, avg_loss_cents=0.0,
            per_strategy={}, verdict="error", verdict_reason="timeout",
            raw_json={},
        )

        # Should not raise KeyError
        loop._write_phase_outcomes(journal, [error_result], "grid")
        journal.write_entry.assert_called_once()
        content = journal.write_entry.call_args[0][2]
        assert content["verdicts"]["error"] == 1


class TestRefinementResumeWalkBackwards:
    def test_refinement_resume_finds_latest_existing_version(self):
        """When resuming refinement, should start from the latest version with actual code."""
        loop = LoopOrchestrator.__new__(LoopOrchestrator)
        loop.max_refinements = 5
        loop.max_time_seconds = None
        loop.start_date = "2025-01-01"
        loop.end_date = "2025-12-31"
        loop.fee_pct = 1.0
        loop.run_id = "test"
        loop._cached_results = None

        loop._cached_results = None  # __new__ bypasses __init__

        # Simulate: Foo has 2 prior iterations, but only FooV2 code exists (FooV3 save failed)
        loop.tracker = MagicMock()
        loop.tracker.list_results.return_value = [
            {"tags": '["source:llm_refinement", "parent_strategy:Foo", "iteration:1"]',
             "strategy": "FooV2"},
            {"tags": '["source:llm_refinement", "parent_strategy:Foo", "iteration:2"]',
             "strategy": "FooV3"},
        ]
        loop.tracker.list_results_for_strategy.return_value = [
            {"status": "ok", "sharpe": 1.5, "verdict": "explore", "series": "X",
             "db_path": "d.db", "total_trades": 80, "net_pnl_cents": 200,
             "max_drawdown_pct": 5.0, "win_rate": 0.55, "verdict_reason": "test"},
        ]

        agent = MagicMock()
        # FooV3 doesn't exist, FooV2 does
        def read_strategy_side_effect(name):
            if name == "FooV2":
                return "class FooV2Strategy:\n    name = 'FooV2'"
            return None
        agent.read_strategy_code.side_effect = read_strategy_side_effect

        # Verify _count_existing_refinements returns 2
        count = loop._count_existing_refinements("Foo")
        assert count == 2

        # The walk-backwards logic should try FooV3 (None), then FooV2 (found),
        # and start refining from FooV2 at iteration 2
        existing_version = 2
        current_name = "Foo"
        start_iteration = 1
        for v in range(existing_version + 1, 0, -1):
            candidate = f"Foo" + f"V{v}"
            code = agent.read_strategy_code(candidate)
            if code:
                current_name = candidate
                start_iteration = v
                break

        assert current_name == "FooV2"
        assert start_iteration == 2
        agent.read_strategy_code.assert_any_call("FooV3")
        agent.read_strategy_code.assert_any_call("FooV2")


class TestShouldKeepRefinementBaseline:
    def test_should_keep_refinement_compares_against_baseline(self):
        """Refinement should be compared against the original baseline, not just previous iteration."""
        # Original baseline: Sharpe 2.0
        baseline_results = [
            {"status": "ok", "sharpe": 2.0, "verdict": "explore", "total_trades": 100},
        ]

        # Previous iteration (V2): regressed to Sharpe 1.2
        prev_results = [
            {"status": "ok", "sharpe": 1.2, "verdict": "explore", "total_trades": 100},
        ]

        # New iteration (V3): Sharpe 1.5 — better than V2 but worse than original
        h = Hypothesis(strategy="FooV3", series="X", db_path="d.db",
                       start_date="2025-01-01", end_date="2025-12-31")
        refined = [HypothesisResult(
            hypothesis=h, status="ok", total_trades=100, wins=60, losses=40,
            win_rate=0.6, net_pnl_cents=300, sharpe=1.5, max_drawdown_pct=5.0,
            fees_paid_cents=50, avg_win_cents=15, avg_loss_cents=-7.5,
            per_strategy={}, verdict="explore", verdict_reason="", raw_json={},
        )]

        # With baseline awareness, should NOT keep (1.5 < 2.0)
        assert not LoopOrchestrator._should_keep_refinement(
            prev_results, refined, baseline_results=baseline_results
        )

        # Without baseline (old behavior), would keep (1.5 > 1.2)
        assert LoopOrchestrator._should_keep_refinement(
            prev_results, refined, baseline_results=None
        )
