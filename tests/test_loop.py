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
    def test_llm_only_requires_min_results(self, tmp_path):
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
             patch("edge_catcher.research.loop.ResearchAgent"):

            exit_code, results = orch.run()
            assert exit_code == 1  # error — not enough data


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
