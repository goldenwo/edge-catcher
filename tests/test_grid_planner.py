# tests/test_grid_planner.py
"""Tests for edge_catcher.research.grid_planner module."""

from __future__ import annotations

from unittest.mock import patch, MagicMock

import pytest

from edge_catcher.research.grid_planner import GridPlanner
from edge_catcher.research.hypothesis import Hypothesis, HypothesisResult
from edge_catcher.research.tracker import Tracker


def _grid_result(strategy="C", series="KXBTCD", db_path="data/kalshi.db",
                 verdict="kill", verdict_reason="k", **kw) -> HypothesisResult:
    """Helper that creates a result with explicit db_path control."""
    h = Hypothesis(strategy=strategy, series=series, db_path=db_path,
                   start_date="2025-01-01", end_date="2025-12-31")
    defaults = dict(status="ok", total_trades=100, wins=90, losses=10,
                    win_rate=0.9, net_pnl_cents=500.0, sharpe=2.5,
                    max_drawdown_pct=5.0, fees_paid_cents=100.0,
                    avg_win_cents=10.0, avg_loss_cents=-5.0,
                    per_strategy={}, raw_json={})
    defaults.update(kw)
    return HypothesisResult(hypothesis=h, verdict=verdict,
                            verdict_reason=verdict_reason, **defaults)


class TestGridPlannerGenerate:
    def test_generates_all_combos(self, tmp_path):
        tracker = Tracker(tmp_path / "research.db")
        planner = GridPlanner(tracker=tracker)
        strategies = ["A", "B"]
        series_map = {"data/kalshi.db": ["KXBTCD", "KXETH"]}
        hypotheses = planner.generate(
            strategies=strategies,
            series_map=series_map,
            start_date="2025-01-01",
            end_date="2025-12-31",
            fee_pct=1.0,
        )
        # 2 strategies × 2 series = 4
        assert len(hypotheses) == 4
        combos = {(h.strategy, h.series, h.db_path) for h in hypotheses}
        assert ("A", "KXBTCD", "data/kalshi.db") in combos
        assert ("B", "KXETH", "data/kalshi.db") in combos

    def test_skips_already_tested(self, tmp_path):
        tracker = Tracker(tmp_path / "research.db")
        # Pre-save a result for A × KXBTCD
        r = _grid_result(strategy="A", series="KXBTCD", verdict="kill", verdict_reason="k")
        tracker.save_result(r)

        planner = GridPlanner(tracker=tracker)
        hypotheses = planner.generate(
            strategies=["A", "B"],
            series_map={"data/kalshi.db": ["KXBTCD", "KXETH"]},
            start_date="2025-01-01",
            end_date="2025-12-31",
            fee_pct=1.0,
        )
        strategies_series = [(h.strategy, h.series) for h in hypotheses]
        assert ("A", "KXBTCD") not in strategies_series
        assert len(hypotheses) == 3

    def test_tags_source_grid(self, tmp_path):
        tracker = Tracker(tmp_path / "research.db")
        planner = GridPlanner(tracker=tracker)
        hypotheses = planner.generate(
            strategies=["A"],
            series_map={"data/kalshi.db": ["KXBTCD"]},
            start_date="2025-01-01",
            end_date="2025-12-31",
        )
        assert hypotheses[0].tags == ["source:grid"]

    def test_empty_strategies(self, tmp_path):
        tracker = Tracker(tmp_path / "research.db")
        planner = GridPlanner(tracker=tracker)
        hypotheses = planner.generate(
            strategies=[],
            series_map={"data/kalshi.db": ["KXBTCD"]},
            start_date="2025-01-01",
            end_date="2025-12-31",
        )
        assert hypotheses == []

    def test_empty_series(self, tmp_path):
        tracker = Tracker(tmp_path / "research.db")
        planner = GridPlanner(tracker=tracker)
        hypotheses = planner.generate(
            strategies=["A"],
            series_map={},
            start_date="2025-01-01",
            end_date="2025-12-31",
        )
        assert hypotheses == []


class TestGridPlannerOrdering:
    def test_warm_leads_first(self, tmp_path):
        """Strategies with prior promote/explore results should come first."""
        tracker = Tracker(tmp_path / "research.db")
        # Strategy B has a promote result on KXBTCD
        r = _grid_result(strategy="B", series="KXBTCD", verdict="promote", verdict_reason="p")
        tracker.save_result(r)

        planner = GridPlanner(tracker=tracker)
        hypotheses = planner.generate(
            strategies=["A", "B"],
            series_map={"data/kalshi.db": ["KXBTCD", "KXETH"]},
            start_date="2025-01-01",
            end_date="2025-12-31",
        )
        # B should appear before A (B has warm results)
        # B × KXBTCD is already tested, so B × KXETH should be first
        assert hypotheses[0].strategy == "B"
        assert hypotheses[0].series == "KXETH"
