# tests/test_run_queue.py
"""Tests for edge_catcher.research.run_queue module."""

from __future__ import annotations

import json
import time
from unittest.mock import MagicMock, patch

import pytest

from edge_catcher.research.audit import AuditLog
from edge_catcher.research.data_source_config import make_ds
from edge_catcher.research.hypothesis import Hypothesis, HypothesisResult
from edge_catcher.research.run_queue import RunQueue
from edge_catcher.research.tracker import Tracker


def _make_hypothesis(strategy="C", series="SERIES_A", **kwargs) -> Hypothesis:
    kwargs.pop("db_path", None)  # no longer a field
    defaults = dict(
        data_sources=make_ds(db="kalshi.db", series=series),
        start_date="2025-01-01",
        end_date="2025-12-31",
        tags=["source:grid"],
    )
    defaults.update(kwargs)
    return Hypothesis(strategy=strategy, **defaults)


def _make_mock_result(h: Hypothesis, verdict="promote") -> HypothesisResult:
    return HypothesisResult(
        hypothesis=h, status="ok", total_trades=100, wins=90, losses=10,
        win_rate=0.9, net_pnl_cents=500.0, sharpe=2.5, max_drawdown_pct=5.0,
        fees_paid_cents=100.0, avg_win_cents=10.0, avg_loss_cents=-5.0,
        per_strategy={}, verdict=verdict, verdict_reason="test", raw_json={},
    )


class TestRunQueueSubmit:
    def test_submit_sequential(self, tmp_path):
        tracker = Tracker(tmp_path / "research.db")
        audit = AuditLog(tmp_path / "research.db")
        agent = MagicMock()

        h1 = _make_hypothesis(strategy="A")
        h2 = _make_hypothesis(strategy="B", series="SERIES_E")
        agent.run_hypothesis.side_effect = [
            _make_mock_result(h1, "promote"),
            _make_mock_result(h2, "kill"),
        ]

        queue = RunQueue(agent=agent, audit=audit, parallel=1)
        results = queue.submit([h1, h2], phase="grid")

        assert len(results) == 2
        assert agent.run_hypothesis.call_count == 2
        # Verify audit log recorded executions
        execs = audit.list_executions()
        assert len(execs) == 2

    def test_submit_empty_list(self, tmp_path):
        audit = AuditLog(tmp_path / "research.db")
        agent = MagicMock()
        queue = RunQueue(agent=agent, audit=audit)
        results = queue.submit([], phase="grid")
        assert results == []
        agent.run_hypothesis.assert_not_called()

    def test_submit_records_queue_position(self, tmp_path):
        audit = AuditLog(tmp_path / "research.db")
        agent = MagicMock()

        hypotheses = [_make_hypothesis(series=f"SER{i}") for i in range(3)]
        agent.run_hypothesis.side_effect = [
            _make_mock_result(h, "kill") for h in hypotheses
        ]

        queue = RunQueue(agent=agent, audit=audit)
        queue.submit(hypotheses, phase="grid")

        execs = audit.list_executions()
        positions = sorted([e["queue_position"] for e in execs])
        assert positions == [0, 1, 2]

    def test_submit_respects_max_time(self, tmp_path):
        audit = AuditLog(tmp_path / "research.db")
        agent = MagicMock()

        hypotheses = [_make_hypothesis(series=f"SER{i}") for i in range(100)]

        def slow_run(h):
            time.sleep(0.05)
            return _make_mock_result(h, "kill")

        agent.run_hypothesis.side_effect = slow_run

        # 0.1 second timeout — should run only a handful
        queue = RunQueue(agent=agent, audit=audit)
        results = queue.submit(hypotheses, phase="grid", max_time_seconds=0.1)

        assert len(results) < len(hypotheses)
        assert len(results) > 0

    def test_submit_handles_error_results(self, tmp_path):
        audit = AuditLog(tmp_path / "research.db")
        agent = MagicMock()

        h = _make_hypothesis()
        error_result = HypothesisResult.error(h, "db not found")
        agent.run_hypothesis.return_value = error_result

        queue = RunQueue(agent=agent, audit=audit)
        results = queue.submit([h], phase="grid")

        assert len(results) == 1
        assert results[0].status == "error"
        execs = audit.list_executions()
        assert execs[0]["status"] == "error"
