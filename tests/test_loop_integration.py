# tests/test_loop_integration.py
"""Integration test: full loop with mocked backtests and LLM."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from edge_catcher.research.loop import LoopOrchestrator
from edge_catcher.research.tracker import Tracker
from edge_catcher.research.audit import AuditLog


def _mock_backtest_json(**overrides) -> str:
	data = {
		"status": "ok", "total_trades": 100, "wins": 90, "losses": 10,
		"win_rate": 0.90, "net_pnl_cents": 500.0, "sharpe": 2.5,
		"max_drawdown_pct": 5.0, "total_fees_paid": 100.0,
		"avg_win_cents": 10.0, "avg_loss_cents": -5.0, "per_strategy": {},
	}
	data.update(overrides)
	return json.dumps(data)


class TestLoopIntegration:
	def test_grid_only_full_cycle(self, tmp_path):
		"""Grid-only loop: discovers strategies, runs backtests, produces audit trail."""
		db_path = str(tmp_path / "research.db")

		orch = LoopOrchestrator(
			research_db=db_path,
			start_date="2025-01-01",
			end_date="2025-12-31",
			max_runs=5,
			grid_only=True,
		)

		mock_proc = MagicMock()
		mock_proc.stdout = _mock_backtest_json()
		mock_proc.stderr = ""

		with patch.object(orch, "_discover_strategies", return_value=["A", "B"]), \
			 patch.object(orch, "_discover_series", return_value={
				 "data/kalshi.db": ["KXBTCD", "KXETH"]
			 }), \
			 patch("edge_catcher.research.agent.subprocess.run", return_value=mock_proc):

			exit_code, results = orch.run()

		# Should have run backtests (up to budget of 5)
		assert len(results) > 0
		assert len(results) <= 5

		# Tracker should have results
		tracker = Tracker(db_path)
		assert len(tracker.list_results()) == len(results)

		# Audit log should have executions and integrity checks
		audit = AuditLog(db_path)
		assert len(audit.list_executions()) == len(results)
		checks = audit.list_integrity_checks()
		checkpoints = [c["checkpoint"] for c in checks]
		assert "loop_start" in checkpoints
		assert "post_grid" in checkpoints
		assert "loop_end" in checkpoints

	def test_resume_skips_completed(self, tmp_path):
		"""Second run should skip already-tested hypotheses."""
		db_path = str(tmp_path / "research.db")

		mock_proc = MagicMock()
		mock_proc.stdout = _mock_backtest_json()
		mock_proc.stderr = ""

		def run_loop():
			orch = LoopOrchestrator(
				research_db=db_path,
				start_date="2025-01-01",
				end_date="2025-12-31",
				max_runs=2,
				grid_only=True,
			)
			with patch.object(orch, "_discover_strategies", return_value=["A"]), \
				 patch.object(orch, "_discover_series", return_value={
					 "data/kalshi.db": ["S1", "S2", "S3", "S4"]
				 }), \
				 patch("edge_catcher.research.agent.subprocess.run", return_value=mock_proc):
				return orch.run()

		# First run: budget=2, should run 2 of 4
		exit_code1, results1 = run_loop()
		assert len(results1) == 2
		assert exit_code1 == 2  # partial

		# Second run: should skip the 2 already done, run 2 more
		exit_code2, results2 = run_loop()
		assert len(results2) == 2

		# Total in tracker should be 4
		tracker = Tracker(db_path)
		assert len(tracker.list_results()) == 4
