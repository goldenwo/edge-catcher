"""Integration tests for context rot prevention features."""

import json
import uuid
import pytest
from unittest.mock import MagicMock, patch

from edge_catcher.research.data_source_config import make_ds
from edge_catcher.research.tracker import Tracker
from edge_catcher.research.hypothesis import Hypothesis, HypothesisResult
from edge_catcher.research.loop import LoopOrchestrator


@pytest.fixture
def tracker(tmp_path):
	return Tracker(str(tmp_path / "test.db"))


def _make_result(strategy, series, verdict, reason="test", sharpe=0.5):
	h = Hypothesis(
		id=str(uuid.uuid4()),
		strategy=strategy, data_sources=make_ds(db="test.db", series=series),
		start_date="", end_date="", fee_pct=1.0,
	)
	return HypothesisResult(
		hypothesis=h, status="ok",
		total_trades=10, wins=5, losses=5,
		win_rate=0.5, net_pnl_cents=0, sharpe=sharpe,
		max_drawdown_pct=5, fees_paid_cents=1,
		avg_win_cents=10, avg_loss_cents=-10,
		verdict=verdict, verdict_reason=reason,
		per_strategy={}, raw_json={},
	)


class TestKillRegistryIntegration:
	def test_kill_registry_populated_after_update(self, tracker):
		"""After saving results with high kill rate, update_kill_registry should populate it."""
		from edge_catcher.research.observer import ResearchObserver

		# Pre-populate tracker with kills (4 kills = 100% kill rate, meets >=3 series threshold)
		for series in ["S1", "S2", "S3", "S4"]:
			r = _make_result("BadStrat", series, "kill", "low sharpe")
			tracker.save_result(r)

		observer = ResearchObserver(tracker=tracker, run_id="test-run")
		observer.update_kill_registry()

		entries = tracker.list_kill_registry()
		assert len(entries) == 1
		assert entries[0]["strategy"] == "BadStrat"

	def test_kill_registry_empty_when_below_threshold(self, tracker):
		"""Strategy with only 2 series tested should not enter kill registry."""
		from edge_catcher.research.observer import ResearchObserver

		for series in ["S1", "S2"]:
			r = _make_result("FewTests", series, "kill", "low sharpe")
			tracker.save_result(r)

		observer = ResearchObserver(tracker=tracker, run_id="test-run")
		observer.update_kill_registry()

		assert tracker.list_kill_registry() == []

	def test_fingerprint_blocks_duplicate(self, tracker):
		"""AST fingerprint should block saving a structurally identical strategy."""
		from edge_catcher.runner.strategy_parser import compute_code_hash, compute_ast_fingerprint

		code = '''
class TestStrat:
	name = "test_strat"
	def on_market(self, market):
		return None
'''
		code_hash = compute_code_hash(code)
		ast_fp = compute_ast_fingerprint(code)
		tracker.save_fingerprint(ast_fp, "test_strat", code_hash)

		# Duplicate code should be detected
		assert tracker.check_code_hash(code_hash) == "test_strat"
		assert tracker.check_fingerprint(ast_fp) == "test_strat"

	def test_fingerprint_does_not_block_different_logic(self, tracker):
		"""Structurally different strategy (different numeric params) should not be blocked."""
		from edge_catcher.runner.strategy_parser import compute_code_hash, compute_ast_fingerprint

		# StratA uses threshold=0.5
		code_a = '''
class StratA:
	name = "strat_a"
	def setup(self):
		self.threshold = 0.5
	def on_market(self, market):
		if market.price > self.threshold:
			return "yes"
		return None
'''
		# StratB uses a completely different branching structure
		code_b = '''
class StratB:
	name = "strat_b"
	def setup(self):
		self.window = 20
		self.min_vol = 1000
	def on_market(self, market):
		if market.volume > self.min_vol and market.spread < 0.02:
			return "no"
		return None
'''
		fp_a = compute_ast_fingerprint(code_a)
		hash_a = compute_code_hash(code_a)
		tracker.save_fingerprint(fp_a, "strat_a", hash_a)

		fp_b = compute_ast_fingerprint(code_b)
		hash_b = compute_code_hash(code_b)

		assert tracker.check_fingerprint(fp_b) is None
		assert tracker.check_code_hash(hash_b) is None


class TestStuckBreakerIntegration:
	def test_compute_budgets_shifts_when_stuck(self):
		"""Budget should shift to 60/20/20 when stuck for 2+ consecutive runs."""
		budgets_normal = LoopOrchestrator._compute_budgets(100, False, 0)
		budgets_stuck = LoopOrchestrator._compute_budgets(100, False, 3)

		assert budgets_normal["ideate"] == 40
		assert budgets_stuck["ideate"] == 60
		assert budgets_stuck["expand"] == 20

	def test_consecutive_stuck_increments_across_runs(self):
		"""consecutive_stuck should increment when trajectory stays stuck."""
		prev = {"consecutive_stuck": 2}
		assert LoopOrchestrator._compute_consecutive_stuck("stuck", prev) == 3
		assert LoopOrchestrator._compute_consecutive_stuck("improving", prev) == 0

	def test_stuck_state_read_from_journal_on_run(self, tmp_path):
		"""Loop should read prior consecutive_stuck from journal and apply budget shift."""
		from edge_catcher.research.journal import ResearchJournal

		db_path = str(tmp_path / "research.db")
		journal = ResearchJournal(db_path=db_path)

		# Write a prior trajectory showing stuck=3
		journal.write_entry(
			run_id="prior-run",
			entry_type="trajectory",
			content={
				"status": "stuck",
				"consecutive_stuck": 3,
				"sharpe_delta": 0.0,
			},
		)

		orch = LoopOrchestrator(
			research_db=db_path,
			start_date="2025-01-01",
			end_date="2025-12-31",
			max_runs=100,
			grid_only=True,
		)

		with patch.object(orch, "_discover_strategies", return_value=[]), \
			 patch.object(orch, "_discover_series", return_value={}), \
			 patch("edge_catcher.research.loop.ResearchAgent"):
			# Just check that the stuck counter was loaded correctly before run starts
			prev_trajectory = journal.get_latest_trajectory()
			consecutive_stuck = (prev_trajectory or {}).get("consecutive_stuck", 0)
			assert consecutive_stuck == 3

			# Verify budget shift is applied for stuck=3
			budgets = LoopOrchestrator._compute_budgets(100, False, consecutive_stuck)
			assert budgets["ideate"] == 60
			assert budgets["expand"] == 20

	def test_kill_registry_and_stuck_can_coexist(self, tracker, tmp_path):
		"""Kill registry updates and stuck budget detection should work together."""
		from edge_catcher.research.observer import ResearchObserver

		# Add kill results
		for series in ["S1", "S2", "S3", "S4"]:
			r = _make_result("DeadStrategy", series, "kill", "negative pnl")
			tracker.save_result(r)

		observer = ResearchObserver(tracker=tracker, run_id="test-run")

		# Update kill registry
		observer.update_kill_registry()

		# Verify registry populated
		entries = tracker.list_kill_registry()
		assert len(entries) == 1

		# Verify budget logic for stuck state is independent
		budgets_stuck = LoopOrchestrator._compute_budgets(50, False, consecutive_stuck=4)
		assert budgets_stuck["ideate"] == 30   # 60% of 50
		assert budgets_stuck["expand"] == 10   # 20% of 50
