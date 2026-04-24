"""Tests for the stuck circuit breaker."""



class TestConsecutiveStuckCounter:
	def test_stuck_increments_counter(self):
		from edge_catcher.research.loop import LoopOrchestrator
		# Previous trajectory had consecutive_stuck=1, current is stuck
		prev = {"consecutive_stuck": 1}
		assert LoopOrchestrator._compute_consecutive_stuck("stuck", prev) == 2

	def test_plateauing_increments_counter(self):
		from edge_catcher.research.loop import LoopOrchestrator
		prev = {"consecutive_stuck": 0}
		assert LoopOrchestrator._compute_consecutive_stuck("plateauing", prev) == 1

	def test_improving_resets_counter(self):
		from edge_catcher.research.loop import LoopOrchestrator
		prev = {"consecutive_stuck": 5}
		assert LoopOrchestrator._compute_consecutive_stuck("improving", prev) == 0

	def test_no_previous_trajectory(self):
		from edge_catcher.research.loop import LoopOrchestrator
		assert LoopOrchestrator._compute_consecutive_stuck("stuck", None) == 1

	def test_previous_without_counter(self):
		from edge_catcher.research.loop import LoopOrchestrator
		prev = {}  # old trajectory without the field
		assert LoopOrchestrator._compute_consecutive_stuck("stuck", prev) == 1


class TestBudgetShift:
	def test_shift_at_2_consecutive(self):
		from edge_catcher.research.loop import LoopOrchestrator
		budgets = LoopOrchestrator._compute_budgets(
			max_runs=100, grid_only=False, consecutive_stuck=2,
		)
		assert budgets["ideate"] == 60  # 60% instead of 40%
		assert budgets["expand"] == 20  # 20% instead of 40%

	def test_no_shift_at_1_consecutive(self):
		from edge_catcher.research.loop import LoopOrchestrator
		budgets = LoopOrchestrator._compute_budgets(
			max_runs=100, grid_only=False, consecutive_stuck=1,
		)
		assert budgets["ideate"] == 40  # normal 40%
		assert budgets["expand"] == 40  # normal 40%

	def test_grid_only_unaffected(self):
		from edge_catcher.research.loop import LoopOrchestrator
		budgets = LoopOrchestrator._compute_budgets(
			max_runs=100, grid_only=True, consecutive_stuck=5,
		)
		assert budgets["ideate"] == 0
		assert budgets["expand"] == 100
