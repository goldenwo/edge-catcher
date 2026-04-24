"""Tests for journal bias mitigation: near-miss observations and gate margins."""

import json
from unittest.mock import MagicMock, patch

from edge_catcher.research.data_source_config import make_ds
from edge_catcher.research.hypothesis import Hypothesis, HypothesisResult


def _make_result(strategy, series, verdict, sharpe, validation_details=None):
	"""Helper to build a HypothesisResult."""
	h = Hypothesis(
		strategy=strategy, data_sources=make_ds(db="test.db", series=series),
		start_date="", end_date="", fee_pct=1.0,
	)
	r = HypothesisResult(
		hypothesis=h, status="ok", total_trades=50, wins=30, losses=20,
		win_rate=0.6, net_pnl_cents=100, sharpe=sharpe,
		max_drawdown_pct=5, fees_paid_cents=10,
		avg_win_cents=10, avg_loss_cents=-5,
		verdict=verdict, verdict_reason="test",
		per_strategy={}, raw_json={},
	)
	return r


class TestNearMissObservations:
	def test_near_miss_written_for_highest_sharpe_kill(self):
		"""Should write a near-miss observation for the highest-Sharpe killed strategy."""
		from edge_catcher.research.observer import ResearchObserver

		tracker = MagicMock()
		tracker.get_result_by_id.return_value = {
			"validation_details": json.dumps([
				{"gate": "monte_carlo", "passed": False, "details": {"p_value": 0.12}},
			]),
		}
		observer = ResearchObserver(tracker=tracker, run_id="test-run")

		journal = MagicMock()
		results = [
			_make_result("GoodStrat", "S1", "promote", 1.5),
			_make_result("AlmostStrat", "S2", "kill", 1.2),
			_make_result("BadStrat", "S3", "kill", -0.5),
		]

		observer.write_phase_outcomes(journal, results, "ideate")

		# Check that a near-miss observation was written
		calls = journal.write_entry.call_args_list
		near_miss_calls = [
			c for c in calls
			if c[0][1] == "observation" and "NEAR-MISS" in c[0][2].get("pattern", "")
		]
		assert len(near_miss_calls) >= 1
		assert "AlmostStrat" in near_miss_calls[0][0][2]["pattern"]

	def test_no_near_miss_when_no_kills(self):
		"""Should not write near-miss if there are no kills."""
		from edge_catcher.research.observer import ResearchObserver

		tracker = MagicMock()
		observer = ResearchObserver(tracker=tracker, run_id="test-run")
		journal = MagicMock()
		results = [_make_result("GoodStrat", "S1", "promote", 1.5)]

		observer.write_phase_outcomes(journal, results, "ideate")

		calls = journal.write_entry.call_args_list
		near_miss_calls = [
			c for c in calls
			if c[0][1] == "observation" and "NEAR-MISS" in c[0][2].get("pattern", "")
		]
		assert len(near_miss_calls) == 0


class TestGateMarginAnnotations:
	def test_promote_observation_includes_gate_margins(self):
		"""Promoted strategy observations should include gate pass details."""
		from edge_catcher.research.observer import ResearchObserver

		tracker = MagicMock()
		tracker.get_result_by_id.return_value = {
			"validation_details": json.dumps([
				{"gate": "monte_carlo", "passed": True, "details": {"p_value": 0.02}},
				{"gate": "deflated_sharpe", "passed": True, "details": {"dsr_margin": 0.3}},
				{"gate": "temporal_consistency", "passed": True, "details": {"profitable_windows": 4, "total_windows": 5}},  # noqa: E501
				{"gate": "param_sensitivity", "passed": True, "details": {"neighbors_passing": 3, "neighbors_tested": 4}},  # noqa: E501
			]),
		}
		observer = ResearchObserver(tracker=tracker, run_id="test-run")

		journal = MagicMock()
		results = [_make_result("GoodStrat", "S1", "promote", 1.5)]

		# promote observations are in write_journal_summary
		from edge_catcher.research.journal import ResearchJournal
		with patch.object(ResearchJournal, 'classify_trajectory', return_value='improving'):
			observer.write_journal_summary(journal, results, prev_content=None)

		calls = journal.write_entry.call_args_list
		# Find the promote observation
		promote_obs = [
			c for c in calls
			if c[0][1] == "observation" and "PROMOTED" in c[0][2].get("pattern", "")
		]
		assert len(promote_obs) >= 1
		pattern = promote_obs[0][0][2]["pattern"]
		# Should contain gate margin details
		assert "mc_p=" in pattern or "dsr=" in pattern or "temporal=" in pattern
