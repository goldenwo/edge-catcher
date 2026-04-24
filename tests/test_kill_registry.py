"""Tests for the persistent kill registry in Tracker."""


import pytest

from edge_catcher.research.tracker import Tracker


@pytest.fixture
def tracker(tmp_path):
	db_path = str(tmp_path / "test_research.db")
	return Tracker(db_path)


class TestKillRegistry:
	def test_upsert_and_list(self, tracker):
		tracker.upsert_kill_registry(
			strategy="BadStrat",
			kill_count=8,
			series_tested=10,
			kill_rate=0.8,
			reason_summary='["low sharpe: 5x", "negative pnl: 3x"]',
		)
		entries = tracker.list_kill_registry()
		assert len(entries) == 1
		assert entries[0]["strategy"] == "BadStrat"
		assert entries[0]["kill_rate"] == 0.8
		assert entries[0]["permanent"] == 1  # SQLite stores bool as int

	def test_upsert_updates_existing(self, tracker):
		tracker.upsert_kill_registry("BadStrat", 8, 10, 0.8, "[]")
		tracker.upsert_kill_registry("BadStrat", 12, 14, 0.857, '["new reason"]')
		entries = tracker.list_kill_registry()
		assert len(entries) == 1
		assert entries[0]["kill_count"] == 12
		assert entries[0]["reason_summary"] == '["new reason"]'

	def test_reset_sets_permanent_false(self, tracker):
		tracker.upsert_kill_registry("BadStrat", 8, 10, 0.8, "[]")
		tracker.reset_kill_registry("BadStrat")
		entries = tracker.list_kill_registry()
		assert entries[0]["permanent"] == 0

	def test_re_kill_after_reset(self, tracker):
		"""A reset strategy that gets killed again becomes permanent again."""
		tracker.upsert_kill_registry("BadStrat", 8, 10, 0.8, "[]")
		tracker.reset_kill_registry("BadStrat")
		# Re-killed: upsert should set permanent back to TRUE
		tracker.upsert_kill_registry("BadStrat", 12, 14, 0.857, "[]")
		entries = tracker.list_kill_registry()
		assert entries[0]["permanent"] == 1

	def test_list_permanent_only(self, tracker):
		tracker.upsert_kill_registry("Dead1", 8, 10, 0.8, "[]")
		tracker.upsert_kill_registry("Dead2", 9, 10, 0.9, "[]")
		tracker.reset_kill_registry("Dead1")
		permanent = tracker.list_kill_registry(permanent_only=True)
		assert len(permanent) == 1
		assert permanent[0]["strategy"] == "Dead2"

	def test_empty_registry(self, tracker):
		assert tracker.list_kill_registry() == []


class TestKillRegistryUpdate:
	"""Test the observer's update_kill_registry logic."""

	def test_strategy_with_high_kill_rate_enters_registry(self, tracker):
		"""Strategy killed on 4/5 series (80%) should enter the registry."""
		from edge_catcher.research.observer import ResearchObserver
		observer = ResearchObserver(tracker=tracker, run_id="test-run")

		# Simulate: strategy killed on 4 series, explore on 1
		# But has NO promote/review verdicts
		_save_result(tracker, "KillMe", "S1", "kill", "low sharpe")
		_save_result(tracker, "KillMe", "S2", "kill", "low sharpe")
		_save_result(tracker, "KillMe", "S3", "kill", "negative pnl")
		_save_result(tracker, "KillMe", "S4", "kill", "low sharpe")
		_save_result(tracker, "KillMe", "S5", "explore", "borderline")

		observer.update_kill_registry()

		entries = tracker.list_kill_registry()
		assert len(entries) == 1
		assert entries[0]["strategy"] == "KillMe"
		assert entries[0]["kill_rate"] == 0.8

	def test_strategy_with_promote_excluded(self, tracker):
		"""Strategy with any promote verdict should NOT enter the registry."""
		from edge_catcher.research.observer import ResearchObserver
		observer = ResearchObserver(tracker=tracker, run_id="test-run")

		_save_result(tracker, "MixedStrat", "S1", "kill", "low sharpe")
		_save_result(tracker, "MixedStrat", "S2", "kill", "low sharpe")
		_save_result(tracker, "MixedStrat", "S3", "kill", "low sharpe")
		_save_result(tracker, "MixedStrat", "S4", "promote", "good")

		observer.update_kill_registry()
		assert tracker.list_kill_registry() == []

	def test_strategy_below_threshold_excluded(self, tracker):
		"""Strategy with <3 series tested should not enter."""
		from edge_catcher.research.observer import ResearchObserver
		observer = ResearchObserver(tracker=tracker, run_id="test-run")

		_save_result(tracker, "FewTests", "S1", "kill", "low sharpe")
		_save_result(tracker, "FewTests", "S2", "kill", "low sharpe")

		observer.update_kill_registry()
		assert tracker.list_kill_registry() == []


class TestKillRegistryPrompt:
	def test_kill_registry_block_replaces_top10(self, tracker):
		"""Kill registry entries should appear in the ideation prompt."""
		from edge_catcher.research.llm_ideator import LLMIdeator
		from unittest.mock import MagicMock

		# Add entries to kill registry
		for i in range(15):
			tracker.upsert_kill_registry(
				f"Dead{i}", kill_count=10+i, series_tested=12+i,
				kill_rate=0.8+i*0.01, reason_summary=f'["reason {i}"]',
			)

		ideator = LLMIdeator(
			tracker=tracker, audit=MagicMock(), client=MagicMock(),
		)
		prompt = ideator.build_ideation_prompt(
			available_strategies=["StratA"],
			series_map={"data/test.db": ["SER1"]},
		)

		# All 15 should appear (under cap of 50)
		assert "Dead14" in prompt
		assert "Dead0" in prompt
		# Should use "Kill Registry" heading, not old "Kill Patterns"
		assert "Kill Registry" in prompt

	def test_kill_registry_cap_at_50(self, tracker):
		"""Registry block caps at 50 entries."""
		from edge_catcher.research.llm_ideator import LLMIdeator
		from unittest.mock import MagicMock

		for i in range(60):
			tracker.upsert_kill_registry(
				f"Dead{i:03d}", kill_count=10, series_tested=12,
				kill_rate=0.83, reason_summary='["reason"]',
			)

		ideator = LLMIdeator(
			tracker=tracker, audit=MagicMock(), client=MagicMock(),
		)
		prompt = ideator.build_ideation_prompt(
			available_strategies=["StratA"],
			series_map={"data/test.db": ["SER1"]},
		)

		assert "... and 10 more killed strategies" in prompt


class TestKillRegistryCLI:
	def test_list_subcommand(self, tracker, tmp_path, capsys):
		tracker.upsert_kill_registry("Dead1", 8, 10, 0.8, '["low sharpe"]')
		from edge_catcher.cli.research import run as _cmd_research
		from types import SimpleNamespace
		args = SimpleNamespace(
			research_db=str(tmp_path / "test_research.db"),
			research_command="kill-registry",
			kill_registry_action="list",
			force=False,
		)
		_cmd_research(args)
		captured = capsys.readouterr()
		assert "Dead1" in captured.out

	def test_reset_subcommand(self, tracker, tmp_path):
		tracker.upsert_kill_registry("Dead1", 8, 10, 0.8, '["low sharpe"]')
		from edge_catcher.cli.research import run as _cmd_research
		from types import SimpleNamespace
		args = SimpleNamespace(
			research_db=str(tmp_path / "test_research.db"),
			research_command="kill-registry",
			kill_registry_action="reset",
			kill_registry_strategy="Dead1",
			force=False,
		)
		_cmd_research(args)
		entries = tracker.list_kill_registry()
		assert entries[0]["permanent"] == 0


def _save_result(tracker, strategy, series, verdict, reason):
	"""Helper: save a hypothesis + result pair via HypothesisResult."""
	import uuid
	from edge_catcher.research.hypothesis import Hypothesis, HypothesisResult

	from edge_catcher.research.data_source_config import make_ds
	h = Hypothesis(
		id=str(uuid.uuid4()),
		strategy=strategy,
		data_sources=make_ds(db="test.db", series=series),
		start_date="",
		end_date="",
		fee_pct=1.0,
	)
	result = HypothesisResult(
		hypothesis=h, status="ok",
		total_trades=10, wins=3, losses=7,
		win_rate=0.3, net_pnl_cents=-50, sharpe=-0.5,
		max_drawdown_pct=10, fees_paid_cents=5,
		avg_win_cents=10, avg_loss_cents=-10,
		per_strategy={}, raw_json={},
		verdict=verdict, verdict_reason=reason,
	)
	tracker.save_result(result)
