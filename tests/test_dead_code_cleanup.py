"""Tests for dead code cleanup in strategies_local.py."""

import pytest
from pathlib import Path

from edge_catcher.runner.strategy_parser import cleanup_dead_strategies


SAMPLE_FILE = '''"""Auto-generated strategies."""

from edge_catcher.runner.strategies import Strategy


class AliveStrat(Strategy):
	name = "alive_strat"
	def on_market(self, market):
		return None


class DeadStrat(Strategy):
	name = "dead_strat"
	def on_market(self, market):
		return "yes"


class AlsoAlive(Strategy):
	name = "also_alive"
	def on_market(self, market):
		return "no"
'''


class TestCleanupDeadStrategies:
	def test_removes_dead_class(self, tmp_path):
		file_path = tmp_path / "strategies_local.py"
		file_path.write_text(SAMPLE_FILE)

		removed = cleanup_dead_strategies(file_path, dead_names=["DeadStrat"])
		assert removed == ["DeadStrat"]

		content = file_path.read_text()
		assert "class DeadStrat" not in content
		assert "class AliveStrat" in content
		assert "class AlsoAlive" in content

	def test_removes_multiple(self, tmp_path):
		file_path = tmp_path / "strategies_local.py"
		file_path.write_text(SAMPLE_FILE)

		removed = cleanup_dead_strategies(file_path, dead_names=["DeadStrat", "AlsoAlive"])
		assert set(removed) == {"DeadStrat", "AlsoAlive"}

		content = file_path.read_text()
		assert "class AliveStrat" in content
		assert "class DeadStrat" not in content
		assert "class AlsoAlive" not in content

	def test_no_matches_does_nothing(self, tmp_path):
		file_path = tmp_path / "strategies_local.py"
		file_path.write_text(SAMPLE_FILE)

		removed = cleanup_dead_strategies(file_path, dead_names=["NonExistent"])
		assert removed == []
		assert file_path.read_text() == SAMPLE_FILE

	def test_file_not_exists_returns_empty(self, tmp_path):
		file_path = tmp_path / "strategies_local.py"
		removed = cleanup_dead_strategies(file_path, dead_names=["Anything"])
		assert removed == []

	def test_empty_dead_names(self, tmp_path):
		file_path = tmp_path / "strategies_local.py"
		file_path.write_text(SAMPLE_FILE)
		removed = cleanup_dead_strategies(file_path, dead_names=[])
		assert removed == []
