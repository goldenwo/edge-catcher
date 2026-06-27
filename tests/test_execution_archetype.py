"""Tests for execution-archetype resolution + fill-fragility classification."""

from edge_catcher.research import execution_archetype as ea


def test_is_fragile_classification():
	assert ea.is_fragile("taker_synthetic") is True
	assert ea.is_fragile("unknown") is True       # conservative default
	assert ea.is_fragile("maker") is False
	assert ea.is_fragile("taker_prints") is False


def test_resolve_reads_declared_attribute(monkeypatch):
	class Maker:
		execution_archetype = "maker"
	monkeypatch.setattr(
		"edge_catcher.cli.backtest.build_strategy_map",
		lambda: ({"m": Maker}, False),
	)
	assert ea.resolve_execution_archetype("m") == "maker"


def test_resolve_defaults_unknown_for_missing_strategy(monkeypatch):
	monkeypatch.setattr(
		"edge_catcher.cli.backtest.build_strategy_map",
		lambda: ({}, False),
	)
	assert ea.resolve_execution_archetype("nope") == "unknown"


def test_resolve_defaults_unknown_for_unannotated_strategy(monkeypatch):
	class Bare:
		pass
	monkeypatch.setattr(
		"edge_catcher.cli.backtest.build_strategy_map",
		lambda: ({"b": Bare}, False),
	)
	assert ea.resolve_execution_archetype("b") == "unknown"


def test_resolve_rejects_invalid_archetype(monkeypatch):
	class Weird:
		execution_archetype = "bogus"
	monkeypatch.setattr(
		"edge_catcher.cli.backtest.build_strategy_map",
		lambda: ({"w": Weird}, False),
	)
	assert ea.resolve_execution_archetype("w") == "unknown"
