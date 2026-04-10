"""Tests for edge_catcher.monitors.discovery."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
import yaml

from edge_catcher.monitors.discovery import (
	discover_strategies,
	get_enabled_strategies,
	load_config,
)
from edge_catcher.monitors.strategy_base import PaperStrategy, Signal


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_MINIMAL_STRATEGY_SRC = textwrap.dedent("""\
	from edge_catcher.monitors.strategy_base import PaperStrategy, Signal

	class MyStrat(PaperStrategy):
		name = "my-strat"
		supported_series = ["SERIES_A"]
		default_params = {}
		def on_tick(self, ctx):
			return []
""")


def _write_strategies_file(tmp_path: Path, src: str) -> Path:
	p = tmp_path / "strategies_local.py"
	p.write_text(src)
	return p


def _make_config(extra: dict | None = None) -> dict:
	cfg = {
		"sizing": {
			"risk_per_trade_cents": 200,
			"max_slippage_cents": 2,
			"min_fill": 3,
		},
		"strategies": {
			"my-strat": {
				"enabled": True,
				"series": ["SERIES_A"],
				"params": {},
			}
		},
	}
	if extra:
		cfg.update(extra)
	return cfg


class _StubStrat(PaperStrategy):
	name = "stub"
	supported_series = ["SERIES_A"]
	default_params = {"foo": 1}

	def on_tick(self, ctx):
		return []


# ---------------------------------------------------------------------------
# load_config
# ---------------------------------------------------------------------------

class TestLoadConfig:
	def test_loads_yaml(self, tmp_path: Path) -> None:
		cfg_file = tmp_path / "paper_trader.yaml"
		cfg_file.write_text(yaml.dump({"sizing": {"default": 5}}))
		result = load_config(cfg_file)
		assert result == {"sizing": {"default": 5}}

	def test_raises_for_missing_file(self, tmp_path: Path) -> None:
		missing = tmp_path / "nonexistent.yaml"
		with pytest.raises(FileNotFoundError):
			load_config(missing)


# ---------------------------------------------------------------------------
# discover_strategies
# ---------------------------------------------------------------------------

class TestDiscoverStrategies:
	def test_discovers_from_file(self, tmp_path: Path) -> None:
		strats_file = _write_strategies_file(tmp_path, _MINIMAL_STRATEGY_SRC)
		result = discover_strategies(strats_file)
		assert len(result) == 1
		assert result[0].name == "my-strat"

	def test_returns_empty_for_missing_file(self, tmp_path: Path) -> None:
		missing = tmp_path / "strategies_local.py"
		result = discover_strategies(missing)
		assert result == []

	def test_filters_non_strategy_classes(self, tmp_path: Path) -> None:
		src = _MINIMAL_STRATEGY_SRC + textwrap.dedent("""\

			class NotAStrategy:
				pass

			class AlsoNot:
				name = "also-not"
				def on_tick(self, ctx):
					return []
		""")
		strats_file = _write_strategies_file(tmp_path, src)
		result = discover_strategies(strats_file)
		# Only MyStrat is a PaperStrategy subclass
		assert len(result) == 1
		assert result[0].name == "my-strat"

	def test_ignores_paper_strategy_itself(self, tmp_path: Path) -> None:
		"""PaperStrategy base class must not be returned."""
		src = textwrap.dedent("""\
			from edge_catcher.monitors.strategy_base import PaperStrategy
		""")
		strats_file = _write_strategies_file(tmp_path, src)
		result = discover_strategies(strats_file)
		assert result == []

	def test_discovers_multiple_strategies(self, tmp_path: Path) -> None:
		src = _MINIMAL_STRATEGY_SRC + textwrap.dedent("""\

			class SecondStrat(PaperStrategy):
				name = "second-strat"
				supported_series = ["SERIES_B"]
				default_params = {}
				def on_tick(self, ctx):
					return []
		""")
		strats_file = _write_strategies_file(tmp_path, src)
		result = discover_strategies(strats_file)
		names = {s.name for s in result}
		assert names == {"my-strat", "second-strat"}


# ---------------------------------------------------------------------------
# get_enabled_strategies
# ---------------------------------------------------------------------------

class TestGetEnabledStrategies:
	def _strat(self, name: str = "stub") -> PaperStrategy:
		s = _StubStrat()
		s.name = name  # type: ignore[assignment]
		return s

	def test_filters_disabled_strategies(self) -> None:
		config = {
			"sizing": {"risk_per_trade_cents": 200, "max_slippage_cents": 2, "min_fill": 3},
			"strategies": {
				"stub": {"enabled": False, "series": ["SERIES_A"]},
			},
		}
		result = get_enabled_strategies(config, [self._strat()])
		assert result == []

	def test_returns_enabled_strategies(self) -> None:
		config = {
			"sizing": {"risk_per_trade_cents": 200, "max_slippage_cents": 2, "min_fill": 3},
			"strategies": {
				"stub": {"enabled": True, "series": ["SERIES_A"]},
			},
		}
		strat = self._strat()
		result = get_enabled_strategies(config, [strat])
		assert len(result) == 1
		assert result[0] is strat

	def test_merges_param_overrides(self) -> None:
		config = {
			"sizing": {"risk_per_trade_cents": 200, "max_slippage_cents": 2, "min_fill": 3},
			"strategies": {
				"stub": {
					"enabled": True,
					"series": ["SERIES_A"],
					"params": {"foo": 99},
				},
			},
		}
		strat = self._strat()
		result = get_enabled_strategies(config, [strat])
		assert result[0].default_params["foo"] == 99

	def test_raises_on_missing_sizing(self) -> None:
		config = {
			# no "sizing" key
			"strategies": {
				"stub": {
					"enabled": True,
					"series": ["SERIES_A"],
				},
			},
		}
		with pytest.raises(ValueError, match="sizing"):
			get_enabled_strategies(config, [self._strat()])

	def test_ignores_strategies_not_in_config(self) -> None:
		"""Strategies present in all_strategies but not in config are skipped."""
		config = {
			"sizing": {"risk_per_trade_cents": 200, "max_slippage_cents": 2, "min_fill": 3},
			"strategies": {},
		}
		result = get_enabled_strategies(config, [self._strat()])
		assert result == []

	def test_raises_on_invalid_sizing_config(self) -> None:
		"""Validates sizing config has required keys."""
		config = {
			"sizing": {"risk_per_trade_cents": 0},  # invalid: must be > 0
			"strategies": {
				"stub": {"enabled": True, "series": ["SERIES_A"]},
			},
		}
		with pytest.raises(ValueError, match="risk_per_trade_cents"):
			get_enabled_strategies(config, [self._strat()])
