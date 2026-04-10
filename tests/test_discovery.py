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
	resolve_sizing,
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
		"sizing": {"default": 10},
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
# resolve_sizing
# ---------------------------------------------------------------------------

class TestResolveSizing:
	def test_strategy_specific_sizing(self) -> None:
		config = {
			"sizing": {"default": 5},
			"strategies": {
				"my-strat": {"sizing": {"SERIES_A": 20}},
			},
		}
		assert resolve_sizing(config, "my-strat", "SERIES_A") == 20

	def test_falls_back_to_default(self) -> None:
		config = {
			"sizing": {"default": 7},
			"strategies": {"my-strat": {}},
		}
		assert resolve_sizing(config, "my-strat", "SERIES_A") == 7

	def test_sizing_dict_with_base_key(self) -> None:
		config = {
			"sizing": {"default": {"base": 15}},
			"strategies": {"my-strat": {}},
		}
		assert resolve_sizing(config, "my-strat", "SERIES_A") == 15

	def test_sizing_dict_with_default_key(self) -> None:
		config = {
			"sizing": {"default": {"default": 12}},
			"strategies": {"my-strat": {}},
		}
		assert resolve_sizing(config, "my-strat", "SERIES_A") == 12

	def test_raises_when_no_sizing(self) -> None:
		config: dict = {"strategies": {}}
		with pytest.raises(ValueError, match="sizing"):
			resolve_sizing(config, "my-strat", "SERIES_A")

	def test_strategy_specific_dict_sizing(self) -> None:
		config = {
			"strategies": {
				"my-strat": {"sizing": {"SERIES_A": {"base": 8}}},
			},
		}
		assert resolve_sizing(config, "my-strat", "SERIES_A") == 8


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
			"sizing": {"default": 5},
			"strategies": {
				"stub": {"enabled": False, "series": ["SERIES_A"]},
			},
		}
		result = get_enabled_strategies(config, [self._strat()])
		assert result == []

	def test_returns_enabled_strategies(self) -> None:
		config = {
			"sizing": {"default": 5},
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
			"sizing": {"default": 5},
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
			"sizing": {"default": 5},
			"strategies": {},
		}
		result = get_enabled_strategies(config, [self._strat()])
		assert result == []

	def test_raises_on_missing_sizing_for_specific_series(self) -> None:
		"""Validates sizing for each series listed in config."""
		config = {
			# no top-level sizing.default
			"strategies": {
				"stub": {
					"enabled": True,
					"series": ["SERIES_A"],
					"sizing": {},  # empty — no series-specific either
				},
			},
		}
		with pytest.raises(ValueError, match="sizing"):
			get_enabled_strategies(config, [self._strat()])
