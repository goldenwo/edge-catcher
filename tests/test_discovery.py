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

	def test_rejects_unsupported_series_by_default(self) -> None:
		"""If a strategy declares supported_series, config must use only those.

		Prevents the strategy_b-on-KXDOGED class of bug where a strategy is
		enabled on a series it was never validated on.
		"""
		config = {
			"sizing": {"risk_per_trade_cents": 200, "max_slippage_cents": 2, "min_fill": 3},
			"strategies": {
				"stub": {"enabled": True, "series": ["SERIES_A", "UNSUPPORTED_SERIES"]},
			},
		}
		with pytest.raises(ValueError, match="UNSUPPORTED_SERIES"):
			get_enabled_strategies(config, [self._strat()])

	def test_allows_all_supported_series(self) -> None:
		"""All requested series are in supported_series → no error."""
		class MultiSeries(PaperStrategy):
			name = "multi"
			supported_series = ["SERIES_A", "SERIES_B", "SERIES_C"]
			default_params = {}
			def on_tick(self, ctx):
				return []

		config = {
			"sizing": {"risk_per_trade_cents": 200, "max_slippage_cents": 2, "min_fill": 3},
			"strategies": {
				"multi": {"enabled": True, "series": ["SERIES_A", "SERIES_B"]},
			},
		}
		result = get_enabled_strategies(config, [MultiSeries()])
		assert len(result) == 1

	def test_empty_supported_series_means_no_restriction(self) -> None:
		"""A strategy with supported_series=[] has no allow-list, so any
		config series is permitted. This is the opt-out for strategies
		that genuinely work on any series (e.g. framework test fixtures).
		"""
		class AnySeries(PaperStrategy):
			name = "any"
			supported_series: list[str] = []
			default_params = {}
			def on_tick(self, ctx):
				return []

		config = {
			"sizing": {"risk_per_trade_cents": 200, "max_slippage_cents": 2, "min_fill": 3},
			"strategies": {
				"any": {"enabled": True, "series": ["WHATEVER"]},
			},
		}
		result = get_enabled_strategies(config, [AnySeries()])
		assert len(result) == 1

	def test_supported_series_validation_can_be_disabled(self) -> None:
		"""``strict_series_validation: false`` turns the error into a warning."""
		config = {
			"sizing": {"risk_per_trade_cents": 200, "max_slippage_cents": 2, "min_fill": 3},
			"strict_series_validation": False,
			"strategies": {
				"stub": {"enabled": True, "series": ["UNSUPPORTED_SERIES"]},
			},
		}
		# Should NOT raise; strategy loads with warning
		result = get_enabled_strategies(config, [self._strat()])
		assert len(result) == 1


# ---------------------------------------------------------------------------
# Manifest-based supported_series loading
# ---------------------------------------------------------------------------

def _write_manifest(tmp_path: Path, strategies: dict) -> Path:
	import json
	p = tmp_path / "supported_series_manifest.json"
	p.write_text(json.dumps({
		"generated_at": "2026-04-12T00:00:00Z",
		"source": "data/research.db",
		"strategies": strategies,
	}))
	return p


class TestManifestSupportedSeries:
	"""Manifest merges into each strategy's effective supported_series."""

	def _base_cfg(self, manifest_path: Path, series: list[str]) -> dict:
		return {
			"sizing": {"risk_per_trade_cents": 200, "max_slippage_cents": 2, "min_fill": 3},
			"supported_series_manifest": str(manifest_path),
			"strategies": {
				"my-strat": {"enabled": True, "series": series},
			},
		}

	def _strat_with_whitelist(self, whitelist: list[str]) -> PaperStrategy:
		class _S(PaperStrategy):
			name = "my-strat"
			supported_series = whitelist
			default_params: dict = {}

			def on_tick(self, ctx):
				return []

		return _S()

	def test_manifest_supplies_whitelist_when_class_is_empty(self, tmp_path: Path) -> None:
		"""Class opts out (supported_series=[]) but manifest provides the list."""
		manifest = _write_manifest(tmp_path, {"my-strat": {"series": ["SERIES_A"]}})
		cfg = self._base_cfg(manifest, ["SERIES_A"])
		result = get_enabled_strategies(cfg, [self._strat_with_whitelist([])])
		assert len(result) == 1

	def test_manifest_rejects_out_of_list_when_class_is_empty(self, tmp_path: Path) -> None:
		"""Empty class whitelist + manifest: manifest still enforces membership."""
		manifest = _write_manifest(tmp_path, {"my-strat": {"series": ["SERIES_A"]}})
		cfg = self._base_cfg(manifest, ["SERIES_B"])
		with pytest.raises(ValueError, match="SERIES_B"):
			get_enabled_strategies(cfg, [self._strat_with_whitelist([])])

	def test_class_and_manifest_union(self, tmp_path: Path) -> None:
		"""Effective whitelist is union(class.supported_series, manifest[name].series)."""
		manifest = _write_manifest(tmp_path, {"my-strat": {"series": ["SERIES_B"]}})
		cfg = self._base_cfg(manifest, ["SERIES_A", "SERIES_B"])
		result = get_enabled_strategies(cfg, [self._strat_with_whitelist(["SERIES_A"])])
		assert len(result) == 1

	def test_union_still_rejects_series_in_neither(self, tmp_path: Path) -> None:
		"""Union is not permissive — a series in neither set still fails."""
		manifest = _write_manifest(tmp_path, {"my-strat": {"series": ["SERIES_B"]}})
		cfg = self._base_cfg(manifest, ["SERIES_C"])
		with pytest.raises(ValueError, match="SERIES_C"):
			get_enabled_strategies(cfg, [self._strat_with_whitelist(["SERIES_A"])])
