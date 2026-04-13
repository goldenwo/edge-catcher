"""Strategy discovery and config helpers for the paper trading framework."""

from __future__ import annotations

import importlib.util
import inspect
import json
import logging
from pathlib import Path

import yaml

from edge_catcher.monitors.sizing import validate_sizing_config
from edge_catcher.monitors.strategy_base import PaperStrategy

logger = logging.getLogger(__name__)

_DEFAULT_STRATEGIES_PATH = Path("edge_catcher/monitors/strategies_local.py")


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def load_config(config_path: Path) -> dict:
	"""Load a YAML config file.

	Args:
		config_path: Absolute or relative path to the YAML file.

	Returns:
		Parsed config as a dict.

	Raises:
		FileNotFoundError: If the file does not exist.
	"""
	if not config_path.exists():
		raise FileNotFoundError(f"Config file not found: {config_path}")
	with config_path.open("r", encoding="utf-8") as fh:
		return yaml.safe_load(fh) or {}


# ---------------------------------------------------------------------------
# Strategy discovery
# ---------------------------------------------------------------------------

def discover_strategies(module_path: Path | None = None) -> list[PaperStrategy]:
	"""Dynamically load and instantiate all PaperStrategy subclasses from a file.

	Args:
		module_path: Path to the Python file to load.  Defaults to
		             ``edge_catcher/monitors/strategies_local.py``.

	Returns:
		List of instantiated strategy objects.  Returns an empty list if the
		file does not exist.
	"""
	path = module_path if module_path is not None else _DEFAULT_STRATEGIES_PATH

	if not Path(path).exists():
		logger.debug("Strategies file not found: %s — returning empty list", path)
		return []

	try:
		spec = importlib.util.spec_from_file_location("_strategies_local", path)
		if spec is None or spec.loader is None:
			logger.error("Could not create module spec for %s", path)
			return []
		module = importlib.util.module_from_spec(spec)
		spec.loader.exec_module(module)  # type: ignore[union-attr]
	except Exception:
		logger.exception("Error loading strategies file: %s", path)
		return []

	strategies: list[PaperStrategy] = []
	for attr_name in dir(module):
		obj = getattr(module, attr_name)
		if (
			inspect.isclass(obj)
			and issubclass(obj, PaperStrategy)
			and obj is not PaperStrategy
			and isinstance(getattr(obj, "name", None), str)
		):
			try:
				strategies.append(obj())
			except Exception:
				logger.exception("Error instantiating strategy class %s", attr_name)

	return strategies


# ---------------------------------------------------------------------------
# Filtering + merging
# ---------------------------------------------------------------------------

def _load_manifest_series(manifest_path: str | Path | None) -> dict[str, list[str]]:
	"""Load ``{strategy: [series,...]}`` from the supported-series manifest.

	Returns an empty dict silently only when no path is configured (opt-out).
	If a path *is* configured but the file is missing or malformed, raise
	``ValueError`` — silently falling back to ``{}`` would collapse the
	effective whitelist to empty for every strategy whose class declares
	``supported_series = []``, re-introducing the pre-manifest
	opt-out-everything regime without any operator signal.
	"""
	if not manifest_path:
		return {}
	path = Path(manifest_path)
	if not path.exists():
		raise ValueError(
			f"supported_series_manifest configured but not found: {path}"
		)
	try:
		data = json.loads(path.read_text(encoding="utf-8"))
	except json.JSONDecodeError as exc:
		raise ValueError(
			f"supported_series_manifest at {path} is not valid JSON: {exc}"
		) from exc

	out: dict[str, list[str]] = {}
	for name, entry in (data.get("strategies") or {}).items():
		series = list((entry or {}).get("series") or [])
		if series:
			out[name] = series
	return out


def get_enabled_strategies(
	config: dict,
	all_strategies: list[PaperStrategy],
) -> list[PaperStrategy]:
	"""Filter to enabled strategies and apply config param overrides.

	Enforces ``supported_series`` on each strategy: if the strategy
	declares a non-empty whitelist, the config's series list must be a
	subset of it. Prevents enabling a strategy on a series it was never
	validated on. Set ``strict_series_validation: false`` at the top
	level of config to downgrade the error to a warning.

	TODO(metrics-follow-up): this function doesn't currently return the
	set of (strategy, series) pairs that were rejected during filtering.
	The paper trader's ``entries_skipped_unsupported`` gauge exists in
	metrics.py but is populated with 0 because there's nowhere to count
	the rejected pairs at startup. To close that gap, reshape this
	function to return ``(enabled, rejected_pairs)`` and have
	``run_engine`` call ``metrics.set_gauge("entries_skipped_unsupported",
	len(rejected_pairs))``. Deferred from Task 6a of the audit-followups
	plan — gauge infrastructure is fully wired end-to-end, just needs a
	population source.

	Args:
		config:         Full config dict.
		all_strategies: All discovered strategy instances.

	Returns:
		Filtered, merged list of enabled strategies.

	Raises:
		ValueError: If sizing config is missing or invalid, or if a
		            strategy is enabled on an unsupported series under
		            strict validation.
	"""
	validate_sizing_config(config)

	strats_cfg: dict = config.get("strategies", {})
	by_name: dict[str, PaperStrategy] = {s.name: s for s in all_strategies}
	strict = config.get("strict_series_validation", True)
	manifest_series = _load_manifest_series(config.get("supported_series_manifest"))

	enabled: list[PaperStrategy] = []

	for name, scfg in strats_cfg.items():
		if not scfg.get("enabled", False):
			continue
		strat = by_name.get(name)
		if strat is None:
			logger.warning("Config references strategy '%s' but it was not discovered", name)
			continue

		class_supported = list(getattr(strat, "supported_series", []) or [])
		manifest_supported = manifest_series.get(name, [])
		# Union preserves class-declared order first, then appends manifest-only entries,
		# so error messages stay deterministic across runs.
		effective: list[str] = list(class_supported)
		for s in manifest_supported:
			if s not in effective:
				effective.append(s)

		requested = list(scfg.get("series", []) or [])
		if effective and requested:
			unsupported = [s for s in requested if s not in effective]
			if unsupported:
				msg = (
					f"Strategy '{name}' is enabled on series not in its supported_series "
					f"whitelist: {unsupported}. Declared supported: {effective}. "
					f"Either add the series to the strategy's supported_series, or set "
					f"'strict_series_validation: false' in config to downgrade this to a warning."
				)
				if strict:
					raise ValueError(msg)
				logger.warning(msg)

		# Merge param overrides
		params_override: dict = scfg.get("params", {}) or {}
		if params_override:
			strat.default_params = {**strat.default_params, **params_override}

		enabled.append(strat)

	return enabled
