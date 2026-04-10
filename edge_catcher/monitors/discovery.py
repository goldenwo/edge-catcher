"""Strategy discovery and config helpers for the paper trading framework."""

from __future__ import annotations

import importlib.util
import inspect
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

def get_enabled_strategies(
	config: dict,
	all_strategies: list[PaperStrategy],
) -> list[PaperStrategy]:
	"""Filter to enabled strategies and apply config param overrides.

	Args:
		config:         Full config dict.
		all_strategies: All discovered strategy instances.

	Returns:
		Filtered, merged list of enabled strategies.

	Raises:
		ValueError: If sizing config is missing or invalid.
	"""
	validate_sizing_config(config)

	strats_cfg: dict = config.get("strategies", {})
	by_name: dict[str, PaperStrategy] = {s.name: s for s in all_strategies}

	enabled: list[PaperStrategy] = []

	for name, scfg in strats_cfg.items():
		if not scfg.get("enabled", False):
			continue
		strat = by_name.get(name)
		if strat is None:
			logger.warning("Config references strategy '%s' but it was not discovered", name)
			continue

		# Merge param overrides
		params_override: dict = scfg.get("params", {}) or {}
		if params_override:
			strat.default_params = {**strat.default_params, **params_override}

		enabled.append(strat)

	return enabled
