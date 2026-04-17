"""Shared configuration helpers for the Edge Catcher API."""
from __future__ import annotations

import os
from pathlib import Path


def validate_db(db: str) -> Path:
	"""Resolve a db filename to a safe Path within data/. Raises ValueError on invalid input."""
	from api.adapter_registry import ADAPTERS
	valid_dbs = {Path(a.db_file).name for a in ADAPTERS}
	if db not in valid_dbs:
		raise ValueError(f"Unknown database: {db}. Valid: {sorted(valid_dbs)}")
	return Path("data") / db


def get_resolver():
	"""Return a DataSourceResolver configured from the environment."""
	from edge_catcher.research.data_source_resolver import DataSourceResolver
	return DataSourceResolver.from_environment()


def config_path() -> Path:
	"""Return config path for hypotheses and fees config."""
	explicit = os.getenv("CONFIG_PATH")
	if explicit:
		return Path(explicit)
	return Path("config")


def markets_yaml() -> Path:
	"""Return the primary markets config path (Kalshi BTC)."""
	return Path("config") / "markets-btc.yaml"


def research_db_path() -> Path:
	"""Return the research database path from env or default."""
	return Path(os.getenv("RESEARCH_DB", "data/research.db"))


def load_merged_hypotheses() -> dict:
	"""Merge hypotheses from config/ and config.local/ (local overrides public)."""
	import yaml
	merged: dict = {}
	for cfg_dir in [config_path(), Path("config.local")]:
		cfg_file = cfg_dir / "hypotheses.yaml"
		if cfg_file.exists():
			with open(cfg_file) as f:
				data = yaml.safe_load(f) or {}
			merged.update(data.get("hypotheses", {}))
	return merged
