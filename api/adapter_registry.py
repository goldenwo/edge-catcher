"""Central adapter registry — aggregates per-exchange adapter lists.

To add a new exchange: create edge_catcher/adapters/<exchange>/registry.py
with a <EXCHANGE>_ADAPTERS: list[AdapterMeta], then import + concat here.

See docs/adr/0001-adapter-registry.md.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

from edge_catcher.adapters.base import AdapterMeta  # re-export
from edge_catcher.adapters.kalshi.registry import KALSHI_ADAPTERS
from edge_catcher.adapters.coinbase.registry import COINBASE_ADAPTERS
from edge_catcher.fees import FeeModel, ZERO_FEE


log = logging.getLogger(__name__)


ADAPTERS: list[AdapterMeta] = [*KALSHI_ADAPTERS, *COINBASE_ADAPTERS]


def get_adapter(adapter_id: str) -> Optional[AdapterMeta]:
	return next((a for a in ADAPTERS if a.id == adapter_id), None)


def resolve_db_for_series(series: str) -> Optional[Path]:
	"""Find which database contains a given series_ticker."""
	import sqlite3
	from edge_catcher.storage.db import get_connection

	seen: set[str] = set()
	for adapter in ADAPTERS:
		db_path = Path(adapter.db_file)
		db_key = str(db_path)
		if db_key in seen or not db_path.exists():
			continue
		seen.add(db_key)
		try:
			conn = get_connection(db_path)
			try:
				row = conn.execute(
					"SELECT 1 FROM markets WHERE series_ticker = ? LIMIT 1", (series,)
				).fetchone()
				if row:
					return db_path
			finally:
				conn.close()
		except sqlite3.Error as e:
			log.debug("resolve_db_for_series: skipping %s (%s)", db_path, e)
			continue
	return None


def is_api_key_set(meta: AdapterMeta) -> bool:
	if not meta.api_key_env_var:
		return False
	return bool(os.getenv(meta.api_key_env_var))


def get_fee_model(adapter_id: str) -> FeeModel:
	"""Return the fee model for a specific adapter by ID (preferred lookup).

	Falls back to ZERO_FEE with a warning if the adapter_id is unknown —
	every registered adapter declares a fee_model explicitly, so a fallback
	hit indicates a misconfiguration.
	"""
	adapter = get_adapter(adapter_id)
	if adapter is not None:
		return adapter.fee_model
	log.warning(
		"get_fee_model: no adapter registered with id=%r; returning ZERO_FEE. "
		"This likely indicates a typo or stale adapter reference.",
		adapter_id,
	)
	return ZERO_FEE


def get_fee_model_for_db(db_path: str, series: str | None = None) -> FeeModel:
	"""Return the fee model for a given DB path, with optional per-series override.

	Resolution: if series is provided and the adapter has fee_overrides
	matching a prefix of the series, return the override. Otherwise
	return the adapter's default fee model.

	Falls back to ZERO_FEE with a warning if no adapter declares the given
	db_file — a lookup miss indicates a stale path or unregistered adapter.

	Caveat: if multiple adapters share the same db_file (e.g. Coinbase
	eth/sol/xrp/doge all point at data/ohlc.db), the FIRST match in
	ADAPTERS wins. This is safe when the shared-DB adapters have the
	same fee_model, but would silently ignore per-adapter fee_overrides
	on non-first entries. Prefer per-series fee_overrides on the first
	adapter for that DB, or split into distinct db_file paths.
	"""
	resolved = str(Path(db_path).resolve())
	for adapter in ADAPTERS:
		adapter_resolved = str(Path(adapter.db_file).resolve())
		if resolved == adapter_resolved:
			if series and adapter.fee_overrides:
				for prefix, fee_model in adapter.fee_overrides.items():
					if series.startswith(prefix):
						return fee_model
			return adapter.fee_model
	log.warning(
		"get_fee_model_for_db: no adapter declares db_file=%r; returning ZERO_FEE. "
		"This likely indicates a stale DB path or unregistered adapter.",
		db_path,
	)
	return ZERO_FEE
