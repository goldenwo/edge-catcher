"""Central adapter registry — aggregates per-exchange adapter lists.

To add a new exchange: create edge_catcher/adapters/<exchange>/registry.py
with a <EXCHANGE>_ADAPTERS: list[AdapterMeta], then import + concat here.

See docs/adr/0001-adapter-registry.md.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from edge_catcher.adapters.base import AdapterMeta  # re-export
from edge_catcher.adapters.kalshi.registry import KALSHI_ADAPTERS
from edge_catcher.adapters.coinbase.registry import COINBASE_ADAPTERS
from edge_catcher.fees import FeeModel, STANDARD_FEE


ADAPTERS: list[AdapterMeta] = [*KALSHI_ADAPTERS, *COINBASE_ADAPTERS]


def get_adapter(adapter_id: str) -> Optional[AdapterMeta]:
	return next((a for a in ADAPTERS if a.id == adapter_id), None)


def resolve_db_for_series(series: str) -> Optional[Path]:
	"""Find which database contains a given series_ticker."""
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
		except Exception:
			continue
	return None


def is_api_key_set(meta: AdapterMeta) -> bool:
	if not meta.api_key_env_var:
		return False
	return bool(os.getenv(meta.api_key_env_var))


def get_fee_model(adapter_id: str) -> FeeModel:
	"""Return the fee model for a specific adapter by ID (preferred lookup)."""
	adapter = get_adapter(adapter_id)
	return adapter.fee_model if adapter else STANDARD_FEE


def get_fee_model_for_db(db_path: str, series: str | None = None) -> FeeModel:
	"""Return the fee model for a given DB path, with optional per-series override.

	Resolution: if series is provided and the adapter has fee_overrides
	matching a prefix of the series, return the override. Otherwise
	return the adapter's default fee model.
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
	return STANDARD_FEE
