"""Polymarket adapter metadata. See docs/adr/0001-adapter-registry.md.

Polymarket data is split across two free public APIs:
  - Gamma  (https://gamma-api.polymarket.com)  — markets/events metadata
  - CLOB   (https://clob.polymarket.com)        — orderbooks + trade history

The adapter joins them: Gamma supplies the markets list (filtered by the
configured topic/series), CLOB supplies per-market trade history. No auth.

Single registry entry to start; future categories (politics, sports, etc.)
get their own AdapterMeta entries pointing at separate db_files + market
YAMLs once the topical scope is defined.
"""
from __future__ import annotations

from edge_catcher.adapters.base import AdapterMeta
from edge_catcher.adapters.polymarket.fees import POLYMARKET_FEE


POLYMARKET_ADAPTERS: list[AdapterMeta] = [
	AdapterMeta(
		id="polymarket_default",
		exchange="polymarket",
		name="Polymarket (default)",
		description=(
			"Download market metadata + trade history from Polymarket's public "
			"Gamma + CLOB APIs. No auth. Single-DB starter — split into "
			"per-category adapters as topical scopes solidify."
		),
		db_file="data/polymarket.db",
		fee_model=POLYMARKET_FEE,
		requires_api_key=False,
		api_key_env_var=None,
		default_start_date="2024-01-01",
		markets_yaml="config/markets-polymarket.yaml",
	),
]
