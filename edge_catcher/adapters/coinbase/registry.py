"""Coinbase adapter metadata. See docs/adr/0001-adapter-registry.md.

Coinbase-specific product IDs live in `extra["product_id"]`, not as a
typed attribute on AdapterMeta."""
from __future__ import annotations

from edge_catcher.adapters.base import AdapterMeta
from edge_catcher.fees import ZERO_FEE


COINBASE_ADAPTERS: list[AdapterMeta] = [
	AdapterMeta(
		id="coinbase_btc",
		exchange="coinbase",
		name="Coinbase BTC-USD",
		description="Download 1-minute BTC-USD OHLC candles from Coinbase (no API key required).",
		db_file="data/btc.db",
		fee_model=ZERO_FEE,
		requires_api_key=False,
		default_start_date="2025-03-21",
		extra={"product_id": "BTC-USD"},
	),
	AdapterMeta(
		id="coinbase_eth",
		exchange="coinbase",
		name="Coinbase ETH-USD",
		description="Download 1-minute ETH-USD OHLC candles from Coinbase (no API key required).",
		db_file="data/ohlc.db",
		fee_model=ZERO_FEE,
		requires_api_key=False,
		default_start_date="2025-01-01",
		extra={"product_id": "ETH-USD"},
	),
	AdapterMeta(
		id="coinbase_sol",
		exchange="coinbase",
		name="Coinbase SOL-USD",
		description="Download 1-minute SOL-USD OHLC candles from Coinbase (no API key required).",
		db_file="data/ohlc.db",
		fee_model=ZERO_FEE,
		requires_api_key=False,
		default_start_date="2025-01-01",
		extra={"product_id": "SOL-USD"},
	),
	AdapterMeta(
		id="coinbase_xrp",
		exchange="coinbase",
		name="Coinbase XRP-USD",
		description="Download 1-minute XRP-USD OHLC candles from Coinbase (no API key required).",
		db_file="data/ohlc.db",
		fee_model=ZERO_FEE,
		requires_api_key=False,
		default_start_date="2025-01-01",
		extra={"product_id": "XRP-USD"},
	),
	AdapterMeta(
		id="coinbase_doge",
		exchange="coinbase",
		name="Coinbase DOGE-USD",
		description="Download 1-minute DOGE-USD OHLC candles from Coinbase (no API key required).",
		db_file="data/ohlc.db",
		fee_model=ZERO_FEE,
		requires_api_key=False,
		default_start_date="2025-01-01",
		extra={"product_id": "DOGE-USD"},
	),
]
