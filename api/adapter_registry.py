"""Registry of all available data adapters."""
from __future__ import annotations
import os
from pathlib import Path
from typing import Optional
from edge_catcher.adapters.base import AdapterMeta
from edge_catcher.fees import FeeModel, STANDARD_FEE, INDEX_FEE, ZERO_FEE


ADAPTERS: list[AdapterMeta] = [
    AdapterMeta(
        id="kalshi",
        exchange="kalshi",
        name="Kalshi BTC",
        description="Download settled BTC contracts (KXBTC/D/W/M/15M) and trade history from Kalshi.",
        db_file="data/kalshi.db",
        fee_model=STANDARD_FEE,
        requires_api_key=False,
        api_key_env_var="KALSHI_API_KEY",
        default_start_date="2025-03-21",
        markets_yaml="config/markets.yaml",
    ),
    AdapterMeta(
        id="coinbase_btc",
        exchange="coinbase",
        name="Coinbase BTC-USD",
        description="Download 1-minute BTC-USD OHLC candles from Coinbase (no API key required).",
        db_file="data/btc.db",
        fee_model=ZERO_FEE,
        requires_api_key=False,
        api_key_env_var=None,
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
    AdapterMeta(
        id="kalshi_sports",
        exchange="kalshi",
        name="Kalshi Sports",
        description="Download settled NBA/MLB spread and moneyline contracts from Kalshi.",
        db_file="data/kalshi-sports.db",
        fee_model=STANDARD_FEE,
        requires_api_key=False,
        api_key_env_var="KALSHI_API_KEY",
        default_start_date="2025-01-01",
        markets_yaml="config/markets-sports.yaml",
    ),
    AdapterMeta(
        id="kalshi_crypto",
        exchange="kalshi",
        name="Kalshi Crypto (Altcoins)",
        description="Download settled altcoin contracts (ETH/SOL/XRP/DOGE/BNB/HYPE — hourly, daily, 15M) from Kalshi.",
        db_file="data/kalshi-altcrypto.db",
        fee_model=STANDARD_FEE,
        requires_api_key=False,
        api_key_env_var="KALSHI_API_KEY",
        default_start_date="2025-01-01",
        markets_yaml="config/markets-altcrypto.yaml",
    ),
    AdapterMeta(
        id="kalshi_weather",
        exchange="kalshi",
        name="Kalshi Weather",
        description="Download settled weather contracts (temperature, rain) from Kalshi.",
        db_file="data/kalshi-weather.db",
        fee_model=STANDARD_FEE,
        requires_api_key=False,
        api_key_env_var="KALSHI_API_KEY",
        default_start_date="2025-01-01",
        markets_yaml="config/markets-weather.yaml",
    ),
    AdapterMeta(
        id="kalshi_financials",
        exchange="kalshi",
        name="Kalshi Financials",
        description="Download settled financials/economics contracts (Nasdaq, S&P, yields, jobless) from Kalshi.",
        db_file="data/kalshi-financials.db",
        fee_model=STANDARD_FEE,
        requires_api_key=False,
        api_key_env_var="KALSHI_API_KEY",
        default_start_date="2025-01-01",
        markets_yaml="config/markets-financials.yaml",
        fee_overrides={"KXINX": INDEX_FEE, "KXNASDAQ100": INDEX_FEE},
    ),
    AdapterMeta(
        id="kalshi_entertainment",
        exchange="kalshi",
        name="Kalshi Entertainment",
        description="Download settled entertainment contracts (Spotify charts, awards) from Kalshi.",
        db_file="data/kalshi-entertainment.db",
        fee_model=STANDARD_FEE,
        requires_api_key=False,
        api_key_env_var="KALSHI_API_KEY",
        default_start_date="2025-01-01",
        markets_yaml="config/markets-entertainment.yaml",
    ),
    AdapterMeta(
        id="kalshi_politics",
        exchange="kalshi",
        name="Kalshi Politics & Elections",
        description="Download settled politics/election/mentions contracts from Kalshi.",
        db_file="data/kalshi-politics.db",
        fee_model=STANDARD_FEE,
        requires_api_key=False,
        api_key_env_var="KALSHI_API_KEY",
        default_start_date="2025-01-01",
        markets_yaml="config/markets-politics.yaml",
    ),
    AdapterMeta(
        id="kalshi_esports",
        exchange="kalshi",
        name="Kalshi Esports",
        description="Download settled esports contracts (CS2, LoL, ATP tennis, J-League) from Kalshi.",
        db_file="data/kalshi-esports.db",
        fee_model=STANDARD_FEE,
        requires_api_key=False,
        api_key_env_var="KALSHI_API_KEY",
        default_start_date="2025-01-01",
        markets_yaml="config/markets-esports.yaml",
    ),
]

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
    from pathlib import Path
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
