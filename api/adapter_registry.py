"""Registry of all available data adapters."""
from __future__ import annotations
import os
from dataclasses import dataclass
from typing import Optional

@dataclass
class AdapterMeta:
    id: str
    name: str
    description: str
    requires_api_key: bool
    api_key_env_var: Optional[str] = None
    default_start_date: Optional[str] = None  # ISO date, shown as default in UI

ADAPTERS: list[AdapterMeta] = [
    AdapterMeta(
        id="kalshi",
        name="Kalshi BTC",
        description="Download settled BTC contracts (KXBTC/D/W/M/15M) and trade history from Kalshi.",
        requires_api_key=False,
        api_key_env_var="KALSHI_API_KEY",
        default_start_date="2025-03-21",
    ),
    AdapterMeta(
        id="coinbase_btc",
        name="Coinbase BTC-USD",
        description="Download 1-minute BTC-USD OHLC candles from Coinbase (no API key required).",
        requires_api_key=False,
        api_key_env_var=None,
        default_start_date="2025-03-21",
    ),
    AdapterMeta(
        id="kalshi_sports",
        name="Kalshi Sports",
        description="Download settled NBA/MLB spread and moneyline contracts from Kalshi.",
        requires_api_key=False,
        api_key_env_var="KALSHI_API_KEY",
        default_start_date="2025-01-01",
    ),
    AdapterMeta(
        id="kalshi_crypto",
        name="Kalshi Crypto (Altcoins)",
        description="Download settled altcoin contracts (ETH/SOL/XRP/DOGE/BNB/HYPE — hourly, daily, 15M) from Kalshi.",
        requires_api_key=False,
        api_key_env_var="KALSHI_API_KEY",
        default_start_date="2025-01-01",
    ),
    AdapterMeta(
        id="kalshi_weather",
        name="Kalshi Weather",
        description="Download settled weather contracts (temperature, rain) from Kalshi.",
        requires_api_key=False,
        api_key_env_var="KALSHI_API_KEY",
        default_start_date="2025-01-01",
    ),
    AdapterMeta(
        id="kalshi_financials",
        name="Kalshi Financials",
        description="Download settled financials/economics contracts (Nasdaq, S&P, yields, jobless) from Kalshi.",
        requires_api_key=False,
        api_key_env_var="KALSHI_API_KEY",
        default_start_date="2025-01-01",
    ),
    AdapterMeta(
        id="kalshi_entertainment",
        name="Kalshi Entertainment",
        description="Download settled entertainment contracts (Spotify charts, awards) from Kalshi.",
        requires_api_key=False,
        api_key_env_var="KALSHI_API_KEY",
        default_start_date="2025-01-01",
    ),
    AdapterMeta(
        id="kalshi_politics",
        name="Kalshi Politics & Elections",
        description="Download settled politics/election/mentions contracts from Kalshi.",
        requires_api_key=False,
        api_key_env_var="KALSHI_API_KEY",
        default_start_date="2025-01-01",
    ),
    AdapterMeta(
        id="kalshi_esports",
        name="Kalshi Esports",
        description="Download settled esports contracts (CS2, LoL, ATP tennis, J-League) from Kalshi.",
        requires_api_key=False,
        api_key_env_var="KALSHI_API_KEY",
        default_start_date="2025-01-01",
    ),
]

def get_adapter(adapter_id: str) -> Optional[AdapterMeta]:
    return next((a for a in ADAPTERS if a.id == adapter_id), None)

def is_api_key_set(meta: AdapterMeta) -> bool:
    if not meta.api_key_env_var:
        return False
    return bool(os.getenv(meta.api_key_env_var))
