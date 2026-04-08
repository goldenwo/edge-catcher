"""Registry of all available data adapters."""
from __future__ import annotations
import os
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Optional
from edge_catcher.fees import FeeModel, STANDARD_FEE, INDEX_FEE, ZERO_FEE


def _db_file_from_markets_yaml(markets_yaml: str) -> str:
    """Derive DB path from markets YAML filename.

    e.g. config.local/markets-weather.yaml → data/kalshi-weather.db
         config.local/markets.yaml         → data/kalshi.db
    """
    stem = PurePosixPath(markets_yaml).stem  # "markets-weather"
    suffix = stem.removeprefix("markets")     # "-weather"
    return f"data/kalshi{suffix}.db"


@dataclass
class AdapterMeta:
    id: str
    name: str
    description: str
    requires_api_key: bool
    api_key_env_var: Optional[str] = None
    default_start_date: Optional[str] = None  # ISO date, shown as default in UI
    markets_yaml: Optional[str] = None  # path to markets YAML (None = non-Kalshi adapters)
    db_file: str = ""  # database file this adapter writes to (auto-derived from markets_yaml)
    fee_model: FeeModel = field(default_factory=lambda: STANDARD_FEE)
    coinbase_product_id: Optional[str] = None  # e.g. "ETH-USD" — set for Coinbase adapters
    fee_overrides: dict[str, FeeModel] = field(default_factory=dict)

    def __post_init__(self):
        if not self.db_file and self.markets_yaml:
            self.db_file = _db_file_from_markets_yaml(self.markets_yaml)
        elif not self.db_file:
            self.db_file = "data/kalshi.db"

ADAPTERS: list[AdapterMeta] = [
    AdapterMeta(
        id="kalshi",
        name="Kalshi BTC",
        description="Download settled BTC contracts (KXBTC/D/W/M/15M) and trade history from Kalshi.",
        requires_api_key=False,
        api_key_env_var="KALSHI_API_KEY",
        default_start_date="2025-03-21",
        markets_yaml="config.local/markets.yaml",
    ),
    AdapterMeta(
        id="coinbase_btc",
        name="Coinbase BTC-USD",
        description="Download 1-minute BTC-USD OHLC candles from Coinbase (no API key required).",
        requires_api_key=False,
        api_key_env_var=None,
        default_start_date="2025-03-21",
        db_file="data/btc.db",
        fee_model=ZERO_FEE,
        coinbase_product_id="BTC-USD",
    ),
    AdapterMeta(
        id="coinbase_eth",
        name="Coinbase ETH-USD",
        description="Download 1-minute ETH-USD OHLC candles from Coinbase (no API key required).",
        requires_api_key=False,
        default_start_date="2025-01-01",
        db_file="data/ohlc.db",
        fee_model=ZERO_FEE,
        coinbase_product_id="ETH-USD",
    ),
    AdapterMeta(
        id="coinbase_sol",
        name="Coinbase SOL-USD",
        description="Download 1-minute SOL-USD OHLC candles from Coinbase (no API key required).",
        requires_api_key=False,
        default_start_date="2025-01-01",
        db_file="data/ohlc.db",
        fee_model=ZERO_FEE,
        coinbase_product_id="SOL-USD",
    ),
    AdapterMeta(
        id="coinbase_xrp",
        name="Coinbase XRP-USD",
        description="Download 1-minute XRP-USD OHLC candles from Coinbase (no API key required).",
        requires_api_key=False,
        default_start_date="2025-01-01",
        db_file="data/ohlc.db",
        fee_model=ZERO_FEE,
        coinbase_product_id="XRP-USD",
    ),
    AdapterMeta(
        id="coinbase_doge",
        name="Coinbase DOGE-USD",
        description="Download 1-minute DOGE-USD OHLC candles from Coinbase (no API key required).",
        requires_api_key=False,
        default_start_date="2025-01-01",
        db_file="data/ohlc.db",
        fee_model=ZERO_FEE,
        coinbase_product_id="DOGE-USD",
    ),
    AdapterMeta(
        id="kalshi_sports",
        name="Kalshi Sports",
        description="Download settled NBA/MLB spread and moneyline contracts from Kalshi.",
        requires_api_key=False,
        api_key_env_var="KALSHI_API_KEY",
        default_start_date="2025-01-01",
        markets_yaml="config.local/markets-sports.yaml",
    ),
    AdapterMeta(
        id="kalshi_crypto",
        name="Kalshi Crypto (Altcoins)",
        description="Download settled altcoin contracts (ETH/SOL/XRP/DOGE/BNB/HYPE — hourly, daily, 15M) from Kalshi.",
        requires_api_key=False,
        api_key_env_var="KALSHI_API_KEY",
        default_start_date="2025-01-01",
        markets_yaml="config.local/markets-altcrypto.yaml",
    ),
    AdapterMeta(
        id="kalshi_weather",
        name="Kalshi Weather",
        description="Download settled weather contracts (temperature, rain) from Kalshi.",
        requires_api_key=False,
        api_key_env_var="KALSHI_API_KEY",
        default_start_date="2025-01-01",
        markets_yaml="config.local/markets-weather.yaml",
    ),
    AdapterMeta(
        id="kalshi_financials",
        name="Kalshi Financials",
        description="Download settled financials/economics contracts (Nasdaq, S&P, yields, jobless) from Kalshi.",
        requires_api_key=False,
        api_key_env_var="KALSHI_API_KEY",
        default_start_date="2025-01-01",
        markets_yaml="config.local/markets-financials.yaml",
        fee_overrides={"KXINX": INDEX_FEE, "KXNASDAQ100": INDEX_FEE},
    ),
    AdapterMeta(
        id="kalshi_entertainment",
        name="Kalshi Entertainment",
        description="Download settled entertainment contracts (Spotify charts, awards) from Kalshi.",
        requires_api_key=False,
        api_key_env_var="KALSHI_API_KEY",
        default_start_date="2025-01-01",
        markets_yaml="config.local/markets-entertainment.yaml",
    ),
    AdapterMeta(
        id="kalshi_politics",
        name="Kalshi Politics & Elections",
        description="Download settled politics/election/mentions contracts from Kalshi.",
        requires_api_key=False,
        api_key_env_var="KALSHI_API_KEY",
        default_start_date="2025-01-01",
        markets_yaml="config.local/markets-politics.yaml",
    ),
    AdapterMeta(
        id="kalshi_esports",
        name="Kalshi Esports",
        description="Download settled esports contracts (CS2, LoL, ATP tennis, J-League) from Kalshi.",
        requires_api_key=False,
        api_key_env_var="KALSHI_API_KEY",
        default_start_date="2025-01-01",
        markets_yaml="config.local/markets-esports.yaml",
    ),
]

def get_adapter(adapter_id: str) -> Optional[AdapterMeta]:
    return next((a for a in ADAPTERS if a.id == adapter_id), None)

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
