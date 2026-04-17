import os
import time
import json
import random
import logging
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone

import yaml
import requests

from edge_catcher.adapters.base import MarketAdapter
from edge_catcher.storage.models import Market, Trade

logger = logging.getLogger(__name__)

# Minimal schemas for validating Kalshi API responses.
# Each entry defines the required top-level keys and optional per-item field lists.
# NOTE: Kalshi returns numeric fields with _dollars (string) or _fp (string) suffixes.
# yes_bid/yes_ask are absent from settled market responses — removed from schema.
SCHEMAS: Dict[str, Any] = {
    "markets_list": {
        "required": ["markets"],
        "markets_item": ["ticker", "status"],
    },
    "market_detail": {
        "required": ["market"],
        "item": ["ticker", "status"],
    },
    "trades_list": {
        "required": ["trades"],
        "trades_item": [
            "trade_id",
            "ticker",
            "yes_price_dollars",
            "no_price_dollars",
            "count_fp",
            "taker_side",
            "created_time",
        ],
    },
}


class KalshiAdapter(MarketAdapter):
    """Kalshi REST API adapter.

    Fetches market metadata and trade history from the Kalshi elections API.
    NOTE: base_url points to the elections subdomain as specified in markets-btc.yaml.
    For crypto series (KXBTC*), verify whether api.kalshi.com is needed instead.
    """

    SCHEMAS = SCHEMAS

    def __init__(
        self,
        config_path: Path = Path("config/markets-btc.yaml"),
        api_key: Optional[str] = None,
        dry_run: bool = False,
    ) -> None:
        # Resolve config path relative to cwd if not absolute
        config_path = Path(config_path)
        if not config_path.is_absolute():
            config_path = Path.cwd() / config_path

        with open(config_path, "r") as fh:
            config = yaml.safe_load(fh)

        kalshi_cfg = config["adapters"]["kalshi"]
        # NOTE: base_url is https://api.elections.kalshi.com/trade-api/v2 per build plan
        self.base_url: str = kalshi_cfg["base_url"].rstrip("/")
        self.rate_limit_seconds: float = float(kalshi_cfg.get("rate_limit_seconds", 0.2))
        self.series: List[str] = kalshi_cfg.get("series", [])
        self.pagination_limit: int = int(
            kalshi_cfg.get("pagination", {}).get("default_limit", 200)
        )
        # Configurable status filter — defaults to ["settled"] if not in config
        self.statuses: List[str] = kalshi_cfg.get("statuses", ["settled"])

        # API key: explicit param takes priority, then environment variable
        self.api_key: Optional[str] = api_key or os.environ.get("KALSHI_API_KEY")
        self.dry_run: bool = dry_run
        self.min_available_ram_pct: float = float(
            kalshi_cfg.get("min_available_ram_pct", 10)
        )

        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        if self.api_key:
            self.session.headers.update({"Authorization": f"Bearer {self.api_key}"})

        self._last_request_time: float = 0.0

    # ------------------------------------------------------------------
    # Memory guard
    # ------------------------------------------------------------------

    def _check_memory(self) -> None:
        """Pause if available RAM drops below a percentage of total. Scales across machines."""
        try:
            import psutil
            mem = psutil.virtual_memory()
            available_pct = mem.available / mem.total * 100
            if available_pct < self.min_available_ram_pct:
                available_mb = mem.available / (1024 * 1024)
                total_gb = mem.total / (1024 ** 3)
                logger.warning(
                    f"Low RAM: {available_mb:.0f}MB free ({available_pct:.1f}% of {total_gb:.0f}GB). "
                    f"Threshold: {self.min_available_ram_pct}%. Pausing 30s..."
                )
                time.sleep(30)
        except ImportError:
            pass  # psutil optional — skip silently if not installed

    # ------------------------------------------------------------------
    # Rate limiting
    # ------------------------------------------------------------------

    def _rate_limit(self) -> None:
        """Enforce minimum time between requests."""
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < self.rate_limit_seconds:
            time.sleep(self.rate_limit_seconds - elapsed)
        self._last_request_time = time.monotonic()

    # ------------------------------------------------------------------
    # HTTP primitives
    # ------------------------------------------------------------------

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[dict] = None,
        max_retries: int = 5,
    ) -> dict:
        """Make authenticated request with rate limiting and exponential backoff on 429s/5xx."""
        url = f"{self.base_url}{path}"
        for attempt in range(max_retries):
            self._rate_limit()
            try:
                response = self.session.request(method, url, params=params, timeout=30)
                if response.status_code == 429:
                    wait = min(60, (2 ** attempt) + random.uniform(0, 1))
                    logger.warning(
                        f"Rate limited (429). Waiting {wait:.1f}s (attempt {attempt+1}/{max_retries})"
                    )
                    time.sleep(wait)
                    continue
                if response.status_code >= 500:
                    wait = min(60, (2 ** attempt) + random.uniform(0, 1))
                    logger.warning(
                        f"Server error {response.status_code}. Waiting {wait:.1f}s (attempt {attempt+1}/{max_retries})"
                    )
                    time.sleep(wait)
                    continue
                response.raise_for_status()  # 4xx errors (not 429) raise immediately
                try:
                    return response.json()
                except requests.exceptions.JSONDecodeError as e:
                    logger.error(
                        f"Invalid JSON from {url}: status={response.status_code}, "
                        f"body_preview={response.text[:200]!r}"
                    )
                    raise ValueError(f"Non-JSON response from {url}: {e}") from e
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                logger.warning(f"Request error on attempt {attempt+1}/{max_retries}: {e}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(2 ** attempt)
                continue
        raise RuntimeError(f"Max retries exceeded for {url}")

    # ------------------------------------------------------------------
    # Schema validation
    # ------------------------------------------------------------------

    def validate_response(self, data: dict, schema_key: str) -> bool:
        """Validate API response against expected schema.

        Raises ValueError with details if required keys are missing.
        Logs a warning if first-item fields are missing (non-fatal).
        Returns True on success.
        """
        if schema_key not in self.SCHEMAS:
            raise ValueError(f"Unknown schema key: {schema_key}")
        schema = self.SCHEMAS[schema_key]
        # Validate top-level required keys
        missing = [k for k in schema["required"] if k not in data]
        if missing:
            raise ValueError(
                f"API response for schema '{schema_key}' missing required keys: {missing}. "
                f"Got keys: {list(data.keys())}"
            )
        # Validate first item's fields if item schema exists
        list_key = schema["required"][0]  # e.g., "markets", "trades"
        item_schema_key = f"{list_key}_item" if f"{list_key}_item" in schema else "item"
        if item_schema_key in schema and data.get(list_key):
            raw = data[list_key]
            first_item = raw if isinstance(raw, dict) else raw[0]
            missing_item_fields = [f for f in schema[item_schema_key] if f not in first_item]
            if missing_item_fields:
                logger.warning(
                    f"Schema '{schema_key}': first item missing expected fields: {missing_item_fields}"
                )
                # Log warning but don't raise — API may add/remove non-critical fields
        return True

    # ------------------------------------------------------------------
    # Abstract method implementations
    # ------------------------------------------------------------------

    def collect_markets(self, series_tickers: Optional[List[str]] = None) -> List[Market]:
        """Fetch all markets for the configured series (or provided list).

        Paginates through GET /markets for each series ticker.
        In dry_run mode only one page per series is fetched.
        """
        all_markets: List[Market] = []
        for series, page_markets in self.iter_market_pages(series_tickers):
            all_markets.extend(page_markets)
        return all_markets

    def iter_market_pages(self, series_tickers: Optional[List[str]] = None):
        """Yield (series_ticker, List[Market]) for each page fetched.

        Allows callers to save incrementally rather than waiting for the full
        download to complete. Paginates GET /markets per series.
        In dry_run mode only one page per series is yielded.
        """
        targets = series_tickers if series_tickers is not None else self.series

        for series in targets:
            logger.info(f"Fetching markets for series: {series}")
            cursor: Optional[str] = None
            series_total = 0

            max_pages = 10_000
            page_count = 0
            while True:
                page_count += 1
                if page_count > max_pages:
                    logger.error(
                        f"Exceeded {max_pages} pages — possible infinite loop. Stopping."
                    )
                    break

                params: Dict[str, Any] = {
                    "series_ticker": series,
                    "limit": self.pagination_limit,
                }
                # Apply each configured status as a separate query param
                # (Kalshi accepts multiple status= params or a single one)
                if self.statuses:
                    params["status"] = self.statuses[0]  # primary filter
                if cursor:
                    params["cursor"] = cursor

                data = self._request("GET", "/markets", params=params)
                self.validate_response(data, "markets_list")

                page_markets_raw = data.get("markets") or []
                page_markets = [self._parse_market(m) for m in page_markets_raw]
                series_total += len(page_markets)

                cursor = data.get("cursor") or ""
                logger.info(
                    f"Series {series}: page {page_count}, "
                    f"{len(page_markets)} markets (total so far: {series_total}), "
                    f"cursor={cursor!r}"
                )

                yield series, page_markets

                if not cursor or self.dry_run:
                    break

            logger.info(f"Series {series}: finished — {series_total} markets total")

    def collect_trades(self, ticker: str, since: Optional[str] = None) -> List[Trade]:
        """Fetch all trades for a market ticker.

        Paginates through GET /markets/trades?ticker={ticker}.
        If since (ISO datetime string) is provided, stops when trade timestamps go
        earlier than that cutoff. Also passes min_ts as a Unix timestamp hint to
        the API to reduce data transfer.
        In dry_run mode only one page is fetched.
        """
        self._check_memory()
        since_dt: Optional[datetime] = None
        params: Dict[str, Any] = {"ticker": ticker, "limit": self.pagination_limit}

        if since:
            since_dt = datetime.fromisoformat(since)
            if since_dt.tzinfo is None:
                since_dt = since_dt.replace(tzinfo=timezone.utc)
            params["min_ts"] = int(since_dt.timestamp())

        all_trades: List[Trade] = []
        cursor: Optional[str] = None

        max_pages = 10_000
        page_count = 0
        while True:
            page_count += 1
            if page_count > max_pages:
                logger.error(
                    f"Exceeded {max_pages} pages — possible infinite loop. Stopping."
                )
                break

            if cursor:
                params["cursor"] = cursor

            try:
                data = self._request("GET", "/markets/trades", params=params)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    logger.warning(f"Ticker {ticker}: 404 (delisted/expired). Skipping.")
                    break
                raise

            self.validate_response(data, "trades_list")

            page_trades = data.get("trades") or []
            logger.debug(
                f"Ticker {ticker}: page {page_count}, "
                f"{len(page_trades)} trades (cursor={cursor!r})"
            )

            # NOTE: Assumes trades are returned newest-first (descending created_time).
            # This is Kalshi API behavior as of 2026-03. If order changes, stop_early logic breaks.
            stop_early = False
            for t in page_trades:
                trade = self._parse_trade(t)
                if trade is None:
                    continue
                if since_dt is not None:
                    trade_ts = trade.created_time
                    if trade_ts.tzinfo is None:
                        trade_ts = trade_ts.replace(tzinfo=timezone.utc)
                    if trade_ts < since_dt:
                        stop_early = True
                        break
                all_trades.append(trade)

            cursor = data.get("cursor") or ""
            if not cursor or stop_early or self.dry_run:
                break

        logger.info(f"Ticker {ticker}: collected {len(all_trades)} trades")
        return all_trades

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------

    def _parse_market(self, data: dict) -> Market:
        """Parse Kalshi API market dict into Market dataclass.

        Kalshi returns numeric fields with _dollars (string dollar amount) or
        _fp (string fixed-point) suffixes. We convert:
          yes_bid_dollars / yes_ask_dollars → cents as float (multiply by 100)
          last_price_dollars → cents as float
          volume_fp / open_interest_fp → integer
          notional_value_dollars → float dollars
        Status "finalized" is normalized to "settled" to match internal conventions.
        """

        def _dt(val: Optional[str]) -> Optional[datetime]:
            if not val:
                return None
            try:
                dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
                return dt
            except (ValueError, AttributeError):
                return None

        def _dollars_to_cents(val) -> Optional[float]:
            """Convert dollar string like '0.1900' to cents float like 19.0."""
            if val is None:
                return None
            try:
                return round(float(val) * 100, 4)
            except (ValueError, TypeError):
                return None

        def _fp_to_int(val) -> Optional[int]:
            """Convert fixed-point string like '49.00' to int 49."""
            if val is None:
                return None
            try:
                return int(float(val))
            except (ValueError, TypeError):
                return None

        # Normalize status: Kalshi returns "finalized" for settled markets
        raw_status = data.get("status", "")
        status = "settled" if raw_status == "finalized" else raw_status

        # Derive series_ticker from event_ticker — the API does not return it directly.
        # e.g. event_ticker="KXBTCD-26MAR2623" → series_ticker="KXBTCD"
        event_ticker = data.get("event_ticker", "")
        series_ticker = data.get("series_ticker") or (
            event_ticker.split("-")[0] if event_ticker else ""
        )

        return Market(
            ticker=data.get("ticker", ""),
            event_ticker=event_ticker,
            series_ticker=series_ticker,
            title=data.get("title", ""),
            status=status,
            result=data.get("result"),
            yes_bid=_dollars_to_cents(data.get("yes_bid_dollars")),
            yes_ask=_dollars_to_cents(data.get("yes_ask_dollars")),
            last_price=_dollars_to_cents(data.get("last_price_dollars")),
            open_interest=_fp_to_int(data.get("open_interest_fp")),
            volume=_fp_to_int(data.get("volume_fp")),
            expiration_time=_dt(data.get("expiration_time")),
            close_time=_dt(data.get("close_time")),
            created_time=_dt(data.get("created_time")),
            settled_time=_dt(data.get("settled_time") or data.get("settlement_ts")),
            open_time=_dt(data.get("open_time")),
            notional_value=_dollars_to_cents(data.get("notional_value_dollars")),
            floor_strike=data.get("floor_strike"),
            cap_strike=data.get("cap_strike"),
            raw_data=json.dumps(data),
        )

    def _parse_trade(self, data: dict) -> Optional[Trade]:
        """Parse Kalshi API trade dict into Trade dataclass.

        Kalshi returns yes_price_dollars/no_price_dollars as dollar strings
        (e.g. "0.1900") and count_fp as a fixed-point string (e.g. "49.00").
        We convert to integer cents and integer contract count respectively.

        Returns None if created_time is missing or unparseable (caller must handle).
        """
        ct_str = data.get("created_time", "")
        if not ct_str:
            logger.warning(f"Trade {data.get('trade_id')} has no created_time — skipping")
            return None
        try:
            created_time = datetime.fromisoformat(ct_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            logger.warning(
                f"Trade {data.get('trade_id')} has unparseable created_time {ct_str!r} — skipping"
            )
            return None

        def _dollars_to_cents_int(val) -> int:
            if val is None:
                return 0
            try:
                return int(round(float(val) * 100))
            except (ValueError, TypeError):
                return 0

        def _fp_to_int(val) -> int:
            if val is None:
                return 0
            try:
                return int(float(val))
            except (ValueError, TypeError):
                return 0

        return Trade(
            trade_id=data.get("trade_id", ""),
            ticker=data.get("ticker", ""),
            yes_price=_dollars_to_cents_int(data.get("yes_price_dollars")),
            no_price=_dollars_to_cents_int(data.get("no_price_dollars")),
            count=_fp_to_int(data.get("count_fp")),
            taker_side=data.get("taker_side", ""),
            created_time=created_time,
            raw_data=json.dumps(data),
        )

    # ------------------------------------------------------------------
    # Dry-run diagnostic
    # ------------------------------------------------------------------

    def dry_run_check(self) -> dict:
        """Fetch one page from each series, log schemas, return summary.

        Operates regardless of self.dry_run flag so it can be called
        explicitly as a diagnostic tool.
        """
        summary: Dict[str, Any] = {}
        for series in self.series:
            params: Dict[str, Any] = {
                "series_ticker": series,
                "limit": self.pagination_limit,
            }
            if self.statuses:
                params["status"] = self.statuses[0]
            data = self._request("GET", "/markets", params=params)
            markets = data.get("markets") or []
            sample_keys: List[str] = list(markets[0].keys()) if markets else []
            logger.info(
                f"dry_run_check series={series}: count={len(markets)}, "
                f"sample_keys={sample_keys}"
            )
            summary[series] = {"count": len(markets), "sample_keys": sample_keys}
        return summary

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    def get_configured_series(self) -> List[str]:
        """Return the list of series tickers from config."""
        return self.series
