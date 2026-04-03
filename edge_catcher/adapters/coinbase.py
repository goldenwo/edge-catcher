"""Coinbase Advanced Trade public API adapter for 1-minute OHLC (any product)."""

import logging
import threading
import time

import requests

logger = logging.getLogger(__name__)

# Shared rate limiter: Coinbase allows 10 req/s for public endpoints.
# A global lock ensures concurrent adapters don't exceed the limit.
_rate_lock = threading.Lock()
_last_request_time = 0.0
_MIN_REQUEST_INTERVAL = 0.15  # ~6.6 req/s max across all adapters (safe margin)


def _rate_limit():
    """Block until enough time has passed since the last Coinbase request."""
    global _last_request_time
    with _rate_lock:
        now = time.monotonic()
        wait = _MIN_REQUEST_INTERVAL - (now - _last_request_time)
        if wait > 0:
            time.sleep(wait)
        _last_request_time = time.monotonic()


class CoinbaseAdapter:
    BASE_URL_TEMPLATE = "https://api.coinbase.com/api/v3/brokerage/market/products/{product_id}/candles"
    GRANULARITY = "ONE_MINUTE"
    PAGE_SIZE = 350

    def __init__(self, product_id: str = "BTC-USD"):
        self.product_id = product_id
        self.table_name = product_id.split("-")[0].lower() + "_ohlc"
        self.base_url = self.BASE_URL_TEMPLATE.format(product_id=product_id)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "edge-catcher/1.0"})

    def fetch_candles(self, start_ts: int, end_ts: int) -> list[dict]:
        """Fetch one page of candles. Returns list of raw candle dicts."""
        _rate_limit()
        params = {
            "start": str(start_ts),
            "end": str(end_ts),
            "granularity": self.GRANULARITY,
            "limit": str(self.PAGE_SIZE),
        }
        resp = self.session.get(self.base_url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get("candles", [])

    def download_range(self, start_ts: int, end_ts: int, conn, progress_callback=None) -> int:
        """
        Download all candles in [start_ts, end_ts], insert into self.table_name.
        Skips rows already in DB (INSERT OR IGNORE).
        Returns count of new rows inserted.

        progress_callback(pct: int, page: int, total_pages: int, total_inserted: int)
        is called after each page if provided.
        """
        import math
        window = self.PAGE_SIZE * 60  # seconds covered per page
        total_pages = math.ceil((end_ts - start_ts) / window)
        total_inserted = 0
        page = 0
        cursor = start_ts

        while cursor < end_ts:
            window_end = min(cursor + window, end_ts)
            candles = self.fetch_candles(cursor, window_end)

            rows = [
                (
                    int(c["start"]),
                    float(c["open"]),
                    float(c["high"]),
                    float(c["low"]),
                    float(c["close"]),
                    float(c["volume"]),
                )
                for c in candles
            ]

            if rows:
                conn.executemany(
                    f"INSERT OR IGNORE INTO {self.table_name} (timestamp, open, high, low, close, volume) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    rows,
                )
                inserted = conn.execute("SELECT changes()").fetchone()[0]
                total_inserted += inserted
                conn.commit()

            page += 1
            pct = min(100, round(page / total_pages * 100))
            if progress_callback:
                progress_callback(pct, page, total_pages, total_inserted)
            elif page % 50 == 0:
                logger.info(
                    "download_range: page %d/%d (%d%%), total_inserted=%d",
                    page, total_pages, pct, total_inserted,
                )

            cursor = window_end

        return total_inserted
