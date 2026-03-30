"""Coinbase Advanced Trade public API adapter for BTC-USD 1-minute OHLC."""

import logging
import time

import requests

logger = logging.getLogger(__name__)


class CoinbaseAdapter:
    BASE_URL = "https://api.coinbase.com/api/v3/brokerage/market/products/BTC-USD/candles"
    GRANULARITY = "ONE_MINUTE"
    PAGE_SIZE = 350
    RATE_LIMIT_SLEEP = 0.4

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "edge-catcher/1.0"})

    def fetch_candles(self, start_ts: int, end_ts: int) -> list[dict]:
        """Fetch one page of candles. Returns list of raw candle dicts."""
        params = {
            "start": str(start_ts),
            "end": str(end_ts),
            "granularity": self.GRANULARITY,
            "limit": str(self.PAGE_SIZE),
        }
        resp = self.session.get(self.BASE_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get("candles", [])

    def download_range(self, start_ts: int, end_ts: int, conn, progress_callback=None) -> int:
        """
        Download all candles in [start_ts, end_ts], insert into btc_ohlc table.
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
                    "INSERT OR IGNORE INTO btc_ohlc (timestamp, open, high, low, close, volume) "
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
            time.sleep(self.RATE_LIMIT_SLEEP)

        return total_inserted
