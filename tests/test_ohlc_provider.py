"""Tests for OHLCProvider."""

import sqlite3
from datetime import datetime, timezone

import pytest


@pytest.fixture
def ohlc_db(tmp_path):
    """Create a test OHLC database with a btc_ohlc table."""
    db_path = str(tmp_path / "ohlc.db")
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE btc_ohlc (
            timestamp INTEGER, open REAL, high REAL,
            low REAL, close REAL, volume REAL
        )
    """)
    # Insert 10 candles, 60 seconds apart, starting at 2025-01-01 00:00 UTC
    base_ts = 1735689600  # 2025-01-01 00:00:00 UTC
    for i in range(10):
        ts = base_ts + i * 60
        conn.execute(
            "INSERT INTO btc_ohlc VALUES (?, ?, ?, ?, ?, ?)",
            (ts, 100.0 + i, 101.0 + i, 99.0 + i, 100.5 + i, 1.0),
        )
    conn.commit()
    conn.close()
    return db_path


class TestOHLCProvider:
    def test_get_price_returns_latest_close(self, ohlc_db):
        from edge_catcher.research.ohlc_provider import OHLCProvider

        provider = OHLCProvider({"btc": (ohlc_db, "btc_ohlc")})
        try:
            # Query at a time after the 5th candle
            ts = datetime(2025, 1, 1, 0, 4, 30, tzinfo=timezone.utc)
            price = provider.get_price("btc", ts)
            # Should return close of candle at :04:00 = 100.5 + 4 = 104.5
            assert price == pytest.approx(104.5)
        finally:
            provider.close()

    def test_get_price_unknown_asset(self, ohlc_db):
        from edge_catcher.research.ohlc_provider import OHLCProvider

        provider = OHLCProvider({"btc": (ohlc_db, "btc_ohlc")})
        try:
            ts = datetime(2025, 1, 1, 0, 5, 0, tzinfo=timezone.utc)
            assert provider.get_price("eth", ts) is None
        finally:
            provider.close()

    def test_get_candle_returns_ohlc(self, ohlc_db):
        from edge_catcher.research.ohlc_provider import OHLCProvider

        provider = OHLCProvider({"btc": (ohlc_db, "btc_ohlc")})
        try:
            ts = datetime(2025, 1, 1, 0, 3, 0, tzinfo=timezone.utc)
            candle = provider.get_candle("btc", ts)
            assert candle is not None
            assert candle.close == pytest.approx(103.5)
            assert candle.open == pytest.approx(103.0)
        finally:
            provider.close()

    def test_get_recent_returns_n_candles(self, ohlc_db):
        from edge_catcher.research.ohlc_provider import OHLCProvider

        provider = OHLCProvider({"btc": (ohlc_db, "btc_ohlc")})
        try:
            ts = datetime(2025, 1, 1, 0, 9, 0, tzinfo=timezone.utc)
            candles = provider.get_recent("btc", ts, n_candles=5)
            assert len(candles) == 5
            # Should be in chronological order (oldest first)
            assert candles[0].timestamp < candles[-1].timestamp
        finally:
            provider.close()

    def test_lazy_connection(self, ohlc_db):
        """Connections should not open until first query."""
        from edge_catcher.research.ohlc_provider import OHLCProvider

        provider = OHLCProvider({"btc": (ohlc_db, "btc_ohlc")})
        assert len(provider._connections) == 0
        ts = datetime(2025, 1, 1, 0, 5, 0, tzinfo=timezone.utc)
        provider.get_price("btc", ts)
        assert "btc" in provider._connections
        provider.close()
