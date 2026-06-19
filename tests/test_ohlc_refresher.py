import sqlite3

from edge_catcher.adapters.coinbase.adapter import valid_candle_row
from edge_catcher.live.ohlc_refresher import (
	RefreshConfig, refresh_once, backfill, staleness_age, should_warn_staleness,
)
from edge_catcher.storage.db import init_ohlc_table


class FakeAdapter:
	def __init__(self, product_id, candles):
		self.product_id = product_id
		self.table_name = product_id.split("-")[0].lower() + "_ohlc"
		self._candles = candles
		self.fetch_calls = []

	def fetch_candles(self, start_ts, end_ts):
		self.fetch_calls.append((start_ts, end_ts))
		return [c for c in self._candles if start_ts <= int(c["start"]) <= end_ts]

	def upsert_candles(self, rows, conn):
		good = [r for r in rows if valid_candle_row(r)]
		conn.executemany(
			f"INSERT OR REPLACE INTO {self.table_name} (timestamp, open, high, low, close, volume) "
			"VALUES (?,?,?,?,?,?)", good)
		conn.commit()
		return len(good)


def _raw(start, close, vol=1.0):
	return {"start": str(start), "open": close, "high": close, "low": close,
		"close": close, "volume": vol}


def _conn():
	c = sqlite3.connect(":memory:")
	init_ohlc_table(c, "eth_ohlc")
	return c


def _cfg(**kw):
	base = dict(db_path=":memory:", products=["ETH-USD"], poll_interval_s=20,
		staleness_warn_s=75, startup_lookback_s=7200, carry_forward=True)
	base.update(kw)
	return RefreshConfig(**base)


def test_refresh_once_writes_forming_bar_and_reports_freshness():
	now = 1781895455  # 35s into minute 1781895420
	fa = FakeAdapter("ETH-USD", [_raw(1781895360, 100.0), _raw(1781895420, 105.0, 3.5)])
	conn = _conn()
	freshness = refresh_once(fa, conn, now, _cfg())
	assert conn.execute("SELECT close FROM eth_ohlc WHERE timestamp=1781895420").fetchone()[0] == 105.0
	assert freshness == 1781895420


def test_refresh_once_carry_forward_when_forming_absent_late_in_minute():
	now = 1781895450  # 30s into minute 1781895420, no bar yet
	fa = FakeAdapter("ETH-USD", [_raw(1781895360, 100.0)])
	conn = _conn()
	refresh_once(fa, conn, now, _cfg())
	row = conn.execute("SELECT close, volume FROM eth_ohlc WHERE timestamp=1781895420").fetchone()
	assert row == (100.0, 0.0)


def test_refresh_once_no_carry_forward_early_in_minute():
	now = 1781895425  # 5s in; previous bar still fresh, do not synthesize
	fa = FakeAdapter("ETH-USD", [_raw(1781895360, 100.0)])
	conn = _conn()
	refresh_once(fa, conn, now, _cfg())
	assert conn.execute("SELECT COUNT(*) FROM eth_ohlc WHERE timestamp=1781895420").fetchone()[0] == 0


def test_refresh_once_carry_forward_disabled():
	now = 1781895450
	fa = FakeAdapter("ETH-USD", [_raw(1781895360, 100.0)])
	conn = _conn()
	refresh_once(fa, conn, now, _cfg(carry_forward=False))
	assert conn.execute("SELECT COUNT(*) FROM eth_ohlc WHERE timestamp=1781895420").fetchone()[0] == 0


def test_backfill_from_last_timestamp():
	now = 1781895455
	conn = _conn()
	conn.execute("INSERT INTO eth_ohlc VALUES (1781895000, 1,1,1,1,1)")
	conn.commit()
	fetched = {}

	class CaptureAdapter(FakeAdapter):
		def fetch_candles(self, start_ts, end_ts):
			fetched["start"] = start_ts
			return [_raw(1781895060, 2.0), _raw(1781895120, 3.0)]

	backfill(CaptureAdapter("ETH-USD", []), conn, now, lookback_s=7200)
	assert fetched["start"] == 1781895000
	assert conn.execute("SELECT COUNT(*) FROM eth_ohlc").fetchone()[0] == 3


def test_backfill_empty_table_uses_lookback():
	now = 1781895455
	conn = _conn()
	fetched = {}

	class CaptureAdapter(FakeAdapter):
		def fetch_candles(self, start_ts, end_ts):
			fetched["start"] = start_ts
			return []

	backfill(CaptureAdapter("ETH-USD", []), conn, now, lookback_s=7200)
	assert fetched["start"] == now - 7200


def test_staleness_age_and_warn():
	now = 1781895500
	assert staleness_age(1781895480, now) == 20
	assert should_warn_staleness(1781895480, now, warn_s=75) is False
	assert should_warn_staleness(1781895400, now, warn_s=75) is True
	assert should_warn_staleness(None, now, warn_s=75) is True
