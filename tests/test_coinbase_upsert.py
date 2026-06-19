import sqlite3

import pytest

from edge_catcher.adapters.coinbase.adapter import (
	CoinbaseAdapter, valid_candle_row, _candle_to_row,
)
from edge_catcher.storage.db import init_ohlc_table


def _raw(start, close, vol=1.0):
	return {"start": str(start), "open": close, "high": close,
		"low": close, "close": close, "volume": vol}


def test_candle_to_row_parses_types():
	row = _candle_to_row(_raw(1781895360, 1700.42, 38.2))
	assert row == (1781895360, 1700.42, 1700.42, 1700.42, 1700.42, 38.2)
	assert isinstance(row[0], int) and isinstance(row[4], float)


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), -float("inf"), 0.0, -5.0, 1e12])
def test_valid_candle_row_rejects_nonfinite_and_out_of_band(bad):
	row = (1781895360, bad, bad, bad, bad, 1.0)
	assert valid_candle_row(row) is False


def test_valid_candle_row_accepts_normal():
	assert valid_candle_row((1781895360, 1700.0, 1701.0, 1699.0, 1700.5, 12.3)) is True


def test_valid_candle_row_rejects_negative_volume():
	assert valid_candle_row((1781895360, 1700.0, 1700.0, 1700.0, 1700.0, -1.0)) is False


def _conn():
	c = sqlite3.connect(":memory:")
	init_ohlc_table(c, "eth_ohlc")
	return c


def test_upsert_replaces_forming_bar():
	c = _conn()
	a = CoinbaseAdapter("ETH-USD")
	assert a.upsert_candles([_candle_to_row(_raw(60, 100.0, 1.0))], c) == 1
	assert a.upsert_candles([_candle_to_row(_raw(60, 105.0, 9.0))], c) == 1
	rows = c.execute("SELECT timestamp, close, volume FROM eth_ohlc").fetchall()
	assert rows == [(60, 105.0, 9.0)]


def test_download_range_ignore_keeps_history_immutable():
	c = _conn()
	c.execute("INSERT INTO eth_ohlc VALUES (60, 100, 100, 100, 100, 1)")
	c.commit()
	c.execute("INSERT OR IGNORE INTO eth_ohlc (timestamp, open, high, low, close, volume) "
		"VALUES (60, 200, 200, 200, 200, 2)")
	c.commit()
	assert c.execute("SELECT close FROM eth_ohlc WHERE timestamp=60").fetchone()[0] == 100


def test_upsert_skips_invalid_rows_and_counts_only_written():
	c = _conn()
	a = CoinbaseAdapter("ETH-USD")
	rows = [
		_candle_to_row(_raw(60, 100.0)),
		(120, float("inf"), float("inf"), float("inf"), float("inf"), 1.0),
	]
	written = a.upsert_candles(rows, c)
	assert written == 1
	assert c.execute("SELECT COUNT(*) FROM eth_ohlc").fetchone()[0] == 1
