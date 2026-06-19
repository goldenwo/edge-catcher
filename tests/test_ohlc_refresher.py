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


def test_reader_sees_writer_commit_without_reopen(tmp_path):
	import datetime as _dt
	from pathlib import Path
	from edge_catcher.research.ohlc_provider import OHLCProvider
	from edge_catcher.storage.db import get_connection

	db = str(tmp_path / "ohlc.db")
	w = get_connection(Path(db))          # WAL writer (PRAGMA journal_mode=WAL)
	init_ohlc_table(w, "eth_ohlc")
	w.commit()
	prov = OHLCProvider({"eth": (db, "eth_ohlc")})
	t = _dt.datetime(2026, 6, 19, 0, 1, 0, tzinfo=_dt.timezone.utc)
	assert prov.get_candle("eth", t) is None          # establishes reader connection
	w.execute("INSERT OR REPLACE INTO eth_ohlc VALUES (?,?,?,?,?,?)",
		(int(t.timestamp()), 1, 1, 1, 1, 1))
	w.commit()
	got = prov.get_candle("eth", t)                    # SAME reader conn must see the commit
	assert got is not None and got.timestamp == int(t.timestamp())
	prov.close()
	w.close()


def test_cli_builds_config_from_yaml(tmp_path):
	import yaml
	from edge_catcher.cli import ohlc_refresh as cli_refresh
	cfg_file = tmp_path / "c.yaml"
	cfg_file.write_text(yaml.safe_dump({"ohlc_refresh": {
		"enabled": True, "db_path": "data/ohlc.db",
		"products": ["ETH-USD", "SOL-USD", "DOGE-USD"]}}), encoding="utf-8")
	cfg = cli_refresh._load_config(str(cfg_file))
	assert cfg is not None and cfg.products == ["ETH-USD", "SOL-USD", "DOGE-USD"]
	assert cfg.poll_interval_s == 20


def test_cli_load_config_disabled_returns_none(tmp_path):
	import yaml
	from edge_catcher.cli import ohlc_refresh as cli_refresh
	cfg_file = tmp_path / "c.yaml"
	cfg_file.write_text(yaml.safe_dump({"ohlc_refresh": {"enabled": False}}), encoding="utf-8")
	assert cli_refresh._load_config(str(cfg_file)) is None


def test_refresh_once_future_bar_not_counted_as_fresh(caplog):
	import logging
	now = 1781895455  # minute 1781895420
	# Coinbase returns a bar dated in the FUTURE relative to local now (clock-skew simulation)
	fa = FakeAdapter("ETH-USD", [_raw(1781895360, 100.0), _raw(1781895480, 101.0)])  # 1781895480 > now
	conn = _conn()
	with caplog.at_level(logging.WARNING):
		freshness = refresh_once(fa, conn, now, _cfg())
	# The future bar (1781895480) must NOT be returned as freshness; only bars <= now count.
	# With carry_forward=True and now=1781895455 (35s into cur_min=1781895420),
	# the future bar satisfies timestamp==cur_min? No: 1781895480 != 1781895420.
	# So carry-forward MAY fire (1781895420 absent) and write synth bar @ 1781895420.
	# Either way freshness must be <= now.
	assert freshness is not None
	assert freshness <= now
	assert any("clock-skew" in r.message for r in caplog.records)


def test_from_yaml_missing_key_raises_clear_error():
	import pytest
	with pytest.raises(ValueError, match="db_path"):
		RefreshConfig.from_yaml({"ohlc_refresh": {"enabled": True, "products": ["ETH-USD"]}})
