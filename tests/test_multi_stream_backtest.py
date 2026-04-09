"""Tests for multi-stream backtest: merge_streams and TradeStream."""

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from edge_catcher.runner.event_backtest import (
	TradeStream,
	merge_streams,
	EventBacktester,
	Portfolio,
)
from edge_catcher.runner.strategies import Signal, Strategy
from edge_catcher.fees import STANDARD_FEE, ZERO_FEE
from edge_catcher.research.data_source_resolver import PrimarySource, ResolvedSource


_TEST_SCHEMA = """
CREATE TABLE IF NOT EXISTS markets (
	ticker TEXT PRIMARY KEY,
	event_ticker TEXT DEFAULT '',
	series_ticker TEXT NOT NULL,
	title TEXT DEFAULT '',
	status TEXT DEFAULT 'open',
	result TEXT,
	yes_bid INTEGER DEFAULT 0,
	yes_ask INTEGER DEFAULT 0,
	last_price INTEGER DEFAULT 0,
	open_interest INTEGER DEFAULT 0,
	volume INTEGER DEFAULT 0,
	expiration_time TEXT,
	close_time TEXT,
	created_time TEXT,
	settled_time TEXT,
	open_time TEXT,
	notional_value INTEGER DEFAULT 0,
	floor_strike REAL,
	cap_strike REAL,
	raw_data TEXT DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS trades (
	trade_id TEXT PRIMARY KEY,
	ticker TEXT NOT NULL,
	yes_price INTEGER NOT NULL,
	no_price INTEGER NOT NULL,
	count INTEGER DEFAULT 1,
	taker_side TEXT DEFAULT 'yes',
	created_time TEXT NOT NULL,
	raw_data TEXT DEFAULT '{}'
);
"""


def _dt(offset_hours: float = 0) -> datetime:
	base = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
	return base + timedelta(hours=offset_hours)


def _iso(dt: datetime) -> str:
	return dt.isoformat()


def _make_db(tmp_path: Path, name: str, markets: list[dict], trades: list[dict]) -> Path:
	db_path = tmp_path / name
	conn = sqlite3.connect(str(db_path))
	conn.row_factory = sqlite3.Row
	conn.executescript(_TEST_SCHEMA)
	for m in markets:
		m.setdefault("event_ticker", "")
		m.setdefault("title", "")
		m.setdefault("status", "settled")
		m.setdefault("yes_bid", 0)
		m.setdefault("yes_ask", 0)
		m.setdefault("last_price", 0)
		m.setdefault("open_interest", 0)
		m.setdefault("volume", 0)
		m.setdefault("expiration_time", None)
		m.setdefault("created_time", None)
		m.setdefault("settled_time", None)
		m.setdefault("open_time", _iso(_dt(-24)))
		m.setdefault("notional_value", 0)
		m.setdefault("floor_strike", None)
		m.setdefault("cap_strike", None)
		m.setdefault("raw_data", "{}")
		cols = list(m.keys())
		placeholders = ", ".join(["?"] * len(cols))
		conn.execute(
			f"INSERT INTO markets ({', '.join(cols)}) VALUES ({placeholders})",
			[m[c] for c in cols],
		)
	for t in trades:
		t.setdefault("count", 1)
		t.setdefault("taker_side", "yes")
		t.setdefault("raw_data", "{}")
		cols = list(t.keys())
		placeholders = ", ".join(["?"] * len(cols))
		conn.execute(
			f"INSERT INTO trades ({', '.join(cols)}) VALUES ({placeholders})",
			[t[c] for c in cols],
		)
	conn.commit()
	conn.close()
	return db_path


class BuyAnyYes(Strategy):
	"""Test strategy: buy YES on any trade in range."""
	name = "test-buy-any-yes"

	def on_trade(self, trade, market, portfolio):
		if 60 <= trade.yes_price <= 90 and not portfolio.has_position(trade.ticker, self.name):
			return [Signal(action="buy", ticker=trade.ticker, side="yes",
						  price=trade.yes_price, size=1, reason="test")]
		return []


class TestMergeStreams:
	def test_single_stream_passthrough(self, tmp_path):
		db = _make_db(tmp_path, "a.db",
			markets=[{"ticker": "M-1", "series_ticker": "SA", "close_time": _iso(_dt(5)), "result": "yes"}],
			trades=[
				{"trade_id": "t1", "ticker": "M-1", "yes_price": 70, "no_price": 30, "created_time": _iso(_dt(0))},
				{"trade_id": "t2", "ticker": "M-1", "yes_price": 80, "no_price": 20, "created_time": _iso(_dt(1))},
			],
		)
		streams = [TradeStream(db_path=str(db), series="SA")]
		merged = list(merge_streams(streams))
		assert len(merged) == 2
		assert merged[0][0].trade_id == "t1"
		assert merged[1][0].trade_id == "t2"

	def test_two_streams_interleaved(self, tmp_path):
		db_a = _make_db(tmp_path, "a.db",
			markets=[{"ticker": "A-1", "series_ticker": "SA", "close_time": _iso(_dt(5)), "result": "yes"}],
			trades=[
				{"trade_id": "a1", "ticker": "A-1", "yes_price": 70, "no_price": 30, "created_time": _iso(_dt(0))},
				{"trade_id": "a2", "ticker": "A-1", "yes_price": 80, "no_price": 20, "created_time": _iso(_dt(2))},
			],
		)
		db_b = _make_db(tmp_path, "b.db",
			markets=[{"ticker": "B-1", "series_ticker": "SB", "close_time": _iso(_dt(5)), "result": "no"}],
			trades=[
				{"trade_id": "b1", "ticker": "B-1", "yes_price": 60, "no_price": 40, "created_time": _iso(_dt(1))},
				{"trade_id": "b2", "ticker": "B-1", "yes_price": 50, "no_price": 50, "created_time": _iso(_dt(3))},
			],
		)
		streams = [
			TradeStream(db_path=str(db_a), series="SA"),
			TradeStream(db_path=str(db_b), series="SB"),
		]
		merged = list(merge_streams(streams))
		ids = [t[0].trade_id for t in merged]
		assert ids == ["a1", "b1", "a2", "b2"]

	def test_deterministic_tiebreaker(self, tmp_path):
		db_a = _make_db(tmp_path, "a.db",
			markets=[{"ticker": "A-1", "series_ticker": "SA", "close_time": _iso(_dt(5)), "result": "yes"}],
			trades=[
				{"trade_id": "a1", "ticker": "A-1", "yes_price": 70, "no_price": 30, "created_time": _iso(_dt(0))},
			],
		)
		db_b = _make_db(tmp_path, "b.db",
			markets=[{"ticker": "B-1", "series_ticker": "SB", "close_time": _iso(_dt(5)), "result": "yes"}],
			trades=[
				{"trade_id": "b1", "ticker": "B-1", "yes_price": 60, "no_price": 40, "created_time": _iso(_dt(0))},
			],
		)
		streams = [
			TradeStream(db_path=str(db_a), series="SA"),
			TradeStream(db_path=str(db_b), series="SB"),
		]
		merged = list(merge_streams(streams))
		ids = [t[0].trade_id for t in merged]
		assert ids == ["a1", "b1"]


class TestMultiPrimaryBacktest:
	def test_trades_from_both_dbs(self, tmp_path):
		db_a = _make_db(tmp_path, "a.db",
			markets=[{"ticker": "M-1", "series_ticker": "SA",
					  "close_time": _iso(_dt(5)), "result": "yes"}],
			trades=[
				{"trade_id": "a1", "ticker": "M-1", "yes_price": 75, "no_price": 25,
				 "created_time": _iso(_dt(0))},
			],
		)
		db_b = _make_db(tmp_path, "b.db",
			markets=[{"ticker": "N-1", "series_ticker": "SB",
					  "close_time": _iso(_dt(5)), "result": "yes"}],
			trades=[
				{"trade_id": "b1", "ticker": "N-1", "yes_price": 80, "no_price": 20,
				 "created_time": _iso(_dt(1))},
			],
		)
		resolved = ResolvedSource(
			primaries=[
				PrimarySource(db_path=str(db_a), series="SA", fee_model=STANDARD_FEE),
				PrimarySource(db_path=str(db_b), series="SB", fee_model=ZERO_FEE),
			],
			ohlc_config={},
		)
		result = EventBacktester().run_multi(
			resolved=resolved,
			strategies=[BuyAnyYes()],
			initial_cash=10000.0,
			slippage_cents=0,
		)
		assert result.total_trades == 2

	def test_fee_model_per_source(self, tmp_path):
		db_a = _make_db(tmp_path, "a.db",
			markets=[{"ticker": "M-1", "series_ticker": "SA",
					  "close_time": _iso(_dt(5)), "result": "yes"}],
			trades=[
				{"trade_id": "a1", "ticker": "M-1", "yes_price": 75, "no_price": 25,
				 "created_time": _iso(_dt(0))},
			],
		)
		db_b = _make_db(tmp_path, "b.db",
			markets=[{"ticker": "N-1", "series_ticker": "SB",
					  "close_time": _iso(_dt(5)), "result": "yes"}],
			trades=[
				{"trade_id": "b1", "ticker": "N-1", "yes_price": 80, "no_price": 20,
				 "created_time": _iso(_dt(1))},
			],
		)
		resolved = ResolvedSource(
			primaries=[
				PrimarySource(db_path=str(db_a), series="SA", fee_model=STANDARD_FEE),
				PrimarySource(db_path=str(db_b), series="SB", fee_model=ZERO_FEE),
			],
			ohlc_config={},
		)
		result = EventBacktester().run_multi(
			resolved=resolved,
			strategies=[BuyAnyYes()],
			initial_cash=10000.0,
			slippage_cents=0,
		)
		zero_fee_trade = [t for t in result.trade_sample if t.ticker == "N-1"]
		assert len(zero_fee_trade) == 1
		assert zero_fee_trade[0].fee_cents == 0.0
		std_fee_trade = [t for t in result.trade_sample if t.ticker == "M-1"]
		assert len(std_fee_trade) == 1
		assert std_fee_trade[0].fee_cents > 0
