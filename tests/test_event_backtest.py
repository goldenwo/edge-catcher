"""Tests for event-driven backtester: strategies, portfolio, and integration."""

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from edge_catcher.runner.event_backtest import (
	BacktestResult,
	CompletedTrade,
	EventBacktester,
	Portfolio,
)
from edge_catcher.adapters.kalshi.fees import STANDARD_FEE
from edge_catcher.runner.strategies import Signal, Strategy
from edge_catcher.storage.models import Market, Trade


# ---------------------------------------------------------------------------
# Test-only strategy stubs (originals moved to gitignored strategies_local.py)
# ---------------------------------------------------------------------------

class BuyYesInRange(Strategy):
	"""Buy YES when yes_price is in [min_price, max_price]."""
	name = 'test-buy-yes-range'

	def __init__(self, min_price: int = 70, max_price: int = 99, size: int = 1,
	             take_profit=None, stop_loss=None) -> None:
		self.min_price = min_price
		self.max_price = max_price
		self.size = size
		self.take_profit = take_profit
		self.stop_loss = stop_loss

	def on_trade(self, trade, market, portfolio):
		if self.take_profit is not None and portfolio.has_position(trade.ticker, self.name):
			pos = portfolio.positions.get((trade.ticker, self.name))
			if pos is not None:
				if self.take_profit is not None and trade.yes_price >= pos.entry_price + self.take_profit:
					return [Signal(action='sell', ticker=trade.ticker, side=pos.side, price=trade.yes_price,
						size=pos.size, reason=f'take_profit: {trade.yes_price}>={pos.entry_price}+{self.take_profit}')]
				if self.stop_loss is not None and trade.yes_price <= pos.entry_price - self.stop_loss:
					return [Signal(action='sell', ticker=trade.ticker, side=pos.side, price=trade.yes_price,
						size=pos.size, reason=f'stop_loss: {trade.yes_price}<={pos.entry_price}-{self.stop_loss}')]
			return []
		if (self.min_price <= trade.yes_price <= self.max_price
				and not portfolio.has_position(trade.ticker, self.name)):
			return [Signal(action='buy', ticker=trade.ticker, side='yes', price=trade.yes_price,
				size=self.size, reason=f'yes_price={trade.yes_price} in [{self.min_price},{self.max_price}]')]
		return []


class BuyNoOnDrop(Strategy):
	"""Contrarian NO — buy NO when yes_price drops >= threshold."""
	name = 'test-buy-no-drop'

	def __init__(self, min_price: int = 50, max_price: int = 80, drop_threshold: int = 5, size: int = 1) -> None:
		self.min_price = min_price
		self.max_price = max_price
		self.drop_threshold = drop_threshold
		self.size = size
		self._last_known_price: dict[str, int] = {}

	def on_trade(self, trade, market, portfolio):
		signals: list[Signal] = []
		prev = self._last_known_price.get(trade.ticker)
		if (prev is not None
				and self.min_price <= trade.yes_price <= self.max_price
				and (prev - trade.yes_price) >= self.drop_threshold
				and not portfolio.has_position(trade.ticker, self.name)):
			no_price = 100 - trade.yes_price
			signals.append(Signal(action='buy', ticker=trade.ticker, side='no', price=no_price,
				size=self.size, reason=f'yes_price dropped {prev}->{trade.yes_price}'))
		self._last_known_price[trade.ticker] = trade.yes_price
		return signals


class BuyNoInRange(Strategy):
	"""Test stub — buy NO when yes_price in [min, max]."""
	name = 'test-buy-no-range'

	def __init__(self, min_price: int = 5, max_price: int = 30, size: int = 1,
	             take_profit=None, stop_loss=None) -> None:
		self.min_price = min_price
		self.max_price = max_price
		self.size = size
		self.take_profit = take_profit
		self.stop_loss = stop_loss

	def on_trade(self, trade, market, portfolio):
		if self.take_profit is not None and portfolio.has_position(trade.ticker, self.name):
			pos = portfolio.positions.get((trade.ticker, self.name))
			if pos is not None:
				current_no_price = 100 - trade.yes_price
				if self.take_profit is not None and current_no_price >= pos.entry_price + self.take_profit:
					return [Signal(
						action='sell', ticker=trade.ticker, side=pos.side, price=current_no_price,
						size=pos.size,
						reason=f'take_profit: no={current_no_price}>={pos.entry_price}+{self.take_profit}',
					)]
				if self.stop_loss is not None and current_no_price <= pos.entry_price - self.stop_loss:
					return [Signal(action='sell', ticker=trade.ticker, side=pos.side, price=current_no_price,
						size=pos.size, reason=f'stop_loss: no={current_no_price}<={pos.entry_price}-{self.stop_loss}')]
			return []
		if (self.min_price <= trade.yes_price <= self.max_price
				and not portfolio.has_position(trade.ticker, self.name)):
			no_price = 100 - trade.yes_price
			return [Signal(action='buy', ticker=trade.ticker, side='no', price=no_price,
				size=self.size, reason=f'yes_price={trade.yes_price} in range [{self.min_price},{self.max_price}]')]
		return []


class ActiveExitStub(Strategy):
	"""Active exit — buy YES in range, exit on TP/SL."""
	name = 'test-active-exit'

	def __init__(self, min_price: int = 40, max_price: int = 60, take_profit: int = 8,
	             stop_loss: int = 5, size: int = 1) -> None:
		self.min_price = min_price
		self.max_price = max_price
		self.take_profit = take_profit
		self.stop_loss = stop_loss
		self.size = size

	def on_trade(self, trade, market, portfolio):
		if portfolio.has_position(trade.ticker, self.name):
			pos = portfolio.positions.get((trade.ticker, self.name))
			if pos is not None:
				if trade.yes_price >= pos.entry_price + self.take_profit:
					return [Signal(action='sell', ticker=trade.ticker, side=pos.side, price=trade.yes_price,
						size=pos.size, reason='take_profit')]
				if trade.yes_price <= pos.entry_price - self.stop_loss:
					return [Signal(action='sell', ticker=trade.ticker, side=pos.side, price=trade.yes_price,
						size=pos.size, reason='stop_loss')]
			return []
		if self.min_price <= trade.yes_price <= self.max_price:
			return [Signal(action='buy', ticker=trade.ticker, side='yes', price=trade.yes_price,
				size=self.size, reason=f'yes_price={trade.yes_price} in [{self.min_price},{self.max_price}]')]
		return []


class FirstTradeEntry(Strategy):
	"""Test stub — act on first trade per market when extreme."""
	name = 'test-first-trade'

	def __init__(self, threshold_high: int = 60, threshold_low: int = 40,
	             take_profit: int = 8, stop_loss: int = 5, size: int = 1) -> None:
		self.threshold_high = threshold_high
		self.threshold_low = threshold_low
		self.take_profit = take_profit
		self.stop_loss = stop_loss
		self.size = size
		self._seen_tickers: dict[str, bool] = {}

	def on_trade(self, trade, market, portfolio):
		if portfolio.has_position(trade.ticker, self.name):
			pos = portfolio.positions.get((trade.ticker, self.name))
			if pos is not None:
				check_price = trade.yes_price if pos.side == 'yes' else 100 - trade.yes_price
				if check_price >= pos.entry_price + self.take_profit:
					return [Signal(action='sell', ticker=trade.ticker, side=pos.side, price=check_price,
						size=pos.size, reason='take_profit')]
				if check_price <= pos.entry_price - self.stop_loss:
					return [Signal(action='sell', ticker=trade.ticker, side=pos.side, price=check_price,
						size=pos.size, reason='stop_loss')]
			return []
		if trade.ticker in self._seen_tickers:
			return []
		self._seen_tickers[trade.ticker] = True
		if trade.yes_price > self.threshold_high:
			no_price = 100 - trade.yes_price
			return [Signal(action='buy', ticker=trade.ticker, side='no', price=no_price,
				size=self.size, reason=f'first-trade high: yes_price={trade.yes_price} > {self.threshold_high}')]
		if trade.yes_price < self.threshold_low:
			return [Signal(action='buy', ticker=trade.ticker, side='yes', price=trade.yes_price,
				size=self.size, reason=f'first-trade low: yes_price={trade.yes_price} < {self.threshold_low}')]
		return []


class DualThreshold(Strategy):
	"""Test stub — two-sided threshold entry."""
	name = 'test-dual-threshold'

	def __init__(self, fav_threshold: int = 85, long_threshold: int = 15, size: int = 1) -> None:
		self.fav_threshold = fav_threshold
		self.long_threshold = long_threshold
		self.size = size

	def on_trade(self, trade, market, portfolio):
		if portfolio.has_position(trade.ticker, self.name):
			return []
		if trade.yes_price >= self.fav_threshold:
			return [Signal(action='buy', ticker=trade.ticker, side='no', price=100 - trade.yes_price,
				size=self.size, reason=f'high threshold: yes_price={trade.yes_price} >= {self.fav_threshold}')]
		if trade.yes_price <= self.long_threshold:
			return [Signal(action='buy', ticker=trade.ticker, side='yes', price=trade.yes_price,
				size=self.size, reason=f'low threshold: yes_price={trade.yes_price} <= {self.long_threshold}')]
		return []


# ---------------------------------------------------------------------------
# Schema for test databases
# ---------------------------------------------------------------------------

_TEST_SCHEMA = """
CREATE TABLE IF NOT EXISTS markets (
    ticker TEXT PRIMARY KEY,
    event_ticker TEXT,
    series_ticker TEXT,
    title TEXT,
    status TEXT,
    result TEXT,
    yes_bid REAL,
    yes_ask REAL,
    last_price REAL,
    open_interest INTEGER,
    volume INTEGER,
    expiration_time TEXT,
    close_time TEXT,
    created_time TEXT,
    settled_time TEXT,
    open_time TEXT,
    notional_value REAL,
    floor_strike REAL,
    cap_strike REAL,
    raw_data TEXT,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS trades (
    trade_id TEXT PRIMARY KEY,
    ticker TEXT NOT NULL,
    yes_price INTEGER NOT NULL,
    no_price INTEGER NOT NULL,
    count INTEGER NOT NULL,
    taker_side TEXT NOT NULL,
    created_time TEXT NOT NULL,
    raw_data TEXT
);
"""


def _dt(offset_hours: float = 0) -> datetime:
	"""UTC datetime anchored at a fixed base, shifted by offset_hours."""
	base = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
	return base + timedelta(hours=offset_hours)


def _iso(dt: datetime) -> str:
	return dt.isoformat()


def _make_db(tmp_path: Path, markets: list[dict], trades: list[dict]) -> Path:
	"""Create a test SQLite DB with given markets and trades. Returns db path."""
	db_path = tmp_path / "test_backtest.db"
	conn = sqlite3.connect(str(db_path))
	conn.row_factory = sqlite3.Row
	conn.executescript(_TEST_SCHEMA)

	for m in markets:
		conn.execute(
			"""INSERT INTO markets
			(ticker, event_ticker, series_ticker, title, status, result,
			 yes_bid, yes_ask, last_price, open_interest, volume,
			 expiration_time, close_time, created_time, settled_time, open_time,
			 notional_value, floor_strike, cap_strike, raw_data)
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
			(
				m['ticker'],
				m.get('event_ticker', 'EV'),
				m.get('series_ticker', 'TESTSERIES'),
				m.get('title', 'Test Market'),
				m.get('status', 'settled'),
				m.get('result'),
				m.get('yes_bid'),
				m.get('yes_ask'),
				m.get('last_price'),
				m.get('open_interest'),
				m.get('volume', 100),
				m.get('expiration_time'),
				m.get('close_time'),
				m.get('created_time', _iso(_dt(-2))),
				m.get('settled_time'),
				m.get('open_time', _iso(_dt(-24))),
				m.get('notional_value'),
				m.get('floor_strike'),
				m.get('cap_strike'),
				None,
			),
		)

	for t in trades:
		conn.execute(
			"""INSERT INTO trades
			(trade_id, ticker, yes_price, no_price, count, taker_side, created_time, raw_data)
			VALUES (?,?,?,?,?,?,?,?)""",
			(
				t['trade_id'],
				t['ticker'],
				t['yes_price'],
				t.get('no_price', 100 - t['yes_price']),
				t.get('count', 1),
				t.get('taker_side', 'yes'),
				t['created_time'],
				None,
			),
		)

	conn.commit()
	conn.close()
	return db_path


def _make_market(ticker: str = 'TEST-1', result: str = 'yes') -> Market:
	"""Build a Market dataclass for unit tests (no DB)."""
	return Market(
		ticker=ticker,
		event_ticker='EV',
		series_ticker='TESTSERIES',
		title='Test',
		status='settled',
		result=result,
		yes_bid=None, yes_ask=None, last_price=None,
		open_interest=None, volume=100,
		expiration_time=None,
		close_time=_dt(1),
		created_time=_dt(-24),
		settled_time=_dt(2),
		open_time=_dt(-24),
		notional_value=None, floor_strike=None, cap_strike=None,
	)


def _make_trade(ticker: str = 'TEST-1', yes_price: int = 80, offset_hours: float = 0) -> Trade:
	"""Build a Trade dataclass for unit tests (no DB)."""
	return Trade(
		trade_id=f'tr-{ticker}-{yes_price}-{offset_hours}',
		ticker=ticker,
		yes_price=yes_price,
		no_price=100 - yes_price,
		count=1,
		taker_side='yes',
		created_time=_dt(offset_hours),
	)


# ---------------------------------------------------------------------------
# Portfolio unit tests
# ---------------------------------------------------------------------------

class TestPortfolio:
	def test_open_position_deducts_cash(self):
		port = Portfolio(1000.0, fee_fn=lambda p, s: 0.0)
		sig = Signal(action='buy', ticker='T', side='yes', price=50, size=1, reason='test')
		result = port.open_position(sig, 'test-buy-yes-range', _dt(), slippage=0)
		assert result is True
		assert port.cash == 950.0
		assert port.has_position('T', 'test-buy-yes-range')

	def test_open_position_slippage_increases_cost(self):
		port = Portfolio(1000.0, fee_fn=lambda p, s: 0.0)
		sig = Signal(action='buy', ticker='T', side='yes', price=50, size=1, reason='test')
		port.open_position(sig, 'test-buy-yes-range', _dt(), slippage=2)
		assert port.cash == 948.0
		assert port.positions[('T', 'test-buy-yes-range')].entry_price == 52

	def test_open_position_returns_false_when_insufficient_cash(self):
		port = Portfolio(10.0)
		sig = Signal(action='buy', ticker='T', side='yes', price=50, size=1, reason='test')
		result = port.open_position(sig, 'test-buy-yes-range', _dt(), slippage=0)
		assert result is False
		assert port.cash == 10.0
		assert not port.has_position('T', 'test-buy-yes-range')

	def test_close_position_adds_proceeds(self):
		port = Portfolio(1000.0, fee_fn=lambda p, s: 0.0)
		sig = Signal(action='buy', ticker='T', side='yes', price=50, size=1, reason='test')
		port.open_position(sig, 'test-buy-yes-range', _dt(), slippage=0)
		ct = port.close_position('T', 'test-buy-yes-range', 60, _dt(1), 'take_profit', slippage=0)
		assert ct is not None
		assert port.cash == 1010.0  # started 1000, paid 50, got back 60
		assert ct.pnl_cents == 10
		assert ct.exit_reason == 'take_profit'

	def test_close_position_slippage_reduces_exit(self):
		port = Portfolio(1000.0, fee_fn=lambda p, s: 0.0)
		sig = Signal(action='buy', ticker='T', side='yes', price=50, size=1, reason='test')
		port.open_position(sig, 'test-buy-yes-range', _dt(), slippage=1)  # entry = 51
		ct = port.close_position('T', 'test-buy-yes-range', 60, _dt(1), 'take_profit', slippage=1)  # exit = 59
		assert ct is not None
		assert ct.entry_price == 51
		assert ct.exit_price == 59
		assert ct.pnl_cents == 8  # 59 - 51

	def test_settle_yes_position_win(self):
		port = Portfolio(1000.0, fee_fn=lambda p, s: 0.0)
		sig = Signal(action='buy', ticker='T', side='yes', price=75, size=1, reason='test')
		port.open_position(sig, 'test-buy-yes-range', _dt(), slippage=0)
		ct = port.settle_position('T', 'test-buy-yes-range', 'yes', _dt(2))
		assert ct is not None
		assert ct.exit_price == 100
		assert ct.pnl_cents == 25  # 100 - 75
		assert port.cash == 1025.0

	def test_settle_yes_position_loss(self):
		port = Portfolio(1000.0, fee_fn=lambda p, s: 0.0)
		sig = Signal(action='buy', ticker='T', side='yes', price=75, size=1, reason='test')
		port.open_position(sig, 'test-buy-yes-range', _dt(), slippage=0)
		ct = port.settle_position('T', 'test-buy-yes-range', 'no', _dt(2))
		assert ct is not None
		assert ct.exit_price == 0
		assert ct.pnl_cents == -75
		assert port.cash == 925.0

	def test_settle_no_position_win(self):
		port = Portfolio(1000.0, fee_fn=lambda p, s: 0.0)
		sig = Signal(action='buy', ticker='T', side='no', price=30, size=1, reason='test')
		port.open_position(sig, 'test-buy-no-drop', _dt(), slippage=0)
		ct = port.settle_position('T', 'test-buy-no-drop', 'no', _dt(2))
		assert ct is not None
		assert ct.exit_price == 100
		assert ct.pnl_cents == 70

	def test_settle_no_position_loss(self):
		port = Portfolio(1000.0, fee_fn=lambda p, s: 0.0)
		sig = Signal(action='buy', ticker='T', side='no', price=30, size=1, reason='test')
		port.open_position(sig, 'test-buy-no-range', _dt(), slippage=0)
		ct = port.settle_position('T', 'test-buy-no-range', 'yes', _dt(2))
		assert ct is not None
		assert ct.exit_price == 0
		assert ct.pnl_cents == -30

	def test_entry_fee_charged_at_open(self):
		# Buy NO at 87¢: fee = ceil(0.07 * 1 * 0.87 * 0.13 * 100) = ceil(0.7917) = 1¢
		import math
		port = Portfolio(1000.0, fee_fn=STANDARD_FEE.calculate)
		sig = Signal(action='buy', ticker='T', side='no', price=87, size=1, reason='test')
		port.open_position(sig, 'test-buy-no-range', _dt(), slippage=0)
		expected_fee = math.ceil(0.07 * 1 * 0.87 * 0.13 * 100)  # 1¢
		assert port.total_fees_paid == pytest.approx(expected_fee, rel=1e-6)
		assert port.cash == pytest.approx(1000.0 - 87 - expected_fee, rel=1e-6)

	def test_fee_scales_with_fee_fn(self):
		# fee_fn with 0.25 multiplier simulates maker fee (0.25 * 0.07 = 1.75% equivalent)
		port = Portfolio(1000.0, fee_fn=lambda p, s: 0.25 * 0.07 * p * (100 - p) / 100 * s)
		sig = Signal(action='buy', ticker='T', side='no', price=87, size=1, reason='test')
		port.open_position(sig, 'test-buy-no-range', _dt(), slippage=0)
		expected_fee = 0.25 * 0.07 * 87 * 13 / 100
		assert port.total_fees_paid == pytest.approx(expected_fee, rel=1e-6)

	def test_no_fee_at_settlement(self):
		# Fee is only charged at entry — settlement should not add more fees
		port = Portfolio(1000.0, fee_fn=STANDARD_FEE.calculate)
		sig = Signal(action='buy', ticker='T', side='yes', price=75, size=1, reason='test')
		port.open_position(sig, 'test-buy-yes-range', _dt(), slippage=0)
		fee_at_entry = port.total_fees_paid
		port.settle_position('T', 'test-buy-yes-range', 'yes', _dt(2))
		assert port.total_fees_paid == fee_at_entry  # no new fees at settlement

	def test_get_equity_marks_at_entry(self):
		port = Portfolio(1000.0, fee_fn=lambda p, s: 0.0)
		sig = Signal(action='buy', ticker='T', side='yes', price=50, size=1, reason='test')
		port.open_position(sig, 'test-buy-yes-range', _dt(), slippage=0)
		# equity = cash(950) + position_entry_value(50) = 1000
		assert port.get_equity() == 1000.0

	def test_one_position_per_ticker_strategy(self):
		port = Portfolio(1000.0)
		sig1 = Signal(action='buy', ticker='T', side='yes', price=50, size=1, reason='test')
		sig2 = Signal(action='buy', ticker='T', side='yes', price=55, size=1, reason='test')
		port.open_position(sig1, 'test-buy-yes-range', _dt(), slippage=0)
		assert port.has_position('T', 'test-buy-yes-range')
		# Second open for same (ticker, strategy) would be rejected by strategy (has_position check)
		# But portfolio itself allows it — the guard is in the strategy
		port.open_position(sig2, 'test-buy-no-drop', _dt(), slippage=0)  # different strategy: ok
		assert port.has_position('T', 'test-buy-no-drop')


# ---------------------------------------------------------------------------
# Exit fee tests
# ---------------------------------------------------------------------------

class TestExitFee:
	def test_close_position_charges_exit_fee(self):
		"""close_position() should charge a fee on the exit price."""
		import math
		port = Portfolio(1000.0, fee_fn=STANDARD_FEE.calculate)
		sig = Signal(action='buy', ticker='T', side='yes', price=50, size=1, reason='test')
		port.open_position(sig, 'strat', _dt(), slippage=0)
		entry_fee = port.total_fees_paid
		port.close_position('T', 'strat', 60, _dt(1), 'take_profit', slippage=0)
		exit_fee = math.ceil(0.07 * 1 * 0.60 * 0.40 * 100)  # 2¢
		assert port.total_fees_paid == pytest.approx(entry_fee + exit_fee)
		# cash = 1000 - (50 + entry_fee) + (60 - exit_fee)
		assert port.cash == pytest.approx(1000.0 - 50 - entry_fee + 60 - exit_fee)

	def test_close_position_fee_cents_is_entry_plus_exit(self):
		"""CompletedTrade.fee_cents should reflect total fees (entry + exit)."""
		import math
		port = Portfolio(1000.0, fee_fn=STANDARD_FEE.calculate)
		sig = Signal(action='buy', ticker='T', side='yes', price=50, size=1, reason='test')
		port.open_position(sig, 'strat', _dt(), slippage=0)
		ct = port.close_position('T', 'strat', 60, _dt(1), 'take_profit', slippage=0)
		entry_fee = math.ceil(0.07 * 1 * 0.50 * 0.50 * 100)  # 2¢
		exit_fee = math.ceil(0.07 * 1 * 0.60 * 0.40 * 100)   # 2¢
		assert ct.fee_cents == pytest.approx(entry_fee + exit_fee)

	def test_close_position_exit_at_zero_no_fee(self):
		"""Exit at price 0 (after slippage) should have 0 exit fee."""
		port = Portfolio(1000.0, fee_fn=STANDARD_FEE.calculate)
		sig = Signal(action='buy', ticker='T', side='yes', price=50, size=1, reason='test')
		port.open_position(sig, 'strat', _dt(), slippage=0)
		entry_fee = port.total_fees_paid
		ct = port.close_position('T', 'strat', 0, _dt(1), 'stop_loss', slippage=0)
		# P=0 → P*(1-P)=0 → exit fee = 0
		assert port.total_fees_paid == pytest.approx(entry_fee)
		assert ct.pnl_cents == pytest.approx(-50 - entry_fee)

	def test_close_position_exit_at_hundred_no_fee(self):
		"""Exit at price 100 should have 0 exit fee (P*(1-P)=0)."""
		port = Portfolio(1000.0, fee_fn=STANDARD_FEE.calculate)
		sig = Signal(action='buy', ticker='T', side='yes', price=50, size=1, reason='test')
		port.open_position(sig, 'strat', _dt(), slippage=0)
		entry_fee = port.total_fees_paid
		ct = port.close_position('T', 'strat', 100, _dt(1), 'take_profit', slippage=0)
		assert port.total_fees_paid == pytest.approx(entry_fee)
		assert ct.pnl_cents == pytest.approx(50 - entry_fee)

	def test_settle_position_still_no_exit_fee(self):
		"""Settlement must NOT charge an exit fee — only close_position does."""
		port = Portfolio(1000.0, fee_fn=STANDARD_FEE.calculate)
		sig = Signal(action='buy', ticker='T', side='yes', price=75, size=1, reason='test')
		port.open_position(sig, 'strat', _dt(), slippage=0)
		entry_fee = port.total_fees_paid
		port.settle_position('T', 'strat', 'yes', _dt(2))
		assert port.total_fees_paid == pytest.approx(entry_fee)

	def test_close_position_pnl_includes_both_fees(self):
		"""PnL = (exit - entry) * size - entry_fee - exit_fee."""
		import math

		def fee_fn(p, s):
			return math.ceil(0.07 * s * (p / 100) * (1 - p / 100) * 100) if p > 0 and p < 100 else 0.0
		port = Portfolio(1000.0, fee_fn=fee_fn)
		sig = Signal(action='buy', ticker='T', side='yes', price=50, size=3, reason='test')
		port.open_position(sig, 'strat', _dt(), slippage=0)
		ct = port.close_position('T', 'strat', 60, _dt(1), 'take_profit', slippage=0)
		entry_fee = math.ceil(0.07 * 3 * 0.50 * 0.50 * 100)  # ceil(5.25) = 6
		exit_fee = math.ceil(0.07 * 3 * 0.60 * 0.40 * 100)   # ceil(5.04) = 6
		expected_pnl = (60 - 50) * 3 - entry_fee - exit_fee   # 30 - 6 - 6 = 18
		assert ct.pnl_cents == pytest.approx(expected_pnl)


# ---------------------------------------------------------------------------
# Strategy unit tests
# ---------------------------------------------------------------------------

class TestBuyYesInRange:
	def test_emits_buy_in_range(self):
		strategy = BuyYesInRange(min_price=70, max_price=99)
		port = Portfolio(10000.0)
		market = _make_market()
		trade = _make_trade(yes_price=80)
		signals = strategy.on_trade(trade, market, port)
		assert len(signals) == 1
		assert signals[0].action == 'buy'
		assert signals[0].side == 'yes'
		assert signals[0].price == 80

	def test_no_signal_below_min(self):
		strategy = BuyYesInRange(min_price=70, max_price=99)
		port = Portfolio(10000.0)
		market = _make_market()
		trade = _make_trade(yes_price=60)
		signals = strategy.on_trade(trade, market, port)
		assert signals == []

	def test_no_signal_above_max(self):
		strategy = BuyYesInRange(min_price=70, max_price=99)
		port = Portfolio(10000.0)
		market = _make_market()
		trade = _make_trade(yes_price=100)
		signals = strategy.on_trade(trade, market, port)
		assert signals == []

	def test_no_signal_when_position_exists(self):
		strategy = BuyYesInRange(min_price=70, max_price=99)
		port = Portfolio(10000.0)
		market = _make_market()
		trade = _make_trade(yes_price=80)
		# Manually open a position
		sig = Signal(action='buy', ticker='TEST-1', side='yes', price=80, size=1, reason='manual')
		port.open_position(sig, 'test-buy-yes-range', _dt(), slippage=0)
		signals = strategy.on_trade(trade, market, port)
		assert signals == []

	def test_buys_at_boundary(self):
		strategy = BuyYesInRange(min_price=70, max_price=99)
		port = Portfolio(10000.0)
		market = _make_market()
		for price in (70, 99):
			trade = _make_trade(yes_price=price)
			signals = strategy.on_trade(trade, market, port)
			assert len(signals) == 1


class TestBuyNoOnDrop:
	def test_no_signal_without_prior_price(self):
		strategy = BuyNoOnDrop()
		port = Portfolio(10000.0)
		market = _make_market()
		trade = _make_trade(yes_price=70)
		signals = strategy.on_trade(trade, market, port)
		assert signals == []

	def test_no_signal_insufficient_drop(self):
		strategy = BuyNoOnDrop(drop_threshold=5)
		port = Portfolio(10000.0)
		market = _make_market()
		strategy.on_trade(_make_trade(yes_price=70), market, port)  # set last known
		signals = strategy.on_trade(_make_trade(yes_price=67), market, port)  # drop=3 < 5
		assert signals == []

	def test_emits_buy_on_sufficient_drop(self):
		strategy = BuyNoOnDrop(drop_threshold=5)
		port = Portfolio(10000.0)
		market = _make_market()
		strategy.on_trade(_make_trade(yes_price=70), market, port)  # set last known = 70
		trade2 = _make_trade(yes_price=64)  # drop=6 >= 5, in [50,80]
		signals = strategy.on_trade(trade2, market, port)
		assert len(signals) == 1
		assert signals[0].action == 'buy'
		assert signals[0].side == 'no'
		assert signals[0].price == 36  # 100 - 64

	def test_tracks_last_known_price(self):
		strategy = BuyNoOnDrop()
		port = Portfolio(10000.0)
		market = _make_market()
		strategy.on_trade(_make_trade(yes_price=70), market, port)
		assert strategy._last_known_price.get('TEST-1') == 70
		strategy.on_trade(_make_trade(yes_price=65), market, port)
		assert strategy._last_known_price.get('TEST-1') == 65

	def test_one_position_per_ticker(self):
		strategy = BuyNoOnDrop(drop_threshold=5)
		port = Portfolio(10000.0)
		market = _make_market()
		strategy.on_trade(_make_trade(yes_price=70), market, port)  # prime price
		signals = strategy.on_trade(_make_trade(yes_price=64), market, port)
		assert len(signals) == 1
		port.open_position(signals[0], 'test-buy-no-drop', _dt(), slippage=0)
		# Second drop: no new signal because position exists
		strategy.on_trade(_make_trade(yes_price=64), market, port)  # reset last known
		signals2 = strategy.on_trade(_make_trade(yes_price=58), market, port)
		assert signals2 == []


class TestBuyNoInRange:
	def test_emits_buy_in_range(self):
		strategy = BuyNoInRange(min_price=5, max_price=30)
		port = Portfolio(10000.0)
		market = _make_market()
		trade = _make_trade(yes_price=15)
		signals = strategy.on_trade(trade, market, port)
		assert len(signals) == 1
		assert signals[0].side == 'no'
		assert signals[0].price == 85  # 100 - 15

	def test_no_signal_outside_range(self):
		strategy = BuyNoInRange()
		port = Portfolio(10000.0)
		market = _make_market()
		for price in (4, 31, 50):
			signals = strategy.on_trade(_make_trade(yes_price=price), market, port)
			assert signals == []

	def test_no_signal_when_position_exists(self):
		strategy = BuyNoInRange()
		port = Portfolio(10000.0)
		market = _make_market()
		sig = Signal(action='buy', ticker='TEST-1', side='no', price=85, size=1, reason='manual')
		port.open_position(sig, 'test-buy-no-range', _dt(), slippage=0)
		signals = strategy.on_trade(_make_trade(yes_price=15), market, port)
		assert signals == []


class TestActiveExitStub:
	def test_emits_buy_in_range(self):
		strategy = ActiveExitStub(min_price=40, max_price=60)
		port = Portfolio(10000.0)
		market = _make_market()
		trade = _make_trade(yes_price=50)
		signals = strategy.on_trade(trade, market, port)
		assert len(signals) == 1
		assert signals[0].action == 'buy'
		assert signals[0].side == 'yes'

	def test_emits_sell_on_take_profit(self):
		strategy = ActiveExitStub(min_price=40, max_price=60, take_profit=8, stop_loss=5)
		port = Portfolio(10000.0)
		market = _make_market()
		# Open position manually at actual entry 51 (signal 50 + slippage 1)
		buy_sig = Signal(action='buy', ticker='TEST-1', side='yes', price=50, size=1, reason='test')
		port.open_position(buy_sig, 'test-active-exit', _dt(), slippage=1)  # entry_price = 51
		# TP: sell when yes_price >= 51 + 8 = 59
		trade_tp = _make_trade(yes_price=59)
		signals = strategy.on_trade(trade_tp, market, port)
		assert len(signals) == 1
		assert signals[0].action == 'sell'
		assert 'take_profit' in signals[0].reason

	def test_emits_sell_on_stop_loss(self):
		strategy = ActiveExitStub(min_price=40, max_price=60, take_profit=8, stop_loss=5)
		port = Portfolio(10000.0)
		market = _make_market()
		buy_sig = Signal(action='buy', ticker='TEST-1', side='yes', price=50, size=1, reason='test')
		port.open_position(buy_sig, 'test-active-exit', _dt(), slippage=1)  # entry_price = 51
		# SL: sell when yes_price <= 51 - 5 = 46
		trade_sl = _make_trade(yes_price=46)
		signals = strategy.on_trade(trade_sl, market, port)
		assert len(signals) == 1
		assert signals[0].action == 'sell'
		assert 'stop_loss' in signals[0].reason

	def test_no_sell_between_tp_and_sl(self):
		strategy = ActiveExitStub(min_price=40, max_price=60, take_profit=8, stop_loss=5)
		port = Portfolio(10000.0)
		market = _make_market()
		buy_sig = Signal(action='buy', ticker='TEST-1', side='yes', price=50, size=1, reason='test')
		port.open_position(buy_sig, 'test-active-exit', _dt(), slippage=1)  # entry=51
		# Prices 47..58 should NOT trigger TP (need >=59) or SL (need <=46)
		for price in (47, 50, 55, 58):
			signals = strategy.on_trade(_make_trade(yes_price=price), market, port)
			assert signals == [], f"Unexpected sell signal at yes_price={price}"


# ---------------------------------------------------------------------------
# Slippage integration tests
# ---------------------------------------------------------------------------

class TestSlippage:
	def test_entry_slippage_applied(self):
		port = Portfolio(1000.0, fee_fn=lambda p, s: 0.0)
		sig = Signal(action='buy', ticker='T', side='yes', price=50, size=2, reason='test')
		port.open_position(sig, 'test-buy-yes-range', _dt(), slippage=3)
		pos = port.positions[('T', 'test-buy-yes-range')]
		assert pos.entry_price == 53  # 50 + 3
		assert port.cash == 1000.0 - 53 * 2

	def test_exit_slippage_applied(self):
		port = Portfolio(1000.0, fee_fn=lambda p, s: 0.0)
		sig = Signal(action='buy', ticker='T', side='yes', price=50, size=1, reason='test')
		port.open_position(sig, 'test-buy-yes-range', _dt(), slippage=1)  # entry=51
		ct = port.close_position('T', 'test-buy-yes-range', 65, _dt(1), 'take_profit', slippage=2)
		assert ct is not None
		assert ct.exit_price == 63  # 65 - 2
		assert ct.pnl_cents == 12   # 63 - 51


# ---------------------------------------------------------------------------
# No-lookahead test
# ---------------------------------------------------------------------------

class TestNoLookahead:
	def test_position_not_settled_before_close_time(self, tmp_path):
		"""Settlement must not happen before market.close_time."""
		close = _dt(2)  # market closes at T+2h
		markets = [{
			'ticker': 'NL-1',
			'series_ticker': 'NLSERIES',
			'close_time': _iso(close),
			'open_time': _iso(_dt(-24)),
			'result': 'yes',
			'status': 'settled',
		}]
		trades = [
			# Trade before close: strategy A sees yes_price=80 -> buys
			{
				'trade_id': 'tr1',
				'ticker': 'NL-1',
				'yes_price': 80,
				'no_price': 20,
				'count': 1,
				'taker_side': 'yes',
				'created_time': _iso(_dt(1)),  # T+1h < close_time T+2h
			},
			# Trade after close: triggers settlement sweep
			{
				'trade_id': 'tr2',
				'ticker': 'NL-1',
				'yes_price': 80,
				'no_price': 20,
				'count': 1,
				'taker_side': 'yes',
				'created_time': _iso(_dt(3)),  # T+3h > close_time T+2h
			},
		]
		db_path = _make_db(tmp_path, markets, trades)
		result = EventBacktester().run(
			series='NLSERIES',
			strategies=[BuyYesInRange(min_price=70, max_price=99)],
			db_path=db_path,
		)
		# Exactly one completed trade (settled after close_time)
		assert result.total_trades == 1
		assert result.trade_sample[0].exit_reason == 'settlement'
		assert result.trade_sample[0].exit_price == 100  # result='yes', side='yes'


# ---------------------------------------------------------------------------
# BacktestResult metrics tests
# ---------------------------------------------------------------------------

class TestBacktestResult:
	def _make_result(self, pnls: list[int]) -> BacktestResult:
		from datetime import datetime, timezone
		now = datetime.now(timezone.utc)
		port = Portfolio(1000.0)
		for i, p in enumerate(pnls):
			port.equity_snapshots.append((now, 1000.0 + sum(pnls[:i+1])))
			ct = CompletedTrade(
				ticker='T', side='yes', strategy='test-buy-yes-range',
				entry_price=50, entry_time=now,
				exit_price=50 + p, exit_time=now,
				pnl_cents=p, exit_reason='settlement',
			)
			port._record_trade(ct)
		# Re-run metrics the way the backtester does
		from edge_catcher.runner.event_backtest import _compute_metrics
		sharpe, max_dd, win_rate, avg_win, avg_loss, wins, losses, per_strategy = _compute_metrics(
			port, port.equity_snapshots,
		)
		return BacktestResult(
			total_trades=port.total_trades,
			wins=wins,
			losses=losses,
			net_pnl_cents=port.net_pnl_cents,
			total_fees_paid=0,
			sharpe=sharpe,
			max_drawdown_pct=max_dd,
			win_rate=win_rate,
			avg_win_cents=avg_win,
			avg_loss_cents=avg_loss,
			equity_curve=port.equity_snapshots,
			per_strategy=per_strategy,
			per_strategy_curves=port._per_strategy_curves,
			trade_sample=port._trade_sample,
		)

	def test_win_rate(self):
		result = self._make_result([10, 20, -5, -10])
		assert result.wins == 2
		assert result.losses == 2
		assert result.win_rate == pytest.approx(0.5)

	def test_net_pnl(self):
		result = self._make_result([10, 20, -5])
		assert result.net_pnl_cents == 25

	def test_avg_win_and_loss(self):
		result = self._make_result([10, 30, -5, -15])
		assert result.avg_win_cents == pytest.approx(20.0)
		assert result.avg_loss_cents == pytest.approx(-10.0)

	def test_sharpe_zero_when_single_trade(self):
		result = self._make_result([10])
		assert result.sharpe == 0.0

	def test_sharpe_nonzero_with_variance(self):
		result = self._make_result([10, -10, 20, -5])
		assert result.sharpe != 0.0

	def test_max_drawdown(self):
		# equity: 1010, 1000, 990 -> peak=1010, max_dd=(1010-990)/1010 * 100
		result = self._make_result([10, -10, -10])
		assert result.max_drawdown_pct > 0.0

	def test_summary_contains_key_fields(self):
		result = self._make_result([10, -5])
		s = result.summary()
		assert 'Total trades' in s
		assert 'Win rate' in s
		assert 'Net P&L' in s
		assert 'Sharpe' in s

	def test_to_dict_is_json_serializable(self):
		import json
		result = self._make_result([10, -5, 20])
		d = result.to_dict()
		# Should not raise
		json.dumps(d)
		assert 'total_trades' in d
		assert 'trade_log' in d
		assert 'equity_curve' in d

	def test_to_dict_includes_pnl_values(self):
		"""BacktestResult.to_dict() should include the full pnl_values list."""
		from edge_catcher.runner.event_backtest import BacktestResult

		result = BacktestResult(
			total_trades=3, wins=2, losses=1,
			net_pnl_cents=15, total_fees_paid=3.0,
			sharpe=1.5, max_drawdown_pct=2.0, win_rate=0.67,
			avg_win_cents=10.0, avg_loss_cents=-5.0,
			equity_curve=[], per_strategy={}, per_strategy_curves={},
			trade_sample=[], pnl_values=[10, 10, -5],
		)
		d = result.to_dict()
		assert "pnl_values" in d
		assert d["pnl_values"] == [10, 10, -5]


# ---------------------------------------------------------------------------
# Integration: full backtest on synthetic dataset
# ---------------------------------------------------------------------------

class TestIntegration:
	def test_strategy_a_full_backtest(self, tmp_path):
		"""Full run: YES buyer at 80, market resolves YES -> win."""
		close = _dt(2)
		markets = [{
			'ticker': 'INT-A',
			'series_ticker': 'INTSERIES',
			'close_time': _iso(close),
			'open_time': _iso(_dt(-24)),
			'result': 'yes',
			'status': 'settled',
		}]
		trades = [
			{'trade_id': 'ta1', 'ticker': 'INT-A', 'yes_price': 80, 'no_price': 20,
			 'count': 1, 'taker_side': 'yes', 'created_time': _iso(_dt(0))},
			{'trade_id': 'ta2', 'ticker': 'INT-A', 'yes_price': 82, 'no_price': 18,
			 'count': 1, 'taker_side': 'yes', 'created_time': _iso(_dt(3))},
		]
		db_path = _make_db(tmp_path, markets, trades)
		result = EventBacktester().run(
			series='INTSERIES',
			strategies=[BuyYesInRange(min_price=70, max_price=99)],
			initial_cash=1000.0,
			slippage_cents=0,
			db_path=db_path,
			fee_fn=lambda p, s: 0.0,
		)
		assert result.total_trades == 1
		assert result.wins == 1
		assert result.net_pnl_cents == 20  # 100 - 80

	def test_strategy_c_full_backtest_win(self, tmp_path):
		"""NO buyer at (100-15=85), market resolves NO -> win."""
		markets = [{
			'ticker': 'INT-C',
			'series_ticker': 'INTSERIES2',
			'close_time': _iso(_dt(2)),
			'open_time': _iso(_dt(-24)),
			'result': 'no',
			'status': 'settled',
		}]
		trades = [
			{'trade_id': 'tc1', 'ticker': 'INT-C', 'yes_price': 15, 'no_price': 85,
			 'count': 1, 'taker_side': 'yes', 'created_time': _iso(_dt(0))},
			{'trade_id': 'tc2', 'ticker': 'INT-C', 'yes_price': 15, 'no_price': 85,
			 'count': 1, 'taker_side': 'yes', 'created_time': _iso(_dt(3))},
		]
		db_path = _make_db(tmp_path, markets, trades)
		result = EventBacktester().run(
			series='INTSERIES2',
			strategies=[BuyNoInRange()],
			initial_cash=1000.0,
			slippage_cents=0,
			db_path=db_path,
			fee_fn=lambda p, s: 0.0,
		)
		assert result.total_trades == 1
		assert result.wins == 1
		assert result.net_pnl_cents == 15  # 100 - 85

	def test_strategy_tp_exit_on_take_profit(self, tmp_path):
		"""ActiveExitStub buys YES at 50, exits at 60 (TP=8, entry+slippage=51, TP at 59)."""
		markets = [{
			'ticker': 'INT-TP',
			'series_ticker': 'INTSERIES3',
			'close_time': _iso(_dt(10)),
			'open_time': _iso(_dt(-24)),
			'result': 'yes',
			'status': 'open',
		}]
		trades = [
			{'trade_id': 'tp1', 'ticker': 'INT-TP', 'yes_price': 50, 'no_price': 50,
			 'count': 1, 'taker_side': 'yes', 'created_time': _iso(_dt(0))},
			# entry_price = 50 + 1(slippage) = 51; TP at 51+8=59
			{'trade_id': 'tp2', 'ticker': 'INT-TP', 'yes_price': 59, 'no_price': 41,
			 'count': 1, 'taker_side': 'yes', 'created_time': _iso(_dt(1))},
		]
		db_path = _make_db(tmp_path, markets, trades)
		result = EventBacktester().run(
			series='INTSERIES3',
			strategies=[ActiveExitStub(min_price=40, max_price=60, take_profit=8, stop_loss=5)],
			initial_cash=1000.0,
			slippage_cents=1,
			db_path=db_path,
			fee_fn=lambda p, s: 0.0,
		)
		assert result.total_trades == 1
		assert result.trade_sample[0].exit_reason == 'take_profit'
		# exit = 59 - 1(slippage) = 58; entry = 51; pnl = 7
		assert result.trade_sample[0].pnl_cents == 7

	def test_strategy_tp_settles_if_no_tp_sl(self, tmp_path):
		"""ActiveExitStub holds to settlement if TP/SL never hit."""
		markets = [{
			'ticker': 'INT-TP2',
			'series_ticker': 'INTSERIES4',
			'close_time': _iso(_dt(2)),
			'open_time': _iso(_dt(-24)),
			'result': 'yes',
			'status': 'settled',
		}]
		trades = [
			{'trade_id': 'tp21', 'ticker': 'INT-TP2', 'yes_price': 50, 'no_price': 50,
			 'count': 1, 'taker_side': 'yes', 'created_time': _iso(_dt(0))},
			# Price stays between SL and TP
			{'trade_id': 'tp22', 'ticker': 'INT-TP2', 'yes_price': 53, 'no_price': 47,
			 'count': 1, 'taker_side': 'yes', 'created_time': _iso(_dt(3))},
		]
		db_path = _make_db(tmp_path, markets, trades)
		result = EventBacktester().run(
			series='INTSERIES4',
			strategies=[ActiveExitStub(min_price=40, max_price=60, take_profit=8, stop_loss=5)],
			initial_cash=1000.0,
			slippage_cents=0,
			db_path=db_path,
		)
		assert result.total_trades == 1
		assert result.trade_sample[0].exit_reason == 'settlement'

	def test_multi_ticker_multi_strategy(self, tmp_path):
		"""Multiple tickers + strategies A and C in same run."""
		markets = [
			{
				'ticker': 'MT-1',
				'series_ticker': 'MTSERIES',
				'close_time': _iso(_dt(2)),
				'open_time': _iso(_dt(-24)),
				'result': 'yes',
				'status': 'settled',
			},
			{
				'ticker': 'MT-2',
				'series_ticker': 'MTSERIES',
				'close_time': _iso(_dt(2)),
				'open_time': _iso(_dt(-24)),
				'result': 'no',
				'status': 'settled',
			},
		]
		trades = [
			# MT-1: YES buyer at 75 (in range 70-99), resolves YES -> win
			{'trade_id': 'mt1', 'ticker': 'MT-1', 'yes_price': 75, 'no_price': 25,
			 'count': 1, 'taker_side': 'yes', 'created_time': _iso(_dt(0))},
			# MT-2: NO buyer at (100-10=90), resolves NO -> win
			{'trade_id': 'mt2', 'ticker': 'MT-2', 'yes_price': 10, 'no_price': 90,
			 'count': 1, 'taker_side': 'yes', 'created_time': _iso(_dt(0))},
			# Trigger settlement for both
			{'trade_id': 'mt3', 'ticker': 'MT-1', 'yes_price': 99, 'no_price': 1,
			 'count': 1, 'taker_side': 'yes', 'created_time': _iso(_dt(3))},
		]
		db_path = _make_db(tmp_path, markets, trades)
		result = EventBacktester().run(
			series='MTSERIES',
			strategies=[BuyYesInRange(), BuyNoInRange()],
			initial_cash=10000.0,
			slippage_cents=0,
			db_path=db_path,
		)
		assert result.total_trades == 2
		assert result.wins == 2
		assert 'test-buy-yes-range' in result.per_strategy
		assert 'test-buy-no-range' in result.per_strategy

	def test_capital_constraint_prevents_open(self, tmp_path):
		"""Portfolio rejects buy when cash is insufficient."""
		markets = [{
			'ticker': 'CAP-1',
			'series_ticker': 'CAPSERIES',
			'close_time': _iso(_dt(2)),
			'open_time': _iso(_dt(-24)),
			'result': 'yes',
			'status': 'settled',
		}]
		# initial_cash=5, price=80*1=80 cents required -> can't open
		trades = [
			{'trade_id': 'c1', 'ticker': 'CAP-1', 'yes_price': 80, 'no_price': 20,
			 'count': 1, 'taker_side': 'yes', 'created_time': _iso(_dt(0))},
			{'trade_id': 'c2', 'ticker': 'CAP-1', 'yes_price': 80, 'no_price': 20,
			 'count': 1, 'taker_side': 'yes', 'created_time': _iso(_dt(3))},
		]
		db_path = _make_db(tmp_path, markets, trades)
		result = EventBacktester().run(
			series='CAPSERIES',
			strategies=[BuyYesInRange(min_price=70, max_price=99)],
			initial_cash=5.0,  # less than 80 cents needed
			slippage_cents=0,
			db_path=db_path,
		)
		assert result.total_trades == 0  # no position opened

	def test_empty_series_returns_empty_result(self, tmp_path):
		"""No markets for series -> empty BacktestResult."""
		db_path = _make_db(tmp_path, [], [])
		result = EventBacktester().run(
			series='NOSUCHSERIES',
			strategies=[BuyYesInRange()],
			db_path=db_path,
		)
		assert result.total_trades == 0
		assert result.net_pnl_cents == 0

	def test_on_progress_callback_fires(self, tmp_path):
		"""on_progress callback fires with structured data."""
		close = _dt(2)
		markets = [{
			'ticker': 'PROG-A',
			'series_ticker': 'PROGSERIES',
			'close_time': _iso(close),
			'open_time': _iso(_dt(-24)),
			'result': 'yes',
			'status': 'settled',
		}]
		trades = [
			{'trade_id': f'tp{i}', 'ticker': 'PROG-A', 'yes_price': 80, 'no_price': 20,
			 'count': 1, 'taker_side': 'yes', 'created_time': _iso(_dt(0) + timedelta(seconds=i))}
			for i in range(5)
		]
		db_path = _make_db(tmp_path, markets, trades)
		progress_calls = []
		EventBacktester().run(
			series='PROGSERIES',
			strategies=[BuyYesInRange(min_price=70, max_price=99)],
			initial_cash=1000.0,
			slippage_cents=0,
			db_path=db_path,
			fee_fn=lambda p, s: 0.0,
			on_progress=lambda info: progress_calls.append(info),
		)
		# Initial callback at trade 0 should always fire
		assert len(progress_calls) >= 1
		first = progress_calls[0]
		assert first["trades_processed"] == 0
		assert first["trades_estimated"] == 5
		assert "net_pnl_cents" in first
		assert "wins" in first
		assert "losses" in first

	def test_on_progress_callback_fires_at_1k(self, tmp_path):
		"""on_progress callback fires every 1k trades (plus initial call at 0)."""
		close = _dt(2)
		markets = [{
			'ticker': 'PROG-B',
			'series_ticker': 'PROGSERIES2',
			'close_time': _iso(close),
			'open_time': _iso(_dt(-24)),
			'result': 'yes',
			'status': 'settled',
		}]
		trades = [
			{'trade_id': f'tq{i}', 'ticker': 'PROG-B', 'yes_price': 80, 'no_price': 20,
			 'count': 1, 'taker_side': 'yes', 'created_time': _iso(_dt(0) + timedelta(seconds=i))}
			for i in range(2_001)
		]
		db_path = _make_db(tmp_path, markets, trades)
		progress_calls = []
		EventBacktester().run(
			series='PROGSERIES2',
			strategies=[BuyYesInRange(min_price=70, max_price=99)],
			initial_cash=100000.0,
			slippage_cents=0,
			db_path=db_path,
			fee_fn=lambda p, s: 0.0,
			on_progress=lambda info: progress_calls.append(info),
		)
		# Initial callback (trades_processed=0) + at least one 1k checkpoint
		assert len(progress_calls) >= 2
		assert progress_calls[0]["trades_processed"] == 0
		second = progress_calls[1]
		assert second["trades_processed"] == 1000
		assert second["trades_estimated"] == 2001


# ---------------------------------------------------------------------------
# FirstTradeEntry (first-trade stub) unit tests
# ---------------------------------------------------------------------------

class TestFirstTradeEntry:
	def test_first_trade_entry_above_threshold_buys_no(self):
		strategy = FirstTradeEntry(threshold_high=60, threshold_low=40)
		port = Portfolio(10000.0)
		market = _make_market()
		trade = _make_trade(yes_price=65)
		signals = strategy.on_trade(trade, market, port)
		assert len(signals) == 1
		assert signals[0].action == 'buy'
		assert signals[0].side == 'no'
		assert signals[0].price == 35  # 100 - 65

	def test_first_trade_entry_below_threshold_buys_yes(self):
		strategy = FirstTradeEntry(threshold_high=60, threshold_low=40)
		port = Portfolio(10000.0)
		market = _make_market()
		trade = _make_trade(yes_price=35)
		signals = strategy.on_trade(trade, market, port)
		assert len(signals) == 1
		assert signals[0].action == 'buy'
		assert signals[0].side == 'yes'
		assert signals[0].price == 35

	def test_no_signal_when_first_trade_in_midrange(self):
		strategy = FirstTradeEntry(threshold_high=60, threshold_low=40)
		port = Portfolio(10000.0)
		market = _make_market()
		trade = _make_trade(yes_price=50)
		signals = strategy.on_trade(trade, market, port)
		assert signals == []

	def test_ignores_subsequent_trades(self):
		strategy = FirstTradeEntry(threshold_high=60, threshold_low=40)
		port = Portfolio(10000.0)
		market = _make_market()
		# First trade: triggers entry signal
		signals1 = strategy.on_trade(_make_trade(yes_price=65), market, port)
		assert len(signals1) == 1
		port.open_position(signals1[0], 'test-first-trade', _dt(), slippage=0)
		# Second trade: same ticker, position exists — check TP/SL only (not entry)
		# yes_price=63 doesn't trigger TP (need no_price >= 35+8=43, i.e. yes<=57) or SL
		signals2 = strategy.on_trade(_make_trade(yes_price=63), market, port)
		assert signals2 == []

	def test_ignores_subsequent_trades_without_position(self):
		"""When first trade is in midrange (no entry), subsequent trades are also ignored."""
		strategy = FirstTradeEntry(threshold_high=60, threshold_low=40)
		port = Portfolio(10000.0)
		market = _make_market()
		strategy.on_trade(_make_trade(yes_price=50), market, port)  # no entry
		signals = strategy.on_trade(_make_trade(yes_price=65), market, port)  # second trade
		assert signals == []

	def test_one_position_per_ticker(self):
		strategy = FirstTradeEntry(threshold_high=60, threshold_low=40)
		port = Portfolio(10000.0)
		market = _make_market()
		# Different ticker should get its own entry
		t1_signals = strategy.on_trade(_make_trade(ticker='TEST-1', yes_price=65), market, port)
		assert len(t1_signals) == 1
		t2_signals = strategy.on_trade(_make_trade(ticker='TEST-2', yes_price=65), market, port)
		assert len(t2_signals) == 1

	def test_take_profit_exit_no_position(self):
		"""NO position TP: fires when no_price (100-yes_price) rises enough."""
		strategy = FirstTradeEntry(threshold_high=60, threshold_low=40, take_profit=8, stop_loss=5)
		port = Portfolio(10000.0)
		market = _make_market()
		# Enter: yes_price=65, no_price=35, entry_price stored = 35 + slippage=1 = 36
		buy_sig = Signal(action='buy', ticker='TEST-1', side='no', price=35, size=1, reason='test')
		port.open_position(buy_sig, 'test-first-trade', _dt(), slippage=1)  # entry_price = 36
		# TP fires when current no_price >= 36 + 8 = 44, i.e. yes_price <= 56
		trade_tp = _make_trade(yes_price=56)  # no_price = 44
		signals = strategy.on_trade(trade_tp, market, port)
		assert len(signals) == 1
		assert signals[0].action == 'sell'
		assert signals[0].side == 'no'
		assert 'take_profit' in signals[0].reason

	def test_stop_loss_exit_no_position(self):
		"""NO position SL: fires when no_price falls enough (yes_price rises)."""
		strategy = FirstTradeEntry(threshold_high=60, threshold_low=40, take_profit=8, stop_loss=5)
		port = Portfolio(10000.0)
		market = _make_market()
		buy_sig = Signal(action='buy', ticker='TEST-1', side='no', price=35, size=1, reason='test')
		port.open_position(buy_sig, 'test-first-trade', _dt(), slippage=1)  # entry_price = 36
		# SL fires when current no_price <= 36 - 5 = 31, i.e. yes_price >= 69
		trade_sl = _make_trade(yes_price=69)  # no_price = 31
		signals = strategy.on_trade(trade_sl, market, port)
		assert len(signals) == 1
		assert signals[0].action == 'sell'
		assert signals[0].side == 'no'
		assert 'stop_loss' in signals[0].reason

	def test_take_profit_exit_yes_position(self):
		"""YES position TP: fires when yes_price rises enough."""
		strategy = FirstTradeEntry(threshold_high=60, threshold_low=40, take_profit=8, stop_loss=5)
		port = Portfolio(10000.0)
		market = _make_market()
		buy_sig = Signal(action='buy', ticker='TEST-1', side='yes', price=35, size=1, reason='test')
		port.open_position(buy_sig, 'test-first-trade', _dt(), slippage=1)  # entry_price = 36
		# TP: yes_price >= 36 + 8 = 44
		trade_tp = _make_trade(yes_price=44)
		signals = strategy.on_trade(trade_tp, market, port)
		assert len(signals) == 1
		assert signals[0].action == 'sell'
		assert signals[0].side == 'yes'
		assert 'take_profit' in signals[0].reason

	def test_no_exit_between_tp_and_sl(self):
		strategy = FirstTradeEntry(threshold_high=60, threshold_low=40, take_profit=8, stop_loss=5)
		port = Portfolio(10000.0)
		market = _make_market()
		buy_sig = Signal(action='buy', ticker='TEST-1', side='no', price=35, size=1, reason='test')
		port.open_position(buy_sig, 'test-first-trade', _dt(), slippage=1)  # entry_price = 36
		# Prices where no_price stays 32..43 should not trigger (TP needs >=44, SL needs <=31)
		for yes_price in (57, 60, 65, 68):
			# no_price = 43, 40, 35, 32 — all between SL(31) and TP(44)
			signals = strategy.on_trade(_make_trade(yes_price=yes_price), market, port)
			assert signals == [], f"Unexpected signal at yes_price={yes_price}"


# ---------------------------------------------------------------------------
# DualThreshold (two-sided stub) unit tests
# ---------------------------------------------------------------------------

class TestDualThreshold:
	def test_buys_no_at_fav_threshold(self):
		strategy = DualThreshold(fav_threshold=85, long_threshold=15)
		port = Portfolio(10000.0)
		market = _make_market()
		trade = _make_trade(yes_price=90)
		signals = strategy.on_trade(trade, market, port)
		assert len(signals) == 1
		assert signals[0].action == 'buy'
		assert signals[0].side == 'no'
		assert signals[0].price == 10  # 100 - 90

	def test_buys_no_at_exact_fav_threshold(self):
		strategy = DualThreshold(fav_threshold=85, long_threshold=15)
		port = Portfolio(10000.0)
		market = _make_market()
		trade = _make_trade(yes_price=85)
		signals = strategy.on_trade(trade, market, port)
		assert len(signals) == 1
		assert signals[0].side == 'no'

	def test_buys_yes_at_low_threshold(self):
		strategy = DualThreshold(fav_threshold=85, long_threshold=15)
		port = Portfolio(10000.0)
		market = _make_market()
		trade = _make_trade(yes_price=10)
		signals = strategy.on_trade(trade, market, port)
		assert len(signals) == 1
		assert signals[0].action == 'buy'
		assert signals[0].side == 'yes'
		assert signals[0].price == 10

	def test_buys_yes_at_exact_long_threshold(self):
		strategy = DualThreshold(fav_threshold=85, long_threshold=15)
		port = Portfolio(10000.0)
		market = _make_market()
		trade = _make_trade(yes_price=15)
		signals = strategy.on_trade(trade, market, port)
		assert len(signals) == 1
		assert signals[0].side == 'yes'

	def test_no_signal_in_midrange(self):
		strategy = DualThreshold(fav_threshold=85, long_threshold=15)
		port = Portfolio(10000.0)
		market = _make_market()
		for price in (16, 50, 84):
			signals = strategy.on_trade(_make_trade(yes_price=price), market, port)
			assert signals == [], f"Unexpected signal at yes_price={price}"

	def test_no_duplicate_position(self):
		"""Once a position is open, subsequent trades don't generate new entries."""
		strategy = DualThreshold(fav_threshold=85, long_threshold=15)
		port = Portfolio(10000.0)
		market = _make_market()
		signals1 = strategy.on_trade(_make_trade(yes_price=90), market, port)
		assert len(signals1) == 1
		port.open_position(signals1[0], 'test-dual-threshold', _dt(), slippage=0)
		# Second trade at same extreme: position exists, no new signal
		signals2 = strategy.on_trade(_make_trade(yes_price=92), market, port)
		assert signals2 == []

	def test_holds_to_settlement_no_on_market_close(self):
		"""on_market_close returns [] — strategy relies on engine settlement."""
		strategy = DualThreshold()
		port = Portfolio(10000.0)
		buy_sig = Signal(action='buy', ticker='TEST-1', side='no', price=10, size=1, reason='test')
		port.open_position(buy_sig, 'test-dual-threshold', _dt(), slippage=0)
		close_signals = strategy.on_market_close('TEST-1', 'yes', port)
		assert close_signals == []
		# Position should still be open (engine handles settlement)
		assert port.has_position('TEST-1', 'test-dual-threshold')

	def test_different_tickers_each_get_entry(self):
		strategy = DualThreshold(fav_threshold=85, long_threshold=15)
		port = Portfolio(10000.0)
		market1 = _make_market(ticker='T1')
		market2 = _make_market(ticker='T2')
		s1 = strategy.on_trade(_make_trade(ticker='T1', yes_price=90), market1, port)
		s2 = strategy.on_trade(_make_trade(ticker='T2', yes_price=90), market2, port)
		assert len(s1) == 1
		assert len(s2) == 1
