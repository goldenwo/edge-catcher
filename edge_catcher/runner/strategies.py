"""Strategy abstractions and example strategy for the event-driven backtester.

This file contains:
- Strategy base class (ABC) — implement this for custom strategies
- Signal dataclass — buy/sell signals returned by strategies
- ExampleStrategy — a simple reference implementation
- VolumeMixin — reusable running trade counter filter
- MomentumMixin — reusable BTC price momentum filter

For your own strategies, create `strategies_local.py` (gitignored) and import
the base classes from here. See `strategies_local.py.example` for a template.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from edge_catcher.storage.models import Market, Trade

if TYPE_CHECKING:
	from datetime import datetime
	from edge_catcher.research.ohlc_provider import OHLCProvider
	from edge_catcher.runner.event_backtest import Portfolio


@dataclass
class Signal:
	action: Literal['buy', 'sell']
	ticker: str
	side: Literal['yes', 'no']
	price: int  # cents
	size: int   # contracts
	reason: str


class Strategy(ABC):
	"""Base class for all trading strategies.

	Implement `on_trade` to receive each trade as it streams from the DB.
	Return a list of Signal objects (buy/sell).
	The backtester handles position management, settlement, and P&L tracking.
	"""
	name: str
	ohlc: "OHLCProvider | None" = None  # Set by backtester if external OHLC data is available

	@abstractmethod
	def on_trade(self, trade: Trade, market: Market, portfolio: 'Portfolio') -> list[Signal]:
		...

	def on_market_close(self, ticker: str, result: str, portfolio: 'Portfolio') -> list[Signal]:
		return []  # default: no action, settlement handled by engine


# ---------------------------------------------------------------------------
# Reusable Filter Mixins
# ---------------------------------------------------------------------------

class VolumeMixin:
	"""Mixin: skip entry when a ticker has had more than max_trades observed trades.

	Use as a base class alongside a Strategy subclass:
		class MyFilteredStrategy(VolumeMixin, MyStrategy):
			...
	"""

	def _init_volume_filter(self, max_trades: int = 20) -> None:
		self.max_trades = max_trades
		self._trade_counts: dict[str, int] = {}

	def _increment_and_check(self, ticker: str) -> bool:
		"""Increment trade counter. Returns True if count EXCEEDS max_trades (should skip)."""
		count = self._trade_counts.get(ticker, 0) + 1
		self._trade_counts[ticker] = count
		return count > self.max_trades


class MomentumMixin:
	"""Mixin: skip entry when an external price (e.g. BTC) moved >max_move_pct recently.

	Pass a dict of {minute_timestamp: close_price} at init time.
	The mixin checks price change over the last `lookback_minutes`.
	"""

	def _init_momentum(self, max_move_pct: float = 1.0, lookback_minutes: int = 60,
	                    price_data: dict[int, float] | None = None) -> None:
		self.max_move_pct = max_move_pct
		self.lookback_minutes = lookback_minutes
		self._price_data = price_data or {}
		self._price_timestamps = sorted(self._price_data.keys())

	def _price_moved_too_much(self, trade_time: 'datetime') -> bool:
		"""Check if price moved more than max_move_pct in the last lookback_minutes."""
		if not self._price_timestamps:
			return False

		import bisect
		ts = int(trade_time.timestamp())
		idx = bisect.bisect_right(self._price_timestamps, ts) - 1
		if idx < 0:
			return False
		current_price = self._price_data[self._price_timestamps[idx]]

		past_ts_target = ts - (self.lookback_minutes * 60)
		past_idx = bisect.bisect_right(self._price_timestamps, past_ts_target) - 1
		if past_idx < 0:
			return False
		past_price = self._price_data[self._price_timestamps[past_idx]]

		if past_price == 0:
			return False
		move_pct = abs(current_price - past_price) / past_price * 100
		return move_pct > self.max_move_pct


# ---------------------------------------------------------------------------
# Example Strategy (reference implementation)
# ---------------------------------------------------------------------------

class ExampleStrategy(Strategy):
	"""Example: buy YES when yes_price is in [min_price, max_price]. Hold to settlement.

	This is a minimal reference strategy. Copy and modify for your own research.
	Parameters (min_price, max_price) are configurable via CLI --min-price / --max-price.
	"""

	name = 'example'

	def __init__(self, min_price: int = 40, max_price: int = 60, size: int = 1) -> None:
		self.min_price = min_price
		self.max_price = max_price
		self.size = size

	def on_trade(self, trade: Trade, market: Market, portfolio: 'Portfolio') -> list[Signal]:
		if (
			self.min_price <= trade.yes_price <= self.max_price
			and not portfolio.has_position(trade.ticker, self.name)
		):
			return [Signal(
				action='buy',
				ticker=trade.ticker,
				side='yes',
				price=trade.yes_price,
				size=self.size,
				reason=f'yes_price={trade.yes_price} in [{self.min_price},{self.max_price}]',
			)]
		return []
