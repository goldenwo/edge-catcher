"""Market state data models for the paper trading framework.

Contains OrderbookSnapshot, FillResult, TickContext, MarketState,
and the derive_event_ticker helper.
"""

import re
from collections import deque
from dataclasses import dataclass, field
from typing import Any


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class FillResult:
	"""Result of simulating a fill against an orderbook side."""
	fill_size: int
	blended_price_cents: int
	slippage_cents: int
	fill_pct: float
	intended_size: int = 0


@dataclass
class OrderbookSnapshot:
	"""Snapshot of a market's orderbook.

	Levels are (price_dollars: float, quantity: int) tuples sorted from
	best (lowest ask) to worst.  All prices in dollars to match Kalshi API
	format; `walk_book` converts to cents internally.
	"""
	yes_levels: list[tuple[float, int]]
	no_levels: list[tuple[float, int]]

	@property
	def depth(self) -> int:
		"""Total resting quantity across both sides."""
		return sum(q for _, q in self.yes_levels) + sum(q for _, q in self.no_levels)

	@property
	def spread(self) -> int:
		"""Bid-ask spread in cents: best_yes_ask + best_no_ask - 100.

		Returns 0 if either side is empty.
		"""
		if not self.yes_levels or not self.no_levels:
			return 0
		best_yes_ask = round(self.yes_levels[0][0] * 100)
		best_no_ask = round(self.no_levels[0][0] * 100)
		return best_yes_ask + best_no_ask - 100

	def walk_book(self, side: str, size: int) -> FillResult:
		"""Walk the book for *side* ('yes' or 'no'), accumulating fills.

		Levels are consumed best-to-worst.  Returns a FillResult describing
		the simulated fill.  If the book is empty returns fill_size=0.

		Args:
			side: 'yes' or 'no'
			size: number of contracts to fill

		Returns:
			FillResult with fill details in cents.
		"""
		levels = self.yes_levels if side == "yes" else self.no_levels
		if not levels:
			return FillResult(
				fill_size=0,
				blended_price_cents=0,
				slippage_cents=0,
				fill_pct=0.0,
				intended_size=size,
			)

		best_price_cents = round(levels[0][0] * 100)
		remaining = size
		total_cost_cents = 0
		total_filled = 0

		for price_dollars, qty in levels:
			if remaining <= 0:
				break
			price_cents = round(price_dollars * 100)
			take = min(qty, remaining)
			total_cost_cents += take * price_cents
			total_filled += take
			remaining -= take

		if total_filled == 0:
			return FillResult(
				fill_size=0,
				blended_price_cents=0,
				slippage_cents=0,
				fill_pct=0.0,
				intended_size=size,
			)

		blended = round(total_cost_cents / total_filled)
		slippage = blended - best_price_cents
		fill_pct = total_filled / size

		return FillResult(
			fill_size=total_filled,
			blended_price_cents=blended,
			slippage_cents=slippage,
			fill_pct=fill_pct,
			intended_size=size,
		)


@dataclass
class TickContext:
	"""Context passed to strategies on every market tick.

	Prices are in cents.  orderbook prices (walk_book output) are also cents.
	"""
	ticker: str
	event_ticker: str
	yes_bid: int
	yes_ask: int
	no_bid: int
	no_ask: int
	orderbook: OrderbookSnapshot
	price_history: list[int]
	open_positions: list[dict[str, Any]]
	persisted_state: dict[str, Any]
	market_metadata: dict[str, Any]
	series: str = ""
	is_first_observation: bool = False
	taker_side: str | None = None
	trade_count: int | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_STRIKE_RE = re.compile(r"-T\d+$")


def derive_event_ticker(ticker: str) -> str:
	"""Strip the final -Tnnnn strike suffix from a ticker string.

	If no -T<digits> segment exists at the end, returns ticker unchanged.

	Examples:
		'KXBTC15M-26APR10-T1234' -> 'KXBTC15M-26APR10'
		'KXBTC15M-26APR10'       -> 'KXBTC15M-26APR10'
		'FOO-BAR-TBAZ'           -> 'FOO-BAR-TBAZ'
	"""
	return _STRIKE_RE.sub("", ticker)


# ---------------------------------------------------------------------------
# MarketState
# ---------------------------------------------------------------------------

class MarketState:
	"""Maintains per-ticker price history and orderbook snapshots.

	Args:
		limit: Maximum number of price observations retained per ticker.
	"""

	def __init__(self, limit: int = 100) -> None:
		self._limit = limit
		self._series: dict[str, deque[int]] = {}
		self._first_seen: set[str] = set()
		self._orderbooks: dict[str, OrderbookSnapshot] = {}
		self._metadata: dict[str, dict] = {}

	# ------------------------------------------------------------------
	# Ticker registration
	# ------------------------------------------------------------------

	def register_ticker(self, ticker: str, meta: dict | None = None) -> None:
		"""Register a ticker so it can receive updates."""
		if ticker not in self._series:
			self._series[ticker] = deque(maxlen=self._limit)
		if meta:
			self._metadata[ticker] = meta

	def unregister_ticker(self, ticker: str) -> None:
		"""Remove all state associated with *ticker*."""
		self._series.pop(ticker, None)
		self._metadata.pop(ticker, None)
		self._first_seen.discard(ticker)
		self._orderbooks.pop(ticker, None)

	# ------------------------------------------------------------------
	# Price history
	# ------------------------------------------------------------------

	def update_price(self, ticker: str, price_cents: int) -> bool:
		"""Append a price observation. Auto-registers if unknown.

		Returns:
			True if this is the first observation for the ticker.
		"""
		if ticker not in self._series:
			self._series[ticker] = deque(maxlen=self._limit)
		self._series[ticker].append(price_cents)
		if ticker not in self._first_seen:
			self._first_seen.add(ticker)
			return True
		return False

	def get_price_history(self, ticker: str) -> deque[int] | None:
		"""Return the price history deque for *ticker*, or None if unknown."""
		return self._series.get(ticker)

	def get_metadata(self, ticker: str) -> dict:
		"""Return metadata for *ticker*, or empty dict if unknown."""
		return self._metadata.get(ticker, {})

	def all_tickers(self) -> list[str]:
		"""Return all registered ticker strings."""
		return list(self._series.keys())

	def clear(self) -> None:
		"""Remove all ticker state (used on WS reconnect before re-seeding)."""
		self._series.clear()
		self._first_seen.clear()
		self._orderbooks.clear()
		self._metadata.clear()

	# ------------------------------------------------------------------
	# Orderbook management
	# ------------------------------------------------------------------

	def seed_orderbook(self, ticker: str, snapshot: OrderbookSnapshot) -> None:
		"""Replace the orderbook for *ticker* with *snapshot*."""
		self._orderbooks[ticker] = snapshot

	def get_orderbook(self, ticker: str) -> OrderbookSnapshot | None:
		"""Return the current orderbook snapshot for *ticker*, or None."""
		return self._orderbooks.get(ticker)

	def apply_orderbook_delta(
		self,
		ticker: str,
		side: str,
		price: float,
		delta: int,
	) -> None:
		"""Apply an incremental orderbook update.

		Adds *delta* to the quantity at *price* on *side* ('yes' or 'no').
		Levels with quantity <= 0 are removed.  Levels are kept sorted
		best-to-worst (ascending price for asks).

		Args:
			ticker: Market ticker.
			side:   'yes' or 'no'.
			price:  Price in dollars (Kalshi format).
			delta:  Quantity change (positive = add, negative = remove).
		"""
		ob = self._orderbooks.get(ticker)
		if ob is None:
			return

		levels: list[tuple[float, int]] = (
			ob.yes_levels if side == "yes" else ob.no_levels
		)

		# Update existing level or insert new
		updated = False
		new_levels: list[tuple[float, int]] = []
		for p, q in levels:
			if round(p * 100) == round(price * 100):  # compare in cents to avoid float issues
				new_q = q + delta
				if new_q > 0:
					new_levels.append((p, new_q))
				updated = True
			else:
				new_levels.append((p, q))

		if not updated and delta > 0:
			new_levels.append((price, delta))

		# Sort ascending (best ask first)
		new_levels.sort(key=lambda x: x[0])

		if side == "yes":
			self._orderbooks[ticker] = OrderbookSnapshot(
				yes_levels=new_levels,
				no_levels=ob.no_levels,
			)
		else:
			self._orderbooks[ticker] = OrderbookSnapshot(
				yes_levels=ob.yes_levels,
				no_levels=new_levels,
			)
