"""Market state data models for the paper trading framework.

Contains OrderbookSnapshot, FillResult, TickContext, MarketState,
and the derive_event_ticker helper.
"""

import logging
import math
import re
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from edge_catcher.engine.fill_math import FillEvent, blended_price_cents

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Orderbook quantity precision
# ---------------------------------------------------------------------------

# Canonical Kalshi-quantity decimal precision. Kalshi sends <= 2 dp; 4 gives
# margin and erases float64 accumulation noise so the stored book is
# deterministic (see spec 4.5). Single home for the precision.
_QTY_DP = 4

# Upper magnitude bound for a single quantity. The int->float widening removed
# the arbitrary-precision headroom int() had: two ~1e308 levels (or deltas
# accumulating there) overflow OrderbookSnapshot.depth to inf, so round(depth)
# raises OverflowError and json.dumps emits non-standard "Infinity" into
# persisted bundles (corrupting replay). No real Kalshi level approaches this;
# 1e9 contracts is far above any market yet far below the float64 overflow
# regime, keeping summed depth exactly representable. Out-of-range is rejected
# like non-finite (None -> caller skips the level / no-ops the delta).
_QTY_MAX = 1e9

_rejected_qty_count = 0


def _parse_qty(raw: object) -> float | None:
	"""Parse a Kalshi fixed-point quantity to rounded float contracts.

	Returns ``None`` when *raw* is unparseable, non-finite, or implausibly large
	(``abs > _QTY_MAX``), so callers skip the level / no-op the delta exactly as
	they treat a malformed frame today. Restores the rejection that
	``int(float(...))`` used to provide via OverflowError/ValueError before the
	int cast was dropped, plus the magnitude headroom arbitrary-precision int
	summation implicitly gave (a float book sums to inf where an int book never
	did).
	"""
	global _rejected_qty_count
	try:
		f = float(raw)  # type: ignore[arg-type]
	except (TypeError, ValueError, OverflowError):
		# OverflowError: float() of a Python int too large to represent as a
		# float64 (e.g. a JSON integer literal with >308 digits). Caught here so
		# the contract holds at EVERY call site — recovery/replay-seed/snapshot
		# invoke _parse_qty outside any try, unlike the V2 delta path.
		return None
	if not math.isfinite(f) or abs(f) > _QTY_MAX:
		_rejected_qty_count += 1
		# Rate-limited: a non-finite / out-of-range qty is an anomaly (Kalshi's
		# wire is well-defined). Log the first few and then sparsely.
		if _rejected_qty_count <= 10 or _rejected_qty_count % 1000 == 0:
			log.warning("rejected non-finite/out-of-range orderbook qty %r (count=%d)", raw, _rejected_qty_count)
		return None
	return round(f, _QTY_DP)


# ---------------------------------------------------------------------------
# Tradeable-price guard
# ---------------------------------------------------------------------------

def _is_tradeable_cents(price_dollars: float) -> bool:
	"""Return True iff *price_dollars* is an integer cent in [1¢, 99¢].

	Kalshi markets trade only at integer cents; sub-cent ghost levels
	(0.1¢–0.9¢) have been observed in REST /orderbook responses for 15m
	crypto series and must be filtered before they reach the in-memory
	book. A 1e-3 tolerance is required because float representations of
	decimal sub-cents (e.g. 0.007 * 100 = 0.70000000000001) otherwise
	round to 1 and would be falsely accepted as "1¢".
	"""
	price_cents_float = price_dollars * 100
	price_cents = round(price_cents_float)
	if not (1 <= price_cents <= 99):
		return False
	return abs(price_cents_float - price_cents) < 1e-3


# ---------------------------------------------------------------------------
# Fill trimming
# ---------------------------------------------------------------------------

def _trim_fills(fills: list[FillEvent], target: int) -> list[FillEvent]:
	"""Prefix of *fills* whose sizes sum to exactly *target* whole contracts,
	trimming the last included fill if needed (priced over exactly the filled
	quantity, per spec 4.4 rule (b))."""
	out: list[FillEvent] = []
	remaining: float = target
	for f in fills:
		if remaining <= 0:
			break
		take = min(f["size"], remaining)
		out.append({"price": f["price"], "size": take})
		remaining -= take
	return out


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

	Levels are (price_dollars: float, quantity: float) resting BIDS per side
	(Kalshi wire shape), sorted ascending — the BEST bid is the LAST
	element; levels[0] is the penny floor.  Never read levels[0] as an
	ask: cross the book via implied_asks()/best_* accessors.

	Quantities are fractional contracts: Kalshi sends fixed-point qty (e.g.
	"20.56", sub-1.0 levels like "0.65"); they are stored rounded to _QTY_DP
	via _parse_qty at every ingest boundary, so the book is always finite and
	deterministic.  Fill walkers floor the *total* fill to whole contracts.
	"""
	yes_levels: list[tuple[float, float]]
	no_levels: list[tuple[float, float]]

	@property
	def depth(self) -> float:
		"""Total resting quantity across both sides."""
		return sum(q for _, q in self.yes_levels) + sum(q for _, q in self.no_levels)

	def implied_asks(self, side: str) -> list[tuple[int, float]]:
		"""Implied ask ladder (price_cents, qty) to BUY *side*, cheapest first.

		yes_levels/no_levels hold resting BIDS (Kalshi wire shape, dollars,
		ascending — best bid LAST).  Buying side S crosses the OPPOSITE
		side's bids: an opposite bid at price p implies an ask at 100 − p,
		so the cheapest ask comes from the highest opposite bid.  Empty
		opposite side ⇒ no implied liquidity ⇒ [].
		"""
		opposite = self.no_levels if side == "yes" else self.yes_levels
		return [
			(100 - round(p * 100), q)
			for p, q in sorted(opposite, key=lambda lvl: lvl[0], reverse=True)
		]

	@property
	def best_yes_bid(self) -> int | None:
		"""Best (highest) resting YES bid in cents, or None if side empty."""
		if not self.yes_levels:
			return None
		return round(max(p for p, _ in self.yes_levels) * 100)

	@property
	def best_no_bid(self) -> int | None:
		"""Best (highest) resting NO bid in cents, or None if side empty."""
		if not self.no_levels:
			return None
		return round(max(p for p, _ in self.no_levels) * 100)

	@property
	def best_yes_ask(self) -> int | None:
		"""Best implied YES ask in cents (100 − best NO bid), or None."""
		no_bid = self.best_no_bid
		return None if no_bid is None else 100 - no_bid

	@property
	def best_no_ask(self) -> int | None:
		"""Best implied NO ask in cents (100 − best YES bid), or None."""
		yes_bid = self.best_yes_bid
		return None if yes_bid is None else 100 - yes_bid

	@property
	def spread(self) -> int:
		"""Bid-ask spread in cents: best_yes_ask + best_no_ask − 100.

		Equivalently 100 − (best_yes_bid + best_no_bid): the no-arb gap
		between the implied asks.  Non-negative on a sane book.
		Returns 0 if either side is empty (unknown, prior sentinel kept).
		"""
		yes_bid = self.best_yes_bid
		no_bid = self.best_no_bid
		if yes_bid is None or no_bid is None:
			return 0
		return 100 - (yes_bid + no_bid)

	def walk_book(self, side: str, size: int) -> FillResult:
		"""Walk the implied-ask ladder for *side*, accumulating fills.

		Consumes ``implied_asks(side)`` cheapest-first (the opposite side's
		resting bids, converted at 100 − p).  Returns a FillResult in cents.
		If there is no implied liquidity returns fill_size=0.

		Args:
			side: 'yes' or 'no'
			size: number of contracts to fill

		Returns:
			FillResult with fill details in cents.
		"""
		levels = self.implied_asks(side)
		if not levels or size <= 0:
			return FillResult(
				fill_size=0,
				blended_price_cents=0,
				slippage_cents=0,
				fill_pct=0.0,
				intended_size=size,
			)

		best_price_cents = levels[0][0]
		remaining: float = size
		fills: list[FillEvent] = []
		for price_cents, qty in levels:
			if remaining <= 0:
				break
			take = min(qty, remaining)
			fills.append({"price": price_cents, "size": take})
			remaining -= take

		# Round before the int() floor: the per-level takes are 4dp-exact but
		# their float64 sum can carry downward noise (a true 4.0 summing to
		# 3.9999999999999996), which a bare int() would floor to 3 — dropping a
		# whole contract. Rounding to _QTY_DP first recovers the true 4dp total.
		fill_size = int(round(sum(f["size"] for f in fills), _QTY_DP))  # floor to whole contracts (rule a)
		if fill_size == 0:
			return FillResult(
				fill_size=0,
				blended_price_cents=0,
				slippage_cents=0,
				fill_pct=0.0,
				intended_size=size,
			)

		blended = blended_price_cents(_trim_fills(fills, fill_size))  # rule (b): VWAP over fill_size
		slippage = blended - best_price_cents
		fill_pct = fill_size / size  # rule (c)

		return FillResult(
			fill_size=fill_size,
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
	# wall-clock for this tick; set by dispatch. Strategies needing time-to-close read this.
	now: datetime | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_STRIKE_RE = re.compile(r"-[TB][\d.]+$")


def derive_event_ticker(ticker: str) -> str:
	"""Strip the final strike suffix from a ticker string.

	Handles both -Tnnnn (integer strike) and -Bn.nnn (decimal strike) suffixes.
	If no recognised strike segment exists at the end, returns ticker unchanged.

	Examples:
		'KXBTC15M-26APR10-T1234'          -> 'KXBTC15M-26APR10'
		'KXSERIES-25JAN0112-B1.2345'      -> 'KXSERIES-25JAN0112'
		'KXBTC15M-26APR10'                -> 'KXBTC15M-26APR10'
		'FOO-BAR-TBAZ'                    -> 'FOO-BAR-TBAZ'
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
		"""Register a ticker so it can receive updates.

		Metadata merge is intentionally a "don't-clobber" update: only keys
		whose new value is non-None are written.  This means:

		* A present falsy value (``""`` or ``0``) in *meta* DOES overwrite the
		  stored key — falsy is not the same as absent.
		* A caller CANNOT clear a field by re-registering it with ``None`` —
		  ``None`` is silently skipped.  This is deliberate: it protects rich
		  metadata like ``floor_strike`` and ``close_time`` (set by the first
		  full registration) from being wiped by a later partial or meta-less
		  call that happens to pass ``None`` for those keys.
		"""
		if ticker not in self._series:
			self._series[ticker] = deque(maxlen=self._limit)
		if meta:
			existing = self._metadata.setdefault(ticker, {})
			existing.update({k: v for k, v in meta.items() if v is not None})

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

	def get_yes_ask(self, ticker: str) -> int | None:
		"""Best implied YES ask in cents (100 − best NO bid), or None if unknown.

		Preferred over reading trade WS `yes_price` — trade messages carry the
		executed price of a completed trade, which can be off-book."""
		ob = self._orderbooks.get(ticker)
		if ob is None:
			return None
		return ob.best_yes_ask

	def get_yes_bid(self, ticker: str) -> int | None:
		"""Best resting YES bid in cents, or None if unknown."""
		ob = self._orderbooks.get(ticker)
		if ob is None:
			return None
		return ob.best_yes_bid

	def apply_orderbook_delta(
		self,
		ticker: str,
		side: str,
		price: float,
		delta: float,
	) -> None:
		"""Apply an incremental orderbook update.

		Adds *delta* to the quantity at *price* on *side* ('yes' or 'no').
		Levels with quantity <= 0 are removed.  Levels are kept sorted
		(ascending price; resting bids — best bid last).

		The accumulated quantity is re-rounded to _QTY_DP so the stored book
		stays finite and float64 accumulation noise can't leave a phantom
		~1e-15 level that the `<= 0` removal test would miss.  Callers feed a
		*delta* already sanitized by _parse_qty (rounded + finite).

		Args:
			ticker: Market ticker.
			side:   'yes' or 'no'.
			price:  Price in dollars (Kalshi format).
			delta:  Quantity change (positive = add, negative = remove).
		"""
		# Reject sub-cent / out-of-range ghost deltas — see _is_tradeable_cents.
		if not _is_tradeable_cents(price):
			return

		ob = self._orderbooks.get(ticker)
		if ob is None:
			return

		levels: list[tuple[float, float]] = (
			ob.yes_levels if side == "yes" else ob.no_levels
		)

		# Update existing level or insert new
		updated = False
		new_levels: list[tuple[float, float]] = []
		for p, q in levels:
			if round(p * 100) == round(price * 100):  # compare in cents to avoid float issues
				new_q = round(q + delta, _QTY_DP)
				# Keep iff in (0, _QTY_MAX]: <= 0 removes the level; > _QTY_MAX is an
				# anomalous accumulation (each delta is already _QTY_MAX-bounded at ingest,
				# so this is unreachable in practice) — drop it so the stored total stays
				# finite + bounded, keeping depth / round() / JSON safe (defense-in-depth).
				if 0 < new_q <= _QTY_MAX:
					new_levels.append((p, new_q))
				updated = True
			else:
				new_levels.append((p, q))

		if not updated and delta > 0:
			new_levels.append((price, delta))

		# Sort ascending (wire shape: bids low→high, best bid last)
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
