"""Sizing pipeline for the paper trading engine.

Pure functions that convert a risk budget + orderbook state into a fill decision.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Literal

from edge_catcher.engine.fill_math import FillEvent, blended_price_cents
from edge_catcher.engine.market_state import FillResult, OrderbookSnapshot

log = logging.getLogger(__name__)


FillSkipReason = Literal["stale_book", "empty_book", "budget_too_small", "below_min_fill"]


@dataclass(frozen=True)
class FillSkip:
	"""Returned by resolve_fill when a trade should not be booked.

	The reason distinguishes skip categories so operational metrics can
	answer questions like "how many entries got skipped as stale this hour".
	"""
	reason: FillSkipReason


def compute_raw_size(risk_cents: int, entry_price_cents: int) -> int:
	"""Convert a dollar risk budget to a contract count.

	Args:
		risk_cents:        Maximum cents to risk on this trade.
		entry_price_cents: Entry price per contract in cents.

	Returns:
		Number of contracts (floor division). May be 0 if budget < price.

	Raises:
		ValueError: If entry_price_cents <= 0 or risk_cents < 0.
	"""
	if risk_cents < 0:
		raise ValueError(
			f"risk_cents must be >= 0, got {risk_cents}"
		)
	if entry_price_cents <= 0:
		raise ValueError(
			f"entry_price_cents must be > 0, got {entry_price_cents}"
		)
	return risk_cents // entry_price_cents


def walk_book_with_ceiling(
	book: OrderbookSnapshot,
	side: str,
	size: int,
	max_slippage_cents: int,
	max_cost_cents: int | None = None,
) -> FillResult:
	"""Walk the book with a slippage ceiling.

	Same as OrderbookSnapshot.walk_book but stops consuming levels once
	the price exceeds best_price + max_slippage_cents.  The ceiling is
	inclusive — the best price is always eligible.

	Args:
		book:               Orderbook snapshot.
		side:               'yes' or 'no'.
		size:               Target number of contracts.
		max_slippage_cents: Maximum allowed price above best in cents.
		max_cost_cents:     Optional hard cap on total fill cost. When
		                    set, the walker stops consuming levels the
		                    moment adding one more contract at the
		                    current price would push total_cost_cents
		                    above this value. Used by resolve_fill to
		                    enforce ``risk_per_trade_cents`` exactly —
		                    prevents the longshot-entry oversizing bug
		                    where a 2¢ signal gets a 4¢+ book walk that
		                    blows through the risk budget.

	Returns:
		FillResult with intended_size set to *size*.
	"""
	levels = book.yes_levels if side == "yes" else book.no_levels
	if not levels or size <= 0:
		return FillResult(
			fill_size=0,
			blended_price_cents=0,
			slippage_cents=0,
			fill_pct=0.0,
			intended_size=size,
		)

	best_price_cents = round(levels[0][0] * 100)
	ceiling_cents = best_price_cents + max_slippage_cents
	remaining = size
	remaining_budget = max_cost_cents  # None = unlimited
	# Collect per-level fills as we walk so we can hand the list to
	# ``fill_math.blended_price_cents`` — the SINGLE source of truth for the
	# volume-weighted-average computation. Both paper (this function) and
	# live (LiveExecutor._translate_order) MUST route through that helper so
	# replay-live parity holds byte-exact. The math is identical to the
	# pre-D inline ``round(total_cost_cents / total_filled)`` line (proven
	# byte-exact by tests/test_engine_fill_math.py test #14).
	fills: list[FillEvent] = []
	total_filled = 0

	for price_dollars, qty in levels:
		if remaining <= 0:
			break
		if remaining_budget is not None and remaining_budget <= 0:
			break
		price_cents = round(price_dollars * 100)
		if price_cents > ceiling_cents:
			break
		take = min(qty, remaining)
		# Cap take so total cost never exceeds max_cost_cents. Integer
		# floor division ensures we stay strictly at-or-below the budget.
		if remaining_budget is not None:
			max_by_budget = remaining_budget // price_cents
			take = min(take, max_by_budget)
		if take == 0:
			break
		fills.append({"price": price_cents, "size": take})
		total_filled += take
		remaining -= take
		if remaining_budget is not None:
			remaining_budget -= take * price_cents

	if total_filled == 0:
		return FillResult(
			fill_size=0,
			blended_price_cents=0,
			slippage_cents=0,
			fill_pct=0.0,
			intended_size=size,
		)

	blended = blended_price_cents(fills)
	# Guard: if the book has sub-cent prices that round to 0, the blended
	# price is unusable as a cost basis. Treat as no fill so the trade is
	# skipped rather than entered with a corrupt 0¢ price.
	if blended == 0:
		return FillResult(
			fill_size=0,
			blended_price_cents=0,
			slippage_cents=0,
			fill_pct=0.0,
			intended_size=size,
		)
	slippage = blended - best_price_cents
	fill_pct = total_filled / size

	return FillResult(
		fill_size=total_filled,
		blended_price_cents=blended,
		slippage_cents=slippage,
		fill_pct=fill_pct,
		intended_size=size,
	)


def validate_sizing_config(config: dict) -> None:
	"""Validate that the sizing config section has all required keys.

	Raises:
		ValueError: If any key is missing or invalid.
	"""
	sizing = config.get("sizing")
	if not sizing or not isinstance(sizing, dict):
		raise ValueError(
			"Config missing 'sizing' section. Add:\n"
			"sizing:\n"
			"  risk_per_trade_cents: 200\n"
			"  max_slippage_cents: 2\n"
			"  min_fill: 3"
		)

	risk = sizing.get("risk_per_trade_cents")
	if risk is None or risk <= 0:
		raise ValueError(
			f"sizing.risk_per_trade_cents must be > 0, got {risk!r}. "
			"This is the max cents to risk per trade (e.g. 200 = $2.00)."
		)

	slippage = sizing.get("max_slippage_cents")
	if slippage is None or slippage < 0:
		raise ValueError(
			f"sizing.max_slippage_cents must be >= 0, got {slippage!r}. "
			"This caps how far above best price the fill can walk."
		)

	min_fill = sizing.get("min_fill")
	if min_fill is None or min_fill < 1:
		raise ValueError(
			f"sizing.min_fill must be >= 1, got {min_fill!r}. "
			"Trades with fewer fillable contracts are skipped."
		)


def resolve_fill(
	config: dict,
	entry_price_cents: int,
	side: str,
	book: OrderbookSnapshot,
) -> FillResult | FillSkip:
	"""Run the sizing pipeline: risk budget → book walk → min-fill gate.

	Reads from config["sizing"]:
	  - risk_per_trade_cents: passed to compute_raw_size
	  - max_slippage_cents: passed to walk_book_with_ceiling
	  - min_fill: gate check on fill_size
	  - require_fresh_book: (optional, default True) if True, skip entries
	    when the fill side's orderbook is empty OR its best price diverges
	    from entry_price (see two-gate stale-book rule below).

	Three fill-gate cases:
	  1. Empty fill side → no one is offering on the side we want to buy.
	     Skipped as FillSkip("empty_book") when require_fresh_book=True.
	     Reason: the "entry_price fallback" path produces phantom fills when
	     the ticker's reported yes_ask is a derived/estimated value not a
	     fillable offer. The legacy fallback remains only when
	     require_fresh_book=False.
	  2. Populated but best diverges from entry_price (abs > 10c) →
	     phantom liquidity or WS lag. Skipped as FillSkip("stale_book") when
	     require_fresh_book=True.
	  3. Populated + fresh → normal walked-book fill.

	Returns:
		FillResult if trade should proceed, FillSkip with a reason if not.
	"""
	sizing = config["sizing"]
	risk_cents = sizing["risk_per_trade_cents"]
	max_slippage = sizing["max_slippage_cents"]
	min_fill_threshold = sizing["min_fill"]
	require_fresh_book = sizing.get("require_fresh_book", True)

	raw_size = compute_raw_size(risk_cents, entry_price_cents)
	if raw_size == 0:
		log.debug("Skip: budget %dc too small for %dc entry", risk_cents, entry_price_cents)
		return FillSkip(reason="budget_too_small")

	levels = book.yes_levels if side == "yes" else book.no_levels
	book_empty = not levels
	book_populated_but_stale = False
	if not book_empty:
		best_book_cents = round(levels[0][0] * 100)
		# Simple absolute threshold: best price > 10c from entry_price = stale.
		# A tighter relative-divergence gate was tried but regressed legitimate
		# walker-walks-down-to-real-book opportunities; the absolute 10c rule
		# is sufficient once trade-channel bid/ask is already sourced from the
		# orderbook (see dispatch.py).
		if abs(best_book_cents - entry_price_cents) > 10:
			log.debug(
				"Book populated but stale: best=%dc entry=%dc",
				best_book_cents, entry_price_cents,
			)
			book_populated_but_stale = True

	if book_populated_but_stale and require_fresh_book:
		log.info(
			"Skip: populated-but-stale book (best diverges from entry_price) "
			"with require_fresh_book=true",
		)
		return FillSkip(reason="stale_book")

	if book_empty and require_fresh_book:
		log.info(
			"Skip: empty fill side for %s (entry=%dc) with require_fresh_book=true",
			side, entry_price_cents,
		)
		return FillSkip(reason="empty_book")

	if book_empty or book_populated_but_stale:
		# Legacy fallback — only reachable when require_fresh_book=False.
		if raw_size < min_fill_threshold:
			return FillSkip(reason="below_min_fill")
		return FillResult(
			fill_size=raw_size,
			blended_price_cents=0,   # signals stale book; trade_store uses entry_price for PnL
			slippage_cents=0,
			fill_pct=1.0,
			intended_size=raw_size,
		)

	# Pass risk_cents as the walker's hard cost cap. `compute_raw_size`
	# computes contracts from the signal's entry_price, but the real
	# book walk can fill at higher prices (2-5¢ divergence is common
	# and stays under the stale-book 10¢ threshold). Without this cap,
	# longshot entries at 2¢ signal / 4¢ actual fill silently doubled
	# the configured per-trade risk — the 2026-04-14 paper-trader
	# oversizing bug.
	fill = walk_book_with_ceiling(
		book, side, raw_size, max_slippage, max_cost_cents=risk_cents,
	)

	if fill.fill_size < min_fill_threshold:
		log.debug(
			"Skip: fill %d < min_fill %d (wanted %d %s)",
			fill.fill_size, min_fill_threshold, raw_size, side,
		)
		return FillSkip(reason="below_min_fill")

	return fill


# ---------------------------------------------------------------------------
# PaperExecutor
# ---------------------------------------------------------------------------

import json  # noqa: E402 — imported here to keep the pure-function block above clean

from edge_catcher.engine.executor import OrderRequest, OrderResult  # noqa: E402
from edge_catcher.engine.market_state import MarketState  # noqa: E402


class PaperExecutor:
	"""Simulated executor — walks the orderbook in MarketState, returns deterministic fills.

	Composes the module-level pure functions (resolve_fill / walk_book_with_ceiling /
	compute_raw_size) with the Executor protocol shape. No new fill semantics — paper
	behavior is byte-exact across the migration.

	Fees: NOT computed here. trade_store.record_trade computes them via
	STANDARD_FEE.calculate at row-write time. The OrderResult.fees_cents field is
	deferred to D when LiveExecutor needs to surface Kalshi-reported fees.
	"""

	def __init__(self, market_state: MarketState, config: dict) -> None:
		# `config` is the same dict threaded through the engine. The
		# resolve_fill function above (lines 195-298, ported verbatim from the
		# pre-G monitors/sizing.py) reads:
		#   config["sizing"]:
		#     - risk_per_trade_cents      (int)
		#     - max_slippage_cents        (int)
		#     - min_fill                  (int — NOT "min_fill_size")
		#     - require_fresh_book        (bool, optional, default True)
		# No new keys introduced by G.
		self._ms = market_state
		self._config = config

	async def place(self, req: OrderRequest) -> OrderResult:
		# Async signature with sync-only body is the locked pattern (plan §1.1):
		# the orderbook walk is pure CPU with no I/O, but we adopt `async def`
		# so dispatch can `await executor.place(...)` uniformly across paper
		# and live executors. No `await` is needed in this body.
		#
		# MarketState.get_orderbook returns Optional; the dispatch path defaults
		# to an empty OrderbookSnapshot for unseeded tickers (see
		# engine/dispatch.py:465), and resolve_fill treats empty books as a
		# FillSkip(empty_book) when require_fresh_book is on. Match that
		# semantics here so the executor never sees a None book.
		snapshot = self._ms.get_orderbook(req.ticker) or OrderbookSnapshot([], [])
		fill_or_skip = resolve_fill(
			self._config, req.limit_price_cents, req.side, snapshot,
		)
		if isinstance(fill_or_skip, FillSkip):
			return OrderResult(
				status="rejected",
				intended_size=req.size_contracts,
				filled_size=0,
				blended_entry_cents=0,
				fill_pct=0.0,
				slippage_cents=0,
				book_depth=snapshot.depth,
				book_snapshot=None,
				rejection_reason=fill_or_skip.reason,
			)
		fill = fill_or_skip                              # FillResult
		side_levels = (
			snapshot.yes_levels if req.side == "yes" else snapshot.no_levels
		)
		return OrderResult(
			status="filled",
			intended_size=fill.intended_size,
			filled_size=fill.fill_size,
			blended_entry_cents=fill.blended_price_cents,  # 0-sentinel preserved verbatim
			fill_pct=fill.fill_pct,
			slippage_cents=fill.slippage_cents,
			book_depth=snapshot.depth,
			book_snapshot=json.dumps(side_levels),
		)
