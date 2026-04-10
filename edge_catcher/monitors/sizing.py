"""Sizing pipeline for the paper trading engine.

Pure functions that convert a risk budget + orderbook state into a fill decision.
"""

from __future__ import annotations

from edge_catcher.monitors.market_state import FillResult, OrderbookSnapshot


def compute_raw_size(risk_cents: int, entry_price_cents: int) -> int:
	"""Convert a dollar risk budget to a contract count.

	Args:
		risk_cents:        Maximum cents to risk on this trade.
		entry_price_cents: Entry price per contract in cents.

	Returns:
		Number of contracts (floor division). May be 0 if budget < price.

	Raises:
		ValueError: If entry_price_cents <= 0.
	"""
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
	total_cost_cents = 0
	total_filled = 0

	for price_dollars, qty in levels:
		if remaining <= 0:
			break
		price_cents = round(price_dollars * 100)
		if price_cents > ceiling_cents:
			break
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
