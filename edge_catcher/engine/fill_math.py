"""Blended-price math shared by paper and live executors.

SINGLE source of truth for the volume-weighted average fill price. Both
``PaperExecutor.walk_book_with_ceiling`` (existing inline formula) and
``LiveExecutor._translate_order`` (D's wire-shape translation) MUST call
``blended_price_cents`` so replay-live parity holds byte-exact.

Semantics match paper's existing inline line (engine/executors/paper.py
``blended = round(total_cost_cents / total_filled)``). Test #14 in
``tests/test_engine_fill_math.py`` asserts byte-exact agreement against
that line.
"""
from __future__ import annotations

from typing import Iterable, TypedDict


class FillEvent(TypedDict):
	"""One fill from Kalshi's per-order ``fills`` array (or the paper-side
	equivalent constructed from a book walk).

	``price`` is integer cents (1..99 normal; 0/100 tolerated by the formula).
	``size`` is a contract count and may be **fractional** for a paper-side
	per-level take against a fractional book (live still passes whole ints).
	"""

	price: int
	size: float


def blended_price_cents(fills: Iterable[FillEvent]) -> int:
	"""Volume-weighted average fill price, rounded to the nearest cent.

	For partial-IOC and walked-book entries, the "blended" price is the
	weighted-average across fills::

	    Σ(price_i * size_i) / Σ(size_i)

	Returns 0 if no fills (0-sentinel matching ``FillResult.blended_price_cents``
	convention — downstream code treats 0 as "no fill" / stale fallback).

	The function is total over any iterable of FillEvent — empty iterables
	return 0 rather than raising. Callers needing a hard failure on empty
	input must check ``filled_size`` separately (LiveExecutor does this in
	``_translate_order`` to map filled_count>0 + empty fills to ``pending``).

	Args:
		fills: iterable of FillEvent dicts, each with ``price`` (cents) and
			``size`` (contracts).

	Returns:
		Weighted-average price in cents (rounded), or 0 if total size is 0.
	"""
	total_cost: float = 0.0
	total_size: float = 0.0
	for fill in fills:
		total_cost += fill["price"] * fill["size"]
		total_size += fill["size"]
	if total_size == 0:
		return 0
	return round(total_cost / total_size)


def signed_slippage_cents(*, blended: int, limit: int, action: str) -> int:
	"""Slippage with a uniform sign convention: positive = WORSE than limit
	regardless of buy/sell side. Negative = better than limit.

	Single source of truth shared by ``PaperExecutor.walk_book_with_ceiling``
	and ``LiveExecutor._translate_order`` so F's UI / B's reconciler / any
	downstream slippage analytics can read this field with one interpretation.

	Args:
		blended: Weighted-average actual fill price in cents.
		limit:   The OrderRequest's limit_price_cents (the price we offered).
		action:  ``"buy"`` or ``"sell"`` (from ``OrderRequest.action``).

	Returns:
		``blended - limit`` for buys (positive when we paid more than we asked).
		``limit - blended`` for sells (positive when we received less than we asked).

	Without the sign flip, F's slippage-distribution chart had to know the
	action to interpret the sign; the unified convention lets the UI render
	one histogram for entries + exits without action-aware branching.

	Raises:
		ValueError: if ``action`` is not exactly ``"buy"`` or ``"sell"``.
			This is a shared live-money helper — a silent sell-formula
			fallthrough for an unexpected action string (``"BUY"``, ``""``,
			a future ``"cancel"``) would produce a wrong slippage number
			that silently corrupts F's chart and B's reconciliation. Loud
			failure beats a silent wrong answer (zero-error lens).
	"""
	if action == "buy":
		return blended - limit
	if action == "sell":
		return limit - blended
	raise ValueError(
		f"signed_slippage_cents: action must be 'buy' or 'sell', got {action!r}"
	)
