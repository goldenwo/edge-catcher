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

	Both fields are integers — ``price`` is cents (1..99 in normal operation;
	0/100 are pathological but mathematically tolerated by the formula) and
	``size`` is contract count (>= 0).
	"""

	price: int
	size: int


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
	total_cost = 0
	total_size = 0
	for fill in fills:
		total_cost += fill["price"] * fill["size"]
		total_size += fill["size"]
	if total_size == 0:
		return 0
	return round(total_cost / total_size)
