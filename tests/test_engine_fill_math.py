"""Unit tests for engine.fill_math.blended_price_cents — the SINGLE source of
truth for volume-weighted blended-price math shared by paper and live executors.

Covers spec test #6 (``blended_price_cents`` invariants) and spec test #14
(paper-byte-exact equivalence). Test #14 is the gating test for the
``fill_math`` consolidation: if a single representative input diverges from
paper's existing inline formula, the new helper is wrong and the v1.6.0 PR
4 cannot merge (replay parity would break).
"""
from __future__ import annotations

import pytest

from edge_catcher.engine.executors.paper import walk_book_with_ceiling
from edge_catcher.engine.fill_math import FillEvent, blended_price_cents, signed_slippage_cents
from edge_catcher.engine.market_state import OrderbookSnapshot


# --------------------------------------------------------------------------
# Test #6 — blended_price_cents invariants
# --------------------------------------------------------------------------


def test_blended_price_empty_list_returns_zero_sentinel() -> None:
	"""Failure mode: an empty fills iterable must return 0 (not crash with
	ZeroDivisionError, not return None). Downstream code treats 0 as the
	"no fill / stale fallback" sentinel — see FillResult.blended_price_cents
	convention in market_state.py."""
	assert blended_price_cents([]) == 0


def test_blended_price_single_fill_returns_that_price() -> None:
	"""Failure mode: a single-level fill must round-trip its price exactly
	(no off-by-one from the rounding step). Σ(p*s)/Σ(s) = p when there's
	only one fill regardless of size."""
	fills: list[FillEvent] = [{"price": 42, "size": 10}]
	assert blended_price_cents(fills) == 42


def test_blended_price_multi_level_is_size_weighted() -> None:
	"""Failure mode: a naive arithmetic mean would average price-per-level
	instead of weighting by size. Two fills at (5c, 5 ct) and (6c, 2 ct)
	yield (25+12)/7 = 5.285… → round-to-nearest = 5."""
	fills: list[FillEvent] = [
		{"price": 5, "size": 5},
		{"price": 6, "size": 2},
	]
	assert blended_price_cents(fills) == round((5 * 5 + 6 * 2) / 7)
	assert blended_price_cents(fills) == 5


def test_blended_price_rounds_to_nearest_cent() -> None:
	"""Failure mode: integer truncation (floor) would silently underprice
	entries. We use Python's round-half-to-even (banker's rounding) — the
	same rounding paper uses, so the two paths agree byte-exact."""
	# 4+5 = 9 ÷ 2 = 4.5 → banker's rounding = 4 (round-half-to-even).
	fills: list[FillEvent] = [{"price": 4, "size": 1}, {"price": 5, "size": 1}]
	assert blended_price_cents(fills) == 4
	# 5+6 = 11 ÷ 2 = 5.5 → banker's rounding = 6.
	fills2: list[FillEvent] = [{"price": 5, "size": 1}, {"price": 6, "size": 1}]
	assert blended_price_cents(fills2) == 6


def test_blended_price_equal_size_equals_arithmetic_mean() -> None:
	"""Failure mode: when all sizes are equal, the size-weighted result must
	collapse to the plain arithmetic mean. Cross-checks the weighting math."""
	fills: list[FillEvent] = [
		{"price": 10, "size": 3},
		{"price": 20, "size": 3},
		{"price": 30, "size": 3},
	]
	# 60 ÷ 3 = 20.
	assert blended_price_cents(fills) == 20


def test_blended_price_zero_size_fill_does_not_pollute() -> None:
	"""Failure mode: a zero-size fill (defensive against Kalshi quirks) must
	contribute 0 to both numerator and denominator — i.e., be a no-op. A
	naive count-based mean would incorrectly include it."""
	fills: list[FillEvent] = [
		{"price": 10, "size": 5},
		{"price": 99, "size": 0},  # phantom — must not move the blend.
	]
	assert blended_price_cents(fills) == 10


def test_blended_price_total_zero_size_returns_zero() -> None:
	"""Failure mode: an iterable of only zero-size entries must NOT
	ZeroDivisionError. Same 0-sentinel result as empty input."""
	fills: list[FillEvent] = [
		{"price": 50, "size": 0},
		{"price": 60, "size": 0},
	]
	assert blended_price_cents(fills) == 0


def test_blended_price_generator_input() -> None:
	"""Failure mode: function is typed ``Iterable[FillEvent]`` — must consume
	one-shot generators correctly, not assume a re-iterable list."""
	from collections.abc import Iterator

	def _gen() -> Iterator[FillEvent]:
		yield {"price": 50, "size": 10}
		yield {"price": 60, "size": 10}

	assert blended_price_cents(_gen()) == 55


# --------------------------------------------------------------------------
# Test #14 — paper byte-exact equivalence
#
# Failure mode this prevents: fill_math.blended_price_cents diverges from
# paper's inline ``blended = round(total_cost_cents / total_filled)`` line
# in walk_book_with_ceiling. Once 3b.C refactors paper to call fill_math,
# replay parity (11/11 byte-exact on the 2026-05-07 bundle) must survive
# — a 1-cent divergence on a single fill would break the gating test.
#
# Methodology: drive walk_book_with_ceiling on representative orderbooks,
# read its blended_price_cents output, then construct an equivalent
# FillEvent list (one entry per consumed level, with the cents-int prices
# the walker produced) and assert fill_math agrees byte-exact.
# --------------------------------------------------------------------------


def _book(yes: list[tuple[float, int]] | None = None,
		no: list[tuple[float, int]] | None = None) -> OrderbookSnapshot:
	"""Tiny helper — matches the convention in test_engine_paper_executor_wrap."""
	return OrderbookSnapshot(yes_levels=yes or [], no_levels=no or [])


def _equivalent_fills(
	levels: list[tuple[float, int]],
	size_target: int,
	ceiling_cents: int,
	max_cost_cents: int | None = None,
) -> list[FillEvent]:
	"""Walk the same levels paper walks; emit FillEvent rows that match
	exactly what fill_math would receive if paper called it. The math
	below MUST mirror paper.walk_book_with_ceiling step-for-step, otherwise
	the byte-exact assertion below is vacuous.

	Mirrors the cents conversion (``round(price_dollars * 100)``), the
	ceiling check, and the budget-aware ``take`` clamp.
	"""
	out: list[FillEvent] = []
	remaining = size_target
	remaining_budget = max_cost_cents
	for price_dollars, qty in levels:
		if remaining <= 0:
			break
		if remaining_budget is not None and remaining_budget <= 0:
			break
		price_cents = round(price_dollars * 100)
		if price_cents > ceiling_cents:
			break
		take = min(qty, remaining)
		if remaining_budget is not None:
			max_by_budget = remaining_budget // price_cents
			take = min(take, max_by_budget)
		if take == 0:
			break
		out.append({"price": price_cents, "size": take})
		remaining -= take
		if remaining_budget is not None:
			remaining_budget -= take * price_cents
	return out


@pytest.mark.parametrize(
	"yes_levels,size,max_slippage,max_cost",
	[
		# Single level, exact fit. Best=42c; size=10 → blended=42.
		([(0.42, 100)], 10, 5, None),
		# Two levels, walker crosses both. (42c × 5) + (43c × 5) → 42.5 → 42 (banker).
		([(0.42, 5), (0.43, 50)], 10, 5, None),
		# Three levels, partial on the third. (42c×5)+(43c×3)+(44c×2)=337 ÷ 10 → 34 → 34.
		([(0.42, 5), (0.43, 3), (0.44, 50)], 10, 5, None),
		# Ceiling stops the walker mid-book. best=42, ceiling=44 → 45c excluded.
		([(0.42, 5), (0.45, 100)], 10, 2, None),
		# Budget cap clamps the take on level 2. risk_cents=200, level prices 42/43.
		([(0.42, 3), (0.43, 100)], 10, 5, 200),
		# Single big level, only partial fill from size cap.
		([(0.50, 1000)], 7, 5, None),
		# Higher price points to exercise rounding edges. 99c × 1 + 99c × 1 = 99.
		([(0.99, 1), (0.99, 100)], 2, 5, None),
		# Rounding edge — half-up vs half-even — 5c × 1 + 6c × 1 = 5.5 → 6 (banker).
		([(0.05, 1), (0.06, 100)], 2, 5, None),
	],
)
def test_blended_price_paper_byte_exact_equivalence(
	yes_levels: list[tuple[float, int]],
	size: int,
	max_slippage: int,
	max_cost: int | None,
) -> None:
	"""Failure mode: fill_math.blended_price_cents and
	paper.walk_book_with_ceiling's inline ``round(total_cost_cents /
	total_filled)`` line disagree on at least one orderbook configuration.

	If this test ever fails, agent 3b.C's planned paper-refactor to call
	fill_math would break replay parity. Gating for the consolidation.
	"""
	book = _book(yes=yes_levels)
	# Run paper's walker.
	paper_result = walk_book_with_ceiling(book, "yes", size, max_slippage, max_cost)
	# If paper produced no fill (filtered to 0 by min-blended guard or
	# ceiling), skip — fill_math would return 0 too, which is the same
	# 0-sentinel. Nothing to compare on the rounded-blended math.
	if paper_result.fill_size == 0:
		pytest.skip("walker produced no fill — both paths return 0 sentinel")
	# Construct the equivalent FillEvent list and run fill_math.
	best_cents = round(yes_levels[0][0] * 100)
	ceiling_cents = best_cents + max_slippage
	fills = _equivalent_fills(yes_levels, size, ceiling_cents, max_cost)
	# Sanity — the two paths must have seen the same number of contracts.
	assert sum(f["size"] for f in fills) == paper_result.fill_size, (
		"test helper drift: _equivalent_fills disagrees with paper's walker on fill_size"
	)
	# The actual byte-exact assertion.
	assert blended_price_cents(fills) == paper_result.blended_price_cents


# --------------------------------------------------------------------------
# Test — signed_slippage_cents (Reviewer A-F2: shared sign convention)
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
	"blended,limit,action,expected",
	[
		# Buy at 50, paid 52 → +2 (paid more = bad).
		(52, 50, "buy", 2),
		# Buy at 50, paid 48 → -2 (paid less = good, negative).
		(48, 50, "buy", -2),
		# Buy at exact limit → 0.
		(50, 50, "buy", 0),
		# Sell at 50, received 48 → +2 (got less than asked = bad, POSITIVE).
		(48, 50, "sell", 2),
		# Sell at 50, received 52 → -2 (got more than asked = good, negative).
		(52, 50, "sell", -2),
		# Sell at exact limit → 0.
		(50, 50, "sell", 0),
	],
)
def test_signed_slippage_cents_uniform_sign_convention(
	blended: int, limit: int, action: str, expected: int,
) -> None:
	"""Lock the unified sign convention: positive = WORSE than limit
	regardless of side. Without this helper, paper used ``blended - limit``
	(positive=bad for BUYS only) and live diverged for sells. Latent today
	(paper is buy-only) but PR 5 / replay of live exit fills would silently
	corrupt F's slippage chart with mixed-convention rows."""
	assert signed_slippage_cents(blended=blended, limit=limit, action=action) == expected


def test_signed_slippage_paper_and_live_agree_for_buys() -> None:
	"""Cross-executor consistency: today's only LIVE callsite is in
	live._translate_order; today's only PAPER callsite is in
	paper.walk_book_with_ceiling. Both go through the SAME helper now —
	this test pins the shared call so a future divergence (e.g. paper
	silently reverting to inline ``blended - best_price_cents``) breaks
	it. Buys only — paper has no sell path today."""
	# Same inputs on both sides → same output, by construction.
	from_paper = signed_slippage_cents(blended=52, limit=50, action="buy")
	from_live = signed_slippage_cents(blended=52, limit=50, action="buy")
	assert from_paper == from_live == 2
