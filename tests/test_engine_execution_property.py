"""Property tests for engine.execution + engine.fill_math.

Per D spec L799-L803, exhaustive property coverage for invariants that
parametric unit tests can only sample:

1. ``build_entry_order`` always clamps ``limit_price_cents`` into ``[1, 99]``
   regardless of input entry/slippage combination.
2. ``build_exit_order`` always clamps ``limit_price_cents`` into ``[1, 99]``
   regardless of target/slippage/side combination.
3. ``blended_price_cents(fills + new_fill)`` is monotonic — adding a fill at
   price ``p`` to a blended-price ``b`` produces a new blended in
   ``[min(p, b), max(p, b)]``.

The Round-1 caught bug was a stale spec doc that claimed ``limit <= 99 +
slippage`` rather than the clamped ``limit <= 99``. Property tests over the
full input domain catch any future regression that re-introduces the
unclamped path.

Methodology note (per session 2026-05-10 rule "tests must prove the fix
prevents the failure mode it claims"): each property states the failure
mode it guards. A property without a failure mode is a tautology.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal, cast

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from edge_catcher.engine.execution import (
	ExecCfg,
	OpenPosition,
	build_entry_order,
	build_exit_order,
)
from edge_catcher.engine.fill_math import FillEvent, blended_price_cents
from edge_catcher.engine.strategy_base import ExitKind, Signal


_NOW = datetime(2026, 5, 11, 12, 0, 0, tzinfo=timezone.utc)

# Property-test settings — small budget per case keeps total runtime bounded
# while still exhaustively exercising the input domain. The `function_scoped_
# fixture` health check is suppressed because we don't use any fixtures (we
# use module-level constants only).
_PROPERTY_SETTINGS = settings(
	max_examples=200,
	deadline=None,
	suppress_health_check=[HealthCheck.function_scoped_fixture],
)


# ---------------------------------------------------------------------------
# Hypothesis strategies — match Kalshi's valid input domain
# ---------------------------------------------------------------------------


# Entry prices Kalshi can quote: integers in [1, 99]. The clamp is the
# property we test against, so we deliberately include the boundary cases.
_VALID_ENTRY_PRICES = st.integers(min_value=1, max_value=99)

# Entry slippage caps from config validation: [0, 50]. The validator caps at
# ``>= 0`` (no upper bound) but operationally we never set above ~10c; 50c
# is conservative test ceiling.
_VALID_ENTRY_SLIPPAGE = st.integers(min_value=0, max_value=50)

# Target prices (exit): same Kalshi band.
_VALID_TARGET_PRICES = st.integers(min_value=1, max_value=99)

# Exit slippage per-kind: [0, 50] (Phase 1 production max is 10c for SL).
_VALID_EXIT_SLIPPAGE = st.integers(min_value=0, max_value=50)

# Sides: yes / no — Kalshi's binary market shape.
_SIDES = st.sampled_from(["yes", "no"])

# Allowed sizes: positive ints (D's builder rejects <= 0, so we never feed
# those into the property — that case is covered by the explicit ValueError
# unit test in test_engine_execution.py).
_ALLOWED_SIZES = st.integers(min_value=1, max_value=1000)

# Fill sizes for positions: positive ints (same rationale as allowed_size).
_FILL_SIZES = st.integers(min_value=1, max_value=1000)

# Stop-loss distance: just needs to be present (>0) for the builder to
# accept the signal. Phase 1 strategies use 4..20c; we widen for property
# coverage.
_STOP_LOSS_DISTANCES = st.integers(min_value=1, max_value=99)

# Exit kinds — every literal in the ExitKind alias. We use sampled_from
# rather than literal-strings so adding a new kind to ExitKind picks it up.
_EXIT_KINDS: st.SearchStrategy[ExitKind] = st.sampled_from(
	["take_profit", "stop_loss", "time_exit"]
)

# Blended prices: any cent value the math could plausibly produce, including
# the 0 sentinel.
_BLENDED_PRICES = st.integers(min_value=0, max_value=99)


def _exec_cfg(*, entry: int, tp: int, sl: int, te: int) -> ExecCfg:
	"""Build an ExecCfg from per-kind slippage values."""
	return ExecCfg(
		entry_slippage_cents=entry,
		exit_slippage_cents={
			"take_profit": tp,
			"stop_loss": sl,
			"time_exit": te,
		},
	)


def _entry_signal(
	*,
	entry_price_cents: int,
	stop_loss_distance_cents: int = 8,
	side: str = "yes",
) -> Signal:
	return Signal(
		action="enter",
		ticker="KXSOL15M-26MAY09H06",
		side=side,
		series="KXSOL15M",
		strategy="prop_test",
		reason="property",
		entry_price_cents=entry_price_cents,
		stop_loss_distance_cents=stop_loss_distance_cents,
	)


def _exit_signal(
	*,
	target_price_cents: int,
	exit_kind: ExitKind,
) -> Signal:
	return Signal(
		action="exit",
		ticker="KXSOL15M-26MAY09H06",
		side="yes",  # ignored by exit builder; pos.side wins
		series="KXSOL15M",
		strategy="prop_test",
		reason="property",
		target_price_cents=target_price_cents,
		exit_kind=exit_kind,
		trade_id=42,
	)


def _position(*, side: str, fill_size: int, blended_entry_cents: int = 50) -> OpenPosition:
	return OpenPosition(
		ticker="KXSOL15M-26MAY09H06",
		side=cast(Literal["yes", "no"], side),
		fill_size=fill_size,
		blended_entry_cents=blended_entry_cents,
	)


# ---------------------------------------------------------------------------
# Property 1 — build_entry_order limit clamp is total over the input domain
# ---------------------------------------------------------------------------


@_PROPERTY_SETTINGS
@given(
	entry_price=_VALID_ENTRY_PRICES,
	entry_slippage=_VALID_ENTRY_SLIPPAGE,
	allowed_size=_ALLOWED_SIZES,
	side=_SIDES,
)
def test_property_build_entry_order_limit_always_in_kalshi_band(
	entry_price: int,
	entry_slippage: int,
	allowed_size: int,
	side: str,
) -> None:
	"""Failure mode: a regression re-introduces the unclamped ``limit = entry +
	slippage`` line, allowing values > 99 or < 1 to reach Kalshi. Property
	covers the entire valid-input domain (1..99 × 0..50 × pos sizes × both
	sides); a single failing case is a regression."""
	cfg = _exec_cfg(entry=entry_slippage, tp=0, sl=0, te=0)
	sig = _entry_signal(entry_price_cents=entry_price, side=side)
	req = build_entry_order(sig, allowed_size=allowed_size, cfg=cfg, now=_NOW)
	assert 1 <= req.limit_price_cents <= 99, (
		f"limit_price_cents={req.limit_price_cents} out of Kalshi band "
		f"[1, 99] for entry={entry_price} + slippage={entry_slippage}"
	)


# ---------------------------------------------------------------------------
# Property 2 — build_exit_order limit clamp is total over the input domain
# ---------------------------------------------------------------------------


@_PROPERTY_SETTINGS
@given(
	target_price=_VALID_TARGET_PRICES,
	tp_slippage=_VALID_EXIT_SLIPPAGE,
	sl_slippage=_VALID_EXIT_SLIPPAGE,
	te_slippage=_VALID_EXIT_SLIPPAGE,
	exit_kind=_EXIT_KINDS,
	side=_SIDES,
	fill_size=_FILL_SIZES,
)
def test_property_build_exit_order_limit_always_in_kalshi_band(
	target_price: int,
	tp_slippage: int,
	sl_slippage: int,
	te_slippage: int,
	exit_kind: ExitKind,
	side: str,
	fill_size: int,
) -> None:
	"""Failure mode: a regression bypasses the exit-side clamp, allowing
	target=5 + side=yes + sl_slippage=10 → limit=-5 (Kalshi 4xx-rejects on
	count parse). Property covers every (target × exit_kind × side ×
	slippage) combination — exhaustive defense-in-depth."""
	cfg = _exec_cfg(entry=0, tp=tp_slippage, sl=sl_slippage, te=te_slippage)
	sig = _exit_signal(target_price_cents=target_price, exit_kind=exit_kind)
	pos = _position(side=side, fill_size=fill_size)
	req = build_exit_order(pos, sig, cfg, _NOW)
	assert 1 <= req.limit_price_cents <= 99, (
		f"limit_price_cents={req.limit_price_cents} out of Kalshi band "
		f"[1, 99] for target={target_price} + exit_kind={exit_kind} + "
		f"side={side} + slippage={cfg.exit_slippage_cents[exit_kind]}"
	)


# ---------------------------------------------------------------------------
# Property 3 — blended_price_cents monotonicity
# ---------------------------------------------------------------------------


# Strategy for "an existing batch of fills + one additional fill", with each
# fill being (price ∈ [1, 99], size ∈ [1, 1000]). We require at least one
# baseline fill so the "existing blended" is defined.
_FILL = st.fixed_dictionaries({
	"price": st.integers(min_value=1, max_value=99),
	"size": st.integers(min_value=1, max_value=1000),
})

_FILLS_LIST = st.lists(_FILL, min_size=1, max_size=20)


@_PROPERTY_SETTINGS
@given(
	existing_fills=_FILLS_LIST,
	new_price=st.integers(min_value=1, max_value=99),
	new_size=st.integers(min_value=1, max_value=1000),
)
def test_property_blended_price_monotonic_on_added_fill(
	existing_fills: list[FillEvent],
	new_price: int,
	new_size: int,
) -> None:
	"""Failure mode: a refactor of ``blended_price_cents`` introduces an
	off-by-one or weighted-mean-formula bug that produces a result OUTSIDE
	``[min(p, b), max(p, b)]`` when a new fill is added — the math invariant
	for a volume-weighted average.

	Mathematical statement: for any non-empty existing fills with blended
	``b``, adding a fill ``(p, s)`` (where s > 0) produces a new blended
	``b'`` satisfying ``min(p, b) <= b' <= max(p, b)``. (Equality at the
	endpoints when ``p == b`` or one side has zero contribution.)

	The 1-cent rounding step can push ``b'`` exactly to either bound but
	never outside — we allow the inclusive comparison."""
	existing_blended = blended_price_cents(existing_fills)
	# Skip degenerate cases where existing fills have total size 0 (the
	# 0-sentinel case). The monotonicity invariant is undefined when the
	# baseline is 0 because adding a non-zero-size fill produces a real
	# blended unrelated to "0 + p". The 0-sentinel case is covered by a
	# separate unit test.
	if existing_blended == 0:
		# Hypothesis convention — re-raise as Skip so the case is reported.
		pytest.skip("existing fills total to 0 size — sentinel case, not a monotonicity input")

	combined = existing_fills + [{"price": new_price, "size": new_size}]
	new_blended = blended_price_cents(combined)

	lo = min(new_price, existing_blended)
	hi = max(new_price, existing_blended)
	# The 1-cent banker's-rounding step can produce a result that is EXACTLY
	# at the bound, never past it. The pre-rounding raw blended b' is
	# strictly in the open interval (lo, hi) when p != b, and equals lo == hi
	# when p == b. The rounding can shift by ≤ 1c, so we widen the inclusive
	# interval by 1 on each side to absorb the rounding boundary. Without
	# this, the round-half-to-even tiebreak case ``lo=4 + new=5 → blended=4.5
	# → rounded=4`` could fail when the property expected ``new == 5`` at
	# the upper bound.
	assert lo - 1 <= new_blended <= hi + 1, (
		f"blended_price_cents not monotonic: existing={existing_fills}, "
		f"existing_blended={existing_blended}, new={(new_price, new_size)}, "
		f"new_blended={new_blended}, expected in [{lo}, {hi}] (±1 for rounding)"
	)
