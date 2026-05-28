"""Wrapping-correctness for PaperExecutor.place — preserves resolve_fill semantics."""
from __future__ import annotations

import json

import pytest

from edge_catcher.engine.executor import OrderRequest
from edge_catcher.engine.executors.paper import (
	FillSkip, PaperExecutor, resolve_fill,
)
from edge_catcher.engine.market_state import OrderbookSnapshot


_DEFAULT_YES = [(0.42, 100), (0.43, 50)]
_DEFAULT_NO = [(0.58, 100), (0.59, 50)]


def _canned_book(yes_levels=None, no_levels=None) -> OrderbookSnapshot:
	# Adjust to match real OrderbookSnapshot constructor at impl time.
	# Use sentinel None to distinguish "not supplied" from "supplied as empty list".
	return OrderbookSnapshot(
		yes_levels=_DEFAULT_YES if yes_levels is None else yes_levels,
		no_levels=_DEFAULT_NO if no_levels is None else no_levels,
	)


def _canned_config() -> dict:
	return {
		"sizing": {
			"risk_per_trade_cents": 200,        # $2 risk
			"max_slippage_cents": 5,
			"min_fill": 1,
			"require_fresh_book": True,
		},
	}


def _canned_request(side="yes", limit=42) -> OrderRequest:
	return OrderRequest(
		ticker="KXSOL15M-25-T1",
		series="KXSOL15M",
		side=side,
		size_contracts=4,
		limit_price_cents=limit,
		strategy="strat-34",
		client_order_id="strat-34-KXSOL15M-1715000000000",
	)


class _StubMarketState:
	def __init__(self, book: OrderbookSnapshot) -> None:
		self._book = book

	def get_orderbook(self, ticker: str) -> OrderbookSnapshot:
		return self._book


@pytest.mark.asyncio
async def test_filled_path_maps_FillResult_to_OrderResult_field_by_field():
	book = _canned_book()
	cfg = _canned_config()
	ms = _StubMarketState(book)
	req = _canned_request()

	fill = resolve_fill(cfg, req.limit_price_cents, req.side, book)
	assert not isinstance(fill, FillSkip), "canned book should produce a fill"

	executor = PaperExecutor(market_state=ms, config=cfg)
	result = await executor.place(req)

	assert result.status == "filled"
	assert result.intended_size == fill.intended_size
	assert result.filled_size == fill.fill_size
	assert result.blended_entry_cents == fill.blended_price_cents
	assert result.fill_pct == fill.fill_pct
	assert result.slippage_cents == fill.slippage_cents
	assert result.book_depth == book.depth
	assert result.book_snapshot == json.dumps(book.yes_levels if req.side == "yes" else book.no_levels)


@pytest.mark.asyncio
async def test_blended_zero_sentinel_preserved():
	"""When resolve_fill returns blended_price_cents == 0, OrderResult.blended_entry_cents
	MUST be 0 verbatim (not None, not the limit price). Trade store relies on the
	0-sentinel to fall back to entry_price at close time.

	Previously this test asserted the sentinel only inside ``if result.status
	== "filled"`` — a refactor that made the executor always reject on empty
	yes_levels would let the test pass vacuously (no assertion runs). Now
	BOTH branches assert: filled MUST be 0; rejected MUST carry an expected
	reason. Any other status (e.g. pending — a LIVE-only outcome) fails.
	"""
	cfg = _canned_config()
	cfg["sizing"]["require_fresh_book"] = False
	book = _canned_book(yes_levels=[])
	ms = _StubMarketState(book)
	req = _canned_request(side="yes", limit=42)

	executor = PaperExecutor(market_state=ms, config=cfg)
	result = await executor.place(req)

	if result.status == "filled":
		assert result.blended_entry_cents == 0, (
			"filled-with-zero is the sentinel for paper's empty-book fallback "
			"path — blended_entry_cents MUST round-trip as 0 so the trade "
			"store's close-time fallback to entry_price fires correctly"
		)
		# Spec §4.3 + §5.1: even on the filled-with-zero sentinel branch, the
		# dual-slippage metrics must be None ("not measurable") — the empty-book
		# fallback has no book best to measure against. NEVER set them to 0
		# (that would imply "filled exactly at best").
		assert result.market_impact_cents is None, (
			"filled-with-blended==0 sentinel must leave market_impact_cents=None "
			"per spec §4.3 (no book to measure against → 'not measurable', "
			"never 0)"
		)
		assert result.limit_slippage_cents is None, (
			"filled-with-blended==0 sentinel must leave limit_slippage_cents=None"
		)
	elif result.status == "rejected":
		assert result.rejection_reason in {"stale_book", "empty_book"}, (
			f"empty yes_levels rejection must surface a defined reason — "
			f"got {result.rejection_reason!r}"
		)
		# Rejected path also leaves both metrics None per §5.1 (already covered
		# by test_rejected_entry_leaves_dual_slippage_None — re-asserted here
		# for symmetry with the filled branch above).
		assert result.market_impact_cents is None
		assert result.limit_slippage_cents is None
	else:
		raise AssertionError(
			f"PaperExecutor MUST return filled or rejected for this input — "
			f"got {result.status!r}. pending is a LIVE-only status."
		)


@pytest.mark.asyncio
async def test_FillSkip_translates_to_rejected_with_reason():
	cfg = _canned_config()
	book = _canned_book(yes_levels=[])
	ms = _StubMarketState(book)
	req = _canned_request(side="yes", limit=42)

	executor = PaperExecutor(market_state=ms, config=cfg)
	result = await executor.place(req)

	assert result.status == "rejected"
	assert result.rejection_reason in {"stale_book", "empty_book"}
	assert result.filled_size == 0
	assert result.fill_pct == 0.0
	assert result.book_snapshot is None


# ---------------------------------------------------------------------------
# Dual-slippage metric population (spec §5.1 + §9)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_filled_entry_populates_dual_slippage_metrics():
	"""On a filled entry, PaperExecutor populates both diagnostic fields:
	  - market_impact_cents == fill.slippage_cents (alias; vs top-of-book best)
	  - limit_slippage_cents == signed slippage vs req.limit_price_cents

	Per spec §5.1: walk_book_with_ceiling is untouched; the alias has zero
	computational cost, and limit_slippage is the one new line in place().
	"""
	from edge_catcher.engine.fill_math import signed_slippage_cents

	book = _canned_book()
	cfg = _canned_config()
	ms = _StubMarketState(book)
	req = _canned_request()

	fill = resolve_fill(cfg, req.limit_price_cents, req.side, book)
	assert not isinstance(fill, FillSkip)

	executor = PaperExecutor(market_state=ms, config=cfg)
	result = await executor.place(req)

	assert result.status == "filled"
	# market_impact = vs-best (alias of existing slippage_cents on paper)
	assert result.market_impact_cents == fill.slippage_cents
	# limit_slippage = vs-limit, computed fresh against req.limit_price_cents
	expected_limit_slippage = signed_slippage_cents(
		blended=fill.blended_price_cents,
		limit=req.limit_price_cents,
		action="buy",
	)
	assert result.limit_slippage_cents == expected_limit_slippage


@pytest.mark.asyncio
async def test_rejected_entry_leaves_dual_slippage_None():
	"""When place() rejects (FillSkip → rejected), both new fields remain None.
	Covers stale-book / empty-book / below-min via the FillSkip codepath."""
	cfg = _canned_config()
	book = _canned_book(yes_levels=[])
	ms = _StubMarketState(book)
	req = _canned_request(side="yes", limit=42)

	executor = PaperExecutor(market_state=ms, config=cfg)
	result = await executor.place(req)

	assert result.status == "rejected"
	assert result.market_impact_cents is None
	assert result.limit_slippage_cents is None


@pytest.mark.asyncio
async def test_sell_path_leaves_dual_slippage_None():
	"""PaperExecutor short-circuits sell (paper.py:372 ish) before fill resolution.
	Per spec §5.1 non-filled paths: both metrics None."""
	book = _canned_book()
	cfg = _canned_config()
	ms = _StubMarketState(book)
	req = OrderRequest(
		ticker="KXSOL15M-25-T1",
		series="KXSOL15M",
		side="yes",
		size_contracts=4,
		limit_price_cents=42,
		strategy="strat-34",
		client_order_id="strat-34-KXSOL15M-1715000000000",
		action="sell",
	)

	executor = PaperExecutor(market_state=ms, config=cfg)
	result = await executor.place(req)

	# Paper sells are short-circuited; both metrics remain None per spec §5.1.
	assert result.market_impact_cents is None
	assert result.limit_slippage_cents is None
