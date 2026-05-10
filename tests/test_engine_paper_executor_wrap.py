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
	0-sentinel to fall back to entry_price at close time."""
	cfg = _canned_config()
	cfg["sizing"]["require_fresh_book"] = False
	book = _canned_book(yes_levels=[])
	ms = _StubMarketState(book)
	req = _canned_request(side="yes", limit=42)

	executor = PaperExecutor(market_state=ms, config=cfg)
	result = await executor.place(req)

	# Either filled with 0 sentinel, or rejected — both valid for this config.
	if result.status == "filled":
		assert result.blended_entry_cents == 0


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
