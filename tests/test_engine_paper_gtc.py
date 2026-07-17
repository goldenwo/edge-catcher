"""PaperExecutor GTC (maker/resting) branch tests — SPEC §8.1.

The GTC branch must NOT walk the book or fill synchronously: it sizes with
the two existing paper primitives (compute_raw_size + min_fill gate) and
returns a "resting" ACK; fills arrive later via the RestingOrderTracker.
Maker safety invariants (would_cross, caps, validation) run at DISPATCH
before place() — the executor does not re-check them.
"""
from __future__ import annotations

import pytest

from edge_catcher.engine.executor import OrderRequest
from edge_catcher.engine.executors.honest_paper import (
	FixedSlippageModel, HonestPaperExecutor,
)
from edge_catcher.engine.executors.paper import PaperExecutor, compute_raw_size
from edge_catcher.engine.market_state import OrderbookSnapshot


def _config(risk=200, min_fill=1) -> dict:
	return {
		"sizing": {
			"risk_per_trade_cents": risk,
			"max_slippage_cents": 5,
			"min_fill": min_fill,
			"require_fresh_book": True,
		},
	}


def _gtc_request(limit=15, side="no") -> OrderRequest:
	return OrderRequest(
		ticker="KXTEST-1", series="KXTEST", side=side, size_contracts=1,
		limit_price_cents=limit, strategy="s",
		client_order_id="s-KXTEST-1-1715000000000-abcd1234",
		action="buy", time_in_force="gtc",
	)


class _StubMarketState:
	def __init__(self, book: OrderbookSnapshot) -> None:
		self._book = book

	def get_orderbook(self, ticker: str) -> OrderbookSnapshot:
		return self._book


def _executor(cfg: dict) -> PaperExecutor:
	book = OrderbookSnapshot(yes_levels=[[80, 10]], no_levels=[[14, 5]])
	return PaperExecutor(market_state=_StubMarketState(book), config=cfg)


@pytest.mark.asyncio
async def test_gtc_returns_resting_sized_by_compute_raw_size():
	cfg = _config(risk=200)
	result = await _executor(cfg).place(_gtc_request(limit=15))
	assert result.status == "resting"
	assert result.intended_size == compute_raw_size(200, 15) == 13
	assert result.filled_size == 0
	assert result.blended_entry_cents == 0
	assert result.fill_pct == 0.0
	assert result.order_id == "paper-s-KXTEST-1-1715000000000-abcd1234"


@pytest.mark.asyncio
async def test_gtc_below_min_fill_rejected():
	cfg = _config(risk=200, min_fill=3)          # 200 // 90 = 2 < 3
	result = await _executor(cfg).place(_gtc_request(limit=90))
	assert result.status == "rejected"
	assert result.rejection_reason == "below_min_fill"
	assert result.filled_size == 0


@pytest.mark.asyncio
async def test_gtc_does_not_walk_the_book():
	# An EMPTY book must not matter: no walk, no fresh-book gate — the
	# resting ACK is sizing-only (fills come later from the tracker).
	cfg = _config(risk=200)
	book = OrderbookSnapshot(yes_levels=[], no_levels=[])
	ex = PaperExecutor(market_state=_StubMarketState(book), config=cfg)
	result = await ex.place(_gtc_request(limit=15))
	assert result.status == "resting"


@pytest.mark.asyncio
async def test_ioc_taker_path_unchanged():
	cfg = _config(risk=200)
	req = OrderRequest(
		ticker="KXTEST-1", series="KXTEST", side="no", size_contracts=4,
		limit_price_cents=14, strategy="s",
		client_order_id="s-KXTEST-1-1715000000001-abcd1234",
		action="buy", time_in_force="ioc",
	)
	result = await _executor(cfg).place(req)
	assert result.status in ("filled", "rejected")   # normal taker outcome
	assert result.status != "resting"


@pytest.mark.asyncio
async def test_honest_paper_passes_resting_through_untouched():
	cfg = _config(risk=200)
	base = _executor(cfg)
	wrapped = HonestPaperExecutor(
		base=base, model=FixedSlippageModel(default_cents=3, per_strategy={}),
	)
	result = await wrapped.place(_gtc_request(limit=15))
	assert result.status == "resting"
	assert result.blended_entry_cents == 0           # no slippage applied to a non-fill
