"""Honest paper fill simulator — Phase 1 (FixedSlippageModel + HonestPaperExecutor).

WHY this exists: P1's strat-a verdict found Gap-1 — the optimistic
PaperExecutor's paper win-rate materially overstated live performance on the
same window, i.e. it over-promises. This wraps it with a pessimistic slippage
penalty. See
docs/superpowers/specs/2026-05-28-honest-paper-fill-simulator-design.md.
"""
from __future__ import annotations

import pytest

from edge_catcher.engine.executor import OrderRequest, OrderResult
# Task 1 imports only the model; Task 2 adds HonestPaperExecutor to this line.
from edge_catcher.engine.executors.honest_paper import (
	FixedSlippageModel,
	HonestPaperExecutor,
	SlippageModel,
)
from edge_catcher.engine.market_state import OrderbookSnapshot


def _result(
	*,
	status: str = "filled",
	blended: int = 42,
	market_impact: int | None = 2,
	limit_slippage: int | None = -3,
) -> OrderResult:
	return OrderResult(
		status=status,
		intended_size=4,
		filled_size=4,
		blended_entry_cents=blended,
		fill_pct=1.0,
		slippage_cents=0,
		book_depth=150,
		book_snapshot=None,
		rejection_reason=None,
		order_id=None,
		market_impact_cents=market_impact,
		limit_slippage_cents=limit_slippage,
	)


def _req(*, side: str = "yes", action: str = "buy", strategy: str = "debut_fade") -> OrderRequest:
	return OrderRequest(
		ticker="KXSOL15M-25-T1",
		series="KXSOL15M",
		side=side,
		size_contracts=4,
		limit_price_cents=42,
		strategy=strategy,
		client_order_id="debut_fade-KXSOL15M-1715000000000",
		action=action,
	)


_OB = OrderbookSnapshot(yes_levels=[(0.42, 100)], no_levels=[(0.58, 100)])


def _model(default: int = 2, per_strategy: dict | None = None) -> FixedSlippageModel:
	return FixedSlippageModel(default_cents=default, per_strategy=per_strategy or {})


def test_buy_fill_price_increases_and_metrics_worsen():
	m = _model(per_strategy={"debut_fade": 5})
	out = m.adjust(_result(blended=42, market_impact=2, limit_slippage=-3), _req(action="buy"), _OB)
	assert out.blended_entry_cents == 47          # 42 + 5 (buy pays more)
	assert out.market_impact_cents == 7           # 2 + 5 (worse)
	assert out.limit_slippage_cents == 2          # -3 + 5 (worse)


def test_sell_fill_price_decreases_but_metrics_still_worsen():
	# Regression guard for the round-2 sign bug: a buggy `metric += signed_delta`
	# would move price -5 AND wrongly subtract from the metric (reporting
	# improvement). Slippage is "positive = worse" on BOTH sides, so the metric
	# must INCREASE by the worsening magnitude even though price went DOWN.
	m = _model(per_strategy={"debut_fade": 5})
	out = m.adjust(_result(blended=42, market_impact=2, limit_slippage=-3), _req(action="sell"), _OB)
	assert out.blended_entry_cents == 37          # 42 - 5 (sell receives less)
	assert out.market_impact_cents == 7           # 2 + 5 (worse — NOT 2-5=-3)
	assert out.limit_slippage_cents == 2          # -3 + 5 (worse)


def test_non_filled_results_pass_through_unchanged():
	m = _model()
	for status in ("rejected", "pending"):
		r = _result(status=status)
		assert m.adjust(r, _req(), _OB) is r       # identity — untouched


def test_empty_book_sentinel_passes_through_with_none_metrics():
	m = _model(default=5)
	r = _result(blended=0, market_impact=None, limit_slippage=None)
	out = m.adjust(r, _req(), _OB)
	assert out.blended_entry_cents == 0            # sentinel preserved
	assert out.market_impact_cents is None
	assert out.limit_slippage_cents is None


def test_high_edge_buy_clamps_to_99_and_metric_uses_effective_delta():
	m = _model(default=10)
	out = m.adjust(_result(blended=95, market_impact=0, limit_slippage=0), _req(action="buy"), _OB)
	assert out.blended_entry_cents == 99           # 95 + 10 clamped to 99
	assert out.market_impact_cents == 4            # effective delta 99-95=4, NOT nominal 10
	assert out.limit_slippage_cents == 4


def test_low_edge_sell_clamps_to_1_and_metric_uses_effective_delta():
	m = _model(default=10)
	out = m.adjust(_result(blended=5, market_impact=0, limit_slippage=0), _req(action="sell"), _OB)
	assert out.blended_entry_cents == 1            # 5 - 10 clamped to 1
	assert out.market_impact_cents == 4            # effective delta abs(1-5)=4
	assert out.limit_slippage_cents == 4


def test_none_metrics_preserved_on_non_sentinel_price():
	m = _model(default=3)
	out = m.adjust(_result(blended=42, market_impact=None, limit_slippage=None), _req(), _OB)
	assert out.blended_entry_cents == 45           # price still adjusts
	assert out.market_impact_cents is None         # None stays None
	assert out.limit_slippage_cents is None


def test_per_strategy_override_and_default_fallback():
	m = _model(default=2, per_strategy={"debut_fade": 5})
	out_known = m.adjust(_result(blended=42), _req(strategy="debut_fade", action="buy"), _OB)
	out_default = m.adjust(_result(blended=42), _req(strategy="other", action="buy"), _OB)
	assert out_known.blended_entry_cents == 47     # uses 5
	assert out_default.blended_entry_cents == 44   # falls back to 2


def test_determinism_same_input_same_output():
	m = _model(default=3, per_strategy={"debut_fade": 5})
	r, q = _result(blended=42), _req(action="buy")
	assert m.adjust(r, q, _OB) == m.adjust(r, q, _OB)


def test_fixed_model_satisfies_protocol():
	m: SlippageModel = _model()   # mypy + runtime: FixedSlippageModel IS a SlippageModel
	assert callable(m.adjust)


class _StubMarketState:
	def __init__(self, book: OrderbookSnapshot) -> None:
		self._book = book

	def get_orderbook(self, ticker: str) -> OrderbookSnapshot:
		return self._book


def _paper_config() -> dict:
	return {
		"sizing": {
			"risk_per_trade_cents": 200,
			"max_slippage_cents": 5,
			"min_fill": 1,
			"require_fresh_book": True,
		},
	}


@pytest.mark.asyncio
async def test_wrapper_delegates_to_base_then_applies_model():
	from edge_catcher.engine.executors.paper import PaperExecutor

	book = OrderbookSnapshot(yes_levels=[(0.42, 100), (0.43, 50)], no_levels=[(0.58, 100)])
	base = PaperExecutor(market_state=_StubMarketState(book), config=_paper_config())
	model = FixedSlippageModel(default_cents=5, per_strategy={})
	wrapped = HonestPaperExecutor(base=base, model=model)

	req = _req(side="yes", action="buy", strategy="x")
	base_result = await base.place(req)
	assert base_result.status == "filled", "canned book should fill"

	wrapped_result = await wrapped.place(req)
	# Wrapper price = base blended + 5 (clamped). Metrics worsen by the same.
	assert wrapped_result.blended_entry_cents == min(99, base_result.blended_entry_cents + 5)


@pytest.mark.asyncio
async def test_wrapper_passes_rejected_through_unchanged():
	from edge_catcher.engine.executors.paper import PaperExecutor

	# Empty book → PaperExecutor rejects → wrapper returns it untouched.
	book = OrderbookSnapshot(yes_levels=[], no_levels=[])
	cfg = _paper_config()
	base = PaperExecutor(market_state=_StubMarketState(book), config=cfg)
	wrapped = HonestPaperExecutor(base=base, model=FixedSlippageModel(default_cents=5, per_strategy={}))
	result = await wrapped.place(_req())
	# Empty book + require_fresh_book → PaperExecutor deterministically rejects.
	assert result.status == "rejected"
	assert result.market_impact_cents is None and result.limit_slippage_cents is None


class _PaperExecutorStub:
	"""Minimal Executor for the Protocol-conformance check (defined before use)."""
	async def place(self, req: OrderRequest) -> OrderResult:
		return _result()


def test_wrapper_satisfies_executor_protocol():
	# Pure static/structural check — no await, so NOT an asyncio test.
	from edge_catcher.engine.executor import Executor

	def _takes(_e: Executor) -> None: ...
	base = _PaperExecutorStub()
	_takes(HonestPaperExecutor(base=base, model=FixedSlippageModel(default_cents=1, per_strategy={})))


@pytest.mark.asyncio
async def test_end_to_end_known_book_produces_expected_adjusted_fill():
	from edge_catcher.engine.executors.paper import PaperExecutor

	# Single-level book at 42c yes; FixedSlippageModel default 3.
	book = OrderbookSnapshot(yes_levels=[(0.42, 500)], no_levels=[(0.58, 500)])
	cfg = _paper_config()
	base = PaperExecutor(market_state=_StubMarketState(book), config=cfg)
	wrapped = HonestPaperExecutor(
		base=base, model=FixedSlippageModel(default_cents=3, per_strategy={}),
	)
	req = _req(side="yes", action="buy", strategy="anything")
	base_result = await base.place(req)
	wrapped_result = await wrapped.place(req)
	assert wrapped_result.blended_entry_cents == min(99, base_result.blended_entry_cents + 3)
	# Metrics: if base produced non-None, they worsened by the effective delta.
	if base_result.market_impact_cents is not None:
		assert wrapped_result.market_impact_cents == base_result.market_impact_cents + (
			wrapped_result.blended_entry_cents - base_result.blended_entry_cents
		)


@pytest.mark.asyncio
async def test_injected_market_state_book_reaches_model():
	# When market_state is injected, _orderbook_for reads the REAL book and hands
	# it to model.adjust — the Phase-2 contract Task 3's composition wiring relies
	# on. (Phase-1 FixedSlippageModel ignores the book, so a recording spy model
	# is the only way to observe this path.)
	from edge_catcher.engine.executors.paper import PaperExecutor

	book = OrderbookSnapshot(yes_levels=[(0.42, 100)], no_levels=[(0.58, 100)])
	ms = _StubMarketState(book)
	base = PaperExecutor(market_state=ms, config=_paper_config())

	class _RecordingModel:
		def __init__(self) -> None:
			self.seen_orderbook: OrderbookSnapshot | None = None

		def adjust(self, result: OrderResult, request: OrderRequest, orderbook: OrderbookSnapshot) -> OrderResult:
			self.seen_orderbook = orderbook
			return result

	model = _RecordingModel()
	wrapped = HonestPaperExecutor(base=base, model=model, market_state=ms)
	await wrapped.place(_req(action="buy"))
	assert model.seen_orderbook is book   # the injected market_state's book reached the model
