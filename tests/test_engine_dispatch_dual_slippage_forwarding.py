"""Dispatch dual-slippage forwarding tests (spec §4.2 / §9).

Pins the two dispatch wire-up obligations:

1. Pre-place ``store.record_intent`` receives the top-of-book implied ASK
   (in cents) for the side being bought — ``100 − best opposite-side bid``
   (yes_levels/no_levels are resting BIDS) — plus the OrderRequest's actual
   ``limit_price_cents``. Empty OPPOSITE side → ``entry_best_price_cents=None``
   (spec §4.3 "not measurable", never 0).
2. The filled branch forwards ``_result.market_impact_cents`` and
   ``_result.limit_slippage_cents`` from the executor through to
   ``store.record_trade``. Paper persists; live ignores (live computes at
   transition_pending_to_open per Step 10).

These are the only two new D-side forwarding obligations dual-slippage
adds. The §9 G-parity + 11/11 paper byte-unchanged contract is covered by
the existing test_executor_replay_parity suite.
"""
from __future__ import annotations

from datetime import datetime, timezone
from types import MappingProxyType
from typing import Any
from unittest.mock import MagicMock

import pytest

from edge_catcher.engine.dispatch import _handle_enter
from edge_catcher.engine.execution import ExecCfg
from edge_catcher.engine.executor import OrderRequest, OrderResult
from edge_catcher.engine.market_state import OrderbookSnapshot
from edge_catcher.engine.metrics import Metrics
from edge_catcher.engine.strategy_base import Signal


_NOW = datetime(2026, 5, 28, 12, 0, 0, tzinfo=timezone.utc)


def _entry_signal(*, side: str = "yes") -> Signal:
	return Signal(
		action="enter",
		ticker="KXSOL15M-26MAY28H12",
		series="KXSOL15M",
		side=side,
		strategy="strat_34",
		reason="test",
		entry_price_cents=42,
		stop_loss_distance_cents=8,
		protective_stop_cents=8,
	)


def _ctx_with_orderbook(
	*,
	yes_levels: list[tuple[float, int]] | None = None,
	no_levels: list[tuple[float, int]] | None = None,
	yes_ask: int = 42,
	no_ask: int = 58,
) -> MagicMock:
	"""TickContext stub carrying a REAL OrderbookSnapshot. The snapshot is
	what dispatch reads for the dual-slippage book-best reference (spec
	§4.2). yes_bid is derived narrow so the live spread-gate stays inert."""
	if yes_levels is None:
		yes_levels = [(0.41, 100)]  # dollars; cents = 41
	if no_levels is None:
		no_levels = [(0.58, 100)]  # dollars; cents = 58
	orderbook = OrderbookSnapshot(yes_levels=yes_levels, no_levels=no_levels)
	return MagicMock(
		yes_ask=yes_ask,
		yes_bid=yes_ask - 2,
		no_ask=no_ask,
		orderbook=orderbook,
	)


def _exec_cfg() -> ExecCfg:
	return ExecCfg(
		entry_slippage_cents=2,
		exit_slippage_cents=MappingProxyType({"stop_loss": 3, "take_profit": 1}),
	)


def _filled_result(*, market_impact_cents: int | None = 1, limit_slippage_cents: int | None = -3) -> OrderResult:
	"""A filled OrderResult carrying the two new dual-slippage diagnostic
	fields. Paper populates these inside its place() (commit 35a717f); live
	leaves them None (live computes at transition_pending_to_open)."""
	return OrderResult(
		status="filled",
		intended_size=7,
		filled_size=7,
		blended_entry_cents=42,
		fill_pct=1.0,
		slippage_cents=1,
		order_id="ord-test-ds",
		rejection_reason=None,
		market_impact_cents=market_impact_cents,
		limit_slippage_cents=limit_slippage_cents,
	)


class _CapturingStore:
	"""Captures record_intent + record_trade kwargs verbatim."""

	def __init__(self) -> None:
		self.intent_kwargs: dict[str, Any] = {}
		self.trade_calls: list[dict[str, Any]] = []

	def record_intent(self, **kwargs: Any) -> None:
		self.intent_kwargs = dict(kwargs)

	def record_trade(self, **kwargs: Any) -> int:
		self.trade_calls.append(kwargs)
		return 1

	def get_trade_by_id(self, trade_id: int) -> dict[str, Any]:
		return {"id": trade_id, "status": "open"}

	def record_pending(self, **kwargs: Any) -> None:
		pass

	def record_rejected(self, **kwargs: Any) -> None:
		pass


# ---------------------------------------------------------------------------
# record_intent — book-best snapshot + req.limit_price_cents
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_record_intent_receives_yes_book_best_in_cents() -> None:
	"""yes-side entry → entry_best_price_cents = best implied YES ask in
	cents: 100 − best NO bid (no_levels are resting BIDS; default fixture
	best NO bid = 58 → implied ask 42). The reference lands on the pending
	row so transition_pending_to_open can compute market_impact_cents at
	fill."""
	store = _CapturingStore()

	async def _fake_place(req: OrderRequest) -> OrderResult:
		return _filled_result()

	executor = MagicMock()
	executor.place = _fake_place

	await _handle_enter(
		_entry_signal(side="yes"),
		_ctx_with_orderbook(yes_levels=[(0.41, 100)]),
		store,
		{"_metrics": Metrics(), "_exec_cfg": _exec_cfg()},
		executor,
		now=_NOW,
		allowed_size=7,
	)

	assert store.intent_kwargs.get("entry_best_price_cents") == 42, (
		"yes-side entry must capture the implied YES ask: 100 − best NO "
		"bid (58c) = 42c — NOT the same-side penny floor"
	)


@pytest.mark.asyncio
async def test_record_intent_receives_no_book_best_in_cents() -> None:
	"""no-side entry → entry_best_price_cents = best implied NO ask in
	cents: 100 − best YES bid (default fixture best YES bid = 41 → implied
	ask 59). A NO buy crosses the YES side's resting bids."""
	store = _CapturingStore()

	async def _fake_place(req: OrderRequest) -> OrderResult:
		return _filled_result()

	executor = MagicMock()
	executor.place = _fake_place

	await _handle_enter(
		_entry_signal(side="no"),
		_ctx_with_orderbook(no_levels=[(0.57, 100)]),
		store,
		{"_metrics": Metrics(), "_exec_cfg": _exec_cfg()},
		executor,
		now=_NOW,
		allowed_size=7,
	)

	assert store.intent_kwargs.get("entry_best_price_cents") == 59


@pytest.mark.asyncio
async def test_record_intent_empty_opposite_levels_yields_none() -> None:
	"""Empty OPPOSITE-side levels → entry_best_price_cents=None per spec
	§4.3 ("not measurable", NEVER 0). The implied ask for a yes-side buy
	comes from the NO side's resting bids (100 − best NO bid), so the
	trigger condition is the OPPOSITE side being empty — an empty same
	side is irrelevant. Covers any path where the orderbook is thin /
	missing at the pre-place call moment."""
	store = _CapturingStore()

	async def _fake_place(req: OrderRequest) -> OrderResult:
		return _filled_result()

	executor = MagicMock()
	executor.place = _fake_place

	await _handle_enter(
		_entry_signal(side="yes"),
		_ctx_with_orderbook(no_levels=[]),  # empty OPPOSITE (no) book
		store,
		{"_metrics": Metrics(), "_exec_cfg": _exec_cfg()},
		executor,
		now=_NOW,
		allowed_size=7,
	)

	assert "entry_best_price_cents" in store.intent_kwargs, (
		"record_intent must always be called with the kwarg present"
	)
	assert store.intent_kwargs["entry_best_price_cents"] is None


@pytest.mark.asyncio
async def test_record_intent_spec_worked_example_deep_no_book() -> None:
	"""Spec worked example: yes-side entry against
	no_levels=[(0.01, 800), (0.75, 40)] → best NO bid is 75c (best bid is
	the HIGHEST, i.e. the LAST ascending level — never levels[0], the
	penny floor) → implied YES ask = 100 − 75 = 25."""
	store = _CapturingStore()

	async def _fake_place(req: OrderRequest) -> OrderResult:
		return _filled_result()

	executor = MagicMock()
	executor.place = _fake_place

	await _handle_enter(
		_entry_signal(side="yes"),
		_ctx_with_orderbook(no_levels=[(0.01, 800), (0.75, 40)]),
		store,
		{"_metrics": Metrics(), "_exec_cfg": _exec_cfg()},
		executor,
		now=_NOW,
		allowed_size=7,
	)

	assert store.intent_kwargs.get("entry_best_price_cents") == 25, (
		"implied YES ask must come from the HIGHEST NO bid (75c → 25c), "
		"not the 1c penny floor (which would imply 99c)"
	)


@pytest.mark.asyncio
async def test_record_intent_receives_limit_from_order_request() -> None:
	"""entry_limit_price_cents on the pending row equals the OrderRequest's
	limit_price_cents — the executor's actual offered limit. On the live
	path, build_entry_order applies taker-with-cap slippage to derive this
	from the Signal's entry_price_cents + ExecCfg.entry_slippage_cents
	(yes-side: yes_ask + slippage, capped at 99)."""
	store = _CapturingStore()

	placed_reqs: list[OrderRequest] = []

	async def _fake_place(req: OrderRequest) -> OrderResult:
		placed_reqs.append(req)
		return _filled_result()

	executor = MagicMock()
	executor.place = _fake_place

	await _handle_enter(
		_entry_signal(side="yes"),
		_ctx_with_orderbook(),
		store,
		{"_metrics": Metrics(), "_exec_cfg": _exec_cfg()},
		executor,
		now=_NOW,
		allowed_size=7,
	)

	assert len(placed_reqs) == 1
	assert store.intent_kwargs.get("entry_limit_price_cents") == placed_reqs[0].limit_price_cents, (
		"entry_limit_price_cents must equal the OrderRequest's limit (the "
		"actual price we offered, post-taker-cap)"
	)


# ---------------------------------------------------------------------------
# record_trade — filled-branch forwards _result.market_impact / limit_slippage
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_filled_branch_forwards_dual_slippage_into_record_trade() -> None:
	"""Per spec §9: dispatch's filled branch forwards
	_result.market_impact_cents and _result.limit_slippage_cents into
	store.record_trade. Paper persists onto paper_trades (commit 023a9b5 +
	27a7695); live's record_trade is a CAS to transition_pending_to_open
	which ignores both (live computes at transition per Step 10)."""
	store = _CapturingStore()

	async def _fake_place(req: OrderRequest) -> OrderResult:
		return _filled_result(market_impact_cents=2, limit_slippage_cents=-4)

	executor = MagicMock()
	executor.place = _fake_place

	await _handle_enter(
		_entry_signal(side="yes"),
		_ctx_with_orderbook(),
		store,
		{"_metrics": Metrics(), "_exec_cfg": _exec_cfg()},
		executor,
		now=_NOW,
		allowed_size=7,
	)

	assert len(store.trade_calls) == 1
	kw = store.trade_calls[0]
	assert kw.get("market_impact_cents") == 2, (
		"filled branch must forward _result.market_impact_cents — paper "
		"persists it; live ignores (computes at transition)"
	)
	assert kw.get("limit_slippage_cents") == -4


@pytest.mark.asyncio
async def test_filled_branch_forwards_none_dual_slippage_when_executor_omits() -> None:
	"""LiveExecutor leaves both metric fields None (live computes at
	transition_pending_to_open). dispatch must forward None verbatim — the
	paper TradeStore stores NULL (spec §4.3 sentinel) and live's CAS
	ignores. Either way the forward must happen — the kwarg must be present
	in the record_trade call so paper/in-memory have a deterministic value."""
	store = _CapturingStore()

	async def _fake_place(req: OrderRequest) -> OrderResult:
		# LiveExecutor would leave both at None (spec §5.2).
		return _filled_result(market_impact_cents=None, limit_slippage_cents=None)

	executor = MagicMock()
	executor.place = _fake_place

	await _handle_enter(
		_entry_signal(side="yes"),
		_ctx_with_orderbook(),
		store,
		{"_metrics": Metrics(), "_exec_cfg": _exec_cfg()},
		executor,
		now=_NOW,
		allowed_size=7,
	)

	assert len(store.trade_calls) == 1
	kw = store.trade_calls[0]
	assert "market_impact_cents" in kw, "kwarg must always be present"
	assert kw["market_impact_cents"] is None
	assert "limit_slippage_cents" in kw
	assert kw["limit_slippage_cents"] is None
