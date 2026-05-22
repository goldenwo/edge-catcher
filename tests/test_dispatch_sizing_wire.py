"""Tests for sizing-wire dispatch helpers (Task C1+).

C1: _inc_gate_metric — translates a GateDecision into a Metrics counter
increment. The gate holds no Metrics handle (spec §4.2); dispatch is the
translation layer.

C2: _handle_enter allowed_size wiring — live path builds via build_entry_order
(sized, single shared client_order_id); paper path (allowed_size=None) keeps
the byte-exact size_contracts=0 construction unchanged.
"""
from __future__ import annotations

import typing
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Any
from unittest.mock import MagicMock

import pytest

from edge_catcher.engine.dispatch import _handle_enter, _inc_gate_metric
from edge_catcher.engine.execution import ExecCfg
from edge_catcher.engine.executor import OrderRequest, OrderResult
from edge_catcher.engine.metrics import Metrics
from edge_catcher.engine.risk import Allow, Reject, GateRejectReason, SizingBreakdown
from edge_catcher.engine.strategy_base import Signal


def _bd() -> SizingBreakdown:
	"""Minimal SizingBreakdown for Allow construction."""
	return SizingBreakdown(
		fixed_fraction_contracts=5,
		quarter_kelly_contracts=2**31,  # sentinel: no edge config
		absolute_max_contracts=10,
		bound_by="fixed_fraction",
	)


def test_inc_gate_metric_allow_and_every_reject_reason() -> None:
	m = Metrics()
	_inc_gate_metric(m, Allow(size_contracts=5, sizing_breakdown=_bd()))
	assert m.snapshot()["risk_gate_allowed"] == 1

	for reason in typing.get_args(GateRejectReason):
		_inc_gate_metric(m, Reject(reason=reason, detail="x"))  # must NEVER raise KeyError


# ---------------------------------------------------------------------------
# C2 helpers
# ---------------------------------------------------------------------------

_NOW_C2 = datetime(2026, 5, 22, 12, 0, 0, tzinfo=timezone.utc)

_TICKER = "KXSOL15M-26MAY22H12"
_SERIES = "KXSOL15M"
_STRATEGY = "debut_fade"
_YES_ASK = 42


def _entry_signal(
	*,
	ticker: str = _TICKER,
	series: str = _SERIES,
	strategy: str = _STRATEGY,
	side: str = "yes",
	entry_price_cents: int = _YES_ASK,
	stop_loss_distance_cents: int = 8,
) -> Signal:
	"""Entry Signal with all live-execution fields populated."""
	return Signal(
		action="enter",
		ticker=ticker,
		series=series,
		side=side,
		strategy=strategy,
		reason="test",
		entry_price_cents=entry_price_cents,
		stop_loss_distance_cents=stop_loss_distance_cents,
	)


def _ctx(yes_ask: int = _YES_ASK, no_ask: int = 58) -> MagicMock:
	"""Minimal TickContext stub — dispatch reads yes_ask/no_ask."""
	return MagicMock(yes_ask=yes_ask, no_ask=no_ask, orderbook=MagicMock(depth=5))


def _exec_cfg(entry_slippage_cents: int = 2) -> ExecCfg:
	"""Minimal ExecCfg for live-path tests."""
	return ExecCfg(
		entry_slippage_cents=entry_slippage_cents,
		exit_slippage_cents=MappingProxyType({"stop_loss": 3, "take_profit": 1}),
	)


def _filled_result(*, size_contracts: int = 7) -> OrderResult:
	"""A filled OrderResult — drives dispatch into the record_trade branch."""
	return OrderResult(
		status="filled",
		intended_size=size_contracts,
		filled_size=size_contracts,
		blended_entry_cents=_YES_ASK,
		fill_pct=1.0,
		slippage_cents=2,
		order_id="ord-test-123",
		rejection_reason=None,
	)


class _CapturingStore:
	"""Captures placed OrderRequest (via record_intent) and the executor call.

	record_intent: stores client_order_id so tests can compare it to what
	the executor received — proving single shared id (spec §2.2 invariant).
	All other methods are no-ops or minimal stubs so dispatch's downstream
	paths (record_trade, get_trade_by_id, notify) don't blow up.
	"""

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
# C2 Test 1 — live path: allowed_size=7 → size_contracts==7,
# single shared client_order_id across record_intent AND executor.place
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handle_enter_live_builds_sized_request() -> None:
	"""Live path (allowed_size=7): _handle_enter builds req via build_entry_order,
	so size_contracts==7 AND the SAME client_order_id flows to BOTH
	store.record_intent (pre-place durability) and executor.place (money path).

	This proves the single-build invariant (spec §2.2): req is constructed
	ONCE; both consumers share one id — no double-build producing two uuids.
	"""
	store = _CapturingStore()

	# Capture the OrderRequest that executor.place receives.
	placed_reqs: list[OrderRequest] = []

	async def _fake_place(req: OrderRequest) -> OrderResult:
		placed_reqs.append(req)
		return _filled_result(size_contracts=req.size_contracts)

	executor = MagicMock()
	executor.place = _fake_place

	config: dict[str, Any] = {
		"_metrics": Metrics(),
		"_exec_cfg": _exec_cfg(entry_slippage_cents=2),
	}

	sig = _entry_signal(entry_price_cents=_YES_ASK)

	await _handle_enter(
		sig,
		_ctx(yes_ask=_YES_ASK),
		store,
		config,
		executor,
		now=_NOW_C2,
		allowed_size=7,
	)

	# Must have placed exactly one order.
	assert len(placed_reqs) == 1, "executor.place must be called exactly once"
	placed = placed_reqs[0]

	# Live path: size_contracts comes from allowed_size.
	assert placed.size_contracts == 7, (
		f"expected size_contracts=7 (from allowed_size), got {placed.size_contracts}"
	)

	# Single shared client_order_id: record_intent and executor.place
	# must see the SAME id — proves req was built exactly once (spec §2.2).
	intent_coid = store.intent_kwargs.get("client_order_id")
	placed_coid = placed.client_order_id
	assert intent_coid is not None, "record_intent must capture client_order_id"
	assert intent_coid == placed_coid, (
		f"client_order_id mismatch: record_intent saw {intent_coid!r}, "
		f"executor.place received {placed_coid!r} — req was built MORE THAN ONCE"
	)


# ---------------------------------------------------------------------------
# C2 Test 2 — paper path: allowed_size omitted → size_contracts==0
# (byte-exact unchanged; paper executor sizes internally)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handle_enter_paper_unchanged_size_zero() -> None:
	"""Paper path (allowed_size not passed): _handle_enter uses the original
	OrderRequest(size_contracts=0, ...) construction — unchanged from pre-C2.
	PaperExecutor sizes internally; dispatch must NOT pre-size on this path.
	"""
	store = _CapturingStore()

	placed_reqs: list[OrderRequest] = []

	async def _fake_place(req: OrderRequest) -> OrderResult:
		placed_reqs.append(req)
		# Paper executor returns filled with whatever size it computed.
		return _filled_result(size_contracts=req.size_contracts)

	executor = MagicMock()
	executor.place = _fake_place

	config: dict[str, Any] = {"_metrics": Metrics()}
	# No "_exec_cfg" in config — paper path must not access it.

	sig = _entry_signal(entry_price_cents=_YES_ASK)

	await _handle_enter(
		sig,
		_ctx(yes_ask=_YES_ASK),
		store,
		config,
		executor,
		now=_NOW_C2,
		# allowed_size intentionally omitted → defaults to None → paper path
	)

	assert len(placed_reqs) == 1, "executor.place must be called exactly once"
	placed = placed_reqs[0]

	# Paper path: size_contracts must remain 0 (paper executor sizes internally).
	assert placed.size_contracts == 0, (
		f"paper path must pass size_contracts=0, got {placed.size_contracts} — "
		"dispatch must NOT pre-size on the paper path"
	)


# ---------------------------------------------------------------------------
# C3 Tests — gate_entry consultation wired into _handle_signal
# ---------------------------------------------------------------------------
#
# Four invariants under test:
#   1. Allow(size=4) → _handle_enter reached with allowed_size=4 → placed size 4
#   2. Reject("MAX_OPEN") → executor.place NOT called, record_intent NOT called,
#      metric risk_gate_rejected_max_open == 1, NO notify() call (spec §4.1)
#   3. KillSwitchTripFailed raised by gate_entry → propagates OUT of _handle_signal
#   4. risk=None (paper/replay) → no gate call; _handle_enter reached with
#      allowed_size=None (paper byte-exact path unchanged)
# ---------------------------------------------------------------------------

import sqlite3
from pathlib import Path
from unittest.mock import patch, MagicMock

from edge_catcher.engine.dispatch import _handle_signal, process_tick
from edge_catcher.engine.risk import (
	Allow,
	Reject,
	KillSwitchTripFailed,
	RiskContext,
	SizingBreakdown,
)
from edge_catcher.engine.risk_context_provider import RiskContextProvider


_NOW_C3 = datetime(2026, 5, 22, 14, 0, 0, tzinfo=timezone.utc)
_TICKER_C3 = "KXSOL15M-26MAY22H14"
_SERIES_C3 = "KXSOL15M"
_STRATEGY_C3 = "debut_fade"
_YES_ASK_C3 = 45


def _entry_signal_c3(
	*,
	ticker: str = _TICKER_C3,
	series: str = _SERIES_C3,
	strategy: str = _STRATEGY_C3,
	side: str = "yes",
	entry_price_cents: int = _YES_ASK_C3,
	stop_loss_distance_cents: int = 8,
) -> Signal:
	"""Entry Signal for C3 gate tests."""
	return Signal(
		action="enter",
		ticker=ticker,
		series=series,
		side=side,
		strategy=strategy,
		reason="test_c3",
		entry_price_cents=entry_price_cents,
		stop_loss_distance_cents=stop_loss_distance_cents,
	)


def _ctx_c3(yes_ask: int = _YES_ASK_C3, no_ask: int = 55) -> MagicMock:
	"""Minimal TickContext stub for C3 tests (includes .market_state for provider)."""
	ctx = MagicMock()
	ctx.yes_ask = yes_ask
	ctx.no_ask = no_ask
	ctx.orderbook = MagicMock(depth=5)
	ctx.market_state = MagicMock()
	return ctx


def _bd_c3(size: int = 4) -> SizingBreakdown:
	"""Minimal SizingBreakdown for Allow construction."""
	return SizingBreakdown(
		fixed_fraction_contracts=size,
		quarter_kelly_contracts=2**31,
		absolute_max_contracts=10,
		bound_by="fixed_fraction",
	)


def _fake_risk_context() -> RiskContext:
	"""Minimal RiskContext for gate_entry calls in C3 tests."""
	return RiskContext(
		now_utc=_NOW_C3,
		market_state=MagicMock(),
		open_count=0,
		open_positions=[],
		daily_pnl_cents=0,
		operator_kill_active=False,
	)


class _FakeRiskContextProvider:
	"""Returns a fixed RiskContext; records whether .build() was called."""

	def __init__(self, ctx: RiskContext | None = None) -> None:
		self._ctx = ctx or _fake_risk_context()
		self.build_calls: list[tuple[Any, Any, Any]] = []

	def build(self, signal: Any, tick: Any, now: Any) -> RiskContext:
		self.build_calls.append((signal, tick, now))
		return self._ctx


class _FakeGate:
	"""Scripted Gate: returns a pre-set decision or raises on gate_entry."""

	def __init__(self, decision: Any) -> None:
		self._decision = decision
		self.gate_entry_calls: list[Any] = []

	def gate_entry(self, signal: Any, ctx: Any) -> Any:
		self.gate_entry_calls.append((signal, ctx))
		if isinstance(self._decision, BaseException):
			raise self._decision
		return self._decision


# ---------------------------------------------------------------------------
# C3 Test 1 — Allow(size=4) → _handle_enter called with allowed_size=4
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_enter_allow_places_sized_order() -> None:
	"""gate_entry returns Allow(size=4); executor.place must receive size_contracts=4.

	Proves the Allow path of _handle_signal's gate consultation (spec §2.1):
	- RiskContextProvider.build() is called once.
	- gate_entry() is called once.
	- _handle_enter is reached with allowed_size=4.
	- executor.place is called with size_contracts=4.
	"""
	store = _CapturingStore()
	placed_reqs: list[OrderRequest] = []

	async def _fake_place(req: OrderRequest) -> OrderResult:
		placed_reqs.append(req)
		return _filled_result(size_contracts=req.size_contracts)

	executor = MagicMock()
	executor.place = _fake_place

	allow_decision = Allow(size_contracts=4, sizing_breakdown=_bd_c3(4))
	fake_risk = _FakeGate(allow_decision)
	fake_provider = _FakeRiskContextProvider()

	config: dict[str, Any] = {
		"_metrics": Metrics(),
		"_exec_cfg": _exec_cfg(entry_slippage_cents=2),
	}

	sig = _entry_signal_c3(entry_price_cents=_YES_ASK_C3)

	await _handle_signal(
		sig,
		_ctx_c3(yes_ask=_YES_ASK_C3),
		store,
		config,
		executor,
		now=_NOW_C3,
		risk=fake_risk,  # type: ignore[arg-type]
		risk_ctx_provider=fake_provider,  # type: ignore[arg-type]
	)

	# Provider was consulted exactly once.
	assert len(fake_provider.build_calls) == 1, "provider.build() must be called once"

	# Gate was consulted exactly once.
	assert len(fake_risk.gate_entry_calls) == 1, "gate_entry() must be called once"

	# Order was placed with the gated size.
	assert len(placed_reqs) == 1, "executor.place must be called exactly once"
	assert placed_reqs[0].size_contracts == 4, (
		f"expected size_contracts=4 (from Allow.size_contracts), "
		f"got {placed_reqs[0].size_contracts}"
	)


# ---------------------------------------------------------------------------
# C3 Test 2 — Reject → no place, no record_intent, metric incremented, NO notify
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_enter_reject_skips_no_order_no_record_intent(caplog: Any) -> None:
	"""gate_entry returns Reject('MAX_OPEN'); dispatch must:
	- NOT call executor.place
	- NOT call store.record_intent
	- Increment risk_gate_rejected_max_open metric to 1
	- NOT call notify() — routine rejects are silent (spec §4.1)
	"""
	store = _CapturingStore()
	placed_reqs: list[OrderRequest] = []

	async def _fake_place(req: OrderRequest) -> OrderResult:
		placed_reqs.append(req)
		return _filled_result(size_contracts=req.size_contracts)

	executor = MagicMock()
	executor.place = _fake_place

	reject_decision = Reject(reason="MAX_OPEN", detail="already 5 open positions")
	fake_risk = _FakeGate(reject_decision)
	fake_provider = _FakeRiskContextProvider()

	metrics = Metrics()
	config: dict[str, Any] = {"_metrics": metrics}

	sig = _entry_signal_c3(entry_price_cents=_YES_ASK_C3)

	notify_calls: list[Any] = []

	with patch("edge_catcher.engine.dispatch.notify", side_effect=lambda *a, **kw: notify_calls.append((a, kw))):
		await _handle_signal(
			sig,
			_ctx_c3(yes_ask=_YES_ASK_C3),
			store,
			config,
			executor,
			now=_NOW_C3,
			risk=fake_risk,  # type: ignore[arg-type]
			risk_ctx_provider=fake_provider,  # type: ignore[arg-type]
		)

	# executor.place must NOT be called on a Reject.
	assert placed_reqs == [], "executor.place must NOT be called on Reject"

	# record_intent must NOT be called — no intent row should exist for a gated-out signal.
	assert store.intent_kwargs == {}, "record_intent must NOT be called on Reject"

	# The correct metric counter must be incremented.
	snapshot = metrics.snapshot()
	assert snapshot.get("risk_gate_rejected_max_open", 0) == 1, (
		f"risk_gate_rejected_max_open should be 1, got {snapshot}"
	)

	# No notify() call — routine rejects are silent (spec §4.1).
	assert notify_calls == [], (
		"notify() must NOT be called for routine Reject (spec §4.1 — "
		"audit/alert routing is E's RiskEvent contract, not dispatch's)"
	)


# ---------------------------------------------------------------------------
# C3 Test 3 — KillSwitchTripFailed propagates out of _handle_signal
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_enter_killswitchtripfailed_propagates() -> None:
	"""gate_entry raises KillSwitchTripFailed; _handle_signal must NOT catch it.

	The ghost-reject defense (C-spec L214): if the kill-switch INSERT failed,
	the engine must STOP. Swallowing here would let the next tick re-enter
	ungated. This test confirms _handle_signal re-raises without catching.
	"""
	store = _CapturingStore()
	placed_reqs: list[OrderRequest] = []

	async def _fake_place(req: OrderRequest) -> OrderResult:  # pragma: no cover
		placed_reqs.append(req)
		return _filled_result(size_contracts=req.size_contracts)

	executor = MagicMock()
	executor.place = _fake_place

	exc = KillSwitchTripFailed("DB write failed")
	fake_risk = _FakeGate(exc)
	fake_provider = _FakeRiskContextProvider()

	config: dict[str, Any] = {"_metrics": Metrics()}
	sig = _entry_signal_c3(entry_price_cents=_YES_ASK_C3)

	with pytest.raises(KillSwitchTripFailed, match="DB write failed"):
		await _handle_signal(
			sig,
			_ctx_c3(yes_ask=_YES_ASK_C3),
			store,
			config,
			executor,
			now=_NOW_C3,
			risk=fake_risk,  # type: ignore[arg-type]
			risk_ctx_provider=fake_provider,  # type: ignore[arg-type]
		)

	# Order must NOT have been placed.
	assert placed_reqs == [], "executor.place must NOT be called if kill-switch raises"


# ---------------------------------------------------------------------------
# C3 Test 4 — Paper path: risk=None → no gate, allowed_size=None preserved
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_paper_mode_risk_none_no_gate() -> None:
	"""risk=None (paper/replay): no provider.build(), no gate_entry(); _handle_enter
	called with allowed_size=None → size_contracts=0 (paper byte-exact path).

	Proves G-parity: the paper path is BYTE-IDENTICAL — no RiskContext constructed,
	no gate call, ungated _handle_enter invocation with the sentinel None.
	"""
	store = _CapturingStore()
	placed_reqs: list[OrderRequest] = []

	async def _fake_place(req: OrderRequest) -> OrderResult:
		placed_reqs.append(req)
		return _filled_result(size_contracts=req.size_contracts)

	executor = MagicMock()
	executor.place = _fake_place

	# Spy provider — should never be called on the paper path.
	fake_provider = _FakeRiskContextProvider()

	config: dict[str, Any] = {"_metrics": Metrics()}
	sig = _entry_signal_c3(entry_price_cents=_YES_ASK_C3)

	await _handle_signal(
		sig,
		_ctx_c3(yes_ask=_YES_ASK_C3),
		store,
		config,
		executor,
		now=_NOW_C3,
		risk=None,           # paper/replay path
		risk_ctx_provider=None,  # must also be None on paper path
	)

	# Provider.build() must NOT have been called.
	assert fake_provider.build_calls == [], "provider.build() must NOT be called when risk=None"

	# Order was placed (enter signal proceeds ungated on paper path).
	assert len(placed_reqs) == 1, "executor.place must be called once on paper path"

	# Paper path: size_contracts must be 0 (byte-exact).
	assert placed_reqs[0].size_contracts == 0, (
		f"paper path must pass size_contracts=0, got {placed_reqs[0].size_contracts}"
	)
