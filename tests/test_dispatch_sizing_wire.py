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
from unittest.mock import MagicMock, patch

import pytest

from edge_catcher.engine.dispatch import _handle_enter, _handle_signal, _inc_gate_metric
from edge_catcher.engine.execution import ExecCfg
from edge_catcher.engine.executor import OrderRequest, OrderResult
from edge_catcher.engine.metrics import Metrics
from edge_catcher.engine.risk import (
	Allow,
	GateRejectReason,
	KillSwitchTripFailed,
	Reject,
	RiskContext,
	SizingBreakdown,
)
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
		self.build_calls: list[tuple[Any, Any]] = []

	def build(self, signal: Any, now: Any) -> RiskContext:
		self.build_calls.append((signal, now))
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
		risk=None,                        # paper/replay path — gate is a no-op
		risk_ctx_provider=fake_provider,  # provider present but must NOT be called
	)

	# Provider.build() must NOT have been called.
	assert fake_provider.build_calls == [], "provider.build() must NOT be called when risk=None"

	# Order was placed (enter signal proceeds ungated on paper path).
	assert len(placed_reqs) == 1, "executor.place must be called once on paper path"

	# Paper path: size_contracts must be 0 (byte-exact).
	assert placed_reqs[0].size_contracts == 0, (
		f"paper path must pass size_contracts=0, got {placed_reqs[0].size_contracts}"
	)


# ---------------------------------------------------------------------------
# D1 Tests — gate_exit operator-kill veto wired into _handle_signal exit branch
# ---------------------------------------------------------------------------
#
# Three invariants under test:
#   1. operator_kill_active=True → gate_exit → Reject("KILL_OPERATOR") →
#      _handle_exit NOT called (exit fully blocked — full-stop, spec §6)
#   2. operator_kill_active=False → gate_exit → Allow → _handle_exit IS called;
#      Allow.size_contracts is NOT used as exit size (real size from trade row)
#   3. risk=None (paper/replay) → _handle_exit called directly, no gate consulted
#      (G-parity: byte-identical to pre-D1)
# ---------------------------------------------------------------------------

_NOW_D1 = datetime(2026, 5, 22, 16, 0, 0, tzinfo=timezone.utc)
_TICKER_D1 = "KXSOL15M-26MAY22H16"
_SERIES_D1 = "KXSOL15M"
_STRATEGY_D1 = "debut_fade"


def _exit_signal_d1(trade_id: int = 42, side: str = "yes") -> Signal:
	"""Exit Signal carrying a trade_id (required by _handle_exit)."""
	return Signal(
		action="exit",
		ticker=_TICKER_D1,
		series=_SERIES_D1,
		side=side,
		strategy=_STRATEGY_D1,
		reason="test_d1",
		trade_id=trade_id,
	)


def _ctx_d1(yes_bid: int = 60, no_bid: int = 40) -> MagicMock:
	"""TickContext stub for exit tests; _handle_exit reads yes_bid/no_bid."""
	ctx = MagicMock()
	ctx.yes_bid = yes_bid
	ctx.no_bid = no_bid
	ctx.market_state = MagicMock()
	return ctx


def _rctx_d1(*, operator_kill_active: bool) -> RiskContext:
	"""Minimal RiskContext with operator_kill_active set as requested."""
	return RiskContext(
		now_utc=_NOW_D1,
		market_state=MagicMock(),
		open_count=1,
		open_positions=[],
		daily_pnl_cents=0,
		operator_kill_active=operator_kill_active,
	)


class _FakeExitGate:
	"""Scripted Gate for exit tests: gate_entry is unused; gate_exit returns a fixed decision."""

	def __init__(self, exit_decision: Any) -> None:
		self._exit_decision = exit_decision
		self.gate_exit_calls: list[Any] = []

	def gate_entry(self, signal: Any, ctx: Any) -> Any:  # pragma: no cover
		raise AssertionError("gate_entry must NOT be called from the exit branch")

	def gate_exit(self, signal: Any, ctx: Any) -> Any:
		self.gate_exit_calls.append((signal, ctx))
		return self._exit_decision


class _FakeExitRiskContextProvider:
	"""Returns a fixed RiskContext; records whether .build() was called."""

	def __init__(self, rctx: RiskContext) -> None:
		self._rctx = rctx
		self.build_calls: list[tuple[Any, Any]] = []

	def build(self, signal: Any, now: Any) -> RiskContext:
		self.build_calls.append((signal, now))
		return self._rctx


# ---------------------------------------------------------------------------
# D1 Test 1 — operator kill ACTIVE → gate_exit Reject → _handle_exit NOT called
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_exit_blocked_by_operator_kill(monkeypatch: Any) -> None:
	"""gate_exit returns Reject('KILL_OPERATOR') when operator_kill_active=True;
	dispatch must NOT call _handle_exit — exit is fully blocked (spec §6 full-stop).

	Spy: monkeypatch replaces dispatch._handle_exit with a sentinel that records
	calls. If it is invoked, the test fails — the operator kill must BLOCK the exit.
	"""
	import edge_catcher.engine.dispatch as dispatch_mod

	handle_exit_calls: list[Any] = []

	async def _spy_handle_exit(*args: Any, **kwargs: Any) -> None:
		handle_exit_calls.append((args, kwargs))

	monkeypatch.setattr(dispatch_mod, "_handle_exit", _spy_handle_exit)

	store = _CapturingStore()
	placed_reqs: list[OrderRequest] = []

	async def _fake_place(req: OrderRequest) -> OrderResult:  # pragma: no cover
		placed_reqs.append(req)
		return _filled_result(size_contracts=req.size_contracts)

	executor = MagicMock()
	executor.place = _fake_place

	rctx = _rctx_d1(operator_kill_active=True)
	reject_decision = Reject(reason="KILL_OPERATOR", detail="exit blocked by operator kill")
	fake_risk = _FakeExitGate(exit_decision=reject_decision)
	fake_provider = _FakeExitRiskContextProvider(rctx=rctx)

	sig = _exit_signal_d1(trade_id=42)

	await dispatch_mod._handle_signal(
		sig,
		_ctx_d1(),
		store,
		config={},
		executor=executor,
		now=_NOW_D1,
		risk=fake_risk,  # type: ignore[arg-type]
		risk_ctx_provider=fake_provider,  # type: ignore[arg-type]
	)

	# gate_exit must have been consulted exactly once.
	assert len(fake_risk.gate_exit_calls) == 1, (
		"gate_exit() must be called exactly once on the exit path"
	)

	# _handle_exit must NOT be called — operator kill is a full-stop (spec §6).
	assert handle_exit_calls == [], (
		"_handle_exit must NOT be called when gate_exit returns Reject('KILL_OPERATOR')"
	)

	# executor.place must NOT be called (no exit placed).
	assert placed_reqs == [], "executor.place must NOT be called on an operator-kill-blocked exit"


# ---------------------------------------------------------------------------
# D1 Test 2 — operator kill INACTIVE → gate_exit Allow → _handle_exit IS called;
#             Allow.size_contracts is NOT used as exit size
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_exit_allowed_when_operator_kill_inactive(monkeypatch: Any) -> None:
	"""gate_exit returns Allow when operator_kill_active=False (spec §6: only the
	operator kill is a full-stop; auto-tripped caps never block exits).

	Asserts:
	- _handle_exit IS called (exit proceeds).
	- Allow.size_contracts (proxy value 99) is NOT used as the exit size —
	  the real exit size comes from the trade row inside _handle_exit.

	The proxy size invariant is enforced structurally: gate_exit returns Allow with
	a sentinel size (99) distinct from any plausible row fill_size; if dispatch
	passed Allow.size_contracts into _handle_exit as the exit size, a downstream
	assertion would catch the 99 contamination. Here we just confirm _handle_exit
	receives NO size_contracts kwarg from the caller (it reads the row itself).
	"""
	import edge_catcher.engine.dispatch as dispatch_mod

	handle_exit_calls: list[tuple[Any, Any]] = []

	async def _spy_handle_exit(*args: Any, **kwargs: Any) -> None:
		handle_exit_calls.append((args, kwargs))

	monkeypatch.setattr(dispatch_mod, "_handle_exit", _spy_handle_exit)

	store = _CapturingStore()
	placed_reqs: list[OrderRequest] = []

	async def _fake_place(req: OrderRequest) -> OrderResult:  # pragma: no cover
		placed_reqs.append(req)
		return _filled_result(size_contracts=req.size_contracts)

	executor = MagicMock()
	executor.place = _fake_place

	rctx = _rctx_d1(operator_kill_active=False)
	# Sentinel proxy size 99 — must NOT appear in _handle_exit kwargs.
	allow_decision = Allow(size_contracts=99, sizing_breakdown=_bd_c3(99))
	fake_risk = _FakeExitGate(exit_decision=allow_decision)
	fake_provider = _FakeExitRiskContextProvider(rctx=rctx)

	sig = _exit_signal_d1(trade_id=7)

	await dispatch_mod._handle_signal(
		sig,
		_ctx_d1(),
		store,
		config={},
		executor=executor,
		now=_NOW_D1,
		risk=fake_risk,  # type: ignore[arg-type]
		risk_ctx_provider=fake_provider,  # type: ignore[arg-type]
	)

	# gate_exit must have been consulted.
	assert len(fake_risk.gate_exit_calls) == 1, "gate_exit() must be called exactly once"

	# _handle_exit must be called (exit is allowed).
	assert len(handle_exit_calls) == 1, (
		"_handle_exit must be called when gate_exit returns Allow"
	)

	# Allow.size_contracts (proxy=99) must NOT be forwarded as exit size.
	# _handle_exit signature takes no `size_contracts` param — verify it was not
	# smuggled via kwargs either.
	_, kw = handle_exit_calls[0]
	assert "size_contracts" not in kw, (
		f"Allow.size_contracts must NOT be passed to _handle_exit — "
		f"exit size comes from the trade row; got kwargs={sorted(kw)!r}"
	)
	assert kw.get("size_contracts", None) != 99, (
		"Allow.size_contracts sentinel 99 must not leak into _handle_exit kwargs"
	)


# ---------------------------------------------------------------------------
# D1 Test 3 — Paper path: risk=None → _handle_exit called directly, no gate
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_exit_paper_mode_no_gate(monkeypatch: Any) -> None:
	"""risk=None (paper/replay): _handle_exit called directly with no gate consultation.

	G-parity: the paper exit path is BYTE-IDENTICAL — no RiskContext constructed,
	no gate_exit call, unconditional _handle_exit invocation (spec §6 / G-parity).

	Spy: monkeypatch replaces dispatch._handle_exit with a sentinel that records
	calls. Confirms it is called exactly once with no gate intervention.
	"""
	import edge_catcher.engine.dispatch as dispatch_mod

	handle_exit_calls: list[tuple[Any, Any]] = []

	async def _spy_handle_exit(*args: Any, **kwargs: Any) -> None:
		handle_exit_calls.append((args, kwargs))

	monkeypatch.setattr(dispatch_mod, "_handle_exit", _spy_handle_exit)

	store = _CapturingStore()
	placed_reqs: list[OrderRequest] = []

	async def _fake_place(req: OrderRequest) -> OrderResult:  # pragma: no cover
		placed_reqs.append(req)
		return _filled_result(size_contracts=req.size_contracts)

	executor = MagicMock()
	executor.place = _fake_place

	# Spy provider — must NOT be consulted on the paper path.
	fake_provider = _FakeExitRiskContextProvider(rctx=_rctx_d1(operator_kill_active=True))
	# fake_risk with gate_entry guard — must NOT be consulted on paper path.
	fake_risk = _FakeExitGate(exit_decision=Reject(reason="KILL_OPERATOR", detail="should not reach"))

	sig = _exit_signal_d1(trade_id=5)

	await dispatch_mod._handle_signal(
		sig,
		_ctx_d1(),
		store,
		config={},
		executor=executor,
		now=_NOW_D1,
		risk=None,  # paper/replay path — gate is a no-op
		risk_ctx_provider=fake_provider,  # present but must NOT be called
	)

	# Provider.build() must NOT have been called (paper path).
	assert fake_provider.build_calls == [], (
		"provider.build() must NOT be called when risk=None (paper path)"
	)

	# gate_exit must NOT have been consulted.
	assert fake_risk.gate_exit_calls == [], (
		"gate_exit must NOT be called when risk=None (paper/replay G-parity)"
	)

	# _handle_exit must be called exactly once (exit proceeds ungated on paper path).
	assert len(handle_exit_calls) == 1, (
		"_handle_exit must be called exactly once on the paper exit path"
	)


# ---------------------------------------------------------------------------
# Live-signal enrichment — derive entry_price_cents / stop_loss_distance_cents
# from the tick for strategies that don't emit them.
#
# Production strategies (edge_catcher/engine/strategies_local.py) emit
# framework-agnostic enter signals: side/ticker/series/reason, NO execution
# price/stop. The paper path derives the entry price from the tick inside
# _handle_enter (entry_price = ctx.yes_ask), so paper works. The LIVE path has
# TWO consumers that REQUIRE those fields:
#   * gate_entry (risk.py:712) Rejects INVALID_SIGNAL when entry<=0 or sl<=0;
#   * build_entry_order (execution.py:190) raises ValueError if either is None.
# So on the live path the dispatcher must derive both from the tick BEFORE the
# gate sees the signal. CR-5 caught this: the bundled real strategy produced 7
# paper trades and 0 live trades (every entry ValueError'd at build_entry_order;
# in production the real gate would reject every entry as INVALID_SIGNAL first).
# Enrichment is LIVE-ONLY (risk is not None) → paper/replay signals stay None
# (paper derives the price internally + has no gate) → G-parity preserved.
# ---------------------------------------------------------------------------


def _bare_entry_signal(side: str = "yes") -> Signal:
	"""An enter Signal as the real strategies emit it — NO entry_price_cents /
	stop_loss_distance_cents (those are execution concerns the strategy doesn't
	know about)."""
	return Signal(
		action="enter",
		ticker=_TICKER_C3,
		series=_SERIES_C3,
		side=side,
		strategy=_STRATEGY_C3,
		reason="bare-signal (real strategy shape)",
	)


@pytest.mark.asyncio
async def test_live_enter_derives_missing_price_and_stop_before_gate() -> None:
	"""LIVE path: a bare enter signal (no price/stop) is enriched from the tick
	BEFORE the gate is consulted, so the gate's INVALID_SIGNAL check passes and
	build_entry_order can build the order.

	entry_price_cents must equal the tick ask the paper path books at
	(ctx.yes_ask) so live and paper book at the same price. stop_loss_distance_cents
	defaults to the entry cost — the per-contract risk on a binary contract with
	no hard stop — so the fixed-fraction sizing arm becomes
	equity·sizing_pct/entry = pure-fractional capital allocation (Phase-1 intent).
	"""
	store = _CapturingStore()
	placed_reqs: list[OrderRequest] = []

	async def _fake_place(req: OrderRequest) -> OrderResult:
		placed_reqs.append(req)
		return _filled_result(size_contracts=req.size_contracts)

	executor = MagicMock()
	executor.place = _fake_place

	fake_risk = _FakeGate(Allow(size_contracts=4, sizing_breakdown=_bd_c3(4)))
	fake_provider = _FakeRiskContextProvider()
	config: dict[str, Any] = {
		"_metrics": Metrics(),
		"_exec_cfg": _exec_cfg(entry_slippage_cents=0),  # 0 → limit == tick ask
	}

	bare = _bare_entry_signal(side="yes")
	assert bare.entry_price_cents is None and bare.stop_loss_distance_cents is None

	await _handle_signal(
		bare,
		_ctx_c3(yes_ask=45),
		store,
		config,
		executor,
		now=_NOW_C3,
		risk=fake_risk,  # type: ignore[arg-type]
		risk_ctx_provider=fake_provider,  # type: ignore[arg-type]
	)

	# The gate must have been consulted with an ENRICHED signal.
	assert len(fake_risk.gate_entry_calls) == 1, "gate_entry must be called once"
	seen_sig = fake_risk.gate_entry_calls[0][0]
	assert seen_sig.entry_price_cents == 45, (
		"entry_price_cents must be derived from the tick ask (45) before the gate "
		f"sees the signal, got {seen_sig.entry_price_cents!r}"
	)
	assert seen_sig.stop_loss_distance_cents == 45, (
		"stop_loss_distance_cents must default to the entry cost (45) for "
		f"pure-fractional sizing, got {seen_sig.stop_loss_distance_cents!r}"
	)

	# An order was placed with the derived limit (slippage 0 → limit == tick ask).
	assert len(placed_reqs) == 1, "executor.place must be called once (not ValueError)"
	assert placed_reqs[0].limit_price_cents == 45, (
		f"live limit must equal the tick ask 45, got {placed_reqs[0].limit_price_cents}"
	)


@pytest.mark.asyncio
async def test_live_enter_no_side_derives_from_no_ask() -> None:
	"""LIVE path, NO-side bare signal: entry price is derived from ctx.no_ask
	(mirrors paper's _handle_enter entry_price = ctx.no_ask for no-side)."""
	store = _CapturingStore()
	placed_reqs: list[OrderRequest] = []

	async def _fake_place(req: OrderRequest) -> OrderResult:
		placed_reqs.append(req)
		return _filled_result(size_contracts=req.size_contracts)

	executor = MagicMock()
	executor.place = _fake_place

	fake_risk = _FakeGate(Allow(size_contracts=3, sizing_breakdown=_bd_c3(3)))
	fake_provider = _FakeRiskContextProvider()
	config: dict[str, Any] = {
		"_metrics": Metrics(),
		"_exec_cfg": _exec_cfg(entry_slippage_cents=0),
	}

	bare = _bare_entry_signal(side="no")
	await _handle_signal(
		bare,
		_ctx_c3(yes_ask=45, no_ask=55),
		store,
		config,
		executor,
		now=_NOW_C3,
		risk=fake_risk,  # type: ignore[arg-type]
		risk_ctx_provider=fake_provider,  # type: ignore[arg-type]
	)

	seen_sig = fake_risk.gate_entry_calls[0][0]
	assert seen_sig.entry_price_cents == 55, (
		f"no-side entry price must derive from no_ask (55), got {seen_sig.entry_price_cents!r}"
	)
	assert placed_reqs[0].limit_price_cents == 55


@pytest.mark.asyncio
async def test_paper_enter_bare_signal_not_enriched() -> None:
	"""G-parity guard: on the PAPER path (risk=None) a bare signal is NOT
	enriched — its execution fields stay None (paper derives the entry price
	itself inside _handle_enter) and the order is placed with size_contracts=0.
	Enrichment must be live-only so the paper path stays byte-identical."""
	store = _CapturingStore()
	placed_reqs: list[OrderRequest] = []

	async def _fake_place(req: OrderRequest) -> OrderResult:
		placed_reqs.append(req)
		return _filled_result(size_contracts=req.size_contracts)

	executor = MagicMock()
	executor.place = _fake_place

	config: dict[str, Any] = {"_metrics": Metrics()}  # no _exec_cfg → paper path
	bare = _bare_entry_signal(side="yes")

	await _handle_signal(
		bare,
		_ctx_c3(yes_ask=45),
		store,
		config,
		executor,
		now=_NOW_C3,
		risk=None,  # paper/replay — no gate, no enrichment
		risk_ctx_provider=None,
	)

	# Signal left untouched (no live-only enrichment on the paper path).
	assert bare.entry_price_cents is None, "paper path must NOT enrich the signal"
	assert bare.stop_loss_distance_cents is None
	# Paper places size_contracts=0 (executor sizes internally) — byte-exact.
	assert len(placed_reqs) == 1
	assert placed_reqs[0].size_contracts == 0


def test_metrics_registers_wide_spread_counter() -> None:
	"""The live spread gate increments entries_skipped_wide_spread; Metrics.inc
	raises KeyError on an unregistered key, so the counter MUST be registered."""
	m = Metrics()
	m.inc("entries_skipped_wide_spread")
	assert m.snapshot()["entries_skipped_wide_spread"] == 1
