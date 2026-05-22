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
