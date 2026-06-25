"""Tests for dispatch_message routing.

Each test constructs a synthetic event, calls dispatch_message, and asserts
the expected MarketState mutation. These verify the router dispatches each
message type to the correct handler without regressing the behavior of
handlers that were relocated from engine.py.

Behavioral tests for the handlers themselves live in tests/test_engine.py —
the handlers moved modules but their logic and tests stayed intact.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import pytest

from edge_catcher.engine import dispatch as dispatch_module
from edge_catcher.engine.dispatch import (
	_handle_orderbook_delta,
	_handle_orderbook_snapshot,
	dispatch_message,
	process_tick,
)
from edge_catcher.engine.executors.paper import PaperExecutor
from edge_catcher.engine.market_state import MarketState, OrderbookSnapshot, TickContext
from edge_catcher.engine.risk import KillSwitchTripFailed
from edge_catcher.engine.strategy_base import Signal, Strategy
from edge_catcher.engine.trade_store import TradeStore


def _now() -> datetime:
	return datetime(2026, 4, 14, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def market_state() -> MarketState:
	return MarketState()


@pytest.fixture
def store(tmp_path: Path) -> TradeStore:
	ts = TradeStore(tmp_path / "test.db")
	yield ts
	ts.close()


@pytest.fixture
def call_args(market_state: MarketState, store: TradeStore) -> dict:
	"""Common kwargs for dispatch_message calls in this test module."""
	return dict(
		config={},
		market_state=market_state,
		store=store,
		strategies=[],
		strat_by_series={},
		pending_states={},
		dirty=set(),
		executor=PaperExecutor(market_state=market_state, config={}),
		now=_now(),
	)


@pytest.mark.asyncio
async def test_dispatch_routes_orderbook_snapshot(market_state: MarketState, call_args: dict) -> None:
	"""A WS orderbook_snapshot event populates market_state for the ticker."""
	event = {
		"source": "ws",
		"payload": {
			"type": "orderbook_snapshot",
			"msg": {
				"market_ticker": "KXTEST-26APR14",
				"yes": [["0.50", 100]],
				"no": [["0.48", 50]],
			},
		},
	}
	await dispatch_message(event, **call_args)
	ob = market_state.get_orderbook("KXTEST-26APR14")
	assert ob is not None
	assert ob.yes_levels == [(0.50, 100)]
	assert ob.no_levels == [(0.48, 50)]


@pytest.mark.asyncio
async def test_dispatch_routes_orderbook_delta(market_state: MarketState, call_args: dict) -> None:
	"""A WS orderbook_delta event applies a delta to an existing book."""
	# Seed the book first
	market_state.seed_orderbook("KXTEST-26APR14", OrderbookSnapshot(
		yes_levels=[(0.50, 100)],
		no_levels=[(0.48, 50)],
	))
	event = {
		"source": "ws",
		"payload": {
			"type": "orderbook_delta",
			"msg": {
				"market_ticker": "KXTEST-26APR14",
				"yes": [["0.50", -20]],  # 20 contracts lifted
				"no": [],
			},
		},
	}
	await dispatch_message(event, **call_args)
	ob = market_state.get_orderbook("KXTEST-26APR14")
	assert ob.yes_levels == [(0.50, 80)]


@pytest.mark.asyncio
async def test_dispatch_accepts_raw_ws_shape(market_state: MarketState, call_args: dict) -> None:
	"""Raw WS messages (no source/payload wrapper) are also accepted."""
	raw = {
		"type": "orderbook_snapshot",
		"msg": {
			"market_ticker": "KXTEST-26APR14",
			"yes": [["0.42", 10]],
			"no": [["0.60", 15]],
		},
	}
	await dispatch_message(raw, **call_args)
	ob = market_state.get_orderbook("KXTEST-26APR14")
	assert ob is not None
	assert ob.yes_levels == [(0.42, 10)]


@pytest.mark.asyncio
async def test_dispatch_unknown_msg_type_is_noop(market_state: MarketState, call_args: dict) -> None:
	"""An unknown WS msg_type is logged and ignored, not raised."""
	event = {
		"source": "ws",
		"payload": {"type": "heartbeat", "msg": {}},
	}
	# Should not raise
	await dispatch_message(event, **call_args)
	# No orderbook side-effect
	assert market_state.get_orderbook("ANY") is None


# ---------------------------------------------------------------------------
# Synthetic event handlers (Task 8 — replay side of the capture tee points)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dispatch_synthetic_rest_orderbook_seeds_market_state(
	market_state: MarketState, call_args: dict
) -> None:
	"""synthetic.rest_orderbook mirrors the live run_recovery's seed_orderbook
	at the moment the live REST call was made — replay ingests the captured
	yes/no levels directly without re-parsing the raw Kalshi response."""
	event = {
		"source": "synthetic.rest_orderbook",
		"payload": {
			"ticker": "KXTEST-26APR14",
			"yes_levels": [[0.42, 100], [0.41, 50]],
			"no_levels": [[0.58, 75]],
		},
	}
	await dispatch_message(event, **call_args)
	ob = market_state.get_orderbook("KXTEST-26APR14")
	assert ob is not None
	assert (0.42, 100) in ob.yes_levels
	assert (0.58, 75) in ob.no_levels


@pytest.mark.asyncio
async def test_dispatch_synthetic_ticker_discovered_seeds_market_state(
	market_state: MarketState, call_args: dict
) -> None:
	"""synthetic.ticker_discovered is emitted by _ticker_refresh and handled
	identically to synthetic.rest_orderbook on the replay side. The tag
	exists for telemetry — both paths feed the same seed_orderbook call."""
	event = {
		"source": "synthetic.ticker_discovered",
		"payload": {
			"ticker": "KXNEW-26APR14",
			"yes_levels": [[0.30, 20]],
			"no_levels": [[0.70, 15]],
		},
	}
	await dispatch_message(event, **call_args)
	ob = market_state.get_orderbook("KXNEW-26APR14")
	assert ob is not None
	assert (0.30, 20) in ob.yes_levels


@pytest.mark.asyncio
async def test_dispatch_synthetic_settlement_resolves_open_trade(
	store: 'TradeStore', call_args: dict
) -> None:
	"""synthetic.settlement should look up an open trade by composite key
	(strategy, ticker, side, entry_time) and settle it with the captured
	market outcome. The `now` passed in is used as exit_time so replay
	produces byte-identical rows to live."""
	entry_now = datetime(2026, 4, 14, 12, 0, 0, tzinfo=timezone.utc)
	settle_now = datetime(2026, 4, 14, 14, 0, 0, tzinfo=timezone.utc)
	trade_id = store.record_trade(
		ticker="KXSETTLE-26APR14",
		entry_price=40,
		strategy="test-strat",
		side="yes",
		series_ticker="KXSETTLE",
		blended_entry=40,
		now=entry_now,
	)
	event = {
		"source": "synthetic.settlement",
		"payload": {
			"strategy": "test-strat",
			"ticker": "KXSETTLE-26APR14",
			"side": "yes",
			"entry_time": entry_now.isoformat(),
			"result": "yes",  # raw market outcome, NOT 'won'/'lost'
		},
	}
	call_args_with_settle_now = dict(call_args)
	call_args_with_settle_now["now"] = settle_now
	await dispatch_message(event, **call_args_with_settle_now)

	# Verify the trade was settled with the captured now
	settled = store.get_trade_by_id(trade_id)
	assert settled is not None
	assert settled["status"] == "won"
	assert settled["exit_time"] == settle_now.isoformat()
	assert settled["exit_price"] == 100  # yes-side wins at 100


@pytest.mark.asyncio
async def test_dispatch_synthetic_settlement_no_match_is_logged_not_raised(
	store: 'TradeStore', call_args: dict
) -> None:
	"""When the composite key doesn't match any open trade, the handler logs
	a warning and returns — replay shouldn't crash on a stale settlement event."""
	event = {
		"source": "synthetic.settlement",
		"payload": {
			"strategy": "ghost",
			"ticker": "KXGHOST",
			"side": "yes",
			"entry_time": "2026-04-14T12:00:00+00:00",
			"result": "yes",
		},
	}
	await dispatch_message(event, **call_args)  # must not raise


@pytest.mark.asyncio
async def test_dispatch_synthetic_unknown_source_warns_and_returns(call_args: dict) -> None:
	"""An unknown synthetic source logs a warning and returns."""
	event = {
		"source": "synthetic.something_new",
		"payload": {},
	}
	await dispatch_message(event, **call_args)  # must not raise


# ---------------------------------------------------------------------------
# Test 1.c (dispatch contract extension) — MarketState.clear() must propagate
# all the way to the strategy's TickContext.is_first_observation. Guards
# against any future dispatch-side caching that would defeat Change 3.
#
# Per docs/superpowers/plans/replay-first-seen-fix.md §"Step 1 — write tests"
# (1.c). Expected GREEN at commit time (Change 3 needs no source change).
# ---------------------------------------------------------------------------




class _CaptureStrategy(Strategy):
	"""Stub Strategy that just records every TickContext it sees."""

	name = "capture-test"
	supported_series = ["KXTEST"]
	default_params: dict = {}

	def __init__(self) -> None:
		self.captured_contexts: list[TickContext] = []

	def on_tick(self, ctx: TickContext) -> list:
		self.captured_contexts.append(ctx)
		return []


def _seed_ticker_for_trade_dispatch(ms: MarketState, ticker: str) -> None:
	"""Make `ticker` ready to receive a trade-dispatched tick.

	`_handle_trade_msg` requires both:
	  - `get_price_history(ticker) is not None` (i.e. ticker is registered)
	  - a non-empty orderbook so yes_ask / yes_bid resolve
	"""
	ms.register_ticker(ticker, meta={"event_ticker": "KXTEST"})
	ms.seed_orderbook(
		ticker,
		OrderbookSnapshot(
			yes_levels=[(0.50, 100)],
			no_levels=[(0.48, 100)],
		),
	)


def _trade_event(ticker: str, yes_price: float = 0.50) -> dict:
	return {
		"source": "ws",
		"payload": {
			"type": "trade",
			"msg": {
				"market_ticker": ticker,
				"yes_price": yes_price,
				"taker_side": "yes",
				"count": 1,
			},
		},
	}


@pytest.mark.asyncio
async def test_market_state_clear_propagates_to_dispatch(
	market_state: MarketState, store: TradeStore
) -> None:
	"""After MarketState.clear() (and re-seeding to satisfy dispatch's
	guards), a fresh trade tick MUST surface is_first_observation=True
	through dispatch — protects against any future dispatch-side caching
	that would defeat Change 3 (clear discipline).
	"""
	ticker = "KXTEST-T1"
	strat = _CaptureStrategy()

	call_args = dict(
		config={},
		market_state=market_state,
		store=store,
		strategies=[strat],
		strat_by_series={"KXTEST": [strat]},
		pending_states={},
		dirty=set(),
		executor=PaperExecutor(market_state=market_state, config={}),
		now=_now(),
	)

	# --- Stage 1: prime the ticker with one trade so it becomes "seen". ---
	_seed_ticker_for_trade_dispatch(market_state, ticker)
	await dispatch_message(_trade_event(ticker), **call_args)
	assert len(strat.captured_contexts) == 1, "first dispatch should reach the strategy"
	assert strat.captured_contexts[-1].is_first_observation is True, (
		"first observation should be flagged"
	)

	# Sanity: a second dispatch is NOT first-seen.
	await dispatch_message(_trade_event(ticker), **call_args)
	assert strat.captured_contexts[-1].is_first_observation is False, (
		"second observation must not be flagged as first"
	)

	# --- Stage 2: clear, re-seed (the clear-then-reseed cycle that recovery
	# does on WS reconnect), then dispatch a new trade. ---
	market_state.clear()
	_seed_ticker_for_trade_dispatch(market_state, ticker)
	await dispatch_message(_trade_event(ticker), **call_args)
	assert strat.captured_contexts[-1].is_first_observation is True, (
		"after MarketState.clear(), the next dispatched trade tick MUST flag "
		"is_first_observation=True — Change 3 (clear discipline) is broken if not"
	)


# ---------------------------------------------------------------------------
# Risk-gate enforcement — Q1 + Q2 regression tests (PR #36 R2 fixes)
# ---------------------------------------------------------------------------

class _StubStrategy:
	"""Minimal strategy that emits one enter signal per on_tick call."""
	name = "stub"
	emoji = "🔵"

	def on_tick(self, ctx: TickContext) -> list[Signal]:  # type: ignore[override]
		return [Signal(action="enter", ticker=ctx.ticker, side="yes",
			series="X", strategy=self.name, reason="test")]


def _make_tick_ctx() -> TickContext:
	"""Build a minimal TickContext for risk-gate enforcement tests."""
	return TickContext(
		ticker="X", event_ticker="EX", yes_bid=50, yes_ask=51, no_bid=49, no_ask=50,
		orderbook=OrderbookSnapshot(yes_levels=[], no_levels=[]),
		price_history=[], open_positions=[], persisted_state={}, market_metadata={},
	)


@pytest.mark.asyncio
async def test_process_tick_reraises_kill_switch_trip_failed(monkeypatch, market_state, store):
	"""Q1 regression: process_tick MUST re-raise KillSwitchTripFailed so the
	engine STOPS on kill-switch INSERT failure (C-spec L214 ghost-reject
	defense). The broad `except Exception` for other signal-handling errors
	must NOT swallow this specific exception class.
	"""
	async def fake_handle_signal(*args, **kwargs):
		raise KillSwitchTripFailed("simulated kill_switch INSERT failure")

	monkeypatch.setattr(dispatch_module, "_handle_signal", fake_handle_signal)

	ctx = _make_tick_ctx()
	with pytest.raises(KillSwitchTripFailed):
		await process_tick(
			ctx, [_StubStrategy()], store, config={},
			executor=PaperExecutor(market_state=market_state, config={}),
			now=_now(),
			risk=None,
		)


@pytest.mark.asyncio
async def test_process_tick_swallows_non_kill_switch_exceptions(monkeypatch, market_state, store, caplog):
	"""Counter-test for Q1: non-KillSwitchTripFailed exceptions must STILL be
	logged + swallowed (preserves existing per-signal isolation behavior).
	"""
	async def fake_handle_signal(*args, **kwargs):
		raise ValueError("simulated business-logic error")

	monkeypatch.setattr(dispatch_module, "_handle_signal", fake_handle_signal)

	ctx = _make_tick_ctx()
	with caplog.at_level(logging.ERROR):
		# Should NOT raise — process_tick logs and continues.
		await process_tick(
			ctx, [_StubStrategy()], store, config={},
			executor=PaperExecutor(market_state=market_state, config={}),
			now=_now(),
			risk=None,
		)
	assert any("Error handling" in rec.message for rec in caplog.records)



# ---------------------------------------------------------------------------
# KillSwitchTripFailed propagation chain — C-spec L214 ghost-reject defense
# ---------------------------------------------------------------------------
#
# When `Gate._emit_trip` cannot persist the kill row (DB INSERT failure), it
# raises ``KillSwitchTripFailed``. That exception must propagate all the way
# out of ``run_engine`` so the engine actually STOPS — otherwise the next tick
# re-enters the gate, finds no kill row, and lets the previously-blocked trade
# through with real money. The chain is:
#
#   Gate._emit_trip → _handle_signal → process_tick → _handle_*_msg →
#   dispatch_message → engine._ws_loop → run_engine's reconnect loop
#
# The R2 backfill already covers process_tick's re-raise (test above). These
# tests cover the remaining links in the chain.


@pytest.mark.asyncio
async def test_dispatch_message_propagates_kill_switch_trip_failed(
	monkeypatch, market_state, store, call_args
):
	"""dispatch_message must NOT catch KillSwitchTripFailed propagating up from
	_handle_ticker_msg / _handle_trade_msg / process_tick.  The router has no
	try/except around the handler calls — this test verifies that invariant
	is preserved if the router structure changes in future."""
	async def fake_handle_ticker(*args, **kwargs):
		raise KillSwitchTripFailed("simulated kill INSERT failure")

	monkeypatch.setattr(dispatch_module, "_handle_ticker_msg", fake_handle_ticker)

	event = {
		"source": "ws",
		"payload": {
			"type": "ticker",
			"msg": {"market_ticker": "KXTEST-26APR14", "yes_bid": 50, "yes_ask": 51},
		},
	}
	with pytest.raises(KillSwitchTripFailed):
		await dispatch_message(event, **call_args)


def test_ws_loop_reraises_kill_switch_trip_failed_in_source():
	"""engine._ws_loop's per-message try/except must explicitly re-raise
	KillSwitchTripFailed BEFORE the broad `except Exception:` swallow clause.
	Without this, the loop would log + continue to the next message, the next
	tick would re-enter the gate against unchanged DB state (kill INSERT
	failed), and the previously-blocked trade would go through.

	This is a structural test (inspect.getsource) because _ws_loop is tightly
	coupled to the WS connection lifecycle and a behavioral test would require
	mocking websockets.connect, the auth headers, and the recovery path. The
	structural invariant ("KillSwitchTripFailed handler exists and re-raises
	before the broad Exception catch") is what's actually being defended; the
	test fails if a future refactor removes or weakens it.
	"""
	import inspect

	from edge_catcher.engine import engine as engine_module

	source = inspect.getsource(engine_module._ws_loop)

	assert "except KillSwitchTripFailed:" in source, (
		"engine._ws_loop must explicitly handle KillSwitchTripFailed before "
		"the broad `except Exception:` — see C-spec L214 ghost-reject defense"
	)

	# The KillSwitchTripFailed handler must appear BEFORE the broad `except
	# Exception:` so it's matched first (Python except clauses are checked in
	# source order).
	ks_idx = source.index("except KillSwitchTripFailed:")
	exc_idx = source.index("except Exception:")
	assert ks_idx < exc_idx, (
		"`except KillSwitchTripFailed:` must precede `except Exception:` so "
		"the typed handler is matched first; otherwise the broad except will "
		"swallow the kill-trip-failure"
	)

	# The handler body must re-raise. Inspect the slice between the
	# KillSwitchTripFailed handler and the next `except` — there must be a
	# bare `raise` statement.
	handler_body = source[ks_idx : exc_idx]
	assert "\n\t\t\t\traise\n" in handler_body or "\n\t\t\traise\n" in handler_body, (
		f"engine._ws_loop's KillSwitchTripFailed handler must re-raise to "
		f"propagate to the outer reconnect block. Handler body:\n{handler_body}"
	)


def test_outer_reconnect_loop_reraises_kill_switch_trip_failed_in_source():
	"""engine.run_engine's outer reconnect-while-loop must explicitly re-raise
	KillSwitchTripFailed BEFORE its broad `except Exception:` (which reconnects
	with backoff). Without this, even if _ws_loop correctly propagates, the
	outer reconnect block would catch it and resume processing — defeating
	the defense.

	Structural test because the reconnect block is inlined in run_engine and
	would require massive mocking to test behaviorally.
	"""
	import inspect

	from edge_catcher.engine import engine as engine_module

	source = inspect.getsource(engine_module.run_engine)

	# The outer reconnect block contains the same `except Exception:` /
	# reconnect pattern. Verify a KillSwitchTripFailed clause exists in the
	# outer block too.
	assert source.count("except KillSwitchTripFailed:") >= 1, (
		"engine.run_engine's outer reconnect loop must explicitly handle "
		"KillSwitchTripFailed — see C-spec L214 ghost-reject defense.  "
		"Without this, the engine reconnects on kill-INSERT failure and "
		"the next tick re-enters the gate with no kill row persisted."
	)


@pytest.mark.asyncio
async def test_bankroll_refresh_propagates_kill_switch_trip_failed():
	"""BankrollCache.refresh's docstring promises that KillSwitchTripFailed
	from the auto-panic trip propagates out (C-spec L214 ghost-reject defense).
	Callers (E's periodic refresh task, on_fill, on_settlement) must NOT
	wrap this in try/except — otherwise a silent kill-INSERT failure leaves
	the trader running against unchanged DB state.
	"""
	from edge_catcher.engine.risk import (
		BankrollCache,
		KillSwitchTripFailed,
		RiskConfig,
	)

	class _FlakySource:
		"""Always raises — simulates an unreachable venue."""
		async def balance_cents(self) -> int:
			raise ConnectionError("simulated venue unreachable")

	def _failing_emit_trip(reason: str, *, detail: str | None = None, now=None) -> None:
		raise KillSwitchTripFailed("simulated kill_switch INSERT failure")

	cfg = RiskConfig(
		sizing_pct=0.0025,
		daily_loss_pct=0.02,
		drawdown_pct=0.30,
		max_open=20,
		min_fill_contracts=1,
		absolute_panic_floor_cents=3000,
		absolute_max_cents=5000,
		kelly_shrinkage=0.5,
		bankroll_ttl_seconds=300.0,
		bankroll_failures_until_kill=2,
	)
	cache = BankrollCache(_source=_FlakySource(), _cfg=cfg)  # type: ignore[arg-type]
	cache._emit_trip_fn = _failing_emit_trip

	# First refresh: increments failure counter to 1 (below threshold = 2)
	await cache.refresh()
	# Second refresh: counter hits threshold → emit_trip_fn called → raises
	with pytest.raises(KillSwitchTripFailed):
		await cache.refresh()


# ---------------------------------------------------------------------------
# _handle_orderbook_delta V2 scalar shape tests
# ---------------------------------------------------------------------------

def test_dispatch_routes_orderbook_delta_v2():
	# V2 scalar frame applies exactly one delta with the parsed (side, price$, qty).
	ms = MarketState()
	ms.seed_orderbook("KXT", OrderbookSnapshot(yes_levels=[], no_levels=[(0.99, 10)]))
	msg = {"type": "orderbook_delta", "msg": {
		"market_ticker": "KXT", "price_dollars": "0.9900", "delta_fp": "-4.00", "side": "no"}}
	_handle_orderbook_delta(ms, msg)
	ob = ms.get_orderbook("KXT")
	assert (0.99, 6) in ob.no_levels          # 10 + int(float("-4.00")) = 6


def test_dispatch_orderbook_delta_v2_calls_apply_once(monkeypatch):
	# Routing contract: exactly one apply with the exact parsed args (no double, no zero).
	ms = MarketState()
	calls: list = []
	monkeypatch.setattr(ms, "apply_orderbook_delta", lambda *a: calls.append(a))
	msg = {"msg": {"market_ticker": "KXT", "price_dollars": "0.43", "delta_fp": "7.00", "side": "yes"}}
	_handle_orderbook_delta(ms, msg)
	assert calls == [("KXT", "yes", 0.43, 7)]


def test_dispatch_orderbook_delta_v2_malformed_is_silent_noop(monkeypatch):
	# OverflowError ("1e999"), ValueError (""), bad side -> zero apply calls, no exception, no log.
	for bad in ({"side": "yes", "price_dollars": "0.5", "delta_fp": "1e999"},
	            {"side": "yes", "price_dollars": "", "delta_fp": "1"},
	            {"side": "maybe", "price_dollars": "0.5", "delta_fp": "1"}):
		ms = MarketState()
		calls: list = []
		monkeypatch.setattr(ms, "apply_orderbook_delta", lambda *a: calls.append(a))
		logged: list = []
		monkeypatch.setattr(dispatch_module.log, "exception", lambda *a, **k: logged.append(a))
		_handle_orderbook_delta(ms, {"msg": {"market_ticker": "KXT", **bad}})
		assert calls == [] and logged == []


def test_dispatch_orderbook_delta_neither_shape_is_noop(monkeypatch):
	ms = MarketState()
	calls: list = []
	monkeypatch.setattr(ms, "apply_orderbook_delta", lambda *a: calls.append(a))
	_handle_orderbook_delta(ms, {"msg": {"market_ticker": "KXT"}})  # no lists, no V2 scalars
	assert calls == []


def test_dispatch_orderbook_delta_v2_adds_fresh_level():
	# A positive V2 delta to a price NOT on the book ADDS it — the core "book evolves between
	# snapshots" behavior (apply_orderbook_delta's not-updated-and-delta>0 insert path).
	ms = MarketState()
	ms.seed_orderbook("KXT", OrderbookSnapshot(yes_levels=[], no_levels=[]))
	msg = {"msg": {"market_ticker": "KXT", "price_dollars": "0.55", "delta_fp": "3.00", "side": "no"}}
	_handle_orderbook_delta(ms, msg)
	ob = ms.get_orderbook("KXT")
	assert (0.55, 3) in ob.no_levels


def test_dispatch_orderbook_delta_legacy_branch_logs_on_bad_value(monkeypatch):
	# Pins the silent-skip ASYMMETRY: the legacy branch still logs per-failure (its original
	# `except Exception: log.exception`), whereas the V2 branch is silent (see the malformed test,
	# which asserts logged == []). Together they make the asymmetry a tested contract.
	ms = MarketState()
	ms.seed_orderbook("KXT", OrderbookSnapshot(yes_levels=[], no_levels=[]))
	logged: list = []
	monkeypatch.setattr(dispatch_module.log, "exception", lambda *a, **k: logged.append(a))
	# Legacy shape (a `yes` list is present -> legacy branch) with a non-numeric price -> float()
	# raises ValueError -> caught by the legacy `except Exception: log.exception`.
	_handle_orderbook_delta(ms, {"msg": {"market_ticker": "KXT", "yes": [["notaprice", 5]]}})
	assert logged  # legacy branch logged the failure (contrast: V2 malformed frames do NOT log)


def test_dispatch_orderbook_delta_v2_removes_level_at_zero():
	# A V2 delta that drives a level to exactly 0 REMOVES it (apply_orderbook_delta's `new_q > 0`
	# false branch) — the most common real delta: a resting level fully lifted. Pins the removal
	# path at the V2 dispatch seam (review round 3).
	ms = MarketState()
	ms.seed_orderbook("KXT", OrderbookSnapshot(yes_levels=[], no_levels=[(0.99, 4)]))
	msg = {"msg": {"market_ticker": "KXT", "price_dollars": "0.9900", "delta_fp": "-4.00", "side": "no"}}
	_handle_orderbook_delta(ms, msg)
	ob = ms.get_orderbook("KXT")
	assert ob.no_levels == []          # 4 + _parse_qty("-4.00") = 4 + (-4.0) = 0.0 -> level removed


# ---------------------------------------------------------------------------
# _handle_orderbook_snapshot fractional-quantity ingest
# ---------------------------------------------------------------------------

def test_orderbook_snapshot_retains_sub_one_contract_levels():
	# Sub-1.0 and fractional resting quantities survive snapshot ingest now that
	# qty is sanitized via _parse_qty (float) instead of int-truncated (0.65 -> 0,
	# 7.25 -> 7). Mirrors the yes_dollars_fp schema the live snapshot handler reads.
	ms = MarketState()
	msg = {"msg": {"market_ticker": "T", "yes_dollars_fp": [["0.6400", "0.65"], ["0.2400", "7.25"]],
	               "no_dollars_fp": []}}
	_handle_orderbook_snapshot(ms, msg)
	yes = ms.get_orderbook("T").yes_levels
	assert (0.64, 0.65) in yes        # previously erased by int-truncation
	assert (0.24, 7.25) in yes
