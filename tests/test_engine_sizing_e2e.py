"""Tests for the live sizing-wire engine plumbing (Tasks E1 + G1).

Covers:
  - E1 — ``bankroll_refresh_loop`` (spec §5.1):
      * Periodic refresh: refresh() is called at least twice in 2 intervals.
      * One-time WARNING: exactly one notification on threshold crossing, reset
        on success so a second streak would warn again.
  - G1 — the no-op-gate REGRESSION GUARD: an entry-producing tick driven
    through the REAL ``_ws_loop -> dispatch_message`` path reaches
    ``gate.gate_entry`` (the gap that left every live entry ungated, sized
    ``0``, and rejected by ``LiveExecutor`` — zero real orders placed).

Running::

    pytest tests/test_engine_sizing_e2e.py -v
"""
from __future__ import annotations

import asyncio
import contextlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

import edge_catcher.engine.engine as engine_module
from edge_catcher.engine.engine import bankroll_refresh_loop, _ws_loop
from edge_catcher.engine.market_state import MarketState
from edge_catcher.engine.strategy_base import Signal, Strategy, TickContext
from edge_catcher.engine.trade_store import InMemoryTradeStore

# F1 fatal-supervisor test reuses E2's fully-coherent LIVE cfg builder so the
# REAL run_engine boots through the §2 coherence gate to the live task block
# (where the bankroll-refresh task + F1 done-callback are wired). The same
# cross-test idiom test_live_composition_root.py / test_live_daemon_shutdown.py
# use.
from tests.test_live_engine_mode_invariant import make_live_cfg, _write_cfg


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class FakeCache:
	"""Minimal BankrollCache stand-in for testing bankroll_refresh_loop."""

	def __init__(self) -> None:
		self.refresh_calls: list[int] = []   # call index tracker
		self._consecutive_failures: int = 0  # mirrored field (loop reads this)
		self._call_count: int = 0

	async def refresh(self) -> None:
		self._call_count += 1
		self.refresh_calls.append(self._call_count)
		# Default: always succeeds (resets failures each call)
		self._consecutive_failures = 0


class FailingFakeCache:
	"""FakeCache whose refresh() always 'fails' by incrementing the failure counter."""

	def __init__(self) -> None:
		self.refresh_calls: int = 0
		self._consecutive_failures: int = 0

	async def refresh(self) -> None:
		self.refresh_calls += 1
		self._consecutive_failures += 1


class RecoverableFakeCache:
	"""FakeCache that fails for N calls then succeeds, for latch-reset testing."""

	def __init__(self, fail_for: int) -> None:
		self._fail_for = fail_for
		self.refresh_calls: int = 0
		self._consecutive_failures: int = 0

	async def refresh(self) -> None:
		self.refresh_calls += 1
		if self.refresh_calls <= self._fail_for:
			self._consecutive_failures += 1
		else:
			# Success — mirrors BankrollCache.refresh() real behaviour
			self._consecutive_failures = 0


# ---------------------------------------------------------------------------
# Fixture: isolate _risk_channels (restore after each test)
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def restore_risk_channels() -> Any:
	"""Restore engine._risk_channels to its original value after each test."""
	original = engine_module._risk_channels
	yield
	engine_module._risk_channels = original


@pytest.fixture(autouse=True)
def reset_refresh_fatal() -> Any:
	"""Reset engine._REFRESH_FATAL to None around every test (setup AND
	teardown). It is a module global (one engine per process); a leak from the
	F1 fatal-supervisor test would make a later test's clean shutdown re-raise
	a stale exception. Mirrors the _OPERATOR_KILL-style per-test reset."""
	engine_module._REFRESH_FATAL = None
	yield
	engine_module._REFRESH_FATAL = None


# ---------------------------------------------------------------------------
# Test 1: refresh-at-interval
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bankroll_refresh_loop_calls_refresh_at_interval() -> None:
	"""refresh() is called repeatedly; verified via Event-gate (not wall-clock).

	Rather than sleeping a fixed multiple of the interval and hoping N cycles
	ran, we gate on an asyncio.Event that fires once the target call-count is
	reached, with a generous safety timeout.  This is deterministic regardless
	of machine load.
	"""
	TARGET = 3
	done = asyncio.Event()

	class _EventCache:
		def __init__(self) -> None:
			self.calls: int = 0
			self._consecutive_failures: int = 0

		async def refresh(self) -> None:
			self.calls += 1
			self._consecutive_failures = 0
			if self.calls >= TARGET:
				done.set()

	cache = _EventCache()
	task = asyncio.create_task(
		bankroll_refresh_loop(cache, interval=0.001, warn_after=99)
	)
	try:
		# Waits until TARGET calls happen — safety timeout only trips on a genuine hang.
		await asyncio.wait_for(done.wait(), timeout=5.0)
	finally:
		task.cancel()
		with contextlib.suppress(asyncio.CancelledError):
			await task

	assert cache.calls >= TARGET, (
		f"Expected refresh() called >= {TARGET} times, got {cache.calls}"
	)


# ---------------------------------------------------------------------------
# Test 2: one-time WARNING that resets on success
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_sustained_failure_emits_one_time_warning() -> None:
	"""Exactly ONE warning fires on threshold crossing; latch resets on success.

	All three phases use Event-gated progress (not wall-clock sleep) so the
	test is deterministic regardless of machine load.

	Phase A — failure streak:
	  - warn_after=2; cache fails every call (consecutive_failures grows).
	  - Gate: Event set by the fake_send spy the moment the first warning fires.
	  - Assert send() called exactly once (one-time, not per-iteration) and
	    refresh() was called >= 3 times before cancellation.

	Phase B — success resets latch:
	  - A cache that fails twice then succeeds; run until >= 4 calls.
	  - Gate: Event set when call-count reaches 4 (2 fails + 2 successes).
	  - Assert exactly 1 warning from the failing streak; no second warning after
	    the success reset.

	Phase C — latch truly reset — a fresh fail streak warns again:
	  - Brand-new failing cache + loop; gate on second warning Event.
	  - Assert exactly 1 warning for the new streak (proves per-loop `warned`).
	"""
	send_calls: list[Any] = []
	sentinel_channel = object()

	# --- Phase A: failure streak triggers exactly one warning ---------------

	warned_event_a = asyncio.Event()

	def fake_send_a(notification: Any, channels: Any) -> dict:
		send_calls.append((notification, channels))
		warned_event_a.set()  # unblock the wait the moment the warning fires
		return {}

	engine_module._risk_channels = [sentinel_channel]  # type: ignore[assignment]

	class _FailingCache:
		def __init__(self) -> None:
			self.calls: int = 0
			self._consecutive_failures: int = 0

		async def refresh(self) -> None:
			self.calls += 1
			self._consecutive_failures += 1

	failing_cache = _FailingCache()

	with patch("edge_catcher.notifications.send", side_effect=fake_send_a):
		task = asyncio.create_task(
			bankroll_refresh_loop(failing_cache, interval=0.001, warn_after=2)
		)
		try:
			# Gate: wait until the warning fires (not a fixed sleep).
			await asyncio.wait_for(warned_event_a.wait(), timeout=5.0)
			# Let the loop run a few more cycles to confirm no second warning.
			await asyncio.sleep(0.01)
		finally:
			task.cancel()
			with contextlib.suppress(asyncio.CancelledError):
				await task

	assert failing_cache.calls >= 3, (
		f"Expected >= 3 refresh calls, got {failing_cache.calls}"
	)
	assert len(send_calls) == 1, (
		f"Expected exactly 1 warning notification (one-time), got {len(send_calls)}"
	)
	# Check it's the right severity and the correct channel was passed
	notif, channels = send_calls[0]
	assert notif.severity == "warn"
	assert channels is engine_module._risk_channels

	# --- Phase B: success resets the `warned` latch -------------------------

	send_calls.clear()
	engine_module._risk_channels = [sentinel_channel]  # type: ignore[assignment]

	PHASE_B_TARGET = 4  # 2 fails (warn fires) + 2 successes (latch reset)
	done_b = asyncio.Event()

	def fake_send_b(notification: Any, channels: Any) -> dict:
		send_calls.append((notification, channels))
		return {}

	class _RecoverableCache:
		"""Fails for the first `fail_for` calls, then succeeds."""

		def __init__(self, fail_for: int) -> None:
			self._fail_for = fail_for
			self.calls: int = 0
			self._consecutive_failures: int = 0

		async def refresh(self) -> None:
			self.calls += 1
			if self.calls <= self._fail_for:
				self._consecutive_failures += 1
			else:
				self._consecutive_failures = 0
			if self.calls >= PHASE_B_TARGET:
				done_b.set()

	recover_cache = _RecoverableCache(fail_for=2)

	with patch("edge_catcher.notifications.send", side_effect=fake_send_b):
		task = asyncio.create_task(
			bankroll_refresh_loop(recover_cache, interval=0.001, warn_after=2)
		)
		try:
			await asyncio.wait_for(done_b.wait(), timeout=5.0)
		finally:
			task.cancel()
			with contextlib.suppress(asyncio.CancelledError):
				await task

	# Exactly one warning from the first streak; the success reset the latch;
	# subsequent success calls don't re-warn.
	assert len(send_calls) == 1, (
		f"Phase B: expected 1 warning (first streak only), got {len(send_calls)}"
	)

	# --- Phase C: latch truly reset — a fresh fail streak warns again -------
	# Brand-new loop instance → `warned` starts False; confirms per-loop isolation.
	send_calls.clear()
	engine_module._risk_channels = [sentinel_channel]  # type: ignore[assignment]

	warned_event_c = asyncio.Event()

	def fake_send_c(notification: Any, channels: Any) -> dict:
		send_calls.append((notification, channels))
		warned_event_c.set()
		return {}

	class _FreshFailingCache:
		def __init__(self) -> None:
			self.calls: int = 0
			self._consecutive_failures: int = 0

		async def refresh(self) -> None:
			self.calls += 1
			self._consecutive_failures += 1

	fresh_failing = _FreshFailingCache()

	with patch("edge_catcher.notifications.send", side_effect=fake_send_c):
		task = asyncio.create_task(
			bankroll_refresh_loop(fresh_failing, interval=0.001, warn_after=2)
		)
		try:
			await asyncio.wait_for(warned_event_c.wait(), timeout=5.0)
		finally:
			task.cancel()
			with contextlib.suppress(asyncio.CancelledError):
				await task

	assert len(send_calls) == 1, (
		f"Phase C: expected 1 warning for new streak, got {len(send_calls)}"
	)


@pytest.mark.asyncio
async def test_floor_config_suppresses_pre_kill_warning() -> None:
	"""bug_002: at the failures_until_kill==1 floor (warn_after=0) NO warning fires.

	There is no refresh before the kill (it trips on the first failure), and a
	coincident "entries gated STALE_BANKROLL until it recovers" warning would
	misdescribe the manual-clear-only KILL_AUTO_PANIC as a transient gate. The
	loop's ``warn_after >= 1`` guard suppresses it. Driven via the same
	Event-gated pattern as the warning tests (no wall-clock sleeps).
	"""
	send_calls: list[Any] = []
	sentinel_channel = object()
	engine_module._risk_channels = [sentinel_channel]  # type: ignore[assignment]
	done = asyncio.Event()

	class _FailingCache:
		def __init__(self) -> None:
			self.calls: int = 0
			self._consecutive_failures: int = 0

		async def refresh(self) -> None:
			self.calls += 1
			self._consecutive_failures += 1
			if self.calls >= 4:
				done.set()

	cache = _FailingCache()

	def fake_send(notification: Any, channels: Any) -> dict:
		send_calls.append((notification, channels))
		return {}

	with patch("edge_catcher.notifications.send", side_effect=fake_send):
		# warn_after=0 mirrors the boot-site value when
		# bankroll_failures_until_kill == 1.
		task = asyncio.create_task(
			bankroll_refresh_loop(cache, interval=0.001, warn_after=0)
		)
		try:
			await asyncio.wait_for(done.wait(), timeout=5.0)
		finally:
			task.cancel()
			with contextlib.suppress(asyncio.CancelledError):
				await task

	assert cache.calls >= 4, f"expected >= 4 refresh calls, got {cache.calls}"
	assert len(send_calls) == 0, (
		f"floor config (warn_after=0) must emit no pre-kill warning, got {len(send_calls)}"
	)


# ===========================================================================
# G1 — no-op-gate REGRESSION GUARD (THE point of the sizing-wire PR)
#
# The bug: _ws_loop (the REAL live WS feed) had no `risk` param and its
# dispatch_message call omitted risk/risk_ctx_provider — so on the live feed
# the gate was NEVER consulted, every entry fell to the paper size=0 path, and
# LiveExecutor rejected it (zero real orders, a silent no-op). This test drives
# ONE entry-producing ticker message through the REAL _ws_loop -> dispatch_message
# -> _handle_ticker_msg -> process_tick -> _handle_signal chain (NOT a direct
# process_tick call) and asserts gate_entry WAS consulted exactly once.
# ===========================================================================

_G1_SERIES = "KXSTUB15M"
_G1_TICKER = "KXSTUB15M-26MAY22H12-T100"
_G1_NOW = datetime(2026, 5, 22, 12, 0, 0, tzinfo=timezone.utc)


class _AlwaysEnterStrategy(Strategy):
	"""Inert framework stub that emits exactly one ENTER signal per tick.

	No strategy logic leaks (synthetic series, unconditional enter) — its only
	job is to make the dispatch chain reach the entry gate so the regression
	guard can observe ``gate_entry`` being consulted via the REAL ``_ws_loop``.
	"""

	name = "g1-enter-stub"
	supported_series = [_G1_SERIES]
	default_params: dict = {}

	def on_tick(self, ctx: TickContext) -> list[Signal]:
		return [
			Signal(
				action="enter",
				ticker=ctx.ticker,
				side="yes",
				series=ctx.series,
				strategy=self.name,
				reason="g1-regression",
				entry_price_cents=ctx.yes_ask,
				stop_loss_distance_cents=10,
			)
		]


class _OneMessageWS:
	"""Minimal async stand-in for the value ``websockets.connect(...)`` yields.

	An async context manager whose ``async for`` yields exactly one raw WS
	ticker frame then stops — enough to drive ONE pass of ``_ws_loop``'s
	message loop without a real socket. ``send`` (the subscribe write) is a
	no-op. This is the market-data WS that ``_ws_loop`` consumes (distinct from
	``tests/fixtures/mock_kalshi_ws.py``, which models B's account-scope WS).
	"""

	def __init__(self, frames: list[str]) -> None:
		self._frames = frames

	async def __aenter__(self) -> "_OneMessageWS":
		return self

	async def __aexit__(self, *_exc: object) -> bool:
		return False

	async def send(self, _msg: str) -> None:
		return None

	async def __aiter__(self):
		for frame in self._frames:
			yield frame


class _FakeProvider:
	"""Stand-in RiskContextProvider — builds a canned RiskContext per signal.

	Avoids live-DB plumbing in the regression guard: the point under test is
	that ``_ws_loop`` THREADS a provider + gate to ``_handle_signal``, not the
	provider's own DB reads (covered by the provider's unit tests).
	"""

	def __init__(self) -> None:
		self.build_calls = 0

	def build(self, _signal: object, now: datetime) -> Any:
		from edge_catcher.engine.risk import RiskContext

		self.build_calls += 1
		return RiskContext(
			now_utc=now,
			market_state=MarketState(),
			open_positions=[],
			open_count=0,
			daily_pnl_cents=0,
			operator_kill_active=False,
		)


@pytest.mark.asyncio
async def test_live_ws_path_reaches_gate_entry(monkeypatch: pytest.MonkeyPatch) -> None:
	"""REGRESSION GUARD for the no-op-gate gap: an entry-producing tick driven
	through the REAL ``_ws_loop`` -> ``dispatch_message`` path consults the gate.

	Pre-G1 this asserted-once call NEVER happened on the live feed (``_ws_loop``
	dropped ``risk``), so every live entry was ungated/size-0/rejected. Spy the
	REAL ``gate.gate_entry``; feed one ticker frame via ``_ws_loop`` with
	``risk=gate`` + a provider; assert ``gate_entry`` was consulted exactly once.
	"""
	# A MarketState that already knows the ticker (recovery seeds it live;
	# _ws_loop subscribes to market_state.all_tickers()). One price update both
	# registers the ticker and gives _handle_ticker_msg a populated history.
	market_state = MarketState(limit=100)
	market_state.update_price(_G1_TICKER, 50)

	store = InMemoryTradeStore()
	strat = _AlwaysEnterStrategy()
	strat_by_series = {_G1_SERIES: [strat]}

	# Spy the REAL Gate.gate_entry. It returns a Reject so NO order is placed
	# (keeps the guard free of executor plumbing) — being CALLED is the whole
	# assertion: the gate was reached via the live _ws_loop, not bypassed.
	from edge_catcher.engine.risk import Reject

	gate_calls: list[Any] = []

	class _SpyGate:
		def gate_entry(self, signal: Any, rctx: Any) -> Any:
			gate_calls.append((signal, rctx))
			return Reject("MAX_OPEN", detail="g1-regression: reject so no place")

	gate = _SpyGate()
	provider = _FakeProvider()

	# A ticker frame for our seeded ticker — _handle_ticker_msg reaches
	# process_tick on a ticker msg with a valid yes_ask (orderbook defaults
	# to empty, unlike the trade path which requires a populated book).
	import json

	frame = json.dumps({
		"type": "ticker",
		"msg": {"market_ticker": _G1_TICKER, "yes_ask": 0.50, "yes_bid": 0.49},
	})

	# websockets.connect(...) -> our one-message fake socket (no real network).
	monkeypatch.setattr(
		engine_module.websockets, "connect", lambda *_a, **_kw: _OneMessageWS([frame])
	)
	# make_auth_headers reads creds from env — stub to avoid that in the guard.
	monkeypatch.setattr(engine_module, "make_auth_headers", lambda: {})

	ws_ref: list[Any] = [None]
	await _ws_loop(
		{"ws": {}}, market_state, store, [strat],
		strat_by_series, {strat.name: {}}, [_G1_SERIES],
		None, ws_ref, set(),
		None,  # executor — never reached (gate Rejects before _handle_enter)
		risk=gate, risk_ctx_provider=provider,
	)

	assert len(gate_calls) == 1, (
		"gate_entry MUST be consulted exactly once via the REAL _ws_loop -> "
		"dispatch_message path (the no-op-gate gap: pre-G1 _ws_loop dropped "
		f"risk, so the gate was never reached on the live feed); got {len(gate_calls)}"
	)
	assert provider.build_calls == 1, (
		"the provider must be threaded through _ws_loop and used to build the "
		f"RiskContext for the gated signal; got {provider.build_calls} build calls"
	)


# ===========================================================================
# F1 — drain-then-crash fatal supervisor (THE most safety-critical task)
#
# G1 starts bankroll_refresh_loop. If refresh() raises KillSwitchTripFailed
# (the auto-panic trip's kill-WRITE itself failed), asyncio ISOLATES that task
# exception — without F1 it is silently lost and the engine keeps trading
# ungated against an untrusted balance (C-spec L214 ghost-reject hazard). F1's
# done-callback stashes the exception + cancels the root task; run_engine's
# finally drains the in-flight place→persist sections (money-safe) and THEN
# re-raises fail-loud with a FATAL operator alert — it must NOT masquerade as a
# clean SIGTERM drain (which the cli entrypoint swallows as exit 0).
# ===========================================================================


class _FailingBankroll:
	"""BankrollCache stand-in whose refresh() raises KillSwitchTripFailed —
	models the auto-panic trip's kill-WRITE failing (the ghost-reject hazard).

	bankroll_refresh_loop reads ``_consecutive_failures`` AFTER awaiting
	refresh(); refresh() raises first, so that read never happens — but the
	attribute exists so the loop body is structurally satisfiable."""

	def __init__(self) -> None:
		self._consecutive_failures = 0
		self.refresh_calls = 0

	async def refresh(self) -> None:
		from edge_catcher.engine.risk import KillSwitchTripFailed

		self.refresh_calls += 1
		# The kill-WRITE failed (not a recoverable network blip): the trip
		# itself could not be persisted, so the loop's exception MUST propagate.
		raise KillSwitchTripFailed(
			"kill_switch INSERT failed for reason='KILL_AUTO_PANIC': simulated DB failure"
		)


class _FakeGateWithBankroll:
	"""Minimal Gate stand-in carrying a ._bankroll the live block reads."""

	def __init__(self, bankroll: Any) -> None:
		self._bankroll = bankroll


@pytest.mark.asyncio
async def test_refresh_killwrite_failure_crashes_fail_loud(
	tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
	"""Failure mode prevented (FUNDS-AT-RISK, the C-spec L214 ghost-reject
	hazard): a bankroll-refresh KillSwitchTripFailed is isolated by asyncio and
	silently lost, so the engine keeps trading ungated against an untrusted
	balance — OR it crashes but masquerades as a clean SIGTERM drain the cli
	swallows as exit 0 (systemd would not restart / alert).

	Drives the REAL ``run_engine`` (LIVE, coherent cfg) through composition to
	the live task block, where the REAL ``bankroll_refresh_loop`` + the REAL F1
	done-callback are wired. The refresh raises ``KillSwitchTripFailed`` on its
	first tick; F1's done-callback stashes it + cancels the root task; the
	finally drains (money-safe) and re-raises fail-loud.

	Asserts:
	  * ``run_engine`` RAISES ``KillSwitchTripFailed`` (does NOT return / drain
	    cleanly).
	  * the in-flight drain still completed — ``store.close()`` was called
	    exactly once (close-once contract; the drain ran BEFORE the re-raise).
	  * a FATAL (severity="error") risk-channel alert WAS emitted via the
	    notifications ``send`` path AND the clean ``notify`` "drain complete"
	    alert was NOT emitted (the fatal raise skips it).
	"""
	from edge_catcher.engine.risk import KillSwitchTripFailed

	# A fully-coherent LIVE cfg with a SHORT bankroll_ttl so the refresh loop
	# fires fast (interval = ttl/2). Phase-1 caps + creds + channels are all set
	# up by make_live_cfg; we only shrink the TTL for a deterministic-but-quick
	# first refresh tick.
	cfg = make_live_cfg(tmp_path, monkeypatch)
	cfg["risk"]["bankroll_ttl_seconds"] = 0.02   # interval = 0.01s
	cfg["risk"]["bankroll_failures_until_kill"] = 1
	# A coherent sizing: block + the enabled stub let step-2 strategy discovery
	# (validate_sizing_config + a non-empty enabled set) pass so the boot reaches
	# the live task block (mirrors test_live_daemon_shutdown._paper_cfg_path).
	cfg["sizing"] = {
		"risk_per_trade_cents": 500, "max_slippage_cents": 5, "min_fill": 1,
	}
	cfg["strategies"] = {
		_AlwaysEnterStrategy.name: {"enabled": True, "series": [_G1_SERIES]},
	}
	cfg_path = _write_cfg(cfg, tmp_path)

	failing_bankroll = _FailingBankroll()

	# Spy the store-close drain effect + use the paper TradeStore so the REAL
	# finally drain (store.close()) runs and is observable. We stub _compose_live
	# (the heavy live composition: SQLiteTradeStore + KalshiOrderClient + a real
	# Gate over the live DB) to return a paper store + an inert executor + a
	# _LiveRuntime whose gate._bankroll.refresh() raises — the cleanest seam that
	# still exercises the REAL bankroll_refresh_loop + REAL F1 done-callback +
	# REAL finally guard end-to-end.
	store_close_calls = {"n": 0}
	_orig_ts_close = engine_module.TradeStore.close

	def _spy_ts_close(self):  # type: ignore[no-untyped-def]
		store_close_calls["n"] += 1
		return _orig_ts_close(self)

	monkeypatch.setattr(engine_module.TradeStore, "close", _spy_ts_close)

	class _InertExecutor:
		"""Executor stub — never reached (refresh crashes before any tick)."""

		async def place(self, *_a: Any, **_kw: Any) -> Any:
			raise AssertionError("executor.place must not be reached in this test")

	async def _fake_compose_live(config, config_path, db_path, market_state, injected_executor):  # type: ignore[no-untyped-def]
		store = engine_module.TradeStore(db_path)
		runtime = engine_module._LiveRuntime(
			gate=_FakeGateWithBankroll(failing_bankroll),
			kalshi_client=None,
			db_conn=None,  # never used: _ws_loop is blocked, provider.build never runs
		)
		return store, _InertExecutor(), runtime

	monkeypatch.setattr(engine_module, "_compose_live", _fake_compose_live)

	# run_recovery precedes the task block — harmless no-op (no real REST).
	async def _noop_recovery(*_a: Any, **_kw: Any) -> None:
		return None

	monkeypatch.setattr(engine_module, "run_recovery", _noop_recovery)

	# §6 step-4/5 live helpers are lazy-imported from edge_catcher.live
	# .reconciliation at call time — no-op-stub them THERE so no real Kalshi /
	# DB reconciliation runs (the F1 path under test is the refresh supervisor).
	import edge_catcher.live.reconciliation as _reconmod

	async def _noop_startup_reconcile(*_a: Any, **_kw: Any) -> Any:
		# Faithful to the real ``-> StartupReconcileReport`` contract: the live
		# boot now consumes the return for the reconcile-alert Discord fan-out.
		# A clean report yields no notification, leaving the F1 refresh-
		# supervisor path under test unaffected.
		return _reconmod.StartupReconcileReport()

	async def _noop_poll_pending_rows_loop(*_a: Any, **_kw: Any) -> None:
		return None

	monkeypatch.setattr(_reconmod, "startup_reconcile", _noop_startup_reconcile)
	monkeypatch.setattr(_reconmod, "poll_pending_rows_loop", _noop_poll_pending_rows_loop)

	# Step-2 strategy discovery → the inert always-enter stub so the enabled set
	# is non-empty and the boot proceeds to the live task block. It never trades
	# (the refresh crashes the engine before any WS tick — _ws_loop blocks).
	monkeypatch.setattr(
		engine_module, "discover_strategies", lambda: [_AlwaysEnterStrategy()]
	)

	# _ws_loop blocks forever so the root task is parked INSIDE the awaited
	# _ws_loop (all background tasks — incl. the refresh task — already created)
	# when the refresh raises. That is the live steady-state F1 must interrupt.
	# The done-callback cancels the root task; the cancel propagates through this
	# blocked await into run_engine's finally (the money-safe drain) — exactly
	# like a SIGTERM, except _REFRESH_FATAL is set so the finally re-raises.
	async def _blocking_ws_loop(*_a: Any, **_kw: Any) -> None:
		await asyncio.Event().wait()

	monkeypatch.setattr(engine_module, "_ws_loop", _blocking_ws_loop)

	# Spy BOTH alert paths:
	#   * the FATAL alert uses notifications.send (severity="error") — must fire.
	#   * the clean "drain complete" alert uses engine.notify — must NOT fire.
	send_calls: list[Any] = []

	def _spy_send(notification: Any, channels: Any) -> dict:
		send_calls.append(notification)
		return {}

	monkeypatch.setattr("edge_catcher.notifications.send", _spy_send)

	notify_calls: list[str] = []
	monkeypatch.setattr(engine_module, "notify", lambda text: notify_calls.append(text))

	# Act: the REAL run_engine. It must RAISE KillSwitchTripFailed (NOT return).
	with pytest.raises(KillSwitchTripFailed):
		await asyncio.wait_for(
			engine_module.run_engine(config_path=cfg_path), timeout=10.0
		)

	# The refresh task actually fired (the loop awaited refresh once).
	assert failing_bankroll.refresh_calls >= 1, (
		"the REAL bankroll_refresh_loop must have called refresh() (the crash "
		f"trigger); got {failing_bankroll.refresh_calls} calls"
	)

	# The money-safe drain ran BEFORE the fail-loud re-raise: store.close()
	# exactly once (close-once contract — the drain completed, not skipped).
	assert store_close_calls["n"] == 1, (
		"run_engine's finally must drain (store.close() exactly once) BEFORE "
		f"re-raising the fatal — got {store_close_calls['n']} close calls"
	)

	# A FATAL (severity error) risk-channel alert WAS emitted via send().
	fatal_alerts = [
		n for n in send_calls
		if getattr(n, "severity", None) == "error"
		and "FATAL" in getattr(n, "title", "")
	]
	assert len(fatal_alerts) == 1, (
		"exactly one FATAL severity=error risk alert must be emitted via the "
		f"notifications.send path (the operator's ONLY signal — no RiskEvent "
		f"fired because the kill-WRITE failed); got titles={[getattr(n, 'title', None) for n in send_calls]!r}"
	)

	# The clean "SIGTERM drain complete" notify alert was NOT emitted (the fatal
	# raise skips step 7) — a fatal crash must never masquerade as a clean drain.
	assert not any("drain complete" in t for t in notify_calls), (
		"the clean 'SIGTERM drain complete' alert must be SKIPPED on a fatal "
		f"refresh crash (it would masquerade the crash as a clean exit); got {notify_calls!r}"
	)
