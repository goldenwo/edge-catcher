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
from typing import Any
from unittest.mock import patch

import pytest

import edge_catcher.engine.engine as engine_module
from edge_catcher.engine.engine import bankroll_refresh_loop, _ws_loop
from edge_catcher.engine.market_state import MarketState
from edge_catcher.engine.strategy_base import Signal, Strategy, TickContext
from edge_catcher.engine.trade_store import InMemoryTradeStore


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
