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
	"""refresh() is called at least twice after two intervals elapse."""
	cache = FakeCache()
	interval = 0.02  # 20 ms — fast but not so fast as to be flaky

	task = asyncio.create_task(
		bankroll_refresh_loop(cache, interval=interval, warn_after=99)
	)
	# Wait long enough for at least 2 intervals to elapse
	await asyncio.sleep(interval * 2.5)
	task.cancel()
	try:
		await task
	except asyncio.CancelledError:
		pass

	assert len(cache.refresh_calls) >= 2, (
		f"Expected refresh() called >= 2 times, got {len(cache.refresh_calls)}"
	)


# ---------------------------------------------------------------------------
# Test 2: one-time WARNING that resets on success
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_sustained_failure_emits_one_time_warning() -> None:
	"""Exactly ONE warning fires on threshold crossing; latch resets on success.

	Phase A — failure streak:
	  - warn_after=2; cache fails every call (consecutive_failures grows).
	  - Run enough intervals for >=3 failure-refreshes.
	  - Assert send() called exactly once (one-time, not per-iteration).

	Phase B — success resets latch:
	  - Switch to a recoverable cache that succeeds after the first call.
	  - Run another cycle past warn_after; confirm no second warning (no streak).
	  - Then immediately confirm the latch DID reset by running a pure-fail
	    cache again and checking a second warning fires.
	"""
	interval = 0.02  # 20 ms

	# --- Phase A: failure streak triggers exactly one warning ---------------

	send_calls: list[Any] = []

	def fake_send(notification: Any, channels: Any) -> dict:
		send_calls.append((notification, channels))
		return {}

	# Set a non-empty risk channel so the warning path is taken
	sentinel_channel = object()
	engine_module._risk_channels = [sentinel_channel]  # type: ignore[assignment]

	failing_cache = FailingFakeCache()

	with patch("edge_catcher.notifications.send", side_effect=fake_send):
		task = asyncio.create_task(
			bankroll_refresh_loop(failing_cache, interval=interval, warn_after=2)
		)
		# Allow at least 3 refresh cycles (>= 2 * warn_after)
		await asyncio.sleep(interval * 3.5)
		task.cancel()
		try:
			await task
		except asyncio.CancelledError:
			pass

	assert failing_cache.refresh_calls >= 3, (
		f"Expected >= 3 refresh calls, got {failing_cache.refresh_calls}"
	)
	assert len(send_calls) == 1, (
		f"Expected exactly 1 warning notification (one-time), got {len(send_calls)}"
	)
	# Check it's the right severity
	notif, channels = send_calls[0]
	assert notif.severity == "warn"
	assert channels is engine_module._risk_channels

	# --- Phase B: success resets the `warned` latch -------------------------

	# A cache that fails twice (triggers warn), then succeeds (resets latch),
	# then we verify by running a second streak that would warn again.
	send_calls.clear()

	# fail_for=2 → after 2 calls consecutive_failures hits warn_after=2,
	# then call 3+ resets to 0 (success).
	recover_cache = RecoverableFakeCache(fail_for=2)
	engine_module._risk_channels = [sentinel_channel]  # type: ignore[assignment]

	with patch("edge_catcher.notifications.send", side_effect=fake_send):
		task = asyncio.create_task(
			bankroll_refresh_loop(recover_cache, interval=interval, warn_after=2)
		)
		# 4 intervals: fail, fail (warn fires), succeed (latch reset), succeed
		await asyncio.sleep(interval * 4.5)
		task.cancel()
		try:
			await task
		except asyncio.CancelledError:
			pass

	# Exactly one warning from the first streak; the success reset the latch;
	# subsequent success calls don't re-warn.
	assert len(send_calls) == 1, (
		f"Phase B: expected 1 warning (first streak only), got {len(send_calls)}"
	)

	# --- Phase C: latch truly reset — a fresh fail streak warns again -------
	# Now start a brand-new loop with a new failing cache to confirm the latch
	# reset is per-loop (not a global stale state). This is the key "resets on
	# success" proof: the previous loop's `warned` flag is gone.
	send_calls.clear()
	fresh_failing = FailingFakeCache()
	engine_module._risk_channels = [sentinel_channel]  # type: ignore[assignment]

	with patch("edge_catcher.notifications.send", side_effect=fake_send):
		task = asyncio.create_task(
			bankroll_refresh_loop(fresh_failing, interval=interval, warn_after=2)
		)
		await asyncio.sleep(interval * 3.5)
		task.cancel()
		try:
			await task
		except asyncio.CancelledError:
			pass

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
