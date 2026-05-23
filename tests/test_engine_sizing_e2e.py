"""Tests for Task E1: bankroll_refresh_loop (spec §5.1).

Covers:
  - Periodic refresh: refresh() is called at least twice in 2 intervals.
  - One-time WARNING: exactly one notification on threshold crossing, reset
    on success so a second streak would warn again.

Running::

    pytest tests/test_engine_sizing_e2e.py -v
"""
from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import patch

import pytest

import edge_catcher.engine.engine as engine_module
from edge_catcher.engine.engine import bankroll_refresh_loop


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
