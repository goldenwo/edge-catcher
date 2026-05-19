"""Regression guard for the engine notify path's backend (post-G1).

History: sub-project G relocated the paper-trader Discord webhook helper
``monitors.notifications`` → ``engine.notifications``; this module locked
the wire-shape byte-identical across that move.

Sub-project E Phase G1 (spec §6 Path B) then RETIRED the single-webhook
env-var facade entirely: the engine notify path delegates to the unified
``edge_catcher.notifications`` layer with the mode's channel resolved once
at boot. The "notifications-routing risk" mitigation is now: the engine
notify path has exactly ONE delivery backend (the unified ``send()``) and
no ``DISCORD_*WEBHOOK*`` env-var second path. That invariant is asserted
here; the converged-delivery contract detail lives in
``test_notification_mode_label.py``.
"""

from __future__ import annotations

import asyncio

import pytest

import edge_catcher.engine.notifications as notif
from edge_catcher.notifications.envelope import DeliveryResult, Notification


class _SpyChannel:
	def __init__(self, name: str = "spy") -> None:
		self.name = name
		self.sent: list[Notification] = []

	def send(self, notification: Notification) -> DeliveryResult:
		self.sent.append(notification)
		return DeliveryResult(channel_name=self.name, success=True, latency_ms=0.1)


@pytest.fixture(autouse=True)
def _reset_notify_state(monkeypatch):
	"""Clean module state between tests regardless of order."""
	monkeypatch.setattr(notif, "_channels", [], raising=False)
	monkeypatch.setattr(notif, "_pending_tasks", set(), raising=False)
	yield


def test_env_var_facade_is_retired():
	"""No ``DISCORD_*WEBHOOK*`` env-var path remains — the engine notify
	path has a single delivery backend (the unified layer).

	Targets executable code (comments/docstrings stripped): the module
	docstring legitimately *describes* the retired facade.
	"""
	import inspect
	import io
	import tokenize

	src = inspect.getsource(notif)
	code = " ".join(
		tok.string
		for tok in tokenize.generate_tokens(io.StringIO(src).readline)
		if tok.type not in (tokenize.COMMENT, tokenize.STRING)
	)
	assert "os.environ" not in code
	assert "WEBHOOK" not in code.upper()
	assert not hasattr(notif, "discord_notify")


def test_engine_notify_delivers_via_unified_layer():
	"""The rendered notify line reaches the boot-resolved unified channel
	verbatim (the converged, byte-stable delivery backend).
	"""
	spy = _SpyChannel()
	notif.configure_notify([spy])

	async def _run():
		notif.notify("hello world")
		await asyncio.sleep(0.05)

	asyncio.run(_run())

	assert len(spy.sent) == 1
	assert spy.sent[0].body == "hello world"


def test_silent_no_op_without_a_configured_channel():
	"""No channel resolved at boot (the paper analog of "no webhook env var
	set") -> notify short-circuits, firing no delivery.
	"""
	spy = _SpyChannel()
	# Intentionally do NOT configure_notify — _channels stays [].

	async def _run():
		notif.notify("hello world")
		await asyncio.sleep(0.05)

	asyncio.run(_run())

	assert spy.sent == []
