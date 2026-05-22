"""Tests for the engine notify module (post-G1 convergence).

The single-webhook env-var facade (``discord_notify`` reading
``DISCORD_*WEBHOOK*``) was RETIRED in sub-project E Phase G1 (spec §6
Path B): the engine notify path delegates to the unified
``edge_catcher.notifications`` layer with the mode's channel resolved
once at boot. This module keeps the historical guard intent — the
engine notify path stays a sync, non-blocking, never-perturbs-the-caller
shape — restated against the converged backend.

The exhaustive Path-B contract (delegation to the unified ``send()``,
no ``os.environ[...WEBHOOK...]`` read remaining, never-raises) lives in
``test_notification_mode_label.py``.
"""

import asyncio

import edge_catcher.engine.notifications as notif
from edge_catcher.notifications.envelope import DeliveryResult, Notification


class _SpyChannel:
	def __init__(self, name: str = "spy") -> None:
		self.name = name
		self.sent: list[Notification] = []

	def send(self, notification: Notification) -> DeliveryResult:
		self.sent.append(notification)
		return DeliveryResult(channel_name=self.name, success=True, latency_ms=0.1)


class TestFacadeRetired:
	def test_env_var_facade_symbol_is_gone(self):
		"""``discord_notify`` (the retired env-var facade) must not exist —
		no dead second path a future contributor could wire alerts through.
		"""
		assert not hasattr(notif, "discord_notify")

	def test_no_op_without_a_configured_channel(self, monkeypatch):
		"""With nothing resolved at boot (the paper analog of "no webhook
		env var set"), ``notify`` is a silent no-op.
		"""
		monkeypatch.setattr(notif, "_channels", [], raising=False)
		monkeypatch.setattr(notif, "_pending_tasks", set(), raising=False)

		async def _run():
			notif.notify("test message")  # must not raise
			await asyncio.sleep(0.05)

		asyncio.run(_run())


class TestNotifyDelegatesToUnifiedLayer:
	def test_notify_routes_to_boot_channel(self, monkeypatch):
		"""``notify`` delivers the rendered line to the boot-resolved unified
		channel (the converged backend — not an env-var webhook).
		"""
		monkeypatch.setattr(notif, "_channels", [], raising=False)
		monkeypatch.setattr(notif, "_pending_tasks", set(), raising=False)
		spy = _SpyChannel()
		notif.configure_notify([spy])

		async def _run():
			notif.notify("test message")
			await asyncio.sleep(0.05)

		asyncio.run(_run())

		assert len(spy.sent) == 1
		assert spy.sent[0].body == "test message"


class TestNotifySync:
	def test_notify_schedules_task(self, monkeypatch):
		"""notify() should schedule delivery without blocking."""
		monkeypatch.setattr(notif, "_channels", [], raising=False)
		monkeypatch.setattr(notif, "_pending_tasks", set(), raising=False)
		notif.configure_notify([_SpyChannel()])

		async def _run():
			notif.notify("test")
			await asyncio.sleep(0.1)

		asyncio.run(_run())  # should not raise
