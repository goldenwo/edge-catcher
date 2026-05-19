"""Tests for the engine notify path's convergence onto the unified layer.

Sub-project E, Phase G1 (spec §6 Path B). The engine notify path is
converged onto ``edge_catcher.notifications`` (the multi-channel unified
layer: ``load_channels()`` + ``send(Notification, [channels])``, sync,
never-raises/``DeliveryResult``). The single-webhook env-var facade
(``discord_notify`` reading ``DISCORD_*WEBHOOK*``) is RETIRED — no dead
second path a future contributor could wire live alerts through.

Path B contract verified here:
  * the thin engine-facing helper delegates to the unified ``send()`` with
    the **mode's channel resolved once at boot** (``configure_notify``);
  * NO ``os.environ[...WEBHOOK...]`` / ``DISCORD_*WEBHOOK*`` read remains in
    ``engine/notifications.py`` (the env-var facade body is deleted);
  * a converged notify whose unified ``send()`` fails (``DeliveryResult``
    ``success=False``) does NOT raise / does NOT perturb the caller (the
    never-raises contract is preserved — a notification failure cannot
    perturb the trade path, §6/§9).

G2 will extend this module with the mode-label / Class-C guard test; G1
adds only the converge + facade-retired + never-raises tests.
"""
from __future__ import annotations

import ast
import asyncio
import inspect
import io
import tokenize

import edge_catcher.engine.notifications as notif
from edge_catcher.notifications.envelope import DeliveryResult, Notification


def _code_only(src: str) -> str:
	"""Return ``src`` with comments and string/docstring literals removed.

	The facade-retirement assertion targets the *executable code*, not
	explanatory prose: the module docstring legitimately *describes* the
	retired ``DISCORD_*WEBHOOK*`` facade, so a bare substring scan over the
	raw source would false-positive. Tokenizing and dropping COMMENT +
	STRING tokens leaves only what the interpreter runs.
	"""
	out: list[str] = []
	for tok in tokenize.generate_tokens(io.StringIO(src).readline):
		if tok.type in (tokenize.COMMENT, tokenize.STRING):
			continue
		out.append(tok.string)
	return " ".join(out)


class _SpyChannel:
	"""Unified-layer Channel that records the Notification it was sent."""

	def __init__(self, name: str = "engine_spy") -> None:
		self.name = name
		self.sent: list[Notification] = []

	def send(self, notification: Notification) -> DeliveryResult:
		self.sent.append(notification)
		return DeliveryResult(channel_name=self.name, success=True, latency_ms=0.1)


class _FailChannel:
	"""Unified-layer Channel that always returns a failed DeliveryResult."""

	def __init__(self, name: str = "engine_fail") -> None:
		self.name = name

	def send(self, notification: Notification) -> DeliveryResult:
		return DeliveryResult(
			channel_name=self.name,
			success=False,
			error="test fixture: always fails",
			latency_ms=0.1,
		)


class _RaisingChannel:
	"""Adapter that RAISES on send (a programmer-error / adapter bug).

	The unified dispatcher's defensive net must absorb this; the engine
	helper must still never perturb its caller.
	"""

	def __init__(self, name: str = "engine_boom") -> None:
		self.name = name

	def send(self, notification: Notification) -> DeliveryResult:
		raise RuntimeError("adapter blew up")


def _reset(monkeypatch) -> None:
	"""Drop the module-level boot-resolved channel binding + in-flight set
	so each test sees a clean state regardless of order.
	"""
	monkeypatch.setattr(notif, "_channels", [], raising=False)
	monkeypatch.setattr(notif, "_pending_tasks", set(), raising=False)


def test_notify_delegates_to_unified_send_with_boot_channel(monkeypatch):
	"""The engine notify helper, given a boot-resolved channel, routes to
	the unified layer's ``send(Notification(...), [channel])`` — NOT the
	retired env-var path.
	"""
	_reset(monkeypatch)
	spy = _SpyChannel()
	notif.configure_notify([spy])

	async def _run() -> None:
		notif.notify("hello from the engine")
		# notify() schedules onto the running loop; let the task run.
		await asyncio.sleep(0.05)

	asyncio.run(_run())

	assert len(spy.sent) == 1, "the boot-resolved channel must receive exactly one Notification"
	n = spy.sent[0]
	assert isinstance(n, Notification)
	# The existing call sites pass a single rendered string; it must be
	# preserved verbatim somewhere in the envelope the channel receives.
	assert "hello from the engine" in (n.body or "") or "hello from the engine" in (n.title or "")


def test_env_var_facade_is_retired_no_webhook_env_read(monkeypatch):
	"""Source-inspect: NO ``os.environ[...WEBHOOK...]`` / ``DISCORD_*WEBHOOK*``
	read remains anywhere in ``engine/notifications.py`` (the env-var facade
	body — the dead second path — is deleted).
	"""
	src = inspect.getsource(notif)
	code = _code_only(src)  # comments + docstrings stripped — code only

	assert "os.environ" not in code, (
		"engine/notifications.py executable code must not read os.environ — "
		"the env-var facade is retired; delivery is the unified send() "
		"(spec §6 Path B)"
	)
	# No WEBHOOK env-var name token survives in code (prose is allowed to
	# describe the retirement; the code must not reference one).
	assert "WEBHOOK" not in code.upper(), (
		"engine/notifications.py executable code must not reference a "
		"WEBHOOK env var — the dead second path is deleted"
	)

	# Structural: neither `os` nor `httpx` is imported any more (the facade
	# owned both; the unified layer's WebhookChannel owns HTTP now).
	tree = ast.parse(src)
	imported: set[str] = set()
	for node in ast.walk(tree):
		if isinstance(node, ast.Import):
			imported.update(a.name.split(".")[0] for a in node.names)
		elif isinstance(node, ast.ImportFrom) and node.module:
			imported.add(node.module.split(".")[0])
	assert "httpx" not in imported, (
		"engine/notifications.py must not import httpx — the unified "
		"WebhookChannel owns HTTP delivery now"
	)
	assert "os" not in imported, (
		"engine/notifications.py must not import os — no env-var read remains"
	)

	# The retired public symbol must be gone (no dead second entry point).
	assert not hasattr(notif, "discord_notify"), (
		"discord_notify (the env-var facade) must be deleted, not merely unused"
	)


def test_env_var_has_no_effect_behaviorally(monkeypatch):
	"""Behavioral proof of retirement: setting the OLD webhook env var has
	NO effect — notify still delivers via the boot-resolved unified channel,
	and the env var is never read.
	"""
	_reset(monkeypatch)
	monkeypatch.setenv(
		"DISCORD_PAPER_TRADE_LOGS_WEBHOOK_URL",
		"https://discord.com/api/webhooks/SHOULD-NEVER-BE-USED",
	)
	monkeypatch.setenv(
		"DISCORD_LOGS_WEBHOOK_URL",
		"https://discord.com/api/webhooks/ALSO-NEVER",
	)
	spy = _SpyChannel()
	notif.configure_notify([spy])

	async def _run() -> None:
		notif.notify("env var must be ignored")
		await asyncio.sleep(0.05)

	asyncio.run(_run())

	# Delivery went through the unified channel, NOT the env-var webhook.
	assert len(spy.sent) == 1
	assert "env var must be ignored" in spy.sent[0].body


def test_failed_delivery_result_does_not_perturb_caller(monkeypatch):
	"""A converged notify whose unified ``send()`` returns a failure
	``DeliveryResult`` does NOT raise / does NOT perturb the caller — the
	never-raises contract is preserved (a notification failure cannot
	perturb the trade path, §6/§9).
	"""
	_reset(monkeypatch)
	notif.configure_notify([_FailChannel()])

	async def _run() -> None:
		notif.notify("this delivery fails")  # must NOT raise
		await asyncio.sleep(0.05)

	# asyncio.run would surface an unhandled task exception loudly; a clean
	# return is the proof the failure was absorbed.
	asyncio.run(_run())


def test_raising_adapter_does_not_perturb_caller(monkeypatch):
	"""Even an adapter that RAISES (programmer-error) must not propagate
	through the engine helper — the unified dispatcher's defensive net
	plus the helper's scheduling keep the caller / trade path clean.
	"""
	_reset(monkeypatch)
	notif.configure_notify([_RaisingChannel()])

	async def _run() -> None:
		notif.notify("adapter raises")  # must NOT raise
		await asyncio.sleep(0.05)

	asyncio.run(_run())


def test_notify_without_boot_config_is_silent_noop(monkeypatch):
	"""Before ``configure_notify`` runs (or with no channel resolved — the
	paper analog of "no webhook env var set"), ``notify`` is a silent no-op:
	it must not raise and must not require a configured channel.

	This preserves the pre-G facade's "silent no-op without a webhook"
	property so test/dev environments don't fire stray deliveries.
	"""
	_reset(monkeypatch)  # _channels = [] — nothing configured

	async def _run() -> None:
		notif.notify("nobody is listening")  # must NOT raise
		await asyncio.sleep(0.05)

	asyncio.run(_run())


def test_notify_outside_event_loop_is_silent(monkeypatch):
	"""``notify`` from a sync context with NO running loop is a silent no-op
	(unchanged contract — it schedules onto a running loop or returns).
	"""
	_reset(monkeypatch)
	notif.configure_notify([_SpyChannel()])
	# No running event loop here — must return without raising.
	notif.notify("no loop running")
