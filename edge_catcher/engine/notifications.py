"""Engine signal-flow notify path — converged onto the unified layer.

Spec §6 Path B (NORMATIVE). The engine's notify path delegates to the
unified ``edge_catcher.notifications`` layer (multi-channel:
``load_channels()`` + ``send(Notification, [channels])``, sync,
never-raises / ``DeliveryResult``). The single-webhook env-var facade
that previously lived here (a ``DISCORD_*WEBHOOK*`` reader) is **RETIRED**
— there is no dead second path a future contributor could wire live
alerts through.

Boot-resolved channel (§6 / §1 keystone): the mode's channel(s) are
resolved ONCE at boot via :func:`configure_notify` (called from
``run_engine`` after the §2 coherence gate, reusing the SAME
``notifications:`` config the §2.4 coherence check parses). ``notify`` is
NOT re-resolved per call — it builds a :class:`Notification` and hands it
to the unified ``send()`` with the boot-resolved channel list.

``send()`` is sync and never raises (returns per-channel
``DeliveryResult``), so a notification failure CANNOT perturb the trade
path (§6 / §9). ``notify`` itself stays the SAME ``notify(text: str)``
sync, non-blocking, bounded-queue shape every existing engine call site
already uses — only the delivery backend changed (env-var-webhook →
unified ``send``); the paper trade-row-producing path is byte-unchanged
(notify is a side-effect, not trade state).

G2 parameterizes the mode label in the rendered message; G3 wires the
dedicated live-risk channel + ``_handle_risk_event`` body. G1 is the
general notify helper + the facade retirement only.
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Coroutine
from typing import Any

from edge_catcher.notifications import Channel, Notification, send

logger = logging.getLogger(__name__)

_MAX_PENDING: int = 20  # max queued notification tasks
_pending_tasks: set[asyncio.Task] = set()

# Boot-resolved channel binding (§6 Path B). Empty until ``configure_notify``
# is called once at boot (the paper analog of "no webhook env var set" is an
# empty list ⇒ notify is a silent no-op, preserving the pre-G facade's
# silent-without-a-webhook property). NEVER re-resolved per call.
_channels: list[Channel] = []


def configure_notify(channels: list[Channel]) -> None:
	"""Install the boot-resolved notification channel(s) (§6 Path B).

	Called ONCE from ``run_engine`` after the §2 coherence gate, with the
	mode's channel(s) resolved from the unified ``notifications:`` config
	(the SAME config the §2.4 coherence check parses). Subsequent ``notify``
	calls deliver to these channels via the unified ``send()`` — there is no
	per-call re-resolution (mirrors the §1 keystone: wired at boot, not
	per-call).

	An empty list is valid and means "notifications disabled" — ``notify``
	then short-circuits silently (the paper analog of the retired facade's
	"no webhook env var set" no-op).
	"""
	global _channels
	_channels = list(channels)


def _deliver(text: str) -> None:
	"""Build a :class:`Notification` from the rendered line every existing
	engine call site passes and hand it to the unified ``send()``.

	The call sites pass a single fully-rendered string (``_format_enter_/
	close_message`` output, the lost-CAS / shutdown lines). The unified
	envelope is channel-agnostic; the rendered line is carried verbatim as
	the body so the delivered content is preserved across the convergence.
	``send()`` is sync and never raises (per-channel ``DeliveryResult``);
	this function therefore cannot perturb its caller / the trade path.
	"""
	notification = Notification(title="edge-catcher", body=text)
	send(notification, _channels)


# Indirection seam: tests swap the delivery function (the unified-layer
# analog of the retired ``set_notify_backend``). Production always uses
# ``_deliver``; ``notify`` awaits this so a synchronous ``send()`` still
# runs off the trade path's critical section via the event loop.
_notify_fn: Callable[[str], Any] = _deliver


def set_notify_backend(fn: Callable[[str], Any]) -> None:
	"""Replace the delivery function (default: :func:`_deliver`).

	Test seam only — production resolves channels via
	:func:`configure_notify` and delivers through the unified ``send()``.
	"""
	global _notify_fn
	_notify_fn = fn


async def _deliver_async(text: str) -> None:
	"""Run the (sync, never-raising) delivery without blocking the loop.

	The unified ``send()`` is synchronous; offload it to a thread so a slow
	channel (e.g. a webhook POST) never stalls the engine's event loop.
	Defensive ``except`` is belt-and-suspenders only — ``send()`` already
	never raises; a raising test backend must still not perturb the caller.
	"""
	try:
		await asyncio.to_thread(_notify_fn, text)
	except Exception:  # noqa: BLE001 — never let a notify perturb the trade path
		logger.debug("notify delivery raised (absorbed; trade path unaffected)", exc_info=True)


def notify(text: str) -> None:
	"""Schedule a notification from sync context (requires a running event loop).

	Same shape every existing engine call site uses: sync, non-blocking,
	bounded — drops notifications if more than ``_MAX_PENDING`` are in
	flight. With no running loop, or no channel configured, it is a silent
	no-op (the paper analog of the retired facade's no-webhook no-op). The
	delivery backend is now the unified ``send()`` (§6 Path B), not an
	env-var webhook; the paper trade-row path is byte-unchanged.
	"""
	if not _channels:
		# Nothing resolved at boot (paper analog of "no webhook env var").
		return
	try:
		loop = asyncio.get_running_loop()
	except RuntimeError:
		return

	# Prune completed tasks.
	done = {t for t in _pending_tasks if t.done()}
	_pending_tasks.difference_update(done)

	if len(_pending_tasks) >= _MAX_PENDING:
		logger.debug("Notification queue full (%d pending), dropping: %s", len(_pending_tasks), text[:80])
		return

	coro: Coroutine[Any, Any, None] = _deliver_async(text)
	task: asyncio.Task = loop.create_task(coro)
	_pending_tasks.add(task)
	task.add_done_callback(_pending_tasks.discard)
