"""Discord webhook notifications with rate limiting and bounded concurrency.

DEPRECATED in v1.1+: prefer `edge_catcher.notifications` for new code.
This module is paper-trader-internal and will be migrated onto the
unified notifications layer in a future release. See
`docs/superpowers/specs/2026-04-26-notifications-design.md` §11.
"""

import asyncio
import logging
import os
import time
import warnings
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any

import httpx

warnings.warn(
	"edge_catcher.monitors.notifications is deprecated; use edge_catcher.notifications "
	"for new code. This module will be migrated onto the unified notifications layer "
	"in a future release.",
	DeprecationWarning,
	stacklevel=2,
)

logger = logging.getLogger(__name__)

_last_notify_time: float = 0.0
_NOTIFY_MIN_INTERVAL: float = 1.0  # min seconds between webhook calls
_MAX_PENDING: int = 20  # max queued notification tasks
_pending_tasks: set[asyncio.Task] = set()
_client: httpx.AsyncClient | None = None


def _get_client() -> httpx.AsyncClient:
	"""Reuse a single httpx client for all notifications."""
	global _client
	if _client is None or _client.is_closed:
		_client = httpx.AsyncClient(timeout=5.0)
	return _client


async def discord_notify(text: str) -> None:
	"""POST a message to Discord webhook. Rate-limited to avoid 429s."""
	global _last_notify_time
	url = (
		os.environ.get("DISCORD_PAPER_TRADE_LOGS_WEBHOOK_URL")
		or os.environ.get("DISCORD_LOGS_WEBHOOK_URL")
	)
	if not url:
		return
	now = time.monotonic()
	wait = _NOTIFY_MIN_INTERVAL - (now - _last_notify_time)
	if wait > 0:
		await asyncio.sleep(wait)
	_last_notify_time = time.monotonic()
	try:
		client = _get_client()
		resp = await client.post(url, json={"content": text})
		if resp.status_code == 429:
			retry_after = float(resp.headers.get("Retry-After", "2"))
			await asyncio.sleep(retry_after)
	except Exception:
		pass


_notify_fn: Callable[[str], Awaitable[None]] = discord_notify


def set_notify_backend(fn: Callable[[str], Awaitable[None]]) -> None:
	"""Replace the notification backend (default: discord_notify)."""
	global _notify_fn
	_notify_fn = fn


def notify(text: str) -> None:
	"""Schedule a notification from sync context (requires a running event loop).

	Bounded: drops notifications if more than _MAX_PENDING are in flight.
	"""
	try:
		loop = asyncio.get_running_loop()
	except RuntimeError:
		return

	# Prune completed tasks
	_pending_tasks.discard(None)  # no-op, just triggers the set's internal cleanup
	done = {t for t in _pending_tasks if t.done()}
	_pending_tasks.difference_update(done)

	if len(_pending_tasks) >= _MAX_PENDING:
		logger.debug("Notification queue full (%d pending), dropping: %s", len(_pending_tasks), text[:80])
		return

	# Cast: _notify_fn is typed as Callable[[str], Awaitable[None]] for caller
	# flexibility (override-by-injection in tests), but asyncio.create_task
	# wants a Coroutine. The runtime contract is satisfied by both adapters
	# (discord_notify is a coroutine function); the type signature is the
	# narrower one that mypy can verify.
	coro: Coroutine[Any, Any, None] = _notify_fn(text)  # type: ignore[assignment]
	task: asyncio.Task = loop.create_task(coro)
	_pending_tasks.add(task)
	task.add_done_callback(_pending_tasks.discard)
