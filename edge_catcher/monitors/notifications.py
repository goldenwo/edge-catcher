"""Paper-trader-internal Discord webhook notifications.

This module is intentionally separate from `edge_catcher.notifications`
(the user-facing pluggable delivery layer) because the two solve
different problems with different constraints:

- `edge_catcher.notifications` — synchronous, config-driven, multi-adapter
  delivery for the reporting CLI. No rate limiting, no concurrency control,
  no async. Designed for low-frequency end-user notifications (daily P&L).

- This module — async (so the trading loop never blocks on HTTP), rate-limited
  (one webhook call per second, with 429 retry), and bounded-concurrency
  (max 20 pending tasks, drops overflow). Designed for the paper trader's
  per-trade Discord-only firehose.

Migration onto the unified layer is NOT planned: doing so would require
adding async + rate-limiting + concurrency to the user-facing surface,
which is explicitly out of scope per the v1.1 notifications design.
The two layers are intentional specialisations, not duplication.
"""

import asyncio
import logging
import os
import time
from collections.abc import Awaitable, Callable

import httpx

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

	task = loop.create_task(_notify_fn(text))
	_pending_tasks.add(task)
	task.add_done_callback(_pending_tasks.discard)
