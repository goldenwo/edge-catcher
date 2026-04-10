"""Discord webhook notifications with rate limiting and bounded concurrency."""

import asyncio
import logging
import os
import time

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


def notify(text: str) -> None:
	"""Schedule a Discord notification from sync context (requires a running event loop).

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

	task = loop.create_task(discord_notify(text))
	_pending_tasks.add(task)
	task.add_done_callback(_pending_tasks.discard)
