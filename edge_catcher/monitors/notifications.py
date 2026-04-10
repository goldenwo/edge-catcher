"""Discord webhook notifications with rate limiting."""

import asyncio
import os
import time

import httpx

_last_notify_time: float = 0.0
_NOTIFY_MIN_INTERVAL: float = 1.0  # min seconds between webhook calls


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
		async with httpx.AsyncClient(timeout=5.0) as client:
			resp = await client.post(url, json={"content": text})
			if resp.status_code == 429:
				retry_after = float(resp.headers.get("Retry-After", "2"))
				await asyncio.sleep(retry_after)
	except Exception:
		pass


def notify(text: str) -> None:
	"""Schedule a Discord notification from sync context (requires a running event loop)."""
	try:
		asyncio.get_running_loop().create_task(discord_notify(text))
	except RuntimeError:
		pass
