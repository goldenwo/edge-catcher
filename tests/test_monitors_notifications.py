"""Tests for Discord notification module."""

import asyncio
import os
from unittest.mock import AsyncMock, patch



class TestDiscordNotify:
	def test_skips_when_no_webhook_url(self):
		"""Should silently return when no webhook URL is configured."""
		from edge_catcher.monitors.notifications import discord_notify
		with patch.dict(os.environ, {}, clear=True):
			asyncio.run(discord_notify("test message"))

	def test_sends_to_webhook(self):
		"""Should POST to the configured webhook URL."""
		from edge_catcher.monitors.notifications import discord_notify

		mock_client = AsyncMock()
		mock_resp = AsyncMock()
		mock_resp.status_code = 200
		mock_client.__aenter__ = AsyncMock(return_value=mock_client)
		mock_client.__aexit__ = AsyncMock(return_value=False)
		mock_client.post = AsyncMock(return_value=mock_resp)

		with patch.dict(os.environ, {"DISCORD_PAPER_TRADE_LOGS_WEBHOOK_URL": "https://example.com/webhook"}):
			with patch("edge_catcher.monitors.notifications.httpx.AsyncClient", return_value=mock_client):
				asyncio.run(discord_notify("test message"))

		mock_client.post.assert_called_once()
		call_args = mock_client.post.call_args
		assert call_args[1]["json"]["content"] == "test message"


class TestNotifySync:
	def test_notify_schedules_task(self):
		"""notify() should schedule discord_notify without blocking."""
		from edge_catcher.monitors.notifications import notify

		async def _run():
			notify("test")
			await asyncio.sleep(0.1)

		asyncio.run(_run())  # should not raise
