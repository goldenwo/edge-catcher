"""Regression test for the monitors/notifications.py -> engine/notifications.py move.

Sub-project G relocates the paper-trader-internal Discord webhook helper from
``edge_catcher.monitors.notifications`` to ``edge_catcher.engine.notifications``.
The wire-shape MUST be byte-identical across the move so the live paper trader
keeps producing the exact same Discord posts after cutover.

This is the strongest mitigation for the "notifications-routing risk" called out
in spec §Risks: mock ``httpx.AsyncClient.post`` and assert the request URL +
JSON body match the pre-G behavior verbatim.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from edge_catcher.engine.notifications import discord_notify


@pytest.fixture(autouse=True)
def _reset_notify_state(monkeypatch):
	"""Drop the module-level rate-limit timestamp + reusable client between tests
	so each test sees a clean state regardless of order.
	"""
	import edge_catcher.engine.notifications as notif

	monkeypatch.setattr(notif, "_last_notify_time", 0.0, raising=False)
	monkeypatch.setattr(notif, "_client", None, raising=False)
	yield


def test_discord_notify_posts_to_paper_webhook_url(monkeypatch):
	"""When DISCORD_PAPER_TRADE_LOGS_WEBHOOK_URL is set, post lands there
	with the engine's canonical ``{"content": text}`` body.
	"""
	monkeypatch.setenv(
		"DISCORD_PAPER_TRADE_LOGS_WEBHOOK_URL",
		"https://discord.com/api/webhooks/PAPER-TEST",
	)
	mock_post = AsyncMock()
	mock_post.return_value.status_code = 200

	with patch("httpx.AsyncClient.post", mock_post):
		asyncio.run(discord_notify("hello world"))

	mock_post.assert_called_once()
	args, kwargs = mock_post.call_args
	assert args[0] == "https://discord.com/api/webhooks/PAPER-TEST"
	assert kwargs["json"] == {"content": "hello world"}


def test_discord_notify_falls_back_to_logs_webhook(monkeypatch):
	"""Pre-G facade behavior: prefers DISCORD_PAPER_TRADE_LOGS_WEBHOOK_URL,
	falls back to DISCORD_LOGS_WEBHOOK_URL when the paper-specific var is unset.
	"""
	monkeypatch.delenv("DISCORD_PAPER_TRADE_LOGS_WEBHOOK_URL", raising=False)
	monkeypatch.setenv(
		"DISCORD_LOGS_WEBHOOK_URL",
		"https://discord.com/api/webhooks/FALLBACK",
	)
	mock_post = AsyncMock()
	mock_post.return_value.status_code = 200

	with patch("httpx.AsyncClient.post", mock_post):
		asyncio.run(discord_notify("hello world"))

	mock_post.assert_called_once()
	assert mock_post.call_args.args[0] == "https://discord.com/api/webhooks/FALLBACK"


def test_discord_notify_silent_no_op_without_webhook_url(monkeypatch):
	"""Neither env var set -> the function must short-circuit before touching
	httpx, so test environments without webhooks don't fire stray POSTs.
	"""
	monkeypatch.delenv("DISCORD_PAPER_TRADE_LOGS_WEBHOOK_URL", raising=False)
	monkeypatch.delenv("DISCORD_LOGS_WEBHOOK_URL", raising=False)
	mock_post = AsyncMock()

	with patch("httpx.AsyncClient.post", mock_post):
		asyncio.run(discord_notify("hello world"))

	mock_post.assert_not_called()
