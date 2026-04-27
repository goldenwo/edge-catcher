"""Tests for WebhookChannel — generic style + HTTP error handling."""
from __future__ import annotations

from unittest.mock import MagicMock

import httpx
import pytest

from edge_catcher.notifications.adapters.webhook import WebhookChannel
from edge_catcher.notifications.envelope import Notification


class FakeResponse:
	def __init__(self, status_code: int, text: str = ""):
		self.status_code = status_code
		self.text = text


def _patch_post(monkeypatch, response: FakeResponse | None = None, raise_exc: Exception | None = None):
	"""Replace httpx.Client.post with a recorder.

	Returns a list that captures (url, json) of every call.
	"""
	calls = []

	def fake_post(self, url, json=None, **kwargs):
		calls.append((url, json))
		if raise_exc is not None:
			raise raise_exc
		return response or FakeResponse(204)

	monkeypatch.setattr(httpx.Client, "post", fake_post)
	return calls


def test_generic_style_payload_shape(monkeypatch):
	calls = _patch_post(monkeypatch, FakeResponse(204))
	ch = WebhookChannel(name="generic_hook", url="https://example.com/hook", style="generic")
	r = ch.send(Notification(title="T", body="B", severity="warn", payload={"k": 1}))
	assert r.success
	assert len(calls) == 1
	url, body = calls[0]
	assert url == "https://example.com/hook"
	assert body["title"] == "T"
	assert body["body"] == "B"
	assert body["severity"] == "warn"
	assert body["payload"] == {"k": 1}
	assert "ts" in body


def test_http_5xx_returns_failure_with_truncated_body(monkeypatch):
	long_body = "x" * 500
	_patch_post(monkeypatch, FakeResponse(502, long_body))
	ch = WebhookChannel(name="hook", url="https://example.com/hook", style="generic")
	r = ch.send(Notification(title="T", body="B"))
	assert r.success is False
	assert "502" in (r.error or "")
	# body must be truncated to 200 chars in error string
	assert (r.error or "").count("x") <= 200


def test_http_4xx_returns_failure(monkeypatch):
	_patch_post(monkeypatch, FakeResponse(400, "bad request"))
	ch = WebhookChannel(name="hook", url="https://example.com/hook")
	r = ch.send(Notification(title="T", body="B"))
	assert r.success is False
	assert "400" in (r.error or "")


def test_timeout_caught(monkeypatch):
	_patch_post(monkeypatch, raise_exc=httpx.TimeoutException("slow"))
	ch = WebhookChannel(name="hook", url="https://example.com/hook")
	r = ch.send(Notification(title="T", body="B"))
	assert r.success is False
	assert "Timeout" in (r.error or "") or "slow" in (r.error or "")


def test_generic_http_error_caught(monkeypatch):
	_patch_post(monkeypatch, raise_exc=httpx.HTTPError("connect refused"))
	ch = WebhookChannel(name="hook", url="https://example.com/hook")
	r = ch.send(Notification(title="T", body="B"))
	assert r.success is False
	assert "connect refused" in (r.error or "") or "HTTPError" in (r.error or "")
