"""Webhook adapter — POSTs JSON to a URL with style-specific payload shape."""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Literal

import httpx

from edge_catcher.notifications.envelope import DeliveryResult, Notification


WebhookStyle = Literal["discord", "slack", "generic"]

_HTTP_BODY_TRUNCATE = 200


class WebhookChannel:
	"""HTTP webhook delivery."""

	def __init__(
		self,
		name: str,
		url: str,
		style: WebhookStyle = "generic",
		timeout_seconds: float = 10.0,
	) -> None:
		self.name = name
		self.url = url
		self.style = style
		self.timeout_seconds = timeout_seconds

	def send(self, notification: Notification) -> DeliveryResult:
		t0 = time.perf_counter()
		ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
		body = self._build_payload(notification, ts)
		try:
			with httpx.Client(timeout=self.timeout_seconds) as client:
				resp = client.post(self.url, json=body)
		except httpx.TimeoutException as exc:
			return self._fail(t0, repr(exc))
		except httpx.HTTPError as exc:
			return self._fail(t0, repr(exc))
		except Exception as exc:  # defensive — adapter must not raise
			return self._fail(t0, repr(exc))

		if resp.status_code >= 400:
			truncated = (resp.text or "")[:_HTTP_BODY_TRUNCATE]
			return self._fail(t0, f"http {resp.status_code}: {truncated}")
		return DeliveryResult(
			channel_name=self.name,
			success=True,
			latency_ms=(time.perf_counter() - t0) * 1000,
		)

	def _fail(self, t0: float, error: str) -> DeliveryResult:
		return DeliveryResult(
			channel_name=self.name,
			success=False,
			error=error,
			latency_ms=(time.perf_counter() - t0) * 1000,
		)

	def _build_payload(self, n: Notification, ts: str) -> dict:
		if self.style == "generic":
			return {
				"title": n.title,
				"body": n.body,
				"severity": n.severity,
				"payload": n.payload,
				"ts": ts,
			}
		# Other styles filled in by Task 6 + 7.
		raise NotImplementedError(f"style {self.style!r} not yet implemented")
