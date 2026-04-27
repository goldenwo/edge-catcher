"""Stdout adapter — writes formatted text to sys.stdout."""
from __future__ import annotations

import sys
import time
from datetime import datetime, timezone

from edge_catcher.notifications.envelope import DeliveryResult, Notification


class StdoutChannel:
	"""Plain-text notification delivery to stdout."""

	def __init__(self, name: str) -> None:
		self.name = name

	def send(self, notification: Notification) -> DeliveryResult:
		t0 = time.perf_counter()
		ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
		try:
			sys.stdout.write(
				f"[{ts}] [{notification.severity}] {notification.title}\n"
				f"{notification.body}\n"
			)
			sys.stdout.flush()
		except OSError as exc:
			return DeliveryResult(
				channel_name=self.name,
				success=False,
				error=repr(exc),
				latency_ms=(time.perf_counter() - t0) * 1000,
			)
		return DeliveryResult(
			channel_name=self.name,
			success=True,
			latency_ms=(time.perf_counter() - t0) * 1000,
		)
