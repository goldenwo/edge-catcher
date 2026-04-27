"""Notification dispatcher — fan-out send to multiple channels."""
from __future__ import annotations

import logging
import time

from edge_catcher.notifications.base import Channel
from edge_catcher.notifications.envelope import DeliveryResult, Notification

logger = logging.getLogger(__name__)


def send(
	notification: Notification,
	channels: list[Channel],
) -> dict[str, DeliveryResult]:
	"""Deliver `notification` to each channel sequentially in input order.

	Returns {channel.name: DeliveryResult}. Per-channel exceptions are
	caught defensively and translated into failed DeliveryResults; the
	function never propagates delivery-time exceptions.
	"""
	results: dict[str, DeliveryResult] = {}
	for ch in channels:
		t0 = time.perf_counter()
		try:
			result = ch.send(notification)
		except Exception as exc:  # programmer-error safety net
			logger.warning(
				"adapter %r raised on send (this signals an adapter bug, not a delivery problem): %r",
				ch.name, exc,
			)
			result = DeliveryResult(
				channel_name=ch.name,
				success=False,
				error=f"adapter raised: {exc!r}",
				latency_ms=(time.perf_counter() - t0) * 1000,
			)
		results[ch.name] = result
	return results
