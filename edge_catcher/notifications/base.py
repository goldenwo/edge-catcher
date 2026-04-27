"""Channel protocol — the contract every notification adapter satisfies."""
from __future__ import annotations

from typing import Protocol, runtime_checkable

from edge_catcher.notifications.envelope import DeliveryResult, Notification


@runtime_checkable
class Channel(Protocol):
	"""Adapters implementing this protocol can be passed to send().

	An adapter MUST NOT raise on delivery failure. Network errors, HTTP
	non-2xx, SMTP failures, file I/O errors etc. MUST be caught inside
	the adapter and returned as DeliveryResult(success=False, error=...).

	An adapter MAY raise on programmer errors (NotImplementedError,
	internal AssertionError, malformed Notification). The dispatcher
	catches these defensively and translates them to a failed
	DeliveryResult, but they signal a bug, not a delivery problem.
	"""
	name: str

	def send(self, notification: Notification) -> DeliveryResult:
		...
