"""Notification-module exception types."""
from __future__ import annotations


class NotificationConfigError(Exception):
	"""Raised at config-load time for any setup-side problem.

	Examples: missing config file, malformed YAML, unknown channel type,
	missing required field, missing referenced env var, version mismatch.

	Delivery-time problems do NOT raise this exception — they are reported
	per-channel via DeliveryResult.
	"""
