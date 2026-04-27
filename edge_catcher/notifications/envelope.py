"""Notification envelope and per-channel delivery result types."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class Notification:
	"""Structured message handed to one or more delivery channels.

	Adapters pick the fields they care about; channel-agnostic by design.
	"""
	title: str
	body: str
	severity: Literal["info", "warn", "error"] = "info"
	payload: dict | None = None


@dataclass(frozen=True)
class DeliveryResult:
	"""Per-channel outcome returned by Channel.send()."""
	channel_name: str
	success: bool
	error: str | None = None
	latency_ms: float = 0.0
