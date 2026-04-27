"""edge_catcher.notifications — pluggable notification delivery layer.

Public API:
  - Notification, DeliveryResult: envelope types
  - Channel: protocol every adapter satisfies
  - send(notification, channels): dispatch to multiple channels
  - load_channels(yaml_path): build channels from YAML config
  - NotificationConfigError: raised for setup-time problems

See docs/superpowers/specs/2026-04-26-notifications-design.md for the
full design.
"""
from __future__ import annotations

from edge_catcher.notifications.base import Channel
from edge_catcher.notifications.dispatcher import send
from edge_catcher.notifications.envelope import DeliveryResult, Notification
from edge_catcher.notifications.exceptions import NotificationConfigError
from edge_catcher.notifications.loader import load_channels

__all__ = [
	"Channel",
	"DeliveryResult",
	"Notification",
	"NotificationConfigError",
	"load_channels",
	"send",
]
