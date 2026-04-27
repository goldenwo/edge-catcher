"""Tests for the top-level edge_catcher.notifications public API."""
from __future__ import annotations


def test_top_level_imports():
	from edge_catcher.notifications import (
		Channel,
		DeliveryResult,
		Notification,
		NotificationConfigError,
		load_channels,
		send,
	)
	assert Channel is not None
	assert DeliveryResult is not None
	assert Notification is not None
	assert NotificationConfigError is not None
	assert load_channels is not None
	assert send is not None


def test_all_attribute_lists_public_api():
	import edge_catcher.notifications as ns
	assert set(ns.__all__) == {
		"Channel", "DeliveryResult", "Notification",
		"NotificationConfigError", "load_channels", "send",
	}
