"""Tests for edge_catcher.live.errors — exception class hierarchy."""
from __future__ import annotations

from edge_catcher.live.errors import (
	CapExceededError,
	ConfigError,
	KalshiAPIError,
	LiveError,
	NetworkError,
	OrderAlreadyFinal,
	OrderRejected,
)


def test_all_subclass_live_error():
	for cls in (ConfigError, CapExceededError, KalshiAPIError, NetworkError):
		assert issubclass(cls, LiveError)


def test_order_rejected_subclasses_kalshi_api_error():
	assert issubclass(OrderRejected, KalshiAPIError)


def test_order_already_final_subclasses_kalshi_api_error():
	assert issubclass(OrderAlreadyFinal, KalshiAPIError)


def test_cap_exceeded_message_format():
	err = CapExceededError(2.50, 1.00, "CLI cap")
	assert "$2.50" in str(err)
	assert "$1.00" in str(err)
	assert "CLI cap" in str(err)
	assert err.exposure_dollars == 2.50
	assert err.cap_dollars == 1.00
	assert err.cap_name == "CLI cap"


def test_kalshi_api_error_truncates_long_body():
	long_body = "x" * 1000
	err = KalshiAPIError(400, long_body, "/portfolio/orders")
	# Truncated to 500 chars in message
	assert "x" * 500 in str(err)
	assert "x" * 501 not in str(err)
	# Full body retained on the instance
	assert err.body == long_body
	assert err.status == 400
	assert err.path == "/portfolio/orders"
