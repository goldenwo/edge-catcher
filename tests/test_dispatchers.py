"""Tests for api.dispatchers registries."""
import pytest

from api.dispatchers import (
	dispatch_download,
	dispatch_data_check,
	DOWNLOAD_DISPATCHERS,
	DATA_CHECK_DISPATCHERS,
)


class _FakeMeta:
	def __init__(self, exchange: str):
		self.exchange = exchange


def test_dispatch_unknown_exchange_raises():
	with pytest.raises(ValueError, match="No download dispatcher"):
		dispatch_download("x", _FakeMeta("nonexistent"), req=None, state=None)


def test_data_check_unknown_exchange_returns_false():
	"""Unknown exchange behaves like the original if/elif fall-through."""
	assert dispatch_data_check(_FakeMeta("nonexistent"), conn=None) is False


def test_dispatch_registry_contains_kalshi_and_coinbase():
	assert "kalshi" in DOWNLOAD_DISPATCHERS
	assert "coinbase" in DOWNLOAD_DISPATCHERS
	assert "kalshi" in DATA_CHECK_DISPATCHERS
	assert "coinbase" in DATA_CHECK_DISPATCHERS


def test_dispatch_routes_to_registered_handler():
	"""Smoke-test that dispatch_download calls the registered handler."""
	called = []
	DOWNLOAD_DISPATCHERS["test_exchange"] = lambda aid, meta, req, state: (
		called.append((aid, meta, req, state)) or ("target_fn", ("a1", "a2"))
	)
	try:
		target, args = dispatch_download(
			"adapt-1", _FakeMeta("test_exchange"), req={"k": "v"}, state={"s": 1}
		)
		assert target == "target_fn"
		assert args == ("a1", "a2")
		assert len(called) == 1
	finally:
		del DOWNLOAD_DISPATCHERS["test_exchange"]


def test_data_check_routes_to_registered_handler():
	"""Smoke-test that dispatch_data_check calls the registered handler."""
	DATA_CHECK_DISPATCHERS["test_exchange"] = lambda meta, conn: True
	try:
		assert dispatch_data_check(_FakeMeta("test_exchange"), conn=None) is True
	finally:
		del DATA_CHECK_DISPATCHERS["test_exchange"]
