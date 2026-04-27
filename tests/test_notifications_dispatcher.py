"""Tests for the dispatcher — send() collects per-channel results."""
from __future__ import annotations

from edge_catcher.notifications.dispatcher import send
from edge_catcher.notifications.envelope import DeliveryResult, Notification


class FakeOK:
	def __init__(self, name: str, latency: float = 1.0):
		self.name = name
		self._lat = latency

	def send(self, notification: Notification) -> DeliveryResult:
		return DeliveryResult(channel_name=self.name, success=True, latency_ms=self._lat)


class FakeFail:
	def __init__(self, name: str, error: str = "boom"):
		self.name = name
		self._err = error

	def send(self, notification: Notification) -> DeliveryResult:
		return DeliveryResult(channel_name=self.name, success=False, error=self._err, latency_ms=0.5)


class FakeRaise:
	"""Adapter that violates the contract by raising. Dispatcher must catch."""
	def __init__(self, name: str):
		self.name = name

	def send(self, notification: Notification) -> DeliveryResult:
		raise RuntimeError("internal bug")


def test_collects_results_in_order():
	channels = [FakeOK("a"), FakeFail("b"), FakeOK("c")]
	results = send(Notification(title="t", body="b"), channels)
	assert list(results.keys()) == ["a", "b", "c"]
	assert results["a"].success is True
	assert results["b"].success is False
	assert results["c"].success is True
	assert results["a"].latency_ms == 1.0  # adapter-reported latency passes through


def test_returns_dict_keyed_by_name():
	channels = [FakeOK("first"), FakeOK("second")]
	results = send(Notification(title="t", body="b"), channels)
	assert "first" in results and "second" in results


def test_adapter_exception_caught_and_translated():
	channels = [FakeOK("a"), FakeRaise("b"), FakeOK("c")]
	results = send(Notification(title="t", body="b"), channels)
	# c must still run after b raised
	assert results["a"].success is True
	assert results["b"].success is False
	assert "internal bug" in (results["b"].error or "") or "RuntimeError" in (results["b"].error or "")
	assert results["c"].success is True


def test_empty_channels_returns_empty_dict():
	results = send(Notification(title="t", body="b"), [])
	assert results == {}


def test_latency_ms_populated_for_caught_exception():
	# Even when an adapter raises, latency_ms must reflect actual elapsed
	# time, not the default 0.0. Sleep before raising so we can detect
	# whether the dispatcher actually brackets the call with perf_counter.
	import time as _time

	class SlowFakeRaise:
		name = "slow"

		def send(self, n):
			_time.sleep(0.01)  # 10 ms
			raise RuntimeError("boom")

	results = send(Notification(title="t", body="b"), [SlowFakeRaise()])
	# Must be > 5 ms (well above the 0.0 default; well below the sleep
	# duration to leave headroom on slow CI runners).
	assert results["slow"].latency_ms >= 5.0
	assert results["slow"].success is False
