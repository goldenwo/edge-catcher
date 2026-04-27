"""Tests for the notification envelope and exception types."""
from __future__ import annotations

import pytest

from edge_catcher.notifications.envelope import DeliveryResult, Notification
from edge_catcher.notifications.exceptions import NotificationConfigError


class TestNotification:
	def test_minimum_fields(self):
		n = Notification(title="t", body="b")
		assert n.title == "t"
		assert n.body == "b"
		assert n.severity == "info"
		assert n.payload is None

	def test_all_fields(self):
		n = Notification(title="t", body="b", severity="warn", payload={"k": 1})
		assert n.severity == "warn"
		assert n.payload == {"k": 1}

	def test_immutability(self):
		n = Notification(title="t", body="b")
		with pytest.raises(Exception):  # FrozenInstanceError or AttributeError
			n.title = "u"  # type: ignore[misc]

	def test_severity_literal_documented(self):
		# severity is typed as Literal["info", "warn", "error"] — runtime is just str.
		# This test documents accepted values rather than enforcing them at runtime.
		for s in ("info", "warn", "error"):
			Notification(title="t", body="b", severity=s)  # no error


class TestDeliveryResult:
	def test_success_defaults(self):
		r = DeliveryResult(channel_name="x", success=True)
		assert r.success is True
		assert r.error is None
		assert r.latency_ms == 0.0

	def test_failure(self):
		r = DeliveryResult(channel_name="x", success=False, error="boom", latency_ms=12.3)
		assert r.success is False
		assert r.error == "boom"
		assert r.latency_ms == 12.3

	def test_immutability(self):
		r = DeliveryResult(channel_name="x", success=True)
		with pytest.raises(Exception):
			r.success = False  # type: ignore[misc]


class TestNotificationConfigError:
	def test_is_exception_subclass(self):
		assert issubclass(NotificationConfigError, Exception)

	def test_carries_message(self):
		err = NotificationConfigError("bad config: foo")
		assert "bad config: foo" in str(err)


class TestChannelProtocol:
	def test_duck_typed_channel_satisfies_protocol(self):
		from edge_catcher.notifications.base import Channel

		class FakeChannel:
			name = "fake"

			def send(self, notification: Notification) -> DeliveryResult:
				return DeliveryResult(channel_name=self.name, success=True)

		ch = FakeChannel()
		# Runtime-checkable Protocol — isinstance must accept duck-typed implementations.
		assert isinstance(ch, Channel)

	def test_missing_send_method_fails_isinstance(self):
		from edge_catcher.notifications.base import Channel

		class Broken:
			name = "broken"
			# no send() method

		assert not isinstance(Broken(), Channel)
