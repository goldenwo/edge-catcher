"""Tests for SMTPChannel — message construction + envelope handling."""
from __future__ import annotations

import json
import smtplib
from unittest.mock import MagicMock

from edge_catcher.notifications.adapters.smtp import SMTPChannel
from edge_catcher.notifications.envelope import Notification


def _make_mock_smtp(monkeypatch, raise_on=None):
	mock_smtp_instance = MagicMock()
	mock_smtp_class = MagicMock(return_value=mock_smtp_instance)
	if raise_on:
		getattr(mock_smtp_instance, raise_on).side_effect = smtplib.SMTPException(f"{raise_on} failed")
	monkeypatch.setattr(smtplib, "SMTP", mock_smtp_class)
	return mock_smtp_class, mock_smtp_instance


def test_send_message_called_with_to_addrs(monkeypatch):
	mock_class, mock_instance = _make_mock_smtp(monkeypatch)
	ch = SMTPChannel(
		name="email",
		host="smtp.example.com",
		port=587,
		user="u",
		password="p",
		from_addr="from@x",
		to=["a@x", "b@x"],
	)
	r = ch.send(Notification(title="T", body="B"))
	assert r.success
	# send_message must be called with to_addrs explicitly (multi-recipient envelope guarantee)
	args, kwargs = mock_instance.send_message.call_args
	assert kwargs.get("to_addrs") == ["a@x", "b@x"]
	assert kwargs.get("from_addr") == "from@x"
	# Success path also calls quit() in the finally block (lock the contract).
	mock_instance.quit.assert_called_once()


def test_message_subject_and_headers(monkeypatch):
	mock_class, mock_instance = _make_mock_smtp(monkeypatch)
	ch = SMTPChannel(
		name="email", host="h", port=587, user="u", password="p",
		from_addr="from@x", to=["to@x"],
	)
	ch.send(Notification(title="Daily P&L", body="net +$11", severity="warn"))
	msg = mock_instance.send_message.call_args[0][0]
	# str() unwraps the Header object stdlib uses for non-ASCII safety.
	assert str(msg["Subject"]) == "[warn] Daily P&L"
	assert str(msg["From"]) == "from@x"
	assert str(msg["To"]) == "to@x"


def test_payload_appended_to_body(monkeypatch):
	mock_class, mock_instance = _make_mock_smtp(monkeypatch)
	ch = SMTPChannel(
		name="email", host="h", port=587, user="u", password="p",
		from_addr="f@x", to=["t@x"],
	)
	ch.send(Notification(title="T", body="B", payload={"k": 1}))
	msg = mock_instance.send_message.call_args[0][0]
	content = msg.get_content()
	assert "B" in content
	# JSON payload appended after two newlines
	assert json.dumps({"k": 1}, indent=2) in content


def test_no_payload_means_body_only(monkeypatch):
	mock_class, mock_instance = _make_mock_smtp(monkeypatch)
	ch = SMTPChannel(
		name="email", host="h", port=587, user="u", password="p",
		from_addr="f@x", to=["t@x"],
	)
	ch.send(Notification(title="T", body="just B"))
	msg = mock_instance.send_message.call_args[0][0]
	assert msg.get_content().strip() == "just B"


def test_starttls_used_when_use_tls_true(monkeypatch):
	mock_class, mock_instance = _make_mock_smtp(monkeypatch)
	ch = SMTPChannel(
		name="email", host="h", port=587, user="u", password="p",
		from_addr="f@x", to=["t@x"], use_tls=True,
	)
	ch.send(Notification(title="T", body="B"))
	mock_instance.starttls.assert_called_once()


def test_starttls_skipped_when_use_tls_false(monkeypatch):
	mock_class, mock_instance = _make_mock_smtp(monkeypatch)
	ch = SMTPChannel(
		name="email", host="h", port=587, user="u", password="p",
		from_addr="f@x", to=["t@x"], use_tls=False,
	)
	ch.send(Notification(title="T", body="B"))
	mock_instance.starttls.assert_not_called()


def test_login_called_with_credentials(monkeypatch):
	mock_class, mock_instance = _make_mock_smtp(monkeypatch)
	ch = SMTPChannel(
		name="email", host="h", port=587, user="USR", password="PWD",
		from_addr="f@x", to=["t@x"],
	)
	ch.send(Notification(title="T", body="B"))
	mock_instance.login.assert_called_once_with("USR", "PWD")


def test_quit_called_in_finally(monkeypatch):
	mock_class, mock_instance = _make_mock_smtp(monkeypatch, raise_on="login")
	ch = SMTPChannel(
		name="email", host="h", port=587, user="u", password="bad",
		from_addr="f@x", to=["t@x"],
	)
	r = ch.send(Notification(title="T", body="B"))
	assert r.success is False
	mock_instance.quit.assert_called_once()


def test_smtpexception_caught(monkeypatch):
	mock_class, mock_instance = _make_mock_smtp(monkeypatch, raise_on="send_message")
	ch = SMTPChannel(
		name="email", host="h", port=587, user="u", password="p",
		from_addr="f@x", to=["t@x"],
	)
	r = ch.send(Notification(title="T", body="B"))
	assert r.success is False
	assert "send_message failed" in (r.error or "") or "SMTPException" in (r.error or "")


def test_oserror_caught(monkeypatch):
	def fail(*a, **kw):
		raise OSError("connection refused")
	monkeypatch.setattr(smtplib, "SMTP", fail)
	ch = SMTPChannel(
		name="email", host="h", port=587, user="u", password="p",
		from_addr="f@x", to=["t@x"],
	)
	r = ch.send(Notification(title="T", body="B"))
	assert r.success is False
	assert "connection refused" in (r.error or "") or "OSError" in (r.error or "")
	# When SMTP() constructor itself raises, no instance was constructed,
	# so quit() should not have been called on anything (no AttributeError).
	# This is implicitly verified by the test not crashing in the finally block.


def test_default_timeout_is_10_seconds(monkeypatch):
	mock_class, mock_instance = _make_mock_smtp(monkeypatch)
	ch = SMTPChannel(
		name="email", host="h", port=587, user="u", password="p",
		from_addr="f@x", to=["t@x"],
	)
	ch.send(Notification(title="T", body="B"))
	# smtplib.SMTP(host, port, timeout=...) — third positional or `timeout` kw
	args, kwargs = mock_class.call_args
	# Timeout MUST be passed — either as keyword or as the third positional arg.
	assert kwargs.get("timeout") == 10 or (len(args) >= 3 and args[2] == 10), (
		f"timeout=10 was not passed to smtplib.SMTP(): args={args}, kwargs={kwargs}"
	)


def test_custom_timeout_passed_through(monkeypatch):
	mock_class, mock_instance = _make_mock_smtp(monkeypatch)
	ch = SMTPChannel(
		name="email", host="h", port=587, user="u", password="p",
		from_addr="f@x", to=["t@x"], timeout_seconds=3.0,
	)
	ch.send(Notification(title="T", body="B"))
	args, kwargs = mock_class.call_args
	# Either positional 3rd arg or `timeout` kwarg should be 3.0
	assert (len(args) >= 3 and args[2] == 3.0) or kwargs.get("timeout") == 3.0


def test_header_with_crlf_returns_failed_result_not_raise(monkeypatch):
	"""A title containing CR/LF triggers email.errors.HeaderParseError on
	msg["Subject"] = ... — adapter must catch it (no-raise contract) and
	return DeliveryResult(success=False), not propagate the exception."""
	# SMTP class is mocked; we won't reach the network. The exception comes
	# from EmailMessage construction itself.
	mock_class, mock_instance = _make_mock_smtp(monkeypatch)
	ch = SMTPChannel(
		name="email", host="h", port=587, user="u", password="p",
		from_addr="f@x", to=["t@x"],
	)
	# CRLF injection in title — would be a header-injection attack vector if
	# accepted; email module raises ValueError to prevent it.
	r = ch.send(Notification(title="Subject\r\nBcc: attacker@evil", body="B"))
	assert r.success is False
	assert r.error  # populated, not None
	# Latency comes from the adapter (we set t0 first), not the dispatcher.
	assert r.latency_ms >= 0
	# SMTP() should never have been constructed because the message build failed first.
	mock_class.assert_not_called()


def test_quit_failure_logged_not_swallowed(monkeypatch, caplog):
	"""When smtp_conn.quit() raises in finally, the failure is logged at
	DEBUG (developer-visible) but does NOT propagate or override the
	primary outcome."""
	import logging as _logging

	mock_class, mock_instance = _make_mock_smtp(monkeypatch)
	# Make quit() raise to exercise the except clause.
	mock_instance.quit.side_effect = OSError("connection already closed")
	ch = SMTPChannel(
		name="email", host="h", port=587, user="u", password="p",
		from_addr="f@x", to=["t@x"],
	)
	with caplog.at_level(_logging.DEBUG, logger="edge_catcher.notifications.adapters.smtp"):
		r = ch.send(Notification(title="T", body="B"))
	# Send itself succeeded (mocks all return ok).
	assert r.success is True
	# quit() was called and raised; the failure was logged not propagated.
	mock_instance.quit.assert_called_once()
	debug_records = [rec for rec in caplog.records if rec.levelno == _logging.DEBUG]
	assert any("quit" in rec.getMessage().lower() for rec in debug_records)
