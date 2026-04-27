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
