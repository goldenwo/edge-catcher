"""Tests for StdoutChannel."""
from __future__ import annotations

from edge_catcher.notifications.adapters.stdout import StdoutChannel
from edge_catcher.notifications.envelope import Notification


def test_writes_formatted_text(capsys):
	ch = StdoutChannel(name="console")
	n = Notification(title="Daily P&L", body="Net: +$11.12", severity="info")
	result = ch.send(n)
	captured = capsys.readouterr()
	assert "[info]" in captured.out
	assert "Daily P&L" in captured.out
	assert "Net: +$11.12" in captured.out
	assert result.success is True
	assert result.channel_name == "console"


def test_includes_timestamp(capsys):
	ch = StdoutChannel(name="console")
	ch.send(Notification(title="t", body="b"))
	captured = capsys.readouterr()
	# ISO-style 'T' separator + 'Z' suffix
	assert "T" in captured.out
	assert "Z" in captured.out


def test_severity_rendered(capsys):
	ch = StdoutChannel(name="console")
	ch.send(Notification(title="x", body="y", severity="error"))
	captured = capsys.readouterr()
	assert "[error]" in captured.out


def test_payload_ignored(capsys):
	ch = StdoutChannel(name="console")
	ch.send(Notification(title="x", body="y", payload={"secret": "v"}))
	captured = capsys.readouterr()
	assert "secret" not in captured.out
