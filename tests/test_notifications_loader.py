"""Tests for load_channels — YAML parsing, env interpolation, schema validation."""
from __future__ import annotations

from pathlib import Path

import pytest

from edge_catcher.notifications.adapters.file import FileChannel
from edge_catcher.notifications.adapters.smtp import SMTPChannel
from edge_catcher.notifications.adapters.stdout import StdoutChannel
from edge_catcher.notifications.adapters.webhook import WebhookChannel
from edge_catcher.notifications.exceptions import NotificationConfigError
from edge_catcher.notifications.loader import load_channels


def _write(tmp_path: Path, text: str) -> Path:
	p = tmp_path / "n.yaml"
	p.write_text(text, encoding="utf-8")
	return p


def test_loads_simple_stdout_channel(tmp_path):
	p = _write(tmp_path, "channels:\n  console:\n    type: stdout\n")
	channels = load_channels(p)
	assert "console" in channels
	assert isinstance(channels["console"], StdoutChannel)
	assert channels["console"].name == "console"


def test_loads_file_channel_with_path(tmp_path):
	p = _write(tmp_path, "channels:\n  log:\n    type: file\n    path: /tmp/x.jsonl\n")
	channels = load_channels(p)
	assert isinstance(channels["log"], FileChannel)


def test_env_var_whole_string(tmp_path, monkeypatch):
	monkeypatch.setenv("HOOK_URL", "https://discord.example/h")
	p = _write(tmp_path, """\
channels:
  d:
    type: webhook
    url: ${HOOK_URL}
    style: discord
""")
	channels = load_channels(p)
	assert isinstance(channels["d"], WebhookChannel)
	assert channels["d"].url == "https://discord.example/h"


def test_env_var_embedded(tmp_path, monkeypatch):
	monkeypatch.setenv("TOKEN", "abc123")
	p = _write(tmp_path, """\
channels:
  d:
    type: webhook
    url: https://example.com/hook/${TOKEN}/path
""")
	channels = load_channels(p)
	assert channels["d"].url == "https://example.com/hook/abc123/path"


def test_env_var_in_list_element(tmp_path, monkeypatch):
	monkeypatch.setenv("ALERT_EMAIL", "alerts@example.com")
	monkeypatch.setenv("SMTP_USER", "u")
	monkeypatch.setenv("SMTP_PASS", "p")
	p = _write(tmp_path, """\
channels:
  e:
    type: smtp
    host: h
    port: 587
    user: ${SMTP_USER}
    password: ${SMTP_PASS}
    from: f@x
    to: [${ALERT_EMAIL}, ops@x]
""")
	channels = load_channels(p)
	smtp = channels["e"]
	assert isinstance(smtp, SMTPChannel)
	assert smtp.to == ["alerts@example.com", "ops@x"]


def test_non_string_scalar_not_interpolated(tmp_path, monkeypatch):
	# port: 587 is an int in YAML. ${...} substitution must NOT touch it.
	monkeypatch.setenv("SMTP_USER", "u")
	monkeypatch.setenv("SMTP_PASS", "p")
	p = _write(tmp_path, """\
channels:
  e:
    type: smtp
    host: h
    port: 587
    user: ${SMTP_USER}
    password: ${SMTP_PASS}
    from: f@x
    to: [t@x]
""")
	channels = load_channels(p)
	assert channels["e"].port == 587
	assert isinstance(channels["e"].port, int)


def test_missing_env_var_raises(tmp_path, monkeypatch):
	monkeypatch.delenv("MISSING_ONE", raising=False)
	p = _write(tmp_path, """\
channels:
  d:
    type: webhook
    url: ${MISSING_ONE}
""")
	with pytest.raises(NotificationConfigError, match="MISSING_ONE"):
		load_channels(p)


def test_unknown_channel_type_raises(tmp_path):
	p = _write(tmp_path, "channels:\n  x:\n    type: telegram\n")
	with pytest.raises(NotificationConfigError, match="unknown channel type"):
		load_channels(p)


def test_required_field_missing_raises(tmp_path):
	# webhook without url
	p = _write(tmp_path, "channels:\n  d:\n    type: webhook\n    style: discord\n")
	with pytest.raises(NotificationConfigError, match="url"):
		load_channels(p)


def test_unknown_field_raises(tmp_path):
	p = _write(tmp_path, """\
channels:
  d:
    type: stdout
    bogus: 1
""")
	with pytest.raises(NotificationConfigError, match="bogus"):
		load_channels(p)


def test_unknown_top_level_field_raises(tmp_path):
	p = _write(tmp_path, """\
channels:
  d:
    type: stdout
unknown_top: 1
""")
	with pytest.raises(NotificationConfigError, match="unknown_top"):
		load_channels(p)


def test_from_keyword_mapped_to_from_addr(tmp_path, monkeypatch):
	monkeypatch.setenv("U", "u")
	monkeypatch.setenv("P", "p")
	p = _write(tmp_path, """\
channels:
  e:
    type: smtp
    host: h
    port: 25
    user: ${U}
    password: ${P}
    from: alerts@example.com
    to: [me@x]
""")
	channels = load_channels(p)
	assert channels["e"].from_addr == "alerts@example.com"


def test_version_absent_accepted(tmp_path):
	p = _write(tmp_path, "channels:\n  d:\n    type: stdout\n")
	channels = load_channels(p)
	assert "d" in channels


def test_version_1_explicitly_accepted(tmp_path):
	p = _write(tmp_path, "version: 1\nchannels:\n  d:\n    type: stdout\n")
	channels = load_channels(p)
	assert "d" in channels


def test_version_2_rejected(tmp_path):
	p = _write(tmp_path, "version: 2\nchannels:\n  d:\n    type: stdout\n")
	with pytest.raises(NotificationConfigError, match="version"):
		load_channels(p)


def test_version_string_rejected(tmp_path):
	# YAML-quoted "1" is a string, not int 1. Reject explicitly so users
	# get a clear error rather than a silently-coerced acceptance.
	p = _write(tmp_path, 'version: "1"\nchannels:\n  d:\n    type: stdout\n')
	with pytest.raises(NotificationConfigError, match="version"):
		load_channels(p)


def test_smtp_to_must_be_nonempty_list(tmp_path, monkeypatch):
	monkeypatch.setenv("U", "u")
	monkeypatch.setenv("P", "p")
	# Empty list — must raise.
	p = _write(tmp_path, """\
channels:
  e:
    type: smtp
    host: h
    port: 587
    user: ${U}
    password: ${P}
    from: f@x
    to: []
""")
	with pytest.raises(NotificationConfigError, match="to"):
		load_channels(p)


def test_smtp_to_must_be_a_list_not_string(tmp_path, monkeypatch):
	monkeypatch.setenv("U", "u")
	monkeypatch.setenv("P", "p")
	# Bare string (forgotten brackets) — must raise rather than silently
	# producing wrong split-by-character behavior.
	p = _write(tmp_path, """\
channels:
  e:
    type: smtp
    host: h
    port: 587
    user: ${U}
    password: ${P}
    from: f@x
    to: just_one@x
""")
	with pytest.raises(NotificationConfigError, match="to"):
		load_channels(p)


def test_missing_file_raises(tmp_path):
	with pytest.raises(NotificationConfigError, match="not found"):
		load_channels(tmp_path / "does_not_exist.yaml")


def test_malformed_yaml_raises(tmp_path):
	p = _write(tmp_path, "channels:\n  d:\n    type: [this is not a string\n")
	with pytest.raises(NotificationConfigError, match="malformed"):
		load_channels(p)
