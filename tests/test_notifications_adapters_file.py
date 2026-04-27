"""Tests for FileChannel."""
from __future__ import annotations

import json
from pathlib import Path

from edge_catcher.notifications.adapters.file import FileChannel
from edge_catcher.notifications.envelope import Notification


def test_appends_jsonl_line(tmp_path: Path):
	target = tmp_path / "log.jsonl"
	ch = FileChannel(name="daily_log", path=str(target))
	r1 = ch.send(Notification(title="t1", body="b1"))
	r2 = ch.send(Notification(title="t2", body="b2", payload={"k": 1}))
	assert r1.success and r2.success
	lines = target.read_text(encoding="utf-8").splitlines()
	assert len(lines) == 2
	rec0 = json.loads(lines[0])
	rec1 = json.loads(lines[1])
	assert rec0["title"] == "t1"
	assert rec1["title"] == "t2"
	assert rec1["payload"] == {"k": 1}


def test_creates_parent_dir(tmp_path: Path):
	target = tmp_path / "nested" / "deep" / "log.jsonl"
	assert not target.parent.exists()
	ch = FileChannel(name="x", path=str(target))
	r = ch.send(Notification(title="t", body="b"))
	assert r.success
	assert target.parent.is_dir()
	assert target.is_file()


def test_utf8_encoding(tmp_path: Path):
	target = tmp_path / "log.jsonl"
	ch = FileChannel(name="x", path=str(target))
	ch.send(Notification(title="café — 🎯", body="naïve"))
	content = target.read_text(encoding="utf-8")
	assert "café" in content
	assert "🎯" in content


def test_record_shape(tmp_path: Path):
	target = tmp_path / "log.jsonl"
	ch = FileChannel(name="my_channel", path=str(target))
	ch.send(Notification(title="t", body="b", severity="warn", payload={"a": 1}))
	rec = json.loads(target.read_text(encoding="utf-8").splitlines()[0])
	assert set(rec.keys()) == {"ts", "channel", "title", "body", "severity", "payload"}
	assert rec["channel"] == "my_channel"
	assert rec["severity"] == "warn"
	# Lock the no-payload case too: payload key MUST be present and MUST be None.
	ch.send(Notification(title="t2", body="b2"))
	rec2 = json.loads(target.read_text(encoding="utf-8").splitlines()[1])
	assert set(rec2.keys()) == {"ts", "channel", "title", "body", "severity", "payload"}
	assert rec2["payload"] is None


from datetime import datetime, timezone
import json as _json


def test_non_json_payload_serialized_via_default_str(tmp_path):
	"""Non-JSON-native payload values (datetime, Decimal etc.) are coerced via
	default=str rather than raising TypeError out of send()."""
	target = tmp_path / "log.jsonl"
	ch = FileChannel(name="x", path=str(target))
	ts = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
	r = ch.send(Notification(title="t", body="b", payload={"created_at": ts}))
	assert r.success is True
	rec = _json.loads(target.read_text(encoding="utf-8").splitlines()[0])
	# default=str coerces datetime to its str() form.
	assert isinstance(rec["payload"]["created_at"], str)
	assert "2026-01-01" in rec["payload"]["created_at"]


def test_permission_error_caught(tmp_path: Path, monkeypatch):
	target = tmp_path / "log.jsonl"
	ch = FileChannel(name="x", path=str(target))

	def raises_permission(*a, **kw):
		raise PermissionError("denied")

	monkeypatch.setattr("builtins.open", raises_permission)
	r = ch.send(Notification(title="t", body="b"))
	assert r.success is False
	assert "PermissionError" in (r.error or "") or "denied" in (r.error or "")
