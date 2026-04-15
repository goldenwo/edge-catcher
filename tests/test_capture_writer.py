"""Tests for RawFrameWriter — the capture pipeline's append-only JSONL writer.

The highest-priority invariant is the exception-swallowing guarantee: the
writer must NEVER let an exception propagate into the engine message loop.
If this test regresses, the capture pipeline becomes a live-trading bug.
"""
from __future__ import annotations

import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from edge_catcher.monitors.capture.writer import (
	CaptureLockError,
	RawFrameWriter,
)


def _today_jsonl(tmp_path: Path) -> Path:
	return tmp_path / f"kalshi_engine_{datetime.now(timezone.utc).date().isoformat()}.jsonl"


def _read_events(tmp_path: Path) -> list[dict]:
	"""Return all non-header events from today's JSONL."""
	path = _today_jsonl(tmp_path)
	if not path.exists():
		return []
	events = []
	for line in path.read_text(encoding="utf-8").splitlines():
		if not line.strip():
			continue
		obj = json.loads(line)
		if obj.get("header"):
			continue
		events.append(obj)
	return events


# ---------------------------------------------------------------------------
# 1. Basic write behavior
# ---------------------------------------------------------------------------

def test_write_ws_appends_jsonl_line(tmp_path: Path) -> None:
	"""write_ws writes exactly one event line after the schema header."""
	writer = RawFrameWriter(tmp_path, enabled=True)
	try:
		writer.write_ws({"type": "orderbook_delta", "msg": {"market_ticker": "KXTEST"}})
	finally:
		writer.close()

	path = _today_jsonl(tmp_path)
	lines = path.read_text(encoding="utf-8").strip().split("\n")
	assert len(lines) == 2  # header + 1 event
	header = json.loads(lines[0])
	assert header["header"] is True
	assert header["schema_version"] == 1
	assert header["exchange"] == "kalshi"
	event = json.loads(lines[1])
	assert event["source"] == "ws"
	assert event["recv_seq"] == 1
	assert event["payload"]["msg"]["market_ticker"] == "KXTEST"
	# recv_ts must be an ISO string with timezone info
	ts = datetime.fromisoformat(event["recv_ts"])
	assert ts.tzinfo is not None


def test_write_synthetic_uses_prefixed_source(tmp_path: Path) -> None:
	"""write_synthetic tags the source as 'synthetic.<kind>'."""
	writer = RawFrameWriter(tmp_path, enabled=True)
	try:
		writer.write_synthetic("rest_orderbook", {"ticker": "KXTEST"})
		writer.write_synthetic("ticker_discovered", {"ticker": "KXTEST"})
		writer.write_synthetic("settlement", {"strategy": "s1"})
	finally:
		writer.close()

	events = _read_events(tmp_path)
	sources = [e["source"] for e in events]
	assert sources == [
		"synthetic.rest_orderbook",
		"synthetic.ticker_discovered",
		"synthetic.settlement",
	]


def test_recv_seq_monotonic_across_writes(tmp_path: Path) -> None:
	"""recv_seq increments 1, 2, 3, ... within a single writer instance."""
	writer = RawFrameWriter(tmp_path, enabled=True)
	try:
		for i in range(10):
			writer.write_ws({"type": "test", "i": i})
	finally:
		writer.close()

	events = _read_events(tmp_path)
	seqs = [e["recv_seq"] for e in events]
	assert seqs == list(range(1, 11))


# ---------------------------------------------------------------------------
# 2. NEVER-RAISES invariant — non-negotiable
#
# Any code path that lets an exception escape from write_ws/write_synthetic
# into the engine loop is the highest-priority bug in the codebase.
# See plan review Fix #6 — we test multiple failure modes, not just one.
# ---------------------------------------------------------------------------

def test_write_ws_never_raises_on_closed_file(tmp_path: Path) -> None:
	"""If the underlying file has been closed out from under the writer,
	write_ws must log-and-return, not raise."""
	writer = RawFrameWriter(tmp_path, enabled=True)
	try:
		# Force a first write so _active_file exists
		writer.write_ws({"type": "test", "warmup": True})
		writer._active_file.close()
		# These must NOT raise
		writer.write_ws({"type": "test", "after_close": True})
		writer.write_synthetic("settlement", {"strategy": "x"})
	finally:
		writer.close()


def test_write_ws_never_raises_on_unserializable_payload(tmp_path: Path) -> None:
	"""A payload that can't be JSON-serialized (e.g. contains a set) must
	not crash the writer."""
	writer = RawFrameWriter(tmp_path, enabled=True)
	try:
		bad = {"type": "test", "weird": {1, 2, 3}}  # set is not JSON-serializable
		writer.write_ws(bad)  # must not raise
		# Subsequent writes should still work with a good payload
		writer.write_ws({"type": "test", "ok": True})
	finally:
		writer.close()
	events = _read_events(tmp_path)
	# Only the good write should have been persisted
	assert any(e["payload"].get("ok") for e in events)


def test_write_ws_never_raises_on_sidecar_flush_failure(tmp_path: Path) -> None:
	"""_flush_seq failing (e.g. permission denied on sidecar) must not
	propagate — it's a best-effort checkpoint, not a correctness gate."""
	writer = RawFrameWriter(tmp_path, enabled=True, seq_flush_every=1)
	original_flush = writer._flush_seq

	def boom() -> None:
		raise OSError("sidecar boom")

	try:
		writer._flush_seq = boom  # type: ignore[method-assign]
		writer.write_ws({"type": "test"})  # triggers _flush_seq internally; must not raise
	finally:
		writer._flush_seq = original_flush  # type: ignore[method-assign]
		writer.close()


def test_write_ws_never_raises_when_stopped(tmp_path: Path) -> None:
	"""After disk-pressure stop, subsequent writes are no-ops, not errors."""
	writer = RawFrameWriter(tmp_path, enabled=True)
	try:
		writer._stopped = True  # simulate disk pressure stop
		writer.write_ws({"type": "test"})  # no-op
		writer.write_synthetic("settlement", {})  # no-op
	finally:
		writer.close()
	# Nothing persisted beyond the header (if any)
	events = _read_events(tmp_path)
	assert events == []


# ---------------------------------------------------------------------------
# 3. recv_seq sidecar recovery
# ---------------------------------------------------------------------------

def test_recv_seq_recovers_from_corrupted_sidecar(tmp_path: Path) -> None:
	"""A corrupted .recv_seq sidecar triggers tail-scan recovery and the
	next write gets a recv_seq strictly greater than any persisted one."""
	writer = RawFrameWriter(tmp_path, enabled=True, seq_flush_every=2)
	try:
		for _ in range(5):
			writer.write_ws({"type": "test"})
	finally:
		writer.close()

	# Corrupt the sidecar
	(tmp_path / ".recv_seq").write_text("not a number")

	# Reopen — should recover by scanning the active JSONL tail
	writer2 = RawFrameWriter(tmp_path, enabled=True)
	try:
		writer2.write_ws({"type": "test", "after": "recovery"})
	finally:
		writer2.close()

	events = _read_events(tmp_path)
	seqs = [e["recv_seq"] for e in events]
	# All pre-recovery seqs should be 1..5, post-recovery seq should be 6
	assert seqs == [1, 2, 3, 4, 5, 6]


def test_recv_seq_sidecar_ahead_of_jsonl_tail(tmp_path: Path) -> None:
	"""When the sidecar holds a value higher than the tail scan (because
	writes were flushed to the sidecar but not yet to the file, or the
	file was truncated), the writer trusts the higher value to avoid
	reusing a recv_seq."""
	# Seed a JSONL with a low max seq and a sidecar with a higher value
	today_path = _today_jsonl(tmp_path)
	today_path.parent.mkdir(parents=True, exist_ok=True)
	today_path.write_text(
		json.dumps({"schema_version": 1, "exchange": "kalshi", "header": True}) + "\n"
		+ json.dumps({"recv_seq": 3, "source": "ws", "payload": {}}) + "\n"
	)
	(tmp_path / ".recv_seq").write_text("100")

	writer = RawFrameWriter(tmp_path, enabled=True)
	try:
		writer.write_ws({"type": "test"})
	finally:
		writer.close()

	events = _read_events(tmp_path)
	# The final event should have recv_seq=101 (sidecar+1), not 4 (tail+1)
	assert events[-1]["recv_seq"] == 101


# ---------------------------------------------------------------------------
# 4. Disk pressure
# ---------------------------------------------------------------------------

def test_disk_pressure_stops_writer(tmp_path: Path) -> None:
	"""When free disk drops below min_free_gb, the writer permanently stops."""
	writer = RawFrameWriter(tmp_path, enabled=True, min_free_gb=10)
	try:
		with patch.object(writer, "_free_disk_bytes", return_value=5 * (1024 ** 3)):
			# Force the periodic disk check
			writer._writes_since_disk_check = 999
			writer.write_ws({"type": "test", "before_stop": True})
			assert writer._stopped is True
		# Post-stop writes are no-ops even after the patch is lifted
		writer.write_ws({"type": "test", "after_stop": True})
	finally:
		writer.close()

	events = _read_events(tmp_path)
	# 'before_stop' event was written BEFORE _maybe_check_disk set _stopped,
	# but the check happens BEFORE the seq increment and write. So the
	# first write after _writes_since_disk_check hits the threshold is
	# actually suppressed. Verify no after_stop payload was persisted.
	assert not any(e["payload"].get("after_stop") for e in events)


def test_disk_pressure_survives_enabled_toggle(tmp_path: Path) -> None:
	"""A stopped writer stays stopped — you must restart the paper-trader."""
	writer = RawFrameWriter(tmp_path, enabled=True)
	try:
		writer._stopped = True
		writer.write_ws({"type": "test"})
		assert writer._stopped is True
	finally:
		writer.close()


# ---------------------------------------------------------------------------
# 5. POSIX flock (skipped on Windows)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(sys.platform == "win32", reason="fcntl is POSIX-only")
def test_two_writers_contend_on_flock(tmp_path: Path) -> None:
	"""A second writer against the same directory raises CaptureLockError.
	After the first writer releases, a third writer can acquire."""
	writer1 = RawFrameWriter(tmp_path, enabled=True)
	try:
		with pytest.raises(CaptureLockError):
			RawFrameWriter(tmp_path, enabled=True)
	finally:
		writer1.close()

	# After release, a new writer should be able to acquire
	writer2 = RawFrameWriter(tmp_path, enabled=True)
	writer2.close()


# ---------------------------------------------------------------------------
# 6. Disabled-flag
# ---------------------------------------------------------------------------

def test_disabled_writer_is_noop(tmp_path: Path) -> None:
	"""When enabled=False, writes are no-ops and no files are created."""
	writer = RawFrameWriter(tmp_path, enabled=False)
	try:
		writer.write_ws({"type": "test"})
		writer.write_synthetic("settlement", {})
	finally:
		writer.close()

	# No JSONL files should exist
	files = list(tmp_path.glob("kalshi_engine_*.jsonl"))
	assert files == []


def test_disabled_writer_does_not_create_output_dir(tmp_path: Path) -> None:
	"""A disabled writer must not even create the output directory — so
	the operator can tell from 'directory exists' whether capture is live."""
	subdir = tmp_path / "capture_disabled"
	writer = RawFrameWriter(subdir, enabled=False)
	writer.close()
	assert not subdir.exists()


# ---------------------------------------------------------------------------
# 7. Rotation
# ---------------------------------------------------------------------------

def test_rotation_callback_fires_synchronously_with_old_day(tmp_path: Path) -> None:
	"""A rotation_callback registered at construction time fires SYNCHRONOUSLY
	on every rotation with the old (just-closed) day as its sole argument.

	Sync invocation is a deliberate design choice — the callback needs to
	snapshot live engine state (e.g. deepcopy market_state) on the engine
	thread to avoid races. The callback is responsible for backgrounding
	any slow work itself.
	"""
	seen: list[date] = []

	def cb(old_day: date) -> None:
		seen.append(old_day)

	writer = RawFrameWriter(tmp_path, enabled=True, rotation_callback=cb)
	try:
		writer.write_ws({"type": "test"})  # seed today's file
		old_day = writer._active_date
		writer._rotate(date(2099, 1, 1))
		# Sync contract: callback has fired by the time _rotate returns
		assert seen == [old_day]
	finally:
		writer.close()


def test_rotation_callback_exception_is_logged_not_propagated(tmp_path: Path) -> None:
	"""If the rotation_callback raises, the error is logged and swallowed —
	the engine loop must never be affected by a bundle-assembly failure."""
	def boom(old_day: date) -> None:
		raise RuntimeError("bundle assembly failed for real reasons")

	writer = RawFrameWriter(tmp_path, enabled=True, rotation_callback=boom)
	try:
		writer.write_ws({"type": "test"})
		# Must not raise — the writer swallows callback exceptions.
		writer._rotate(date(2099, 1, 1))
	finally:
		writer.close()


def test_rotation_opens_new_file_with_header(tmp_path: Path) -> None:
	"""Directly invoking _rotate(new_day) closes the old file and opens a new
	one for new_day with a fresh schema header. We don't write to the new file
	after rotating because _maybe_rotate on the next write will snap _active_date
	back to the real UTC today — so we verify rotation at the file-creation level
	and test writes to a rotated-away day separately if ever needed."""
	writer = RawFrameWriter(tmp_path, enabled=True)
	try:
		# Seed the writer by writing one event to today's file
		writer.write_ws({"type": "test", "before": True})
		old_day = writer._active_date
		assert old_day is not None
		old_file = tmp_path / f"kalshi_engine_{old_day.isoformat()}.jsonl"

		# Rotate to a future date (picked far enough that no real soak collision)
		new_day = date(2099, 1, 1)
		writer._rotate(new_day)
		assert writer._active_date == new_day
		assert writer._active_file is not None  # a new file was opened
	finally:
		writer.close()

	# Old file has the pre-rotation event
	assert old_file.exists()
	old_events = [
		json.loads(line) for line in old_file.read_text(encoding="utf-8").splitlines()
		if line.strip() and not json.loads(line).get("header")
	]
	assert any(e["payload"].get("before") for e in old_events)

	# New file exists with its own schema header (and only the header — no events)
	new_file = tmp_path / "kalshi_engine_2099-01-01.jsonl"
	assert new_file.exists()
	new_lines = new_file.read_text(encoding="utf-8").strip().split("\n")
	new_header = json.loads(new_lines[0])
	assert new_header["header"] is True
	assert new_header["schema_version"] == 1
