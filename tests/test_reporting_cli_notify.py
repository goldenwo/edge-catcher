"""Tests for the reporting CLI's --notify flag.

In-process testing: most tests call main(argv=[...]) directly so we can
monkeypatch deps and use capsys without the cost+flake of subprocess.
One subprocess test verifies the package entry point still works
end-to-end ('python -m edge_catcher.reporting').

A 'fail' channel is registered in _TYPE_TO_CLASS via monkeypatch for
the all-fail / mixed-success cases — avoids real network calls per the
project's "fully mocked" rule.
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

import pytest

from edge_catcher.notifications import loader
from edge_catcher.notifications.envelope import DeliveryResult, Notification

FIXTURE_DB = (
	Path(__file__).parent.parent
	/ "edge_catcher" / "data" / "examples" / "paper_trades_demo.db"
)


# --- Test-only failing channel, registered into _TYPE_TO_CLASS via monkeypatch.

class _AlwaysFailChannel:
	"""Test-only adapter that always returns DeliveryResult(success=False)."""

	def __init__(self, name: str) -> None:
		self.name = name

	def send(self, notification: Notification) -> DeliveryResult:
		return DeliveryResult(
			channel_name=self.name,
			success=False,
			error="test fixture: always fails",
			latency_ms=0.1,
		)


@pytest.fixture
def register_fail_channel(monkeypatch):
	"""Register a 'fail' channel type in the loader registry for the test session."""
	monkeypatch.setitem(loader._TYPE_TO_CLASS, "fail", _AlwaysFailChannel)
	monkeypatch.setitem(loader._REQUIRED_FIELDS, "fail", set())
	monkeypatch.setitem(loader._OPTIONAL_FIELDS, "fail", set())


# --- In-process tests (preferred for speed + mockability)

def test_no_notify_flag_unchanged_behavior(capsys):
	from edge_catcher.reporting.__main__ import main
	rc = main(["--db", str(FIXTURE_DB)])
	captured = capsys.readouterr()
	assert rc == 0
	# Existing CLI uses json.dumps(report, indent=2, default=str). Lock both.
	data = json.loads(captured.out)
	assert "all_time" in data
	# indent=2 means non-flat output — at least one line starts with two spaces
	# under a key. Cheap proof that indent isn't 0/None:
	assert "\n  " in captured.out


def test_notify_stdout_channel_succeeds(tmp_path, capsys):
	cfg = tmp_path / "n.yaml"
	cfg.write_text("channels:\n  console:\n    type: stdout\n", encoding="utf-8")
	from edge_catcher.reporting.__main__ import main
	rc = main(["--db", str(FIXTURE_DB), "--notify-config", str(cfg), "--notify", "console"])
	captured = capsys.readouterr()
	assert rc == 0
	# Stderr has the results table.
	assert "console" in captured.err
	assert "OK" in captured.err
	# Without --quiet, the JSON dump is still on stdout (backward compat).
	# (StdoutChannel ALSO writes to stdout, so we can't simply parse the whole
	# thing as JSON. Find the JSON object boundaries.)
	# Crude split: last line that starts with "}" is the JSON closing brace.
	assert '"all_time"' in captured.out


def test_quiet_suppresses_json_dump_on_stdout(tmp_path, capsys):
	cfg = tmp_path / "n.yaml"
	cfg.write_text("channels:\n  console:\n    type: stdout\n", encoding="utf-8")
	from edge_catcher.reporting.__main__ import main
	rc = main([
		"--db", str(FIXTURE_DB), "--notify-config", str(cfg),
		"--notify", "console", "--quiet",
	])
	captured = capsys.readouterr()
	assert rc == 0
	# StdoutChannel still wrote its formatted text to stdout, but the
	# reporting module's JSON dump did not. Check for the JSON-only marker
	# `"all_time": {` (with the indent-2 leading two spaces) — present
	# without --quiet, absent with it.
	assert '"all_time": {' not in captured.out


def test_unknown_channel_name_exits_2(tmp_path, capsys):
	cfg = tmp_path / "n.yaml"
	cfg.write_text("channels:\n  console:\n    type: stdout\n", encoding="utf-8")
	from edge_catcher.reporting.__main__ import main
	rc = main([
		"--db", str(FIXTURE_DB), "--notify-config", str(cfg),
		"--notify", "missing_name",
	])
	captured = capsys.readouterr()
	assert rc == 2
	assert "missing_name" in captured.err.lower() or "unknown" in captured.err.lower()


def test_missing_config_file_exits_2(tmp_path, capsys):
	from edge_catcher.reporting.__main__ import main
	rc = main([
		"--db", str(FIXTURE_DB),
		"--notify-config", str(tmp_path / "does_not_exist.yaml"),
		"--notify", "anything",
	])
	captured = capsys.readouterr()
	assert rc == 2
	assert "config" in captured.err.lower() or "not found" in captured.err.lower()


def test_all_channels_failing_exits_1(tmp_path, capsys, register_fail_channel):
	cfg = tmp_path / "n.yaml"
	cfg.write_text("channels:\n  dead:\n    type: fail\n", encoding="utf-8")
	from edge_catcher.reporting.__main__ import main
	rc = main([
		"--db", str(FIXTURE_DB), "--notify-config", str(cfg),
		"--notify", "dead",
	])
	captured = capsys.readouterr()
	assert rc == 1
	assert "dead" in captured.err
	assert "FAIL" in captured.err


def test_mixed_success_exits_0(tmp_path, capsys, register_fail_channel):
	cfg = tmp_path / "n.yaml"
	cfg.write_text(
		"channels:\n"
		"  console:\n    type: stdout\n"
		"  dead:\n    type: fail\n",
		encoding="utf-8",
	)
	from edge_catcher.reporting.__main__ import main
	rc = main([
		"--db", str(FIXTURE_DB), "--notify-config", str(cfg),
		"--notify", "console", "--notify", "dead",
	])
	captured = capsys.readouterr()
	assert rc == 0
	# Both rendered in stderr table.
	assert "console" in captured.err
	assert "dead" in captured.err


def test_stderr_table_format(tmp_path, capsys):
	"""Lock the documented column shape via regex."""
	cfg = tmp_path / "n.yaml"
	cfg.write_text("channels:\n  console:\n    type: stdout\n", encoding="utf-8")
	from edge_catcher.reporting.__main__ import main
	main([
		"--db", str(FIXTURE_DB), "--notify-config", str(cfg),
		"--notify", "console", "--quiet",
	])
	captured = capsys.readouterr()
	# Find the row with our channel name. Format is:
	#   <name padded to 20+ chars> <status (OK|FAIL) padded to 7 chars> <tail>
	# where tail is "<int>ms" on success and an error string on failure.
	row = next(
		line for line in captured.err.splitlines() if "console" in line and "OK" in line
	)
	# Lock 3-column structure with at least one whitespace between columns.
	match = re.match(r"^(?P<name>console\s*)\s+(?P<status>OK)\s+(?P<tail>\S+)$", row)
	assert match is not None, f"row did not match expected format: {row!r}"
	# Channel name column is padded to >= 20 chars (with trailing spaces or not).
	assert len(match.group("name").rstrip()) == len("console")
	# Tail must end in 'ms' (latency for success).
	assert match.group("tail").endswith("ms")


def test_table_header_present(tmp_path, capsys):
	"""Header row 'channel  status  latency' must precede the data rows."""
	cfg = tmp_path / "n.yaml"
	cfg.write_text("channels:\n  console:\n    type: stdout\n", encoding="utf-8")
	from edge_catcher.reporting.__main__ import main
	main([
		"--db", str(FIXTURE_DB), "--notify-config", str(cfg),
		"--notify", "console", "--quiet",
	])
	captured = capsys.readouterr()
	stderr_lines = captured.err.splitlines()
	# First non-empty line is the header.
	non_empty = [line for line in stderr_lines if line.strip()]
	assert "channel" in non_empty[0]
	assert "status" in non_empty[0]
	assert "latency" in non_empty[0]


def test_quiet_without_notify_warns(capsys):
	"""--quiet without --notify is a footgun — warn the user."""
	from edge_catcher.reporting.__main__ import main
	rc = main(["--db", str(FIXTURE_DB), "--quiet"])
	captured = capsys.readouterr()
	assert rc == 0
	# JSON still goes to stdout (--quiet has no effect without --notify).
	assert '"all_time"' in captured.out
	# But user gets warned about the no-op flag.
	assert "warning" in captured.err.lower()
	assert "--quiet" in captured.err


# --- Subprocess test: verify the package entry point still works.

def test_subprocess_entry_point_smoke():
	"""One subprocess invocation to verify `python -m edge_catcher.reporting` works.

	All other tests run in-process for speed and mockability — this is the
	lone end-to-end check that the entry-point dispatch isn't broken.
	"""
	r = subprocess.run(
		[sys.executable, "-m", "edge_catcher.reporting", "--db", str(FIXTURE_DB)],
		capture_output=True, text=True,
	)
	assert r.returncode == 0
	data = json.loads(r.stdout)
	assert "all_time" in data
