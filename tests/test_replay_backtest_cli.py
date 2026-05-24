"""Tests for the replay-backtest CLI's error-envelope contract.

`replay-backtest --json` promises STDOUT is always a single JSON object:
`{"status":"ok",...}` on success or `{"status":"error","message":...}` on
failure. The bundle-not-found case already honored this, but any exception
raised by ``replay_capture`` (corrupt/missing JSONL, bad strategies_local.py,
unsupported manifest schema_version, reordered recv_seq) previously escaped as
an uncaught traceback — breaking the contract for downstream tooling that parses
stdout. These tests lock the envelope for ALL failure modes.
"""
from __future__ import annotations

import argparse
import json
from types import SimpleNamespace

import pytest

from edge_catcher.cli import replay_backtest


def _parse(argv: list[str]) -> argparse.Namespace:
	"""Build args exactly as the real CLI does (real defaults + func binding)."""
	parser = argparse.ArgumentParser()
	sub = parser.add_subparsers(dest="command")
	replay_backtest.register(sub)
	return parser.parse_args(argv)


def _write_bundle_with_schema(tmp_path, schema_version: int):
	"""Create a bundle dir whose manifest has the given schema_version.

	A value other than 1 makes ``replay_capture`` raise ``ValueError`` right
	after loading the manifest — a real failure mode reached through the real
	code path (no monkeypatching)."""
	bundle = tmp_path / "2026-04-15"
	bundle.mkdir()
	(bundle / "manifest.json").write_text(
		json.dumps({"schema_version": schema_version, "capture_date": bundle.name}),
		encoding="utf-8",
	)
	return bundle


@pytest.mark.parametrize("json_flag", [True, False])
def test_replay_capture_exception_emits_error_envelope(tmp_path, capsys, json_flag):
	"""A real replay_capture failure (unsupported manifest schema_version) must
	surface as the {"status":"error",...} envelope on stdout + exit 1 — NOT an
	uncaught ValueError traceback. Holds in both --json and human-readable modes
	(matching the existing bundle-not-found path, which prints the envelope
	unconditionally)."""
	bundle = _write_bundle_with_schema(tmp_path, schema_version=999)

	argv = ["replay-backtest", "--bundle", str(bundle)]
	if json_flag:
		argv.append("--json")
	args = _parse(argv)

	with pytest.raises(SystemExit) as exc:
		replay_backtest.run(args)
	assert exc.value.code == 1

	out = capsys.readouterr().out
	envelope = json.loads(out)  # stdout MUST be parseable JSON, not a traceback
	assert envelope["status"] == "error"
	assert "schema_version" in envelope["message"]


def test_arbitrary_exception_type_is_caught(tmp_path, capsys, monkeypatch):
	"""The handler is a type-agnostic catch-all: any Exception from
	replay_capture (here a RuntimeError standing in for BundleStrategyLoadError /
	FileNotFoundError / the loader's reordered-recv_seq ValueError) becomes the
	error envelope, not a traceback."""
	bundle = tmp_path / "2026-04-15"
	bundle.mkdir()

	async def _boom(*args, **kwargs):
		raise RuntimeError("synthetic replay failure")

	monkeypatch.setattr(
		"edge_catcher.engine.replay.backtester.replay_capture", _boom
	)

	args = _parse(["replay-backtest", "--bundle", str(bundle), "--json"])
	with pytest.raises(SystemExit) as exc:
		replay_backtest.run(args)
	assert exc.value.code == 1

	envelope = json.loads(capsys.readouterr().out)
	assert envelope["status"] == "error"
	assert "synthetic replay failure" in envelope["message"]


def test_success_path_unaffected(tmp_path, capsys, monkeypatch):
	"""Regression guard: wrapping replay_capture in try/except must not disturb
	the success path — a normal result still prints the status=ok envelope and
	does NOT sys.exit."""
	bundle = tmp_path / "2026-04-15"
	bundle.mkdir()

	fake = SimpleNamespace(
		events_processed=3,
		duration_seconds=1.2345,
		capture_start_ts="2026-04-15T00:00:00+00:00",
		capture_end_ts="2026-04-15T23:59:59+00:00",
		strategies_loaded=["demo-strat"],
		trades=[{"strategy": "demo-strat", "ticker": "KXTEST-FOO", "side": "yes"}],
	)

	async def _ok(*args, **kwargs):
		return fake

	monkeypatch.setattr(
		"edge_catcher.engine.replay.backtester.replay_capture", _ok
	)

	args = _parse(["replay-backtest", "--bundle", str(bundle), "--json"])
	replay_backtest.run(args)  # must NOT raise SystemExit

	envelope = json.loads(capsys.readouterr().out)
	assert envelope["status"] == "ok"
	assert envelope["trade_count"] == 1
	assert envelope["events_processed"] == 3
