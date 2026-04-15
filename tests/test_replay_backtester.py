"""Unit tests for edge_catcher.monitors.replay.backtester helpers.

Targets the helper functions directly — parity test coverage lives in
test_replay_parity.py and requires a real bundle fixture via env var.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from edge_catcher.monitors.replay.backtester import _seed_strategy_state
from edge_catcher.monitors.trade_store import InMemoryTradeStore


def _write_envelope(path: Path, states: dict) -> None:
	"""Helper: write a valid strategy_state_at_start.json envelope."""
	envelope = {
		"schema_version": 1,
		"captured_at": datetime.now(timezone.utc).isoformat(),
		"states": states,
	}
	path.write_text(json.dumps(envelope, sort_keys=True, indent=2), encoding="utf-8")


def test_seed_strategy_state_from_prior_bundle(tmp_path):
	"""Explicit prior_bundle resolution: helper loads envelope into store."""
	prior = tmp_path / "2026-04-14"
	prior.mkdir()
	bundle = tmp_path / "2026-04-15"
	bundle.mkdir()
	_write_envelope(prior / "strategy_state_at_start.json", {
		"strat-a": {"seen:FOO": True, "counter": 5},
		"strat-b": {"entered:BAR": 1},
	})

	store = InMemoryTradeStore()
	_seed_strategy_state(store, bundle, prior_bundle=prior)

	assert store.load_all_states() == {
		"strat-a": {"seen:FOO": True, "counter": 5},
		"strat-b": {"entered:BAR": 1},
	}


def test_seed_strategy_state_missing_file(tmp_path, caplog):
	"""Prior resolution returns None: no-op, info log, store untouched."""
	import logging

	bundle = tmp_path / "2026-04-15"
	bundle.mkdir()
	# No prior dir, no sibling → _resolve_prior_file returns None

	store = InMemoryTradeStore()
	with caplog.at_level(logging.INFO):
		_seed_strategy_state(store, bundle, prior_bundle=None)

	assert store.load_all_states() == {}
	assert "no prior strategy_state" in caplog.text


def test_seed_strategy_state_schema_version_mismatch(tmp_path):
	"""Envelope with schema_version != 1 raises ValueError naming the version."""
	prior = tmp_path / "2026-04-14"
	prior.mkdir()
	bundle = tmp_path / "2026-04-15"
	bundle.mkdir()
	(prior / "strategy_state_at_start.json").write_text(
		json.dumps({"schema_version": 999, "captured_at": "x", "states": {}}),
		encoding="utf-8",
	)

	store = InMemoryTradeStore()
	with pytest.raises(ValueError, match="999"):
		_seed_strategy_state(store, bundle, prior_bundle=prior)


def test_seed_strategy_state_malformed_json(tmp_path):
	"""Malformed JSON file propagates JSONDecodeError (fail loud)."""
	prior = tmp_path / "2026-04-14"
	prior.mkdir()
	bundle = tmp_path / "2026-04-15"
	bundle.mkdir()
	(prior / "strategy_state_at_start.json").write_text("{not-json", encoding="utf-8")

	store = InMemoryTradeStore()
	with pytest.raises(json.JSONDecodeError):
		_seed_strategy_state(store, bundle, prior_bundle=prior)
