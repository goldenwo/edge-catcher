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


def test_pending_states_projected_from_store_after_seeding():
	"""Documents the projection pattern Task 10 adopts inside replay_capture.
	Passes in isolation — it tests the expression, not the wire-up. The
	end-to-end guard for the wire-up is the next test."""
	from types import SimpleNamespace

	store = InMemoryTradeStore()
	store.save_state("strat-a", {"seen:FOO": True, "counter": 5})
	store.save_state("strat-c", {"unused": "disabled-strat-should-not-leak"})

	strategies = [SimpleNamespace(name="strat-a"), SimpleNamespace(name="strat-b")]
	pending_states = {s.name: store.load_state(s.name) for s in strategies}

	assert pending_states == {
		"strat-a": {"seen:FOO": True, "counter": 5},
		"strat-b": {},
	}
	# strat-c's state is in the store but NOT in pending_states
	assert "strat-c" not in pending_states


def test_replay_capture_seeds_and_flushes_strategy_state(tmp_path):
	"""End-to-end wire-up test — the guard for Task 10's replay_capture edits.

	Builds a minimal bundle (manifest + empty JSONL) with a prior-day
	strategy_state snapshot, calls replay_capture with override strategies
	and config, and asserts the seeded state round-trips through the store.

	FAILS if any of these is missing from replay_capture:
	  - _seed_strategy_state(store, bundle, prior_bundle) call
	  - pending_states projection change (from {name: {}} to {name: store.load_state(name)})
	  - ReplayResult.store field
	  - end-of-replay flush loop
	"""
	from edge_catcher.monitors.replay.backtester import replay_capture
	from edge_catcher.monitors.strategy_base import PaperStrategy

	class NoopStrategy(PaperStrategy):
		name = "counter-strat"
		supported_series = ["KXTEST"]
		default_params: dict = {}

		def on_tick(self, ctx):
			return []

	# Prior bundle: seed counter=5
	prior = tmp_path / "2026-04-14"
	prior.mkdir()
	_write_envelope(prior / "strategy_state_at_start.json", {
		"counter-strat": {"counter": 5},
	})

	# Current bundle: minimal manifest + empty JSONL (no events to dispatch)
	bundle = tmp_path / "2026-04-15"
	bundle.mkdir()
	(bundle / "manifest.json").write_text(
		json.dumps({
			"schema_version": 1,
			"capture_date": "2026-04-15",
			"engine_commit": "test",
			"engine_dirty": False,
		}),
		encoding="utf-8",
	)
	(bundle / "kalshi_engine_2026-04-15.jsonl").write_text("", encoding="utf-8")

	config = {"strategies": {"counter-strat": {"series": ["KXTEST"]}}}

	result = replay_capture(
		bundle_path=bundle,
		strategies=[NoopStrategy()],
		config=config,
		prior_bundle=prior,
	)

	# Full chain: _seed_strategy_state populated the store from the prior
	# snapshot → projection put counter=5 into pending_states → no events
	# dispatched → end-of-replay flush saved pending_states back to store.
	# The final store state reflects the seeded value.
	assert result.store is not None, (
		"ReplayResult.store field missing — Task 10 Edit 1 incomplete"
	)
	final = result.store.load_all_states()
	assert final == {"counter-strat": {"counter": 5}}, (
		f"expected seeded state to round-trip through replay_capture, "
		f"got {final}. If empty, either _seed_strategy_state was not "
		f"called (Edit 2 missing) or the projection / flush edits are "
		f"incomplete (Edits 3/4)."
	)
