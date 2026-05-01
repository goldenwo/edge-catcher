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


def test_seeded_state_round_trips_through_replay_path(tmp_path):
	"""THE behavioral test. Proves the entire chain:
	_seed_strategy_state → store → projection into pending_states →
	process_tick mutates the projection → end-of-replay flush → store
	reflects the final state. Uses process_tick directly to avoid
	fighting dispatch.py's WS message parsing and market_state priming.
	"""
	from datetime import datetime, timezone
	from edge_catcher.monitors.replay.backtester import _seed_strategy_state
	from edge_catcher.monitors.market_state import OrderbookSnapshot, TickContext
	from edge_catcher.monitors.dispatch import process_tick
	from edge_catcher.monitors.strategy_base import PaperStrategy

	class CounterStrategy(PaperStrategy):
		name = "counter-strat"
		supported_series = ["KXTEST"]
		default_params: dict = {}

		def on_tick(self, ctx):
			# Mutate persisted_state IN PLACE — this is exactly what
			# real strategies do. The mutation must land in the dict
			# that pending_states[name] points at.
			ctx.persisted_state["counter"] = ctx.persisted_state.get("counter", 0) + 1
			return []

	# --- Stage 1: prior bundle with seeded counter=5 ---
	prior = tmp_path / "2026-04-14"
	prior.mkdir()
	bundle = tmp_path / "2026-04-15"
	bundle.mkdir()
	_write_envelope(prior / "strategy_state_at_start.json", {
		"counter-strat": {"counter": 5},
	})

	# --- Stage 2: seed the store via the new helper ---
	store = InMemoryTradeStore()
	_seed_strategy_state(store, bundle, prior_bundle=prior)
	assert store.load_state("counter-strat") == {"counter": 5}

	# --- Stage 3: build pending_states via the Task 10 projection ---
	strat = CounterStrategy()
	strategies = [strat]
	pending_states = {s.name: store.load_state(s.name) for s in strategies}
	# The projection copies out of the store, so pending_states["counter-strat"]
	# is a NEW dict equal to {"counter": 5}. Mutations to it do not touch
	# store._strategy_state until the end-of-replay flush fires.
	assert pending_states["counter-strat"] == {"counter": 5}

	# --- Stage 4: call process_tick with persisted_state pointing at the
	#              projection dict — this is exactly what dispatch.py:462
	#              and :541 do in the real path (pending_states.get(name, {})
	#              returns the dict at that key by identity when present). ---
	ctx = TickContext(
		ticker="KXTEST-FOO",
		event_ticker="KXTEST-FOO",
		yes_bid=49, yes_ask=51, no_bid=49, no_ask=51,
		orderbook=OrderbookSnapshot([], []),
		price_history=[],
		open_positions=[],
		persisted_state=pending_states["counter-strat"],
		market_metadata={},
		series="KXTEST",
		is_first_observation=True,
	)
	now = datetime.now(timezone.utc)
	process_tick(ctx, [strat], store, config={}, now=now)

	# The mutation lands in pending_states because persisted_state
	# and pending_states["counter-strat"] are the same dict object.
	assert pending_states["counter-strat"] == {"counter": 6}
	# But the store is still at 5 — nothing has flushed yet.
	assert store.load_state("counter-strat") == {"counter": 5}

	# --- Stage 5: end-of-replay flush (what Task 10 adds to replay_capture) ---
	for s in strategies:
		state = pending_states.get(s.name)
		if state is not None:
			store.save_state(s.name, state)

	# --- Stage 6: the store now reflects the mutation ---
	assert store.load_state("counter-strat") == {"counter": 6}


# ---------------------------------------------------------------------------
# Test 1.b — _seed_market_state must derive is_first_observation correctly
# under v2 (carries first_seen), legacy (no schema_version), and v1 (explicit
# old version) envelopes. Verified BOTH at the strategy contract layer
# (dispatch_message → ctx.is_first_observation) AND directly via
# update_price's return value (defense-in-depth).
#
# Per docs/superpowers/plans/replay-first-seen-fix.md §"Step 1 — write tests"
# (1.b). RED until Step 3 lands the reader change.
# ---------------------------------------------------------------------------


from edge_catcher.monitors.dispatch import dispatch_message  # noqa: E402
from edge_catcher.monitors.market_state import (  # noqa: E402
	MarketState,
	OrderbookSnapshot,
	TickContext,
)
from edge_catcher.monitors.replay.backtester import _seed_market_state  # noqa: E402
from edge_catcher.monitors.strategy_base import PaperStrategy  # noqa: E402
from edge_catcher.monitors.trade_store import TradeStore  # noqa: E402


class _CaptureStrategyB(PaperStrategy):
	"""Stub strategy that records every TickContext it sees."""

	name = "capture-b"
	supported_series = ["KXSEED", "KXNOTSEEN"]
	default_params: dict = {}

	def __init__(self) -> None:
		self.captured_contexts: list[TickContext] = []

	def on_tick(self, ctx: TickContext) -> list:
		self.captured_contexts.append(ctx)
		return []


def _trade_event_for(ticker: str) -> dict:
	return {
		"source": "ws",
		"payload": {
			"type": "trade",
			"msg": {
				"market_ticker": ticker,
				"yes_price": 0.50,
				"taker_side": "yes",
				"count": 1,
			},
		},
	}


def _ensure_dispatch_preconditions(ms: MarketState, ticker: str) -> None:
	"""`_handle_trade_msg` requires both a registered ticker (price_history
	is not None) AND a populated orderbook (yes_ask/yes_bid resolve).

	The point of test 1.b is what `is_first_observation` evaluates to — NOT
	whether the ticker happens to be present. So we explicitly satisfy the
	dispatch guards without touching `_first_seen` (`seed_orderbook` and
	`register_ticker` do not mutate `_first_seen`).
	"""
	ms.register_ticker(ticker, meta={"event_ticker": ticker.split("-")[0]})
	ms.seed_orderbook(
		ticker,
		OrderbookSnapshot(
			yes_levels=[(0.50, 100)],
			no_levels=[(0.48, 100)],
		),
	)


_ENVELOPES = {
	"v2": {
		"schema_version": 2,
		"first_seen": ["KXSEED-T1"],
		"orderbooks": {},
		"metadata": {},
	},
	"legacy_no_version": {
		"orderbooks": {"KXSEED-T1": {"yes_levels": [], "no_levels": []}},
		"metadata": {},
	},
	"v1_explicit": {
		"schema_version": 1,
		"orderbooks": {"KXSEED-T1": {"yes_levels": [], "no_levels": []}},
		"metadata": {},
	},
}


@pytest.mark.parametrize("schema_label,envelope", list(_ENVELOPES.items()))
def test_seed_market_state_derives_first_seen(
	schema_label: str, envelope: dict, tmp_path: Path
) -> None:
	"""For each envelope shape, `_seed_market_state` must produce a state
	such that:

	  - dispatching a `trade` event for "KXSEED-T1" → ctx.is_first_observation == False
	    (because the prior bundle already saw it; replay continuity is the
	    whole point of the seed).
	  - dispatching a `trade` event for "KXNOTSEEN-T1" → ctx.is_first_observation == True
	    (a genuinely new ticker for this replay day).

	Defense-in-depth: a second, independent MarketState seeded the same way
	must also report the matching booleans directly from `update_price`.
	"""
	prior = tmp_path / "2026-04-14"
	prior.mkdir()
	bundle = tmp_path / "2026-04-15"
	bundle.mkdir()
	(prior / "market_state_at_start.json").write_text(
		json.dumps(envelope), encoding="utf-8"
	)

	# --- Strategy-visible contract via dispatch_message ---
	ms = MarketState()
	_seed_market_state(ms, bundle, prior_bundle=prior)

	# Make the two tickers passable through dispatch's guards. This must NOT
	# mutate _first_seen (sanity-check that explicitly).
	_first_seen_before = set(ms._first_seen)  # noqa: SLF001
	_ensure_dispatch_preconditions(ms, "KXSEED-T1")
	_ensure_dispatch_preconditions(ms, "KXNOTSEEN-T1")
	assert ms._first_seen == _first_seen_before, (  # noqa: SLF001
		"register_ticker/seed_orderbook unexpectedly mutated _first_seen — "
		"that would invalidate this test's premise"
	)

	strat = _CaptureStrategyB()
	store = TradeStore(tmp_path / f"store_{schema_label}.db")
	try:
		call_args = dict(
			config={},
			market_state=ms,
			store=store,
			strategies=[strat],
			strat_by_series={"KXSEED": [strat], "KXNOTSEEN": [strat]},
			pending_states={},
			dirty=set(),
			now=datetime(2026, 4, 15, 12, 0, 0, tzinfo=timezone.utc),
		)

		dispatch_message(_trade_event_for("KXSEED-T1"), **call_args)
		assert len(strat.captured_contexts) == 1, (
			f"[{schema_label}] dispatch did not reach the strategy for KXSEED-T1"
		)
		assert strat.captured_contexts[-1].is_first_observation is False, (
			f"[{schema_label}] KXSEED-T1 was in the prior-bundle first_seen set "
			"(or implied by the legacy/v1 orderbook entry); the seeded MarketState "
			"must NOT report this as a first observation"
		)

		dispatch_message(_trade_event_for("KXNOTSEEN-T1"), **call_args)
		assert len(strat.captured_contexts) == 2, (
			f"[{schema_label}] dispatch did not reach the strategy for KXNOTSEEN-T1"
		)
		assert strat.captured_contexts[-1].is_first_observation is True, (
			f"[{schema_label}] KXNOTSEEN-T1 was NOT in the prior-bundle state; "
			"its first dispatched trade must be flagged as first observation"
		)
	finally:
		store.close()

	# --- Defense-in-depth: direct update_price on a fresh MarketState ---
	ms2 = MarketState()
	_seed_market_state(ms2, bundle, prior_bundle=prior)
	assert ms2.update_price("KXSEED-T1", 50) is False, (
		f"[{schema_label}] update_price for the seeded ticker must return False"
	)
	assert ms2.update_price("KXNOTSEEN-T1", 50) is True, (
		f"[{schema_label}] update_price for an un-seeded ticker must return True"
	)
