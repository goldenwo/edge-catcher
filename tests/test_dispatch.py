"""Tests for dispatch_message routing.

Each test constructs a synthetic event, calls dispatch_message, and asserts
the expected MarketState mutation. These verify the router dispatches each
message type to the correct handler without regressing the behavior of
handlers that were relocated from engine.py.

Behavioral tests for the handlers themselves live in tests/test_engine.py —
the handlers moved modules but their logic and tests stayed intact.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from edge_catcher.monitors.dispatch import dispatch_message
from edge_catcher.monitors.market_state import MarketState, OrderbookSnapshot
from edge_catcher.monitors.trade_store import TradeStore


def _now() -> datetime:
	return datetime(2026, 4, 14, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def market_state() -> MarketState:
	return MarketState()


@pytest.fixture
def store(tmp_path: Path) -> TradeStore:
	ts = TradeStore(tmp_path / "test.db")
	yield ts
	ts.close()


@pytest.fixture
def call_args(market_state: MarketState, store: TradeStore) -> dict:
	"""Common kwargs for dispatch_message calls in this test module."""
	return dict(
		config={},
		market_state=market_state,
		store=store,
		strategies=[],
		strat_by_series={},
		pending_states={},
		dirty=set(),
		now=_now(),
	)


def test_dispatch_routes_orderbook_snapshot(market_state: MarketState, call_args: dict) -> None:
	"""A WS orderbook_snapshot event populates market_state for the ticker."""
	event = {
		"source": "ws",
		"payload": {
			"type": "orderbook_snapshot",
			"msg": {
				"market_ticker": "KXTEST-26APR14",
				"yes": [["0.50", 100]],
				"no": [["0.48", 50]],
			},
		},
	}
	dispatch_message(event, **call_args)
	ob = market_state.get_orderbook("KXTEST-26APR14")
	assert ob is not None
	assert ob.yes_levels == [(0.50, 100)]
	assert ob.no_levels == [(0.48, 50)]


def test_dispatch_routes_orderbook_delta(market_state: MarketState, call_args: dict) -> None:
	"""A WS orderbook_delta event applies a delta to an existing book."""
	# Seed the book first
	market_state.seed_orderbook("KXTEST-26APR14", OrderbookSnapshot(
		yes_levels=[(0.50, 100)],
		no_levels=[(0.48, 50)],
	))
	event = {
		"source": "ws",
		"payload": {
			"type": "orderbook_delta",
			"msg": {
				"market_ticker": "KXTEST-26APR14",
				"yes": [["0.50", -20]],  # 20 contracts lifted
				"no": [],
			},
		},
	}
	dispatch_message(event, **call_args)
	ob = market_state.get_orderbook("KXTEST-26APR14")
	assert ob.yes_levels == [(0.50, 80)]


def test_dispatch_accepts_raw_ws_shape(market_state: MarketState, call_args: dict) -> None:
	"""Raw WS messages (no source/payload wrapper) are also accepted."""
	raw = {
		"type": "orderbook_snapshot",
		"msg": {
			"market_ticker": "KXTEST-26APR14",
			"yes": [["0.42", 10]],
			"no": [["0.60", 15]],
		},
	}
	dispatch_message(raw, **call_args)
	ob = market_state.get_orderbook("KXTEST-26APR14")
	assert ob is not None
	assert ob.yes_levels == [(0.42, 10)]


def test_dispatch_unknown_msg_type_is_noop(market_state: MarketState, call_args: dict) -> None:
	"""An unknown WS msg_type is logged and ignored, not raised."""
	event = {
		"source": "ws",
		"payload": {"type": "heartbeat", "msg": {}},
	}
	# Should not raise
	dispatch_message(event, **call_args)
	# No orderbook side-effect
	assert market_state.get_orderbook("ANY") is None


def test_dispatch_synthetic_source_not_yet_supported(call_args: dict) -> None:
	"""Synthetic sources are reserved for Task 8. Router should log-and-return,
	not raise, so a premature synthetic event in a test bundle is a soft failure
	rather than a crash."""
	event = {
		"source": "synthetic.rest_orderbook",
		"payload": {"ticker": "KXTEST", "yes_levels": [], "no_levels": []},
	}
	dispatch_message(event, **call_args)  # must not raise
