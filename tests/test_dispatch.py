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


# ---------------------------------------------------------------------------
# Synthetic event handlers (Task 8 — replay side of the capture tee points)
# ---------------------------------------------------------------------------

def test_dispatch_synthetic_rest_orderbook_seeds_market_state(
	market_state: MarketState, call_args: dict
) -> None:
	"""synthetic.rest_orderbook mirrors the live run_recovery's seed_orderbook
	at the moment the live REST call was made — replay ingests the captured
	yes/no levels directly without re-parsing the raw Kalshi response."""
	event = {
		"source": "synthetic.rest_orderbook",
		"payload": {
			"ticker": "KXTEST-26APR14",
			"yes_levels": [[0.42, 100], [0.41, 50]],
			"no_levels": [[0.58, 75]],
		},
	}
	dispatch_message(event, **call_args)
	ob = market_state.get_orderbook("KXTEST-26APR14")
	assert ob is not None
	assert (0.42, 100) in ob.yes_levels
	assert (0.58, 75) in ob.no_levels


def test_dispatch_synthetic_ticker_discovered_seeds_market_state(
	market_state: MarketState, call_args: dict
) -> None:
	"""synthetic.ticker_discovered is emitted by _ticker_refresh and handled
	identically to synthetic.rest_orderbook on the replay side. The tag
	exists for telemetry — both paths feed the same seed_orderbook call."""
	event = {
		"source": "synthetic.ticker_discovered",
		"payload": {
			"ticker": "KXNEW-26APR14",
			"yes_levels": [[0.30, 20]],
			"no_levels": [[0.70, 15]],
		},
	}
	dispatch_message(event, **call_args)
	ob = market_state.get_orderbook("KXNEW-26APR14")
	assert ob is not None
	assert (0.30, 20) in ob.yes_levels


def test_dispatch_synthetic_settlement_resolves_open_trade(
	store: 'TradeStore', call_args: dict
) -> None:
	"""synthetic.settlement should look up an open trade by composite key
	(strategy, ticker, side, entry_time) and settle it with the captured
	market outcome. The `now` passed in is used as exit_time so replay
	produces byte-identical rows to live."""
	entry_now = datetime(2026, 4, 14, 12, 0, 0, tzinfo=timezone.utc)
	settle_now = datetime(2026, 4, 14, 14, 0, 0, tzinfo=timezone.utc)
	trade_id = store.record_trade(
		ticker="KXSETTLE-26APR14",
		entry_price=40,
		strategy="test-strat",
		side="yes",
		series_ticker="KXSETTLE",
		blended_entry=40,
		now=entry_now,
	)
	event = {
		"source": "synthetic.settlement",
		"payload": {
			"strategy": "test-strat",
			"ticker": "KXSETTLE-26APR14",
			"side": "yes",
			"entry_time": entry_now.isoformat(),
			"result": "yes",  # raw market outcome, NOT 'won'/'lost'
		},
	}
	call_args_with_settle_now = dict(call_args)
	call_args_with_settle_now["now"] = settle_now
	dispatch_message(event, **call_args_with_settle_now)

	# Verify the trade was settled with the captured now
	settled = store.get_trade_by_id(trade_id)
	assert settled is not None
	assert settled["status"] == "won"
	assert settled["exit_time"] == settle_now.isoformat()
	assert settled["exit_price"] == 100  # yes-side wins at 100


def test_dispatch_synthetic_settlement_no_match_is_logged_not_raised(
	store: 'TradeStore', call_args: dict
) -> None:
	"""When the composite key doesn't match any open trade, the handler logs
	a warning and returns — replay shouldn't crash on a stale settlement event."""
	event = {
		"source": "synthetic.settlement",
		"payload": {
			"strategy": "ghost",
			"ticker": "KXGHOST",
			"side": "yes",
			"entry_time": "2026-04-14T12:00:00+00:00",
			"result": "yes",
		},
	}
	dispatch_message(event, **call_args)  # must not raise


def test_dispatch_synthetic_unknown_source_warns_and_returns(call_args: dict) -> None:
	"""An unknown synthetic source logs a warning and returns."""
	event = {
		"source": "synthetic.something_new",
		"payload": {},
	}
	dispatch_message(event, **call_args)  # must not raise
