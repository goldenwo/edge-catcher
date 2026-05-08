"""Wiring tests for engine/dispatch.py — exact-kwargs assertion on record_trade.

The executor protocol must route OrderResult fields to record_trade with exact
kwargs (not just call counts). A naive call-count mock would pass even with
values swapped.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from edge_catcher.engine.dispatch import _handle_enter
from edge_catcher.engine.executor import OrderResult
from edge_catcher.engine.strategy_base import Signal


@pytest.fixture
def now():
	return datetime(2026, 5, 7, 21, 0, 0, tzinfo=timezone.utc)


def test_filled_routes_record_trade_with_exact_kwargs(now):
	"""record_trade mock must receive every field with the right value.
	A naive call-count mock would pass even with values swapped."""
	store = MagicMock()
	store.record_trade.return_value = 42        # synthetic trade id
	executor = MagicMock()
	executor.place.return_value = OrderResult(
		status="filled",
		intended_size=4,
		filled_size=4,
		blended_entry_cents=42,
		fill_pct=1.0,
		slippage_cents=1,
		book_depth=5,
		book_snapshot='[[0.42, 100]]',
	)
	signal = Signal(
		action="enter", ticker="KXSOL15M-25-T1", side="yes",
		series="KXSOL15M", strategy="debut-fade", reason="signal",
	)
	ctx = MagicMock(yes_ask=42, no_ask=58, orderbook=MagicMock(depth=5))
	config = {"_metrics": MagicMock()}

	_handle_enter(signal, ctx, store, config, executor, now=now)

	store.record_trade.assert_called_once()
	kwargs = store.record_trade.call_args.kwargs
	assert kwargs["ticker"] == "KXSOL15M-25-T1"
	assert kwargs["entry_price"] == 42
	assert kwargs["strategy"] == "debut-fade"
	assert kwargs["side"] == "yes"
	assert kwargs["series_ticker"] == "KXSOL15M"
	assert kwargs["intended_size"] == 4
	assert kwargs["fill_size"] == 4
	assert kwargs["blended_entry"] == 42
	assert kwargs["book_depth"] == 5
	assert kwargs["fill_pct"] == 1.0
	assert kwargs["slippage_cents"] == 1
	assert kwargs["book_snapshot"] == '[[0.42, 100]]'
	assert kwargs["now"] == now


def test_rejected_stale_book_routes_metric_inc_skipped_stale(now):
	store = MagicMock()
	executor = MagicMock()
	executor.place.return_value = OrderResult(
		status="rejected",
		intended_size=4, filled_size=0,
		blended_entry_cents=0, fill_pct=0.0, slippage_cents=0,
		rejection_reason="stale_book",
	)
	signal = Signal(action="enter", ticker="x", side="yes", series="x", strategy="x", reason="r")
	ctx = MagicMock(yes_ask=42, no_ask=58, orderbook=MagicMock(depth=5))
	metrics = MagicMock()
	config = {"_metrics": metrics}

	_handle_enter(signal, ctx, store, config, executor, now=now)

	store.record_trade.assert_not_called()
	metrics.inc.assert_any_call("entries_skipped_stale")


def test_rejected_other_routes_metric_inc_skipped_other(now):
	store = MagicMock()
	executor = MagicMock()
	executor.place.return_value = OrderResult(
		status="rejected",
		intended_size=4, filled_size=0,
		blended_entry_cents=0, fill_pct=0.0, slippage_cents=0,
		rejection_reason="empty_book",
	)
	signal = Signal(action="enter", ticker="x", side="yes", series="x", strategy="x", reason="r")
	ctx = MagicMock(yes_ask=42, no_ask=58, orderbook=MagicMock(depth=5))
	metrics = MagicMock()
	config = {"_metrics": metrics}

	_handle_enter(signal, ctx, store, config, executor, now=now)

	metrics.inc.assert_any_call("entries_skipped_other")


def test_pending_branch_is_noop(now):
	"""G's PR MUST keep status='pending' as a bare pass — paper never returns
	pending, and a premature call to record_pending_order would crash."""
	store = MagicMock()
	executor = MagicMock()
	executor.place.return_value = OrderResult(
		status="pending",
		intended_size=4, filled_size=0,
		blended_entry_cents=0, fill_pct=0.0, slippage_cents=0,
	)
	signal = Signal(action="enter", ticker="x", side="yes", series="x", strategy="x", reason="r")
	ctx = MagicMock(yes_ask=42, no_ask=58, orderbook=MagicMock(depth=5))
	config = {"_metrics": MagicMock()}

	_handle_enter(signal, ctx, store, config, executor, now=now)

	store.record_trade.assert_not_called()


def test_entry_price_out_of_range_returns_without_calling_executor(now):
	"""Range guard 1..99 fires BEFORE executor.place — preserves existing skip behavior."""
	store = MagicMock()
	executor = MagicMock()
	signal = Signal(action="enter", ticker="x", side="yes", series="x", strategy="x", reason="r")
	# yes_ask=100 → out of range
	ctx = MagicMock(yes_ask=100, no_ask=0, orderbook=MagicMock(depth=5))
	config = {"_metrics": MagicMock()}

	_handle_enter(signal, ctx, store, config, executor, now=now)

	executor.place.assert_not_called()
	store.record_trade.assert_not_called()
