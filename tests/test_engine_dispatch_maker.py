"""Dispatch maker-path integration tests (SPEC §8.2).

Pins the guard chain (live-guard → cap → validate+would_cross →
duplicate_level, all BEFORE place()), the resting branch (tracker
registration + maker_placed), tick-driven fill booking (record_trade with
blended == rest price, slippage 0; augment_fill on later partials), TTL
expiry via step, the live-mode tripwire (2b flips it to asserts-persistence
in the same PR that lands live resting persistence — SPEC §15.1), the
empty-tracker no-op guarantee, and exit-while-partially-resting ordering.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from edge_catcher.engine.dispatch import (
	_handle_enter, _handle_exit, step_resting_orders,
)
from edge_catcher.engine.executors.paper import PaperExecutor
from edge_catcher.engine.market_state import OrderbookSnapshot
from edge_catcher.engine.metrics import Metrics
from edge_catcher.engine.resting import Print, QueueFillModel, RestingOrderTracker
from edge_catcher.engine.strategy_base import Signal

_NOW = datetime(2026, 7, 16, 12, 0, 0, tzinfo=timezone.utc)


class _StubStore:
	"""Captures the store calls the maker path makes (idiom of
	test_engine_dispatch_pending_branch._StubStore, extended with
	augment_fill/exit_trade for the fill/exit flows)."""

	def __init__(self) -> None:
		self.trade_calls: list[dict[str, Any]] = []
		self.augment_calls: list[tuple[int, int]] = []
		self.exit_calls: list[tuple[int, int]] = []
		self.intent_calls: list[dict[str, Any]] = []
		self._next_id = 100

	def record_trade(self, **kwargs: Any) -> int:
		self.trade_calls.append(kwargs)
		self._next_id += 1
		return self._next_id

	def augment_fill(self, trade_id: int, added_size: int) -> None:
		self.augment_calls.append((trade_id, added_size))

	def record_intent(self, **kwargs: Any) -> None:
		self.intent_calls.append(kwargs)

	def record_pending(self, **kwargs: Any) -> None:
		return None

	def record_rejected(self, **kwargs: Any) -> None:
		return None

	def get_trade_by_id(self, trade_id: int) -> dict[str, Any] | None:
		return {"id": trade_id, "status": "open", "fill_size": 4}

	def exit_trade(self, trade_id: int, exit_price: int, **kwargs: Any) -> bool:
		self.exit_calls.append((trade_id, exit_price))
		return True


class _Ctx:
	"""Minimal TickContext stand-in for the maker entry path."""

	def __init__(self, book: OrderbookSnapshot, metadata: dict | None = None) -> None:
		self.yes_bid = 79
		self.yes_ask = 86
		self.no_bid = 14
		self.no_ask = 21
		self.orderbook = book
		self.market_metadata = metadata or {}


def _maker_sig(**over: Any) -> Signal:
	base: dict[str, Any] = dict(
		action="enter", ticker="KXTEST-1", side="no", series="KXTEST",
		strategy="s", reason="r", entry_price_cents=15, exec_style="maker",
		rest_ttl_seconds=300, stop_loss_distance_cents=5)
	base.update(over)
	return Signal(**base)


def _book() -> OrderbookSnapshot:
	# Level prices are DOLLAR floats (OrderbookSnapshot convention). NO bids
	# at 15c (depth 7) — our level; implied NO ask = 100 - 80 = 20c.
	return OrderbookSnapshot(yes_levels=[[0.80, 10]], no_levels=[[0.15, 7]])


def _config(cap: int = 2, tracker: RestingOrderTracker | None = None,
            metrics: Metrics | None = None) -> dict:
	return {
		"sizing": {"risk_per_trade_cents": 200, "max_slippage_cents": 5,
		           "min_fill": 1, "require_fresh_book": True},
		"execution": {"max_resting_per_strategy": cap},
		"_metrics": metrics or Metrics(),
		"_tracker": tracker,
	}


def _tracker() -> RestingOrderTracker:
	return RestingOrderTracker(QueueFillModel(), mid_provider=lambda t: 16)


class _StubMarketState:
	def __init__(self, book: OrderbookSnapshot) -> None:
		self._book = book

	def get_orderbook(self, ticker: str) -> OrderbookSnapshot:
		return self._book


def _executor(cfg: dict) -> PaperExecutor:
	return PaperExecutor(market_state=_StubMarketState(_book()), config=cfg)


async def _enter(sig: Signal, cfg: dict, *, allowed_size: int | None = None) -> None:
	await _handle_enter(sig, _Ctx(_book()), _StubStoreOf(cfg), cfg,
	                    _executor(cfg), now=_NOW, allowed_size=allowed_size)


_STORES: dict[int, _StubStore] = {}


def _StubStoreOf(cfg: dict) -> _StubStore:
	"""One store per config object so tests can assert on it afterwards."""
	key = id(cfg)
	if key not in _STORES:
		_STORES[key] = _StubStore()
	return _STORES[key]


# ---------------------------------------------------------------------------
# Resting branch end-to-end
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_maker_signal_rests_and_registers() -> None:
	tr = _tracker()
	cfg = _config(tracker=tr)
	await _enter(_maker_sig(), cfg)
	assert tr.in_flight_count() == 1
	assert cfg["_metrics"].snapshot()["maker_placed"] == 1
	assert _StubStoreOf(cfg).trade_calls == []          # nothing booked yet
	row = tr.ledger[0]
	assert row.rest_price_cents == 15
	assert row.queue_ahead_at_place == 7.0              # visible depth at our level
	# durability hook ran unconditionally (paper no-op store):
	assert len(_StubStoreOf(cfg).intent_calls) == 1


@pytest.mark.asyncio
async def test_crossing_print_books_then_augments() -> None:
	tr = _tracker()
	cfg = _config(tracker=tr)
	await _enter(_maker_sig(), cfg)
	store = _StubStoreOf(cfg)
	# At-level print, size 9 -> queue 7 consumed, 2 fill (first fill: booked).
	step_resting_orders(cfg, store, "KXTEST-1",
	                    [Print(_NOW.timestamp() + 10, 85, 9.0, "yes")], _NOW)
	assert len(store.trade_calls) == 1
	booked = store.trade_calls[0]
	assert booked["entry_price"] == 15
	assert booked["blended_entry"] == 15
	assert booked["slippage_cents"] == 0
	assert booked["fill_size"] == 2
	# Second at-level print fills 5 more -> augment, not a new row.
	step_resting_orders(cfg, store, "KXTEST-1",
	                    [Print(_NOW.timestamp() + 20, 85, 5.0, "yes")], _NOW)
	assert len(store.trade_calls) == 1
	assert store.augment_calls == [(101, 5)]
	snap = cfg["_metrics"].snapshot()
	assert snap["maker_placed"] == 1


@pytest.mark.asyncio
async def test_ttl_expiry_after_partial_counts_partial() -> None:
	tr = _tracker()
	cfg = _config(tracker=tr)
	await _enter(_maker_sig(rest_ttl_seconds=100), cfg)
	store = _StubStoreOf(cfg)
	step_resting_orders(cfg, store, "KXTEST-1",
	                    [Print(_NOW.timestamp() + 10, 85, 9.0, "yes")], _NOW)
	late = datetime.fromtimestamp(_NOW.timestamp() + 500, tz=timezone.utc)
	step_resting_orders(cfg, store, "KXTEST-1", [], late)
	snap = cfg["_metrics"].snapshot()
	assert snap["maker_partial"] == 1
	assert snap["maker_expired"] == 1
	assert tr.in_flight_count() == 0
	assert len(store.trade_calls) == 1                  # residual position stays booked


# ---------------------------------------------------------------------------
# Guard chain (SPEC §8.2 order) — place() must never be reached on a skip
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cap_zero_skips_maker_disabled() -> None:
	tr = _tracker()
	cfg = _config(cap=0, tracker=tr)
	await _enter(_maker_sig(), cfg)
	assert tr.in_flight_count() == 0
	assert cfg["_metrics"].snapshot()["maker_skip_disabled"] == 1


@pytest.mark.asyncio
async def test_invalid_signal_skips() -> None:
	tr = _tracker()
	cfg = _config(tracker=tr)
	await _enter(_maker_sig(rest_ttl_seconds=None), cfg)
	assert cfg["_metrics"].snapshot()["maker_skip_invalid_signal"] == 1
	assert tr.in_flight_count() == 0


@pytest.mark.asyncio
async def test_crossing_rest_price_skips_would_cross() -> None:
	tr = _tracker()
	cfg = _config(tracker=tr)
	await _enter(_maker_sig(entry_price_cents=20), cfg)   # implied NO ask = 20
	assert cfg["_metrics"].snapshot()["maker_skip_would_cross"] == 1
	assert tr.in_flight_count() == 0


@pytest.mark.asyncio
async def test_second_same_level_skips_duplicate_level() -> None:
	tr = _tracker()
	cfg = _config(tracker=tr)
	await _enter(_maker_sig(), cfg)
	await _enter(_maker_sig(), cfg)
	assert tr.in_flight_count() == 1
	assert cfg["_metrics"].snapshot()["maker_skip_duplicate_level"] == 1


@pytest.mark.asyncio
async def test_close_window_without_close_ts_skips() -> None:
	tr = _tracker()
	cfg = _config(tracker=tr)
	# ctx.market_metadata has no close time -> a close-window thesis is
	# unexecutable (SPEC §5 internals total rule).
	await _enter(_maker_sig(cancel_before_close_seconds=900), cfg)
	assert cfg["_metrics"].snapshot()["maker_skip_invalid_signal"] == 1
	assert tr.in_flight_count() == 0


# ---------------------------------------------------------------------------
# Live-mode tripwire (SPEC §4.4 / §15.1)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_live_sized_path_rejects_maker_not_supported_live() -> None:
	# allowed_size is not None <=> the C-gate ran <=> live path.
	tr = _tracker()
	cfg = _config(tracker=tr)
	await _enter(_maker_sig(), cfg, allowed_size=3)
	assert tr.in_flight_count() == 0
	assert cfg["_metrics"].snapshot()["maker_skip_disabled"] == 0
	# 2b flips this tripwire to asserts-persistence in the SAME PR that
	# lands live resting-row persistence + the reconciler exemption.
	assert _StubStoreOf(cfg).trade_calls == []
	assert _StubStoreOf(cfg).intent_calls == []          # rejected BEFORE durability hook


@pytest.mark.asyncio
async def test_live_executor_config_also_rejects() -> None:
	tr = _tracker()
	cfg = _config(tracker=tr)
	cfg["executor"] = "live"
	await _enter(_maker_sig(), cfg)
	assert tr.in_flight_count() == 0


# ---------------------------------------------------------------------------
# Hot-path no-op guarantee (SPEC §5 internals / §12.7)
# ---------------------------------------------------------------------------

def test_empty_tracker_step_is_noop() -> None:
	tr = _tracker()
	cfg = _config(tracker=tr)
	store = _StubStore()
	events = step_resting_orders(cfg, store, "KXTEST-1",
	                             [Print(_NOW.timestamp(), 85, 9.0, "yes")], _NOW)
	assert events == []
	assert store.trade_calls == [] and store.augment_calls == []


def test_no_tracker_configured_is_noop() -> None:
	cfg = _config(tracker=None)
	store = _StubStore()
	events = step_resting_orders(cfg, store, "KXTEST-1",
	                             [Print(_NOW.timestamp(), 85, 9.0, "yes")], _NOW)
	assert events == []


# ---------------------------------------------------------------------------
# Exit-while-partially-resting (SPEC §8.2 total ordering)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_exit_cancels_resting_remainder_first() -> None:
	tr = _tracker()
	cfg = _config(tracker=tr)
	await _enter(_maker_sig(), cfg)
	store = _StubStoreOf(cfg)
	# Partial fill books trade_id 101 and leaves remainder resting.
	step_resting_orders(cfg, store, "KXTEST-1",
	                    [Print(_NOW.timestamp() + 10, 85, 9.0, "yes")], _NOW)
	assert tr.in_flight_count() == 1
	exit_sig = Signal(action="exit", ticker="KXTEST-1", side="no",
	                  series="KXTEST", strategy="s", reason="tp", trade_id=101)
	await _handle_exit(exit_sig, _Ctx(_book()), store, now=_NOW,
	                   executor=_executor(cfg), config=cfg)
	# Remainder cancelled BEFORE the exit placed; ledger shows partial.
	assert tr.in_flight_count() == 0
	assert tr.ledger[0].disposition == "partial"
	assert tr.ledger[0].end_cause == "cancelled"
	assert store.exit_calls, "exit close still proceeds for the filled part"
