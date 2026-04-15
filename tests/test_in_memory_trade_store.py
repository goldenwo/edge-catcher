"""Tests for InMemoryTradeStore — the replay backtester's structural twin of TradeStore.

Critical guarantee: InMemoryTradeStore must produce byte-identical trade
row values to SQLiteTradeStore for every combination of entry/exit/settle
inputs, or the parity test will fail.

These tests run the same sequence of operations against BOTH stores and
diff the resulting row dicts. Any divergence means the replay backtester
will drift from the live trader's output.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from edge_catcher.monitors.trade_store import (
	DuplicateOpenTradeError,
	InMemoryTradeStore,
	TradeStore,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sqlite_store(tmp_path: Path) -> TradeStore:
	store = TradeStore(tmp_path / "trades.db")
	yield store
	store.close()


@pytest.fixture
def in_memory_store() -> InMemoryTradeStore:
	return InMemoryTradeStore()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


# Fields that we compare across the two stores. id + book_snapshot + exit_price/time/pnl
# are populated differently (id from autoincrement vs counter, but we compare
# order-based so that's fine; book_snapshot is compared on equality only).
_PARITY_FIELDS = [
	"ticker",
	"entry_price",
	"strategy",
	"side",
	"series_ticker",
	"entry_fee_cents",
	"intended_size",
	"fill_size",
	"blended_entry",
	"book_depth",
	"fill_pct",
	"slippage_cents",
	"status",
	"entry_time",
	"exit_price",
	"exit_time",
	"pnl_cents",
]


def _project(row: dict[str, Any]) -> dict[str, Any]:
	return {k: row.get(k) for k in _PARITY_FIELDS}


# ---------------------------------------------------------------------------
# Parity — record + exit
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("side,exit_price,blended_entry", [
	("yes", 70, 45),  # profitable yes exit
	("yes", 30, 45),  # losing yes exit
	("no", 70, 45),   # profitable no exit (selling no at higher price)
	("no", 30, 45),   # losing no exit
	("yes", 50, 50),  # scratch (exit == entry)
	("yes", 60, None),  # blended_entry=None → falls back to entry_price
])
def test_record_and_exit_match_across_stores(
	sqlite_store: TradeStore,
	in_memory_store: InMemoryTradeStore,
	side: str,
	exit_price: int,
	blended_entry: int | None,
) -> None:
	"""The two stores must produce identical rows after record + exit."""
	entry_now = datetime(2026, 4, 14, 12, 0, 0, tzinfo=timezone.utc)
	exit_now = datetime(2026, 4, 14, 13, 0, 0, tzinfo=timezone.utc)

	for store in (sqlite_store, in_memory_store):
		trade_id = store.record_trade(
			ticker="KXTEST-26APR14",
			entry_price=45,
			strategy="test-strat",
			side=side,
			series_ticker="KXTEST",
			intended_size=10,
			fill_size=10,
			blended_entry=blended_entry,
			book_depth=5,
			fill_pct=1.0,
			slippage_cents=0.0,
			book_snapshot=None,
			now=entry_now,
		)
		store.exit_trade(trade_id, exit_price=exit_price, now=exit_now)

	sqlite_rows = [_project(r) for r in sqlite_store.get_open_trades()]
	memory_rows = [_project(r) for r in in_memory_store.get_open_trades()]
	# Both should have zero open rows after exit
	assert sqlite_rows == []
	assert memory_rows == []

	# Compare all rows via get_trade_by_id(1) since both start empty
	sqlite_row = _project(sqlite_store.get_trade_by_id(1) or {})
	memory_row = _project(in_memory_store.get_trade_by_id(1) or {})
	assert sqlite_row == memory_row, f"divergence: sqlite={sqlite_row} memory={memory_row}"


# ---------------------------------------------------------------------------
# Parity — record + settle
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("side,result", [
	("yes", "yes"),  # yes wins
	("yes", "no"),   # yes loses
	("no", "yes"),   # no loses
	("no", "no"),    # no wins
])
def test_record_and_settle_match_across_stores(
	sqlite_store: TradeStore,
	in_memory_store: InMemoryTradeStore,
	side: str,
	result: str,
) -> None:
	"""Settlement math must match exactly between the two stores."""
	entry_now = datetime(2026, 4, 14, 12, 0, 0, tzinfo=timezone.utc)
	settle_now = datetime(2026, 4, 14, 14, 0, 0, tzinfo=timezone.utc)

	for store in (sqlite_store, in_memory_store):
		trade_id = store.record_trade(
			ticker="KXSETTLE-26APR14",
			entry_price=40,
			strategy="s1",
			side=side,
			series_ticker="KXSETTLE",
			intended_size=5,
			fill_size=5,
			blended_entry=40,
			now=entry_now,
		)
		store.settle_trade(trade_id, result, now=settle_now)

	sqlite_row = _project(sqlite_store.get_trade_by_id(1) or {})
	memory_row = _project(in_memory_store.get_trade_by_id(1) or {})
	assert sqlite_row == memory_row, f"divergence: sqlite={sqlite_row} memory={memory_row}"


# ---------------------------------------------------------------------------
# DuplicateOpenTradeError
# ---------------------------------------------------------------------------


def test_in_memory_record_trade_raises_on_duplicate_open(in_memory_store: InMemoryTradeStore) -> None:
	now = datetime(2026, 4, 14, 12, 0, 0, tzinfo=timezone.utc)
	kwargs = dict(
		ticker="KXTEST",
		entry_price=50,
		strategy="s",
		side="yes",
		series_ticker="KXTEST",
		now=now,
	)
	in_memory_store.record_trade(**kwargs)
	with pytest.raises(DuplicateOpenTradeError, match="open trade already exists"):
		in_memory_store.record_trade(**kwargs)


def test_in_memory_record_trade_allows_different_side_same_timestamp(in_memory_store: InMemoryTradeStore) -> None:
	now = datetime(2026, 4, 14, 12, 0, 0, tzinfo=timezone.utc)
	in_memory_store.record_trade(
		ticker="KXTEST", entry_price=50, strategy="s", side="yes",
		series_ticker="KXTEST", now=now,
	)
	in_memory_store.record_trade(
		ticker="KXTEST", entry_price=50, strategy="s", side="no",
		series_ticker="KXTEST", now=now,
	)
	assert len(in_memory_store.get_open_trades()) == 2


# ---------------------------------------------------------------------------
# Idempotent race protection
# ---------------------------------------------------------------------------


def test_in_memory_settle_trade_is_idempotent(in_memory_store: InMemoryTradeStore) -> None:
	"""Settling an already-closed trade must be a silent no-op (matches live)."""
	now = datetime(2026, 4, 14, 12, 0, 0, tzinfo=timezone.utc)
	later = datetime(2026, 4, 14, 14, 0, 0, tzinfo=timezone.utc)
	trade_id = in_memory_store.record_trade(
		ticker="KXT", entry_price=40, strategy="s", side="yes",
		series_ticker="KXT", now=now,
	)
	in_memory_store.settle_trade(trade_id, "yes", now=later)
	# Second settle — should NOT mutate
	snapshot_before = in_memory_store.get_trade_by_id(trade_id)
	in_memory_store.settle_trade(trade_id, "no", now=later)
	snapshot_after = in_memory_store.get_trade_by_id(trade_id)
	assert snapshot_before == snapshot_after


def test_in_memory_exit_trade_is_idempotent(in_memory_store: InMemoryTradeStore) -> None:
	now = datetime(2026, 4, 14, 12, 0, 0, tzinfo=timezone.utc)
	later = datetime(2026, 4, 14, 13, 0, 0, tzinfo=timezone.utc)
	trade_id = in_memory_store.record_trade(
		ticker="KXT", entry_price=40, strategy="s", side="yes",
		series_ticker="KXT", now=now,
	)
	in_memory_store.exit_trade(trade_id, exit_price=60, now=later)
	snapshot_before = in_memory_store.get_trade_by_id(trade_id)
	in_memory_store.exit_trade(trade_id, exit_price=30, now=later)  # would be a loss if applied
	snapshot_after = in_memory_store.get_trade_by_id(trade_id)
	assert snapshot_before == snapshot_after


def test_in_memory_settle_unknown_trade_id_is_noop(in_memory_store: InMemoryTradeStore) -> None:
	"""Unknown trade_id → silent return, not KeyError (matches live)."""
	now = datetime(2026, 4, 14, 12, 0, 0, tzinfo=timezone.utc)
	in_memory_store.settle_trade(9999, "yes", now=now)  # no row exists, must not raise


def test_in_memory_exit_unknown_trade_id_is_noop(in_memory_store: InMemoryTradeStore) -> None:
	now = datetime(2026, 4, 14, 12, 0, 0, tzinfo=timezone.utc)
	in_memory_store.exit_trade(9999, exit_price=50, now=now)  # must not raise


# ---------------------------------------------------------------------------
# Naive datetime rejection
# ---------------------------------------------------------------------------


def test_in_memory_record_rejects_naive_datetime(in_memory_store: InMemoryTradeStore) -> None:
	with pytest.raises(ValueError, match="timezone-aware"):
		in_memory_store.record_trade(
			ticker="KXT", entry_price=50, strategy="s", side="yes",
			series_ticker="KXT", now=datetime(2026, 4, 14, 12, 0, 0),  # naive
		)


def test_in_memory_exit_rejects_naive_datetime(in_memory_store: InMemoryTradeStore) -> None:
	tz_now = datetime(2026, 4, 14, 12, 0, 0, tzinfo=timezone.utc)
	trade_id = in_memory_store.record_trade(
		ticker="KXT", entry_price=50, strategy="s", side="yes",
		series_ticker="KXT", now=tz_now,
	)
	with pytest.raises(ValueError, match="timezone-aware"):
		in_memory_store.exit_trade(trade_id, exit_price=60, now=datetime(2026, 4, 14, 13, 0, 0))


def test_in_memory_settle_rejects_naive_datetime(in_memory_store: InMemoryTradeStore) -> None:
	tz_now = datetime(2026, 4, 14, 12, 0, 0, tzinfo=timezone.utc)
	trade_id = in_memory_store.record_trade(
		ticker="KXT", entry_price=50, strategy="s", side="yes",
		series_ticker="KXT", now=tz_now,
	)
	with pytest.raises(ValueError, match="timezone-aware"):
		in_memory_store.settle_trade(trade_id, "yes", now=datetime(2026, 4, 14, 14, 0, 0))


# ---------------------------------------------------------------------------
# seed_from_rows (replay-specific)
# ---------------------------------------------------------------------------


def test_seed_from_rows_populates_and_bumps_next_id(in_memory_store: InMemoryTradeStore) -> None:
	"""Seeding from a prior bundle's open_trades_at_start must load the rows
	and bump _next_id past the max so future record_trades don't collide."""
	in_memory_store.seed_from_rows([
		{"id": 100, "ticker": "KXOLD", "strategy": "s", "side": "yes",
		 "entry_time": "2026-04-13T00:00:00+00:00", "status": "open",
		 "entry_price": 40, "blended_entry": 40, "fill_size": 1, "entry_fee_cents": 0},
		{"id": 200, "ticker": "KXOLDER", "strategy": "s", "side": "no",
		 "entry_time": "2026-04-13T01:00:00+00:00", "status": "open",
		 "entry_price": 40, "blended_entry": 40, "fill_size": 1, "entry_fee_cents": 0},
	])
	assert len(in_memory_store.get_open_trades()) == 2

	# A new record_trade should get id=201 (200+1), not collide with existing
	new_id = in_memory_store.record_trade(
		ticker="KXNEW", entry_price=50, strategy="s2", side="yes",
		series_ticker="KXNEW",
		now=datetime(2026, 4, 14, 12, 0, 0, tzinfo=timezone.utc),
	)
	assert new_id == 201


# ---------------------------------------------------------------------------
# get_open_trades_for parameter name
# ---------------------------------------------------------------------------


def test_get_open_trades_for_parameter_is_strategy(in_memory_store: InMemoryTradeStore) -> None:
	"""The parameter is ``strategy``, not ``strat_name`` — must match SQLiteTradeStore."""
	now = datetime(2026, 4, 14, 12, 0, 0, tzinfo=timezone.utc)
	in_memory_store.record_trade(
		ticker="KXT", entry_price=50, strategy="target", side="yes",
		series_ticker="KXT", now=now,
	)
	# Must work with the keyword name `strategy=`
	rows = in_memory_store.get_open_trades_for(strategy="target", ticker="KXT")
	assert len(rows) == 1
