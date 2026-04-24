"""Tests for the `now` parameter on TradeStore record/exit/settle methods.

The capture/replay pipeline requires that the live engine and the replay
backtester can both write byte-identical trade rows when fed the same events.
That only works if `entry_time` / `exit_time` come from a caller-provided
timestamp, not from an internal `datetime.now()` call. See
docs/superpowers/specs/2026-04-14-orderbook-capture-replay-design.md §4.7.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from edge_catcher.monitors.trade_store import TradeStore


@pytest.fixture
def store(tmp_path: Path) -> TradeStore:
	ts = TradeStore(tmp_path / "test.db")
	yield ts
	ts.close()


@pytest.fixture
def fixed_now() -> datetime:
	return datetime(2026, 4, 14, 12, 0, 0, tzinfo=timezone.utc)


def test_record_trade_uses_now_parameter(store: TradeStore, fixed_now: datetime) -> None:
	"""record_trade should write the caller-provided `now` as entry_time."""
	trade_id = store.record_trade(
		ticker="KXTEST-26APR14",
		entry_price=50,
		strategy="test-strat",
		side="yes",
		series_ticker="KXTEST",
		now=fixed_now,
	)
	row = store._conn.execute(
		"SELECT entry_time FROM paper_trades WHERE id=?", (trade_id,)
	).fetchone()
	assert row[0] == fixed_now.isoformat()


def test_record_trade_rejects_naive_datetime(store: TradeStore) -> None:
	"""A timezone-naive `now` must be rejected so replay can't silently drift."""
	naive = datetime(2026, 4, 14, 12, 0, 0)  # no tzinfo
	with pytest.raises(ValueError, match="timezone-aware"):
		store.record_trade(
			ticker="KXTEST",
			entry_price=50,
			strategy="test",
			side="yes",
			series_ticker="KXTEST",
			now=naive,  # type: ignore[arg-type]
		)


def test_exit_trade_uses_now_parameter(store: TradeStore, fixed_now: datetime) -> None:
	"""exit_trade takes only (trade_id, exit_price, now) — pnl/status are
	computed internally. See trade_store.py:183. Do NOT add pnl_cents."""
	entry_now = fixed_now
	exit_now = datetime(2026, 4, 14, 13, 0, 0, tzinfo=timezone.utc)
	trade_id = store.record_trade(
		ticker="KXTEST",
		entry_price=40,
		strategy="test",
		side="yes",
		series_ticker="KXTEST",
		blended_entry=40,
		now=entry_now,
	)
	store.exit_trade(trade_id, exit_price=60, now=exit_now)
	row = store._conn.execute(
		"SELECT exit_time FROM paper_trades WHERE id=?", (trade_id,)
	).fetchone()
	assert row[0] == exit_now.isoformat()


def test_exit_trade_rejects_naive_datetime(store: TradeStore, fixed_now: datetime) -> None:
	trade_id = store.record_trade(
		ticker="KXTEST",
		entry_price=40,
		strategy="test",
		side="yes",
		series_ticker="KXTEST",
		now=fixed_now,
	)
	with pytest.raises(ValueError, match="timezone-aware"):
		store.exit_trade(trade_id, exit_price=60, now=datetime(2026, 4, 14, 13, 0, 0))  # type: ignore[arg-type]


def test_settle_trade_uses_now_parameter(store: TradeStore, fixed_now: datetime) -> None:
	"""settle_trade takes result='yes'/'no' (the market outcome), NOT
	'won'/'lost'. The store translates based on side. See trade_store.py:148.
	settle_trade writes exit_time (not a separate settled_time column)."""
	entry_now = fixed_now
	settle_now = datetime(2026, 4, 14, 14, 0, 0, tzinfo=timezone.utc)
	trade_id = store.record_trade(
		ticker="KXTEST",
		entry_price=40,
		strategy="test",
		side="yes",
		series_ticker="KXTEST",
		blended_entry=40,
		now=entry_now,
	)
	store.settle_trade(trade_id, "yes", now=settle_now)
	row = store._conn.execute(
		"SELECT exit_time, status FROM paper_trades WHERE id=?", (trade_id,)
	).fetchone()
	assert row[0] == settle_now.isoformat()
	assert row[1] == "won"


def test_settle_trade_rejects_naive_datetime(store: TradeStore, fixed_now: datetime) -> None:
	trade_id = store.record_trade(
		ticker="KXTEST",
		entry_price=40,
		strategy="test",
		side="yes",
		series_ticker="KXTEST",
		now=fixed_now,
	)
	with pytest.raises(ValueError, match="timezone-aware"):
		store.settle_trade(trade_id, "yes", now=datetime(2026, 4, 14, 14, 0, 0))  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# DuplicateOpenTradeError: composite-key uniqueness on (strategy, ticker, side, entry_time)
#
# Per spec §4.1, the composite key must be unique across open trades so that
# the synthetic.settlement event in replay can look up the target row
# unambiguously. Live engine treats a collision as a fatal bug.
# ---------------------------------------------------------------------------

def test_record_trade_raises_on_duplicate_open(store: TradeStore, fixed_now: datetime) -> None:
	"""Two record_trade calls with the same (strategy, ticker, side, entry_time) must fail."""
	from edge_catcher.monitors.trade_store import DuplicateOpenTradeError

	store.record_trade(
		ticker="KXTEST-26APR14",
		entry_price=50,
		strategy="test-strat",
		side="yes",
		series_ticker="KXTEST",
		now=fixed_now,
	)
	with pytest.raises(DuplicateOpenTradeError, match="open trade already exists"):
		store.record_trade(
			ticker="KXTEST-26APR14",
			entry_price=55,  # different entry price is still a collision
			strategy="test-strat",
			side="yes",
			series_ticker="KXTEST",
			now=fixed_now,  # same timestamp → composite key collision
		)


def test_record_trade_allows_different_side_same_timestamp(store: TradeStore, fixed_now: datetime) -> None:
	"""Same strategy+ticker+time but different side is NOT a collision."""
	store.record_trade(
		ticker="KXTEST-26APR14",
		entry_price=50,
		strategy="test-strat",
		side="yes",
		series_ticker="KXTEST",
		now=fixed_now,
	)
	# Should not raise
	store.record_trade(
		ticker="KXTEST-26APR14",
		entry_price=50,
		strategy="test-strat",
		side="no",
		series_ticker="KXTEST",
		now=fixed_now,
	)
	assert len(store.get_open_trades()) == 2


def test_record_trade_allows_after_close(store: TradeStore, fixed_now: datetime) -> None:
	"""A settled trade frees its composite key for a new open on the same key."""
	trade_id = store.record_trade(
		ticker="KXTEST-26APR14",
		entry_price=50,
		strategy="test-strat",
		side="yes",
		series_ticker="KXTEST",
		now=fixed_now,
	)
	store.settle_trade(trade_id, "yes", now=fixed_now)
	# After close, the composite key is free — a new open on the same key is allowed
	store.record_trade(
		ticker="KXTEST-26APR14",
		entry_price=50,
		strategy="test-strat",
		side="yes",
		series_ticker="KXTEST",
		now=fixed_now,
	)
	# One open (the second) + one settled (the first) = 2 total rows
	assert len(store.get_open_trades()) == 1
