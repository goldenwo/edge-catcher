"""Tests for edge_catcher.monitors.trade_store."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from edge_catcher.monitors.trade_store import TradeStore


def _now() -> datetime:
	"""Return a timezone-aware wall-clock datetime for test `now=` kwargs.

	These tests don't assert anything about timestamps, they just need a
	valid value to satisfy the required parameter. For timestamp-sensitive
	assertions see tests/test_trade_store_now.py.
	"""
	return datetime.now(timezone.utc)


@pytest.fixture
def store(tmp_path: Path) -> TradeStore:
	ts = TradeStore(tmp_path / "test.db")
	yield ts
	ts.close()


# ---------------------------------------------------------------------------
# 1. Init: both tables exist, sizing columns present
# ---------------------------------------------------------------------------

def test_init_tables_exist(store: TradeStore) -> None:
	conn = store._conn
	tables = {
		row[0]
		for row in conn.execute(
			"SELECT name FROM sqlite_master WHERE type='table'"
		).fetchall()
	}
	assert "paper_trades" in tables
	assert "strategy_state" in tables


def test_init_sizing_columns_present(store: TradeStore) -> None:
	conn = store._conn
	cols = {
		row[1]
		for row in conn.execute("PRAGMA table_info(paper_trades)").fetchall()
	}
	for col in ("intended_size", "fill_size", "blended_entry", "book_depth", "fill_pct", "slippage_cents"):
		assert col in cols, f"Missing column: {col}"


def test_init_book_snapshot_column_present(store: TradeStore) -> None:
	cols = {
		row[1]
		for row in store._conn.execute("PRAGMA table_info(paper_trades)").fetchall()
	}
	assert "book_snapshot" in cols


def test_record_trade_with_book_snapshot(store: TradeStore) -> None:
	snapshot = '[[0.03, 12], [0.04, 25]]'
	trade_id = store.record_trade(
		ticker="T1", entry_price=3, strategy="test", side="no",
		series_ticker="SERIES", book_snapshot=snapshot,
		now=_now(),
	)
	row = store._conn.execute(
		"SELECT book_snapshot FROM paper_trades WHERE id=?", (trade_id,)
	).fetchone()
	assert row[0] == snapshot


def test_record_trade_book_snapshot_default_null(store: TradeStore) -> None:
	trade_id = store.record_trade(
		ticker="T1", entry_price=3, strategy="test", side="no",
		series_ticker="SERIES",
		now=_now(),
	)
	row = store._conn.execute(
		"SELECT book_snapshot FROM paper_trades WHERE id=?", (trade_id,)
	).fetchone()
	assert row[0] is None


# ---------------------------------------------------------------------------
# 2. record_trade: record and retrieve, fee auto-calculated (> 0 for price=50 size=10)
# ---------------------------------------------------------------------------

def test_record_trade_returns_id(store: TradeStore) -> None:
	trade_id = store.record_trade(
		ticker="KXBTC-25MAR-T30000",
		entry_price=50,
		strategy="strategy_a",
		side="yes",
		series_ticker="KXBTC15M",
		intended_size=10,
		fill_size=10,
		blended_entry=50,
		book_depth=5,
		fill_pct=1.0,
		slippage_cents=2.0,
		now=_now(),
	)
	assert isinstance(trade_id, int)
	assert trade_id > 0


def test_record_trade_fee_auto_calculated(store: TradeStore) -> None:
	trade_id = store.record_trade(
		ticker="KXBTC-25MAR-T30000",
		entry_price=50,
		strategy="strategy_a",
		side="yes",
		series_ticker="KXBTC15M",
		intended_size=10,
		fill_size=10,
		blended_entry=50,
		book_depth=5,
		fill_pct=1.0,
		slippage_cents=0.0,
		now=_now(),
	)
	row = store._conn.execute(
		"SELECT entry_fee_cents FROM paper_trades WHERE id=?", (trade_id,)
	).fetchone()
	assert row is not None
	assert row[0] > 0  # fee > 0 for price=50 size=10


def test_record_trade_retrieve_open(store: TradeStore) -> None:
	store.record_trade(
		ticker="KXBTC-25MAR-T30000",
		entry_price=45,
		strategy="strategy_a",
		side="yes",
		series_ticker="KXBTC15M",
		intended_size=1,
		fill_size=1,
		blended_entry=45,
		now=_now(),
	)
	open_trades = store.get_open_trades()
	assert len(open_trades) == 1
	assert open_trades[0]["ticker"] == "KXBTC-25MAR-T30000"
	assert open_trades[0]["status"] == "open"


# ---------------------------------------------------------------------------
# 3. settle_trade: yes-side win, PnL includes size
# ---------------------------------------------------------------------------

def test_settle_trade_yes_win(store: TradeStore) -> None:
	trade_id = store.record_trade(
		ticker="KXBTC-25MAR-T30000",
		entry_price=40,
		strategy="strategy_a",
		side="yes",
		series_ticker="KXBTC15M",
		intended_size=5,
		fill_size=5,
		blended_entry=40,
		now=_now(),
	)
	store.settle_trade(trade_id, "yes", now=_now())
	row = store._conn.execute(
		"SELECT status, pnl_cents, exit_price FROM paper_trades WHERE id=?", (trade_id,)
	).fetchone()
	assert row[0] == "won"
	assert row[2] == 100  # exit_price = 100 for yes win
	# PnL = fill_size * (100 - 40) - entry_fee
	entry_fee = store._conn.execute(
		"SELECT entry_fee_cents FROM paper_trades WHERE id=?", (trade_id,)
	).fetchone()[0]
	expected_pnl = 5 * (100 - 40) - entry_fee
	assert row[1] == expected_pnl


def test_settle_trade_yes_loss(store: TradeStore) -> None:
	trade_id = store.record_trade(
		ticker="KXBTC-25MAR-T30000",
		entry_price=40,
		strategy="strategy_a",
		side="yes",
		series_ticker="KXBTC15M",
		intended_size=5,
		fill_size=5,
		blended_entry=40,
		now=_now(),
	)
	store.settle_trade(trade_id, "no", now=_now())
	row = store._conn.execute(
		"SELECT status, pnl_cents, exit_price FROM paper_trades WHERE id=?", (trade_id,)
	).fetchone()
	assert row[0] == "lost"
	assert row[2] == 0  # exit_price = 0 for yes/no loss
	entry_fee = store._conn.execute(
		"SELECT entry_fee_cents FROM paper_trades WHERE id=?", (trade_id,)
	).fetchone()[0]
	expected_pnl = 5 * (0 - 40) - entry_fee
	assert row[1] == expected_pnl


def test_settle_trade_no_side_win(store: TradeStore) -> None:
	trade_id = store.record_trade(
		ticker="KXBTC-25MAR-T30000",
		entry_price=35,
		strategy="strategy_a",
		side="no",
		series_ticker="KXBTC15M",
		intended_size=3,
		fill_size=3,
		blended_entry=35,
		now=_now(),
	)
	store.settle_trade(trade_id, "no", now=_now())  # "no" wins → exit at 100
	row = store._conn.execute(
		"SELECT status, exit_price FROM paper_trades WHERE id=?", (trade_id,)
	).fetchone()
	assert row[0] == "won"
	assert row[1] == 100


# ---------------------------------------------------------------------------
# 4. exit_trade: closes trade at given price
# ---------------------------------------------------------------------------

def test_exit_trade_profit(store: TradeStore) -> None:
	trade_id = store.record_trade(
		ticker="KXBTC-25MAR-T30000",
		entry_price=40,
		strategy="strategy_a",
		side="yes",
		series_ticker="KXBTC15M",
		intended_size=2,
		fill_size=2,
		blended_entry=40,
		now=_now(),
	)
	store.exit_trade(trade_id, exit_price=50, now=_now())
	row = store._conn.execute(
		"SELECT status, exit_price, pnl_cents FROM paper_trades WHERE id=?", (trade_id,)
	).fetchone()
	assert row[0] == "won"
	assert row[1] == 50
	entry_fee = store._conn.execute(
		"SELECT entry_fee_cents FROM paper_trades WHERE id=?", (trade_id,)
	).fetchone()[0]
	from edge_catcher.fees import STANDARD_FEE
	exit_fee = int(STANDARD_FEE.calculate(50, 2))
	expected_pnl = 2 * (50 - 40) - entry_fee - exit_fee
	assert row[2] == expected_pnl


def test_exit_trade_loss(store: TradeStore) -> None:
	trade_id = store.record_trade(
		ticker="KXBTC-25MAR-T30000",
		entry_price=40,
		strategy="strategy_a",
		side="yes",
		series_ticker="KXBTC15M",
		intended_size=2,
		fill_size=2,
		blended_entry=40,
		now=_now(),
	)
	store.exit_trade(trade_id, exit_price=35, now=_now())
	row = store._conn.execute(
		"SELECT status FROM paper_trades WHERE id=?", (trade_id,)
	).fetchone()
	assert row[0] == "lost"


# ---------------------------------------------------------------------------
# 5. get_open_trades_for: filters by strategy and ticker
# ---------------------------------------------------------------------------

def test_get_open_trades_for_filters(store: TradeStore) -> None:
	store.record_trade(
		ticker="KXBTC-25MAR-T30000",
		entry_price=40,
		strategy="strategy_a",
		side="yes",
		series_ticker="KXBTC15M",
		intended_size=1,
		fill_size=1,
		blended_entry=40,
		now=_now(),
	)
	store.record_trade(
		ticker="KXBTC-25MAR-T30000",
		entry_price=45,
		strategy="strategy_b",
		side="no",
		series_ticker="KXBTC15M",
		intended_size=1,
		fill_size=1,
		blended_entry=45,
		now=_now(),
	)
	store.record_trade(
		ticker="KXXRP-25MAR-T2",
		entry_price=30,
		strategy="strategy_a",
		side="yes",
		series_ticker="KXXRP",
		intended_size=1,
		fill_size=1,
		blended_entry=30,
		now=_now(),
	)

	result = store.get_open_trades_for("strategy_a", "KXBTC-25MAR-T30000")
	assert len(result) == 1
	assert result[0]["strategy"] == "strategy_a"
	assert result[0]["ticker"] == "KXBTC-25MAR-T30000"


def test_get_open_trades_for_excludes_closed(store: TradeStore) -> None:
	trade_id = store.record_trade(
		ticker="KXBTC-25MAR-T30000",
		entry_price=40,
		strategy="strategy_a",
		side="yes",
		series_ticker="KXBTC15M",
		intended_size=1,
		fill_size=1,
		blended_entry=40,
		now=_now(),
	)
	store.settle_trade(trade_id, "yes", now=_now())
	result = store.get_open_trades_for("strategy_a", "KXBTC-25MAR-T30000")
	assert len(result) == 0


# ---------------------------------------------------------------------------
# 6. Strategy state: save+load, load empty returns {}, overwrite, load_all_states
# ---------------------------------------------------------------------------

def test_save_and_load_state(store: TradeStore) -> None:
	store.save_state("strategy_a", {"last_entry": "KXBTC-T1", "count": 5})
	state = store.load_state("strategy_a")
	assert state["last_entry"] == "KXBTC-T1"
	assert state["count"] == 5


def test_load_state_empty_returns_empty_dict(store: TradeStore) -> None:
	state = store.load_state("nonexistent-strategy")
	assert state == {}


def test_save_state_overwrites(store: TradeStore) -> None:
	store.save_state("strategy_a", {"count": 1})
	store.save_state("strategy_a", {"count": 99, "new_key": "hello"})
	state = store.load_state("strategy_a")
	assert state["count"] == 99
	assert state["new_key"] == "hello"
	# Old keys not in second save should be gone
	assert len(state) == 2


def test_load_all_states(store: TradeStore) -> None:
	store.save_state("strategy_a", {"a": 1})
	store.save_state("strategy_b", {"b": 2})
	all_states = store.load_all_states()
	assert "strategy_a" in all_states
	assert "strategy_b" in all_states
	assert all_states["strategy_a"]["a"] == 1
	assert all_states["strategy_b"]["b"] == 2


def test_load_all_states_empty(store: TradeStore) -> None:
	all_states = store.load_all_states()
	assert all_states == {}
