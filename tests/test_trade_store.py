"""Tests for edge_catcher.engine.trade_store."""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from edge_catcher.engine import trade_store as trade_store_mod
from edge_catcher.engine.trade_store import TradeStore


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


# ---------------------------------------------------------------------------
# Dual-slippage columns + backfill (spec §4.2)
# ---------------------------------------------------------------------------


def test_init_dual_slippage_columns_present(store: TradeStore) -> None:
	"""Fresh paper_trades schema includes both diagnostic columns. INTEGER per
	spec §4.2 — matches live_trades; deliberately differs from legacy
	slippage_cents REAL.
	"""
	cols = {
		row[1]: row[2]  # name → type
		for row in store._conn.execute("PRAGMA table_info(paper_trades)").fetchall()
	}
	assert "market_impact_cents" in cols, "spec §4.2 requires market_impact_cents on paper_trades"
	assert "limit_slippage_cents" in cols, "spec §4.2 requires limit_slippage_cents on paper_trades"
	assert cols["market_impact_cents"] == "INTEGER", "must be INTEGER per spec §4.2"
	assert cols["limit_slippage_cents"] == "INTEGER", "must be INTEGER per spec §4.2"


def test_migrate_adds_dual_slippage_to_pre_existing_db(tmp_path: Path) -> None:
	"""An existing paper_trades DB without the dual-slippage columns must gain
	them via _MIGRATION_COLUMNS when TradeStore opens it (spec §4.2). Mirrors
	the existing fill_pct / slippage_cents ALTER pattern.
	"""
	import sqlite3
	db_path = tmp_path / "preexisting.db"
	# Create the OLD schema manually (no dual-slippage columns).
	conn = sqlite3.connect(str(db_path))
	conn.execute(
		"CREATE TABLE paper_trades ("
		"id INTEGER PRIMARY KEY AUTOINCREMENT, "
		"ticker TEXT NOT NULL, entry_price INTEGER NOT NULL, "
		"entry_time TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'open', "
		"strategy TEXT NOT NULL DEFAULT 'unknown', side TEXT NOT NULL DEFAULT 'yes', "
		"slippage_cents REAL)"
	)
	conn.commit()
	conn.close()

	# Open via TradeStore — triggers _migrate(); the new columns must appear.
	store = TradeStore(db_path)
	try:
		cols = {
			row[1]
			for row in store._conn.execute("PRAGMA table_info(paper_trades)").fetchall()
		}
		assert "market_impact_cents" in cols
		assert "limit_slippage_cents" in cols
	finally:
		store.close()


def test_record_trade_persists_dual_slippage_metrics(store: TradeStore) -> None:
	"""record_trade accepts + persists market_impact_cents and limit_slippage_cents.
	PaperExecutor populates these on filled entries (spec §5.1); the store
	rounds them in via INSERT."""
	trade_id = store.record_trade(
		ticker="T1", entry_price=50, strategy="test", side="yes",
		series_ticker="SERIES",
		now=_now(),
		market_impact_cents=3,
		limit_slippage_cents=-5,
	)
	row = store._conn.execute(
		"SELECT market_impact_cents, limit_slippage_cents FROM paper_trades WHERE id=?",
		(trade_id,),
	).fetchone()
	assert row[0] == 3, f"market_impact_cents should be 3, got {row[0]}"
	assert row[1] == -5, f"limit_slippage_cents should be -5, got {row[1]}"


def test_record_trade_dual_slippage_default_NULL(store: TradeStore) -> None:
	"""Omitting the dual-slippage kwargs leaves both columns NULL (PaperExecutor
	default; preserves rows from non-paper test fixtures that don't set them)."""
	trade_id = store.record_trade(
		ticker="T2", entry_price=50, strategy="test", side="yes",
		series_ticker="SERIES",
		now=_now(),
	)
	row = store._conn.execute(
		"SELECT market_impact_cents, limit_slippage_cents FROM paper_trades WHERE id=?",
		(trade_id,),
	).fetchone()
	assert row[0] is None
	assert row[1] is None


def test_migrate_backfills_market_impact_from_slippage_cents(tmp_path: Path) -> None:
	"""Per spec §4.2: paper's legacy slippage_cents IS vs-best market-impact
	(identical value). Backfill: UPDATE paper_trades SET market_impact_cents =
	slippage_cents WHERE market_impact_cents IS NULL AND blended_entry IS NOT
	NULL. limit_slippage_cents stays NULL for pre-migration rows (not
	derivable — no stored limit). The blended_entry gate excludes empty-book
	sentinel rows per spec §4.3 (paper converts blended_entry=0 → NULL on
	INSERT; "not measurable", never 0).
	"""
	import sqlite3
	db_path = tmp_path / "preexisting.db"
	conn = sqlite3.connect(str(db_path))
	conn.execute(
		"CREATE TABLE paper_trades ("
		"id INTEGER PRIMARY KEY AUTOINCREMENT, "
		"ticker TEXT NOT NULL, entry_price INTEGER NOT NULL, "
		"entry_time TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'open', "
		"strategy TEXT NOT NULL DEFAULT 'unknown', side TEXT NOT NULL DEFAULT 'yes', "
		"slippage_cents REAL, blended_entry INTEGER)"
	)
	# T1: real fill, slippage=7 → backfill to market_impact_cents=7.
	conn.execute(
		"INSERT INTO paper_trades (ticker, entry_price, entry_time, slippage_cents, blended_entry) "
		"VALUES ('T1', 50, '2026-01-01T00:00:00Z', 7.0, 57)"
	)
	# T2: real fill, slippage=0 (filled exactly at best) → backfill to market_impact_cents=0.
	conn.execute(
		"INSERT INTO paper_trades (ticker, entry_price, entry_time, slippage_cents, blended_entry) "
		"VALUES ('T2', 60, '2026-01-01T00:00:00Z', 0.0, 60)"
	)
	# T3: empty-book sentinel row (blended_entry NULL — paper writes
	# blended_entry=None when fill.blended_price_cents==0). Backfill MUST
	# SKIP this row — market_impact_cents stays NULL per spec §4.3 ("not
	# measurable, never 0"). Pre-fix this row would have backfilled to 0.
	conn.execute(
		"INSERT INTO paper_trades (ticker, entry_price, entry_time, slippage_cents, blended_entry) "
		"VALUES ('T3', 70, '2026-01-01T00:00:00Z', 0.0, NULL)"
	)
	conn.commit()
	conn.close()

	# Opening via TradeStore triggers the backfill.
	store = TradeStore(db_path)
	try:
		rows = store._conn.execute(
			"SELECT ticker, market_impact_cents, limit_slippage_cents, slippage_cents "
			"FROM paper_trades ORDER BY id"
		).fetchall()
		assert rows[0][0] == "T1"
		assert rows[0][1] == 7, f"T1 backfill expected 7, got {rows[0][1]}"
		assert rows[0][2] is None, "limit_slippage_cents must remain NULL for pre-migration"
		assert rows[1][1] == 0, f"T2 backfill (real fill at best) expected 0, got {rows[1][1]}"
		assert rows[1][2] is None
		# Sentinel row: backfill MUST NOT overwrite NULL (else it actively
		# undoes PaperExecutor's new sentinel-path None-write on every startup).
		assert rows[2][0] == "T3"
		assert rows[2][1] is None, (
			"T3 (blended_entry NULL → empty-book sentinel) must stay NULL per "
			f"spec §4.3, NOT backfilled — got {rows[2][1]}. The backfill gate "
			"`blended_entry IS NOT NULL` exists precisely to prevent this."
		)
		assert rows[2][2] is None
	finally:
		store.close()


# ---------------------------------------------------------------------------
# record_intent — Protocol surface + paper accept-and-ignore (spec §4.2/§9)
# ---------------------------------------------------------------------------


def test_protocol_record_intent_includes_dual_slippage_refs() -> None:
	"""Per spec §4.2 (Contract additions): TradeStoreProtocol.record_intent
	gains entry_best_price_cents + entry_limit_price_cents (9 → 11 kwargs,
	defaults None). Live persists these onto the pending row for
	transition_pending_to_open to compute market_impact/limit_slippage at
	fill; paper/in-memory accept-and-ignore. Defaults None so existing
	record_intent(**_intent_kwargs()) sites in tests/dispatch keep working.
	"""
	import inspect

	from edge_catcher.engine.trade_store import TradeStoreProtocol

	sig = inspect.signature(TradeStoreProtocol.record_intent)
	params = sig.parameters
	assert "entry_best_price_cents" in params, (
		"spec §4.2 requires entry_best_price_cents on Protocol.record_intent"
	)
	assert "entry_limit_price_cents" in params, (
		"spec §4.2 requires entry_limit_price_cents on Protocol.record_intent"
	)
	assert params["entry_best_price_cents"].default is None, (
		"default must be None so existing 9-kwarg call sites keep working"
	)
	assert params["entry_limit_price_cents"].default is None, (
		"default must be None so existing 9-kwarg call sites keep working"
	)
	assert params["entry_best_price_cents"].kind is inspect.Parameter.KEYWORD_ONLY
	assert params["entry_limit_price_cents"].kind is inspect.Parameter.KEYWORD_ONLY


def test_paper_record_intent_accepts_dual_slippage_refs(store: TradeStore) -> None:
	"""Per spec §4.2 + §9: paper TradeStore.record_intent accepts (and ignores)
	the two new reference kwargs. Paper has no pending state — synchronous
	fills go straight to record_trade — so this remains a no-op, but the
	kwargs must not raise so dispatch can call uniformly across paper/live.
	"""
	result = store.record_intent(
		ticker="KXT",
		series="KXT",
		strategy="s",
		side="yes",
		intended_size=10,
		entry_price_cents=42,
		stop_loss_distance_cents=8,
		client_order_id="s-KXT-test-intent",
		placed_at_utc="2026-01-01T00:00:00Z",
		entry_best_price_cents=41,
		entry_limit_price_cents=45,
	)
	assert result is None


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
	from edge_catcher.adapters.kalshi.fees import STANDARD_FEE
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
		ticker="SERIES_A-25MAR-T2",
		entry_price=30,
		strategy="strategy_a",
		side="yes",
		series_ticker="SERIES_A",
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


# ---------------------------------------------------------------------------
# Migration safety — _migrate must only swallow 'duplicate column name',
# not every OperationalError. Same bug class as storage/migrations PR #55.
# ---------------------------------------------------------------------------


def test_migrate_propagates_non_duplicate_column_errors(
	tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
	"""A typo in `_MIGRATION_COLUMNS` (e.g. an invalid column type) must
	propagate as ``sqlite3.OperationalError`` instead of being silently
	swallowed by the migration's duplicate-column tolerance.

	Pre-fix: ``except sqlite3.OperationalError: pass`` absorbed EVERY
	``OperationalError`` — typos, disk-full, malformed column defs — and
	left the comment "Column already exists" as a wishful claim. A bug
	in the migration list would be invisible until something downstream
	tried to read the missing column.
	"""
	# Single bogus entry. INTEGER UNIQUE is explicitly forbidden by SQLite's
	# ALTER TABLE ADD COLUMN ("Cannot add a UNIQUE column" — per SQLite docs),
	# so the failure is deterministic and is NOT "duplicate column name".
	# (Plain typos in the type like 'INTEGEER' do NOT work as a test signal
	# because SQLite's type-affinity system happily accepts arbitrary type
	# identifiers; the same goes for unquoted whitespace in column names.)
	monkeypatch.setattr(
		trade_store_mod,
		"_MIGRATION_COLUMNS",
		[("uniq_col", "INTEGER UNIQUE")],
	)

	with pytest.raises(sqlite3.OperationalError) as exc_info:
		TradeStore(tmp_path / "typo.db")

	# Contract: the swallow filter must be specific to 'duplicate column name'.
	# A different OperationalError must surface with its original message.
	assert "duplicate column name" not in str(exc_info.value).lower(), (
		"non-dup-column error was silently rewritten or masked"
	)


def test_migrate_idempotent_on_reopen(tmp_path: Path) -> None:
	"""Re-running ``_migrate`` on a DB whose columns already exist is the
	"Column already exists" path — every shipped column raises
	``duplicate column name`` and is swallowed exactly. The fix must NOT
	break this path.
	"""
	db_path = tmp_path / "reopen.db"

	# First construction: writes _SCHEMA + runs every ALTER once.
	store1 = TradeStore(db_path)
	store1.close()

	# Second construction on the same file: _SCHEMA's CREATE TABLE IF NOT
	# EXISTS is a no-op; every _migrate ALTER raises duplicate-column.
	# Must NOT raise — that's the whole point of the swallow.
	store2 = TradeStore(db_path)
	try:
		# All migration columns must still be present after the re-open.
		cols = {
			row[1]
			for row in store2._conn.execute(
				"PRAGMA table_info(paper_trades)"
			).fetchall()
		}
		for col_name, _col_def in trade_store_mod._MIGRATION_COLUMNS:
			assert col_name in cols, (
				f"migration column {col_name!r} disappeared on reopen; "
				f"got cols={sorted(cols)}"
			)
	finally:
		store2.close()
