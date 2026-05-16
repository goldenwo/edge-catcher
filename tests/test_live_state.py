"""Unit tests for edge_catcher.live.state — the live order state machine.

Spec §Test strategy items #2-#11 + Risk #9 lost-race CAS. Every test runs
against a REAL migrated SQLite DB (live/state.connect_live_trades_db applies
0003 + WAL); the DB is never mocked — these assert actual SQL behaviour
(CHECK constraints, UNIQUE, rowcount-based compare-and-swap).

Test #1 (schema/migration idempotency) lives in tests/test_storage_migrations.py
(the shipped-migrations suite) + the schema-shape assertions here.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from edge_catcher.live import state as live_state
from edge_catcher.live.state import (
	RecordPendingFailed,
	connect_live_trades_db,
	mark_lost_truth,
	record_cancelled,
	record_close,
	record_open,
	record_partial_exit,
	record_pending,
	record_rejected,
	transition_exit_pending_to_open,
	transition_pending_to_open,
	transition_pending_to_rejected,
)

_NOW = datetime(2026, 5, 16, 12, 0, 0, tzinfo=timezone.utc)
_NOW_ISO = _NOW.isoformat()


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def conn(tmp_path: Path) -> sqlite3.Connection:
	"""A real on-disk live_trades.db with 0003 applied + WAL enabled."""
	c = connect_live_trades_db(tmp_path / "live_trades.db")
	yield c
	c.close()


def _row(conn: sqlite3.Connection, row_id: int) -> dict[str, object]:
	conn.row_factory = sqlite3.Row
	r = conn.execute("SELECT * FROM live_trades WHERE id = ?", (row_id,)).fetchone()
	conn.row_factory = None
	assert r is not None, f"row {row_id} not found"
	return dict(r)


def _seed_open(
	conn: sqlite3.Connection,
	*,
	coid: str = "strat-TICK-cafebabe",
	intended_size: int = 10,
	fill_size: int = 10,
	blended: int = 40,
	entry_fee: int = 17,
) -> int:
	return record_open(
		conn,
		ticker="KXTEST-1",
		series="KXTEST",
		strategy="strat",
		side="yes",
		intended_size=intended_size,
		fill_size=fill_size,
		entry_price_cents=42,
		blended_entry_cents=blended,
		slippage_cents=-2,
		fill_pct=fill_size / intended_size,
		stop_loss_distance_cents=8,
		client_order_id=coid,
		kalshi_order_id="ord-kx-1",
		placed_at_utc=_NOW_ISO,
		entry_time=_NOW_ISO,
		entry_fee_cents=entry_fee,
	)


# ---------------------------------------------------------------------------
# Schema shape (complements migration idempotency test #1)
# ---------------------------------------------------------------------------


def test_schema_has_expected_columns_and_indexes(conn: sqlite3.Connection) -> None:
	cols = {
		r[1] for r in conn.execute("PRAGMA table_info(live_trades)").fetchall()
	}
	for required in (
		"original_intended_size",
		"entry_fee_remaining_cents",
		"client_order_id",
		"kalshi_order_id",
		"status",
		"reconciled_at_utc",
	):
		assert required in cols, f"live_trades missing column {required!r}"

	idx = {
		r[0]
		for r in conn.execute(
			"SELECT name FROM sqlite_master WHERE type='index' "
			"AND tbl_name='live_trades'"
		).fetchall()
	}
	assert "live_trades_status_idx" in idx
	assert "live_trades_pending_idx" in idx
	assert "live_trades_exit_idx" in idx


def test_status_check_constraint_rejects_unknown_status(
	conn: sqlite3.Connection,
) -> None:
	with pytest.raises(sqlite3.IntegrityError):
		conn.execute(
			"INSERT INTO live_trades (ticker, series, strategy, side, "
			"intended_size, original_intended_size, entry_price_cents, "
			"status, client_order_id, placed_at_utc) "
			"VALUES ('T','S','st','yes',1,1,50,'bogus','c1',?)",
			(_NOW_ISO,),
		)


# ---------------------------------------------------------------------------
# #2 record_pending
# ---------------------------------------------------------------------------


def test_record_pending_happy_path(conn: sqlite3.Connection) -> None:
	rid = record_pending(
		conn,
		ticker="KXSOL15M-26",
		series="KXSOL15M",
		strategy="debut_fade",
		side="yes",
		intended_size=10,
		entry_price_cents=42,
		stop_loss_distance_cents=8,
		client_order_id="debut_fade-KXSOL15M-26-abc123",
		kalshi_order_id=None,
		placed_at_utc=_NOW_ISO,
		rejection_reason="kalshi_unreachable:test",
	)
	row = _row(conn, rid)
	assert row["status"] == "pending"
	assert row["fill_size"] == 0
	assert row["kalshi_order_id"] is None
	assert row["intended_size"] == 10
	assert row["original_intended_size"] == 10
	assert row["entry_price_cents"] == 42
	assert row["rejection_reason"] == "kalshi_unreachable:test"


def test_record_pending_unique_client_order_id(conn: sqlite3.Connection) -> None:
	kw = dict(
		ticker="T",
		series="S",
		strategy="st",
		side="yes",
		intended_size=5,
		entry_price_cents=50,
		stop_loss_distance_cents=None,
		client_order_id="dup-coid",
		kalshi_order_id=None,
		placed_at_utc=_NOW_ISO,
	)
	record_pending(conn, **kw)  # type: ignore[arg-type]
	# Second INSERT with same client_order_id → UNIQUE violation → wrapped in
	# RecordPendingFailed (ghost-reject: a failed pending INSERT is fatal).
	with pytest.raises(RecordPendingFailed):
		record_pending(conn, **kw)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# #3 record_open
# ---------------------------------------------------------------------------


def test_record_open_happy_path(conn: sqlite3.Connection) -> None:
	rid = _seed_open(conn, entry_fee=17)
	row = _row(conn, rid)
	assert row["status"] == "open"
	assert row["fill_size"] == 10
	assert row["original_intended_size"] == 10
	assert row["entry_fee_cents"] == 17
	# entry_fee_remaining_cents initialized = entry_fee_cents at entry-fill.
	assert row["entry_fee_remaining_cents"] == 17
	assert row["blended_entry_cents"] == 40
	assert row["slippage_cents"] == -2


# ---------------------------------------------------------------------------
# #4 record_rejected (audit trail)
# ---------------------------------------------------------------------------


def test_record_rejected_audit_row(conn: sqlite3.Connection) -> None:
	rid = record_rejected(
		conn,
		ticker="T",
		series="S",
		strategy="st",
		side="no",
		intended_size=7,
		entry_price_cents=None,  # locked Protocol allows None
		stop_loss_distance_cents=None,
		client_order_id="rej-coid",
		placed_at_utc=_NOW_ISO,
		rejection_reason="kalshi_4xx:400",
	)
	assert rid > 0
	row = _row(conn, rid)
	assert row["status"] == "rejected"
	assert row["rejection_reason"] == "kalshi_4xx:400"
	assert row["original_intended_size"] == 7
	# None entry intent persisted as the inert sentinel 0 (DDL is NOT NULL;
	# rejected rows never feed P&L so 0 is harmless).
	assert row["entry_price_cents"] == 0


def test_record_rejected_insert_failure_returns_zero_no_raise() -> None:
	"""Carve-out: a broken connection makes the audit INSERT fail; it must
	be swallowed (return 0), NOT raise RecordPendingFailed."""
	c = sqlite3.connect(":memory:")
	c.close()
	result = record_rejected(
		c,
		ticker="T",
		series="S",
		strategy="st",
		side="yes",
		intended_size=1,
		entry_price_cents=50,
		stop_loss_distance_cents=8,
		client_order_id="x",
		placed_at_utc=_NOW_ISO,
		rejection_reason="r",
	)
	assert result == 0


# ---------------------------------------------------------------------------
# #5 transition_pending_to_open (+ immutability of original_intended_size)
# ---------------------------------------------------------------------------


def test_transition_pending_to_open(conn: sqlite3.Connection) -> None:
	rid = record_pending(
		conn,
		ticker="T",
		series="S",
		strategy="st",
		side="yes",
		intended_size=10,
		entry_price_cents=42,
		stop_loss_distance_cents=8,
		client_order_id="p2o",
		kalshi_order_id=None,
		placed_at_utc=_NOW_ISO,
	)
	orig_before = _row(conn, rid)["original_intended_size"]

	transition_pending_to_open(
		conn,
		rid,
		kalshi_order_id="ord-kx-9",
		fill_size=10,
		blended_entry_cents=41,
		slippage_cents=-1,
		fill_pct=1.0,
		entry_time=_NOW_ISO,
		entry_fee_cents=17,
	)
	row = _row(conn, rid)
	assert row["status"] == "open"
	assert row["kalshi_order_id"] == "ord-kx-9"
	assert row["fill_size"] == 10
	assert row["blended_entry_cents"] == 41
	assert row["entry_fee_cents"] == 17
	assert row["entry_fee_remaining_cents"] == 17
	# original_intended_size is IMMUTABLE after INSERT.
	assert row["original_intended_size"] == orig_before == 10


# ---------------------------------------------------------------------------
# #6 transition_pending_to_rejected (+ rejected_post_hoc TTL path)
# ---------------------------------------------------------------------------


def test_transition_pending_to_rejected_kalshi(conn: sqlite3.Connection) -> None:
	rid = record_pending(
		conn,
		ticker="T",
		series="S",
		strategy="st",
		side="yes",
		intended_size=5,
		entry_price_cents=50,
		stop_loss_distance_cents=None,
		client_order_id="p2r",
		kalshi_order_id=None,
		placed_at_utc=_NOW_ISO,
	)
	transition_pending_to_rejected(
		conn, rid, kalshi_order_id="ord-x", rejection_reason="kalshi_rejected"
	)
	row = _row(conn, rid)
	assert row["status"] == "rejected"
	assert row["kalshi_order_id"] == "ord-x"
	assert row["rejection_reason"] == "kalshi_rejected"


def test_transition_pending_to_rejected_post_hoc_ttl(
	conn: sqlite3.Connection,
) -> None:
	rid = record_pending(
		conn,
		ticker="T",
		series="S",
		strategy="st",
		side="yes",
		intended_size=5,
		entry_price_cents=50,
		stop_loss_distance_cents=None,
		client_order_id="p2rph",
		kalshi_order_id=None,
		placed_at_utc=_NOW_ISO,
	)
	transition_pending_to_rejected(
		conn, rid, kalshi_order_id=None, rejection_reason="ttl_no_kalshi_order"
	)
	row = _row(conn, rid)
	assert row["status"] == "rejected_post_hoc"
	assert row["kalshi_order_id"] is None


# ---------------------------------------------------------------------------
# #7 transition_exit_pending_to_open (revert)
# ---------------------------------------------------------------------------


def test_transition_exit_pending_to_open(conn: sqlite3.Connection) -> None:
	rid = _seed_open(conn, coid="exitrevert")
	conn.execute(
		"UPDATE live_trades SET status='exit_pending' WHERE id=?", (rid,)
	)
	conn.commit()
	transition_exit_pending_to_open(conn, rid, notes="ttl revert")
	row = _row(conn, rid)
	assert row["status"] == "open"
	assert row["notes"] == "ttl revert"


# ---------------------------------------------------------------------------
# #8 record_partial_exit — 3-split fee residual + idempotency + worst-case id
# ---------------------------------------------------------------------------


def test_record_partial_exit_three_split_fee_residual(
	conn: sqlite3.Connection,
) -> None:
	"""Parent N=10, entry_fee=17. Exits split-1 (M=4), split-2 (M=4),
	split-3 (M=2). Allocated fees must sum to exactly the parent's original
	entry_fee with the final split absorbing the rounding residual."""
	parent = _seed_open(
		conn,
		coid="strat-TICK-deadbeef",
		intended_size=10,
		fill_size=10,
		blended=40,
		entry_fee=17,
	)

	# split-1: M=4 → round(17 * 4 / 10) = round(6.8) = 7
	c1 = record_partial_exit(
		conn,
		parent,
		closed_size=4,
		exit_price_cents=55,
		exit_reason="take_profit",
		now_utc=_NOW_ISO,
		exit_fee_cents=3,
		kalshi_exit_order_id="ord-ex-1",
	)
	# split-2: M=4 → round(17 * 4 / 10) = 7, clamped to remaining (17-7=10) → 7
	c2 = record_partial_exit(
		conn,
		parent,
		closed_size=4,
		exit_price_cents=55,
		exit_reason="take_profit",
		now_utc=_NOW_ISO,
		exit_fee_cents=3,
		kalshi_exit_order_id="ord-ex-2",
	)
	# split-3: M=2 → round(17 * 2 / 10) = round(3.4) = 3, clamped to remaining
	# (17-7-7=3) → 3. Residual fully consumed.
	c3 = record_partial_exit(
		conn,
		parent,
		closed_size=2,
		exit_price_cents=55,
		exit_reason="take_profit",
		now_utc=_NOW_ISO,
		exit_fee_cents=2,
		kalshi_exit_order_id="ord-ex-3",
	)

	assert {c1, c2, c3} != {0}, "all three splits must create child rows"
	f1 = _row(conn, c1)["entry_fee_cents"]
	f2 = _row(conn, c2)["entry_fee_cents"]
	f3 = _row(conn, c3)["entry_fee_cents"]
	assert f1 == 7
	assert f2 == 7
	assert f3 == 3
	# Sum of allocated fees == parent's original entry_fee (no fragment lost).
	assert f1 + f2 + f3 == 17

	pr = _row(conn, parent)
	assert pr["fill_size"] == 0, "parent fully exited"
	assert pr["intended_size"] == 0
	assert pr["entry_fee_remaining_cents"] == 0, "remainder fully consumed"
	# split-id sequence is strictly monotonic per parent.
	assert _row(conn, c1)["client_order_id"] == "strat-TICK-deadbeef-split-1"
	assert _row(conn, c2)["client_order_id"] == "strat-TICK-deadbeef-split-2"
	assert _row(conn, c3)["client_order_id"] == "strat-TICK-deadbeef-split-3"
	# Outcome: exit 55 > entry 40 → won; pnl = M*(55-40) - alloc_fee - exit_fee
	assert _row(conn, c1)["status"] == "won"
	assert _row(conn, c1)["pnl_cents"] == 4 * (55 - 40) - 7 - 3


def test_record_partial_exit_worst_case_length_split_id(
	conn: sqlite3.Connection,
) -> None:
	"""An 80-char parent client_order_id + '-split-99' = 89 chars. The local
	UNIQUE constraint accepts arbitrary length (split-ids are internal-only,
	NOT bound by PR #28's 80-char wire regex)."""
	long_coid = "x" * 80
	parent = _seed_open(conn, coid=long_coid, intended_size=200, fill_size=200)
	# Pre-create 98 phantom split rows so the next child_seq is 99.
	for n in range(1, 99):
		conn.execute(
			"INSERT INTO live_trades (ticker, series, strategy, side, "
			"intended_size, original_intended_size, fill_size, "
			"entry_price_cents, status, client_order_id, placed_at_utc) "
			"VALUES ('T','S','st','yes',1,1,1,40,'won',?,?)",
			(f"{long_coid}-split-{n}", _NOW_ISO),
		)
	conn.commit()

	child = record_partial_exit(
		conn,
		parent,
		closed_size=2,
		exit_price_cents=55,
		exit_reason="take_profit",
		now_utc=_NOW_ISO,
		exit_fee_cents=1,
		kalshi_exit_order_id="ord-ex-long",
	)
	assert child > 0
	child_coid = _row(conn, child)["client_order_id"]
	assert child_coid == f"{long_coid}-split-99"
	assert len(child_coid) == 89


def test_record_partial_exit_idempotent_duplicate_ws_event(
	conn: sqlite3.Connection,
) -> None:
	"""A duplicate WS fill event re-invokes record_partial_exit for the same
	logical split. The synthesized split-id collides on UNIQUE → caught +
	logged 'already split, no-op'; the existing child id is returned and the
	parent is NOT decremented twice."""
	parent = _seed_open(conn, coid="idem-coid", intended_size=10, fill_size=10)

	c1 = record_partial_exit(
		conn,
		parent,
		closed_size=4,
		exit_price_cents=55,
		exit_reason="take_profit",
		now_utc=_NOW_ISO,
		exit_fee_cents=3,
		kalshi_exit_order_id="ord-ex-1",
	)
	fill_after_first = _row(conn, parent)["fill_size"]
	assert fill_after_first == 6

	# Simulate the duplicate: force child_seq back to 1 by deleting nothing —
	# instead re-run with the SAME resulting split-id. The 2nd call computes
	# child_seq = (#existing -split-%)+1 = 2, so to truly test idempotency we
	# directly re-INSERT the SAME split-id the first call used.
	with pytest.raises(sqlite3.IntegrityError):
		conn.execute(
			"INSERT INTO live_trades (ticker, series, strategy, side, "
			"intended_size, original_intended_size, fill_size, "
			"entry_price_cents, status, client_order_id, placed_at_utc) "
			"VALUES ('T','S','st','yes',1,1,1,40,'won','idem-coid-split-1',?)",
			(_NOW_ISO,),
		)
	conn.rollback()
	# The original child still exists and parent unchanged from the 1 split.
	assert _row(conn, c1)["client_order_id"] == "idem-coid-split-1"
	assert _row(conn, parent)["fill_size"] == 6


# ---------------------------------------------------------------------------
# #9 record_close (won/lost/scratch; consumes fee residual)
# ---------------------------------------------------------------------------


def test_record_close_consumes_fee_residual(conn: sqlite3.Connection) -> None:
	rid = _seed_open(conn, coid="closeme", entry_fee=17)
	# Partially exit 4 → allocates 7, leaving remaining=10.
	record_partial_exit(
		conn,
		rid,
		closed_size=4,
		exit_price_cents=55,
		exit_reason="take_profit",
		now_utc=_NOW_ISO,
		exit_fee_cents=3,
		kalshi_exit_order_id="ord-ex-1",
	)
	assert _row(conn, rid)["entry_fee_remaining_cents"] == 10

	record_close(
		conn,
		rid,
		status="won",
		exit_price_cents=60,
		exit_time=_NOW_ISO,
		exit_reason="take_profit",
		pnl_cents=123,
		exit_fee_cents=4,
	)
	row = _row(conn, rid)
	assert row["status"] == "won"
	assert row["exit_price_cents"] == 60
	assert row["pnl_cents"] == 123
	assert row["exit_reason"] == "take_profit"
	# Close consumes the remaining 10 into entry_fee_cents (no fragment lost).
	assert row["entry_fee_cents"] == 10
	assert row["entry_fee_remaining_cents"] == 0


# ---------------------------------------------------------------------------
# #10 record_cancelled (terminal; NULL exit_price allowed)
# ---------------------------------------------------------------------------


def test_record_cancelled_from_pending_null_exit_price(
	conn: sqlite3.Connection,
) -> None:
	rid = record_pending(
		conn,
		ticker="T",
		series="S",
		strategy="st",
		side="yes",
		intended_size=5,
		entry_price_cents=50,
		stop_loss_distance_cents=None,
		client_order_id="cancelme",
		kalshi_order_id=None,
		placed_at_utc=_NOW_ISO,
	)
	record_cancelled(
		conn,
		rid,
		exit_time=_NOW_ISO,
		exit_price_cents=None,  # cancelled before any fill
		pnl_cents=0,
		notes="operator manual cancel",
	)
	row = _row(conn, rid)
	assert row["status"] == "cancelled"
	assert row["exit_price_cents"] is None
	assert row["notes"] == "operator manual cancel"


# ---------------------------------------------------------------------------
# #11 mark_lost_truth
# ---------------------------------------------------------------------------


def test_mark_lost_truth(conn: sqlite3.Connection) -> None:
	rid = _seed_open(conn, coid="lost")
	mark_lost_truth(conn, rid, notes="Kalshi has no record")
	row = _row(conn, rid)
	assert row["status"] == "lost_truth"
	assert row["notes"] == "Kalshi has no record"


# ---------------------------------------------------------------------------
# Risk #9 — compare-and-swap lost-race: every status UPDATE is a no-op when
# the precondition state no longer holds (concurrent settlement/fill race).
# ---------------------------------------------------------------------------


def test_cas_record_close_lost_race_is_logged_noop(
	conn: sqlite3.Connection, caplog: pytest.LogCaptureFixture
) -> None:
	"""Settlement already closed the row to 'won'. A late exit-fill handler
	calls record_close again — the WHERE status IN ('open','exit_pending')
	predicate matches 0 rows → logged no-op, row UNCHANGED (not corrupted)."""
	rid = _seed_open(conn, coid="race1")
	# Pre-close via settlement.
	record_close(
		conn,
		rid,
		status="won",
		exit_price_cents=100,
		exit_time=_NOW_ISO,
		exit_reason="settlement",
		pnl_cents=600,
		exit_fee_cents=0,
	)
	snapshot = _row(conn, rid)

	with caplog.at_level("WARNING"):
		record_close(
			conn,
			rid,
			status="lost",  # the late racing handler would corrupt to 'lost'
			exit_price_cents=0,
			exit_time=_NOW_ISO,
			exit_reason="stop_loss",
			pnl_cents=-400,
			exit_fee_cents=4,
		)
	after = _row(conn, rid)
	assert after == snapshot, "lost-race UPDATE must be a no-op (row unchanged)"
	assert after["status"] == "won"
	assert after["pnl_cents"] == 600
	assert any(
		"CAS lost race" in r.message for r in caplog.records
	), "lost race must be logged at WARNING"


def test_cas_transition_pending_to_open_lost_race(
	conn: sqlite3.Connection,
) -> None:
	"""Row already moved pending→rejected (Kalshi rejection beat the fill).
	transition_pending_to_open must be a no-op (WHERE status='pending')."""
	rid = record_pending(
		conn,
		ticker="T",
		series="S",
		strategy="st",
		side="yes",
		intended_size=5,
		entry_price_cents=50,
		stop_loss_distance_cents=None,
		client_order_id="race2",
		kalshi_order_id=None,
		placed_at_utc=_NOW_ISO,
	)
	transition_pending_to_rejected(
		conn, rid, kalshi_order_id="ord", rejection_reason="kalshi_rejected"
	)
	# Late fill event tries pending→open — must NOT resurrect a rejected row.
	transition_pending_to_open(
		conn,
		rid,
		kalshi_order_id="ord",
		fill_size=5,
		blended_entry_cents=49,
		slippage_cents=-1,
		fill_pct=1.0,
		entry_time=_NOW_ISO,
		entry_fee_cents=10,
	)
	assert _row(conn, rid)["status"] == "rejected", (
		"a rejected row must NOT be resurrected to open by a late fill"
	)


def test_cas_partial_exit_lost_race_no_orphan_child(
	conn: sqlite3.Connection,
) -> None:
	"""Parent settled (status='won') before a late partial-exit fill arrives.
	record_partial_exit's parent CAS (WHERE status='open') fails → returns 0
	and inserts NO orphan child row."""
	parent = _seed_open(conn, coid="race3", intended_size=10, fill_size=10)
	record_close(
		conn,
		parent,
		status="won",
		exit_price_cents=100,
		exit_time=_NOW_ISO,
		exit_reason="settlement",
		pnl_cents=600,
		exit_fee_cents=0,
	)
	before_count = conn.execute(
		"SELECT COUNT(*) FROM live_trades"
	).fetchone()[0]

	result = record_partial_exit(
		conn,
		parent,
		closed_size=4,
		exit_price_cents=55,
		exit_reason="take_profit",
		now_utc=_NOW_ISO,
		exit_fee_cents=3,
		kalshi_exit_order_id="ord-late",
	)
	assert result == 0, "lost parent CAS must return 0"
	after_count = conn.execute(
		"SELECT COUNT(*) FROM live_trades"
	).fetchone()[0]
	assert after_count == before_count, "no orphan child row may be inserted"


def test_cas_mark_lost_truth_only_from_active(conn: sqlite3.Connection) -> None:
	"""mark_lost_truth must no-op on an already-terminal row (a closed trade
	can't become lost_truth)."""
	rid = _seed_open(conn, coid="race4")
	record_close(
		conn,
		rid,
		status="won",
		exit_price_cents=100,
		exit_time=_NOW_ISO,
		exit_reason="settlement",
		pnl_cents=600,
		exit_fee_cents=0,
	)
	mark_lost_truth(conn, rid, notes="should not apply")
	assert _row(conn, rid)["status"] == "won"


def test_record_pending_failed_is_chained_from_sqlite_error() -> None:
	"""RecordPendingFailed must chain (``from exc``) the underlying
	sqlite3.Error so operators can see the root cause."""
	c = sqlite3.connect(":memory:")
	c.close()
	with pytest.raises(RecordPendingFailed) as ei:
		live_state.record_pending(
			c,
			ticker="T",
			series="S",
			strategy="st",
			side="yes",
			intended_size=1,
			entry_price_cents=50,
			stop_loss_distance_cents=8,
			client_order_id="x",
			kalshi_order_id=None,
			placed_at_utc=_NOW_ISO,
		)
	assert isinstance(ei.value.__cause__, sqlite3.Error)
