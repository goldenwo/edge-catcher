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
	touch_reconciled,
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


def test_record_pending_accepts_none_entry_price_cents(
	conn: sqlite3.Connection,
) -> None:
	"""Locked cross-PR contract: record_pending.entry_price_cents is
	int | None (Signal.entry_price_cents may be None). A None intent must
	INSERT cleanly — persisted as the inert sentinel 0 (NOT-NULL DDL column;
	pending rows never feed P&L)."""
	rid = record_pending(
		conn,
		ticker="T",
		series="S",
		strategy="st",
		side="yes",
		intended_size=5,
		entry_price_cents=None,
		stop_loss_distance_cents=None,
		client_order_id="none-entry",
		kalshi_order_id=None,
		placed_at_utc=_NOW_ISO,
	)
	row = _row(conn, rid)
	assert row["status"] == "pending"
	assert row["entry_price_cents"] == 0


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
# #5b transition_pending_to_open dual-slippage compute (spec §4.2 / §6 / §9)
#     — single chokepoint for ALL live entry fills (sync record_trade + WS +
#     reconciler). Computes market_impact_cents + limit_slippage_cents from
#     refs persisted on the pending row by live.state.record_pending; NULL
#     ref → metric None per spec §4.3.
# ---------------------------------------------------------------------------


def test_transition_pending_to_open_computes_dual_slippage_metrics(
	conn: sqlite3.Connection,
) -> None:
	"""Per spec §4.2 / §6: transition_pending_to_open reads the two refs from
	the pending row and computes market_impact_cents + limit_slippage_cents
	via signed_slippage_cents(action='buy') at fill time. Buys use
	``blended - limit`` so positive = paid MORE than the reference (worse).
	"""
	rid = record_pending(
		conn,
		ticker="T",
		series="S",
		strategy="st",
		side="yes",
		intended_size=10,
		entry_price_cents=42,
		stop_loss_distance_cents=8,
		client_order_id="p2o-refs",
		kalshi_order_id=None,
		placed_at_utc=_NOW_ISO,
		entry_best_price_cents=41,
		entry_limit_price_cents=45,
	)

	# Blended = 42c. Refs: best=41 (top of book), limit=45 (we offered).
	# Buy convention: market_impact = 42 - 41 = +1 (paid 1c above best — worse).
	# Buy convention: limit_slippage = 42 - 45 = -3 (paid 3c below limit — better).
	transition_pending_to_open(
		conn,
		rid,
		kalshi_order_id="ord-kx-refs",
		fill_size=10,
		blended_entry_cents=42,
		slippage_cents=1,
		fill_pct=1.0,
		entry_time=_NOW_ISO,
		entry_fee_cents=17,
	)
	row = _row(conn, rid)
	assert row["status"] == "open"
	assert row["market_impact_cents"] == 1, (
		"market_impact_cents = blended - entry_best_price_cents for buys; "
		"42 - 41 = 1 (paid 1c above top of book — adverse impact)"
	)
	assert row["limit_slippage_cents"] == -3, (
		"limit_slippage_cents = blended - entry_limit_price_cents for buys; "
		"42 - 45 = -3 (paid 3c below our limit — favorable)"
	)


def test_transition_pending_to_open_null_refs_yield_null_metrics(
	conn: sqlite3.Connection,
) -> None:
	"""Per spec §4.3 + §9: NULL reference column → corresponding metric NULL.
	Covers a pre-0004 pending row reconciling post-0004, dispatch
	pending-fallback paths with no book snapshot, and any path where the
	ref columns default to NULL on INSERT."""
	rid = record_pending(
		conn,
		ticker="T",
		series="S",
		strategy="st",
		side="yes",
		intended_size=10,
		entry_price_cents=42,
		stop_loss_distance_cents=8,
		client_order_id="p2o-noref",
		kalshi_order_id=None,
		placed_at_utc=_NOW_ISO,
		# refs omitted — default None
	)
	transition_pending_to_open(
		conn,
		rid,
		kalshi_order_id="ord-kx-noref",
		fill_size=10,
		blended_entry_cents=42,
		slippage_cents=0,
		fill_pct=1.0,
		entry_time=_NOW_ISO,
		entry_fee_cents=17,
	)
	row = _row(conn, rid)
	assert row["status"] == "open"
	assert row["market_impact_cents"] is None, (
		"spec §4.3: NULL ref → NULL metric, never 0 (0 would mean 'filled exactly at best')"
	)
	assert row["limit_slippage_cents"] is None


def test_transition_pending_to_open_mixed_ref(
	conn: sqlite3.Connection,
) -> None:
	"""Mixed refs: only entry_best persisted (e.g. a hypothetical fallback
	path) → market_impact computed, limit_slippage NULL. Mirrors the
	independent None-guard per metric."""
	rid = record_pending(
		conn,
		ticker="T",
		series="S",
		strategy="st",
		side="yes",
		intended_size=10,
		entry_price_cents=42,
		stop_loss_distance_cents=8,
		client_order_id="p2o-mix",
		kalshi_order_id=None,
		placed_at_utc=_NOW_ISO,
		entry_best_price_cents=40,
		entry_limit_price_cents=None,
	)
	transition_pending_to_open(
		conn,
		rid,
		kalshi_order_id="ord-kx-mix",
		fill_size=10,
		blended_entry_cents=43,
		slippage_cents=3,
		fill_pct=1.0,
		entry_time=_NOW_ISO,
		entry_fee_cents=17,
	)
	row = _row(conn, rid)
	assert row["market_impact_cents"] == 3, "43 - 40 = 3 (paid 3c above best)"
	assert row["limit_slippage_cents"] is None


def test_transition_pending_to_open_mixed_ref_limit_only(
	conn: sqlite3.Connection,
) -> None:
	"""Symmetry mirror of test_transition_pending_to_open_mixed_ref: only
	entry_limit persisted → limit_slippage computed, market_impact NULL.
	Pins independent None-guards on each metric (a one-sided defect in the
	guard logic would only surface on one of the two mirror configurations).
	"""
	rid = record_pending(
		conn,
		ticker="T",
		series="S",
		strategy="st",
		side="yes",
		intended_size=10,
		entry_price_cents=42,
		stop_loss_distance_cents=8,
		client_order_id="p2o-mix-limit",
		kalshi_order_id=None,
		placed_at_utc=_NOW_ISO,
		entry_best_price_cents=None,
		entry_limit_price_cents=45,
	)
	transition_pending_to_open(
		conn,
		rid,
		kalshi_order_id="ord-kx-mix-l",
		fill_size=10,
		blended_entry_cents=42,
		slippage_cents=0,
		fill_pct=1.0,
		entry_time=_NOW_ISO,
		entry_fee_cents=17,
	)
	row = _row(conn, rid)
	assert row["market_impact_cents"] is None
	assert row["limit_slippage_cents"] == -3, "42 - 45 = -3 (paid 3c below limit)"


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
	conn: sqlite3.Connection, caplog: pytest.LogCaptureFixture
) -> None:
	"""Critical #1 + Important #4: a TRUE duplicate WS fill event re-invokes
	record_partial_exit with the SAME kalshi_exit_order_id. The second call
	must be a pure no-op — parent fill_size unchanged, child count unchanged,
	the existing child id returned — even though the first call already
	succeeded (the failure mode the old count-derived split-id missed: the
	2nd call derives a FRESH, non-colliding -split-2 and would double-book).

	This is the real idempotency test (the prior version only re-INSERTed a
	split-id via raw SQL and asserted IntegrityError — it never called the
	function twice, so it exercised a path production never hits)."""
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
	assert c1 > 0
	assert _row(conn, parent)["fill_size"] == 6
	count_after_first = conn.execute(
		"SELECT COUNT(*) FROM live_trades"
	).fetchone()[0]

	# TRUE duplicate: identical args, same kalshi_exit_order_id. Old code
	# computed child_seq=2 → 'idem-coid-split-2' (no UNIQUE collision) →
	# parent decremented to 2 + phantom child. Correct behaviour: no-op.
	with caplog.at_level("INFO"):
		c2 = record_partial_exit(
			conn,
			parent,
			closed_size=4,
			exit_price_cents=55,
			exit_reason="take_profit",
			now_utc=_NOW_ISO,
			exit_fee_cents=3,
			kalshi_exit_order_id="ord-ex-1",
		)

	assert c2 == c1, "duplicate must return the SAME (existing) child id"
	assert _row(conn, parent)["fill_size"] == 6, (
		"parent must NOT be decremented twice for one logical fill"
	)
	assert _row(conn, parent)["intended_size"] == 6
	assert (
		conn.execute("SELECT COUNT(*) FROM live_trades").fetchone()[0]
		== count_after_first
	), "no phantom child row may be inserted on a duplicate WS event"
	assert _row(conn, c1)["client_order_id"] == "idem-coid-split-1"
	assert any(
		"duplicate WS event" in r.message for r in caplog.records
	), "the duplicate must be logged as an idempotent no-op"


class _FailingInsertConnection(sqlite3.Connection):
	"""Real SQLite connection that raises OperationalError on the next
	``INSERT INTO live_trades`` when ``fail_next_insert`` is set. Used to
	simulate a disk-full / I/O failure on the child INSERT *after* the
	parent decrement ran — the only way to prove the decrement is rolled
	back (not merely never applied). Everything else is genuine SQLite.
	"""

	fail_next_insert: bool = False

	def execute(self, sql: str, *args: object) -> sqlite3.Cursor:  # type: ignore[override]
		if (
			self.fail_next_insert
			and sql.lstrip().upper().startswith("INSERT INTO LIVE_TRADES")
		):
			self.fail_next_insert = False
			raise sqlite3.OperationalError("disk I/O error (simulated)")
		return super().execute(sql, *args)


def test_record_partial_exit_atomic_rollback_on_child_insert_failure(
	tmp_path: Path,
) -> None:
	"""Critical #2: if the child INSERT fails with a non-Integrity DB error
	(disk full / OperationalError / I/O), the parent decrement must roll
	back fully — the parent keeps its full contract count and no orphan
	child exists. The old code committed the parent decrement inside
	_cas_update BEFORE the child INSERT, so a failed INSERT silently lost
	contracts (position claims fewer than held on Kalshi, no P&L row).

	Uses a real on-disk migrated DB opened through a Connection subclass
	that raises OperationalError on the child INSERT only — the parent
	UPDATE genuinely commits-or-rolls-back via real SQLite transactions."""
	db = tmp_path / "live_trades.db"
	# Migrate via the production helper, then reopen with the failing factory
	# (same file, real schema/WAL — nothing about SQLite itself is mocked).
	connect_live_trades_db(db).close()
	c = sqlite3.connect(
		str(db), check_same_thread=False, factory=_FailingInsertConnection
	)
	try:
		parent = _seed_open(
			c, coid="atomic-coid", intended_size=10, fill_size=10
		)
		before = _row(c, parent)
		before_count = c.execute(
			"SELECT COUNT(*) FROM live_trades"
		).fetchone()[0]

		c.fail_next_insert = True  # type: ignore[attr-defined]
		with pytest.raises(sqlite3.OperationalError):
			record_partial_exit(
				c,
				parent,
				closed_size=4,
				exit_price_cents=55,
				exit_reason="take_profit",
				now_utc=_NOW_ISO,
				exit_fee_cents=3,
				kalshi_exit_order_id="ord-ex-fail",
			)

		after = _row(c, parent)
		assert after["fill_size"] == before["fill_size"] == 10, (
			"parent decrement MUST roll back on child-INSERT failure (no "
			"silent real-money contract loss)"
		)
		assert after["intended_size"] == before["intended_size"] == 10
		assert (
			after["entry_fee_remaining_cents"]
			== before["entry_fee_remaining_cents"]
		)
		assert (
			c.execute("SELECT COUNT(*) FROM live_trades").fetchone()[0]
			== before_count
		), "no orphan child row may survive a rolled-back split"
	finally:
		c.close()


def test_record_partial_exit_zero_original_intended_size_no_zerodiv(
	conn: sqlite3.Connection, caplog: pytest.LogCaptureFixture
) -> None:
	"""Critical #3: an engine-timeout pending row is synthesized with
	intended_size=0 (dispatch.py defers sizing); transition_pending_to_open
	does NOT mutate original_intended_size (spec-locked immutable). So a
	timeout-pending row reconciled to open then partially exited reaches
	record_partial_exit with original_intended_size=0 → the proportional
	entry-fee `round(fee * M / 0)` raised ZeroDivisionError straight into
	the live exit path. It must instead allocate the full remaining fee,
	log a WARNING, and proceed."""
	rid = record_pending(
		conn,
		ticker="KXSOL15M-26",
		series="KXSOL15M",
		strategy="debut_fade",
		side="yes",
		intended_size=0,  # engine_timeout sizing-deferred placeholder
		entry_price_cents=None,
		stop_loss_distance_cents=None,
		client_order_id="zerodiv-coid",
		kalshi_order_id=None,
		placed_at_utc=_NOW_ISO,
		rejection_reason="engine_timeout:60s",
	)
	assert _row(conn, rid)["original_intended_size"] == 0
	# Reconcile resolves the true size into fill/intended but original stays 0.
	transition_pending_to_open(
		conn,
		rid,
		kalshi_order_id="ord-kx-resolved",
		fill_size=10,
		blended_entry_cents=40,
		slippage_cents=-1,
		fill_pct=1.0,
		entry_time=_NOW_ISO,
		entry_fee_cents=17,
	)
	assert _row(conn, rid)["original_intended_size"] == 0
	assert _row(conn, rid)["entry_fee_remaining_cents"] == 17

	with caplog.at_level("WARNING"):
		child = record_partial_exit(
			conn,
			rid,
			closed_size=4,
			exit_price_cents=55,
			exit_reason="take_profit",
			now_utc=_NOW_ISO,
			exit_fee_cents=3,
			kalshi_exit_order_id="ord-ex-zd",
		)

	assert child > 0, "must NOT raise ZeroDivisionError; child row created"
	# Full remaining entry fee allocated to this child (no proportional math
	# possible with a 0 denominator); record_close later sees a 0 remainder.
	assert _row(conn, child)["entry_fee_cents"] == 17
	assert _row(conn, rid)["entry_fee_remaining_cents"] == 0
	assert any(
		"original_intended_size" in r.message and r.levelname == "WARNING"
		for r in caplog.records
	), "the unusable-denominator fallback must be logged at WARNING"


@pytest.mark.parametrize("bad_size", [0, -3, 11])
def test_record_partial_exit_rejects_out_of_bounds_closed_size(
	conn: sqlite3.Connection, caplog: pytest.LogCaptureFixture, bad_size: int
) -> None:
	"""Important #5: closed_size must satisfy 0 < M <= parent.fill_size. A
	bad/duplicate WS event with M<=0 or M>fill_size would drive
	fill_size/intended_size negative or mint a zero/negative child. The
	guard rejects with a log + returns 0 and writes NOTHING (parent
	untouched, no child)."""
	parent = _seed_open(conn, coid="bounds-coid", intended_size=10, fill_size=10)
	before = _row(conn, parent)
	before_count = conn.execute(
		"SELECT COUNT(*) FROM live_trades"
	).fetchone()[0]

	with caplog.at_level("ERROR"):
		result = record_partial_exit(
			conn,
			parent,
			closed_size=bad_size,
			exit_price_cents=55,
			exit_reason="take_profit",
			now_utc=_NOW_ISO,
			exit_fee_cents=3,
			kalshi_exit_order_id=f"ord-ex-bad-{bad_size}",
		)

	assert result == 0
	assert _row(conn, parent) == before, "parent must be untouched"
	assert (
		conn.execute("SELECT COUNT(*) FROM live_trades").fetchone()[0]
		== before_count
	), "no child row may be written for an out-of-bounds closed_size"
	assert any(
		"out of bounds" in r.message for r in caplog.records
	), "the rejection must be logged"


def test_record_partial_exit_null_blended_entry_is_hard_error(
	conn: sqlite3.Connection,
) -> None:
	"""Minor (a): a NULL blended_entry_cents on a partial-exitable row is an
	invariant violation (open rows always have a blended entry). Silently
	using 0 as the cost basis would mislabel won/lost and corrupt P&L under
	real money — this must raise, not fall back to 0."""
	parent = _seed_open(conn, coid="nullbasis-coid", intended_size=10, fill_size=10)
	# Force the invariant-violating state (cannot occur via the normal API;
	# simulate a corrupt row to prove the guard fires).
	conn.execute(
		"UPDATE live_trades SET blended_entry_cents = NULL WHERE id = ?",
		(parent,),
	)
	conn.commit()

	with pytest.raises(RuntimeError, match="NULL .*blended_entry_cents"):
		record_partial_exit(
			conn,
			parent,
			closed_size=4,
			exit_price_cents=55,
			exit_reason="take_profit",
			now_utc=_NOW_ISO,
			exit_fee_cents=3,
			kalshi_exit_order_id="ord-ex-nb",
		)
	# Hard error fired BEFORE any write — parent untouched.
	assert _row(conn, parent)["fill_size"] == 10


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


def test_cas_lost_race_does_not_emit_false_transition_log(
	conn: sqlite3.Connection, caplog: pytest.LogCaptureFixture
) -> None:
	"""Important #6 (TOCTOU): record_close / record_cancelled read `before`
	BEFORE the CAS UPDATE and used to log 'open→won' / '..→cancelled'
	UNCONDITIONALLY — even on a lost race (rowcount==0 no-op), the INFO line
	falsely claimed a transition that never happened. The transition INFO
	must be gated on the CAS result; only the CAS-lost-race WARNING fires."""
	# record_close on an already-settled row (CAS predicate matches 0 rows).
	rid = _seed_open(conn, coid="toctou-close")
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
	with caplog.at_level("INFO"):
		caplog.clear()
		record_close(
			conn,
			rid,
			status="lost",
			exit_price_cents=0,
			exit_time=_NOW_ISO,
			exit_reason="stop_loss",
			pnl_cents=-400,
			exit_fee_cents=4,
		)
	msgs = [r.message for r in caplog.records]
	assert not any("→lost" in m for m in msgs), (
		"no false 'won→lost' transition INFO may be logged on a lost race"
	)
	assert any("CAS lost race" in m for m in msgs), (
		"the lost race must still be logged at WARNING"
	)

	# record_cancelled on an already-terminal row (won → not in
	# {pending,open,exit_pending} → CAS no-op).
	with caplog.at_level("INFO"):
		caplog.clear()
		record_cancelled(
			conn,
			rid,
			exit_time=_NOW_ISO,
			exit_price_cents=None,
			pnl_cents=0,
			notes="late operator cancel",
		)
	msgs = [r.message for r in caplog.records]
	assert not any("→cancelled" in m for m in msgs), (
		"no false '..→cancelled' transition INFO may be logged on a lost race"
	)
	assert _row(conn, rid)["status"] == "won", "row must remain unchanged"


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


# ---------------------------------------------------------------------------
# touch_reconciled — matrix row 6 "both agree" observability bump
# (spec §332 row 6: "UPDATE reconciled_at_utc; continue"). CAS-guarded by
# the same WHERE status IN ('open','pending','exit_pending') discipline as
# mark_lost_truth — only an active row's reconciled_at_utc is meaningful.
# ---------------------------------------------------------------------------


def test_touch_reconciled_sets_timestamp_on_open_row(
	conn: sqlite3.Connection,
) -> None:
	"""Happy path: an 'open' row's reconciled_at_utc starts NULL (record_open
	never sets it) and is stamped by a winning touch_reconciled CAS."""
	rid = _seed_open(conn, coid="touch-open")
	assert _row(conn, rid)["reconciled_at_utc"] is None

	changed = touch_reconciled(conn, rid, now_utc=_NOW_ISO)

	assert changed is True, "a winning CAS must return True"
	assert _row(conn, rid)["reconciled_at_utc"] == _NOW_ISO
	# It touches ONLY reconciled_at_utc — status / fill_size untouched.
	assert _row(conn, rid)["status"] == "open"


@pytest.mark.parametrize("active_status", ["pending", "exit_pending"])
def test_touch_reconciled_works_on_all_active_states(
	conn: sqlite3.Connection, active_status: str
) -> None:
	"""row 6 "both agree" applies to every active state reconcile can confirm
	against Kalshi: open (above), pending, exit_pending. Terminal rows are
	covered by the lost-race test below."""
	rid = record_pending(
		conn,
		ticker="T",
		series="S",
		strategy="st",
		side="yes",
		intended_size=5,
		entry_price_cents=50,
		stop_loss_distance_cents=None,
		client_order_id=f"touch-{active_status}",
		kalshi_order_id=None,
		placed_at_utc=_NOW_ISO,
	)
	if active_status == "exit_pending":
		conn.execute(
			"UPDATE live_trades SET status='exit_pending' WHERE id=?", (rid,)
		)
		conn.commit()

	changed = touch_reconciled(conn, rid, now_utc=_NOW_ISO)

	assert changed is True
	assert _row(conn, rid)["reconciled_at_utc"] == _NOW_ISO
	assert _row(conn, rid)["status"] == active_status


def test_touch_reconciled_is_idempotent_advances_timestamp(
	conn: sqlite3.Connection,
) -> None:
	"""Calling it twice just rewrites the timestamp — no error, the second
	value wins (idempotent re-touch across two reconcile passes)."""
	rid = _seed_open(conn, coid="touch-idem")
	first = "2026-05-16T12:00:00+00:00"
	second = "2026-05-16T12:00:30+00:00"

	assert touch_reconciled(conn, rid, now_utc=first) is True
	assert _row(conn, rid)["reconciled_at_utc"] == first

	# Second pass: a pure re-touch (no error, timestamp advances).
	assert touch_reconciled(conn, rid, now_utc=second) is True
	assert _row(conn, rid)["reconciled_at_utc"] == second
	assert _row(conn, rid)["status"] == "open"


def test_touch_reconciled_terminal_row_is_logged_noop(
	conn: sqlite3.Connection, caplog: pytest.LogCaptureFixture
) -> None:
	"""CAS lost-race / precondition-miss (mirror of
	test_cas_mark_lost_truth_only_from_active): a row that has since gone
	terminal (won) is NOT bumped — touch_reconciled returns the falsy no-op
	value, logs the shared 'CAS lost race' WARNING, and leaves the row (incl.
	its NULL reconciled_at_utc) byte-for-byte unchanged. NOT an error."""
	rid = _seed_open(conn, coid="touch-terminal")
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
	assert snapshot["reconciled_at_utc"] is None

	with caplog.at_level("WARNING"):
		changed = touch_reconciled(conn, rid, now_utc=_NOW_ISO)

	assert changed is False, "precondition miss must return the falsy no-op"
	assert _row(conn, rid) == snapshot, (
		"a terminal row must be byte-for-byte unchanged (no blind write)"
	)
	assert _row(conn, rid)["reconciled_at_utc"] is None
	assert any(
		"CAS lost race" in r.message for r in caplog.records
	), "a precondition-miss must log the shared CAS-lost-race WARNING"


def test_touch_reconciled_missing_row_is_noop(
	conn: sqlite3.Connection,
) -> None:
	"""A nonexistent row id is a logged no-op (return False), never an
	error — mirrors _cas_update's rowcount==0 contract."""
	assert touch_reconciled(conn, 99999, now_utc=_NOW_ISO) is False
