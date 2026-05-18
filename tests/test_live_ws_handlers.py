"""WS-handler tests — sub-project B / v1.6.0 PR 5, Agent 4.C.

Spec §Test strategy #22-#25 + the Risk #9 lost-race obligation: 4.A
implemented compare-and-swap on every transition; these tests PROVE it holds
under concurrent/interleaved/duplicate Kalshi events, **including the path
where the precondition LOSES the race** (4.A's ``_cas_update`` hits
``rowcount == 0``, logs the canonical ``live_trades CAS lost race`` WARNING,
and no-ops — the row is never corrupted back to an earlier state).

Every test runs against a REAL migrated SQLite DB
(``live.state.connect_live_trades_db`` applies 0003 + WAL). The DB is NEVER
mocked and ``live.state`` is NEVER stubbed — these assert actual row-state
transitions end to end through the merged 4.A write API, driven by the
in-process :class:`MockKalshiWS` (each ``await emit_*`` returns only after
the handler has fully processed the event, so the row post-state is
deterministically observable on return).

Lost-race coverage map (each handler drives at least one real rowcount-0):
* #22 on_fill_event       — fill for a row a concurrent settlement closed.
* #23 on_fill_event       — duplicate partial WS event (same kalshi id).
* #24 on_settlement_event — settlement vs exit_pending, then a late fill.
* #25 on_order_status     — reject arriving after a fill moved row → open.
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

import pytest

from edge_catcher.live.state import (
	connect_live_trades_db,
	record_open,
	record_pending,
)
from edge_catcher.live.ws_handlers import (
	StoreCallbacks,
	on_fill_event,
	on_order_status_event,
	on_settlement_event,
)
from tests.fixtures.mock_kalshi_ws import MockKalshiWS

_NOW_ISO = "2026-05-16T12:00:00+00:00"

# The canonical WARNING substring 4.A's _cas_update emits on a lost race
# (live/state.py::_cas_update). Asserting on it is how we PROVE a test
# actually exercised the rowcount-0 path rather than an incidental no-op.
_CAS_LOST_RACE = "CAS lost race"


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def conn(tmp_path: Path) -> sqlite3.Connection:
	"""Real on-disk live_trades.db with 0003 applied + WAL enabled."""
	c = connect_live_trades_db(tmp_path / "live_trades.db")
	yield c
	c.close()


@pytest.fixture
def cbs() -> StoreCallbacks:
	"""StoreCallbacks with no downstream effect wired (unit-test default —
	the handler treats a None settlement callback as a valid no-op)."""
	return StoreCallbacks()


def _status(conn: sqlite3.Connection, row_id: int) -> str:
	r = conn.execute(
		"SELECT status FROM live_trades WHERE id = ?", (row_id,)
	).fetchone()
	assert r is not None, f"row {row_id} missing"
	return str(r[0])


def _row(conn: sqlite3.Connection, row_id: int) -> dict[str, object]:
	conn.row_factory = sqlite3.Row
	r = conn.execute(
		"SELECT * FROM live_trades WHERE id = ?", (row_id,)
	).fetchone()
	conn.row_factory = None
	assert r is not None, f"row {row_id} missing"
	return dict(r)


def _seed_pending(
	conn: sqlite3.Connection,
	*,
	coid: str = "strat-34-KXSOL15M-1700000000000-cafebabe",
	ticker: str = "KXSOL15M-26MAY16H12",
	side: str = "yes",
	intended_size: int = 10,
) -> int:
	return record_pending(
		conn,
		ticker=ticker,
		series="KXSOL15M",
		strategy="strat-34",
		side=side,
		intended_size=intended_size,
		entry_price_cents=40,
		stop_loss_distance_cents=None,
		client_order_id=coid,
		kalshi_order_id=None,
		placed_at_utc=_NOW_ISO,
	)


def _seed_open(
	conn: sqlite3.Connection,
	*,
	coid: str = "strat-34-KXSOL15M-1700000000000-deadbeef",
	ticker: str = "KXSOL15M-26MAY16H12",
	side: str = "yes",
	intended_size: int = 10,
	fill_size: int = 10,
	blended: int = 40,
	entry_fee: int = 17,
) -> int:
	return record_open(
		conn,
		ticker=ticker,
		series="KXSOL15M",
		strategy="strat-34",
		side=side,
		intended_size=intended_size,
		fill_size=fill_size,
		entry_price_cents=40,
		blended_entry_cents=blended,
		slippage_cents=0,
		fill_pct=fill_size / intended_size,
		stop_loss_distance_cents=20,
		client_order_id=coid,
		kalshi_order_id="kx-entry-1",
		placed_at_utc=_NOW_ISO,
		entry_time=_NOW_ISO,
		entry_fee_cents=entry_fee,
	)


def _force_settled(conn: sqlite3.Connection, row_id: int, status: str = "won") -> None:
	"""Drive a row terminal directly (simulates a concurrent settlement that
	committed before the racing handler's UPDATE) so the next handler call
	hits 4.A's rowcount-0 lost-race path deterministically."""
	conn.execute(
		"UPDATE live_trades SET status = ?, exit_price_cents = 100, "
		"exit_time = ?, exit_reason = 'settlement' WHERE id = ?",
		(status, _NOW_ISO, row_id),
	)
	conn.commit()


def _make_exit_pending(conn: sqlite3.Connection, row_id: int) -> None:
	"""open → exit_pending directly (that edge is D's dispatch path, out of
	4.C scope; 4.A has no helper for it — mirrors the reconciliation tests)."""
	conn.execute(
		"UPDATE live_trades SET status = 'exit_pending' WHERE id = ?",
		(row_id,),
	)
	conn.commit()


def _wire(
	ws: MockKalshiWS, conn: sqlite3.Connection, cbs: StoreCallbacks
) -> None:
	ws.register(
		db=conn,
		store_callbacks=cbs,
		on_fill=on_fill_event,
		on_order_status=on_order_status_event,
		on_settlement=on_settlement_event,
	)


# ===========================================================================
# #22 — on_fill_event: pending → open  (+ lost-race: fill for a settled row)
# ===========================================================================


@pytest.mark.asyncio
async def test_22_fill_pending_to_open(
	conn: sqlite3.Connection, cbs: StoreCallbacks, mock_kalshi_ws: MockKalshiWS
) -> None:
	"""Happy path: a Kalshi fill matching a pending row's client_order_id
	drives pending → open via 4.A's transition_pending_to_open, with the
	blended price computed from the per-fill array (fill_math)."""
	row_id = _seed_pending(conn)
	_wire(mock_kalshi_ws, conn, cbs)

	await mock_kalshi_ws.emit_fill(
		client_order_id="strat-34-KXSOL15M-1700000000000-cafebabe",
		kalshi_order_id="kx-entry-22",
		filled_count=10,
		# two-level walk: 40¢×6 + 41¢×4 → blended round(404/10)=40
		fills=[{"price": 40, "size": 6}, {"price": 41, "size": 4}],
		ticker="KXSOL15M-26MAY16H12",
		side="yes",
	)

	row = _row(conn, row_id)
	assert row["status"] == "open"
	assert row["kalshi_order_id"] == "kx-entry-22"
	assert row["fill_size"] == 10
	assert row["blended_entry_cents"] == 40
	assert row["entry_time"] is not None
	# entry fee seeded from STANDARD_FEE at the WS-fill blended price.
	assert row["entry_fee_cents"] is not None and row["entry_fee_cents"] > 0
	assert row["entry_fee_remaining_cents"] == row["entry_fee_cents"]


@pytest.mark.asyncio
async def test_22_fill_after_concurrent_settlement_is_lost_race_noop(
	conn: sqlite3.Connection,
	cbs: StoreCallbacks,
	mock_kalshi_ws: MockKalshiWS,
	caplog: pytest.LogCaptureFixture,
) -> None:
	"""🚨 Risk #9 lost-race: a concurrent settlement closed the row to 'won'
	BEFORE the (delayed) entry-fill event lands. The fill handler calls 4.A's
	transition_pending_to_open unconditionally; its WHERE status='pending'
	CAS hits rowcount-0 → logged no-op. The row MUST stay 'won' (NOT
	corrupted back to 'open') and the canonical lost-race WARNING MUST fire
	(proves the rowcount-0 path was actually exercised)."""
	row_id = _seed_pending(conn)
	# A settlement raced ahead and closed the pending row (its CAS allows
	# any active state via record_close in production; here we force the
	# committed terminal state the racing fill will collide with).
	_force_settled(conn, row_id, status="won")
	_wire(mock_kalshi_ws, conn, cbs)

	with caplog.at_level(logging.WARNING, logger="edge_catcher.live.state"):
		await mock_kalshi_ws.emit_fill(
			client_order_id="strat-34-KXSOL15M-1700000000000-cafebabe",
			kalshi_order_id="kx-entry-22b",
			filled_count=10,
			fills=[{"price": 41, "size": 10}],
			ticker="KXSOL15M-26MAY16H12",
			side="yes",
		)

	# Row is NOT corrupted back to open; the settlement outcome stands.
	assert _status(conn, row_id) == "won"
	assert _row(conn, row_id)["exit_price_cents"] == 100
	# PROOF the rowcount-0 lost-race path ran (not an incidental skip).
	assert any(
		_CAS_LOST_RACE in r.message and "pending->open" in r.message
		for r in caplog.records
	), f"expected a 4.A CAS lost-race WARNING; got {[r.message for r in caplog.records]!r}"


@pytest.mark.asyncio
async def test_22_duplicate_fill_after_open_is_idempotent_noop(
	conn: sqlite3.Connection,
	cbs: StoreCallbacks,
	mock_kalshi_ws: MockKalshiWS,
	caplog: pytest.LogCaptureFixture,
) -> None:
	"""Reconnect re-delivers the SAME entry fill. The row is already 'open';
	the duplicate is matched by kalshi_order_id and 4.A's CAS no-ops it
	(rowcount-0 on WHERE status='pending'). Row state is unchanged — the
	UNIQUE/CAS make the second apply idempotent (spec Risk #2)."""
	row_id = _seed_pending(conn)
	_wire(mock_kalshi_ws, conn, cbs)
	fill_kwargs = dict(
		client_order_id="strat-34-KXSOL15M-1700000000000-cafebabe",
		kalshi_order_id="kx-entry-22c",
		filled_count=10,
		fills=[{"price": 40, "size": 10}],
		ticker="KXSOL15M-26MAY16H12",
		side="yes",
	)
	await mock_kalshi_ws.emit_fill(**fill_kwargs)  # type: ignore[arg-type]
	first = _row(conn, row_id)
	assert first["status"] == "open"

	with caplog.at_level(logging.WARNING, logger="edge_catcher.live.state"):
		await mock_kalshi_ws.emit_fill(**fill_kwargs)  # type: ignore[arg-type]

	second = _row(conn, row_id)
	assert second == first, "duplicate entry fill must be a pure no-op"
	assert any(_CAS_LOST_RACE in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_22_partial_entry_fill_writes_true_fill_pct_not_hardcoded_one(
	conn: sqlite3.Connection, cbs: StoreCallbacks, mock_kalshi_ws: MockKalshiWS
) -> None:
	"""Kalshi marks a partial IOC entry 'executed' too (fills what it can,
	cancels the rest). A 3-of-10 entry fill must record the TRUE
	fill_pct = fill_size / intended_size (0.3), NOT a hardcoded 1.0 — the DDL
	0003 contract is `fill_size / intended_size`, and the reconcile path
	already computes the real fraction; the WS path must agree or F's
	slippage/partial-fill analytics diverge by which path booked the fill."""
	row_id = _seed_pending(conn, intended_size=10)
	_wire(mock_kalshi_ws, conn, cbs)

	await mock_kalshi_ws.emit_fill(
		client_order_id="strat-34-KXSOL15M-1700000000000-cafebabe",
		kalshi_order_id="kx-entry-22part",
		filled_count=3,
		fills=[{"price": 40, "size": 3}],  # IOC partial: 3 of 10
		ticker="KXSOL15M-26MAY16H12",
		side="yes",
	)

	row = _row(conn, row_id)
	assert row["status"] == "open"
	assert row["fill_size"] == 3
	assert row["intended_size"] == 10, "intended_size unmutated by the fill"
	assert row["fill_pct"] == pytest.approx(3 / 10), (
		f"WS-path partial entry fill must write the true "
		f"fill_pct=fill_size/intended_size (0.3), not a hardcoded 1.0; got "
		f"{row['fill_pct']!r}"
	)
	assert row["fill_pct"] != 1.0, (
		"a 3-of-10 partial entry mis-reported as 100% defeats F's "
		"slippage/partial-fill analytics and diverges from the reconcile path"
	)


@pytest.mark.asyncio
async def test_22_full_entry_fill_still_writes_fill_pct_one(
	conn: sqlite3.Connection, cbs: StoreCallbacks, mock_kalshi_ws: MockKalshiWS
) -> None:
	"""Regression guard: the common 100% entry fill must still yield
	fill_pct == 1.0 — the fix computes the real fraction, it must not break
	the full-fill path (mirrors reconcile's full-fill regression guard)."""
	row_id = _seed_pending(conn, intended_size=10)
	_wire(mock_kalshi_ws, conn, cbs)

	await mock_kalshi_ws.emit_fill(
		client_order_id="strat-34-KXSOL15M-1700000000000-cafebabe",
		kalshi_order_id="kx-entry-22full",
		filled_count=10,
		fills=[{"price": 40, "size": 10}],
		ticker="KXSOL15M-26MAY16H12",
		side="yes",
	)

	row = _row(conn, row_id)
	assert row["status"] == "open"
	assert row["fill_size"] == 10
	assert row["fill_pct"] == pytest.approx(1.0)


# ===========================================================================
# #23 — partial fill on open → split row  (+ duplicate same kalshi-id no-op)
# ===========================================================================


@pytest.mark.asyncio
async def test_23_partial_exit_fill_splits_row(
	conn: sqlite3.Connection, cbs: StoreCallbacks, mock_kalshi_ws: MockKalshiWS
) -> None:
	"""An exit fill closing M<N contracts splits the row via 4.A's
	record_partial_exit: parent fill_size decremented by M, a terminal child
	row inserted for the M closed contracts."""
	parent_id = _seed_open(conn, fill_size=10, blended=40)
	_wire(mock_kalshi_ws, conn, cbs)

	# Exit order's own client_order_id is FRESH (D's build_exit_order) — it
	# matches no row; the handler resolves the parent by ticker+side.
	await mock_kalshi_ws.emit_fill(
		client_order_id="strat-34-KXSOL15M-1700000000777-exit0001",
		kalshi_order_id="kx-exit-23",
		filled_count=4,
		fills=[{"price": 55, "size": 4}],  # 55 > 40 entry → won child
		ticker="KXSOL15M-26MAY16H12",
		side="yes",
	)

	parent = _row(conn, parent_id)
	assert parent["status"] == "open"
	assert parent["fill_size"] == 6, "parent decremented by M=4"
	children = conn.execute(
		"SELECT id, status, fill_size, exit_price_cents, kalshi_order_id "
		"FROM live_trades WHERE id != ? ORDER BY id",
		(parent_id,),
	).fetchall()
	assert len(children) == 1
	child = children[0]
	assert child[1] == "won"
	assert child[2] == 4
	assert child[3] == 55
	assert child[4] == "kx-exit-23"


@pytest.mark.asyncio
async def test_23_duplicate_partial_same_kalshi_id_is_idempotent_noop(
	conn: sqlite3.Connection, cbs: StoreCallbacks, mock_kalshi_ws: MockKalshiWS
) -> None:
	"""🚨 The duplicate-partial no-op (spec §297 CORRECTION 2026-05-16):
	a re-delivered partial WS event repeats the IDENTICAL kalshi_order_id.
	4.A's record_partial_exit dedups on kalshi_exit_order_id — the second
	emit must be a pure no-op: parent NOT double-decremented, NO phantom
	second child. (A FRESH kalshi id would be a legitimate new partial — so
	this test re-emits the SAME id to hit the real dedup path.)"""
	parent_id = _seed_open(conn, fill_size=10, blended=40)
	_wire(mock_kalshi_ws, conn, cbs)
	dup_kwargs = dict(
		client_order_id="strat-34-KXSOL15M-1700000000777-exit0001",
		kalshi_order_id="kx-exit-23DUP",  # the stable Kalshi identity
		filled_count=4,
		fills=[{"price": 55, "size": 4}],
		ticker="KXSOL15M-26MAY16H12",
		side="yes",
	)
	await mock_kalshi_ws.emit_fill(**dup_kwargs)  # type: ignore[arg-type]
	parent_after_first = _row(conn, parent_id)
	children_after_first = conn.execute(
		"SELECT id FROM live_trades WHERE id != ?", (parent_id,)
	).fetchall()
	assert parent_after_first["fill_size"] == 6
	assert len(children_after_first) == 1

	# Re-deliver the EXACT same event (same kalshi_order_id).
	await mock_kalshi_ws.emit_fill(**dup_kwargs)  # type: ignore[arg-type]

	parent_after_dup = _row(conn, parent_id)
	children_after_dup = conn.execute(
		"SELECT id FROM live_trades WHERE id != ?", (parent_id,)
	).fetchall()
	assert parent_after_dup["fill_size"] == 6, (
		"parent must NOT be double-decremented on a duplicate-same-id "
		"partial WS event (spec §297)"
	)
	assert children_after_dup == children_after_first, (
		"no phantom second child on the same-kalshi-id duplicate"
	)


@pytest.mark.asyncio
async def test_23_fresh_kalshi_id_is_a_new_partial_not_a_dedup(
	conn: sqlite3.Connection, cbs: StoreCallbacks, mock_kalshi_ws: MockKalshiWS
) -> None:
	"""Counter-test to #23's dedup: a partial with a DIFFERENT kalshi id is
	a legitimate second partial (NOT a duplicate). Two partials of 4 + 3
	leave the parent at 3 with two distinct children — proves the dedup keys
	on kalshi id, not on the mere fact of a second partial event."""
	parent_id = _seed_open(conn, fill_size=10, blended=40)
	_wire(mock_kalshi_ws, conn, cbs)
	await mock_kalshi_ws.emit_fill(
		client_order_id="strat-34-KXSOL15M-1700000000777-exitA",
		kalshi_order_id="kx-exit-A",
		filled_count=4,
		fills=[{"price": 55, "size": 4}],
		ticker="KXSOL15M-26MAY16H12",
		side="yes",
	)
	await mock_kalshi_ws.emit_fill(
		client_order_id="strat-34-KXSOL15M-1700000000888-exitB",
		kalshi_order_id="kx-exit-B",  # DIFFERENT id → new partial
		filled_count=3,
		fills=[{"price": 30, "size": 3}],  # 30 < 40 → lost child
		ticker="KXSOL15M-26MAY16H12",
		side="yes",
	)

	parent = _row(conn, parent_id)
	assert parent["fill_size"] == 3, "10 - 4 - 3"
	children = conn.execute(
		"SELECT status, fill_size, kalshi_order_id FROM live_trades "
		"WHERE id != ? ORDER BY id",
		(parent_id,),
	).fetchall()
	assert len(children) == 2
	assert {c[2] for c in children} == {"kx-exit-A", "kx-exit-B"}
	assert children[0][0] == "won" and children[1][0] == "lost"


@pytest.mark.asyncio
async def test_23_partial_exit_fill_with_empty_kalshi_id_is_rejected_not_booked(
	conn: sqlite3.Connection,
	cbs: StoreCallbacks,
	mock_kalshi_ws: MockKalshiWS,
	caplog: pytest.LogCaptureFixture,
) -> None:
	"""An exit fill that arrives with NO Kalshi order_id (empty) has no
	idempotency identity. record_partial_exit dedups on kalshi_exit_order_id,
	so booking an empty-keyed partial would let two genuinely-distinct
	empty-id fills collapse into one (silent real-money under-count — the 2nd
	tranche's closed contracts are never booked) OR a reconnect re-delivery
	double-decrement the parent. Zero-error contract: a partial exit with no
	Kalshi identity is REJECTED loud (ERROR) and NOT written — the parent
	stays fully 'open' for reconcile / next-tick recovery (mirrors
	on_fill_event's other 'no trustworthy data → no-op, reconcile owns
	recovery' guards)."""
	parent_id = _seed_open(conn, fill_size=10, blended=40)
	_wire(mock_kalshi_ws, conn, cbs)

	with caplog.at_level(logging.ERROR, logger="edge_catcher.live.state"):
		await mock_kalshi_ws.emit_fill(
			client_order_id="strat-34-KXSOL15M-1700000000777-exitNOID",
			kalshi_order_id="",  # malformed/missing Kalshi identity
			filled_count=4,
			fills=[{"price": 55, "size": 4}],
			ticker="KXSOL15M-26MAY16H12",
			side="yes",
		)

	parent = _row(conn, parent_id)
	assert parent["status"] == "open", "parent must remain open"
	assert parent["fill_size"] == 10, (
		"an exit fill with no Kalshi order_id has no idempotency key — it "
		"must NOT decrement the parent (un-dedupable: would silently collapse "
		"distinct fills or double-book a re-delivery); leave it for reconcile"
	)
	children = conn.execute(
		"SELECT id FROM live_trades WHERE id != ?", (parent_id,)
	).fetchall()
	assert children == [], "no child row may be booked for an un-keyed exit"
	assert any(
		r.levelno >= logging.ERROR
		and ("kalshi_exit_order_id" in r.message or "order_id" in r.message)
		for r in caplog.records
	), "expected a loud ERROR that the exit fill had no Kalshi order id"


@pytest.mark.asyncio
async def test_23_full_exit_fill_closes_in_place_with_fee_correct_pnl(
	conn: sqlite3.Connection, cbs: StoreCallbacks, mock_kalshi_ws: MockKalshiWS
) -> None:
	"""An exit fill closing ALL contracts is a full close (record_close,
	UPDATE in place — no split child). Locks the DDL pnl arithmetic
	(``exit - entry - entry_fee - exit_fee``): the still-owed entry fee MUST
	be subtracted into pnl_cents (4.A's record_close does NOT recompute it).

	entry_fee=17 (seeded). exit blended=60, entry=40, size=10.
	STANDARD_FEE(60,10) = ceil(0.07*10*0.6*0.4*100) = ceil(16.8) = 17 exit fee.
	pnl = 10*(60-40) - 17 (entry) - 17 (exit) = 200 - 34 = 166."""
	parent_id = _seed_open(conn, fill_size=10, blended=40, entry_fee=17)
	_wire(mock_kalshi_ws, conn, cbs)

	await mock_kalshi_ws.emit_fill(
		client_order_id="strat-34-KXSOL15M-1700000000777-fullexit",
		kalshi_order_id="kx-exit-full",
		filled_count=10,
		fills=[{"price": 60, "size": 10}],
		ticker="KXSOL15M-26MAY16H12",
		side="yes",
	)

	row = _row(conn, parent_id)
	assert row["status"] == "won"
	assert row["fill_size"] == 10  # full close UPDATEs in place
	assert row["exit_price_cents"] == 60
	assert row["exit_fee_cents"] == 17
	assert row["pnl_cents"] == 166, (
		"full-close pnl must subtract BOTH the still-owed entry fee and the "
		"exit fee (DDL: exit - entry - entry_fee - exit_fee)"
	)
	# record_close consumes the remaining entry-fee allocation into
	# entry_fee_cents and zeroes the remainder (no rounding fragment lost).
	assert row["entry_fee_cents"] == 17
	assert row["entry_fee_remaining_cents"] == 0
	# Exactly one row — no phantom split child on a full close.
	assert conn.execute("SELECT COUNT(*) FROM live_trades").fetchone()[0] == 1


# ===========================================================================
# #24 — on_settlement_event: open + exit_pending → settled
#        (+ supersede note; + lost-race: late fill after settlement)
# ===========================================================================


@pytest.mark.asyncio
async def test_24_settlement_closes_open_and_exit_pending_with_supersede_note(
	conn: sqlite3.Connection, cbs: StoreCallbacks, mock_kalshi_ws: MockKalshiWS
) -> None:
	"""A market_settlement closes EVERY active row for the ticker. An
	exit_pending row is superseded by settlement (settlement wins) and gets
	the audited supersede note; an open row closes normally. YES settles 100
	→ a yes-side row wins."""
	open_id = _seed_open(
		conn, coid="cid-open-24", ticker="KXSOL15M-26MAY16H12", side="yes"
	)
	exitp_id = _seed_open(
		conn, coid="cid-exitp-24", ticker="KXSOL15M-26MAY16H12", side="yes"
	)
	_make_exit_pending(conn, exitp_id)
	_wire(mock_kalshi_ws, conn, cbs)

	await mock_kalshi_ws.emit_settlement(
		ticker="KXSOL15M-26MAY16H12", settlement_price_cents=100
	)

	open_row = _row(conn, open_id)
	exitp_row = _row(conn, exitp_id)
	assert open_row["status"] == "won"
	assert open_row["exit_price_cents"] == 100
	assert open_row["exit_reason"] == "settlement"
	assert open_row["exit_fee_cents"] == 0  # no Kalshi fee at settlement
	assert open_row["notes"] is None
	assert exitp_row["status"] == "won"
	assert exitp_row["exit_reason"] == "settlement"
	assert exitp_row["notes"] == "settlement superseded in-flight exit"


@pytest.mark.asyncio
async def test_24_settlement_no_side_outcome_and_pnl(
	conn: sqlite3.Connection, cbs: StoreCallbacks, mock_kalshi_ws: MockKalshiWS
) -> None:
	"""NO-side row + settles NO (0¢): NO wins. P&L = size*(payout-entry) -
	entry_fee_remaining; NO payout when YES settles 0¢ is 100¢."""
	row_id = _seed_open(
		conn,
		coid="cid-no-24",
		ticker="KXETH15M-26MAY16H12",
		side="no",
		fill_size=10,
		blended=30,
		entry_fee=15,
	)
	_wire(mock_kalshi_ws, conn, cbs)

	await mock_kalshi_ws.emit_settlement(
		ticker="KXETH15M-26MAY16H12", settlement_price_cents=0
	)

	row = _row(conn, row_id)
	assert row["status"] == "won", "no-side wins when market settles NO (0¢)"
	# payout(no) = 100 - 0 = 100; pnl = 10*(100-30) - 15 = 685
	assert row["pnl_cents"] == 685
	assert row["exit_price_cents"] == 0


@pytest.mark.asyncio
async def test_24_late_fill_after_settlement_is_lost_race_noop(
	conn: sqlite3.Connection,
	cbs: StoreCallbacks,
	mock_kalshi_ws: MockKalshiWS,
	caplog: pytest.LogCaptureFixture,
) -> None:
	"""🚨 Risk #4 no-active-row short-circuit (NOT a rowcount-0 CAS lost
	race): settlement closes the row, THEN a late exit fill for that ticker
	arrives. The fill handler finds no open parent (settlement already
	closed it) → it short-circuits and returns BEFORE any 4.A write — so
	there is deliberately no `_cas_update` rowcount-0 / WARNING here (that
	path is covered by #22/#24's CAS-race siblings). The idempotent no-op is
	proven by the settled row being byte-unchanged AND exactly one (closed)
	row existing, i.e. no phantom exit child was minted."""
	row_id = _seed_open(
		conn, coid="cid-late-24", ticker="KXSOL15M-26MAY16H12", side="yes"
	)
	_wire(mock_kalshi_ws, conn, cbs)
	await mock_kalshi_ws.emit_settlement(
		ticker="KXSOL15M-26MAY16H12", settlement_price_cents=100
	)
	settled = _row(conn, row_id)
	assert settled["status"] == "won"

	# Late exit fill for the now-closed position.
	await mock_kalshi_ws.emit_fill(
		client_order_id="strat-34-KXSOL15M-1700000000999-lateexit",
		kalshi_order_id="kx-exit-late",
		filled_count=10,
		fills=[{"price": 70, "size": 10}],
		ticker="KXSOL15M-26MAY16H12",
		side="yes",
	)

	assert _row(conn, row_id) == settled, "settled row must be untouched"
	all_rows = conn.execute("SELECT COUNT(*) FROM live_trades").fetchone()
	assert all_rows[0] == 1, "no phantom child from the late exit fill"


@pytest.mark.asyncio
async def test_24_settlement_then_settlement_is_idempotent(
	conn: sqlite3.Connection,
	cbs: StoreCallbacks,
	mock_kalshi_ws: MockKalshiWS,
	caplog: pytest.LogCaptureFixture,
) -> None:
	"""A re-delivered settlement event (reconnect) for an already-settled
	ticker: the second pass finds no active rows → no-op; the first close
	stands. Drives the settlement handler's empty-rows branch deterministically."""
	row_id = _seed_open(
		conn, coid="cid-dupsettle-24", ticker="KXSOL15M-26MAY16H12", side="yes"
	)
	_wire(mock_kalshi_ws, conn, cbs)
	await mock_kalshi_ws.emit_settlement(
		ticker="KXSOL15M-26MAY16H12", settlement_price_cents=100
	)
	first = _row(conn, row_id)

	await mock_kalshi_ws.emit_settlement(
		ticker="KXSOL15M-26MAY16H12", settlement_price_cents=100
	)
	assert _row(conn, row_id) == first, "second settlement is a pure no-op"


@pytest.mark.asyncio
async def test_24_settlement_record_close_loses_cas_race_is_noop(
	conn: sqlite3.Connection,
	cbs: StoreCallbacks,
	mock_kalshi_ws: MockKalshiWS,
	caplog: pytest.LogCaptureFixture,
	monkeypatch: pytest.MonkeyPatch,
) -> None:
	"""🚨 Risk #9 — the EXACT spec §950 settlement-handler worst case: the
	settlement handler reads the row as active ('open') into its in-memory
	loop, but a concurrent writer commits the row terminal BEFORE the
	handler's record_close UPDATE runs. record_close's CAS predicate
	(WHERE status IN ('open','exit_pending')) then hits rowcount-0 and
	no-ops — the row is NOT double-closed / corrupted, and the canonical
	4.A 'CAS lost race' WARNING fires (proves the rowcount-0 path executed
	inside the settlement handler, not an incidental skip).

	The racing writer is injected by wrapping ws_handlers.record_close so it
	flips the row to 'lost' (committed) immediately BEFORE delegating to the
	real record_close — faithfully reproducing the SELECT-then-stale-UPDATE
	interleave the spec's CAS mitigation exists to defeat."""
	row_id = _seed_open(
		conn, coid="cid-cas-24", ticker="KXSOL15M-26MAY16H12", side="yes"
	)
	_wire(mock_kalshi_ws, conn, cbs)

	import edge_catcher.live.ws_handlers as wsh

	real_record_close = wsh.record_close
	calls: list[int] = []

	def racing_record_close(c: sqlite3.Connection, rid: int, **kw: object) -> None:
		# Simulate a concurrent writer that committed the row terminal
		# between the settlement handler's SELECT and this UPDATE.
		if not calls:
			calls.append(rid)
			c.execute(
				"UPDATE live_trades SET status='lost', exit_price_cents=0, "
				"exit_time=?, exit_reason='settlement' WHERE id=?",
				(_NOW_ISO, rid),
			)
			c.commit()
		# Now delegate to the REAL 4.A record_close — its CAS must rowcount-0.
		real_record_close(c, rid, **kw)  # type: ignore[arg-type]

	monkeypatch.setattr(wsh, "record_close", racing_record_close)

	with caplog.at_level(logging.WARNING, logger="edge_catcher.live.state"):
		await mock_kalshi_ws.emit_settlement(
			ticker="KXSOL15M-26MAY16H12", settlement_price_cents=100
		)

	# The racing writer's 'lost'/0¢ close stands; settlement's record_close
	# CAS-lost and did NOT overwrite it to 'won'/100¢.
	row = _row(conn, row_id)
	assert row["status"] == "lost"
	assert row["exit_price_cents"] == 0
	assert any(
		_CAS_LOST_RACE in r.message and "->won" in r.message
		for r in caplog.records
	), (
		"expected record_close's 4.A CAS lost-race WARNING inside the "
		f"settlement handler; got {[r.message for r in caplog.records]!r}"
	)


# ===========================================================================
# #25 — on_order_status_event: pending → rejected
#        (+ lost-race: reject after a fill already moved row → open)
# ===========================================================================


@pytest.mark.asyncio
async def test_25_order_status_pending_to_rejected(
	conn: sqlite3.Connection, cbs: StoreCallbacks, mock_kalshi_ws: MockKalshiWS
) -> None:
	"""Kalshi sends a post-submit rejection before our reconcile: the
	pending row goes pending → rejected via 4.A transition_pending_to_rejected,
	carrying the Kalshi-reported rejection_reason."""
	row_id = _seed_pending(conn, coid="cid-rej-25")
	_wire(mock_kalshi_ws, conn, cbs)

	await mock_kalshi_ws.emit_order_status(
		client_order_id="cid-rej-25",
		status="rejected",
		rejection_reason="post_submit_risk_check_failed",
		kalshi_order_id="kx-rej-25",
	)

	row = _row(conn, row_id)
	assert row["status"] == "rejected"
	assert row["rejection_reason"] == "post_submit_risk_check_failed"
	assert row["kalshi_order_id"] == "kx-rej-25"


@pytest.mark.asyncio
async def test_25_reject_after_fill_opened_row_is_lost_race_noop(
	conn: sqlite3.Connection,
	cbs: StoreCallbacks,
	mock_kalshi_ws: MockKalshiWS,
	caplog: pytest.LogCaptureFixture,
) -> None:
	"""🚨 Risk #9 lost-race: a fill already moved the row pending → open, and
	a STALE order_status=rejected arrives afterward. The handler calls 4.A's
	transition_pending_to_rejected unconditionally; its WHERE status='pending'
	CAS hits rowcount-0 → logged no-op. The row MUST stay 'open' (NOT
	corrupted to 'rejected'); the canonical lost-race WARNING MUST fire."""
	row_id = _seed_pending(conn, coid="cid-rej-25b")
	_wire(mock_kalshi_ws, conn, cbs)
	# A fill won the race first → row is now 'open'.
	await mock_kalshi_ws.emit_fill(
		client_order_id="cid-rej-25b",
		kalshi_order_id="kx-entry-25b",
		filled_count=10,
		fills=[{"price": 40, "size": 10}],
		ticker="KXSOL15M-26MAY16H12",
		side="yes",
	)
	assert _status(conn, row_id) == "open"

	with caplog.at_level(logging.WARNING, logger="edge_catcher.live.state"):
		await mock_kalshi_ws.emit_order_status(
			client_order_id="cid-rej-25b",
			status="rejected",
			rejection_reason="stale_reject_should_be_ignored",
		)

	assert _status(conn, row_id) == "open", (
		"a stale reject after a fill opened the row must NOT corrupt it"
	)
	assert any(
		_CAS_LOST_RACE in r.message and "pending->rejected" in r.message
		for r in caplog.records
	), f"expected a 4.A CAS lost-race WARNING; got {[r.message for r in caplog.records]!r}"


@pytest.mark.asyncio
async def test_25_non_terminal_status_is_noop(
	conn: sqlite3.Connection, cbs: StoreCallbacks, mock_kalshi_ws: MockKalshiWS
) -> None:
	"""A benign lifecycle ping (status='resting') is NOT actioned by the
	order_status handler — the pending row is untouched (the fill/settlement
	handlers own the active path)."""
	row_id = _seed_pending(conn, coid="cid-resting-25")
	_wire(mock_kalshi_ws, conn, cbs)

	await mock_kalshi_ws.emit_order_status(
		client_order_id="cid-resting-25", status="resting"
	)

	assert _status(conn, row_id) == "pending"
