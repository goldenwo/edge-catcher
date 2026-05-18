"""Read API for live_trades.db — consumed by engine/risk.py (Gate, RiskContext).

This module is owned by sub-project B.  C's PR shipped it with
STUB_MODE = True so Agent A's risk.py could import and test against the
correct signatures without waiting for B's schema to land.  B's PR (PR 5)
flips STUB_MODE to False and fills in the real SQL queries against the
``live_trades`` table (see storage/migrations/0003_create_live_trades.sql
and live/state.py for the write side).

Signature contract (pinned in C-spec §Cross-module contracts):
  read_open_positions(conn) -> list[OpenPosition]
  read_open_count(conn) -> int
  read_daily_pnl_cents(conn, today_utc: date) -> int

These are pure reads — no writes, no I/O outside the passed connection.
The reporting CLI opens live_trades.db read-only (``?mode=ro``); B's live
process opens it WAL (live/state.connect_live_trades_db) so concurrent
reads are safe.
"""
from __future__ import annotations

import sqlite3
from datetime import date, timedelta

from edge_catcher.engine.executor import OpenPosition

# ---------------------------------------------------------------------------
# B's PR (PR 5) flips this to False alongside the real queries below.  A
# release-gate test (tests/test_engine_live_db.py) asserts this is False so a
# regression that reverts it to True — which would make C's gate see zero
# open positions and zero daily P&L (silent gate-always-allows, Risk #7) —
# fails CI at merge time.
# ---------------------------------------------------------------------------
STUB_MODE: bool = False


def read_open_positions(conn: sqlite3.Connection) -> list[OpenPosition]:
	"""Return all open positions from live_trades.db.

	All rows WHERE status='open', ordered by ``id`` ASC (insertion order) for
	determinism — tests and C's equity mark-to-market rely on stable order.

	Returns:
		Ordered list of open positions.  Empty list when no positions exist
		or when STUB_MODE is True (C's PR only).
	"""
	if STUB_MODE:
		return []
	rows = conn.execute(
		"SELECT ticker, side, fill_size, blended_entry_cents "
		"FROM live_trades WHERE status='open' ORDER BY id ASC"
	).fetchall()
	return [
		OpenPosition(
			ticker=r[0],
			side=r[1],
			fill_size=r[2],
			blended_entry_cents=r[3],
		)
		for r in rows
	]


def read_open_count(conn: sqlite3.Connection) -> int:
	"""Return the count of active positions from live_trades.db.

	Counts each unique active position once.  A single position is in
	EXACTLY ONE of these states at any moment:
	  - 'open'         — entry filled, no exit in flight
	  - 'pending'      — entry POSTed, awaiting Kalshi confirmation
	  - 'exit_pending' — exit POSTed, awaiting Kalshi confirmation
	These are mutually exclusive per row, so this is a deduplicated
	active-row count — a row transitioning open ↔ exit_pending is never
	double-counted.

	Per B brainstorm: ``pending`` DELIBERATELY counts toward C's MAX_OPEN
	gate so a phantom pending row holds its slot until TTL fires; this
	prevents the 'NetworkError fired but Kalshi DID receive — strategy
	re-emits and we accidentally double up' attack.

	Returns:
		Number of active positions.  0 when none exist or when STUB_MODE is
		True.
	"""
	if STUB_MODE:
		return 0
	row = conn.execute(
		"SELECT COUNT(*) FROM live_trades "
		"WHERE status IN ('open', 'pending', 'exit_pending')"
	).fetchone()
	return int(row[0])


def read_daily_pnl_cents(conn: sqlite3.Connection, today_utc: date) -> int:
	"""Return the sum of pnl_cents for trades closed today (UTC).

	Window: half-open ``[today_utc 00:00:00 UTC, today_utc+1 00:00:00 UTC)``.
	This MUST match the boundary KillSwitch's auto-clear-at-midnight uses —
	if they diverge a daily-cap kill could clear at midnight while this read
	still counts losses against the prior day.

	The status filter is an **allowlist** — ``status IN ('won','lost',
	'scratch')`` — counting only realized trade P&L.  It deliberately
	EXCLUDES ``cancelled`` (operator intervention), ``lost_truth`` (Kalshi
	mismatch), ``rejected_post_hoc`` (TTL'd phantom, never traded), and
	``rejected`` (4xx/zero-fill, never traded).

	Args:
		conn: Open SQLite connection to live_trades.db.
		today_utc: The current UTC calendar date (date(), not datetime).

	Returns:
		Sum of pnl_cents for closed trades today.  0 when no closes today or
		when STUB_MODE is True.  Negative values represent net loss.
	"""
	if STUB_MODE:
		return 0
	day_start = f"{today_utc.isoformat()}T00:00:00"
	next_day = f"{(today_utc + timedelta(days=1)).isoformat()}T00:00:00"
	row = conn.execute(
		"SELECT COALESCE(SUM(pnl_cents), 0) FROM live_trades "
		"WHERE status IN ('won', 'lost', 'scratch') "
		"AND exit_time >= ? AND exit_time < ?",
		(day_start, next_day),
	).fetchone()
	return int(row[0])
