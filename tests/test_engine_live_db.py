"""Unit tests for edge_catcher.engine.live_db — C's read API over live_trades.

Spec §Test strategy items #12-#14 (incl. #14b half-open day-window boundary)
+ the Risk #7 STUB_MODE release gate. Real migrated SQLite throughout
(live/state.connect_live_trades_db); the DB is never mocked.
"""
from __future__ import annotations

import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from edge_catcher.engine import live_db
from edge_catcher.engine.executor import OpenPosition
from edge_catcher.engine.live_db import (
	read_daily_pnl_cents,
	read_open_count,
	read_open_positions,
)
from edge_catcher.live.state import connect_live_trades_db

_NOW_ISO = datetime(2026, 5, 16, 12, 0, 0, tzinfo=timezone.utc).isoformat()


@pytest.fixture
def conn(tmp_path: Path) -> sqlite3.Connection:
	c = connect_live_trades_db(tmp_path / "live_trades.db")
	yield c
	c.close()


def _insert(
	conn: sqlite3.Connection,
	*,
	status: str,
	coid: str,
	ticker: str = "KXT-1",
	side: str = "yes",
	fill_size: int = 10,
	blended: int = 40,
	pnl: int | None = None,
	exit_time: str | None = None,
) -> int:
	cur = conn.execute(
		"INSERT INTO live_trades (ticker, series, strategy, side, "
		"intended_size, original_intended_size, fill_size, "
		"entry_price_cents, blended_entry_cents, status, client_order_id, "
		"placed_at_utc, pnl_cents, exit_time) "
		"VALUES (?, 'S', 'st', ?, ?, ?, ?, 42, ?, ?, ?, ?, ?, ?)",
		(
			ticker,
			side,
			fill_size,
			fill_size,
			fill_size,
			blended,
			status,
			coid,
			_NOW_ISO,
			pnl,
			exit_time,
		),
	)
	conn.commit()
	return int(cur.lastrowid or 0)


# ---------------------------------------------------------------------------
# Risk #7 — STUB_MODE release gate
# ---------------------------------------------------------------------------


def test_stub_mode_is_false_release_gate() -> None:
	"""Risk #7: B's PR 5 ships STUB_MODE=False with the real queries. If a
	regression flips it back to True, C's gate would see zero open positions
	+ zero daily P&L (silent gate-always-allows with real money). CI must
	fail here at merge time."""
	assert live_db.STUB_MODE is False, (
		"STUB_MODE MUST be False once B's real live_db queries ship (Risk #7)"
	)


# ---------------------------------------------------------------------------
# #12 read_open_positions
# ---------------------------------------------------------------------------


def test_read_open_positions_returns_open_rows_in_id_order(
	conn: sqlite3.Connection,
) -> None:
	_insert(conn, status="open", coid="o1", ticker="A", fill_size=3, blended=40)
	_insert(conn, status="pending", coid="p1", ticker="B")  # excluded
	_insert(conn, status="open", coid="o2", ticker="C", fill_size=5, blended=55)
	_insert(conn, status="won", coid="w1", ticker="D")  # excluded

	positions = read_open_positions(conn)
	assert positions == [
		OpenPosition(ticker="A", side="yes", fill_size=3, blended_entry_cents=40),
		OpenPosition(ticker="C", side="yes", fill_size=5, blended_entry_cents=55),
	]
	assert all(isinstance(p, OpenPosition) for p in positions)


def test_read_open_positions_empty(conn: sqlite3.Connection) -> None:
	assert read_open_positions(conn) == []


# ---------------------------------------------------------------------------
# #13 read_open_count — pending counts; dedup across open↔exit_pending
# ---------------------------------------------------------------------------


def test_read_open_count_counts_pending_and_exit_pending(
	conn: sqlite3.Connection,
) -> None:
	_insert(conn, status="open", coid="o1")
	_insert(conn, status="pending", coid="p1")
	_insert(conn, status="exit_pending", coid="e1")
	# Terminal/non-active states excluded.
	_insert(conn, status="won", coid="w1")
	_insert(conn, status="rejected", coid="r1")
	_insert(conn, status="lost_truth", coid="lt1")
	assert read_open_count(conn) == 3


def test_read_open_count_dedup_open_exit_pending_transition(
	conn: sqlite3.Connection,
) -> None:
	"""A single row transitioning open → exit_pending → open does NOT change
	read_open_count (the row is in exactly one active state at any moment —
	never double-counted)."""
	rid = _insert(conn, status="open", coid="dedup")
	assert read_open_count(conn) == 1

	conn.execute("UPDATE live_trades SET status='exit_pending' WHERE id=?", (rid,))
	conn.commit()
	assert read_open_count(conn) == 1, "exit_pending still counts once"

	conn.execute("UPDATE live_trades SET status='open' WHERE id=?", (rid,))
	conn.commit()
	assert read_open_count(conn) == 1, "back to open — still exactly one"


# ---------------------------------------------------------------------------
# #14 read_daily_pnl_cents — allowlist + #14b half-open day-window boundary
# ---------------------------------------------------------------------------


def test_read_daily_pnl_sums_only_realized_outcomes(
	conn: sqlite3.Connection,
) -> None:
	day = date(2026, 5, 15)
	_insert(conn, status="won", coid="w1", pnl=500, exit_time="2026-05-15T10:00:00")
	_insert(conn, status="lost", coid="l1", pnl=-200, exit_time="2026-05-15T11:00:00")
	_insert(conn, status="scratch", coid="s1", pnl=-3, exit_time="2026-05-15T12:00:00")
	# Allowlist EXCLUDES these even though they fall in-window:
	_insert(conn, status="cancelled", coid="c1", pnl=-999, exit_time="2026-05-15T09:00:00")
	_insert(conn, status="lost_truth", coid="lt1", pnl=-999, exit_time="2026-05-15T09:00:00")
	_insert(conn, status="rejected_post_hoc", coid="rph1", pnl=-999, exit_time="2026-05-15T09:00:00")
	_insert(conn, status="rejected", coid="r1", pnl=-999, exit_time="2026-05-15T09:00:00")

	assert read_daily_pnl_cents(conn, day) == 500 - 200 - 3


def test_read_daily_pnl_empty_returns_zero(conn: sqlite3.Connection) -> None:
	assert read_daily_pnl_cents(conn, date(2026, 5, 15)) == 0


def test_read_daily_pnl_half_open_window_boundary(
	conn: sqlite3.Connection,
) -> None:
	"""#14b: the window is half-open [day 00:00:00, day+1 00:00:00).
	23:59:59.999 of the target day COUNTS; 00:00:00.000 of the next day does
	NOT (it is tomorrow's first instant). An off-by-one at midnight would
	double-count or zero-count one row at EVERY day boundary."""
	day = date(2026, 5, 15)
	# Just inside the upper bound — counts for 2026-05-15.
	_insert(
		conn,
		status="won",
		coid="edge_in",
		pnl=111,
		exit_time="2026-05-15T23:59:59.999",
	)
	# Exactly the next day's first instant — must NOT count for 2026-05-15.
	_insert(
		conn,
		status="won",
		coid="edge_out_next",
		pnl=222,
		exit_time="2026-05-16T00:00:00.000",
	)
	# Exactly the lower bound — counts (half-open INCLUDES the start).
	_insert(
		conn,
		status="lost",
		coid="edge_lower",
		pnl=-50,
		exit_time="2026-05-15T00:00:00",
	)
	# Previous day's last instant — must NOT count.
	_insert(
		conn,
		status="won",
		coid="edge_prev",
		pnl=999,
		exit_time="2026-05-14T23:59:59.999",
	)

	assert read_daily_pnl_cents(conn, day) == 111 - 50, (
		"only the in-window rows (23:59:59.999 same day + 00:00:00 same day) "
		"count; next-day 00:00:00.000 and prev-day rows are excluded"
	)


def test_read_daily_pnl_matches_killswitch_midnight_boundary(
	conn: sqlite3.Connection,
) -> None:
	"""The lower bound is INCLUSIVE and the upper bound EXCLUSIVE — the exact
	contract C's KillSwitch auto-clear-at-midnight relies on. A row at the
	next day's 00:00:00 belongs to the NEXT day's window, not today's."""
	_insert(
		conn,
		status="lost",
		coid="midnight",
		pnl=-1000,
		exit_time="2026-05-16T00:00:00",
	)
	assert read_daily_pnl_cents(conn, date(2026, 5, 15)) == 0
	assert read_daily_pnl_cents(conn, date(2026, 5, 16)) == -1000
