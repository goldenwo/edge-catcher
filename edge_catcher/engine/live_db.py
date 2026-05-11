"""Read API for live_trades.db — consumed by engine/risk.py (Gate, RiskContext).

This module is owned by sub-project B.  C's PR ships it with STUB_MODE = True
so Agent A's risk.py can import and test against the correct signatures without
waiting for B's schema to land.  B's PR flips STUB_MODE to False and fills in
the real SQL queries.

Signature contract (pinned in C-spec §Cross-module contracts):
  read_open_positions(conn) -> list[OpenPosition]
  read_open_count(conn) -> int
  read_daily_pnl_cents(conn, today_utc: date) -> int

If B's design forces a signature change, C's tests must be updated in B's PR.
"""
from __future__ import annotations

import sqlite3
from datetime import date

from edge_catcher.engine.executor import OpenPosition

# ---------------------------------------------------------------------------
# Stub sentinel — B's PR flips this to False and fills in real queries.
# Tests in tests/test_engine_risk*.py assert this is True in C's PR so that
# the sentinel flip is a visible, reviewable diff in B's PR.
# ---------------------------------------------------------------------------
STUB_MODE: bool = True


def read_open_positions(conn: sqlite3.Connection) -> list[OpenPosition]:
	"""Return all open positions from live_trades.db.

	Reads live_trades.paper_trades WHERE status='open', ordered by trade_id ASC
	(insertion order).  C's gate uses these for equity mark-to-market.

	Returns:
		Ordered list of open positions.  Empty list when no positions exist or
		when STUB_MODE is True (C's PR only).
	"""
	if STUB_MODE:
		return []
	# B fills this in — query live_trades.paper_trades WHERE status='open'.
	raise NotImplementedError("STUB_MODE is False but real query not yet implemented")  # pragma: no cover


def read_open_count(conn: sqlite3.Connection) -> int:
	"""Return the count of open positions from live_trades.db.

	Separate from read_open_positions for callers that need only the count
	(e.g., a UI dashboard query that doesn't need the full position list).
	Gate step 6 uses read_open_positions and derives count via len(); this
	function exists for B's own use and for future consumers.

	Returns:
		Number of open positions.  0 when none exist or when STUB_MODE is True.
	"""
	if STUB_MODE:
		return 0
	raise NotImplementedError("STUB_MODE is False but real query not yet implemented")  # pragma: no cover


def read_daily_pnl_cents(conn: sqlite3.Connection, today_utc: date) -> int:
	"""Return the sum of pnl_cents for trades closed today (UTC).

	Window: [today_utc 00:00:00 UTC, today_utc+1 00:00:00 UTC).  The window
	boundary MUST match KillSwitch's auto-clear-at-midnight logic — if they
	diverge a daily-cap kill could clear at midnight while this read still
	counts losses against the prior day.

	Args:
		conn: Open SQLite connection to live_trades.db.
		today_utc: The current UTC calendar date (date(), not datetime).

	Returns:
		Sum of pnl_cents for closed trades today.  0 when no closes today or
		when STUB_MODE is True.  Negative values represent net loss.
	"""
	if STUB_MODE:
		return 0
	raise NotImplementedError("STUB_MODE is False but real query not yet implemented")  # pragma: no cover
