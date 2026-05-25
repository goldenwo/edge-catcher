"""Tests for the SQL migration runner — focus on the 0004 dual-slippage columns
and the runner's crash-window idempotency for ADD COLUMN migrations."""
from __future__ import annotations

import sqlite3
from pathlib import Path

from edge_catcher.storage.migrations import apply_migrations


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
	return {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def test_0004_adds_four_live_columns(tmp_path: Path) -> None:
	conn = sqlite3.connect(tmp_path / "live.db")
	apply_migrations(conn)  # default dir = the migrations package itself
	cols = _columns(conn, "live_trades")
	for c in (
		"market_impact_cents",
		"limit_slippage_cents",
		"entry_best_price_cents",
		"entry_limit_price_cents",
	):
		assert c in cols, f"{c} missing from live_trades after 0004"


def test_0004_re_run_after_crash_window_is_idempotent(tmp_path: Path) -> None:
	"""Two-commit crash: the migration body commits, but the process dies before
	the tracking-row commit, so 0004 re-runs on the next boot. SQLite ADD COLUMN
	is not natively idempotent — the runner must tolerate the re-run rather than
	crash-loop the daemon."""
	conn = sqlite3.connect(tmp_path / "live.db")
	apply_migrations(conn)
	# Simulate a crash between 0004's body commit and its tracking-row commit.
	conn.execute("DELETE FROM live_schema_migrations WHERE version = 4")
	conn.commit()
	# Re-run: 0004's ADD COLUMNs already exist → must NOT raise, and version is
	# recorded so it never re-runs again.
	apply_migrations(conn)
	applied = {
		r[0]
		for r in conn.execute("SELECT version FROM live_schema_migrations").fetchall()
	}
	assert 4 in applied
