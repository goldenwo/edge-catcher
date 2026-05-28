"""Tests for edge_catcher.storage.migrations.apply_migrations.

Covers:
- Fresh DB gets all shipped migrations applied; live_schema_migrations has 3 rows.
- Idempotent re-run produces no new rows and returns an empty applied list.
- Missing migrations_dir raises FileNotFoundError with a clear message.
- Numeric ordering: 0001 is applied before 0002 even when filesystem iteration
  yields them in reverse order (tested via synthetic files in a tmp dir).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from edge_catcher.storage.migrations import apply_migrations

# Path to the real migrations directory shipped with the package.
_MIGRATIONS_DIR = Path(__file__).parent.parent / "edge_catcher" / "storage" / "migrations"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _open_mem() -> sqlite3.Connection:
	"""Return a fresh in-memory SQLite connection with row_factory set."""
	conn = sqlite3.connect(":memory:")
	conn.row_factory = sqlite3.Row
	return conn


def _applied_versions(conn: sqlite3.Connection) -> list[int]:
	"""Return sorted list of versions recorded in live_schema_migrations."""
	rows = conn.execute(
		"SELECT version FROM live_schema_migrations ORDER BY version ASC"
	).fetchall()
	return [row[0] for row in rows]


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
	row = conn.execute(
		"SELECT name FROM sqlite_master WHERE type='table' AND name=?",
		(table,),
	).fetchone()
	return row is not None


# ---------------------------------------------------------------------------
# Core correctness
# ---------------------------------------------------------------------------

def test_apply_migrations_creates_expected_tables() -> None:
	"""Applying the shipped migrations creates kill_switch, risk_state, and
	live_trades (with 0004's columns added to live_trades)."""
	conn = _open_mem()
	applied = apply_migrations(conn, _MIGRATIONS_DIR)

	assert 1 in applied, "migration 0001 should be applied"
	assert 2 in applied, "migration 0002 should be applied"
	assert 3 in applied, "migration 0003 should be applied"
	assert 4 in applied, "migration 0004 should be applied (dual-slippage columns)"
	assert _table_exists(conn, "kill_switch"), "kill_switch table must exist after 0001"
	assert _table_exists(conn, "risk_state"), "risk_state table must exist after 0002"
	assert _table_exists(conn, "live_trades"), "live_trades table must exist after 0003"


def test_apply_migrations_records_versions() -> None:
	"""live_schema_migrations has exactly 4 rows after applying shipped migrations."""
	conn = _open_mem()
	apply_migrations(conn, _MIGRATIONS_DIR)

	versions = _applied_versions(conn)
	assert versions == [1, 2, 3, 4], f"expected [1, 2, 3, 4], got {versions}"


def test_apply_migrations_idempotent() -> None:
	"""Re-running apply_migrations on an already-migrated DB is a no-op."""
	conn = _open_mem()
	first = apply_migrations(conn, _MIGRATIONS_DIR)
	second = apply_migrations(conn, _MIGRATIONS_DIR)

	assert first == [1, 2, 3, 4], "first run should apply all shipped migrations"
	assert second == [], "second run should apply nothing"
	assert _applied_versions(conn) == [1, 2, 3, 4], "no duplicate rows"


def test_apply_migrations_missing_dir_raises() -> None:
	"""Passing a non-existent directory raises FileNotFoundError."""
	conn = _open_mem()
	bogus = Path("/nonexistent/migrations_dir_xyz")

	with pytest.raises(FileNotFoundError) as exc_info:
		apply_migrations(conn, bogus)

	assert "migrations_dir" in str(exc_info.value).lower() or str(bogus) in str(exc_info.value)


# ---------------------------------------------------------------------------
# Ordering guarantee
# ---------------------------------------------------------------------------

def test_numeric_ordering(tmp_path: Path) -> None:
	"""0001 is applied before 0002 regardless of filesystem iteration order.

	We write synthetic migrations to a temp dir and force their mtime to be
	in reverse order so a naive mtime-sort would apply them backwards.
	"""
	m_dir = tmp_path / "migrations"
	m_dir.mkdir()

	# Write 0002 first (earlier mtime would bias against correct order)
	sql_0002 = m_dir / "0002_create_b.sql"
	sql_0002.write_text(
		"CREATE TABLE IF NOT EXISTS tbl_b (id INTEGER PRIMARY KEY);",
		encoding="utf-8",
	)
	# Write 0001 second (later mtime)
	sql_0001 = m_dir / "0001_create_a.sql"
	sql_0001.write_text(
		"CREATE TABLE IF NOT EXISTS tbl_a (id INTEGER PRIMARY KEY);",
		encoding="utf-8",
	)

	conn = _open_mem()
	applied = apply_migrations(conn, m_dir)

	assert applied == [1, 2], f"expected [1, 2] in order, got {applied}"
	assert _table_exists(conn, "tbl_a"), "tbl_a must exist"
	assert _table_exists(conn, "tbl_b"), "tbl_b must exist"


def test_non_sql_files_ignored(tmp_path: Path) -> None:
	"""Non-.sql files and malformed names in migrations_dir are silently ignored."""
	m_dir = tmp_path / "migrations"
	m_dir.mkdir()

	(m_dir / "README.md").write_text("not a migration", encoding="utf-8")
	(m_dir / "no_prefix.sql").write_text(
		"CREATE TABLE IF NOT EXISTS tbl_bad (id INTEGER PRIMARY KEY);",
		encoding="utf-8",
	)
	(m_dir / "0001_valid.sql").write_text(
		"CREATE TABLE IF NOT EXISTS tbl_good (id INTEGER PRIMARY KEY);",
		encoding="utf-8",
	)

	conn = _open_mem()
	applied = apply_migrations(conn, m_dir)

	assert applied == [1], f"only 0001 should be applied, got {applied}"
	assert _table_exists(conn, "tbl_good")
	assert not _table_exists(conn, "tbl_bad"), "malformed filename must be skipped"


def test_apply_migrations_does_not_collide_with_init_db(tmp_path: Path) -> None:
	"""Q4 regression: storage/db.py:init_db writes (version=1, applied_at) to
	its own ``schema_migrations`` table. The migration runner uses a separate
	``live_schema_migrations`` table so that calling apply_migrations on a
	DB previously initialized by init_db does NOT skip 0001_create_kill_switch.

	Pre-fix bug: shared ``schema_migrations`` table → init_db's hardcoded
	version=1 row → apply_migrations sees version 1 in applied_versions →
	silently skips 0001_create_kill_switch.sql. Result: kill_switch table
	never created on shared DBs (latent live-money risk).
	"""
	from edge_catcher.storage.db import init_db

	db_path = tmp_path / "shared.db"
	# init_db writes version=1 to its own schema_migrations table.
	init_db(db_path)

	# apply_migrations on the SAME DB must apply all shipped migrations, not
	# silently skip 0001 because of init_db's version-1 row collision.
	conn = sqlite3.connect(str(db_path))
	conn.row_factory = sqlite3.Row
	try:
		applied = apply_migrations(conn, _MIGRATIONS_DIR)
		assert applied == [1, 2, 3, 4], (
			f"expected 0001-0004 applied, got {applied} — collision likely"
		)
		assert _table_exists(conn, "kill_switch"), (
			"kill_switch table must exist; would not if 0001 was skipped"
		)
		# Verify the runner's tracking table is separate from db.py's.
		assert _table_exists(conn, "live_schema_migrations"), "runner tracking table must exist"
		assert _table_exists(conn, "schema_migrations"), "db.py's tracking table also exists (decoupled)"
	finally:
		conn.close()


# ---------------------------------------------------------------------------
# 0004 dual-slippage migration — per spec §4.2 + §11
# ---------------------------------------------------------------------------


def _columns_of(conn: sqlite3.Connection, table: str) -> set[str]:
	"""Return the set of column names on ``table``."""
	return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def test_0004_adds_dual_slippage_columns_to_live_trades() -> None:
	"""Per spec §4.2: 0004 adds 4 columns to live_trades — two diagnostic metrics
	(market_impact_cents, limit_slippage_cents) and two reference columns
	(entry_best_price_cents, entry_limit_price_cents) for live to compute the
	metrics at transition_pending_to_open."""
	conn = _open_mem()
	apply_migrations(conn, _MIGRATIONS_DIR)

	cols = _columns_of(conn, "live_trades")
	for col in (
		"market_impact_cents",
		"limit_slippage_cents",
		"entry_best_price_cents",
		"entry_limit_price_cents",
	):
		assert col in cols, f"0004 must add {col!r} to live_trades; got cols={sorted(cols)}"


def test_0004_idempotent_on_crash_window_rerun(tmp_path: Path) -> None:
	"""Per spec §11: SQLite ADD COLUMN is NOT idempotent — a crash between the
	body commit and the tracking-row commit causes the body to re-run, which
	raises 'duplicate column name'. The runner must tolerate that exactly
	(swallow it, log a warning, and record the version so the migration never
	re-runs again). Other OperationalErrors still propagate.

	Setup: apply migrations once (records version=4), then DELETE the
	live_schema_migrations row for version=4 to simulate the crash-window
	state — body applied, tracking row missing. Re-run must succeed.
	"""
	db_path = tmp_path / "crash_window.db"
	conn = sqlite3.connect(str(db_path))
	conn.row_factory = sqlite3.Row
	try:
		apply_migrations(conn, _MIGRATIONS_DIR)
		# Simulate the crash window — body committed, tracking row missing.
		conn.execute("DELETE FROM live_schema_migrations WHERE version = 4")
		conn.commit()

		# Re-running must NOT raise — the runner tolerates 'duplicate column name'.
		applied = apply_migrations(conn, _MIGRATIONS_DIR)
		assert applied == [4], f"crash-window re-run should re-record version 4; got {applied}"
		assert _applied_versions(conn) == [1, 2, 3, 4], "version 4 must be re-recorded"
	finally:
		conn.close()


def test_partial_migration_state(tmp_path: Path) -> None:
	"""If 0001 is already applied, only 0002 is applied on the next call."""
	m_dir = tmp_path / "migrations"
	m_dir.mkdir()

	(m_dir / "0001_create_a.sql").write_text(
		"CREATE TABLE IF NOT EXISTS tbl_a (id INTEGER PRIMARY KEY);",
		encoding="utf-8",
	)

	conn = _open_mem()
	first = apply_migrations(conn, m_dir)
	assert first == [1]

	# Now add 0002 and re-run.
	(m_dir / "0002_create_b.sql").write_text(
		"CREATE TABLE IF NOT EXISTS tbl_b (id INTEGER PRIMARY KEY);",
		encoding="utf-8",
	)
	second = apply_migrations(conn, m_dir)
	assert second == [2], f"only 0002 should be new, got {second}"
	assert _applied_versions(conn) == [1, 2]
