"""Tests for edge_catcher.storage.migrations.apply_migrations.

Covers:
- Fresh DB gets both migrations applied; schema_migrations table has 2 rows.
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
	"""Return sorted list of versions recorded in schema_migrations."""
	rows = conn.execute(
		"SELECT version FROM schema_migrations ORDER BY version ASC"
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
	"""Applying both shipped migrations creates kill_switch and risk_state."""
	conn = _open_mem()
	applied = apply_migrations(conn, _MIGRATIONS_DIR)

	assert 1 in applied, "migration 0001 should be applied"
	assert 2 in applied, "migration 0002 should be applied"
	assert _table_exists(conn, "kill_switch"), "kill_switch table must exist after 0001"
	assert _table_exists(conn, "risk_state"), "risk_state table must exist after 0002"


def test_apply_migrations_records_versions() -> None:
	"""schema_migrations has exactly 2 rows after applying shipped migrations."""
	conn = _open_mem()
	apply_migrations(conn, _MIGRATIONS_DIR)

	versions = _applied_versions(conn)
	assert versions == [1, 2], f"expected [1, 2], got {versions}"


def test_apply_migrations_idempotent() -> None:
	"""Re-running apply_migrations on an already-migrated DB is a no-op."""
	conn = _open_mem()
	first = apply_migrations(conn, _MIGRATIONS_DIR)
	second = apply_migrations(conn, _MIGRATIONS_DIR)

	assert first == [1, 2], "first run should apply both"
	assert second == [], "second run should apply nothing"
	assert _applied_versions(conn) == [1, 2], "no duplicate rows"


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
