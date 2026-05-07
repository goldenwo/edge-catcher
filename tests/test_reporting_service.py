"""Tests for api/reporting_service.py."""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from api import reporting_service
from api.reporting_service import DbInfo, list_dbs, run_report


def _make_paper_trades_db(path: Path, row_count: int = 0) -> None:
	"""Create a sqlite DB with a paper_trades table populated with `row_count` rows."""
	con = sqlite3.connect(str(path))
	try:
		con.execute("""
			CREATE TABLE paper_trades (
				trade_id TEXT PRIMARY KEY,
				strategy TEXT, series_ticker TEXT, status TEXT,
				entry_price INTEGER, fill_size INTEGER,
				pnl_cents INTEGER, entry_fee_cents INTEGER,
				entry_time TEXT, exit_time TEXT
			)
		""")
		for i in range(row_count):
			con.execute(
				"INSERT INTO paper_trades VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
				(f"t{i}", "test-strat", "TEST", "won", 50, 1, 100, 5, "2026-05-02", "2026-05-02"),
			)
		con.commit()
	finally:
		con.close()


def test_module_loads():
	"""Smoke: module imports and exposes expected names."""
	assert callable(list_dbs)
	assert callable(run_report)
	assert DbInfo.__dataclass_fields__.keys() == {"name", "size_mb", "mtime", "row_count"}


# ── list_dbs happy path ────────────────────────────────────────────────────


def test_list_dbs_globs_data_dir(tmp_path):
	"""Returns DbInfo for each *.db with paper_trades; sorted by mtime desc."""
	import time
	for i, name in enumerate(["a.db", "b.db", "c.db"]):
		_make_paper_trades_db(tmp_path / name, row_count=i + 1)
		time.sleep(0.01)  # ensure distinct mtimes
	result = list_dbs(data_dir=tmp_path)
	assert len(result) == 3
	assert [d.name for d in result] == ["c.db", "b.db", "a.db"]
	assert result[0].row_count == 3
	assert result[0].size_mb >= 0


def test_list_dbs_skips_non_paper_trades_dbs(tmp_path):
	"""A *.db without paper_trades is silently skipped."""
	_make_paper_trades_db(tmp_path / "good.db")
	other = tmp_path / "other.db"
	con = sqlite3.connect(str(other))
	con.execute("CREATE TABLE not_paper_trades (x INTEGER)")
	con.commit()
	con.close()
	result = list_dbs(data_dir=tmp_path)
	assert [d.name for d in result] == ["good.db"]


def test_list_dbs_empty(tmp_path):
	"""Empty data_dir → empty list."""
	assert list_dbs(data_dir=tmp_path) == []


def test_list_dbs_missing_dir(tmp_path):
	"""Missing data_dir → empty list (not 500)."""
	assert list_dbs(data_dir=tmp_path / "nonexistent") == []


def test_list_dbs_uses_explicit_data_dir(tmp_path):
	"""Explicit data_dir arg overrides the module default — locks the override path."""
	_make_paper_trades_db(tmp_path / "via_arg.db")
	result = list_dbs(data_dir=tmp_path)
	assert [d.name for d in result] == ["via_arg.db"]


# ── list_dbs adversarial edges ─────────────────────────────────────────────


def test_list_dbs_skips_corrupt_file(tmp_path):
	"""A *.db file that's not actually sqlite is silently skipped."""
	bad = tmp_path / "corrupt.db"
	bad.write_bytes(b"\x00\x01\x02 not a sqlite file")
	_make_paper_trades_db(tmp_path / "good.db")
	result = list_dbs(data_dir=tmp_path)
	assert [d.name for d in result] == ["good.db"]


def test_list_dbs_does_not_raise_with_concurrent_writer(tmp_path):
	"""list_dbs uses mode=ro URI; succeeds even with a writer holding BEGIN IMMEDIATE in WAL mode."""
	db_path = tmp_path / "live.db"
	_make_paper_trades_db(db_path, row_count=5)
	# Hold an open write transaction in WAL mode
	writer = sqlite3.connect(str(db_path))
	writer.execute("PRAGMA journal_mode=WAL")
	writer.execute("BEGIN IMMEDIATE")
	writer.execute(
		"INSERT INTO paper_trades VALUES ('hold', 's', 'T', 'won', 50, 1, 100, 5, 't', 't')"
	)
	try:
		result = list_dbs(data_dir=tmp_path)
		assert len(result) == 1
		assert result[0].name == "live.db"
		assert result[0].row_count >= 5
	finally:
		writer.rollback()
		writer.close()


def test_list_dbs_handles_locked_db(tmp_path):
	"""Coexistence under EXCLUSIVE lock: list_dbs's read-only URI mode tolerates contention.

	BEGIN EXCLUSIVE in rollback mode acquires SQLite's EXCLUSIVE lock, which would
	block any concurrent connection attempting to read or write. We expect
	list_dbs to either succeed (mode=ro coexists) or skip the locked file gracefully —
	in either case, NEVER 500 the whole list. The OperationalError catch in
	list_dbs handles the "skip" path; the positive WAL-coexistence test above
	exercises the "succeed" path.
	"""
	db_path = tmp_path / "locked.db"
	_make_paper_trades_db(db_path, row_count=1)
	# Also create a second healthy DB so we can verify list_dbs's "skip bad,
	# return good" behavior even if the locked one is unreachable.
	_make_paper_trades_db(tmp_path / "healthy.db", row_count=2)
	writer = sqlite3.connect(str(db_path))
	writer.execute("PRAGMA journal_mode=DELETE")  # rollback mode
	writer.execute("BEGIN EXCLUSIVE")
	try:
		# list_dbs MUST NOT raise — either skips locked.db or includes it.
		# The healthy DB MUST appear regardless.
		result = list_dbs(data_dir=tmp_path)
		names = [d.name for d in result]
		assert "healthy.db" in names
		# locked.db is allowed to be present OR skipped; both are correct.
	finally:
		writer.rollback()
		writer.close()


def test_list_dbs_handles_file_disappearing_mid_iteration(tmp_path, monkeypatch):
	"""If a file vanishes between glob and connect, list_dbs continues with the survivors."""
	_make_paper_trades_db(tmp_path / "a.db")
	_make_paper_trades_db(tmp_path / "b.db")

	real_connect = sqlite3.connect
	calls = {"count": 0}

	def flaky_connect(*args, **kwargs):
		calls["count"] += 1
		if calls["count"] == 1:
			raise FileNotFoundError("simulated rotation race")
		return real_connect(*args, **kwargs)

	monkeypatch.setattr("api.reporting_service.sqlite3.connect", flaky_connect)
	result = list_dbs(data_dir=tmp_path)
	assert len(result) == 1  # one survives
