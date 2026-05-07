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
