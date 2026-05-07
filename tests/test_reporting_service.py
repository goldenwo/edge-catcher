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
