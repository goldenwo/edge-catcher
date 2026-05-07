"""Integration tests for /api/reporting/* endpoints."""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

pytest.importorskip("fastapi", reason="fastapi not installed")
from fastapi.testclient import TestClient

from api.main import app


@pytest.fixture
def client():
	# raise_server_exceptions=False so unhandled exceptions in route handlers
	# (e.g., sqlite3.DatabaseError from corrupt DB) become 500 responses we
	# can assert on, instead of bubbling out of TestClient into pytest.
	# The route handler intentionally does NOT catch DatabaseError per the
	# spec — it surfaces operator errors in data/ as 500s.
	return TestClient(app, raise_server_exceptions=False)


def _make_paper_trades_db(path: Path, row_count: int = 0) -> None:
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


def test_get_dbs_returns_list(client, tmp_path, monkeypatch):
	_make_paper_trades_db(tmp_path / "a.db", row_count=2)
	monkeypatch.setattr("api.reporting_service._DATA_DIR", tmp_path)
	resp = client.get("/api/reporting/dbs")
	assert resp.status_code == 200
	body = resp.json()
	assert "dbs" in body
	assert any(d["name"] == "a.db" for d in body["dbs"])


def test_get_run_returns_report(client, tmp_path, monkeypatch):
	_make_paper_trades_db(tmp_path / "a.db", row_count=1)
	monkeypatch.setattr("api.reporting_service._DATA_DIR", tmp_path)
	resp = client.get("/api/reporting/run", params={"db": "a.db"})
	assert resp.status_code == 200
	body = resp.json()
	assert "all_time" in body
	assert body["all_time"]["total_trades"] == 1


def test_get_run_400_on_bad_date(client, tmp_path, monkeypatch):
	_make_paper_trades_db(tmp_path / "a.db")
	monkeypatch.setattr("api.reporting_service._DATA_DIR", tmp_path)
	resp = client.get("/api/reporting/run", params={"db": "a.db", "date": "2026/05/02"})
	assert resp.status_code == 400
	assert "invalid date" in resp.json()["detail"]


def test_get_run_404_on_missing_db(client, tmp_path, monkeypatch):
	monkeypatch.setattr("api.reporting_service._DATA_DIR", tmp_path)
	resp = client.get("/api/reporting/run", params={"db": "ghost.db"})
	assert resp.status_code == 404


def test_get_run_404_on_generate_report_error_dict(client, tmp_path, monkeypatch):
	"""generate_report's {"error": ...} dict normalizes to 404 with the spec's static detail.

	The route maps any FileNotFoundError (whether from missing path OR from the
	error-dict normalization in run_report) to a uniform "DB not found: {db}"
	detail string per the spec API contract. The synthetic error message from
	generate_report is logged service-side but not exposed through the response.
	"""
	_make_paper_trades_db(tmp_path / "a.db")
	monkeypatch.setattr("api.reporting_service._DATA_DIR", tmp_path)
	monkeypatch.setattr(
		"api.reporting_service.generate_report",
		lambda *a, **kw: {"error": "synthetic missing"},
	)
	resp = client.get("/api/reporting/run", params={"db": "a.db"})
	assert resp.status_code == 404
	assert resp.json()["detail"] == "DB not found: a.db"


def test_get_run_500_on_corrupt_db(client, tmp_path, monkeypatch):
	"""Corrupt *.db (real bytes, not sqlite) → 500 with sqlite error in detail."""
	bad = tmp_path / "corrupt.db"
	bad.write_bytes(b"\x00" * 1024)
	monkeypatch.setattr("api.reporting_service._DATA_DIR", tmp_path)
	resp = client.get("/api/reporting/run", params={"db": "corrupt.db"})
	assert resp.status_code == 500
