"""Tests for takeability_status + execution_archetype persistence."""

import sqlite3

from edge_catcher.research.data_source_config import make_ds
from edge_catcher.research.hypothesis import Hypothesis, HypothesisResult
from edge_catcher.research.tracker import Tracker


def _result(strategy="s1"):
	h = Hypothesis(strategy=strategy, data_sources=make_ds(db="x.db", series="S1"))
	return HypothesisResult(
		hypothesis=h, status="ok", total_trades=100, wins=60, losses=40,
		win_rate=0.6, net_pnl_cents=500.0, sharpe=2.0, max_drawdown_pct=5.0,
		fees_paid_cents=10.0, avg_win_cents=15.0, avg_loss_cents=-8.0,
		per_strategy={}, verdict="promote", verdict_reason="ok", raw_json={},
	)


def test_save_result_defaults_takeability_unproven(tmp_path, monkeypatch):
	monkeypatch.setattr(
		"edge_catcher.research.tracker.resolve_execution_archetype",
		lambda name: "maker",
	)
	tracker = Tracker(db_path=tmp_path / "research.db")
	tracker.save_result(_result())
	rows = tracker.list_results()
	assert rows[0]["takeability_status"] == "unproven"
	assert rows[0]["execution_archetype"] == "maker"


def test_set_takeability_status_updates_row(tmp_path):
	tracker = Tracker(db_path=tmp_path / "research.db")
	r = _result()
	tracker.save_result(r)
	tracker.set_takeability_status(r.hypothesis.id, "graduated")
	rows = tracker.list_results()
	assert rows[0]["takeability_status"] == "graduated"


def test_migration_adds_columns_to_existing_db(tmp_path):
	# Simulate a pre-migration results table (missing the two new columns).
	db = tmp_path / "old.db"
	conn = sqlite3.connect(str(db))
	conn.execute(
		"CREATE TABLE results ("
		"hypothesis_id TEXT PRIMARY KEY, verdict TEXT, completed_at TEXT)"
	)
	conn.commit()
	conn.close()

	# Opening via Tracker must run the idempotent migration.
	Tracker(db_path=db)

	conn = sqlite3.connect(str(db))
	cols = {row[1] for row in conn.execute("PRAGMA table_info(results)").fetchall()}
	conn.close()
	assert "takeability_status" in cols
	assert "execution_archetype" in cols
