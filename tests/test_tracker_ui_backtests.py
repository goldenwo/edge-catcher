"""Tests for Tracker.ui_backtests methods (storage consolidation)."""
from __future__ import annotations

import pytest
from edge_catcher.research.tracker import Tracker


@pytest.fixture
def tracker(tmp_path):
	return Tracker(db_path=tmp_path / "test_research.db")


def test_save_and_list_ui_backtest(tracker):
	tracker.save_ui_backtest(
		task_id="bt-001",
		series="KXBTC",
		strategies='["example"]',
		db_path="data/kalshi-btc.db",
		start_date="2025-01-01",
		end_date="2025-06-01",
		total_trades=100,
		wins=60,
		losses=40,
		net_pnl_cents=500,
		sharpe=1.5,
		max_drawdown_pct=5.0,
		win_rate=0.6,
		result_path="reports/backtests/backtest_bt-001.json",
		hypothesis_id=None,
	)
	rows, total = tracker.list_ui_backtests(limit=10, offset=0)
	assert total == 1
	assert rows[0]["task_id"] == "bt-001"
	assert rows[0]["series"] == "KXBTC"
	assert rows[0]["db_path"] == "data/kalshi-btc.db"


def test_delete_ui_backtest(tracker):
	tracker.save_ui_backtest(
		task_id="bt-del",
		series="KXBTC",
		strategies='["example"]',
		db_path="data/kalshi-btc.db",
		total_trades=10,
		wins=5,
		losses=5,
		net_pnl_cents=0,
		sharpe=0.0,
		max_drawdown_pct=0.0,
		win_rate=0.5,
	)
	deleted = tracker.delete_ui_backtest("bt-del")
	assert deleted is True
	_, total = tracker.list_ui_backtests()
	assert total == 0


def test_delete_nonexistent_returns_false(tracker):
	assert tracker.delete_ui_backtest("nope") is False


def test_get_hypothesis_result_by_id(tracker):
	row_id = tracker.save_hypothesis_result(
		test_type="price_efficiency",
		series="KXBTC",
		db="kalshi.db",
		params={"bucket_size": 5},
		thresholds={"min_z": 2.0},
		verdict="EDGE_EXISTS",
		z_stat=2.5,
		fee_adjusted_edge=0.03,
		detail={"buckets": 10},
		rationale="strong signal",
	)
	result = tracker.get_hypothesis_result_by_id(row_id)
	assert result is not None
	assert result["verdict"] == "EDGE_EXISTS"
	assert result["series"] == "KXBTC"


def test_get_hypothesis_result_by_id_not_found(tracker):
	assert tracker.get_hypothesis_result_by_id("nonexistent") is None


def test_delete_result_from_hypothesis_results(tracker):
	row_id = tracker.save_hypothesis_result(
		test_type="price_efficiency",
		series="KXBTC",
		db="kalshi.db",
		params={},
		thresholds={},
		verdict="NO_EDGE",
		z_stat=0.5,
		fee_adjusted_edge=0.0,
		detail={},
	)
	assert tracker.delete_result(row_id) is True
	assert tracker.get_hypothesis_result_by_id(row_id) is None


def test_delete_result_nonexistent(tracker):
	assert tracker.delete_result("nonexistent") is False


def test_count_and_latest(tracker):
	import time
	tracker.save_ui_backtest(
		task_id="bt-a", series="KXBTC", strategies='["s1"]',
		db_path="data/kalshi-btc.db",
		total_trades=10, wins=5, losses=5,
		net_pnl_cents=100, sharpe=1.0,
		max_drawdown_pct=2.0, win_rate=0.5,
	)
	time.sleep(0.01)  # ensure distinct timestamps
	tracker.save_ui_backtest(
		task_id="bt-b", series="KXBTC", strategies='["s2"]',
		db_path="data/kalshi-btc.db",
		total_trades=20, wins=15, losses=5,
		net_pnl_cents=800, sharpe=2.5,
		max_drawdown_pct=1.0, win_rate=0.75,
	)
	assert tracker.count_ui_backtests() == 2
	latest = tracker.latest_ui_backtest()
	assert latest is not None
	assert latest["task_id"] == "bt-b"  # most recent by run_timestamp
