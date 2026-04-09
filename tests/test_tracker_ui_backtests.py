"""Tests for Tracker.ui_backtests methods (storage consolidation)."""
from __future__ import annotations

import pytest
from pathlib import Path
from edge_catcher.research.tracker import Tracker


@pytest.fixture
def tracker(tmp_path):
	return Tracker(db_path=tmp_path / "test_research.db")


def test_save_and_list_ui_backtest(tracker):
	tracker.save_ui_backtest(
		task_id="bt-001",
		series="KXBTC",
		strategies='["example"]',
		db_path="data/kalshi.db",
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
	assert rows[0]["db_path"] == "data/kalshi.db"


def test_delete_ui_backtest(tracker):
	tracker.save_ui_backtest(
		task_id="bt-del",
		series="KXBTC",
		strategies='["example"]',
		db_path="data/kalshi.db",
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


def test_count_and_latest(tracker):
	import time
	tracker.save_ui_backtest(
		task_id="bt-a", series="KXBTC", strategies='["s1"]',
		db_path="data/kalshi.db",
		total_trades=10, wins=5, losses=5,
		net_pnl_cents=100, sharpe=1.0,
		max_drawdown_pct=2.0, win_rate=0.5,
	)
	time.sleep(0.01)  # ensure distinct timestamps
	tracker.save_ui_backtest(
		task_id="bt-b", series="KXBTC", strategies='["s2"]',
		db_path="data/kalshi.db",
		total_trades=20, wins=15, losses=5,
		net_pnl_cents=800, sharpe=2.5,
		max_drawdown_pct=1.0, win_rate=0.75,
	)
	assert tracker.count_ui_backtests() == 2
	latest = tracker.latest_ui_backtest()
	assert latest is not None
	assert latest["task_id"] == "bt-b"  # most recent by run_timestamp
