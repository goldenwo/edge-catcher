"""Tests for edge_catcher.reporting."""
from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from pathlib import Path

from edge_catcher.reporting import generate_report

FIXTURE_DB = (
	Path(__file__).parent.parent
	/ "edge_catcher"
	/ "data"
	/ "examples"
	/ "paper_trades_demo.db"
)


def test_generate_report_returns_expected_keys():
	report = generate_report(FIXTURE_DB)
	assert set(report.keys()) >= {
		"timestamp",
		"date",
		"all_time",
		"today",
		"today_by_strategy",
	}


def test_all_time_math():
	"""Verifies hand-computed totals against the fixture.

	Fixture: 20 closed rows (12W/8L), net pnl 1112c, deployed 98c, fees 20c.
	One row (id=1) has fill_size=4 so deployed_cents = SUM(entry_price*fill_size)
	is arithmetically distinct from the buggy SUM(entry_price) formula (=89), and
	pnl scales accordingly (row 1 settles 4*(100-3) - 1 = 387c).
	"""
	report = generate_report(FIXTURE_DB)
	at = report["all_time"]
	assert at["total_trades"] == 20
	assert at["open_trades"] == 0
	assert at["closed_trades"] == 20
	assert at["wins"] == 12
	assert at["losses"] == 8
	assert at["net_pnl_cents"] == 1112
	assert at["net_pnl_usd"] == 11.12
	assert at["fees_cents"] == 20
	assert at["deployed_cents"] == 98
	assert at["deployed_usd"] == 0.98
	assert at["win_rate_pct"] == 60.0
	# ROI = 1112 / 98 * 100 = 1134.69%
	assert at["roi_deployed_pct"] == round(1112 / 98 * 100, 2)
	# avg = 1112 / 20 = 55.6c
	assert at["avg_pnl_cents"] == round(1112 / 20, 1)


def test_today_filter_uses_exit_time_2026_04_01():
	"""Fixture has 6 settled rows on 2026-04-01 (EDT day bucket)."""
	report = generate_report(FIXTURE_DB, date="2026-04-01")
	assert report["today"]["settled_count"] == 6


def test_today_filter_uses_exit_time_2026_04_02():
	"""Fixture has 6 settled rows on 2026-04-02 (EDT day bucket)."""
	report = generate_report(FIXTURE_DB, date="2026-04-02")
	assert report["today"]["settled_count"] == 6


def test_today_filter_uses_exit_time_2026_04_03():
	"""Fixture has 8 settled rows on 2026-04-03 (EDT day bucket)."""
	report = generate_report(FIXTURE_DB, date="2026-04-03")
	assert report["today"]["settled_count"] == 8


def test_today_filter_returns_zero_for_date_with_no_settlements():
	report = generate_report(FIXTURE_DB, date="2020-01-01")
	assert report["today"]["settled_count"] == 0
	assert report["today"]["pnl_cents"] == 0


def test_today_by_strategy_shape():
	report = generate_report(FIXTURE_DB, date="2026-04-03")
	rows = report["today_by_strategy"]
	assert len(rows) > 0
	for row in rows:
		assert set(row.keys()) == {
			"strategy",
			"series_ticker",
			"status",
			"count",
			"pnl_cents",
		}
		assert row["status"] in ("won", "lost")
		assert row["strategy"] == "longshot_fade_example"


def test_missing_db_returns_error():
	report = generate_report(Path("/tmp/this_does_not_exist_9371.db"))
	assert "error" in report


def test_deployed_uses_entry_price_times_fill_size_not_just_entry_price():
	"""Regression test: the OLD bug summed entry_price alone (per-contract cents),
	not entry_price * fill_size. The fixture intentionally contains at least one
	row with fill_size > 1, so SUM(entry_price * fill_size) is arithmetically
	distinct from SUM(entry_price) and the assertion below actually fails the
	bug instead of just guarding the SQL shape.
	"""
	report = generate_report(FIXTURE_DB)
	con = sqlite3.connect(str(FIXTURE_DB))
	correct = con.execute(
		"SELECT COALESCE(SUM(entry_price * fill_size), 0) "
		"FROM paper_trades WHERE status IN ('won','lost')"
	).fetchone()[0]
	buggy = con.execute(
		"SELECT COALESCE(SUM(entry_price), 0) "
		"FROM paper_trades WHERE status IN ('won','lost')"
	).fetchone()[0]
	con.close()
	# Fixture invariant: the two formulas must produce different totals so the
	# regression guard is meaningful. If this assertion ever trips, someone
	# regenerated the fixture without preserving a fill_size > 1 row.
	assert correct != buggy, (
		f"fixture lost its fill_size>1 row: SUM(entry_price*fill_size)={correct} "
		f"== SUM(entry_price)={buggy}; regression guard is now toothless"
	)
	assert report["all_time"]["deployed_cents"] == correct
	assert report["all_time"]["deployed_cents"] != buggy


def test_cli_prints_json_for_fixture():
	result = subprocess.run(
		[
			sys.executable,
			"-m",
			"edge_catcher.reporting",
			"--db",
			str(FIXTURE_DB),
		],
		capture_output=True,
		text=True,
		check=True,
	)
	data = json.loads(result.stdout)
	assert data["all_time"]["closed_trades"] == 20
	assert data["all_time"]["net_pnl_cents"] == 1112


def test_cli_returns_nonzero_on_missing_db():
	result = subprocess.run(
		[
			sys.executable,
			"-m",
			"edge_catcher.reporting",
			"--db",
			"/tmp/does_not_exist_71294.db",
		],
		capture_output=True,
		text=True,
	)
	assert result.returncode != 0
	data = json.loads(result.stdout)
	assert "error" in data


def test_cli_accepts_date_argument():
	result = subprocess.run(
		[
			sys.executable,
			"-m",
			"edge_catcher.reporting",
			"--db",
			str(FIXTURE_DB),
			"--date",
			"2026-04-03",
		],
		capture_output=True,
		text=True,
		check=True,
	)
	data = json.loads(result.stdout)
	assert data["date"] == "2026-04-03"
	assert data["today"]["settled_count"] == 8


class TestReportToNotification:
	def test_title_includes_date(self):
		from edge_catcher.reporting.notify import report_to_notification
		report = {
			"date": "2026-04-26",
			"all_time": {
				"net_pnl_usd": 11.12, "win_rate_pct": 60.0,
				"closed_trades": 20, "roi_deployed_pct": 1134.69,
			},
			"today": {"pnl_cents": 100},
		}
		n = report_to_notification(report)
		assert "2026-04-26" in n.title
		assert "Daily P&L" in n.title

	def test_body_compact_summary(self):
		from edge_catcher.reporting.notify import report_to_notification
		report = {
			"date": "2026-04-26",
			"all_time": {
				"net_pnl_usd": 11.12, "win_rate_pct": 60.0,
				"closed_trades": 20, "roi_deployed_pct": 1134.69,
			},
			"today": {"pnl_cents": 100},
		}
		n = report_to_notification(report)
		assert "$11.12" in n.body
		assert "60" in n.body
		assert "20" in n.body
		assert "1134" in n.body

	def test_severity_info_when_today_positive(self):
		from edge_catcher.reporting.notify import report_to_notification
		report = {
			"date": "x", "all_time": {"net_pnl_usd": 1, "win_rate_pct": 1, "closed_trades": 1, "roi_deployed_pct": 1},
			"today": {"pnl_cents": 100},
		}
		assert report_to_notification(report).severity == "info"

	def test_severity_warn_when_today_negative(self):
		from edge_catcher.reporting.notify import report_to_notification
		report = {
			"date": "x", "all_time": {"net_pnl_usd": 1, "win_rate_pct": 1, "closed_trades": 1, "roi_deployed_pct": 1},
			"today": {"pnl_cents": -50},
		}
		assert report_to_notification(report).severity == "warn"

	def test_severity_info_when_today_zero(self):
		from edge_catcher.reporting.notify import report_to_notification
		report = {
			"date": "x", "all_time": {"net_pnl_usd": 0, "win_rate_pct": 0, "closed_trades": 0, "roi_deployed_pct": 0},
			"today": {"pnl_cents": 0},
		}
		assert report_to_notification(report).severity == "info"

	def test_payload_is_full_report(self):
		from edge_catcher.reporting.notify import report_to_notification
		report = {
			"date": "x", "all_time": {"net_pnl_usd": 1, "win_rate_pct": 1, "closed_trades": 1, "roi_deployed_pct": 1},
			"today": {"pnl_cents": 100},
		}
		n = report_to_notification(report)
		# Spec §6: payload is the full report dict UNMODIFIED. Identity
		# check (not equality) — locks against future regressions where
		# someone adds a defensive copy.deepcopy() that would silently
		# allow mutations to drift between caller and adapter.
		assert n.payload is report


class TestErrorReportToNotification:
	def test_severity_is_error(self):
		from edge_catcher.reporting.notify import error_report_to_notification
		n = error_report_to_notification({"date": "2026-04-26", "error": "DB not found"})
		assert n.severity == "error"

	def test_title_marks_failure(self):
		from edge_catcher.reporting.notify import error_report_to_notification
		n = error_report_to_notification({"date": "2026-04-26", "error": "x"})
		assert "FAILED" in n.title
		assert "2026-04-26" in n.title

	def test_body_includes_error(self):
		from edge_catcher.reporting.notify import error_report_to_notification
		n = error_report_to_notification({"date": "x", "error": "specific error msg"})
		assert "specific error msg" in n.body

	def test_payload_is_full_report(self):
		from edge_catcher.reporting.notify import error_report_to_notification
		report = {"date": "x", "error": "y"}
		n = error_report_to_notification(report)
		assert n.payload is report

	def test_handles_missing_date(self):
		from edge_catcher.reporting.notify import error_report_to_notification
		n = error_report_to_notification({"error": "no date"})
		assert "unknown" in n.title.lower()
