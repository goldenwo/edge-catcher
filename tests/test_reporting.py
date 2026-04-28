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

	def test_malformed_report_missing_all_time(self):
		from edge_catcher.reporting.notify import report_to_notification
		n = report_to_notification({"date": "x", "today": {"pnl_cents": 0}})
		assert n.severity == "error"
		assert "MALFORMED" in n.title

	def test_malformed_report_missing_today(self):
		from edge_catcher.reporting.notify import report_to_notification
		n = report_to_notification({"date": "x", "all_time": {}})
		assert n.severity == "error"
		assert "MALFORMED" in n.title

	def test_rich_body_has_yesterday_section(self):
		from edge_catcher.reporting.notify import report_to_notification
		report = {
			"date": "2026-04-26",
			"all_time": {
				"net_pnl_usd": 11.12, "win_rate_pct": 60.0,
				"closed_trades": 20, "wins": 12, "net_pnl_cents": 1112,
				"deployed_usd": 0.98, "roi_deployed_pct": 1134.69,
				"avg_pnl_cents": 55.6,
			},
			"today": {"settled_count": 2, "pnl_cents": 100, "pnl_usd": 1.0},
			"today_by_strategy": [
				{"strategy": "debut-fade", "series_ticker": "KXETH", "status": "won",  "count": 1, "pnl_cents": 50},
				{"strategy": "debut-fade", "series_ticker": "KXETH", "status": "lost", "count": 1, "pnl_cents": -25},
				{"strategy": "flow-fade",  "series_ticker": "KXBTC", "status": "won",  "count": 1, "pnl_cents": 75},
			],
			"all_time_by_strategy": [
				{
					"strategy": "debut-fade", "closed_trades": 19, "wins": 12,
					"net_pnl_cents": 1455, "net_pnl_usd": 14.55, "win_rate_pct": 63.2,
				},
				{
					"strategy": "flow-fade", "closed_trades": 1, "wins": 1,
					"net_pnl_cents": 75, "net_pnl_usd": 0.75, "win_rate_pct": 100.0,
				},
			],
			"open_positions": [
				{"strategy": "debut-fade", "series_ticker": "KXSOL", "count": 1},
			],
		}
		n = report_to_notification(report)
		# Yesterday section: each (strategy, series) appears once with W/L counts.
		assert "Yesterday" in n.body
		assert "debut-fade / KXETH: 1W / 1L" in n.body
		assert "flow-fade / KXBTC: 1W / 0L" in n.body

	def test_rich_body_has_all_time_by_strategy_section(self):
		from edge_catcher.reporting.notify import report_to_notification
		report = self._sample_report()
		n = report_to_notification(report)
		assert "All-time by strategy" in n.body
		assert "debut-fade: 19 trades" in n.body
		assert "63.2%" in n.body or "WR: 12/19" in n.body  # accept either rendering

	def test_rich_body_has_portfolio_section(self):
		from edge_catcher.reporting.notify import report_to_notification
		report = self._sample_report()
		n = report_to_notification(report)
		assert "Portfolio" in n.body
		assert "$11.12" in n.body  # net_pnl_usd
		assert "1134" in n.body    # roi_deployed_pct (no need for exact decimal)
		assert "60" in n.body      # overall win rate

	def test_rich_body_has_open_positions_section(self):
		from edge_catcher.reporting.notify import report_to_notification
		report = self._sample_report()
		n = report_to_notification(report)
		assert "Open positions" in n.body
		assert "debut-fade/KXSOL" in n.body or "debut-fade / KXSOL" in n.body

	def test_yesterday_section_handles_all_non_won_lost_statuses(self):
		"""Latent-bug guard: if today_by_strategy has rows but ALL have a
		status outside ('won','lost') — e.g., a future schema addition like
		'pending' — the formatter MUST NOT emit a dangling section header.
		It should fall back to the same 'No settled trades.' message as the
		empty-input case."""
		from edge_catcher.reporting.notify import report_to_notification
		report = self._sample_report()
		# Replace today_by_strategy with rows that have unexpected statuses:
		report["today_by_strategy"] = [
			{"strategy": "s", "series_ticker": "X", "status": "pending",   "count": 1, "pnl_cents": 0},
			{"strategy": "s", "series_ticker": "Y", "status": "cancelled", "count": 2, "pnl_cents": 0},
		]
		n = report_to_notification(report)
		assert "No settled trades" in n.body
		# Dangling header check: there must NOT be a "Yesterday (date):" line
		# followed immediately by a blank line + the next section header.
		# Equivalently: the Yesterday line must contain the "No settled" text.
		yesterday_line = next(line for line in n.body.split("\n") if line.startswith("**Yesterday"))
		assert "No settled trades" in yesterday_line, (
			f"dangling Yesterday header: {yesterday_line!r}"
		)

	def test_empty_yesterday_says_no_settled_trades(self):
		from edge_catcher.reporting.notify import report_to_notification
		report = self._sample_report()
		report["today_by_strategy"] = []  # no trades yesterday
		report["today"]["pnl_cents"] = 0
		n = report_to_notification(report)
		assert "No settled trades" in n.body

	def test_empty_open_positions_says_none(self):
		from edge_catcher.reporting.notify import report_to_notification
		report = self._sample_report()
		report["open_positions"] = []
		n = report_to_notification(report)
		assert "Open positions:" in n.body
		assert "None" in n.body

	def test_no_all_time_strategies_omits_section(self):
		"""Brand-new DB with only open trades: the all-time-by-strategy section
		is omitted entirely (no empty header)."""
		from edge_catcher.reporting.notify import report_to_notification
		report = self._sample_report()
		report["all_time_by_strategy"] = []
		n = report_to_notification(report)
		# Section header MUST NOT appear when there's nothing to list:
		assert "All-time by strategy" not in n.body

	def test_negative_pnl_no_plus_sign(self):
		"""A negative total PnL renders without a leading '+'."""
		from edge_catcher.reporting.notify import report_to_notification
		report = self._sample_report()
		report["all_time"]["net_pnl_cents"] = -555
		report["all_time"]["net_pnl_usd"] = -5.55
		n = report_to_notification(report)
		# No "+$-5.55" or "+-5.55" — sign handling must be correct
		assert "+$-" not in n.body
		assert "-$5.55" in n.body or "$-5.55" in n.body  # either rendering OK

	def test_severity_warn_when_today_pnl_negative(self):
		"""Already covered by an existing test, but lock again under rich-body context."""
		from edge_catcher.reporting.notify import report_to_notification
		report = self._sample_report()
		report["today"]["pnl_cents"] = -50
		assert report_to_notification(report).severity == "warn"

	def test_open_positions_capped_at_30(self):
		"""Discord rejects embed descriptions > 4096 chars. _section_open_positions
		must cap row display at the documented limit and emit a '…(N more)'
		marker so the rich body stays under the limit even with many opens."""
		from edge_catcher.reporting.notify import report_to_notification, _OPEN_POSITIONS_DISPLAY_LIMIT
		total = _OPEN_POSITIONS_DISPLAY_LIMIT + 20  # 50 rows; 20 will be truncated
		report = self._sample_report()
		# total open positions across distinct (strategy, series) pairs:
		report["open_positions"] = [
			{"strategy": f"s{i // 10}", "series_ticker": f"K{i % 10}", "count": 1}
			for i in range(total)
		]
		n = report_to_notification(report)
		# Cap kicks in — marker must state the number of truncated entries:
		truncated = total - _OPEN_POSITIONS_DISPLAY_LIMIT
		assert f"({truncated} more)" in n.body, (
			f"expected '({truncated} more)' marker; got: {n.body!r}"
		)
		# Sanity: the total body stays well under Discord's 4096-char limit.
		assert len(n.body) < 4096, f"body length {len(n.body)} exceeds Discord limit"

	def test_open_positions_under_limit_no_truncation(self):
		"""Below the cap, no '…(N more)' marker appears."""
		from edge_catcher.reporting.notify import report_to_notification
		report = self._sample_report()
		report["open_positions"] = [
			{"strategy": "s1", "series_ticker": f"K{i}", "count": 1}
			for i in range(5)
		]
		n = report_to_notification(report)
		assert "more)" not in n.body, "should not truncate when under cap"

	# Helper to keep the test fixtures DRY:
	def _sample_report(self):
		return {
			"date": "2026-04-26",
			"all_time": {
				"net_pnl_usd": 11.12, "win_rate_pct": 60.0,
				"closed_trades": 20, "wins": 12, "net_pnl_cents": 1112,
				"deployed_usd": 0.98, "roi_deployed_pct": 1134.69,
				"avg_pnl_cents": 55.6,
			},
			"today": {"settled_count": 2, "pnl_cents": 100, "pnl_usd": 1.0},
			"today_by_strategy": [
				{"strategy": "debut-fade", "series_ticker": "KXETH", "status": "won",  "count": 1, "pnl_cents": 50},
				{"strategy": "debut-fade", "series_ticker": "KXETH", "status": "lost", "count": 1, "pnl_cents": -25},
				{"strategy": "flow-fade",  "series_ticker": "KXBTC", "status": "won",  "count": 1, "pnl_cents": 75},
			],
			"all_time_by_strategy": [
				{
					"strategy": "debut-fade", "closed_trades": 19, "wins": 12,
					"net_pnl_cents": 1455, "net_pnl_usd": 14.55, "win_rate_pct": 63.2,
				},
				{
					"strategy": "flow-fade", "closed_trades": 1, "wins": 1,
					"net_pnl_cents": 75, "net_pnl_usd": 0.75, "win_rate_pct": 100.0,
				},
			],
			"open_positions": [
				{"strategy": "debut-fade", "series_ticker": "KXSOL", "count": 1},
			],
		}


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


class TestOpenPositions:
	def test_returns_list_of_dicts(self, tmp_path):
		"""Schema lock for an open row: open_positions returns dicts with exactly
		{strategy, series_ticker, count} keys and count is an int. Uses a tmp DB
		with a guaranteed open row so the loop is not vacuously satisfied (the
		bundled demo fixture has zero open rows)."""
		from edge_catcher.reporting import generate_report
		import sqlite3
		db = tmp_path / "schema_check.db"
		con = sqlite3.connect(str(db))
		con.execute(
			"CREATE TABLE paper_trades (strategy TEXT, series_ticker TEXT, status TEXT, "
			"entry_price REAL, fill_size INTEGER, pnl_cents INTEGER, entry_fee_cents INTEGER, "
			"exit_time TEXT)"
		)
		# Mix of settled + open: open MUST appear in open_positions, settled MUST NOT.
		con.execute("INSERT INTO paper_trades VALUES ('s1', 'KX', 'won',  50, 1, 10,   1, '2026-04-25T12:00:00Z')")
		con.execute("INSERT INTO paper_trades VALUES ('s1', 'KX', 'open', 50, 1, NULL, 1, NULL)")
		con.commit()
		con.close()
		report = generate_report(db, date="2026-04-25")
		assert "open_positions" in report
		assert isinstance(report["open_positions"], list)
		assert len(report["open_positions"]) >= 1, "fixture has 1 open row, helper must return it"
		# Schema lock — loop now actually executes:
		for row in report["open_positions"]:
			assert set(row.keys()) == {"strategy", "series_ticker", "count"}
			assert isinstance(row["count"], int)

	def test_empty_when_no_open(self, tmp_path):
		"""A DB with only settled trades returns open_positions=[]."""
		from edge_catcher.reporting import generate_report
		import sqlite3
		db = tmp_path / "settled_only.db"
		con = sqlite3.connect(str(db))
		con.execute(
			"CREATE TABLE paper_trades (strategy TEXT, series_ticker TEXT, status TEXT, "
			"entry_price REAL, fill_size INTEGER, pnl_cents INTEGER, entry_fee_cents INTEGER, "
			"exit_time TEXT)"
		)
		con.execute(
			"INSERT INTO paper_trades VALUES ('s1', 'KX', 'won', 50, 1, 10, 1, '2026-04-25T12:00:00Z')"
		)
		con.commit()
		con.close()
		report = generate_report(db, date="2026-04-25")
		assert report["open_positions"] == []

	def test_groups_by_strategy_and_series(self, tmp_path):
		"""Two opens with same (strategy, series) collapse into one row with count=2."""
		from edge_catcher.reporting import generate_report
		import sqlite3
		db = tmp_path / "open_dupes.db"
		con = sqlite3.connect(str(db))
		con.execute(
			"CREATE TABLE paper_trades (strategy TEXT, series_ticker TEXT, status TEXT, "
			"entry_price REAL, fill_size INTEGER, pnl_cents INTEGER, entry_fee_cents INTEGER, "
			"exit_time TEXT)"
		)
		con.execute("INSERT INTO paper_trades VALUES ('debut-fade', 'KXETH', 'open', 50, 1, NULL, 1, NULL)")
		con.execute("INSERT INTO paper_trades VALUES ('debut-fade', 'KXETH', 'open', 51, 1, NULL, 1, NULL)")
		con.execute("INSERT INTO paper_trades VALUES ('flow-fade', 'KXBTC', 'open', 60, 1, NULL, 1, NULL)")
		con.commit()
		con.close()
		report = generate_report(db, date="2026-04-25")
		open_pos = {(r["strategy"], r["series_ticker"]): r["count"] for r in report["open_positions"]}
		assert open_pos == {("debut-fade", "KXETH"): 2, ("flow-fade", "KXBTC"): 1}


class TestAllTimeByStrategy:
	def test_returns_list_of_dicts(self):
		from edge_catcher.reporting import generate_report
		from pathlib import Path
		fixture = Path("edge_catcher/data/examples/paper_trades_demo.db")
		report = generate_report(fixture, date="2026-04-25")
		assert "all_time_by_strategy" in report
		assert isinstance(report["all_time_by_strategy"], list)
		for row in report["all_time_by_strategy"]:
			assert set(row.keys()) == {
				"strategy", "closed_trades", "wins", "net_pnl_cents",
				"net_pnl_usd", "win_rate_pct",
			}

	def test_excludes_open_trades(self, tmp_path):
		"""Open trades MUST NOT count toward closed_trades / wins / pnl."""
		from edge_catcher.reporting import generate_report
		import sqlite3
		db = tmp_path / "mixed.db"
		con = sqlite3.connect(str(db))
		con.execute(
			"CREATE TABLE paper_trades (strategy TEXT, series_ticker TEXT, status TEXT, "
			"entry_price REAL, fill_size INTEGER, pnl_cents INTEGER, entry_fee_cents INTEGER, "
			"exit_time TEXT)"
		)
		# 1 won, 1 lost, 1 open — the open should not appear or count.
		con.execute("INSERT INTO paper_trades VALUES ('s1', 'KX', 'won',  50, 1,  10, 1, '2026-04-25T12:00:00Z')")
		con.execute("INSERT INTO paper_trades VALUES ('s1', 'KX', 'lost', 50, 1, -10, 1, '2026-04-25T13:00:00Z')")
		con.execute("INSERT INTO paper_trades VALUES ('s1', 'KX', 'open', 50, 1, NULL, 1, NULL)")
		con.commit()
		con.close()
		report = generate_report(db, date="2026-04-25")
		s1 = next(r for r in report["all_time_by_strategy"] if r["strategy"] == "s1")
		assert s1["closed_trades"] == 2
		assert s1["wins"] == 1
		assert s1["net_pnl_cents"] == 0  # +10 + -10
		assert s1["win_rate_pct"] == 50.0

	def test_per_strategy_breakdown(self, tmp_path):
		"""Two strategies with different win rates surface separately."""
		from edge_catcher.reporting import generate_report
		import sqlite3
		db = tmp_path / "two_strats.db"
		con = sqlite3.connect(str(db))
		con.execute(
			"CREATE TABLE paper_trades (strategy TEXT, series_ticker TEXT, status TEXT, "
			"entry_price REAL, fill_size INTEGER, pnl_cents INTEGER, entry_fee_cents INTEGER, "
			"exit_time TEXT)"
		)
		# debut-fade: 3W / 1L = 75% WR, +20¢ net (10+10+10-10)
		for pnl in (10, 10, 10, -10):
			status = "won" if pnl > 0 else "lost"
			con.execute(
				"INSERT INTO paper_trades VALUES ('debut-fade', 'KX', ?, 50, 1, ?, 1, '2026-04-25T12:00:00Z')",
				(status, pnl),
			)
		# flow-fade: 1W / 1L = 50% WR, +0¢ net
		con.execute(
			"INSERT INTO paper_trades VALUES ('flow-fade', 'KX', 'won', "
			"50, 1, 20, 1, '2026-04-25T12:00:00Z')"
		)
		con.execute(
			"INSERT INTO paper_trades VALUES ('flow-fade', 'KX', 'lost', "
			"50, 1, -20, 1, '2026-04-25T13:00:00Z')"
		)
		con.commit()
		con.close()
		report = generate_report(db, date="2026-04-25")
		by_strat = {r["strategy"]: r for r in report["all_time_by_strategy"]}
		assert by_strat["debut-fade"]["closed_trades"] == 4
		assert by_strat["debut-fade"]["wins"] == 3
		assert by_strat["debut-fade"]["net_pnl_cents"] == 20  # 10+10+10-10
		assert by_strat["debut-fade"]["win_rate_pct"] == 75.0
		assert by_strat["flow-fade"]["closed_trades"] == 2
		assert by_strat["flow-fade"]["wins"] == 1
		assert by_strat["flow-fade"]["net_pnl_cents"] == 0
		assert by_strat["flow-fade"]["win_rate_pct"] == 50.0

	def test_empty_when_no_settled(self, tmp_path):
		from edge_catcher.reporting import generate_report
		import sqlite3
		db = tmp_path / "open_only.db"
		con = sqlite3.connect(str(db))
		con.execute(
			"CREATE TABLE paper_trades (strategy TEXT, series_ticker TEXT, status TEXT, "
			"entry_price REAL, fill_size INTEGER, pnl_cents INTEGER, entry_fee_cents INTEGER, "
			"exit_time TEXT)"
		)
		con.execute("INSERT INTO paper_trades VALUES ('s1', 'KX', 'open', 50, 1, NULL, 1, NULL)")
		con.commit()
		con.close()
		report = generate_report(db, date="2026-04-25")
		assert report["all_time_by_strategy"] == []
