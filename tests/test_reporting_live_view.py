"""Tests for the ``paper_trades`` compatibility VIEW over ``live_trades``.

Sub-project E / v1.6.0 PR 6, Phase H1 (spec §7, R2-Gap2).

The daily-P&L reporting CLI (``edge_catcher.reporting``) hardcodes
``FROM paper_trades`` and reads the paper-DB column names (``entry_price``,
``series_ticker``, ``status``, ``pnl_cents``, ``entry_fee_cents``,
``fill_size``, ``strategy``). To run that CLI UNMODIFIED against the live
DB, migration 0003 appends a ``paper_trades`` VIEW over ``live_trades``
that:

- renames ``entry_price_cents → entry_price`` and ``series → series_ticker``,
- projects ``status`` through a CASE so ``exit_pending`` (a still-held
  position with an exit order in flight; no paper analog) reports as
  ``open`` — otherwise the operator UNDER-sees live exposure,
- passes the other live-only statuses through RAW (naturally excluded
  because reporting only matches ``open``/``won``/``lost``/``scratch``),
- uses ``DROP VIEW IF EXISTS; CREATE VIEW`` so a crash-recovery re-run of
  0003 (runner docstring §"Atomicity warning") does not error AND always
  picks up the latest projection (SQLite has no CREATE OR REPLACE VIEW).

These tests apply the REAL shipped migration 0003 via the REAL migration
runner (``apply_migrations``) — no hand-rolled divergent applier.
"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path

from edge_catcher.reporting import generate_report
from edge_catcher.storage.migrations import apply_migrations

# Source of the reporting CLI — the H2 change (E/§7 R2-Gap1 + §7:147) edits
# exactly this file's SQL predicates + the _all_time_stats closed-count.
_REPORTING_SRC = (
	Path(__file__).parent.parent / "edge_catcher" / "reporting" / "__init__.py"
)

# Path to the real migrations directory shipped with the package.
_MIGRATIONS_DIR = Path(__file__).parent.parent / "edge_catcher" / "storage" / "migrations"
_M0003 = _MIGRATIONS_DIR / "0003_create_live_trades.sql"

# Reporting CLI's actual column reads from `paper_trades` (verified against
# edge_catcher/reporting/__init__.py). The VIEW must supply every one.
_REPORTING_READ_COLUMNS = (
	"status",
	"pnl_cents",
	"entry_fee_cents",
	"entry_price",
	"fill_size",
	"strategy",
	"series_ticker",
)

# Column names that exist on the live_trades TABLE but must NOT leak through
# the VIEW under their raw names (they are renamed to the paper names).
_RENAMED_AWAY = ("entry_price_cents", "series")

# The full set of NOT-NULL columns required to INSERT a live_trades row,
# with placeholder-safe defaults. Keyed so individual tests override only
# the fields under test (status, pnl_cents, ...).
_BASE_ROW = {
	"ticker": "KXBTC-25MAY19-T100",
	"series": "KXBTC",
	"strategy": "demo-strat",
	"side": "yes",
	"intended_size": 10,
	"original_intended_size": 10,
	"fill_size": 10,
	"entry_price_cents": 42,
	"status": "open",
	"client_order_id": "coid-0001",
	"placed_at_utc": "2026-05-19T06:00:00+00:00",
}


def _open_mem() -> sqlite3.Connection:
	"""Return a fresh in-memory SQLite connection with row_factory set.

	Mirrors the helper in tests/test_storage_migrations.py so this test
	exercises the same connection shape the runner is used with.
	"""
	conn = sqlite3.connect(":memory:")
	conn.row_factory = sqlite3.Row
	return conn


def _migrate(conn: sqlite3.Connection) -> None:
	"""Apply ALL shipped migrations (0001..0003) via the real runner."""
	apply_migrations(conn, _MIGRATIONS_DIR)


def _insert_live_row(conn: sqlite3.Connection, **overrides: object) -> int:
	"""Insert one live_trades row (base defaults + overrides). Returns its id."""
	row = {**_BASE_ROW, **overrides}
	cols = ", ".join(row.keys())
	placeholders = ", ".join("?" for _ in row)
	cur = conn.execute(
		f"INSERT INTO live_trades ({cols}) VALUES ({placeholders})",
		tuple(row.values()),
	)
	conn.commit()
	return int(cur.lastrowid)


def _view_columns(conn: sqlite3.Connection) -> list[str]:
	"""Return the column names exposed by the paper_trades VIEW."""
	cur = conn.execute("SELECT * FROM paper_trades")
	return [d[0] for d in cur.description]


# ---------------------------------------------------------------------------
# (a) the VIEW exists and is queryable
# ---------------------------------------------------------------------------

def test_paper_trades_view_is_queryable() -> None:
	"""SELECT * FROM paper_trades succeeds after migration 0003."""
	conn = _open_mem()
	_migrate(conn)

	rows = conn.execute("SELECT * FROM paper_trades").fetchall()
	assert rows == [], "fresh DB → view resolves and yields zero rows"

	# It must be a VIEW (derived, stateless), not a TABLE.
	kind = conn.execute(
		"SELECT type FROM sqlite_master WHERE name='paper_trades'"
	).fetchone()
	assert kind is not None, "paper_trades must exist in sqlite_master"
	assert kind[0] == "view", f"paper_trades must be a VIEW, got {kind[0]!r}"


# ---------------------------------------------------------------------------
# (b) the view is paper-shaped (renames applied)
# ---------------------------------------------------------------------------

def test_paper_trades_view_is_paper_shaped() -> None:
	"""The VIEW exposes the paper column names; the raw live names are gone."""
	conn = _open_mem()
	_migrate(conn)

	cols = set(_view_columns(conn))

	# Renamed-TO names present.
	assert "entry_price" in cols, "entry_price_cents must be projected AS entry_price"
	assert "series_ticker" in cols, "series must be projected AS series_ticker"

	# Renamed-AWAY raw names absent (reporting reads the paper names only).
	for raw in _RENAMED_AWAY:
		assert raw not in cols, f"raw live column {raw!r} must not leak through the view"

	# Every column the reporting CLI reads must be present in the view.
	missing = [c for c in _REPORTING_READ_COLUMNS if c not in cols]
	assert not missing, f"reporting reads columns the view omits: {missing}"


# ---------------------------------------------------------------------------
# (c) status CASE: exit_pending → open; closed pass through; live-only raw
# ---------------------------------------------------------------------------

def test_exit_pending_projects_as_open() -> None:
	"""exit_pending is a STILL-HELD position → view must report it as 'open'
	(else the operator under-sees live exposure)."""
	conn = _open_mem()
	_migrate(conn)

	rid = _insert_live_row(conn, status="exit_pending", client_order_id="coid-ep")

	view_status = conn.execute(
		"SELECT status FROM paper_trades WHERE id=?", (rid,)
	).fetchone()[0]
	assert view_status == "open", (
		f"exit_pending must project as 'open', got {view_status!r}"
	)

	# And it IS counted by reporting's open predicate.
	open_count = conn.execute(
		"SELECT COUNT(*) FROM paper_trades WHERE status='open'"
	).fetchone()[0]
	assert open_count == 1, "exit_pending row must be counted as an open position"


def test_closed_statuses_pass_through_unchanged() -> None:
	"""won / lost / scratch project verbatim (reporting matches these)."""
	conn = _open_mem()
	_migrate(conn)

	cases = {
		"won": "coid-won",
		"lost": "coid-lost",
		"scratch": "coid-scr",
	}
	ids = {
		st: _insert_live_row(conn, status=st, client_order_id=coid)
		for st, coid in cases.items()
	}

	for st, rid in ids.items():
		got = conn.execute(
			"SELECT status FROM paper_trades WHERE id=?", (rid,)
		).fetchone()[0]
		assert got == st, f"{st!r} must pass through unchanged, got {got!r}"


def test_live_only_statuses_pass_through_raw_and_are_excluded() -> None:
	"""pending / rejected pass through RAW (not rewritten) and are therefore
	naturally excluded by reporting's open/won/lost/scratch matching."""
	conn = _open_mem()
	_migrate(conn)

	pid = _insert_live_row(conn, status="pending", client_order_id="coid-pend")
	jid = _insert_live_row(conn, status="rejected", client_order_id="coid-rej")

	got_pending = conn.execute(
		"SELECT status FROM paper_trades WHERE id=?", (pid,)
	).fetchone()[0]
	got_rejected = conn.execute(
		"SELECT status FROM paper_trades WHERE id=?", (jid,)
	).fetchone()[0]
	assert got_pending == "pending", f"pending must stay raw, got {got_pending!r}"
	assert got_rejected == "rejected", f"rejected must stay raw, got {got_rejected!r}"

	# Reporting's matched buckets must NOT pick these up.
	matched = conn.execute(
		"SELECT COUNT(*) FROM paper_trades "
		"WHERE status IN ('open','won','lost','scratch')"
	).fetchone()[0]
	assert matched == 0, (
		"pending/rejected must be excluded by reporting's status matching"
	)


def test_renamed_columns_carry_values_through() -> None:
	"""entry_price / series_ticker carry the underlying live values, and a
	split-row residual (parent stays 'open') is excluded from closed sums."""
	conn = _open_mem()
	_migrate(conn)

	rid = _insert_live_row(
		conn,
		status="exit_pending",
		client_order_id="coid-val",
		entry_price_cents=37,
		series="KXETH",
		fill_size=5,
		pnl_cents=None,
	)

	r = conn.execute(
		"SELECT entry_price, series_ticker, status, fill_size "
		"FROM paper_trades WHERE id=?",
		(rid,),
	).fetchone()
	assert r["entry_price"] == 37, "entry_price must carry entry_price_cents value"
	assert r["series_ticker"] == "KXETH", "series_ticker must carry series value"
	assert r["status"] == "open", "exit_pending residual must project open"
	assert r["fill_size"] == 5, "fill_size passes through unchanged"

	# A still-open (exit_pending) residual must NOT enter closed-PnL sums.
	closed_net = conn.execute(
		"SELECT COALESCE(SUM(CASE WHEN status IN ('won','lost') "
		"THEN pnl_cents END), 0) FROM paper_trades"
	).fetchone()[0]
	assert closed_net == 0, "open residual must be excluded from closed net P&L"


# ---------------------------------------------------------------------------
# (d) idempotency: re-running 0003 does not error; view still resolves
# ---------------------------------------------------------------------------

def test_runner_idempotent_reapply_is_safe() -> None:
	"""Calling apply_migrations twice (the normal file-skip path) leaves the
	view intact and queryable."""
	conn = _open_mem()
	first = apply_migrations(conn, _MIGRATIONS_DIR)
	second = apply_migrations(conn, _MIGRATIONS_DIR)

	assert 3 in first, "0003 applied on first run"
	assert second == [], "second run is a file-level no-op"

	# View still resolves.
	assert conn.execute("SELECT * FROM paper_trades").fetchall() == []


def test_0003_body_reexecution_does_not_error() -> None:
	"""Crash-recovery re-run: the runner docstring's 'Atomicity warning' says
	if the process dies between the body COMMIT and the tracking-row COMMIT,
	the 0003 BODY re-executes on next startup. Simulate that by running the
	0003 SQL body through executescript() TWICE on the same connection (the
	exact mechanism the runner uses at line 133). DROP VIEW IF EXISTS makes
	this re-run-safe; a bare CREATE VIEW would raise
	'view paper_trades already exists'.
	"""
	conn = _open_mem()
	sql_0003 = _M0003.read_text(encoding="utf-8")

	# First application of the 0003 body (mirrors runner: conn.executescript).
	conn.executescript(sql_0003)
	assert conn.execute("SELECT * FROM paper_trades").fetchall() == []

	# Second application of the SAME body — must NOT raise.
	conn.executescript(sql_0003)  # would raise without DROP VIEW IF EXISTS

	# View still resolves correctly after the re-run.
	conn.execute(
		"INSERT INTO live_trades "
		"(ticker, series, strategy, side, intended_size, "
		" original_intended_size, fill_size, entry_price_cents, status, "
		" client_order_id, placed_at_utc) "
		"VALUES (?,?,?,?,?,?,?,?,?,?,?)",
		(
			"KXBTC-X", "KXBTC", "s", "yes", 1, 1, 1, 50,
			"exit_pending", "coid-rerun", "2026-05-19T06:00:00+00:00",
		),
	)
	conn.commit()
	got = conn.execute(
		"SELECT status, entry_price, series_ticker FROM paper_trades"
	).fetchone()
	assert got["status"] == "open", "view still rewrites exit_pending after re-run"
	assert got["entry_price"] == 50, "view still projects entry_price after re-run"
	assert got["series_ticker"] == "KXBTC", "view still projects series_ticker"


# ===========================================================================
# H2 — broaden closed-trade filter to include 'scratch' (E / §7 R2-Gap1)
#      + coherent _all_time_stats closed-count / win-rate denominator
#      (§7:147 scratch-accounting).
#
# Two NORMATIVE requirements both bind:
#  (1) R2-Gap1: broaden the 6 SQL `status IN ('won','lost')` predicates +
#      the line-7 docstring literal ATOMICALLY (a partial edit silently
#      skews net-P&L/fees/deployed vs trade counts).
#  (2) §7:147: scratch counts toward the closed-trade count and the
#      win-rate DENOMINATOR; contributes its actual pnl_cents (~0, never
#      hardcoded 0); reported as its OWN line (neither win nor loss).
#
# Class-C invariant (HARD): paper reporting byte-identical before/after.
# Paper DBs have ZERO status='scratch' rows ⇒ the whole change is a
# provable no-op on scratch-free data (no value changed, NO new dict key).
# ===========================================================================


def _paper_schema_sql() -> str:
	"""The paper_trades TABLE shape the reporting CLI reads (mirrors the
	columns used by tests/test_reporting.py's tmp-DB fixtures)."""
	return (
		"CREATE TABLE paper_trades ("
		"strategy TEXT, series_ticker TEXT, status TEXT, "
		"entry_price REAL, fill_size INTEGER, pnl_cents INTEGER, "
		"entry_fee_cents INTEGER, exit_time TEXT)"
	)


def _seed_paper_rows(con: sqlite3.Connection) -> None:
	"""Scratch-FREE paper-shaped data: won / lost / open only (the exact
	shape every production paper_trades_v2.db has — zero scratch rows)."""
	rows = [
		# strategy, series, status, entry_price, fill_size, pnl, fee, exit_time
		("debut-fade", "KXETH", "won", 50, 2, 90, 1, "2026-04-25T16:00:00Z"),
		("debut-fade", "KXETH", "lost", 50, 1, -40, 1, "2026-04-25T17:00:00Z"),
		("debut-fade", "KXSOL", "won", 60, 1, 30, 1, "2026-04-26T16:00:00Z"),
		("flow-fade", "KXBTC", "lost", 45, 3, -55, 2, "2026-04-26T17:00:00Z"),
		("flow-fade", "KXBTC", "open", 45, 1, None, 1, None),
	]
	con.executemany(
		"INSERT INTO paper_trades VALUES (?,?,?,?,?,?,?,?)", rows
	)
	con.commit()


def _old_predicate_expectation(db_path: Path, date: str) -> dict:
	"""Recompute the FULL report dict using the OLD (pre-H2) predicates
	`status IN ('won','lost')` directly against the same DB. If the live
	`generate_report` output equals this on scratch-free data, the H2
	change is a *proven* no-op there (the reporting G-parity analog)."""
	con = sqlite3.connect(str(db_path))
	try:
		# --- all_time (OLD: closed = wins + losses; predicates won/lost) ---
		row = con.execute(
			"""SELECT
				COUNT(*),
				SUM(CASE WHEN status='open' THEN 1 ELSE 0 END),
				SUM(CASE WHEN status='won' THEN 1 ELSE 0 END),
				SUM(CASE WHEN status='lost' THEN 1 ELSE 0 END),
				COALESCE(SUM(CASE WHEN status IN ('won','lost') THEN pnl_cents END), 0),
				COALESCE(SUM(CASE WHEN status IN ('won','lost') THEN entry_fee_cents END), 0),
				COALESCE(SUM(CASE WHEN status IN ('won','lost') THEN entry_price * fill_size END), 0)
			FROM paper_trades"""
		).fetchone()
		total, open_, wins, losses, net_pnl, fees, deployed = row
		closed = (wins or 0) + (losses or 0)
		win_rate = (wins / closed * 100) if closed else 0.0
		avg_pnl = (net_pnl / closed) if closed else 0.0
		roi = (net_pnl / deployed * 100) if deployed else 0.0
		all_time = {
			"total_trades": total,
			"open_trades": open_ or 0,
			"closed_trades": closed,
			"wins": wins or 0,
			"losses": losses or 0,
			"win_rate_pct": round(win_rate, 1),
			"net_pnl_cents": net_pnl,
			"net_pnl_usd": round(net_pnl / 100, 2),
			"avg_pnl_cents": round(avg_pnl, 1),
			"fees_cents": fees,
			"deployed_cents": deployed,
			"deployed_usd": round(deployed / 100, 2),
			"roi_deployed_pct": round(roi, 2),
		}
		# --- today (OLD predicate) ---
		n, pnl = con.execute(
			"""SELECT COUNT(*), COALESCE(SUM(pnl_cents), 0)
			FROM paper_trades
			WHERE status IN ('won','lost')
			  AND date(datetime(exit_time, '-4 hours')) = ?""",
			(date,),
		).fetchone()
		today = {"settled_count": n, "pnl_cents": pnl, "pnl_usd": round(pnl / 100, 2)}
		# --- today_by_strategy (OLD predicate) ---
		tbs = [
			{"strategy": r[0], "series_ticker": r[1], "status": r[2],
			 "count": r[3], "pnl_cents": r[4]}
			for r in con.execute(
				"""SELECT strategy, series_ticker, status, COUNT(*),
					COALESCE(SUM(pnl_cents), 0)
				FROM paper_trades
				WHERE status IN ('won','lost')
				  AND date(datetime(exit_time, '-4 hours')) = ?
				GROUP BY strategy, series_ticker, status
				ORDER BY strategy, series_ticker, status""",
				(date,),
			).fetchall()
		]
		# --- open_positions (unchanged predicate) ---
		op = [
			{"strategy": r[0], "series_ticker": r[1], "count": r[2]}
			for r in con.execute(
				"""SELECT strategy, series_ticker, COUNT(*)
				FROM paper_trades WHERE status = 'open'
				GROUP BY strategy, series_ticker
				ORDER BY strategy, series_ticker"""
			).fetchall()
		]
		# --- all_time_by_strategy (OLD predicate) ---
		abs_ = []
		for strategy, c, w, np_ in con.execute(
			"""SELECT strategy, COUNT(*),
				SUM(CASE WHEN status = 'won' THEN 1 ELSE 0 END),
				COALESCE(SUM(pnl_cents), 0)
			FROM paper_trades WHERE status IN ('won','lost')
			GROUP BY strategy ORDER BY strategy"""
		).fetchall():
			wr = (w / c * 100) if c else 0.0
			abs_.append({
				"strategy": strategy,
				"closed_trades": c,
				"wins": w or 0,
				"net_pnl_cents": np_,
				"net_pnl_usd": round(np_ / 100, 2),
				"win_rate_pct": round(wr, 1),
			})
	finally:
		con.close()
	return {
		"all_time": all_time,
		"today": today,
		"today_by_strategy": tbs,
		"open_positions": op,
		"all_time_by_strategy": abs_,
	}


# ---------------------------------------------------------------------------
# (1) R2-Gap1 — predicate-literal count assertion on the SOURCE
# ---------------------------------------------------------------------------

def test_all_won_lost_predicates_broadened_atomically() -> None:
	"""ATOMICITY guard (R2-Gap1): after H2 the reporting source has
	EXACTLY 7 broadened `('won','lost','scratch')` literals — the 6 SQL
	`status IN (...)` predicates PLUS the line-7 docstring literal —
	ZERO remaining non-scratch `('won','lost')` literals (SQL OR doc),
	and the wins/losses CASE expressions (`status='won'` /
	`status='lost'`) are byte-UNCHANGED — scratch is never reclassified
	as a win or a loss. A partial edit silently skews
	net-P&L/fees/deployed vs trade counts; this pins all-or-nothing
	(both the SQL predicates AND the doc, atomically)."""
	src = _REPORTING_SRC.read_text(encoding="utf-8")

	# No bare `('won','lost')` may survive anywhere (SQL predicates AND the
	# line-7 prose literal). Whitespace-tolerant; the broadened form
	# `('won','lost','scratch')` does NOT match this pattern.
	bare = re.findall(r"'won'\s*,\s*'lost'\s*\)", src)
	assert bare == [], (
		f"{len(bare)} un-broadened ('won','lost') literal(s) remain — "
		f"R2-Gap1 demands an ATOMIC broadening of all 6 SQL predicates "
		f"+ the line-7 docstring; a partial edit skews money-vs-counts"
	)

	# Exactly 7 broadened literals: the 6 SQL predicates + the line-7
	# docstring sentence — none missed, none extra. (The new
	# `status='scratch'` sibling CASE-sum is a single-status equality,
	# NOT a `(...,'scratch')` IN-list, so it is correctly NOT counted
	# here.)
	broadened = re.findall(r"'won'\s*,\s*'lost'\s*,\s*'scratch'\s*\)", src)
	assert len(broadened) == 7, (
		f"expected exactly 7 broadened ('won','lost','scratch') literals "
		f"(6 SQL predicates + the line-7 docstring), found {len(broadened)}"
	)

	# Split the docstring literal from the executable code: the module
	# docstring is the first triple-quoted block. The 6 SQL predicates
	# live in the CODE body; exactly 1 broadened literal lives in the
	# docstring (the line-7 sentence). This pins "6 SQL + 1 doc" exactly.
	doc_m = re.match(r'\s*"""(.*?)"""', src, re.DOTALL)
	assert doc_m, "module docstring not found"
	docstring = doc_m.group(1)
	code_body = src[doc_m.end():]
	doc_lits = re.findall(
		r"'won'\s*,\s*'lost'\s*,\s*'scratch'\s*\)", docstring
	)
	code_lits = re.findall(
		r"status IN \('won','lost','scratch'\)", code_body
	)
	assert len(doc_lits) == 1, (
		f"expected exactly 1 broadened literal in the module docstring "
		f"(the line-7 sentence), found {len(doc_lits)}"
	)
	assert len(code_lits) == 6, (
		f"expected exactly 6 broadened `status IN (...)` SQL predicates "
		f"in the CODE body, found {len(code_lits)}"
	)

	# The line-7 docstring literal must be the broadened form (R2-Gap1
	# requires the :7 doc updated atomically with the predicates).
	assert (
		"status IN ('won','lost','scratch') is safer than" in docstring
	), "the line-7 docstring literal must be broadened with the predicates"

	# The wins/losses CASE split is LOCKED — unchanged counts of the
	# single-status equality predicates. _all_time_stats has one
	# `status='won'` + one `status='lost'`; _all_time_by_strategy has one
	# extra `status = 'won'`. (open is matched by `status='open'` / `=
	# 'open'`, not touched here.)
	won_eq = re.findall(r"status\s*=\s*'won'", src)
	lost_eq = re.findall(r"status\s*=\s*'lost'", src)
	assert len(won_eq) == 2, (
		f"the locked wins CASE split changed: expected 2 `status='won'` "
		f"equality predicates, found {len(won_eq)}"
	)
	assert len(lost_eq) == 1, (
		f"the locked losses CASE split changed: expected 1 "
		f"`status='lost'` equality predicate, found {len(lost_eq)}"
	)


def test_all_time_stats_closed_count_includes_scratches_sibling() -> None:
	"""§7:147: _all_time_stats must add a SIBLING
	`SUM(CASE WHEN status='scratch' THEN 1 ELSE 0 END) AS scratches`
	PARALLEL to the locked wins/losses CASE sums, and fold it into the
	Python closed-count (`closed = (wins or 0)+(losses or 0)+(scratches or
	0)`) so closed_trades / win_rate / avg_pnl share a coherent
	denominator. The scratch CASE-sum is used ONLY internally — NO new
	top-level dict key (Class-C byte-identity)."""
	src = _REPORTING_SRC.read_text(encoding="utf-8")

	assert re.search(
		r"SUM\(CASE WHEN status\s*=\s*'scratch' THEN 1 ELSE 0 END\)\s*"
		r"AS\s+scratches",
		src,
	), (
		"§7:147 requires a `SUM(CASE WHEN status='scratch' THEN 1 ELSE 0 "
		"END) AS scratches` sibling parallel to the wins/losses CASE sums"
	)

	# The closed-count expression must include scratches in the denominator.
	assert re.search(
		r"closed\s*=\s*\(wins or 0\)\s*\+\s*\(losses or 0\)\s*\+\s*"
		r"\(scratches or 0\)",
		src,
	), (
		"closed-count must become "
		"`(wins or 0)+(losses or 0)+(scratches or 0)` so win_rate / "
		"avg_pnl share the §7:147 denominator"
	)


def test_no_new_top_level_all_time_key_added() -> None:
	"""Class-C HARD invariant: the `scratches` CASE-sum is internal-only.
	The _all_time_stats returned dict MUST NOT gain a new top-level key
	(e.g. a `scratches`/`scratch_trades` headline). §7:147 'reported as
	its own line' is satisfied by the by-strategy GROUP BY breakdown, NOT
	a new headline key. Locks paper byte-identity at the schema level."""
	src = _REPORTING_SRC.read_text(encoding="utf-8")
	# The exact returned-dict key set of _all_time_stats (pre-H2). Pinning
	# the literal block guards against any new key being inserted.
	expected_keys = {
		'"total_trades"', '"open_trades"', '"closed_trades"', '"wins"',
		'"losses"', '"win_rate_pct"', '"net_pnl_cents"', '"net_pnl_usd"',
		'"avg_pnl_cents"', '"fees_cents"', '"deployed_cents"',
		'"deployed_usd"', '"roi_deployed_pct"',
	}
	# Slice the _all_time_stats return dict literal out of the source.
	m = re.search(
		r"def _all_time_stats\(.*?\n\treturn \{(.*?)\n\t\}",
		src,
		re.DOTALL,
	)
	assert m, "could not locate _all_time_stats return dict"
	ret_block = m.group(1)
	found_keys = set(re.findall(r'"\w+"(?=\s*:)', ret_block))
	assert found_keys == expected_keys, (
		f"_all_time_stats returned-dict keys changed: "
		f"added={found_keys - expected_keys}, "
		f"removed={expected_keys - found_keys}. The `scratches` CASE-sum "
		f"must be internal-only — NO new headline key (Class-C)."
	)


# ---------------------------------------------------------------------------
# (2) paper byte-identical (HARD) — provable no-op on scratch-free data
# ---------------------------------------------------------------------------

def test_paper_byte_identical_on_scratch_free_data(tmp_path) -> None:
	"""The reporting G-parity analog. On a scratch-FREE (won/lost/open
	only) paper-shaped DB, `generate_report` after H2 must return a dict
	whose `all_time` / `today` / `today_by_strategy` / `open_positions` /
	`all_time_by_strategy` are byte/value-IDENTICAL to the SAME data
	computed with the OLD (pre-broadening, closed=wins+losses)
	predicates. Proves the whole change is a no-op on paper: no value
	changed, no key added, no row dropped."""
	db = tmp_path / "paper_scratch_free.db"
	con = sqlite3.connect(str(db))
	con.executescript(_paper_schema_sql())
	con.close()
	con = sqlite3.connect(str(db))
	_seed_paper_rows(con)
	con.close()

	date = "2026-04-25"
	report = generate_report(db, date=date)
	old = _old_predicate_expectation(db, date)

	# Every reporting sub-structure must match the OLD-predicate result
	# value-for-value (this is the byte-identity proof on scratch-free
	# data — the production paper shape).
	assert report["all_time"] == old["all_time"], (
		"all_time drifted vs OLD predicate on scratch-free data — "
		"paper byte-identity (Class-C) VIOLATED"
	)
	assert report["today"] == old["today"], "today drifted on paper data"
	assert report["today_by_strategy"] == old["today_by_strategy"], (
		"today_by_strategy drifted on paper data"
	)
	assert report["open_positions"] == old["open_positions"], (
		"open_positions drifted on paper data"
	)
	assert report["all_time_by_strategy"] == old["all_time_by_strategy"], (
		"all_time_by_strategy drifted on paper data"
	)

	# Schema-level: no NEW top-level key in all_time (Class-C: no
	# `scratches` headline; the CASE-sum is internal-only).
	assert set(report["all_time"].keys()) == set(old["all_time"].keys()), (
		f"all_time gained/lost a key: "
		f"{set(report['all_time'].keys()) ^ set(old['all_time'].keys())}"
	)


# ---------------------------------------------------------------------------
# (3) live scratch coherence — over the H1 paper_trades VIEW
# ---------------------------------------------------------------------------

def _seed_live_closed(
	con: sqlite3.Connection,
	*,
	include_scratch: bool,
) -> None:
	"""Seed won / lost (+ optionally a scratch with a real small NON-zero
	pnl) into live_trades via _insert_live_row. exit_time is set so the
	rows also fall in the _today bucket. Scratch carries a REAL small pnl
	(never hardcoded 0) per §7:147."""
	common = dict(
		entry_price_cents=50,
		fill_size=2,
		entry_fee_cents=1,
		exit_time="2026-05-19T16:00:00+00:00",
	)
	_insert_live_row(
		con, status="won", client_order_id="coid-w1",
		strategy="live-strat", series="KXBTC",
		pnl_cents=120, **common,
	)
	_insert_live_row(
		con, status="lost", client_order_id="coid-l1",
		strategy="live-strat", series="KXBTC",
		pnl_cents=-80, **common,
	)
	if include_scratch:
		_insert_live_row(
			con, status="scratch", client_order_id="coid-s1",
			strategy="live-strat", series="KXBTC",
			pnl_cents=3,  # REAL small non-zero pnl — never hardcoded 0
			**common,
		)


def test_live_scratch_counts_in_closed_and_winrate_denominator(
	tmp_path,
) -> None:
	"""§7:147 over the H1 VIEW: a `scratch` row IS a closed trade — it
	enters closed_trades, the win_rate DENOMINATOR (so win-rate is
	strictly LOWER than the no-scratch case), net_pnl / fees / deployed
	(its REAL pnl, not 0, not dropped), and appears as its OWN
	`status='scratch'` by-strategy line — but is NOT counted in wins or
	losses (never reclassified)."""
	# --- baseline: NO scratch ---
	db_ns = tmp_path / "live_no_scratch.db"
	c = sqlite3.connect(str(db_ns))
	apply_migrations(c, _MIGRATIONS_DIR)
	_seed_live_closed(c, include_scratch=False)
	c.close()
	rep_ns = generate_report(db_ns, date="2026-05-19")
	at_ns = rep_ns["all_time"]
	assert at_ns["closed_trades"] == 2, "baseline: 1 won + 1 lost = 2 closed"
	assert at_ns["win_rate_pct"] == 50.0, "baseline win-rate = 1/2 = 50%"

	# --- with a scratch (real small non-zero pnl) ---
	db_s = tmp_path / "live_with_scratch.db"
	c = sqlite3.connect(str(db_s))
	apply_migrations(c, _MIGRATIONS_DIR)
	_seed_live_closed(c, include_scratch=True)
	c.close()
	rep = generate_report(db_s, date="2026-05-19")
	at = rep["all_time"]

	# (a) scratch IS a closed trade.
	assert at["closed_trades"] == 3, (
		f"scratch must count toward closed_trades (won+lost+scratch=3); "
		f"got {at['closed_trades']}"
	)
	# (b) scratch IS in the win-rate DENOMINATOR → win-rate strictly LOWER.
	assert at["win_rate_pct"] == round(1 / 3 * 100, 1), (
		f"win_rate denominator must include scratch (1 win / 3 closed "
		f"= 33.3%); got {at['win_rate_pct']}"
	)
	assert at["win_rate_pct"] < at_ns["win_rate_pct"], (
		"a breakeven scratch must DILUTE win-rate (it is a closed trade "
		"that is not a win) — §7:147 deliberate intent"
	)
	# (c) scratch is NOT reclassified as a win or loss.
	assert at["wins"] == 1, "scratch must NOT inflate wins"
	assert at["losses"] == 1, "scratch must NOT inflate losses"
	# (d) scratch's REAL pnl flows into net_pnl (not 0, not dropped).
	#     won 120 + lost -80 + scratch 3 = 43.
	assert at["net_pnl_cents"] == 43, (
		f"scratch's real pnl_cents (3, NOT hardcoded 0) must be summed "
		f"into net_pnl (120-80+3=43); got {at['net_pnl_cents']}"
	)
	# (e) scratch's fee + deployed are included (3 closed rows now).
	assert at["fees_cents"] == 3, "scratch entry_fee (1) must be summed (1+1+1)"
	assert at["deployed_cents"] == 50 * 2 * 3, (
		"scratch deployed (entry_price*fill_size) must be included"
	)
	# (f) avg_pnl uses the §7:147 denominator (net / closed incl. scratch).
	assert at["avg_pnl_cents"] == round(43 / 3, 1), (
		"avg_pnl must divide by the scratch-inclusive closed count"
	)

	# (g) scratch is its OWN line in the by-strategy breakdown
	#     (R2-Gap1 GROUP BY ... status now yields a status='scratch' row),
	#     neither a won nor a lost row.
	tbs = rep["today_by_strategy"]
	statuses = {(r["strategy"], r["status"]) for r in tbs}
	assert ("live-strat", "scratch") in statuses, (
		f"scratch must appear as its own status='scratch' by-strategy "
		f"line (neither win nor loss); got {sorted(statuses)}"
	)
	assert ("live-strat", "won") in statuses
	assert ("live-strat", "lost") in statuses
	scratch_row = next(
		r for r in tbs
		if r["strategy"] == "live-strat" and r["status"] == "scratch"
	)
	assert scratch_row["count"] == 1
	assert scratch_row["pnl_cents"] == 3, (
		"by-strategy scratch line must carry the REAL pnl (3), not 0"
	)

	# (h) all_time_by_strategy folds scratch into its closed-count +
	#     win-rate denominator via the broadened COUNT(*) predicate.
	abs_ = {r["strategy"]: r for r in rep["all_time_by_strategy"]}
	assert abs_["live-strat"]["closed_trades"] == 3, (
		"all_time_by_strategy closed-count must include scratch via the "
		"broadened predicate"
	)
	assert abs_["live-strat"]["wins"] == 1, "scratch not a win here either"
	assert abs_["live-strat"]["net_pnl_cents"] == 43, (
		"all_time_by_strategy net_pnl includes scratch's real pnl"
	)
