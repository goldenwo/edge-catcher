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

import sqlite3
from pathlib import Path

from edge_catcher.storage.migrations import apply_migrations

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
