"""Integration tests — sub-project B / v1.6.0 PR 5, Agent 4.C.

Spec §Test strategy #26-#28. These drive the **real merged state machine**
end to end against a REAL migrated SQLite ``live_trades.db`` (0003 + WAL) —
the DB is NEVER mocked and ``live.state`` is NEVER stubbed:

* **#26** dispatch ``_handle_enter`` (the real engine handler) → the REAL
  ``edge_catcher.live.store.SQLiteTradeStore`` adapter → real
  ``live.state.record_pending`` → ``MockKalshiWS`` fill → real
  ``on_fill_event`` → ``transition_pending_to_open`` → a strategy-emitted
  exit fill → real ``record_close``. The dispatch→B seam is exercised
  through the production ``TradeStoreProtocol`` adapter over its own held
  ``live_trades.db`` connection (the LOCKED 11-kwarg ``record_pending``
  contract is independently pinned by
  ``tests/test_engine_dispatch_pending_branch.py``). The filled-entry
  variant drives the genuine E-shaped live lifecycle
  (``live.state.record_open`` with real ids → ``on_settlement_event`` →
  ``record_close``) NOT the paper-shaped ``store.record_trade`` (which the
  real adapter deliberately fails loud on — see the SCOPE NOTE below).

* **#27** the live schema is queryable in SQLite read-only mode
  (``file:...?mode=ro``) — the load-bearing Risk #5 / spec-§186 property
  (the operator's reporting CLI reads ``live_trades.db`` while B writes).
  See the SCOPE NOTE on the reporting-CLI gap.

* **#28** cross-process WAL: a writer process appends rows while a reader
  process reads the same DB read-only — concurrent reads are safe under
  WAL (Risk #5).

Run from the project venv (``.venv/Scripts/python.exe``); the subprocess
tests resolve ``edge_catcher`` from this worktree.
"""
from __future__ import annotations

import sqlite3
import subprocess
import sys
import textwrap
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from edge_catcher.engine.dispatch import _handle_enter
from edge_catcher.engine.executor import OrderResult
from edge_catcher.engine.strategy_base import Signal
from edge_catcher.live.state import (
	connect_live_trades_db,
	record_close,
	record_open,
)
from edge_catcher.live.store import SQLiteTradeStore
from edge_catcher.live.ws_handlers import (
	StoreCallbacks,
	on_fill_event,
	on_order_status_event,
	on_settlement_event,
)
from tests.fixtures.mock_kalshi_ws import MockKalshiWS

_NOW = datetime(2026, 5, 16, 12, 0, 0, tzinfo=timezone.utc)
_NOW_ISO = _NOW.isoformat()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def live_db_path(tmp_path: Path) -> Path:
	"""Path to a fresh migrated live_trades.db (0003 + WAL applied on open)."""
	p = tmp_path / "live_trades.db"
	connect_live_trades_db(p).close()
	return p


@pytest.fixture
def conn(live_db_path: Path) -> sqlite3.Connection:
	c = connect_live_trades_db(live_db_path)
	yield c
	c.close()


def _row(conn: sqlite3.Connection, row_id: int) -> dict[str, object]:
	conn.row_factory = sqlite3.Row
	r = conn.execute(
		"SELECT * FROM live_trades WHERE id = ?", (row_id,)
	).fetchone()
	conn.row_factory = None
	assert r is not None, f"row {row_id} missing"
	return dict(r)


# ---------------------------------------------------------------------------
# SCOPE NOTE — real SQLiteTradeStore, no test shim
#
# These tests drive the PRODUCTION ``edge_catcher.live.store.SQLiteTradeStore``
# adapter — the two-stage-reviewed ``TradeStoreProtocol`` impl backed by a real
# ``live_trades.db`` (it opens/migrates the DB via 4.A's
# ``connect_live_trades_db`` and holds one ``sqlite3.Connection`` for its
# lifetime). No test-local shim, no synthesized ids, no DB / ``live.state``
# stubbing. The store's held connection (``store._conn``) is the single
# connection the dispatch write (``record_pending``), the WS handlers
# (``on_fill_event`` / ``on_settlement_event``), and the row reads all share —
# exactly the established idiom ``tests/test_live_store.py::_seed_open_row``
# uses (``record_open(store._conn, ...)``).
#
# **Why the filled-entry #26 variant does NOT go through dispatch's filled
# branch.** ``SQLiteTradeStore.record_trade`` is *deliberately fail-loud*
# (``NotImplementedError``): the paper-shaped ``TradeStoreProtocol.record_trade``
# signature structurally cannot carry D's real ``OrderResult.order_id``
# (→ ``kalshi_order_id``) / ``client_order_id``, so persisting a *synthesized*
# id into a funds-at-risk ``open`` row would mint a row 4.B's reconciler /
# ``on_fill_event`` can never reconcile (real-money correctness hole — see
# ``live/store.py``'s ``PR-5 → PR-6 (E) CONTRACT``). PR 5's merged
# ``engine/dispatch.py`` has NO live-vs-paper branching; wiring the filled
# branch to call ``live.state.record_open`` directly with D's real
# ``OrderResult`` ids is **E's job (PR 6)**. So the filled→settlement variant
# drives that genuine E-shaped lifecycle directly — ``live.state.record_open``
# (real ids) → real ``on_settlement_event`` → real ``record_close`` — rather
# than reintroducing a synthesizing shim or catching the ``NotImplementedError``
# (either would make this real-money test only *appear* to exercise the live
# flow). The pending-entry variant DOES go through dispatch end to end:
# ``OrderResult(status="pending")`` routes dispatch to the real
# ``store.record_pending`` (live-correct on this adapter), then the genuine WS
# fill → ``transition_pending_to_open`` → exit fill → ``record_close`` path.
# ---------------------------------------------------------------------------


def _entry_signal() -> Signal:
	return Signal(
		action="enter",
		ticker="KXSOL15M-26MAY16H12",
		side="yes",
		series="KXSOL15M",
		strategy="strat_34",
		reason="integration-entry",
		entry_price_cents=42,
		stop_loss_distance_cents=8,
	)


def _ctx() -> MagicMock:
	return MagicMock(yes_ask=42, no_ask=58, orderbook=MagicMock(depth=5))


# ===========================================================================
# #26 — full flow: dispatch → record_pending → WS fill → open → exit → close
# ===========================================================================


@pytest.mark.asyncio
async def test_26_dispatch_pending_then_ws_fill_then_exit_close(
	live_db_path: Path, mock_kalshi_ws: MockKalshiWS
) -> None:
	"""End-to-end through the REAL merged machine + the REAL
	``SQLiteTradeStore`` adapter (spec §922 flow verbatim):

	1. D returns ``pending`` (NetworkError) → dispatch ``_handle_enter``
	   calls the real ``SQLiteTradeStore.record_pending`` (→ pure delegation
	   to ``live.state.record_pending``) → a real ``pending`` row.
	2. Kalshi WS ``fill`` for that client_order_id → real ``on_fill_event``
	   → real ``transition_pending_to_open`` (carrying the real
	   ``kalshi_order_id`` FROM the WS event, NOT synthesized) → row ``open``.
	3. The strategy later exits; Kalshi WS ``fill`` for the exit order →
	   real ``on_fill_event`` exit path → real ``record_close`` → row is
	   ``won``/``lost`` with P&L.

	No fail-loud adapter method is reached: the pending OrderResult routes
	dispatch to ``store.record_pending`` (live-correct on this adapter); the
	open/close transitions go through ``live.state`` via the WS handlers,
	never ``store.record_trade`` / ``store.exit_trade``.
	"""
	store = SQLiteTradeStore(live_db_path)
	try:
		conn = store._conn  # the store's own held connection (E-shaped: the
		# dispatch write + WS handlers + reads all share ONE connection)
		cbs = StoreCallbacks()
		mock_kalshi_ws.register(
			db=conn,
			store_callbacks=cbs,
			on_fill=on_fill_event,
			on_order_status=on_order_status_event,
			on_settlement=on_settlement_event,
		)

		# --- 1. Dispatch the entry; D's executor returns pending
		# (NetworkError: order_id=None) so dispatch takes the real
		# ``SQLiteTradeStore.record_pending`` path (→ live.state.record_pending).
		executor = MagicMock()
		executor.place = AsyncMock(
			return_value=OrderResult(
				status="pending",
				intended_size=10,
				filled_size=0,
				blended_entry_cents=0,
				fill_pct=0.0,
				slippage_cents=0,
				rejection_reason="kalshi_unreachable:integration",
				order_id=None,
			)
		)
		await _handle_enter(
			_entry_signal(), _ctx(), store, {"_metrics": MagicMock()},
			executor, now=_NOW,
		)

		# ``SQLiteTradeStore.record_pending`` returns None (pure delegation);
		# resolve the persisted row from the DB. ``_handle_enter`` inserts
		# exactly one row, so the single pending row IS dispatch's write.
		pending_id = conn.execute(
			"SELECT id FROM live_trades WHERE status='pending'"
		).fetchone()[0]
		pending_row = _row(conn, pending_id)
		assert pending_row["status"] == "pending"
		assert pending_row["kalshi_order_id"] is None
		coid = str(pending_row["client_order_id"])
		assert coid, "dispatch must have generated a client_order_id"

		# --- 2. Kalshi confirms the entry via a WS fill → real on_fill_event
		# → real transition_pending_to_open (kalshi_order_id is the WS event's
		# real id, NOT a synthesized one).
		await mock_kalshi_ws.emit_fill(
			client_order_id=coid,
			kalshi_order_id="kx-entry-26",
			filled_count=10,
			fills=[{"price": 42, "size": 7}, {"price": 43, "size": 3}],
			ticker="KXSOL15M-26MAY16H12",
			side="yes",
		)
		opened = _row(conn, pending_id)
		assert opened["status"] == "open"
		assert opened["kalshi_order_id"] == "kx-entry-26"
		assert opened["fill_size"] == 10
		assert opened["blended_entry_cents"] == 42  # round((42*7+43*3)/10)=42

		# --- 3. The strategy exits; Kalshi WS fill for the exit order (fresh
		# coid, matches no row → exit path → full close via record_close).
		await mock_kalshi_ws.emit_fill(
			client_order_id="strat_34-KXSOL15M-1700000099999-exit26",
			kalshi_order_id="kx-exit-26",
			filled_count=10,
			fills=[{"price": 60, "size": 10}],  # 60 > 42 entry → won
			ticker="KXSOL15M-26MAY16H12",
			side="yes",
		)
		closed = _row(conn, pending_id)
		assert closed["status"] == "won"
		assert closed["exit_price_cents"] == 60
		assert closed["exit_time"] is not None
		# pnl = 10*(60-42) - exit_fee  (>0 for this favorable close)
		assert closed["pnl_cents"] is not None and closed["pnl_cents"] > 0
		# Exactly one row — a full close UPDATEs in place, no split child.
		assert (
			conn.execute("SELECT COUNT(*) FROM live_trades").fetchone()[0]
			== 1
		)
	finally:
		store.close()


@pytest.mark.asyncio
async def test_26_filled_entry_then_settlement_close(
	live_db_path: Path, mock_kalshi_ws: MockKalshiWS
) -> None:
	"""Variant: the live **filled-entry** lifecycle, driven exactly as E's
	PR-6 wiring will drive it (spec §"To E" / ``store.py``'s
	``PR-5 → PR-6 (E) CONTRACT``), through the REAL ``SQLiteTradeStore``.

	D returns ``filled`` for a Kalshi IOC entry. The merged
	``engine/dispatch.py`` filled branch calls the paper-shaped
	``store.record_trade(...)``, which the real ``SQLiteTradeStore``
	**deliberately fails loud on** (``NotImplementedError``) — the
	paper-shaped Protocol method structurally cannot carry D's real
	``OrderResult.order_id`` (→ ``kalshi_order_id``) / ``client_order_id``,
	and persisting a *synthesized* id would mint a funds-at-risk ``open``
	row 4.B's reconciler can never reconcile. Wiring dispatch's filled
	branch to ``live.state.record_open`` with D's real values is **E's job
	(PR 6)**; PR 5's dispatch has no live-vs-paper branching.

	So this test:

	* asserts the fail-loud guard is real — dispatching a ``filled``
	  ``OrderResult`` against the live adapter raises ``NotImplementedError``
	  (proving the test is NOT secretly relying on a paper-shaped write that
	  the live architecture forbids — the whole point of the truth-test);
	* then drives the genuine E-shaped filled→settlement lifecycle:
	  ``live.state.record_open`` with D's real ``OrderResult.order_id`` /
	  ``client_order_id`` (the exact call E will wire — same idiom as
	  ``tests/test_live_store.py::_seed_open_row``) → real
	  ``on_settlement_event`` → real ``record_close`` at the binary price.
	"""
	store = SQLiteTradeStore(live_db_path)
	try:
		conn = store._conn
		cbs = StoreCallbacks()
		mock_kalshi_ws.register(
			db=conn,
			store_callbacks=cbs,
			on_fill=on_fill_event,
			on_order_status=on_order_status_event,
			on_settlement=on_settlement_event,
		)
		filled = OrderResult(
			status="filled",
			intended_size=10,
			filled_size=10,
			blended_entry_cents=42,
			fill_pct=1.0,
			slippage_cents=0,
			order_id="kx-entry-26b",
		)
		executor = MagicMock()
		executor.place = AsyncMock(return_value=filled)

		# --- Truth-test: PR-5 dispatch's filled branch hits the paper-shaped
		# store.record_trade, which the live adapter fails loud on. We do NOT
		# work around this with a synthesizing shim or by catching it as a
		# pass — reaching it is the SIGNAL that the filled-entry write is
		# E's-to-wire (live.state.record_open), not a store.record_trade call.
		with pytest.raises(NotImplementedError, match="record_trade"):
			await _handle_enter(
				_entry_signal(), _ctx(), store, {"_metrics": MagicMock()},
				executor, now=_NOW,
			)
		# The fail-loud guard wrote nothing: no half-persisted row.
		assert (
			conn.execute("SELECT COUNT(*) FROM live_trades").fetchone()[0]
			== 0
		)

		# --- Genuine E-shaped filled-entry persistence: live.state.record_open
		# with D's REAL OrderResult.order_id (→ kalshi_order_id) +
		# client_order_id (NOT synthesized) — the exact call E's PR-6 wiring
		# will make for the filled branch.
		signal = _entry_signal()
		open_id = record_open(
			conn,
			ticker=signal.ticker,
			series=signal.series,
			strategy=signal.strategy,
			side=signal.side,
			intended_size=filled.intended_size,
			fill_size=filled.filled_size,
			entry_price_cents=signal.entry_price_cents or 0,
			blended_entry_cents=filled.blended_entry_cents,
			slippage_cents=filled.slippage_cents,
			fill_pct=filled.fill_pct,
			stop_loss_distance_cents=signal.stop_loss_distance_cents or 0,
			client_order_id="strat_34-KXSOL15M-26MAY16H12-itg26b",
			kalshi_order_id=filled.order_id or "",  # D's REAL Kalshi order id
			placed_at_utc=_NOW_ISO,
			entry_time=_NOW_ISO,
			entry_fee_cents=0,
		)
		assert _row(conn, open_id)["status"] == "open"

		# Market settles YES (100¢) — yes-side row wins, via the real
		# on_settlement_event → real record_close.
		await mock_kalshi_ws.emit_settlement(
			ticker="KXSOL15M-26MAY16H12", settlement_price_cents=100
		)
		row = _row(conn, open_id)
		assert row["status"] == "won"
		assert row["exit_reason"] == "settlement"
		assert row["exit_price_cents"] == 100
		assert row["exit_fee_cents"] == 0
	finally:
		store.close()


@pytest.mark.asyncio
async def test_26_settlement_callback_fires_after_rows_closed(
	conn: sqlite3.Connection, mock_kalshi_ws: MockKalshiWS
) -> None:
	"""E's wired bankroll/peak callback is awaited exactly once, AFTER every
	settled row is durably closed (spec §428) — proves the injected-callback
	boundary without importing engine internals."""
	observed_status_at_callback: list[str] = []
	row_holder: dict[str, int] = {}

	async def on_settlement_cb() -> None:
		# When the callback fires, the row must already be terminal.
		rid = row_holder["id"]
		st = conn.execute(
			"SELECT status FROM live_trades WHERE id=?", (rid,)
		).fetchone()[0]
		observed_status_at_callback.append(st)

	cbs = StoreCallbacks(on_settlement=on_settlement_cb)
	mock_kalshi_ws.register(
		db=conn, store_callbacks=cbs, on_settlement=on_settlement_event
	)
	rid = record_open(
		conn,
		ticker="KXSOL15M-26MAY16H12",
		series="KXSOL15M",
		strategy="strat_34",
		side="yes",
		intended_size=10,
		fill_size=10,
		entry_price_cents=42,
		blended_entry_cents=42,
		slippage_cents=0,
		fill_pct=1.0,
		stop_loss_distance_cents=8,
		client_order_id="cid-cb-26",
		kalshi_order_id="kx-cb-26",
		placed_at_utc=_NOW_ISO,
		entry_time=_NOW_ISO,
		entry_fee_cents=17,
	)
	row_holder["id"] = rid

	await mock_kalshi_ws.emit_settlement(
		ticker="KXSOL15M-26MAY16H12", settlement_price_cents=100
	)
	assert observed_status_at_callback == ["won"], (
		"settlement callback must fire exactly once, AFTER the row closed"
	)


# ===========================================================================
# #27 — live schema is readable in SQLite read-only mode (Risk #5 / §186)
# ===========================================================================
#
# SCOPE NOTE / ORCHESTRATOR FINDING:
# The spec (test #27, Risk #5, engine/live_db.py docstring) states the
# operator's reporting CLI is run as
# ``python -m edge_catcher.reporting --db <live_trades.db> --quiet`` and
# "opens the DB with ?mode=ro URI". VERIFIED at HEAD 92dc7b0 that the
# MERGED reporting CLI does NEITHER:
#   * edge_catcher/reporting/__init__.py hard-queries ``FROM paper_trades``
#     with columns ``entry_price`` / ``series_ticker``; the 0003 migration
#     created table ``live_trades`` with ``entry_price_cents`` / ``series``.
#     Running it against a live_trades.db raises
#     ``sqlite3.OperationalError: no such table: paper_trades``.
#   * it opens ``sqlite3.connect(str(db_path))`` — NOT a
#     ``file:...?mode=ro`` URI.
# Spec §186's "mirror paper schema ⇒ a single --db flip works unmodified"
# is therefore FALSE against the merged code. The reporting CLI is
# git-tracked, pre-v1.6.0, has its own paper_trades-pinned test suite, and
# is declared OUT OF SCOPE by spec §32/§45 — 4.C does not modify it.
#
# These tests instead assert the LOAD-BEARING property #27/#28 exist to
# prove (Risk #5): the live schema is safely readable in SQLite read-only
# mode while B holds the WAL write connection. ``test_27_reporting_cli_*``
# documents the CLI gap explicitly (xfail, not a silent skip) so the
# orchestrator sees it.


def _seed_closed_rows(conn: sqlite3.Connection) -> None:
	"""A small realistic mix: one won, one lost (closed), one open."""
	for i, (status, exitp, pnl) in enumerate(
		[("won", 80, 380), ("lost", 10, -320)]
	):
		rid = record_open(
			conn,
			ticker=f"KXSOL15M-26MAY16H1{i}",
			series="KXSOL15M",
			strategy="strat_34",
			side="yes",
			intended_size=10,
			fill_size=10,
			entry_price_cents=42,
			blended_entry_cents=42,
			slippage_cents=0,
			fill_pct=1.0,
			stop_loss_distance_cents=8,
			client_order_id=f"cid-closed-{i}",
			kalshi_order_id=f"kx-closed-{i}",
			placed_at_utc=_NOW_ISO,
			entry_time=_NOW_ISO,
			entry_fee_cents=17,
		)
		record_close(
			conn,
			rid,
			status=status,  # type: ignore[arg-type]
			exit_price_cents=exitp,
			exit_time=_NOW_ISO,
			exit_reason="ws_exit_fill",
			pnl_cents=pnl,
			exit_fee_cents=12,
		)
	record_open(
		conn,
		ticker="KXSOL15M-26MAY16H99",
		series="KXSOL15M",
		strategy="strat_34",
		side="yes",
		intended_size=10,
		fill_size=10,
		entry_price_cents=42,
		blended_entry_cents=42,
		slippage_cents=0,
		fill_pct=1.0,
		stop_loss_distance_cents=8,
		client_order_id="cid-open-99",
		kalshi_order_id="kx-open-99",
		placed_at_utc=_NOW_ISO,
		entry_time=_NOW_ISO,
		entry_fee_cents=17,
	)


def test_27_live_schema_readable_read_only(
	conn: sqlite3.Connection, live_db_path: Path
) -> None:
	"""Risk #5 load-bearing property: while B holds the WAL write connection,
	a SECOND connection opened read-only (``file:...?mode=ro``, exactly the
	URI spec §942 mandates for the reporting CLI) can read the live schema —
	and a write through that read-only handle is rejected (so a reader can
	never block B's writes by issuing one)."""
	_seed_closed_rows(conn)  # B's write connection commits rows.

	ro = sqlite3.connect(f"file:{live_db_path}?mode=ro", uri=True)
	try:
		# Reads work: the daily-P&L-shaped query the reporting layer needs.
		total, wins, losses, net = ro.execute(
			"SELECT COUNT(*), "
			"SUM(CASE WHEN status='won' THEN 1 ELSE 0 END), "
			"SUM(CASE WHEN status='lost' THEN 1 ELSE 0 END), "
			"COALESCE(SUM(CASE WHEN status IN ('won','lost') "
			"THEN pnl_cents END), 0) "
			"FROM live_trades"
		).fetchone()
		assert total == 3 and wins == 1 and losses == 1
		assert net == 380 + (-320)
		# A write via the read-only handle is refused → a reader cannot
		# inadvertently block B's writer (the exact Risk #5 guarantee).
		with pytest.raises(sqlite3.OperationalError):
			ro.execute(
				"INSERT INTO live_trades (ticker, series, strategy, side, "
				"intended_size, original_intended_size, entry_price_cents, "
				"status, client_order_id, placed_at_utc) VALUES "
				"('X','X','x','yes',1,1,1,'pending','ro-x','2026-01-01T00:00:00+00:00')"
			)
			ro.commit()
	finally:
		ro.close()


@pytest.mark.xfail(
	reason=(
		"ORCHESTRATOR FINDING (not a 4.C defect): the merged reporting CLI "
		"(edge_catcher/reporting/__init__.py) hard-queries `FROM paper_trades` "
		"with `entry_price`/`series_ticker`; the 0003 migration created table "
		"`live_trades` with `entry_price_cents`/`series`, so the CLI raises "
		"`no such table: paper_trades` against a live_trades.db. It also opens "
		"plain sqlite3.connect, not the spec-mandated ?mode=ro URI. Spec §186 "
		"'mirror paper schema = single --db flip works unmodified' is false vs "
		"merged code. The reporting CLI is out of 4.C scope (spec §32/§45, "
		"git-tracked, own paper_trades test suite). xfail documents the gap "
		"for the orchestrator rather than silently skipping the spec item."
	),
	strict=True,
)
def test_27_reporting_cli_db_flag_against_live_schema(
	conn: sqlite3.Connection, live_db_path: Path
) -> None:
	"""Spec #27 as literally written: run the reporting CLI against a
	live_trades.db and expect a clean report. Currently xfails because the
	merged CLI queries `paper_trades` (see the module SCOPE NOTE). When the
	orchestrator resolves the reporting↔live-schema gap (a compatibility
	view, a `--table`/`--schema` flag, or a live-aware reporting path), flip
	this to a passing assertion."""
	_seed_closed_rows(conn)
	result = subprocess.run(
		[sys.executable, "-m", "edge_catcher.reporting",
		 "--db", str(live_db_path)],
		capture_output=True,
		text=True,
		timeout=60,
		cwd=str(Path(__file__).resolve().parents[1]),
	)
	# Spec intent: a clean exit-0 report against the live schema.
	assert result.returncode == 0, (
		f"reporting CLI failed against live schema: {result.stderr}"
	)


# ===========================================================================
# #28 — cross-process WAL: concurrent writer + read-only reader are safe
# ===========================================================================


_WRITER_SRC = textwrap.dedent(
	"""
	import sys, time
	from pathlib import Path
	from edge_catcher.live.state import connect_live_trades_db, record_open

	db = Path(sys.argv[1])
	c = connect_live_trades_db(db)
	for i in range(40):
		record_open(
			c,
			ticker=f"KXSOL15M-26MAY16H{i:02d}",
			series="KXSOL15M",
			strategy="wal-writer",
			side="yes",
			intended_size=1,
			fill_size=1,
			entry_price_cents=42,
			blended_entry_cents=42,
			slippage_cents=0,
			fill_pct=1.0,
			stop_loss_distance_cents=8,
			client_order_id=f"wal-coid-{i}",
			kalshi_order_id=f"wal-kx-{i}",
			placed_at_utc="2026-05-16T12:00:00+00:00",
			entry_time="2026-05-16T12:00:00+00:00",
			entry_fee_cents=17,
		)
		time.sleep(0.005)
	c.close()
	"""
)

_READER_SRC = textwrap.dedent(
	"""
	import sqlite3, sys, time

	db = sys.argv[1]

	def _count():
		# Exactly the spec §942 read-only open the reporting CLI must use.
		ro = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
		try:
			return ro.execute(
				"SELECT COUNT(*) FROM live_trades"
			).fetchone()[0]
		finally:
			ro.close()

	# --- Handshake: bounded wait for the writer's FIRST committed row to be
	# visible to THIS read-only cross-process handle under WAL. This *is* the
	# Risk #5 property under test — a read-only reader observing the writer's
	# committed rows without blocking it. The deadline is generous vs the
	# worst observed writer cold-start (heavy-load `import edge_catcher.
	# live.state` + first commit ~1.6s) yet still fails LOUD (exit 3) if a
	# committed row genuinely never becomes readable — that would be a real
	# WAL read-safety defect, not a timing nit. This replaces the previous
	# blind 60-iteration loop whose `max_seen >= 1` lower bound was racy:
	# the reader's stdlib-only start beats the writer's ~0.7s+ package
	# import, so under CPU contention the reader could finish all polls
	# before the writer committed row 1 (reader rc 0, stderr empty —
	# spurious 8-gate failure). The durable `count == 40` end-state
	# assertion below is the true WAL-safety invariant and is unaffected.
	deadline = time.time() + 30.0
	first = 0
	while time.time() < deadline:
		first = _count()
		if first >= 1:
			break
		time.sleep(0.01)
	if first < 1:
		sys.stderr.write(
			"HANDSHAKE_FAILED: read-only reader never observed a "
			"writer-committed row within 30s (real WAL read-safety "
			"failure, not a timing skew)\\n"
		)
		sys.exit(3)

	# --- Observation loop: the writer is now demonstrably live; sample the
	# count concurrently to exercise sustained read-while-write under WAL.
	max_seen = first
	for _ in range(60):
		max_seen = max(max_seen, _count())
		time.sleep(0.005)
	print(max_seen)
	"""
)


def test_28_cross_process_wal_concurrent_reader_is_safe(
	live_db_path: Path,
) -> None:
	"""Risk #5: a writer PROCESS appends rows to live_trades.db (WAL) while a
	reader PROCESS concurrently reads it read-only. Neither errors, and the
	read-only reader provably observes the writer's committed rows under WAL
	(it completes a bounded first-row handshake — exit 3 if a committed row
	never becomes readable, a real WAL read-safety defect) without blocking
	the writer, and all 40 rows are durably present at the end.

	The reader's handshake replaces a former racy `max_seen >= 1` lower
	bound: the reader (stdlib-only start) reliably out-races the writer's
	~0.7s+ `edge_catcher.live.state` import under full-suite CPU contention,
	so the old blind poll loop could finish before the writer committed
	row 1 (reader rc 0 / empty stderr — a spurious 8-gate failure). The
	handshake makes "reader saw committed rows" deterministic; the durable
	`count == 40` end-state is the unchanged true WAL-safety invariant."""
	root = str(Path(__file__).resolve().parents[1])
	writer = subprocess.Popen(
		[sys.executable, "-c", _WRITER_SRC, str(live_db_path)],
		stdout=subprocess.PIPE,
		stderr=subprocess.PIPE,
		text=True,
		cwd=root,
	)
	reader = subprocess.Popen(
		[sys.executable, "-c", _READER_SRC, str(live_db_path)],
		stdout=subprocess.PIPE,
		stderr=subprocess.PIPE,
		text=True,
		cwd=root,
	)
	w_out, w_err = writer.communicate(timeout=90)
	r_out, r_err = reader.communicate(timeout=90)

	assert writer.returncode == 0, f"writer process failed: {w_err}"
	# reader rc 0 == the bounded first-row handshake succeeded: a read-only
	# cross-process handle observed the writer's committed rows under WAL
	# (the Risk #5 property). rc 3 == HANDSHAKE_FAILED (a real read-safety
	# defect, surfaced loud); any other rc == reader crashed. No racy count
	# lower bound — observing committed rows at all is now deterministic.
	assert reader.returncode == 0, (
		f"read-only reader did not complete the WAL first-row handshake "
		f"(rc={reader.returncode}; stderr: {r_err})"
	)
	max_seen = int(r_out.strip() or "0")
	assert max_seen >= 1, (  # guaranteed by the in-reader handshake
		f"read-only reader handshake succeeded but reported max_seen="
		f"{max_seen} (reader stderr: {r_err})"
	)

	# Final state: the writer's 40 rows are all durably present.
	final = connect_live_trades_db(live_db_path)
	try:
		count = final.execute(
			"SELECT COUNT(*) FROM live_trades WHERE strategy='wal-writer'"
		).fetchone()[0]
	finally:
		final.close()
	assert count == 40, f"expected 40 durably-written rows, got {count}"
