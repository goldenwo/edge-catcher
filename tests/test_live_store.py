"""Unit tests for edge_catcher.live.store.SQLiteTradeStore.

The adapter is the thin connection-holding bridge between the engine's
``TradeStoreProtocol`` (``engine/trade_store.py``) and sub-project B's
``live.state`` free functions, backed by a real ``live_trades.db``.

These tests exercise the REAL chain — real ``connect_live_trades_db`` (0003
migration + WAL), real ``live.state`` writes, real SQLite. **Nothing is
mocked** (neither the DB nor the ``live.state`` functions); the highest-stakes
property — ``RecordPendingFailed`` propagating uncaught so the engine's three
``except RecordPendingFailed: raise`` ghost-reject clauses fire — is only
provable against the genuine INSERT-failure path.

Spec cross-refs: §773 (locked 11-kwarg ``record_pending``), §557 (locked
10-kwarg ``record_rejected``), §661 + §930 (``record_rejected`` audit-write
best-effort carve-out), §928 (``RecordPendingFailed`` ghost-reject), the
``_LiveBackedStore`` orchestrator shim in
``tests/test_live_state_integration.py`` (the construction/delegation pattern
this production adapter formalises).
"""
from __future__ import annotations

import inspect
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from edge_catcher.engine.trade_store import TradeStoreProtocol
from edge_catcher.live.state import (
	connect_live_trades_db,
	record_open,
)
from edge_catcher.live.store import SQLiteTradeStore

if TYPE_CHECKING:
	# Static structural-conformance assertion: a SQLiteTradeStore must be
	# assignable to a TradeStoreProtocol-typed name. mypy --strict checks this
	# block; a signature divergence from the Protocol fails the gate (this is
	# the type-level half of test #1's runtime duck check).
	def _accepts_protocol(store: TradeStoreProtocol) -> None: ...

	def _static_conformance_check(s: SQLiteTradeStore) -> None:
		_accepts_protocol(s)


_NOW = datetime(2026, 5, 16, 12, 0, 0, tzinfo=timezone.utc)
_NOW_ISO = _NOW.isoformat()


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
	return tmp_path / "live_trades.db"


@pytest.fixture
def store(db_path: Path) -> SQLiteTradeStore:
	"""A SQLiteTradeStore over a real on-disk live_trades.db (0003 + WAL)."""
	s = SQLiteTradeStore(db_path)
	yield s
	# Each test closes explicitly where it asserts close() behaviour; guard a
	# double-close here so the fixture teardown never raises.
	try:
		s.close()
	except sqlite3.ProgrammingError:
		pass


def _locked_pending_kwargs(**overrides: Any) -> dict[str, Any]:
	"""The exact 11-kwarg set dispatch.py passes to record_pending (pinned by
	test_engine_dispatch_pending_branch.py::
	test_record_pending_kwarg_set_is_exactly_locked_eleven)."""
	base: dict[str, Any] = {
		"ticker": "KXSOL15M-26MAY16H12",
		"series": "KXSOL15M",
		"strategy": "strat_34",
		"side": "yes",
		"intended_size": 10,
		"entry_price_cents": 42,
		"stop_loss_distance_cents": 8,
		"client_order_id": "strat_34-KXSOL15M-26MAY16H12-cafebabe",
		"kalshi_order_id": None,
		"placed_at_utc": _NOW_ISO,
		"rejection_reason": "kalshi_unreachable:connection refused",
	}
	base.update(overrides)
	return base


def _intent_kwargs(**overrides: Any) -> dict[str, Any]:
	"""The exact 9-kwarg set dispatch.py passes to ``record_intent`` (the
	pre-place durability hook — no ``kalshi_order_id`` / ``rejection_reason``;
	a subset of the locked pending kwargs). Used to seed the C1 ``pending``
	row before exercising the E-keystone CAS / backfill paths
	(``record_trade`` / ``record_pending`` / ``record_rejected``) — the exact
	insert-pending-then-CAS-transition model the live store now implements."""
	base: dict[str, Any] = {
		"ticker": "KXSOL15M-26MAY16H12",
		"series": "KXSOL15M",
		"strategy": "strat_34",
		"side": "yes",
		"intended_size": 10,
		"entry_price_cents": 42,
		"stop_loss_distance_cents": 8,
		"client_order_id": "strat_34-KXSOL15M-26MAY16H12-cafebabe",
		"placed_at_utc": _NOW_ISO,
	}
	base.update(overrides)
	return base


def _locked_rejected_kwargs(**overrides: Any) -> dict[str, Any]:
	"""The exact 10-kwarg set dispatch.py passes to record_rejected (pinned by
	test_engine_dispatch_pending_branch.py::
	test_record_rejected_kwarg_set_is_exactly_locked_ten) — no kalshi_order_id;
	rejection_reason REQUIRED."""
	base: dict[str, Any] = {
		"ticker": "KXSOL15M-26MAY16H12",
		"series": "KXSOL15M",
		"strategy": "strat_34",
		"side": "yes",
		"intended_size": 10,
		"entry_price_cents": 42,
		"stop_loss_distance_cents": 8,
		"client_order_id": "strat_34-KXSOL15M-26MAY16H12-rej00001",
		"placed_at_utc": _NOW_ISO,
		"rejection_reason": "kalshi_4xx:400",
	}
	base.update(overrides)
	return base


def _query_one(db_path: Path, sql: str, params: tuple[Any, ...] = ()) -> Any:
	"""Open an INDEPENDENT read connection (separate from the store's held
	conn) and fetch one row — proves the write actually committed to disk, not
	just buffered in the store's connection."""
	rc = connect_live_trades_db(db_path)
	try:
		rc.row_factory = sqlite3.Row
		return rc.execute(sql, params).fetchone()
	finally:
		rc.close()


def _seed_open_row(store: SQLiteTradeStore, **overrides: Any) -> int:
	"""Insert a real status='open' row via live.state.record_open over the
	store's held connection — i.e. the exact path E's PR-6 wiring will use
	for the filled-entry branch (with D's real OrderResult ids). Used to
	exercise the verified-correct READ surface (get_open_trades*) now that
	the paper-shaped ``store.record_trade`` is deliberately fail-loud and no
	longer the open-row writer."""
	base: dict[str, Any] = {
		"ticker": "KXSOL15M-26MAY16H12",
		"series": "KXSOL15M",
		"strategy": "strat_34",
		"side": "yes",
		"intended_size": 5,
		"fill_size": 5,
		"entry_price_cents": 42,
		"blended_entry_cents": 41,
		"slippage_cents": 0,
		"fill_pct": 1.0,
		"stop_loss_distance_cents": 0,
		"client_order_id": "strat_34-KXSOL15M-26MAY16H12-realcoid",
		"kalshi_order_id": "ord-kx-real-0001",
		"placed_at_utc": _NOW_ISO,
		"entry_time": _NOW_ISO,
		"entry_fee_cents": 7,
	}
	base.update(overrides)
	return record_open(store._conn, **base)


# ---------------------------------------------------------------------------
# #1 — Protocol conformance (structural; no inheritance)
# ---------------------------------------------------------------------------


def test_satisfies_trade_store_protocol_runtime_duck(store: SQLiteTradeStore) -> None:
	"""The live-path methods exist with the EXACT keyword-only signatures the
	Protocol declares. A structural (not isinstance) check — the TYPE_CHECKING
	block above is the mypy half; this is the runtime half via
	inspect.signature so a kwarg rename surfaces even without running mypy."""
	# record_pending — exactly the locked 11 keyword-only params.
	pend_sig = inspect.signature(store.record_pending)
	assert set(pend_sig.parameters) == {
		"ticker", "series", "strategy", "side", "intended_size",
		"entry_price_cents", "stop_loss_distance_cents", "client_order_id",
		"kalshi_order_id", "placed_at_utc", "rejection_reason",
	}
	assert all(
		p.kind is inspect.Parameter.KEYWORD_ONLY
		for p in pend_sig.parameters.values()
	), "record_pending params must be keyword-only (matches Protocol + dispatch)"

	# record_rejected — exactly the locked 10 (no kalshi_order_id).
	rej_sig = inspect.signature(store.record_rejected)
	assert set(rej_sig.parameters) == {
		"ticker", "series", "strategy", "side", "intended_size",
		"entry_price_cents", "stop_loss_distance_cents", "client_order_id",
		"placed_at_utc", "rejection_reason",
	}

	# The remaining live-path methods dispatch reaches (Step-1 surface).
	for name in ("record_trade", "get_open_trades", "get_open_trades_for", "close"):
		assert callable(getattr(store, name)), f"missing live-path method {name!r}"

	# Assignable to a Protocol-typed binding at runtime (duck — Protocol is
	# not @runtime_checkable for isinstance, so bind through a function arg).
	def _takes(_p: TradeStoreProtocol) -> None:
		return None

	_takes(store)  # mypy + runtime: SQLiteTradeStore IS a TradeStoreProtocol


# ---------------------------------------------------------------------------
# #2 — record_pending = idempotent kalshi_order_id BACKFILL of the C1 row
#      (E keystone — spec §3 supersedes B's CR-4 insert-on-outcome; the
#      durable pending row already exists from record_intent, this only
#      learns the kalshi_order_id while status STAYS 'pending'). C6 (F):
#      rewritten from the superseded B-era "INSERT a pending row" model.
# ---------------------------------------------------------------------------


def test_record_pending_backfills_kalshi_order_id_on_c1_row(
	store: SQLiteTradeStore, db_path: Path
) -> None:
	"""The live ``record_pending`` is NOT a 2nd insert — it BACKFILLS
	``kalshi_order_id`` onto the C1 ``record_intent`` row (located by
	``client_order_id``) while ``status`` stays ``'pending'`` (fill state
	still UNKNOWN; B's reconciler resolves it later). Exactly ONE row for the
	coid, still ``pending``, ``kalshi_order_id`` now set — never a competing
	second INSERT (spec §3/§4.2)."""
	coid = "strat_34-KXSOL15M-26MAY16H12-pend0001"
	store.record_intent(**_intent_kwargs(client_order_id=coid))

	# Pre-state: exactly one pending C1 row, kalshi_order_id still NULL.
	pre = _query_one(
		db_path,
		"SELECT status, kalshi_order_id FROM live_trades "
		"WHERE client_order_id = ?",
		(coid,),
	)
	assert pre["status"] == "pending"
	assert pre["kalshi_order_id"] is None

	kw = _locked_pending_kwargs(
		client_order_id=coid,
		kalshi_order_id="ord-kx-malformed-abc",  # malformed-fills path
		rejection_reason="kalshi_malformed_fills",
	)
	store.record_pending(**kw)

	# Exactly one row for the coid — backfilled in place, NOT a 2nd INSERT.
	all_rows = _query_one(
		db_path,
		"SELECT COUNT(*) AS n FROM live_trades WHERE client_order_id = ?",
		(coid,),
	)
	assert all_rows["n"] == 1, "backfill must not mint a second row (§3/§4.2)"
	row = _query_one(
		db_path,
		"SELECT * FROM live_trades WHERE client_order_id = ?",
		(coid,),
	)
	assert row is not None
	# Status STAYS 'pending' (still unknown — B's reconciler resolves later).
	assert row["status"] == "pending"
	assert row["ticker"] == "KXSOL15M-26MAY16H12"
	assert row["series"] == "KXSOL15M"
	assert row["strategy"] == "strat_34"
	assert row["side"] == "yes"
	assert row["intended_size"] == 10
	assert row["original_intended_size"] == 10  # set = intended_size by C1
	assert row["fill_size"] == 0
	assert row["entry_price_cents"] == 42
	assert row["stop_loss_distance_cents"] == 8
	# The load-bearing assertion: kalshi_order_id is BACKFILLED on the C1 row.
	assert row["kalshi_order_id"] == "ord-kx-malformed-abc"
	assert row["placed_at_utc"] == _NOW_ISO


def test_record_pending_networkerror_path_kalshi_id_stays_none(
	store: SQLiteTradeStore, db_path: Path
) -> None:
	"""Pure-NetworkError path: ``kalshi_order_id=None`` (no id returned) is a
	no-op backfill — the C1 row's NULL ``kalshi_order_id`` is left as-is (never
	null out an id; B's reconciler discriminates on a NULL id to fall back to
	``client_order_id``). Still exactly one row, still ``pending``."""
	coid = "strat_34-KXSOL15M-26MAY16H12-pendnone"
	store.record_intent(**_intent_kwargs(client_order_id=coid))

	kw = _locked_pending_kwargs(client_order_id=coid, kalshi_order_id=None)
	store.record_pending(**kw)

	row = _query_one(
		db_path,
		"SELECT kalshi_order_id, status FROM live_trades "
		"WHERE client_order_id = ?",
		(coid,),
	)
	assert row["status"] == "pending"
	assert row["kalshi_order_id"] is None
	count = _query_one(
		db_path,
		"SELECT COUNT(*) AS n FROM live_trades WHERE client_order_id = ?",
		(coid,),
	)
	assert count["n"] == 1, "no-op backfill must not mint a second row"


# ---------------------------------------------------------------------------
# #3 — record_rejected = CAS pending→rejected of the C1 row (E keystone —
#      spec §3 supersedes B's CR-4 insert-on-outcome). C6 (E): rewritten
#      from the superseded B-era "INSERT a rejected row" model.
# ---------------------------------------------------------------------------


def test_record_rejected_cas_transitions_c1_row_to_rejected(
	store: SQLiteTradeStore, db_path: Path
) -> None:
	"""The live ``record_rejected`` is NOT a 2nd insert — it CAS-transitions
	the C1 ``record_intent`` ``pending`` row (located by ``client_order_id``)
	to ``rejected`` with the reason persisted. Exactly ONE row for the coid,
	now ``status='rejected'`` — never a competing second INSERT (spec §3
	explicitly supersedes B's CR-4 one-row-per-attempt insert model)."""
	coid = "strat_34-KXSOL15M-26MAY16H12-rej00001"
	store.record_intent(**_intent_kwargs(client_order_id=coid))

	pre = _query_one(
		db_path,
		"SELECT status FROM live_trades WHERE client_order_id = ?",
		(coid,),
	)
	assert pre["status"] == "pending"

	kw = _locked_rejected_kwargs(
		client_order_id=coid, rejection_reason="absolute_max_exceeded"
	)
	store.record_rejected(**kw)

	count = _query_one(
		db_path,
		"SELECT COUNT(*) AS n FROM live_trades WHERE client_order_id = ?",
		(coid,),
	)
	assert count["n"] == 1, "CAS reject must not mint a second row (§3/§4.2)"
	row = _query_one(
		db_path,
		"SELECT * FROM live_trades WHERE client_order_id = ?",
		(coid,),
	)
	assert row is not None
	assert row["status"] == "rejected"
	assert row["rejection_reason"] == "absolute_max_exceeded"
	assert row["intended_size"] == 10
	assert row["original_intended_size"] == 10
	assert row["fill_size"] == 0


# ---------------------------------------------------------------------------
# #4 — record_pending best-effort posture (E keystone — C4). The durable C1
#      pending row already exists from record_intent, so a post-place
#      backfill failure / row-not-found is a logged ERROR audit gap, NEVER
#      fatal and SPECIFICALLY NOT RecordPendingFailed (ghost-reject scope is
#      funds-at-risk PRE-PLACE INSERTs only — that is C1's record_intent,
#      tested in tests/test_live_store_lifecycle.py). C6 (F): rewritten from
#      the superseded B-era "record_pending INSERT propagates
#      RecordPendingFailed" model — that funds-at-risk-INSERT contract now
#      lives on record_intent (the pre-place hook), not record_pending.
# ---------------------------------------------------------------------------


def test_record_pending_backfill_failure_is_best_effort_not_recordpendingfailed(
	db_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
	"""A post-place backfill write failure is NOT fatal: it logs an ERROR
	audit gap and does NOT raise — and SPECIFICALLY NOT ``RecordPendingFailed``
	(the durable C1 ``record_intent`` row already exists; B's reconciler owns
	recovery via ``client_order_id``; raising would needlessly halt the
	engine). Inverse of C1's ``record_intent`` (FATAL pre-place INSERT)."""
	import logging

	store = SQLiteTradeStore(db_path)
	try:
		coid = "strat_34-KXSOL15M-26MAY16H12-ghost001"
		store.record_intent(**_intent_kwargs(client_order_id=coid))
		# Sabotage the schema on the store's OWN held connection so the
		# backfill UPDATE genuinely fails with a sqlite3.Error.
		store._conn.execute("DROP TABLE live_trades")
		store._conn.commit()

		with caplog.at_level(logging.ERROR, logger="edge_catcher.live.store"):
			# MUST NOT raise (RecordPendingFailed or anything else) — §3.1
			# best-effort: the durable C1 pending row already exists.
			store.record_pending(
				**_locked_pending_kwargs(
					client_order_id=coid, kalshi_order_id="ord-kx-ghost"
				)
			)
		store_errs = [
			r for r in caplog.records
			if r.name == "edge_catcher.live.store" and r.levelname == "ERROR"
		]
		assert store_errs, (
			"a post-place backfill failure must be logged at ERROR (audit "
			"gap), not silently swallowed"
		)
	finally:
		store.close()


def test_record_pending_row_not_found_is_audit_gap_no_insert_no_raise(
	store: SQLiteTradeStore, db_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
	"""``record_pending`` with NO preceding ``record_intent`` (the C1 row
	absent — defense-in-depth): it MUST NOT raise, MUST NOT fabricate a
	competing INSERT (that would resurrect B's superseded insert-on-outcome
	model), and MUST emit an ERROR-level audit-gap log. B's reconciler is the
	backstop via ``client_order_id``."""
	import logging

	coid = "strat_34-KXSOL15M-26MAY16H12-noC1row"
	with caplog.at_level(logging.ERROR, logger="edge_catcher.live.store"):
		# MUST NOT raise — §3.1 accepted audit gap, not fatal.
		store.record_pending(
			**_locked_pending_kwargs(
				client_order_id=coid, kalshi_order_id="ord-kx-orphan"
			)
		)
	assert any(
		r.name == "edge_catcher.live.store" and r.levelname == "ERROR"
		for r in caplog.records
	), "a row-not-found record_pending must log an ERROR audit gap"
	# No row was fabricated for that coid.
	count = _query_one(
		db_path,
		"SELECT COUNT(*) AS n FROM live_trades WHERE client_order_id = ?",
		(coid,),
	)
	assert count["n"] == 0, "row-not-found must NOT be a silent INSERT (§3)"


def test_record_pending_is_idempotent_double_call(
	store: SQLiteTradeStore, db_path: Path
) -> None:
	"""The executor-pending branch may fire more than once for the same coid
	(a reconnect re-delivering the same NetworkError outcome). ``record_pending``
	called twice with the same ``kalshi_order_id`` MUST NOT raise, MUST leave
	exactly ONE row, still ``status='pending'``, ``kalshi_order_id``
	unchanged — a re-run is a no-op-equivalent (no corruption, no 2nd row)."""
	coid = "strat_34-KXSOL15M-26MAY16H12-idem001"
	store.record_intent(**_intent_kwargs(client_order_id=coid))
	kw = _locked_pending_kwargs(
		client_order_id=coid, kalshi_order_id="ord-kx-idem"
	)
	store.record_pending(**kw)
	store.record_pending(**kw)  # second call — idempotent, no raise

	row = _query_one(
		db_path,
		"SELECT status, kalshi_order_id FROM live_trades "
		"WHERE client_order_id = ?",
		(coid,),
	)
	assert row["status"] == "pending"
	assert row["kalshi_order_id"] == "ord-kx-idem"
	count = _query_one(
		db_path,
		"SELECT COUNT(*) AS n FROM live_trades WHERE client_order_id = ?",
		(coid,),
	)
	assert count["n"] == 1, "idempotent double-call must not mint a 2nd row"


# ---------------------------------------------------------------------------
# #5 — record_rejected carve-out preserved (inherited from live.state)
# ---------------------------------------------------------------------------


def test_record_rejected_insert_failure_does_not_raise(db_path: Path) -> None:
	"""Carve-out (spec §661/§930, PR #34 precedent): a failed record_rejected
	INSERT strands only an audit row (no Kalshi-side position) — live.state
	logs it best-effort and returns 0 WITHOUT raising RecordPendingFailed.
	The adapter must NOT re-raise/over-broaden: the engine continues."""
	store = SQLiteTradeStore(db_path)
	try:
		store._conn.execute("DROP TABLE live_trades")
		store._conn.commit()

		# Must NOT raise (RecordPendingFailed or anything else) — pure
		# delegation inherits live.state.record_rejected's swallow.
		store.record_rejected(
			**_locked_rejected_kwargs(
				client_order_id="strat_34-KXSOL15M-26MAY16H12-rejfail0"
			)
		)
	finally:
		store.close()


def test_record_rejected_cas_miss_no_pending_row_is_audit_gap_no_insert(
	store: SQLiteTradeStore, db_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
	"""C6 (E): the pre-place C-gate-reject case. dispatch may call
	``record_rejected`` with NO preceding ``record_intent`` (the C-gate
	rejected the order BEFORE C1 inserted a pending row, e.g.
	``absolute_max_exceeded``). The spec author KNEW this CAS-misses and
	DELIBERATELY accepted it as a logged ERROR audit gap, NOT fatal (a
	rejected order holds no position ⇒ not funds-at-risk). It MUST NOT raise,
	MUST NOT silently INSERT a row (that would resurrect B's superseded
	insert-on-outcome model), and MUST emit an ERROR-level audit-gap log."""
	import logging

	coid = "strat_34-KXSOL15M-26MAY16H12-noC1rej"
	with caplog.at_level(logging.ERROR, logger="edge_catcher.live.store"):
		# MUST NOT raise — §3.1 accepted audit gap, not fatal.
		store.record_rejected(
			**_locked_rejected_kwargs(
				client_order_id=coid,
				rejection_reason="absolute_max_exceeded",
			)
		)
	store_errs = [
		r for r in caplog.records
		if r.name == "edge_catcher.live.store" and r.levelname == "ERROR"
	]
	assert store_errs, (
		"a CAS-miss (no C1 pending row) must be logged at ERROR (spec §3.1 "
		"accepted audit gap), not silent"
	)
	assert "CAS-miss" in store_errs[-1].getMessage()
	# No row was fabricated — CAS-miss is NOT a silent INSERT (§3 supersedes
	# B's insert-on-outcome model).
	count = _query_one(
		db_path,
		"SELECT COUNT(*) AS n FROM live_trades WHERE client_order_id = ?",
		(coid,),
	)
	assert count["n"] == 0, "CAS-miss must NOT fabricate a rejected row (§3)"


def test_record_rejected_write_failure_is_best_effort_distinct_transient(
	db_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
	"""C6 (E): a TRANSIENT DB/disk write failure in the CAS is caller-owned
	best-effort — log ERROR (the DISTINCT transient "DB/disk fault" wording,
	NOT the UNEXPECTED/API-drift one), do NOT raise (a rejected order holds no
	position ⇒ not funds-at-risk; PR#34 ``438d843`` precedent). B's
	``transition_pending_to_rejected`` owns only a lost-CAS-race WARNING
	no-op, so the store owns the try/except."""
	import logging

	store = SQLiteTradeStore(db_path)
	try:
		coid = "strat_34-KXSOL15M-26MAY16H12-rejlog00"
		store.record_intent(**_intent_kwargs(client_order_id=coid))
		# DROP the table so the CAS SELECT/UPDATE genuinely fails with a
		# sqlite3.Error (the transient/disk carve-out branch).
		store._conn.execute("DROP TABLE live_trades")
		store._conn.commit()
		with caplog.at_level(logging.ERROR, logger="edge_catcher.live.store"):
			# MUST NOT raise — best-effort per §3.1.
			store.record_rejected(
				**_locked_rejected_kwargs(client_order_id=coid)
			)
	finally:
		store.close()
	store_errs = [
		r for r in caplog.records
		if r.name == "edge_catcher.live.store" and r.levelname == "ERROR"
	]
	assert store_errs, (
		"a write-failure in record_rejected must be logged at ERROR (audit "
		"gap), not silently swallowed"
	)
	msg = store_errs[-1].getMessage()
	# The DISTINCT transient carve-out wording, NOT the UNEXPECTED one.
	assert "DB/disk fault" in msg and "transient" in msg, (
		f"a sqlite3.Error must use the transient DB/disk carve-out — {msg!r}"
	)
	assert "UNEXPECTED" not in msg, (
		f"sqlite3.Error must NOT be the UNEXPECTED/API-drift class — {msg!r}"
	)


# ---------------------------------------------------------------------------
# #6 — close() closes the held connection
# ---------------------------------------------------------------------------


def test_close_closes_connection(db_path: Path) -> None:
	store = SQLiteTradeStore(db_path)
	store.close()
	with pytest.raises(sqlite3.ProgrammingError):
		store._conn.execute("SELECT 1 FROM live_trades")


def test_close_is_idempotent(db_path: Path) -> None:
	"""Double-close must not raise — E's shutdown path may call it more than
	once (SIGTERM + finally block)."""
	store = SQLiteTradeStore(db_path)
	store.close()
	store.close()  # second close is a no-op, not a ProgrammingError


# ---------------------------------------------------------------------------
# #7 — the post-fill live lifecycle methods are IMPLEMENTED (E keystone).
#
# C6 (A)+(B): the PR-5→PR-6 forcing-function PAIR
# (``test_pr6_contract_live_lifecycle_methods_are_failloud_until_e_wires`` +
# the ``strict=True`` xfail twin
# ``test_pr6_contract_xfails_when_e_implements_live_lifecycle``) has done its
# job: C2 (``record_trade`` CAS pending→open) + C5 (``exit_trade`` /
# ``settle_trade`` / ``get_trade_by_id`` route to B's ``record_close`` /
# canonical by-id read) implemented the live lifecycle, so E-obligation #1 is
# MET. Per spec §3.1 / the plan the strict-xfail twin XPASSes once E delivers
# the end-state and MUST be deleted in the same PR — it is gone. This
# rewritten green-guard replaces the old ``_LIVE_LIFECYCLE_CALLS`` fail-loud
# expectation with CONCRETE assertions of the IMPLEMENTED behaviour (mirrors
# the ``tests/test_live_store_lifecycle.py`` canonical patterns). The
# strategy-state methods (``save_state`` / ``load_state`` /
# ``load_all_states``) are resolved by SC-E3b (spec §10 / CR-3) to the
# spec-intended Phase-1 no-op (live starts FLAT every boot — positions
# rehydrate from ``live_trades.db`` via B's reconciler, NOT a store-owned
# ``strategy_state`` table) — positively asserted in
# ``test_strategy_state_methods_are_phase1_intentional_noop`` (itself the
# C6-precedent rewrite of the now-removed
# ``test_strategy_state_methods_remain_fail_loud`` forcing-function guard).
# ---------------------------------------------------------------------------


def _seed_intent_then_open(
	store: SQLiteTradeStore, *, coid: str, kalshi_id: str
) -> int:
	"""Drive a realistic OPEN live row through the real C1→C2 path (the same
	idiom as ``tests/test_live_store_lifecycle.py::_seed_open_live_row``): C1
	``record_intent`` INSERTs the ``pending`` row; C2 ``record_trade``
	CAS-transitions it to ``open`` with the authoritative fill recorded.
	Returns the row id. With ``fill_size=10, blended_entry=42``: C2 books
	``entry_fee_cents = int(round(STANDARD_FEE.calculate(42,10))) = 18`` and
	B's ``transition_pending_to_open`` seeds
	``entry_fee_remaining_cents = 18`` (the remainder ``record_close``
	consumes)."""
	store.record_intent(**_intent_kwargs(client_order_id=coid))
	tid = store.record_trade(
		ticker="KXSOL15M-26MAY16H12",
		entry_price=42,
		strategy="strat_34",
		side="yes",
		series_ticker="KXSOL15M",
		intended_size=10,
		fill_size=10,
		blended_entry=42,
		fill_pct=1.0,
		slippage_cents=0,
		now=_NOW,
		client_order_id=coid,
		kalshi_order_id=kalshi_id,
	)
	return int(tid)


def test_pr6_live_lifecycle_methods_implemented(
	store: SQLiteTradeStore, db_path: Path
) -> None:
	"""E keystone end-state (was the FIX-2 fail-loud guard +
	``_LIVE_LIFECYCLE_CALLS``): the four post-fill live lifecycle methods are
	now IMPLEMENTED, not fail-loud. Concretely asserts the REAL implemented
	behaviour on a seeded ``live_trades.db`` (no mocks):

	* ``record_trade`` CAS-transitions the C1 ``pending`` row → ``open``
	  (exactly one row, ``status='open'``, real ``kalshi_order_id`` set,
	  entry fee booked via B's canonical convention);
	* ``get_trade_by_id`` returns the paper-shaped 18-key dict (and ``None``
	  for an absent id);
	* ``exit_trade`` closes it via B ``record_close`` (won/lost + pnl +
	  entry-fee-remainder consumed);
	* ``settle_trade`` settles a fresh open row (binary 100/0,
	  ``exit_fee_cents=0``, ``exit_reason='settlement'``).
	"""
	# --- record_trade: CAS pending → open (NOT a 2nd INSERT). ---
	coid = "strat_34-KXSOL15M-26MAY16H12-pr6a"
	tid = _seed_intent_then_open(store, coid=coid, kalshi_id="ord-kx-pr6a")
	rows = store._conn.execute(
		"SELECT status, kalshi_order_id FROM live_trades "
		"WHERE client_order_id = ?",
		(coid,),
	).fetchall()
	assert rows == [("open", "ord-kx-pr6a")], (
		"record_trade must CAS the C1 row to open with the real "
		"kalshi_order_id — exactly one row, no competing INSERT"
	)
	open_row = _query_one(
		db_path,
		"SELECT status, fill_size, blended_entry_cents, entry_fee_cents, "
		"entry_fee_remaining_cents FROM live_trades WHERE id = ?",
		(tid,),
	)
	assert open_row["status"] == "open"
	assert open_row["fill_size"] == 10
	assert open_row["blended_entry_cents"] == 42
	# B's canonical entry-fee convention int(round(STANDARD_FEE.calc(42,10))).
	assert open_row["entry_fee_cents"] == 18
	assert open_row["entry_fee_remaining_cents"] == 18

	# --- get_trade_by_id: paper-shaped 18-key dict; None for absent id. ---
	d = store.get_trade_by_id(tid)
	assert d is not None
	assert d["id"] == tid
	assert d["ticker"] == "KXSOL15M-26MAY16H12"
	assert d["entry_price"] == 42  # aliased from entry_price_cents
	assert d["series_ticker"] == "KXSOL15M"  # aliased from series
	assert d["blended_entry"] == 42
	assert d["status"] == "open"
	assert d["book_depth"] is None  # no book-walk for live IOC fills
	# Closed-trade keys present (stable 18-key shape) but None on an open row.
	assert d["exit_price"] is None and d["exit_time"] is None
	assert d["pnl_cents"] is None
	assert set(d.keys()) == {
		"id", "ticker", "entry_price", "strategy", "side", "series_ticker",
		"entry_fee_cents", "intended_size", "fill_size", "blended_entry",
		"book_depth", "fill_pct", "slippage_cents", "status", "entry_time",
		"exit_price", "exit_time", "pnl_cents",
	}
	assert store.get_trade_by_id(999_999) is None  # paper-parity: not a raise

	# --- exit_trade: routes to B record_close (full close). ---
	# exit_fee = int(round(STANDARD_FEE.calculate(60,10))) = 17;
	# pnl = 10*(60-42) - 18 (entry_fee_remaining) - 17 = 145; 60 > 42 → won.
	store.exit_trade(tid, 60, now=_NOW)
	closed = _query_one(
		db_path,
		"SELECT status, exit_price_cents, exit_fee_cents, pnl_cents, "
		"exit_reason, entry_fee_cents, entry_fee_remaining_cents "
		"FROM live_trades WHERE id = ?",
		(tid,),
	)
	assert closed["status"] == "won"
	assert closed["exit_price_cents"] == 60
	assert closed["exit_fee_cents"] == 17
	assert closed["pnl_cents"] == 145
	assert closed["exit_reason"] == "ws_exit_fill"
	# Entry-fee-remainder CONSUMED by B's record_close (the load-bearing
	# B-CAS-close behaviour the paper single-UPDATE does NOT have).
	assert closed["entry_fee_cents"] == 18
	assert closed["entry_fee_remaining_cents"] == 0

	# --- settle_trade: routes to B record_close (settlement close). ---
	coid2 = "strat_34-KXSOL15M-26MAY16H12-pr6s"
	tid2 = _seed_intent_then_open(store, coid=coid2, kalshi_id="ord-kx-pr6s")
	# Market resolves YES → settlement_price 100, yes-side wins:
	# pnl = 10*(100-42) - 18 (entry_fee_remaining) = 562.
	store.settle_trade(tid2, "yes", now=_NOW)
	settled = _query_one(
		db_path,
		"SELECT status, exit_price_cents, exit_fee_cents, exit_reason, "
		"pnl_cents, entry_fee_remaining_cents "
		"FROM live_trades WHERE id = ?",
		(tid2,),
	)
	assert settled["status"] == "won"
	assert settled["exit_price_cents"] == 100
	assert settled["exit_fee_cents"] == 0  # Kalshi charges no settlement fee
	assert settled["exit_reason"] == "settlement"
	assert settled["pnl_cents"] == 562
	assert settled["entry_fee_remaining_cents"] == 0


def test_get_open_trades_returns_open_rows(
	store: SQLiteTradeStore, db_path: Path
) -> None:
	"""get_open_trades returns live open rows mapped to the paper dict shape
	(id/ticker/entry_price/strategy/side/series_ticker/fill_size/blended_entry/
	status) so strategy code + dispatch's TickContext are shape-compatible.

	Open rows are seeded via live.state.record_open directly (the real
	filled-entry writer E's PR-6 wiring uses) — store.record_trade is
	deliberately fail-loud and no longer the open-row writer."""
	tid = _seed_open_row(store, fill_size=5, blended_entry_cents=41)
	# A rejected row must NOT show up among open trades.
	store.record_rejected(
		**_locked_rejected_kwargs(client_order_id="x-rej-not-open")
	)
	rows = store.get_open_trades()
	assert len(rows) == 1
	r = rows[0]
	assert r["id"] == tid
	assert r["ticker"] == "KXSOL15M-26MAY16H12"
	assert r["entry_price"] == 42  # mapped from entry_price_cents
	assert r["strategy"] == "strat_34"
	assert r["side"] == "yes"
	assert r["series_ticker"] == "KXSOL15M"  # mapped from series
	assert r["fill_size"] == 5
	assert r["blended_entry"] == 41  # mapped from blended_entry_cents
	assert r["status"] == "open"


def test_get_open_trades_maps_null_blended_entry_to_none(
	store: SQLiteTradeStore, db_path: Path
) -> None:
	"""Locks the NULL-``blended_entry_cents`` read contract for E's PR-6 wiring.

	The ``0003`` schema declares ``blended_entry_cents INTEGER`` (nullable —
	"VWAP of fills; NULL until fill"). A reconcile-recovered ``open`` row can
	legitimately exist before its fill VWAP is confirmed, so a NULL here is a
	first-class schema-valid state (distinct from the NOT-NULL
	``entry_price_cents``). ``_open_row_to_dict`` must pass it through as
	``None`` — NOT coerce to 0, NOT crash — so E's PR-6 ``TickContext`` /
	strategy code sees a faithful "no blended entry yet" rather than a fake
	0c cost basis.

	Seeding: ``live.state.record_open`` requires a NON-NULL
	``blended_entry_cents`` (its signature is ``blended_entry_cents: int`` and
	it INSERTs the value directly), and ``transition_pending_to_open`` likewise
	always writes a non-NULL value — neither writer can *express* this
	legitimate schema state. So we seed a fully schema-valid open row via the
	real ``record_open`` path (the exact filled-entry writer E's PR-6 wiring
	uses, so every other column is realistic) and then apply a single
	controlled ``UPDATE ... SET blended_entry_cents = NULL`` on the test DB to
	produce the pre-fill-confirmation state the schema deliberately permits but
	no ``live.state`` writer can construct. Still 100% real SQLite — no DB or
	``live.state`` mocks (mirrors ``_seed_open_row`` for everything but the one
	deliberate NULL mutation).
	"""
	tid = _seed_open_row(
		store,
		client_order_id="strat_34-KXSOL15M-26MAY16H12-nullblend",
		kalshi_order_id="ord-kx-real-null0",
	)
	# Drive the row to the legitimate pre-fill-confirmation state the schema
	# permits but record_open / transition_pending_to_open cannot express
	# (both require a non-NULL blended_entry_cents). Controlled UPDATE over the
	# store's own real connection; commit so the independent read connection
	# below proves it landed on disk.
	store._conn.execute(
		"UPDATE live_trades SET blended_entry_cents = NULL WHERE id = ?",
		(tid,),
	)
	store._conn.commit()
	# Sanity: the seeded row is genuinely status='open' with a NULL
	# blended_entry_cents on disk (independent read connection — not the
	# store's buffered conn).
	disk = _query_one(
		db_path,
		"SELECT status, blended_entry_cents FROM live_trades WHERE id = ?",
		(tid,),
	)
	assert disk["status"] == "open"
	assert disk["blended_entry_cents"] is None

	# get_open_trades(): the NULL maps to None cleanly (no coercion, no crash),
	# every other key still correct.
	rows = store.get_open_trades()
	assert len(rows) == 1
	r = rows[0]
	assert r["id"] == tid
	assert r["blended_entry"] is None, (
		"NULL blended_entry_cents must map to None, NOT 0 — a fabricated 0c "
		"cost basis would silently corrupt E's PR-6 P&L/exit logic"
	)
	assert r["status"] == "open"
	assert r["ticker"] == "KXSOL15M-26MAY16H12"
	assert r["entry_price"] == 42  # the NOT-NULL entry_price_cents is unaffected
	assert r["strategy"] == "strat_34"
	assert r["side"] == "yes"
	assert r["series_ticker"] == "KXSOL15M"
	assert r["fill_size"] == 5

	# get_open_trades_for(...) honours the same contract on the filtered path.
	matched = store.get_open_trades_for("strat_34", "KXSOL15M-26MAY16H12")
	assert len(matched) == 1
	assert matched[0]["id"] == tid
	assert matched[0]["blended_entry"] is None


def test_get_open_trades_for_filters_by_strategy_and_ticker(
	store: SQLiteTradeStore,
) -> None:
	_seed_open_row(
		store,
		ticker="KXSOL15M-26MAY16H12",
		series="KXSOL15M",
		client_order_id="strat_34-KXSOL15M-26MAY16H12-coid1",
		kalshi_order_id="ord-kx-real-0001",
	)
	_seed_open_row(
		store,
		ticker="KXETH15M-26MAY16H12",
		series="KXETH15M",
		side="no",
		intended_size=3,
		fill_size=3,
		entry_price_cents=30,
		blended_entry_cents=29,
		client_order_id="strat_34-KXETH15M-26MAY16H12-coid2",
		kalshi_order_id="ord-kx-real-0002",
	)
	matched = store.get_open_trades_for("strat_34", "KXSOL15M-26MAY16H12")
	assert len(matched) == 1
	assert matched[0]["ticker"] == "KXSOL15M-26MAY16H12"
	# Non-matching ticker → empty.
	assert store.get_open_trades_for("strat_34", "KXDOGE-NOPE") == []


def test_strategy_state_methods_are_phase1_intentional_noop(
	store: SQLiteTradeStore,
) -> None:
	"""SC-E3b (spec §10 / CR-3) end-state — REWRITTEN from the C6-era
	forcing-function ``test_strategy_state_methods_remain_fail_loud`` (the
	same C6-precedent this file's own header at :625 sanctions: a
	forcing-function test is REWRITTEN — not silently deleted — by the PR that
	delivers the end-state it guarded the absence of; the OLD test pinned the
	pre-E3 ``NotImplementedError("live-only")`` state, SC-E3b explicitly
	resolves those 3 methods to the Phase-1 no-op, so the assertion INVERTS).

	Failure mode prevented: a future edit silently turns a strategy-state
	method into something OTHER than the spec-intended Phase-1 no-op (e.g.
	re-raises ``NotImplementedError`` and wedges live boot — ``run_engine``'s
	``store.load_all_states()`` is on the boot path — OR starts persisting a
	bogus ``strategy_state`` row that a flat-start restart then mis-rehydrates
	from). The SC-E3b/CR-3 contract: live starts FLAT every boot (zero
	inherited positions; the open book rehydrates from ``live_trades.db`` via
	B's reconciler, NOT a store-owned ``strategy_state`` table). Strategy
	state is reconstructable ⇒ a restart is a flat start; this is the
	spec-INTENDED behaviour (the store stays the sole live-vs-paper seam — the
	§1/§3 keystone — so ``run_engine`` carries NO ``if live:`` strategy-state
	branch), NOT a regression.

	(Asserts the no-op is INTENTIONAL: ``load_all_states`` → ``{}``,
	``save_state`` → ``None`` AND writes NO ``strategy_state`` row,
	``load_state`` → the empty-state default ``{}`` — matching the paper
	``TradeStore.load_state`` "no state" contract + the
	``TradeStoreProtocol`` ``dict[str, Any]`` return. The C5 money path —
	``record_*`` / ``exit_trade`` / ``settle_trade`` / ``get_*`` — is
	UNTOUCHED by SC-E3b and stays positively asserted by
	``test_pr6_live_lifecycle_methods_implemented`` + the
	``tests/test_live_store_lifecycle.py`` C5 suite.)"""
	# load_all_states → {} (seeds run_engine's `all_states.get(name, {})` flat)
	assert store.load_all_states() == {}, (
		"SC-E3b: load_all_states must return {} (Phase-1 flat start — live "
		"rehydrates from live_trades.db via B's reconciler, not a "
		"strategy_state table)"
	)

	# save_state → no-op returning None, persisting NOTHING (no strategy_state
	# row — a flat-start restart must NOT mis-rehydrate a stale strategy row).
	assert store.save_state("strat_34", {"k": 1}) is None, (
		"SC-E3b: save_state is a Phase-1 no-op returning None"
	)
	tables = {
		r[0]
		for r in store._conn.execute(
			"SELECT name FROM sqlite_master WHERE type='table'"
		).fetchall()
	}
	assert "strategy_state" not in tables, (
		"SC-E3b: save_state must write NO strategy_state row/table (Phase-1 "
		"no-op — live has no store-owned strategy-state persistence)"
	)

	# load_state → the empty-state default {} (paper TradeStore.load_state "no
	# state" contract + the TradeStoreProtocol dict[str, Any] return type).
	assert store.load_state("strat_34") == {}, (
		"SC-E3b: load_state must return the empty-state default {} (Phase-1 "
		"flat start), matching paper TradeStore.load_state's no-state return"
	)
	# Even after a save_state call it stays {} (the no-op persisted nothing).
	store.save_state("strat_34", {"k": 1})
	assert store.load_state("strat_34") == {}, (
		"SC-E3b: load_state stays {} after save_state — the Phase-1 no-op "
		"round-trip persists nothing (intentional flat-start, not a bug)"
	)


# ---------------------------------------------------------------------------
# Dual-slippage references — record_intent persists onto the pending row
# (spec §4.2 / §9 / §11 — the two refs feed transition_pending_to_open's
# compute on every entry-fill path: sync record_trade + WS-handler +
# reconciler. Migration 0004 added the two INTEGER columns to live_trades.)
# ---------------------------------------------------------------------------


def test_record_intent_persists_dual_slippage_refs(
	store: SQLiteTradeStore, db_path: Path
) -> None:
	"""Per spec §4.2: SQLiteTradeStore.record_intent threads
	``entry_best_price_cents`` + ``entry_limit_price_cents`` through to
	``live.state.record_pending``, which INSERTs them onto the pending row.
	The values must round-trip exactly (INTEGER cents) and be readable from
	an independent connection — i.e. actually committed to disk."""
	coid = "strat_34-KXSOL15M-26MAY16H12-refs0001"
	store.record_intent(
		**_intent_kwargs(client_order_id=coid),
		entry_best_price_cents=41,
		entry_limit_price_cents=45,
	)

	row = _query_one(
		db_path,
		"SELECT status, entry_best_price_cents, entry_limit_price_cents "
		"FROM live_trades WHERE client_order_id = ?",
		(coid,),
	)
	assert row is not None
	assert row["status"] == "pending"
	assert row["entry_best_price_cents"] == 41, (
		"spec §4.2: pending row must persist entry_best_price_cents for "
		"transition_pending_to_open to compute market_impact_cents at fill"
	)
	assert row["entry_limit_price_cents"] == 45, (
		"spec §4.2: pending row must persist entry_limit_price_cents for "
		"transition_pending_to_open to compute limit_slippage_cents at fill"
	)


def test_record_intent_default_dual_slippage_refs_null(
	store: SQLiteTradeStore, db_path: Path
) -> None:
	"""Omitting the new kwargs (the ~20 existing _intent_kwargs() call sites
	keep working unchanged per spec §4.2) persists NULL for both — "not
	measurable" sentinel per spec §4.3, NOT zero. transition_pending_to_open
	will then leave the metric columns NULL on this row's fill (covers a
	pre-0004 pending row reconciling post-0004; tested by Step 10)."""
	coid = "strat_34-KXSOL15M-26MAY16H12-refsnull"
	store.record_intent(**_intent_kwargs(client_order_id=coid))

	row = _query_one(
		db_path,
		"SELECT entry_best_price_cents, entry_limit_price_cents "
		"FROM live_trades WHERE client_order_id = ?",
		(coid,),
	)
	assert row is not None
	assert row["entry_best_price_cents"] is None, (
		"spec §4.3: default is NULL = 'not measurable', never 0"
	)
	assert row["entry_limit_price_cents"] is None, (
		"spec §4.3: default is NULL = 'not measurable', never 0"
	)


def test_record_trade_ignores_dual_slippage_kwargs_uses_refs_instead(
	store: SQLiteTradeStore, db_path: Path
) -> None:
	"""Per spec §4.2 / §5.2 + simplicity/coverage review: live
	SQLiteTradeStore.record_trade accepts the two metric kwargs uniformly
	(so dispatch never branches paper-vs-live) but IGNORES them — live's
	authoritative compute happens at transition_pending_to_open from the
	refs persisted on the pending row.

	Distinguishing test: pass a wrong sentinel value (999) as the kwarg —
	the row must end up with the COMPUTED metrics (from refs), NOT the
	kwarg sentinel. Proves both halves:
	(1) live IGNORES the kwarg (else the row would carry 999), and
	(2) live COMPUTES from refs at the transition (else the row would be NULL).
	"""
	coid = "strat_34-KXSOL15M-26MAY16H12-ignore-kwarg"
	# Seed pending row with refs: best=40, limit=45.
	store.record_intent(
		**_intent_kwargs(client_order_id=coid),
		entry_best_price_cents=40,
		entry_limit_price_cents=45,
	)
	# CAS to open with blended=42. Pass intentionally-wrong sentinels for the
	# two metric kwargs to prove they're ignored.
	store.record_trade(
		ticker="KXSOL15M-26MAY16H12",
		entry_price=42,
		strategy="strat_34",
		side="yes",
		series_ticker="KXSOL15M",
		intended_size=10,
		fill_size=10,
		blended_entry=42,
		fill_pct=1.0,
		slippage_cents=2,
		now=_NOW,
		client_order_id=coid,
		kalshi_order_id="ord-kx-ignore",
		market_impact_cents=999,
		limit_slippage_cents=999,
	)
	row = _query_one(
		db_path,
		"SELECT market_impact_cents, limit_slippage_cents "
		"FROM live_trades WHERE client_order_id = ?",
		(coid,),
	)
	assert row is not None
	# buy convention: blended - ref. 42-40=+2, 42-45=-3.
	assert row["market_impact_cents"] == 2, (
		"live IGNORES the kwarg AND computes from refs at transition; "
		"got %r — either the kwarg leaked through (broken §5.2) or the "
		"compute was skipped (broken §6)" % row["market_impact_cents"]
	)
	assert row["limit_slippage_cents"] == -3, (
		"live IGNORES the kwarg AND computes from refs at transition; "
		"got %r" % row["limit_slippage_cents"]
	)
