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
	RecordPendingFailed,
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
		"strategy": "debut_fade",
		"side": "yes",
		"intended_size": 10,
		"entry_price_cents": 42,
		"stop_loss_distance_cents": 8,
		"client_order_id": "debut_fade-KXSOL15M-26MAY16H12-cafebabe",
		"kalshi_order_id": None,
		"placed_at_utc": _NOW_ISO,
		"rejection_reason": "kalshi_unreachable:connection refused",
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
		"strategy": "debut_fade",
		"side": "yes",
		"intended_size": 10,
		"entry_price_cents": 42,
		"stop_loss_distance_cents": 8,
		"client_order_id": "debut_fade-KXSOL15M-26MAY16H12-rej00001",
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
		"strategy": "debut_fade",
		"side": "yes",
		"intended_size": 5,
		"fill_size": 5,
		"entry_price_cents": 42,
		"blended_entry_cents": 41,
		"slippage_cents": 0,
		"fill_pct": 1.0,
		"stop_loss_distance_cents": 0,
		"client_order_id": "debut_fade-KXSOL15M-26MAY16H12-realcoid",
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
# #2 — record_pending writes a real pending row, kwargs forwarded faithfully
# ---------------------------------------------------------------------------


def test_record_pending_writes_real_pending_row(
	store: SQLiteTradeStore, db_path: Path
) -> None:
	"""dispatch's pending branch → a real status='pending' row in
	live_trades.db with every locked kwarg faithfully forwarded to
	live.state.record_pending."""
	kw = _locked_pending_kwargs(
		client_order_id="debut_fade-KXSOL15M-26MAY16H12-pend0001",
		kalshi_order_id="ord-kx-malformed-abc",  # malformed-fills path
		rejection_reason="kalshi_malformed_fills",
	)
	store.record_pending(**kw)

	row = _query_one(
		db_path,
		"SELECT * FROM live_trades WHERE client_order_id = ?",
		(kw["client_order_id"],),
	)
	assert row is not None, "pending row must be committed to live_trades.db"
	assert row["status"] == "pending"
	assert row["ticker"] == "KXSOL15M-26MAY16H12"
	assert row["series"] == "KXSOL15M"
	assert row["strategy"] == "debut_fade"
	assert row["side"] == "yes"
	assert row["intended_size"] == 10
	assert row["original_intended_size"] == 10  # set = intended_size on INSERT
	assert row["fill_size"] == 0
	assert row["entry_price_cents"] == 42
	assert row["stop_loss_distance_cents"] == 8
	assert row["kalshi_order_id"] == "ord-kx-malformed-abc"
	assert row["placed_at_utc"] == _NOW_ISO
	assert row["rejection_reason"] == "kalshi_malformed_fills"


def test_record_pending_networkerror_path_kalshi_id_none(
	store: SQLiteTradeStore, db_path: Path
) -> None:
	"""NetworkError path: kalshi_order_id=None must persist as a NULL column
	(B's reconciler discriminates on this to fall back to client_order_id)."""
	kw = _locked_pending_kwargs(
		client_order_id="debut_fade-KXSOL15M-26MAY16H12-pendnone",
		kalshi_order_id=None,
	)
	store.record_pending(**kw)
	row = _query_one(
		db_path,
		"SELECT kalshi_order_id, status FROM live_trades WHERE client_order_id = ?",
		(kw["client_order_id"],),
	)
	assert row["status"] == "pending"
	assert row["kalshi_order_id"] is None


# ---------------------------------------------------------------------------
# #3 — record_rejected writes a real rejected row; reason persisted
# ---------------------------------------------------------------------------


def test_record_rejected_writes_real_rejected_row(
	store: SQLiteTradeStore, db_path: Path
) -> None:
	kw = _locked_rejected_kwargs(rejection_reason="absolute_max_exceeded")
	store.record_rejected(**kw)

	row = _query_one(
		db_path,
		"SELECT * FROM live_trades WHERE client_order_id = ?",
		(kw["client_order_id"],),
	)
	assert row is not None
	assert row["status"] == "rejected"
	assert row["rejection_reason"] == "absolute_max_exceeded"
	assert row["intended_size"] == 10
	assert row["original_intended_size"] == 10
	assert row["fill_size"] == 0


# ---------------------------------------------------------------------------
# #4 — GHOST-REJECT PROPAGATION (the load-bearing test)
# ---------------------------------------------------------------------------


def test_record_pending_propagates_record_pending_failed(db_path: Path) -> None:
	"""THE reason this adapter exists. Point the store at a connection whose
	live_trades table has been DROPPED → live.state.record_pending's INSERT
	hits sqlite3.OperationalError → it raises RecordPendingFailed → the
	adapter MUST let it propagate UNCAUGHT.

	If the adapter wrapped the delegating call in a try/except that swallowed
	this, the engine's three `except RecordPendingFailed: raise` ghost-reject
	clauses (dispatch.process_tick, engine._ws_loop, engine outer reconnect)
	would be dead code in the live path and a funds-at-risk stranded Kalshi
	order would go undetected."""
	store = SQLiteTradeStore(db_path)
	try:
		# Sabotage the schema on the store's OWN held connection so the next
		# INSERT genuinely fails inside live.state.record_pending.
		store._conn.execute("DROP TABLE live_trades")
		store._conn.commit()

		with pytest.raises(RecordPendingFailed):
			store.record_pending(
				**_locked_pending_kwargs(
					client_order_id="debut_fade-KXSOL15M-26MAY16H12-ghost001"
				)
			)
	finally:
		store.close()


def test_record_trade_is_failloud_not_synthesizing(
	store: SQLiteTradeStore, db_path: Path
) -> None:
	"""REAL-MONEY DEFECT FIX (was: ``test_record_trade_maps_to_record_open``
	+ ``..._open_propagates_record_pending_failed``).

	The paper-shaped ``TradeStoreProtocol.record_trade`` signature cannot
	carry D's real ``OrderResult.order_id`` (→ ``kalshi_order_id``) or
	``client_order_id``. The pre-fix adapter SYNTHESIZED placeholder ids
	(``kx-{strategy}-{ts}`` / ``-live``) to satisfy ``record_open``'s
	NOT-NULL columns, persisting a funds-at-risk ``open`` row that 4.B's
	reconciler / ``on_fill_event`` / phantom-pending poller can NEVER
	reconcile (they key off the real Kalshi/client ids).

	``record_trade`` must therefore fail loud exactly like the other
	paper-path lifecycle methods (``self._live_only``-style
	``NotImplementedError``), and the message MUST name ``record_open`` and
	the E/PR-6 wiring obligation per spec §769 / §To-E so the contract is
	discoverable from the failure alone.

	Fails on 5d0a6b5 (where it synthesized + returned an int trade_id);
	passes after the fix.
	"""
	with pytest.raises(NotImplementedError, match="live-only") as exc:
		store.record_trade(
			ticker="KXSOL15M-26MAY16H12",
			entry_price=42,
			strategy="debut_fade",
			side="yes",
			series_ticker="KXSOL15M",
			intended_size=10,
			fill_size=10,
			blended_entry=40,
			fill_pct=1.0,
			slippage_cents=0.0,
			now=_NOW,
		)
	msg = str(exc.value)
	# Names the correct live writer + the E/PR-6 obligation + the spec cite.
	assert "record_open" in msg, "message must name live.state.record_open"
	assert "PR 6" in msg, "message must state the E/PR-6 wiring obligation"
	assert "§769" in msg or "To-E" in msg, "message must cite the spec contract"
	assert "client_order_id" in msg and "kalshi_order_id" in msg, (
		"message must explain WHY the paper signature is not live-correct "
		"(cannot carry the real client_order_id / kalshi_order_id)"
	)
	# It genuinely did NOT write anything (no synthesized open row leaked to
	# disk on the fail-loud path).
	leaked = _query_one(
		db_path,
		"SELECT COUNT(*) AS n FROM live_trades WHERE status = 'open'",
	)
	assert leaked["n"] == 0, "fail-loud record_trade must not persist any row"


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
				client_order_id="debut_fade-KXSOL15M-26MAY16H12-rejfail0"
			)
		)
	finally:
		store.close()


def test_record_rejected_carveout_logs_audit_gap(
	db_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
	"""The carve-out is operator-visible: a failed audit INSERT emits the
	`rejected_audit_write_failed` ERROR line (from live.state, via the
	adapter's pure delegation — the adapter adds no log of its own)."""
	import logging

	store = SQLiteTradeStore(db_path)
	try:
		store._conn.execute("DROP TABLE live_trades")
		store._conn.commit()
		with caplog.at_level(logging.ERROR, logger="edge_catcher.live.state"):
			store.record_rejected(
				**_locked_rejected_kwargs(
					client_order_id="debut_fade-KXSOL15M-26MAY16H12-rejlog00"
				)
			)
	finally:
		store.close()
	assert any(
		"rejected_audit_write_failed" in rec.message for rec in caplog.records
	), "carve-out must surface the audit gap at ERROR (operator-visible)"


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
# #7 — other implemented live-path methods + NotImplementedError stubs
# ---------------------------------------------------------------------------


_LIVE_LIFECYCLE_CALLS = (
	lambda s: s.record_trade(
		ticker="KXSOL15M-26MAY16H12",
		entry_price=42,
		strategy="debut_fade",
		side="yes",
		series_ticker="KXSOL15M",
		now=_NOW,
	),
	lambda s: s.exit_trade(1, 50, now=_NOW),
	lambda s: s.get_trade_by_id(1),
	lambda s: s.settle_trade(1, "yes", now=_NOW),
)


def test_pr6_contract_live_lifecycle_methods_are_failloud_until_e_wires(
	store: SQLiteTradeStore,
) -> None:
	"""FIX-2 ENFORCEMENT (part 1 of 2 — the always-on regression guard).

	Merged ``engine/dispatch.py`` has NO live-vs-paper branching:
	``_handle_signal`` sends every exit Signal to ``_handle_exit`` (which
	calls ``store.exit_trade`` then ``store.get_trade_by_id``) and the
	filled branch calls the paper-shaped ``store.record_trade`` — all of
	which MUST be fail-loud on this adapter until E (PR 6) rewires those
	live arms to B's real ``live.state`` functions (see ``live/store.py``
	``PR-5 → PR-6 (E) CONTRACT``).

	This is a NORMAL test (green now). It is the unambiguous enforcement:
	the moment anyone makes ``record_trade`` / ``exit_trade`` /
	``get_trade_by_id`` / ``settle_trade`` reachable-or-implemented on this
	adapter WITHOUT also addressing the dispatch live-arm rewire, one of
	these ``pytest.raises`` stops matching and this test goes RED — a hard,
	unmissable CI failure that drags the contract back into review.
	Companion ``..._xfails_when_e_implements_them`` below is the
	strict-xfail twin that flips specifically when E delivers the
	end-state, mirroring the 4.C reporting-CLI-gap pattern.

	(Why split: a SINGLE ``strict=True`` xfail asserting "still fail-loud"
	is logically inverted — its body PASSES today, so strict-xfail would
	mark it XPASS=fail on the clean baseline. The clean, correct
	construction is this always-green guard PLUS the separate strict xfail
	that asserts the desired END state — fails now → XFAIL, passes when E
	implements → XPASS → strict CI failure.)
	"""
	for call in _LIVE_LIFECYCLE_CALLS:
		with pytest.raises(NotImplementedError, match="live-only"):
			call(store)


@pytest.mark.xfail(
	strict=True,
	reason="PR-6/E must rewire dispatch live arms; see live/store.py "
	"PR-5 → PR-6 (E) CONTRACT. Asserts the desired END state (the four "
	"live lifecycle methods no longer raise). Fails now (still fail-loud) "
	"→ XFAIL (green). XPASSes (=> strict CI failure) the day E makes them "
	"reachable/implemented, forcing this contract back into review.",
)
def test_pr6_contract_xfails_when_e_implements_live_lifecycle(
	store: SQLiteTradeStore,
) -> None:
	"""FIX-2 ENFORCEMENT (part 2 of 2 — the strict-xfail forcing function).

	Asserts the post-E END state: none of the four paper-shaped live
	lifecycle methods raise ``NotImplementedError`` any more. TODAY they
	all DO raise, so this body fails → ``strict=True`` xfail records XFAIL
	(expected; suite stays green). When E (PR 6) rewires dispatch and
	implements/redirects these so they no longer fail-loud, this body
	passes → XPASS → ``strict=True`` converts XPASS into a CI FAILURE.
	That red is the forcing function: the E-obligation is met, so this
	PR-5→PR-6 contract block (and this pair of tests) must be revisited /
	retired. "Fixed" == "xpass" == "red", per the task's enforcement spec.
	"""
	for call in _LIVE_LIFECYCLE_CALLS:
		# No pytest.raises: if any still raises NotImplementedError it
		# propagates, the body fails, and strict-xfail keeps this XFAIL
		# (green) — i.e. the gap is still open, which is correct today.
		call(store)


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
	assert r["strategy"] == "debut_fade"
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
		client_order_id="debut_fade-KXSOL15M-26MAY16H12-nullblend",
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
	assert r["strategy"] == "debut_fade"
	assert r["side"] == "yes"
	assert r["series_ticker"] == "KXSOL15M"
	assert r["fill_size"] == 5

	# get_open_trades_for(...) honours the same contract on the filtered path.
	matched = store.get_open_trades_for("debut_fade", "KXSOL15M-26MAY16H12")
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
		client_order_id="debut_fade-KXSOL15M-26MAY16H12-coid1",
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
		client_order_id="debut_fade-KXETH15M-26MAY16H12-coid2",
		kalshi_order_id="ord-kx-real-0002",
	)
	matched = store.get_open_trades_for("debut_fade", "KXSOL15M-26MAY16H12")
	assert len(matched) == 1
	assert matched[0]["ticker"] == "KXSOL15M-26MAY16H12"
	# Non-matching ticker → empty.
	assert store.get_open_trades_for("debut_fade", "KXDOGE-NOPE") == []


@pytest.mark.parametrize(
	"method_call",
	[
		lambda s: s.settle_trade(1, "yes", now=_NOW),
		lambda s: s.exit_trade(1, 50, now=_NOW),
		lambda s: s.get_trade_by_id(1),
		lambda s: s.save_state("debut_fade", {"k": 1}),
		lambda s: s.load_state("debut_fade"),
		lambda s: s.load_all_states(),
	],
)
def test_paper_path_methods_raise_not_implemented(
	store: SQLiteTradeStore, method_call: Any
) -> None:
	"""Paper-path Protocol methods with no live-money-correct live.state
	mapping (live close/exit/settle are CAS-guarded WS-handler/reconciliation
	driven against live_trades.db directly, NOT store.settle_trade-shaped;
	live state is in live_trades.db, not the store's strategy_state). They
	raise a clear NotImplementedError rather than silently no-op'ing into a
	wrong real-money result. E wires the live path so these are unreachable."""
	with pytest.raises(NotImplementedError, match="live-only"):
		method_call(store)
