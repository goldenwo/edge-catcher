"""Lifecycle tests for ``SQLiteTradeStore.record_intent`` (sub-project E / C1).

``record_intent`` is the LIVE pre-place durability hook: dispatch (E's later
wiring) calls ``store.record_intent(...)`` UNCONDITIONALLY immediately BEFORE
``await executor.place(req)``. On the live store this must durably INSERT a
``pending`` row keyed by ``client_order_id`` BEFORE any order is sent, so a
severed place→persist is recoverable by B's reconciler and there is never an
untracked real-money position.

Spec cross-refs: §3 (pre-place ``pending`` row, delegate to
``live.state.record_pending`` with ``kalshi_order_id=None``), §3.1
(NORMATIVE — ``record_intent`` failure is FATAL: ``RecordPendingFailed``
propagates, entry aborts BEFORE ``place()``, safe by construction), §4.2
(every post-place outcome is a later CAS transition on THIS row), §5
(conform to B — reuse ``live.state`` writers, no new SQL/threads/locks).

Nothing is mocked in the happy path — the REAL ``connect_live_trades_db``
(0003 migration + WAL) + REAL ``live.state.record_pending`` + REAL SQLite
chain is exercised. The failure test monkeypatches ``live.state``'s
``record_pending`` at the exact module attribute ``store.record_intent``
resolves (``edge_catcher.live.store.record_pending`` — store imports the name
into its own namespace) so the patch genuinely intercepts the call and the
``RecordPendingFailed``-propagates assertion is real, not a no-op patch.
"""
from __future__ import annotations

import sqlite3

import pytest

from edge_catcher.live.state import RecordPendingFailed
from edge_catcher.live.store import SQLiteTradeStore

INTENT = dict(ticker="KXSOL15M-X", series="KXSOL15M", strategy="strat-34",
	side="yes", intended_size=5, entry_price_cents=5,
	stop_loss_distance_cents=3, client_order_id="cid-A",
	placed_at_utc="2026-05-18T00:00:00+00:00")


def test_record_intent_inserts_pending_row(tmp_path):
	s = SQLiteTradeStore(tmp_path / "live_trades.db")
	s.record_intent(**INTENT)
	rows = s._conn.execute(
		"SELECT status, client_order_id FROM live_trades").fetchall()
	assert rows == [("pending", "cid-A")]
	s.close()


def test_record_intent_failure_raises_RecordPendingFailed(tmp_path, monkeypatch):
	s = SQLiteTradeStore(tmp_path / "live_trades.db")
	# Patch the name AS RESOLVED BY store.record_intent. store.py does
	# ``from edge_catcher.live.state import record_pending`` so the live
	# delegation binds ``edge_catcher.live.store.record_pending``; patching
	# ``edge_catcher.live.state.record_pending`` would NOT intercept the call
	# (stale-binding pitfall) — patch the store module's attribute so the
	# RecordPendingFailed-propagates assertion is genuine.
	import edge_catcher.live.store as store_mod
	monkeypatch.setattr(store_mod, "record_pending",
		lambda *a, **k: (_ for _ in ()).throw(RecordPendingFailed("disk")))
	with pytest.raises(RecordPendingFailed):
		s.record_intent(**INTENT)
	s.close()


def test_record_trade_cas_pending_to_open(tmp_path):
	"""C2 / spec §3 (`:400 filled` row), §4.2, §5.

	On the LIVE store ``record_trade`` is NOT an insert — it is a CAS
	``pending → open`` transition on the C1-inserted row located by
	``client_order_id``, via B's ``live.state.transition_pending_to_open``
	(no hand-rolled SQL). After ``record_intent`` (C1 inserts the pending
	row) then ``record_trade`` there must be EXACTLY ONE row, now
	``status='open'`` with the real ``kalshi_order_id`` set and the fill
	fields populated — a transition, never a competing second INSERT
	(§4.2). Entry fee follows B's canonical convention
	``int(round(STANDARD_FEE.calculate(blended_entry_cents, fill_size)))``
	(ws_handlers._entry_fee_cents / reconciliation._resolve_matched_pending).

	With INTENT (entry_price_cents=5, intended_size=5, coid='cid-A') then
	record_trade(fill_size=5, blended_entry=5): blended=5, fill_size=5 →
	STANDARD_FEE.calculate(5,5) = ceil(0.07*5*0.05*0.95*100)=ceil(1.6625)
	=2.0 → int(round(2.0))=2, so entry_fee_cents == 2 and
	entry_fee_remaining_cents seeds == 2 (transition_pending_to_open sets
	both). slippage_cents/fill_pct passed verbatim by the caller.
	"""
	import datetime as _dt

	s = SQLiteTradeStore(tmp_path / "live_trades.db")
	s.record_intent(**INTENT)
	tid = s.record_trade(
		ticker="KXSOL15M-X", entry_price=5, strategy="strat-34",
		side="yes", series_ticker="KXSOL15M", intended_size=5,
		fill_size=5, blended_entry=5, book_depth=None, fill_pct=1.0,
		slippage_cents=0, book_snapshot=None,
		now=_dt.datetime.fromisoformat("2026-05-18T00:00:01+00:00"),
		client_order_id="cid-A", kalshi_order_id="ord-1")

	# Exactly one row, transitioned in place — NOT a second INSERT (§4.2).
	rows = s._conn.execute(
		"SELECT status, kalshi_order_id FROM live_trades").fetchall()
	assert rows == [("open", "ord-1")]

	# Full post-transition row shape (modelled on B's
	# tests/test_live_state_integration.py fixtures): pending → open with
	# the authoritative fill recorded; original_intended_size immutable.
	s._conn.row_factory = sqlite3.Row
	r = s._conn.execute(
		"SELECT * FROM live_trades WHERE client_order_id = 'cid-A'"
	).fetchone()
	assert r["status"] == "open"
	assert r["kalshi_order_id"] == "ord-1"
	assert r["fill_size"] == 5
	assert r["blended_entry_cents"] == 5
	assert r["slippage_cents"] == 0
	assert r["fill_pct"] == 1.0
	assert r["entry_time"] == "2026-05-18T00:00:01+00:00"
	assert r["entry_fee_cents"] == 2
	assert r["entry_fee_remaining_cents"] == 2
	# record_trade returns the transitioned row's id (paper parity:
	# record_trade -> int trade id).
	assert tid == r["id"]
	s.close()


def test_record_trade_requires_kalshi_order_id(tmp_path):
	"""C2 hardening / spec §3 — symmetric fail-loud on the OTHER identity key.

	``record_trade`` already raises ``ValueError`` on a missing
	``client_order_id`` (the C1 row is located by it). ``kalshi_order_id`` is
	symmetrically load-bearing on the live filled path: B's WS reconciler /
	``on_fill_event`` key off it, so an empty / None id would write
	unreconcilable silent-bad-state. On the real live entry path D's
	``place()`` always supplies a real id; an absent one is a wiring bug that
	MUST fail loud (zero-error-with-live-money lens), symmetric to the
	``client_order_id`` guard.

	Same args as ``test_record_trade_cas_pending_to_open`` so the ONLY
	variable is the missing ``kalshi_order_id`` (None, then ""). Both must
	raise ``ValueError``, and the C1 ``pending`` row MUST be left
	uncorrupted — exactly one row, still ``status='pending'`` (the failed
	``record_trade`` must not have transitioned or written it).
	"""
	import datetime as _dt

	s = SQLiteTradeStore(tmp_path / "live_trades.db")
	s.record_intent(**INTENT)

	for bad_kalshi_id in (None, ""):
		with pytest.raises(ValueError):
			s.record_trade(
				ticker="KXSOL15M-X", entry_price=5, strategy="strat-34",
				side="yes", series_ticker="KXSOL15M", intended_size=5,
				fill_size=5, blended_entry=5, book_depth=None, fill_pct=1.0,
				slippage_cents=0, book_snapshot=None,
				now=_dt.datetime.fromisoformat("2026-05-18T00:00:01+00:00"),
				client_order_id="cid-A", kalshi_order_id=bad_kalshi_id)

		# C1 row uncorrupted: still exactly one row, still pending — the
		# failed record_trade neither transitioned nor wrote it.
		rows = s._conn.execute(
			"SELECT status, client_order_id FROM live_trades").fetchall()
		assert rows == [("pending", "cid-A")]

	s.close()


# -----------------------------------------------------------------------------
# C3 — LIVE record_rejected = CAS pending→rejected (NOT insert-on-outcome),
# caller-owned best-effort (spec §3 supersedes B's CR-4 insert model; §3.1
# CAS-miss/write-failure is a logged ERROR audit gap, NOT fatal — a rejected
# order holds no position ⇒ not funds-at-risk).
# -----------------------------------------------------------------------------

REJECT_KW = dict(ticker="KXSOL15M-X", series="KXSOL15M",
	strategy="strat-34", side="yes", intended_size=5, entry_price_cents=5,
	stop_loss_distance_cents=3, placed_at_utc="2026-05-18T00:00:00+00:00")


def test_record_rejected_cas_pending_to_rejected(tmp_path, caplog):
	"""C3 / spec §3 (insert-pending-then-CAS, NOT insert-on-outcome), §4.2, §5.

	On the LIVE store ``record_rejected`` is NOT a 2nd insert — it is a CAS
	``pending → rejected`` transition of the C1-inserted row located by
	``client_order_id``, via B's ``live.state.transition_pending_to_rejected``
	(no hand-rolled SQL). After ``record_intent`` (C1 inserts the pending row)
	then ``record_rejected`` there must be EXACTLY ONE row, now
	``status='rejected'`` — a transition on the existing row, never a
	competing second INSERT (§3 explicitly supersedes B's CR-4
	one-row-per-attempt insert model).

	FIX 1 regression guard (non-vacuous): the WON-CAS happy path
	(``pre_status == 'pending'`` ⇒ the CAS fires and succeeds) is NOT a lost
	race, so it MUST emit ZERO ``edge_catcher.live.store`` ERROR records.
	This pins the FIX-1 lost-race predicate: an inverted guard (firing on
	the dominant ``pending`` pre-status) would flood the real-money audit
	trail with a spurious "lost CAS race … benign late/duplicate reject"
	ERROR on every normal rejection AND bury the genuine ``open``-row
	ordering anomalies — this assertion fails loudly on that inversion.
	"""
	s = SQLiteTradeStore(tmp_path / "live_trades.db")
	s.record_intent(**INTENT)  # C1 inserts pending row, coid='cid-A'
	with caplog.at_level("ERROR", logger="edge_catcher.live.store"):
		s.record_rejected(client_order_id="cid-A",
			rejection_reason="kalshi_4xx", **REJECT_KW)

	# Exactly one row, transitioned in place — NOT a second INSERT (§3/§4.2).
	rows = s._conn.execute(
		"SELECT status, client_order_id FROM live_trades").fetchall()
	assert rows == [("rejected", "cid-A")]

	s._conn.row_factory = sqlite3.Row
	r = s._conn.execute(
		"SELECT * FROM live_trades WHERE client_order_id = 'cid-A'"
	).fetchone()
	assert r["status"] == "rejected"
	assert r["rejection_reason"] == "kalshi_4xx"

	# The won-CAS happy path is NOT a lost race ⇒ ZERO store-logger ERRORs.
	# (Regression guard for the inverted FIX-1 predicate; would FAIL if the
	# guard fired on the dominant ``pending`` pre-status.)
	store_errs = [rec for rec in caplog.records
		if rec.name == "edge_catcher.live.store" and rec.levelname == "ERROR"]
	assert store_errs == [], (
		"the won-CAS happy path (pending→rejected succeeds) must emit NO "
		"store-logger ERROR — it is not a lost race; got: "
		f"{[r.getMessage() for r in store_errs]!r}")
	s.close()


def test_record_rejected_write_failure_is_best_effort_no_raise(
	tmp_path, monkeypatch, caplog):
	"""C3 / spec §3.1 — caller-owned best-effort: a TRANSIENT DB/disk write
	failure in the CAS is NOT fatal (log ERROR, do NOT raise; a rejected
	order holds no position ⇒ not funds-at-risk). Mirrors the PR#34
	``438d843`` precedent.

	B's ``transition_pending_to_rejected`` does NOT own a write-failure
	carve-out (only a lost-CAS-race WARNING no-op) — a ``sqlite3.Error`` from
	``conn.execute`` inside ``_cas_update`` propagates. So the CALLER
	(``store.record_rejected``) owns the try/except. This test specifically
	exercises the ``except sqlite3.Error`` carve-out (the transient/disk
	branch of FIX 2) by raising ``sqlite3.OperationalError``; the
	UNEXPECTED-non-DB branch is covered by
	``test_record_rejected_unexpected_error_distinct_from_db_error``.

	Patched AS RESOLVED BY ``store.record_rejected`` — store.py does
	``from edge_catcher.live.state import transition_pending_to_rejected`` so
	the live delegation binds
	``edge_catcher.live.store.transition_pending_to_rejected``; patching
	``edge_catcher.live.state.*`` would NOT intercept (stale-binding pitfall).
	"""
	s = SQLiteTradeStore(tmp_path / "live_trades.db")
	s.record_intent(**dict(INTENT, client_order_id="cid-B"))
	import edge_catcher.live.store as store_mod
	# Raise a sqlite3.Error subclass so it lands in the `except sqlite3.Error`
	# (transient/disk) carve-out, NOT the broad UNEXPECTED clause.
	monkeypatch.setattr(store_mod, "transition_pending_to_rejected",
		lambda *a, **k: (_ for _ in ()).throw(
			sqlite3.OperationalError("disk I/O error")))

	with caplog.at_level("ERROR", logger="edge_catcher.live.store"):
		# MUST NOT raise — best-effort per §3.1.
		s.record_rejected(client_order_id="cid-B",
			rejection_reason="kalshi_4xx", **REJECT_KW)

	store_errs = [rec for rec in caplog.records
		if rec.name == "edge_catcher.live.store" and rec.levelname == "ERROR"]
	assert store_errs, (
		"a write-failure in record_rejected must be logged at ERROR level "
		"(audit gap), not silently swallowed")
	# Landed in the transient/DB carve-out, NOT the UNEXPECTED/API-drift one.
	msg = store_errs[-1].getMessage()
	assert "DB/disk fault" in msg and "transient" in msg, (
		"a sqlite3.Error must be logged via the transient DB/disk carve-out "
		f"message, not the UNEXPECTED branch — got: {msg!r}")
	assert "UNEXPECTED" not in msg, (
		"sqlite3.Error must NOT be categorized as the UNEXPECTED/possible "
		f"API-drift class — got: {msg!r}")

	# Row not half-corrupted: the C1 pending row survives unchanged (the
	# failed CAS neither transitioned nor wrote it); still exactly one row.
	rows = s._conn.execute(
		"SELECT status, client_order_id FROM live_trades").fetchall()
	assert rows == [("pending", "cid-B")]
	s.close()


def test_record_rejected_unexpected_error_distinct_from_db_error(
	tmp_path, monkeypatch, caplog):
	"""C3 / FIX 2 — the UNEXPECTED (non-``sqlite3.Error``) carve-out is
	DISTINCT from the transient DB/disk one.

	A non-DB exception out of ``transition_pending_to_rejected`` (e.g. a
	``TypeError`` from a wrong kwarg = B-API/signature drift) is a likely
	PERMANENT bug that would otherwise log-and-continue forever with zero
	rejected audit rows. It MUST still be best-effort (no raise — a rejected
	order holds no position) but logged with the DISTINCT
	"UNEXPECTED … possible B-API / signature drift" wording so an operator
	can escalate it faster than a transient disk fault. Patched AS RESOLVED
	BY ``store.record_rejected`` (stale-binding pitfall — see the write-
	failure test).
	"""
	s = SQLiteTradeStore(tmp_path / "live_trades.db")
	s.record_intent(**dict(INTENT, client_order_id="cid-D"))
	import edge_catcher.live.store as store_mod
	monkeypatch.setattr(store_mod, "transition_pending_to_rejected",
		lambda *a, **k: (_ for _ in ()).throw(TypeError("bad kwarg")))

	with caplog.at_level("ERROR", logger="edge_catcher.live.store"):
		# MUST NOT raise — still §3.1 best-effort even for the permanent
		# class (a rejected order holds no position).
		s.record_rejected(client_order_id="cid-D",
			rejection_reason="kalshi_4xx", **REJECT_KW)

	store_errs = [rec for rec in caplog.records
		if rec.name == "edge_catcher.live.store" and rec.levelname == "ERROR"]
	assert store_errs, (
		"an unexpected non-DB error in record_rejected must be logged at "
		"ERROR level, not silently swallowed")
	msg = store_errs[-1].getMessage()
	# The DISTINCT unexpected/API-drift wording, NOT the sqlite3/disk one.
	assert "UNEXPECTED" in msg and "signature drift" in msg, (
		"a non-sqlite3 error must be logged via the DISTINCT UNEXPECTED / "
		f"possible-API-drift message — got: {msg!r}")
	assert "DB/disk fault" not in msg, (
		"a non-sqlite3 error must NOT be categorized as the transient "
		f"DB/disk carve-out — got: {msg!r}")

	# Row not half-corrupted: the C1 pending row survives unchanged.
	rows = s._conn.execute(
		"SELECT status, client_order_id FROM live_trades").fetchall()
	assert rows == [("pending", "cid-D")]
	s.close()


def test_record_rejected_cas_miss_no_pending_row_is_logged_audit_gap_not_fatal(
	tmp_path, caplog):
	"""C3 / spec §3.1 — the pre-place C-gate-reject case
	(``absolute_max_exceeded`` / ``invalid_intended_size``): dispatch may
	call ``record_rejected`` with NO preceding ``record_intent`` (the C-gate
	rejected the order BEFORE C1 inserted a pending row). The spec author
	KNEW this CAS-misses and DELIBERATELY accepted it as a logged ERROR
	audit gap, NOT fatal (a rejected order holds no position ⇒ not
	funds-at-risk). It MUST NOT raise, MUST NOT silently INSERT a row
	(that would resurrect B's superseded insert-on-outcome model), and MUST
	emit an ERROR-level audit-gap log.
	"""
	s = SQLiteTradeStore(tmp_path / "live_trades.db")
	# NO record_intent — CAS-miss by construction (pre-place C-gate reject).

	with caplog.at_level("ERROR", logger="edge_catcher.live.store"):
		# MUST NOT raise — §3.1 accepted audit gap, not fatal.
		s.record_rejected(client_order_id="cid-NEVER-INTENDED",
			rejection_reason="absolute_max_exceeded", **REJECT_KW)

	assert any(rec.levelname == "ERROR" for rec in caplog.records), (
		"a CAS-miss (no pending row) in record_rejected must be logged at "
		"ERROR level (spec §3.1 accepted audit gap), not silent")

	# No row was written for that coid — CAS-miss is NOT a silent INSERT
	# (§3 explicitly supersedes B's insert-on-outcome model; a positionless
	# rejected with no C1 row is an accepted audit gap, never a fabricated
	# row).
	rows = s._conn.execute(
		"SELECT COUNT(*) FROM live_trades "
		"WHERE client_order_id = 'cid-NEVER-INTENDED'").fetchone()
	assert rows[0] == 0
	s.close()


def test_record_rejected_lost_race_open_row_is_logged_anomaly_no_raise(
	tmp_path, caplog):
	"""C3 / FIX 1 — the DANGEROUS lost-CAS-race sub-case: the coid row
	EXISTS but is ``open`` (the order FILLED), then ``record_rejected`` is
	called for the same coid. That means the system believes ONE order both
	filled AND was rejected — a fill/reject ORDERING ANOMALY (a silent-bad-
	state the zero-error lens targets). Before FIX 1 this was invisible on
	the store's audit logger (B's ``_cas_update`` only WARNs by ``row_id``
	on the ``edge_catcher.live.state`` logger — no coid, no reason). It MUST
	now be surfaced DISTINCTLY on the store's coid-keyed audit logger WITH
	the business keys and the ANOMALY wording, MUST NOT raise (§3.1 — a
	rejected order holds no position), and MUST NOT clobber the filled row
	(the won/open position must survive intact).

	The row is driven to ``open`` legitimately via C2's ``record_trade``
	(same args as ``test_record_trade_cas_pending_to_open``) so the ONLY
	thing under test is the subsequent lost-race ``record_rejected``.
	"""
	import datetime as _dt

	s = SQLiteTradeStore(tmp_path / "live_trades.db")
	s.record_intent(**INTENT)  # C1 pending, coid='cid-A'
	# Legitimately transition pending → open (the order filled).
	s.record_trade(
		ticker="KXSOL15M-X", entry_price=5, strategy="strat-34",
		side="yes", series_ticker="KXSOL15M", intended_size=5,
		fill_size=5, blended_entry=5, book_depth=None, fill_pct=1.0,
		slippage_cents=0, book_snapshot=None,
		now=_dt.datetime.fromisoformat("2026-05-18T00:00:01+00:00"),
		client_order_id="cid-A", kalshi_order_id="ord-1")

	with caplog.at_level("ERROR", logger="edge_catcher.live.store"):
		# MUST NOT raise — §3.1 best-effort even though it's an anomaly.
		s.record_rejected(client_order_id="cid-A",
			rejection_reason="kalshi_4xx", **REJECT_KW)

	store_errs = [rec for rec in caplog.records
		if rec.name == "edge_catcher.live.store" and rec.levelname == "ERROR"]
	assert store_errs, (
		"a lost-CAS-race against an open (filled) row must be logged at "
		"ERROR on the store's audit logger, not silent")
	msg = store_errs[-1].getMessage()
	# Distinct lost-race line WITH the business keys + the ANOMALY wording
	# (status is 'open' ⇒ fill/reject ordering anomaly, a real-money
	# concern), NOT the benign late/duplicate variant.
	assert "cid-A" in msg, (
		f"the lost-race log must carry the client_order_id — got: {msg!r}")
	assert "ANOMALY" in msg and "real-money" in msg, (
		"an open-row lost race must be flagged as a fill/reject ordering "
		f"ANOMALY (real-money concern) — got: {msg!r}")
	assert "benign" not in msg, (
		f"an open-row lost race is NOT the benign variant — got: {msg!r}")

	# The filled position was NOT clobbered by the late reject: exactly one
	# row, still status='open', still kalshi_order_id='ord-1' (the CAS
	# correctly no-op'd; the reject did not overwrite the won position).
	rows = s._conn.execute(
		"SELECT status, kalshi_order_id, client_order_id "
		"FROM live_trades").fetchall()
	assert rows == [("open", "ord-1", "cid-A")]
	s.close()


def test_record_rejected_lost_race_already_rejected_is_benign_no_anomaly(
	tmp_path, caplog):
	"""C3 / FIX 1 — the BENIGN lost-CAS-race sub-case: a duplicate / late
	``record_rejected`` for a coid whose row is ALREADY ``rejected`` (the
	first reject landed). The 2nd call's CAS no-ops; it MUST NOT raise,
	MUST still emit the DISTINCT lost-race line (observability), but with
	the BENIGN late/duplicate wording — explicitly NOT the ``open``
	fill/reject ordering-anomaly wording — and MUST NOT overwrite the
	original ``rejection_reason`` (still exactly one row, still
	``rejected``).
	"""
	s = SQLiteTradeStore(tmp_path / "live_trades.db")
	s.record_intent(**dict(INTENT, client_order_id="cid-X"))

	# First reject — CAS pending → rejected lands (the WON-CAS happy path).
	# It is NOT a lost race, so it MUST emit ZERO store-logger ERRORs. This
	# is asserted explicitly (not merely cleared) so the discrimination
	# below cannot be satisfied by a stray spurious happy-path ERROR (the
	# inverted-predicate failure mode this test now genuinely catches).
	with caplog.at_level("ERROR", logger="edge_catcher.live.store"):
		s.record_rejected(client_order_id="cid-X",
			rejection_reason="kalshi_4xx", **REJECT_KW)
	first_errs = [rec for rec in caplog.records
		if rec.name == "edge_catcher.live.store" and rec.levelname == "ERROR"]
	assert first_errs == [], (
		"the FIRST reject is the won-CAS happy path (pending→rejected "
		"succeeds), NOT a lost race — it must emit NO store-logger ERROR; "
		f"got: {[r.getMessage() for r in first_errs]!r}")
	caplog.clear()

	with caplog.at_level("ERROR", logger="edge_catcher.live.store"):
		# Second reject — row already 'rejected', CAS lost race. MUST NOT
		# raise; a DIFFERENT reason to prove it is NOT re-applied.
		s.record_rejected(client_order_id="cid-X",
			rejection_reason="kalshi_5xx", **REJECT_KW)

	store_errs = [rec for rec in caplog.records
		if rec.name == "edge_catcher.live.store" and rec.levelname == "ERROR"]
	# EXACTLY ONE store ERROR — the genuine lost race only. (With the
	# inverted predicate the happy path above also ERRORed and this would
	# not discriminate; the empty-first-errs assertion + clear + this
	# exactly-one check make the test non-vacuous.)
	assert len(store_errs) == 1, (
		"only the genuine lost race (2nd reject, row already 'rejected') "
		"must emit a store-logger ERROR — exactly one; got: "
		f"{[r.getMessage() for r in store_errs]!r}")
	msg = store_errs[0].getMessage()
	# DISTINCT lost-race line, but the BENIGN variant — NOT the open
	# fill/reject ordering-anomaly wording.
	assert "cid-X" in msg, (
		f"the lost-race log must carry the client_order_id — got: {msg!r}")
	assert "kalshi_5xx" in msg, (
		"the lost-race log must carry THIS (2nd) call's rejection_reason — "
		f"got: {msg!r}")
	assert "benign" in msg, (
		"an already-rejected lost race is the BENIGN late/duplicate variant "
		f"— got: {msg!r}")
	assert "ANOMALY" not in msg and "real-money" not in msg, (
		"an already-rejected (not open) lost race must NOT be flagged as the "
		f"fill/reject ordering anomaly — got: {msg!r}")

	# Exactly one row, still 'rejected', and the ORIGINAL reason preserved
	# (the lost-race 2nd call did NOT overwrite it — the CAS no-op'd).
	s._conn.row_factory = sqlite3.Row
	r = s._conn.execute(
		"SELECT * FROM live_trades WHERE client_order_id = 'cid-X'"
	).fetchall()
	assert len(r) == 1
	assert r[0]["status"] == "rejected"
	assert r[0]["rejection_reason"] == "kalshi_4xx"
	s.close()


# -----------------------------------------------------------------------------
# C4 — LIVE record_pending = idempotent BACKFILL of kalshi_order_id on the C1
# L1 row (NOT a 2nd insert-on-outcome), caller-owned best-effort (spec §3
# supersedes B's CR-4 insert-on-outcome; §3.1 post-place backfill failure /
# row-not-found is a logged ERROR audit gap, NOT fatal — the durable pending
# row already exists from record_intent, B's reconciler owns recovery via
# client_order_id; contrast C1's record_intent which IS fatal because there
# the row did NOT exist yet).
# -----------------------------------------------------------------------------


def test_record_pending_backfills_kalshi_order_id_on_L1_row_no_second_insert(
	tmp_path):
	"""C4 / spec §3 (insert-pending-then-CAS, NOT insert-on-outcome), §4.2, §5.

	The executor-pending branch (dispatch.py:478 — NetworkError / timeout /
	malformed-fills; the order MAY be live on Kalshi, fill state UNKNOWN) calls
	``store.record_pending(...)`` UNCONDITIONALLY. The durable ``pending`` row
	ALREADY EXISTS from C1's ``record_intent`` (inserted pre-place, keyed by
	``client_order_id``, ``kalshi_order_id`` NULL). On the LIVE store
	``record_pending`` is therefore NOT a 2nd insert — it is an idempotent
	BACKFILL of ``kalshi_order_id`` onto that C1 row (a NetworkError/timeout
	OrderResult may still carry a Kalshi order id) while ``status`` stays
	``'pending'`` (still unknown; B's reconciler resolves it later via
	``client_order_id``). After ``record_intent`` then ``record_pending`` there
	must be EXACTLY ONE row for the coid, still ``status='pending'``, with
	``kalshi_order_id`` now backfilled — never a competing second INSERT (§3
	explicitly supersedes B's CR-4 one-row-per-attempt insert model).
	"""
	s = SQLiteTradeStore(tmp_path / "live_trades.db")
	s.record_intent(**dict(INTENT, client_order_id="cid-A"))  # C1 pending row

	# Pre-state: exactly one pending row, kalshi_order_id still NULL.
	pre = s._conn.execute(
		"SELECT status, kalshi_order_id FROM live_trades "
		"WHERE client_order_id = 'cid-A'").fetchall()
	assert pre == [("pending", None)]

	s.record_pending(ticker="KXSOL15M-X", series="KXSOL15M",
		strategy="strat-34", side="yes", intended_size=5,
		entry_price_cents=5, stop_loss_distance_cents=3,
		client_order_id="cid-A", kalshi_order_id="ord-9",
		placed_at_utc="2026-05-18T00:00:00+00:00",
		rejection_reason="kalshi_unreachable:timeout")

	# Exactly ONE row for the coid (no 2nd INSERT — §3/§4.2), still pending
	# (state still unknown), kalshi_order_id BACKFILLED in place.
	rows = s._conn.execute(
		"SELECT status, kalshi_order_id, client_order_id FROM live_trades"
	).fetchall()
	assert rows == [("pending", "ord-9", "cid-A")]
	s.close()


def test_record_pending_backfill_failure_is_not_fatal(
	tmp_path, monkeypatch, caplog):
	"""C4 / spec §3.1 — caller-owned best-effort: a post-place backfill
	failure is NOT fatal (log ERROR, do NOT raise, NOT ``RecordPendingFailed``).

	Contrast C1's ``record_intent``: that failure IS fatal
	(``RecordPendingFailed`` propagates, entry aborts BEFORE ``place()`` — the
	row did NOT exist yet, a funds-at-risk INSERT). Here the durable ``pending``
	row ALREADY exists from ``record_intent``; B's reconciler owns recovery via
	``client_order_id``, so a failed ``kalshi_order_id`` backfill strands at
	most an audit-grade detail — raising would needlessly halt the engine while
	the funds-at-risk invariant is already satisfied.

	Patched AS RESOLVED BY ``store.record_pending`` (the backfill writer the
	live impl actually calls, in the ``edge_catcher.live.store`` namespace) so
	the patch genuinely intercepts the call (C1's stale-binding lesson — a
	``edge_catcher.live.state.*`` patch would NOT intercept). The C1 ``pending``
	row MUST survive uncorrupted (still exactly one pending row for the coid).
	"""
	s = SQLiteTradeStore(tmp_path / "live_trades.db")
	s.record_intent(**dict(INTENT, client_order_id="cid-B"))
	import edge_catcher.live.store as store_mod
	monkeypatch.setattr(store_mod, "_backfill_pending_kalshi_order_id",
		lambda *a, **k: (_ for _ in ()).throw(
			sqlite3.OperationalError("disk I/O error")))

	with caplog.at_level("ERROR", logger="edge_catcher.live.store"):
		# MUST NOT raise — best-effort per §3.1 (NOT RecordPendingFailed).
		s.record_pending(ticker="KXSOL15M-X", series="KXSOL15M",
			strategy="strat-34", side="yes", intended_size=5,
			entry_price_cents=5, stop_loss_distance_cents=3,
			client_order_id="cid-B", kalshi_order_id="ord-9",
			placed_at_utc="2026-05-18T00:00:00+00:00",
			rejection_reason="kalshi_unreachable:timeout")

	store_errs = [rec for rec in caplog.records
		if rec.name == "edge_catcher.live.store" and rec.levelname == "ERROR"]
	assert store_errs, (
		"a post-place backfill failure in record_pending must be logged at "
		"ERROR level (audit gap), not silently swallowed")

	# Row not half-corrupted: the C1 pending row survives unchanged (the
	# failed backfill neither transitioned nor wrote it); still exactly one.
	rows = s._conn.execute(
		"SELECT status, client_order_id FROM live_trades").fetchall()
	assert rows == [("pending", "cid-B")]
	s.close()


def test_record_pending_row_not_found_is_logged_audit_gap_not_fatal(
	tmp_path, caplog):
	"""C4 / spec §3.1 — defense-in-depth: ``record_pending`` called with NO
	preceding ``record_intent`` (the C1 row somehow absent). It MUST NOT raise,
	MUST NOT fabricate a competing INSERT (that would resurrect B's superseded
	insert-on-outcome model; spec §3), and MUST emit an ERROR-level audit-gap
	log. B's reconciler is the backstop via ``client_order_id`` (mirrors C3's
	CAS-miss posture for the positionless-rejected case).
	"""
	s = SQLiteTradeStore(tmp_path / "live_trades.db")
	# NO record_intent — row-not-found by construction.

	with caplog.at_level("ERROR", logger="edge_catcher.live.store"):
		# MUST NOT raise — §3.1 accepted audit gap, not fatal.
		s.record_pending(ticker="KXSOL15M-X", series="KXSOL15M",
			strategy="strat-34", side="yes", intended_size=5,
			entry_price_cents=5, stop_loss_distance_cents=3,
			client_order_id="cid-NEVER", kalshi_order_id="ord-9",
			placed_at_utc="2026-05-18T00:00:00+00:00",
			rejection_reason="kalshi_unreachable:timeout")

	assert any(rec.name == "edge_catcher.live.store"
			and rec.levelname == "ERROR" for rec in caplog.records), (
		"a row-not-found (no C1 record_intent) in record_pending must be "
		"logged at ERROR level (spec §3.1 accepted audit gap), not silent")

	# No row written for that coid — row-not-found is NOT a silent INSERT
	# fallback (§3 explicitly supersedes B's insert-on-outcome model).
	rows = s._conn.execute(
		"SELECT COUNT(*) FROM live_trades "
		"WHERE client_order_id = 'cid-NEVER'").fetchone()
	assert rows[0] == 0
	s.close()


def test_record_pending_is_idempotent_double_call(tmp_path):
	"""C4 / spec §3 — idempotent. The executor-pending branch may fire more
	than once for the same coid (a reconnect re-delivering the same
	NetworkError outcome, or dispatch retrying). ``record_pending`` called
	twice with the same ``kalshi_order_id`` MUST NOT raise, MUST leave exactly
	ONE row, still ``status='pending'``, ``kalshi_order_id`` unchanged — a
	re-run is a no-op-equivalent (no corruption, no 2nd row).
	"""
	s = SQLiteTradeStore(tmp_path / "live_trades.db")
	s.record_intent(**dict(INTENT, client_order_id="cid-D"))

	kw = dict(ticker="KXSOL15M-X", series="KXSOL15M", strategy="strat-34",
		side="yes", intended_size=5, entry_price_cents=5,
		stop_loss_distance_cents=3, client_order_id="cid-D",
		kalshi_order_id="ord-1",
		placed_at_utc="2026-05-18T00:00:00+00:00",
		rejection_reason="kalshi_unreachable:timeout")
	s.record_pending(**kw)
	s.record_pending(**kw)  # second call — idempotent, no raise, no 2nd row

	rows = s._conn.execute(
		"SELECT status, kalshi_order_id, client_order_id FROM live_trades"
	).fetchall()
	assert rows == [("pending", "ord-1", "cid-D")]
	s.close()


def test_record_pending_unexpected_non_db_error_is_best_effort_distinct_message(
	tmp_path, monkeypatch, caplog):
	"""C4 / spec §3.1 — the UNEXPECTED (non-``sqlite3.Error``) carve-out is
	DISTINCT from the transient DB/disk one, mirroring C3's
	``record_rejected`` split.

	A non-DB exception out of ``_backfill_pending_kalshi_order_id`` (e.g. a
	``TypeError`` from a wrong kwarg = a future B-API/signature drift, or a
	programming bug) is a likely PERMANENT fault that — WITHOUT the
	``except Exception`` carve-out — would NOT be ``RecordPendingFailed`` (so
	it bypasses ghost-reject) and would ESCAPE ``record_pending`` entirely,
	losing the coid-keyed audit signal + the drift classification and
	violating §3.1's "all best-effort — log ERROR, never raise" promise
	(inconsistent with C3). It MUST still be best-effort (no raise — the
	durable C1 pending row already exists & B's reconciler owns recovery via
	``client_order_id``; specifically NOT ``RecordPendingFailed``) but logged
	with the DISTINCT "UNEXPECTED … possible B-API / signature drift …
	escalate" wording so an operator can escalate it faster than a transient
	disk fault.

	Patched AS RESOLVED BY ``store.record_pending`` (the backfill writer the
	live impl actually calls, in the ``edge_catcher.live.store`` namespace) so
	the patch genuinely intercepts the call (C1's stale-binding lesson — an
	``edge_catcher.live.state.*`` patch would NOT intercept). The C1
	``pending`` row MUST survive uncorrupted (still exactly one pending row
	for the coid, ``kalshi_order_id`` still NULL — the failed backfill did not
	corrupt it).
	"""
	s = SQLiteTradeStore(tmp_path / "live_trades.db")
	s.record_intent(**dict(INTENT, client_order_id="cid-A"))
	import edge_catcher.live.store as store_mod
	# Raise a NON-sqlite3.Error so it lands in the broad UNEXPECTED clause,
	# NOT the `except sqlite3.Error` (transient/disk) carve-out.
	monkeypatch.setattr(store_mod, "_backfill_pending_kalshi_order_id",
		lambda *a, **k: (_ for _ in ()).throw(
			TypeError("bad kwarg — simulated B-API drift")))

	with caplog.at_level("ERROR", logger="edge_catcher.live.store"):
		# MUST NOT raise — still §3.1 best-effort even for the permanent
		# class; specifically NOT RecordPendingFailed (ghost-reject scope is
		# funds-at-risk pre-place INSERTs only; the durable C1 row exists).
		s.record_pending(ticker="KXSOL15M-X", series="KXSOL15M",
			strategy="strat-34", side="yes", intended_size=5,
			entry_price_cents=5, stop_loss_distance_cents=3,
			client_order_id="cid-A", kalshi_order_id="ord-9",
			placed_at_utc="2026-05-18T00:00:00+00:00",
			rejection_reason="kalshi_unreachable:timeout")

	store_errs = [rec for rec in caplog.records
		if rec.name == "edge_catcher.live.store" and rec.levelname == "ERROR"]
	assert store_errs, (
		"an unexpected non-DB error in record_pending must be logged at "
		"ERROR level (audit gap), not silently swallowed or escaped")
	msg = store_errs[-1].getMessage()
	# The DISTINCT unexpected/API-drift/escalate wording, NOT the
	# sqlite3/disk one — and it MUST carry the client_order_id.
	assert "cid-A" in msg, (
		f"the unexpected-error log must carry the client_order_id — "
		f"got: {msg!r}")
	assert "UNEXPECTED" in msg and "signature drift" in msg \
			and "escalate" in msg, (
		"a non-sqlite3 error must be logged via the DISTINCT UNEXPECTED / "
		f"possible-API-drift / escalate message — got: {msg!r}")
	assert "DB/disk fault" not in msg, (
		"a non-sqlite3 error must NOT be categorized as the transient "
		f"DB/disk carve-out — got: {msg!r}")

	# Row not half-corrupted: the C1 pending row survives unchanged (the
	# failed backfill neither transitioned nor wrote it); still exactly one
	# row, still pending, kalshi_order_id still NULL.
	s._conn.row_factory = sqlite3.Row
	r = s._conn.execute(
		"SELECT * FROM live_trades WHERE client_order_id = 'cid-A'"
	).fetchall()
	assert len(r) == 1
	assert r[0]["status"] == "pending"
	assert r[0]["kalshi_order_id"] is None
	s.close()


# -----------------------------------------------------------------------------
# C5 — LIVE exit_trade / settle_trade / get_trade_by_id route to B's CAS close,
# NOT the paper single-UPDATE / fail-loud (spec §3 table `:534/:537 exit` +
# `engine.py:895 settlement` + §5 conform-to-B).
#
# In live production the AUTHORITATIVE close is recorded by B's async WS
# handler / reconciler (`record_close` / `record_partial_exit`) directly
# against live_trades.db — D3 (later) rewires dispatch to NOT call
# `store.exit_trade` synchronously in live mode. But `TradeStoreProtocol`
# still EXPOSES exit_trade/settle_trade/get_trade_by_id (dispatch's paper
# path + tests call them), so for LIVE these store methods must route to B's
# CAS close (`live.state.record_close` — UPDATE-in-place won/lost/scratch
# with entry-fee-remainder consumption), NOT the paper single-UPDATE and NOT
# fail-loud. settlement = `record_close` with exit_reason='settlement',
# exit_fee=0, binary 0/100 (B ships NO separate settlement fn — settlement is
# `record_close` per ws_handlers.on_settlement_event + integration test #26).
#
# FATALITY (the genuinely-new funds-at-risk question): a close acts on a
# real-money OPEN position — but the position's correct eventual close is
# GUARANTEED by B's async reconciler/WS handler (the authoritative close
# path), NOT by this synchronous store method. B's own `record_close`
# contract makes a lost CAS race a logged WARNING no-op (NEVER raises;
# settlement-vs-exit-fill is B's EXPECTED idempotent outcome). Raising here
# would HALT the engine — strictly WORSE for a funds-at-risk open position
# than logging ERROR and letting B's reconciler close it (a halted engine
# stops B's reconciler/WS loop too, removing the very recovery mechanism).
# So: caller-owned best-effort, the SAME uniform taxonomy as C3/C4 (distinct
# ERROR, business keys, sqlite3.Error-vs-unexpected split, lost-CAS
# observability), NEVER RecordPendingFailed (ghost-reject scope is
# funds-at-risk PRE-PLACE INSERTs only — spec §3.1; a close is not one).
# Contrast: C1 record_intent FATAL (pre-place INSERT, row does not exist yet
# → strands a funds-at-risk order); C2 record_trade CAS pending→open
# (loud-fail on a wiring bug — missing identity key); C3 record_rejected /
# C4 record_pending best-effort (positionless / durable-row-already-exists).
# C5 is the post-fill terminal close: best-effort because B's authoritative
# async close path owns recovery — identical posture to C3/C4's "B's
# reconciler owns it", applied to a close.
# -----------------------------------------------------------------------------


def _seed_open_live_row(s, *, coid="cid-A", kalshi_id="ord-1"):
	"""Drive a realistic OPEN live row through the real C1→C2 path.

	C1 ``record_intent`` INSERTs the pending row (INTENT: entry_price_cents=5,
	intended_size=5, side=yes); then C2 ``record_trade`` CAS-transitions it to
	``open`` with the authoritative fill recorded. Returns the row id.

	With ``record_trade(fill_size=10, blended_entry=42, intended_size=10)``:
	C2 computes ``entry_fee_cents = int(round(STANDARD_FEE.calculate(42,10)))``
	= ``int(round(ceil(0.07*10*0.42*0.58*100)))`` = ``int(round(18.0))`` = 18,
	and B's ``transition_pending_to_open`` seeds
	``entry_fee_remaining_cents = entry_fee_cents`` = 18 — the remainder B's
	``record_close`` later consumes.
	"""
	import datetime as _dt

	s.record_intent(**dict(INTENT, client_order_id=coid))
	s.record_trade(
		ticker="KXSOL15M-X", entry_price=42, strategy="strat-34",
		side="yes", series_ticker="KXSOL15M", intended_size=10,
		fill_size=10, blended_entry=42, book_depth=None, fill_pct=1.0,
		slippage_cents=0, book_snapshot=None,
		now=_dt.datetime.fromisoformat("2026-05-18T00:00:01+00:00"),
		client_order_id=coid, kalshi_order_id=kalshi_id)
	rid = s._conn.execute(
		"SELECT id FROM live_trades WHERE client_order_id = ?", (coid,)
	).fetchone()[0]
	return int(rid)


def test_get_trade_by_id_returns_live_row_dict(tmp_path):
	"""C5 / spec §3 (`get_trade_by_id` → canonical by-id read of the live row
	as a dict), §5.

	On the LIVE store ``get_trade_by_id`` must return the live_trades row as a
	paper-shaped dict (so dispatch's exit bookkeeping + tests stay
	store-agnostic — same key set paper ``TradeStore.get_trade_by_id``
	returns: id / ticker / entry_price / strategy / side / series_ticker /
	entry_fee_cents / intended_size / fill_size / blended_entry / book_depth /
	fill_pct / slippage_cents / status / entry_time / exit_price / exit_time /
	pnl_cents). live_trades' cent-suffixed columns are aliased to the paper
	names (entry_price ← entry_price_cents, blended_entry ←
	blended_entry_cents, exit_price ← exit_price_cents) so strategy/dispatch
	code stays venue/store agnostic; book_depth is absent in the live IOC
	schema → reported as None to keep the shape stable. An absent id returns
	None (paper-parity contract).
	"""
	s = SQLiteTradeStore(tmp_path / "live_trades.db")
	rid = _seed_open_live_row(s)

	row = s.get_trade_by_id(rid)
	assert row is not None
	assert row["id"] == rid
	assert row["ticker"] == "KXSOL15M-X"
	# entry_price ← entry_price_cents = the ORIGINAL Signal intent from C1's
	# record_intent (INTENT.entry_price_cents=5). C2's
	# transition_pending_to_open sets blended_entry_cents (the real fill
	# price) but does NOT overwrite entry_price_cents (it stays the Signal
	# intent — DDL 0003 comment "Signal's entry_price intent"), so these two
	# legitimately differ on a live row.
	assert row["entry_price"] == 5  # aliased from entry_price_cents (intent)
	assert row["strategy"] == "strat-34"
	assert row["side"] == "yes"
	assert row["series_ticker"] == "KXSOL15M"  # aliased from series
	assert row["fill_size"] == 10
	assert row["blended_entry"] == 42  # aliased from blended_entry_cents
	assert row["book_depth"] is None  # no book-walk concept for live IOC
	assert row["status"] == "open"
	assert row["entry_fee_cents"] == 18
	# An open row has no exit yet — the closed-trade keys exist (stable shape)
	# but are None, exactly like paper get_trade_by_id's 18-key dict.
	assert row["exit_price"] is None
	assert row["exit_time"] is None
	assert row["pnl_cents"] is None
	# Paper-parity: an absent id is None, NOT a raise / NOT fail-loud.
	assert s.get_trade_by_id(999_999) is None
	s.close()


def test_exit_trade_routes_to_B_record_close_full_close(tmp_path):
	"""C5 / spec §3 table `:534/:537 exit` + §5.

	On the LIVE store ``exit_trade(trade_id, exit_price, *, now)`` is NOT the
	paper single ``status='open' → won/lost`` UPDATE — it routes to B's CAS
	close ``live.state.record_close`` (UPDATE-in-place, CAS precondition
	``status IN ('open','exit_pending')``, ENTRY-FEE-REMAINDER consumed). The
	won/lost/scratch + pnl + exit_fee arithmetic mirrors B's
	``ws_handlers.on_fill_event`` full-close path byte-for-byte (NOT
	hand-rolled, NOT the paper formula) so F's P&L does not diverge by which
	path booked the close:

	* ``exit_fee = int(round(STANDARD_FEE.calculate(exit_price, fill_size)))``
	  (B's ``_entry_fee_cents`` convention — used for exit fees too).
	* ``pnl = fill_size*(exit_price - blended_entry) - entry_fee_remaining
	  - exit_fee`` (B's DDL contract; record_close does NOT recompute pnl —
	  the caller owns the arithmetic, same as on_fill_event).
	* outcome = won if exit>entry, lost if exit<entry, scratch if equal
	  (pre-fee; fees push a scratch to pnl<=0 — B's record_partial_exit rule).

	Seed open row: fill_size=10, blended_entry=42, entry_fee_cents=18
	(entry_fee_remaining_cents seeded =18 by C2). exit_trade(rid, 60):
	exit_fee = int(round(STANDARD_FEE.calculate(60,10))) =
	int(round(ceil(0.07*10*0.6*0.4*100))) = int(round(17.0)) = 17;
	pnl = 10*(60-42) - 18 - 17 = 145; 60 > 42 → won. record_close then
	consumes the remainder: entry_fee_cents = COALESCE(remaining=18,..) = 18,
	entry_fee_remaining_cents = 0. EXACTLY ONE row (full close UPDATEs in
	place — NO split child; record_partial_exit is the WS-handler split path,
	unreachable via the Protocol's full-close exit_trade).
	"""
	import datetime as _dt

	s = SQLiteTradeStore(tmp_path / "live_trades.db")
	rid = _seed_open_live_row(s)

	s.exit_trade(
		rid, 60,
		now=_dt.datetime.fromisoformat("2026-05-18T00:05:00+00:00"))

	# Exactly one row — a full close UPDATEs in place (NO split child; spec
	# §3 — the Protocol's exit_trade is a FULL close, record_partial_exit is
	# the WS-handler split path, not reachable via this method).
	assert s._conn.execute(
		"SELECT COUNT(*) FROM live_trades").fetchone()[0] == 1

	s._conn.row_factory = sqlite3.Row
	r = s._conn.execute(
		"SELECT * FROM live_trades WHERE id = ?", (rid,)).fetchone()
	assert r["status"] == "won"
	assert r["exit_price_cents"] == 60
	assert r["exit_time"] == "2026-05-18T00:05:00+00:00"
	assert r["exit_fee_cents"] == 17
	assert r["pnl_cents"] == 145
	assert r["exit_reason"] == "ws_exit_fill"
	# Entry-fee-remainder CONSUMED by B's record_close (the load-bearing
	# B-CAS-close behaviour the paper single-UPDATE does NOT have):
	# entry_fee_cents = COALESCE(entry_fee_remaining_cents, entry_fee_cents),
	# then entry_fee_remaining_cents zeroed.
	assert r["entry_fee_cents"] == 18
	assert r["entry_fee_remaining_cents"] == 0
	s.close()


def test_exit_trade_loss_and_scratch_outcomes(tmp_path):
	"""C5 — won/lost/scratch determination mirrors B (pre-fee vs blended
	entry; fees then push a scratch to pnl<=0). Two more rows: a clear LOSS
	(exit < entry) and a SCRATCH (exit == entry, pnl <= 0 after fees).
	"""
	import datetime as _dt

	s = SQLiteTradeStore(tmp_path / "live_trades.db")
	_now = _dt.datetime.fromisoformat("2026-05-18T00:06:00+00:00")

	# LOSS: exit 20 < entry 42. exit_fee = int(round(ceil(
	# 0.07*10*0.2*0.8*100))) = int(round(12.0)) = 12;
	# pnl = 10*(20-42) - 18 - 12 = -250; 20 < 42 → lost.
	rid_l = _seed_open_live_row(s, coid="cid-L", kalshi_id="ord-L")
	s.exit_trade(rid_l, 20, now=_now)
	s._conn.row_factory = sqlite3.Row
	rl = s._conn.execute(
		"SELECT * FROM live_trades WHERE id = ?", (rid_l,)).fetchone()
	assert rl["status"] == "lost"
	assert rl["pnl_cents"] == -250
	assert rl["entry_fee_remaining_cents"] == 0

	# SCRATCH: exit 42 == entry 42 (pre-fee equal → scratch). exit_fee =
	# int(round(ceil(0.07*10*0.42*0.58*100))) = 18;
	# pnl = 10*(42-42) - 18 - 18 = -36 (fees push it negative; status still
	# 'scratch' — outcome is the pre-fee compare, exactly as B does it).
	rid_s = _seed_open_live_row(s, coid="cid-S", kalshi_id="ord-S")
	s.exit_trade(rid_s, 42, now=_now)
	rs = s._conn.execute(
		"SELECT * FROM live_trades WHERE id = ?", (rid_s,)).fetchone()
	assert rs["status"] == "scratch"
	assert rs["pnl_cents"] == -36
	s.close()


def test_settle_trade_routes_to_B_record_close_settlement(tmp_path):
	"""C5 / spec §3 table `engine.py:895 settlement` + §5.

	On the LIVE store ``settle_trade(trade_id, result, *, now)`` routes to B's
	settlement close — ``live.state.record_close`` with
	``exit_reason='settlement'``, binary ``exit_price_cents`` 100/0,
	``exit_fee_cents=0`` (Kalshi charges no fee at settlement, spec §423),
	entry-fee-remainder consumed. It SUPERSEDES ``exit_pending`` because
	record_close's CAS precondition is ``status IN ('open','exit_pending')``.
	won/lost by side-vs-result (binary — never scratch) mirrors B's
	``ws_handlers._settlement_outcome`` / ``_settlement_pnl_cents``:

	* payout = settlement_price for a yes-side row, (100 - settlement_price)
	  for a no-side row.
	* pnl = fill_size*(payout - blended_entry) - entry_fee_remaining.

	Seed a yes-side open row (fill_size=10, blended_entry=42,
	entry_fee_remaining=18). settle_trade(rid, "yes") → market resolved YES →
	settlement_price 100, yes-side wins: payout=100,
	pnl = 10*(100-42) - 18 = 562; exit_price_cents=100, exit_fee_cents=0,
	exit_reason='settlement'; record_close consumes the remainder
	(entry_fee_cents=18, entry_fee_remaining_cents=0).
	"""
	import datetime as _dt

	s = SQLiteTradeStore(tmp_path / "live_trades.db")
	rid = _seed_open_live_row(s)

	s.settle_trade(
		rid, "yes",
		now=_dt.datetime.fromisoformat("2026-05-18T00:10:00+00:00"))

	s._conn.row_factory = sqlite3.Row
	r = s._conn.execute(
		"SELECT * FROM live_trades WHERE id = ?", (rid,)).fetchone()
	assert r["status"] == "won"
	assert r["exit_price_cents"] == 100
	assert r["exit_fee_cents"] == 0
	assert r["exit_reason"] == "settlement"
	assert r["exit_time"] == "2026-05-18T00:10:00+00:00"
	assert r["pnl_cents"] == 562
	# Entry-fee-remainder consumed by B's record_close.
	assert r["entry_fee_cents"] == 18
	assert r["entry_fee_remaining_cents"] == 0
	# Exactly one row — settlement is a full UPDATE-in-place close.
	assert s._conn.execute(
		"SELECT COUNT(*) FROM live_trades").fetchone()[0] == 1
	s.close()


def test_settle_trade_loss_and_no_side(tmp_path):
	"""C5 — settlement won/lost is by side-vs-result, never scratch (binary).

	A yes-side row whose market settles NO loses; a NO-side row whose market
	settles NO wins (payout = 100 - settlement_price). Mirrors B's
	``_settlement_outcome`` / ``_settlement_pnl_cents`` exactly.
	"""
	import datetime as _dt

	s = SQLiteTradeStore(tmp_path / "live_trades.db")
	_now = _dt.datetime.fromisoformat("2026-05-18T00:11:00+00:00")

	# yes-side row, market settles NO (result="no") → settlement_price 0,
	# yes-side loses: payout=0, pnl = 10*(0-42) - 18 = -438.
	rid_l = _seed_open_live_row(s, coid="cid-SN", kalshi_id="ord-SN")
	s.settle_trade(rid_l, "no", now=_now)
	s._conn.row_factory = sqlite3.Row
	rl = s._conn.execute(
		"SELECT * FROM live_trades WHERE id = ?", (rid_l,)).fetchone()
	assert rl["status"] == "lost"
	assert rl["exit_price_cents"] == 0
	assert rl["pnl_cents"] == -438
	assert rl["exit_fee_cents"] == 0
	s.close()


def test_exit_trade_failure_is_best_effort_no_raise_no_RecordPendingFailed(
	tmp_path, monkeypatch, caplog):
	"""C5 / spec §3.1 + §5 — caller-owned best-effort: a TRANSIENT DB/disk
	failure in B's CAS close is NOT fatal (log ERROR, do NOT raise; SPECIFICALLY
	NOT ``RecordPendingFailed`` — ghost-reject scope is funds-at-risk PRE-PLACE
	INSERTs only, spec §3.1; a terminal close is not one).

	FATALITY justification (the genuinely-new funds-at-risk question): a close
	acts on a real-money OPEN position, BUT the position's correct eventual
	close is GUARANTEED by B's authoritative async reconciler / WS handler
	(spec §3 table `:534/:537` — the live close is recorded by B's async
	WS/reconciler against live_trades.db, NOT this synchronous store method;
	D3 later rewires dispatch so store.exit_trade is not the live close path).
	B's own ``record_close`` makes a lost CAS race a logged WARNING no-op and
	NEVER raises (settlement-vs-exit-fill is B's EXPECTED idempotent outcome).
	Raising here would HALT the engine — strictly WORSE for a funds-at-risk
	open position than logging ERROR and letting B's reconciler close it (a
	halted engine stops B's reconciler/WS loop too, removing the very recovery
	mechanism). Identical posture + uniform taxonomy to C3/C4's "B's
	reconciler owns recovery", applied to a close.

	Patched AS RESOLVED BY ``store.exit_trade`` — store.py does
	``from edge_catcher.live.state import record_close`` so the live
	delegation binds ``edge_catcher.live.store.record_close``; patching
	``edge_catcher.live.state.*`` would NOT intercept (C1's stale-binding
	lesson). This exercises the ``except sqlite3.Error`` (transient/disk)
	carve-out; the UNEXPECTED-non-DB branch is covered separately below.
	"""
	s = SQLiteTradeStore(tmp_path / "live_trades.db")
	rid = _seed_open_live_row(s, coid="cid-EF", kalshi_id="ord-EF")
	import datetime as _dt
	import edge_catcher.live.store as store_mod
	# sqlite3.Error subclass → lands in the transient/disk carve-out.
	monkeypatch.setattr(store_mod, "record_close",
		lambda *a, **k: (_ for _ in ()).throw(
			sqlite3.OperationalError("disk I/O error")))

	with caplog.at_level("ERROR", logger="edge_catcher.live.store"):
		# MUST NOT raise — best-effort per §3.1; NOT RecordPendingFailed.
		s.exit_trade(
			rid, 60,
			now=_dt.datetime.fromisoformat("2026-05-18T00:12:00+00:00"))

	store_errs = [rec for rec in caplog.records
		if rec.name == "edge_catcher.live.store" and rec.levelname == "ERROR"]
	assert store_errs, (
		"a write-failure in exit_trade must be logged at ERROR level (audit "
		"gap), not silently swallowed")
	msg = store_errs[-1].getMessage()
	assert "DB/disk fault" in msg and "transient" in msg, (
		"a sqlite3.Error must be logged via the transient DB/disk carve-out "
		f"message, not the UNEXPECTED branch — got: {msg!r}")
	assert "UNEXPECTED" not in msg, (
		"sqlite3.Error must NOT be categorized as the UNEXPECTED/possible "
		f"API-drift class — got: {msg!r}")
	# The open position survived uncorrupted (the failed close neither
	# transitioned nor wrote it) — B's authoritative async reconciler/WS
	# handler still owns the eventual close. Exactly one row, still 'open'.
	rows = s._conn.execute(
		"SELECT status, client_order_id FROM live_trades").fetchall()
	assert rows == [("open", "cid-EF")]
	s.close()


def test_settle_trade_unexpected_error_distinct_from_db_error(
	tmp_path, monkeypatch, caplog):
	"""C5 / §5 — the UNEXPECTED (non-``sqlite3.Error``) carve-out is DISTINCT
	from the transient DB/disk one, mirroring C3/C4's split.

	A non-DB exception out of ``record_close`` (e.g. a ``TypeError`` from a
	wrong kwarg = B-API/signature drift) is a likely PERMANENT bug that would
	otherwise log-and-continue forever with zero settled rows. It MUST still
	be best-effort (no raise — B's authoritative async settlement path owns
	recovery; SPECIFICALLY NOT ``RecordPendingFailed``) but logged with the
	DISTINCT "UNEXPECTED … possible B-API / signature drift … escalate"
	wording so an operator can escalate it faster than a transient disk fault.
	Patched AS RESOLVED BY ``store.settle_trade`` (stale-binding lesson).
	"""
	s = SQLiteTradeStore(tmp_path / "live_trades.db")
	rid = _seed_open_live_row(s, coid="cid-SF", kalshi_id="ord-SF")
	import datetime as _dt
	import edge_catcher.live.store as store_mod
	monkeypatch.setattr(store_mod, "record_close",
		lambda *a, **k: (_ for _ in ()).throw(TypeError("bad kwarg")))

	with caplog.at_level("ERROR", logger="edge_catcher.live.store"):
		# MUST NOT raise — still §3.1 best-effort even for the permanent
		# class (B's authoritative async path owns recovery).
		s.settle_trade(
			rid, "yes",
			now=_dt.datetime.fromisoformat("2026-05-18T00:13:00+00:00"))

	store_errs = [rec for rec in caplog.records
		if rec.name == "edge_catcher.live.store" and rec.levelname == "ERROR"]
	assert store_errs, (
		"an unexpected non-DB error in settle_trade must be logged at ERROR "
		"level, not silently swallowed")
	msg = store_errs[-1].getMessage()
	assert "UNEXPECTED" in msg and "signature drift" in msg \
			and "escalate" in msg, (
		"a non-sqlite3 error must be logged via the DISTINCT UNEXPECTED / "
		f"possible-API-drift / escalate message — got: {msg!r}")
	assert "DB/disk fault" not in msg, (
		"a non-sqlite3 error must NOT be categorized as the transient "
		f"DB/disk carve-out — got: {msg!r}")
	# The open position survived uncorrupted.
	rows = s._conn.execute(
		"SELECT status, client_order_id FROM live_trades").fetchall()
	assert rows == [("open", "cid-SF")]
	s.close()


def test_exit_trade_failure_unexpected_error_distinct_from_db_error(
	tmp_path, monkeypatch, caplog):
	"""C5 / §5 review-symmetry — close the asymmetric carve-out coverage gap:
	C5 shipped only the ``exit_trade``-TRANSIENT + ``settle_trade``-UNEXPECTED
	tests. This is the missing ``exit_trade``-UNEXPECTED half (its sibling
	``test_settle_trade_failure_db_error_distinct_from_unexpected`` is the
	missing ``settle_trade``-TRANSIENT half), so BOTH close methods now have
	the full transient/unexpected pair — exact parity with C3/C4's symmetric
	``record_rejected`` / ``record_pending`` carve-out pairs.

	A non-``sqlite3.Error`` out of ``record_close`` (e.g. a ``TypeError`` from
	a wrong kwarg = B-API/signature drift) is a likely PERMANENT bug that
	would otherwise log-and-continue forever with zero closed rows. It MUST
	still be best-effort (no raise — B's authoritative async WS/reconciler
	owns recovery; SPECIFICALLY NOT ``RecordPendingFailed``) but logged with
	the DISTINCT "UNEXPECTED … signature drift … escalate" wording so an
	operator can escalate it faster than a transient disk fault. Patched AS
	RESOLVED BY ``store.exit_trade`` (stale-binding lesson — store.py binds
	``edge_catcher.live.store.record_close``; an ``edge_catcher.live.state.*``
	patch would NOT intercept).
	"""
	s = SQLiteTradeStore(tmp_path / "live_trades.db")
	rid = _seed_open_live_row(s, coid="cid-EU", kalshi_id="ord-EU")
	import datetime as _dt
	import edge_catcher.live.store as store_mod
	monkeypatch.setattr(store_mod, "record_close",
		lambda *a, **k: (_ for _ in ()).throw(TypeError("bad kwarg")))

	with caplog.at_level("ERROR", logger="edge_catcher.live.store"):
		# MUST NOT raise — still §3.1 best-effort even for the permanent
		# class (B's authoritative async WS/reconciler owns recovery).
		s.exit_trade(
			rid, 60,
			now=_dt.datetime.fromisoformat("2026-05-18T00:14:00+00:00"))

	store_errs = [rec for rec in caplog.records
		if rec.name == "edge_catcher.live.store" and rec.levelname == "ERROR"]
	assert store_errs, (
		"an unexpected non-DB error in exit_trade must be logged at ERROR "
		"level, not silently swallowed")
	msg = store_errs[-1].getMessage()
	assert "UNEXPECTED" in msg and "signature drift" in msg \
			and "escalate" in msg, (
		"a non-sqlite3 error must be logged via the DISTINCT UNEXPECTED / "
		f"possible-API-drift / escalate message — got: {msg!r}")
	assert "DB/disk fault" not in msg, (
		"a non-sqlite3 error must NOT be categorized as the transient "
		f"DB/disk carve-out — got: {msg!r}")
	# The open position survived uncorrupted (the failed close neither
	# transitioned nor wrote it). Exactly one row, still 'open'.
	rows = s._conn.execute(
		"SELECT status, client_order_id FROM live_trades").fetchall()
	assert rows == [("open", "cid-EU")]
	s.close()


def test_settle_trade_failure_db_error_distinct_from_unexpected(
	tmp_path, monkeypatch, caplog):
	"""C5 / spec §3.1 + §5 review-symmetry — the missing
	``settle_trade``-TRANSIENT half (sibling of
	``test_exit_trade_failure_unexpected_error_distinct_from_db_error``).

	A TRANSIENT DB/disk fault (``sqlite3.OperationalError``) in B's
	settlement CAS close is NOT fatal (log ERROR, do NOT raise; SPECIFICALLY
	NOT ``RecordPendingFailed`` — ghost-reject scope is funds-at-risk
	PRE-PLACE INSERTs only, spec §3.1; B's authoritative async settlement
	path / reconciler still owns the eventual close). It MUST land in the
	``except sqlite3.Error`` (transient/disk) carve-out — the DISTINCT
	"DB/disk fault … transient" wording, NOT the UNEXPECTED/API-drift one.
	Patched AS RESOLVED BY ``store.settle_trade`` (stale-binding lesson).
	"""
	s = SQLiteTradeStore(tmp_path / "live_trades.db")
	rid = _seed_open_live_row(s, coid="cid-SD", kalshi_id="ord-SD")
	import datetime as _dt
	import edge_catcher.live.store as store_mod
	# sqlite3.Error subclass → lands in the transient/disk carve-out.
	monkeypatch.setattr(store_mod, "record_close",
		lambda *a, **k: (_ for _ in ()).throw(
			sqlite3.OperationalError("disk I/O error")))

	with caplog.at_level("ERROR", logger="edge_catcher.live.store"):
		# MUST NOT raise — best-effort per §3.1; NOT RecordPendingFailed.
		s.settle_trade(
			rid, "yes",
			now=_dt.datetime.fromisoformat("2026-05-18T00:15:00+00:00"))

	store_errs = [rec for rec in caplog.records
		if rec.name == "edge_catcher.live.store" and rec.levelname == "ERROR"]
	assert store_errs, (
		"a write-failure in settle_trade must be logged at ERROR level "
		"(audit gap), not silently swallowed")
	msg = store_errs[-1].getMessage()
	assert "DB/disk fault" in msg and "transient" in msg, (
		"a sqlite3.Error must be logged via the transient DB/disk carve-out "
		f"message, not the UNEXPECTED branch — got: {msg!r}")
	assert "UNEXPECTED" not in msg, (
		"sqlite3.Error must NOT be categorized as the UNEXPECTED/possible "
		f"API-drift class — got: {msg!r}")
	# The open position survived uncorrupted. Exactly one row, still 'open'.
	rows = s._conn.execute(
		"SELECT status, client_order_id FROM live_trades").fetchall()
	assert rows == [("open", "cid-SD")]
	s.close()


def test_exit_trade_lost_cas_terminal_pre_status_logs_distinct_no_raise(
	tmp_path, caplog):
	"""C5 / FIX 3 — the lost-CAS-race observability branch in ``exit_trade``
	(``if pre_status not in ('open','exit_pending')``). C5 had NO direct test
	for it (C3's analogous ``record_rejected`` lost-race path is covered at
	``:371``/``:434``); this closes that gap, mirroring C3's benign-duplicate
	idiom applied to a close.

	A row is closed ONCE legitimately via ``exit_trade`` (CAS open→won), so
	its status is now terminal (``won``). A SECOND ``exit_trade`` for the same
	id then finds ``pre_status='won'`` → B's ``record_close`` CAS no-ops (a
	settlement/exit raced an already-applied close — B's EXPECTED idempotent
	outcome). B's ``_cas_update`` only WARNs by ``row_id`` on the
	``edge_catcher.live.state`` logger (no ``trade_id`` context on THIS
	store's audit trail), so ``exit_trade`` surfaces it DISTINCTLY on the
	store's logger with the business keys. It MUST NOT raise (§3.1 — B owns
	the eventual close), and MUST NOT clobber the already-closed row (the
	booked won/pnl must survive intact).
	"""
	import datetime as _dt

	s = SQLiteTradeStore(tmp_path / "live_trades.db")
	rid = _seed_open_live_row(s, coid="cid-EL", kalshi_id="ord-EL")

	# First exit — legit CAS open→won (60 > blended 42). NOT a lost race ⇒
	# emits ZERO store-logger ERRORs (asserted, so the discrimination below
	# cannot be satisfied by a stray happy-path ERROR — the inverted-predicate
	# failure mode).
	with caplog.at_level("ERROR", logger="edge_catcher.live.store"):
		s.exit_trade(
			rid, 60,
			now=_dt.datetime.fromisoformat("2026-05-18T00:16:00+00:00"))
	first_errs = [rec for rec in caplog.records
		if rec.name == "edge_catcher.live.store" and rec.levelname == "ERROR"]
	assert first_errs == [], (
		"the FIRST exit is the won-CAS happy path (open→won succeeds), NOT a "
		"lost race — it must emit NO store-logger ERROR; got: "
		f"{[r.getMessage() for r in first_errs]!r}")
	caplog.clear()

	# Snapshot the booked close so we can prove the lost race did NOT clobber.
	s._conn.row_factory = sqlite3.Row
	before = dict(s._conn.execute(
		"SELECT * FROM live_trades WHERE id = ?", (rid,)).fetchone())
	assert before["status"] == "won"

	with caplog.at_level("ERROR", logger="edge_catcher.live.store"):
		# Second exit — pre_status='won' (terminal), CAS lost race. MUST NOT
		# raise; a DIFFERENT exit_price to prove it is NOT re-applied.
		s.exit_trade(
			rid, 99,
			now=_dt.datetime.fromisoformat("2026-05-18T00:16:05+00:00"))

	store_errs = [rec for rec in caplog.records
		if rec.name == "edge_catcher.live.store" and rec.levelname == "ERROR"]
	# EXACTLY ONE store ERROR — the genuine lost race only (with an inverted
	# predicate the happy path above would also ERROR and this would not
	# discriminate; the empty-first-errs + clear + exactly-one make it
	# non-vacuous).
	assert len(store_errs) == 1, (
		"only the genuine lost race (2nd exit, row already 'won') must emit "
		f"a store-logger ERROR — exactly one; got: "
		f"{[r.getMessage() for r in store_errs]!r}")
	msg = store_errs[0].getMessage()
	# Distinct lost-CAS line WITH the business keys (trade_id + actual
	# terminal pre_status) and the lost-race / not-re-applied wording.
	assert f"id={rid}" in msg, (
		f"the lost-CAS log must carry the trade_id — got: {msg!r}")
	assert "lost CAS race" in msg, (
		f"must be the distinct lost-CAS-race line — got: {msg!r}")
	# pre_status is %r-formatted into the line ⇒ rendered as status='won'.
	assert "status='won'" in msg, (
		"the lost-CAS log must carry the actual terminal pre_status "
		f"(status='won') — got: {msg!r}")
	assert "not re-applied" in msg, (
		f"must state the close was NOT re-applied — got: {msg!r}")

	# The already-closed row was NOT clobbered by the lost-race 2nd exit:
	# exactly one row, byte-identical to the first (legit) close — the CAS
	# correctly no-op'd; the exit_price=99 did NOT overwrite the booked 60.
	after = dict(s._conn.execute(
		"SELECT * FROM live_trades WHERE id = ?", (rid,)).fetchone())
	assert after == before, (
		"a lost-CAS-race exit must NOT mutate the already-closed row — "
		f"before={before!r} after={after!r}")
	assert s._conn.execute(
		"SELECT COUNT(*) FROM live_trades").fetchone()[0] == 1
	s.close()


def test_settle_trade_lost_cas_terminal_pre_status_logs_distinct_no_raise(
	tmp_path, caplog):
	"""C5 / FIX 3 — symmetric to
	``test_exit_trade_lost_cas_terminal_pre_status_logs_distinct_no_raise``
	for the lost-CAS-race branch in ``settle_trade``
	(``if pre_status not in ('open','exit_pending')``; C5 had NO direct
	test).

	A row is closed ONCE legitimately via ``settle_trade`` (CAS open→won on
	a yes-side row settling YES). A SECOND ``settle_trade`` then finds
	``pre_status='won'`` → B's ``record_close`` CAS no-ops (settlement raced
	an already-applied close — B's EXPECTED idempotent outcome). It MUST be
	surfaced DISTINCTLY on the store's logger with the business keys, MUST
	NOT raise (§3.1 — B's authoritative async settlement path owns it), and
	MUST NOT clobber the already-settled row.
	"""
	import datetime as _dt

	s = SQLiteTradeStore(tmp_path / "live_trades.db")
	rid = _seed_open_live_row(s, coid="cid-SL", kalshi_id="ord-SL")

	# First settle — legit CAS open→won (yes-side, settles YES). NOT a lost
	# race ⇒ ZERO store-logger ERRORs (non-vacuity guard).
	with caplog.at_level("ERROR", logger="edge_catcher.live.store"):
		s.settle_trade(
			rid, "yes",
			now=_dt.datetime.fromisoformat("2026-05-18T00:17:00+00:00"))
	first_errs = [rec for rec in caplog.records
		if rec.name == "edge_catcher.live.store" and rec.levelname == "ERROR"]
	assert first_errs == [], (
		"the FIRST settle is the won-CAS happy path (open→won succeeds), NOT "
		"a lost race — it must emit NO store-logger ERROR; got: "
		f"{[r.getMessage() for r in first_errs]!r}")
	caplog.clear()

	s._conn.row_factory = sqlite3.Row
	before = dict(s._conn.execute(
		"SELECT * FROM live_trades WHERE id = ?", (rid,)).fetchone())
	assert before["status"] == "won"

	with caplog.at_level("ERROR", logger="edge_catcher.live.store"):
		# Second settle — pre_status='won' (terminal), CAS lost race. MUST
		# NOT raise; a DIFFERENT result to prove it is NOT re-applied.
		s.settle_trade(
			rid, "no",
			now=_dt.datetime.fromisoformat("2026-05-18T00:17:05+00:00"))

	store_errs = [rec for rec in caplog.records
		if rec.name == "edge_catcher.live.store" and rec.levelname == "ERROR"]
	assert len(store_errs) == 1, (
		"only the genuine lost race (2nd settle, row already 'won') must "
		f"emit a store-logger ERROR — exactly one; got: "
		f"{[r.getMessage() for r in store_errs]!r}")
	msg = store_errs[0].getMessage()
	assert f"id={rid}" in msg, (
		f"the lost-CAS log must carry the trade_id — got: {msg!r}")
	assert "lost CAS race" in msg, (
		f"must be the distinct lost-CAS-race line — got: {msg!r}")
	# pre_status is %r-formatted into the line ⇒ rendered as status='won'.
	assert "status='won'" in msg, (
		"the lost-CAS log must carry the actual terminal pre_status "
		f"(status='won') — got: {msg!r}")
	assert "not re-applied" in msg, (
		f"must state the settlement close was NOT re-applied — got: {msg!r}")

	# The already-settled row was NOT clobbered by the lost-race 2nd settle:
	# exactly one row, byte-identical to the first (legit) settlement — the
	# CAS correctly no-op'd; result='no' did NOT overwrite the booked YES.
	after = dict(s._conn.execute(
		"SELECT * FROM live_trades WHERE id = ?", (rid,)).fetchone())
	assert after == before, (
		"a lost-CAS-race settle must NOT mutate the already-settled row — "
		f"before={before!r} after={after!r}")
	assert s._conn.execute(
		"SELECT COUNT(*) FROM live_trades").fetchone()[0] == 1
	s.close()
