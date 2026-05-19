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

INTENT = dict(ticker="KXSOL15M-X", series="KXSOL15M", strategy="debut-fade",
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
		ticker="KXSOL15M-X", entry_price=5, strategy="debut-fade",
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
				ticker="KXSOL15M-X", entry_price=5, strategy="debut-fade",
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
	strategy="debut-fade", side="yes", intended_size=5, entry_price_cents=5,
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
		ticker="KXSOL15M-X", entry_price=5, strategy="debut-fade",
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
