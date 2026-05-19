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
