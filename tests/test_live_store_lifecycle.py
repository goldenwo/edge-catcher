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
