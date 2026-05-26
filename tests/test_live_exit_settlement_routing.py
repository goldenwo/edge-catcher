"""Live exit + settlement seam tests (sub-project E / D3).

Controller-adjudicated **R1** (spec-CORRECTION **SC-D3**, §3/§1/§4.2): the §3
table's literal "place exit via executor" + the prerequisite PaperExecutor
sell path + executor/cfg/position threading into ``_handle_exit`` are **E3's**
deliverable, NOT D3's. ``PaperExecutor.place`` is entries-only, so routing the
exit through ``executor.place`` unconditionally (the only §1-keystone-compliant
way) would run entry-sizing on a paper exit — a G-parity-BLOCKING paper
behaviour change — and a per-call mode branch would violate the §1 keystone.

D3's obligation therefore reduces to **proving** (no logic change — C5 already
made the live close money-correct) that the funds-at-risk exit/settlement seam
is:

* **mode-AGNOSTIC** — ``dispatch._handle_exit`` and ``engine._settlement_poller``
  contain NO ``isinstance``/mode/``is_live`` branch; the store/Protocol absorbs
  the live-vs-paper difference (the §1 keystone — the difference is *which*
  store is wired at boot, not a per-call conditional);
* **live = idempotent C5 B-CAS** — live ``store.exit_trade`` →
  ``live.state.record_close`` CAS (``exit_reason='ws_exit_fill'``,
  won/lost/scratch, entry-fee-remainder consumed); live ``store.settle_trade``
  → ``record_close`` settlement CAS (``exit_reason='settlement'``, supersedes
  an in-flight ``exit_pending``). The CAS predicate is
  ``status IN ('open','exit_pending')`` so a second close (B's E3-wired async
  ``on_fill_event``/reconciler racing the sync path) is an IDEMPOTENT logged
  no-op that **never raises** — D3's sync exit races SAFELY with B's
  authoritative async path (B/Kalshi-truth wins; whichever lands the CAS first
  wins, the other no-ops — the benign lost-CAS property C5/D2 spec-reviews
  already adjudicated SOUND vs §4.2);
* **paper byte-EXACT** — the SAME exit + settlement flows against the paper
  ``TradeStore`` / replay ``InMemoryTradeStore`` close exactly as before the
  live path existed (status/pnl/exit_price/timing byte-identical). This is the
  mandatory K2 11/11 G-parity invariant: D3 changed nothing for paper.

**Updated for the SC-D3 (E3) end-state (2026-05-19).** E3 has now delivered
the deferred "place exit via executor" + the PaperExecutor sell path +
executor/cfg threading into ``_handle_exit`` (the controller-adjudicated R1
deferral from D3 → E3, spec §10 SC-D3). ``_handle_exit`` is therefore now
``async`` and takes ``executor``/``config``; this file's harness drives it via
the ``_run_exit`` helper (a ``PaperExecutor`` executor seam + ``config={}`` —
the executor exit-ACK is deliberately inert, the asserted close is still
``store.exit_trade``'s: PAPER byte-exact / LIVE C5 idempotent backstop while
B's async path is authoritative). The forcing-function structural test that
pinned the *absence* of executor threading was rewritten (NOT silently
deleted) to assert the E3 end-state — the same C6 retire-the-forcing-function
precedent the PR-5→PR-6 strict-xfail twin followed. The D3-cycle invariants
(mode-AGNOSTIC, live = idempotent C5 B-CAS, paper byte-EXACT) are UNCHANGED —
SC-D3 added the executor seam without changing the close money logic.

It drives the REAL ``edge_catcher.live.store.SQLiteTradeStore`` over a
real migrated ``live_trades.db`` (the established idiom from
``tests/test_live_state_integration.py`` /
``tests/test_live_store_lifecycle.py`` — the store's single held connection is
the one the dispatch close and the row reads share), the REAL paper
``TradeStore`` over a tmp SQLite db, and the REAL ``InMemoryTradeStore``.

Run from the project venv (``.venv/Scripts/python.exe``).
"""
from __future__ import annotations

import ast
import asyncio
import inspect
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from edge_catcher.engine import dispatch as _dispatch_mod
from edge_catcher.engine import engine as _engine_mod
from edge_catcher.engine.dispatch import _handle_exit
from edge_catcher.engine.executor import OrderRequest, OrderResult
from edge_catcher.engine.executors.paper import PaperExecutor
from edge_catcher.engine.market_state import MarketState
from edge_catcher.engine.strategy_base import Signal
from edge_catcher.engine.trade_store import InMemoryTradeStore, TradeStore
from edge_catcher.live.state import record_close
from edge_catcher.live.store import SQLiteTradeStore


def _run_exit(signal: Signal, ctx, store, *, now: datetime) -> None:
	"""Drive the REAL ``dispatch._handle_exit`` at its SC-D3 (E3) end-state
	signature: ``_handle_exit`` is now async (it awaits ``executor.place`` for
	the exit order — the §1 seam) and takes ``executor``/``config``
	UNCONDITIONALLY (no mode branch — the executor absorbs the live-vs-paper
	difference; the unconditional ``store.exit_trade`` keeps the PAPER close
	byte-EXACT and is C5's idempotent backstop for LIVE).

	A ``PaperExecutor`` is used for the executor seam in EVERY case (incl. the
	live-store cases): the SC-D3 contract is that the AUTHORITATIVE live close
	is B's async ``on_fill_event``/reconciler while ``store.exit_trade`` is the
	idempotent backstop these tests assert — so the executor's exit-ACK is
	deliberately inert here (PaperExecutor's sell path is a deterministic ACK
	whose fill fields ``_handle_exit`` does not consume). ``config={}`` — the
	exit limit is the ctx bid (no ExecCfg slippage walk; selling into the
	resting bid is the immediate taker price). Mirrors the established
	harness so the dispatch test rigs stay in lock-step."""
	asyncio.run(
		_handle_exit(
			signal, ctx, store, now=now,
			executor=PaperExecutor(market_state=MarketState(), config={}),
			config={},
		)
	)

_NOW = datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc)
_NOW_ISO = _NOW.isoformat()
_LATER = datetime(2026, 5, 18, 12, 5, 0, tzinfo=timezone.utc)
# A second, strictly-later close timestamp — used only by the double-close
# idempotency test so the duplicate close is at a DISTINCT later instant than
# the first (keeps the file's "_LATER for every close" convention uniform: the
# first close is _LATER, the no-op dup is _LATER2 — a different time AND price,
# proving the dup truly no-ops rather than coincidentally matching the first).
_LATER2 = datetime(2026, 5, 18, 12, 10, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Harness — mirrors tests/test_live_dispatch_rewire.py /
# tests/test_live_state_integration.py so the dispatch test rigs stay in
# lock-step. An exit Signal carries the trade_id of the row to close; the
# TickContext stub supplies the bid the exit sells into (`_handle_exit` reads
# ctx.yes_bid for a yes-side row, ctx.no_bid for a no-side row).
# ---------------------------------------------------------------------------


def _exit_signal(trade_id: int, side: str = "yes") -> Signal:
	return Signal(
		action="exit",
		ticker="KXSOL15M-26MAY18H12",
		side=side,
		series="KXSOL15M",
		strategy="debut_fade",
		reason="d3-exit",
		trade_id=trade_id,
	)


def _ctx(yes_bid: int = 60, no_bid: int = 40) -> MagicMock:
	"""`_handle_exit` reads ctx.yes_bid / ctx.no_bid (selling hits the bid)."""
	return MagicMock(yes_bid=yes_bid, no_bid=no_bid)


def _row(conn: sqlite3.Connection, row_id: int) -> dict[str, object]:
	"""Read a live_trades row as a dict (the established idiom from
	tests/test_live_state_integration.py:_row — row_factory swapped on/off
	around the single fetch so the store's own held connection is unperturbed
	for subsequent dispatch writes)."""
	conn.row_factory = sqlite3.Row
	r = conn.execute(
		"SELECT * FROM live_trades WHERE id = ?", (row_id,)
	).fetchone()
	conn.row_factory = None
	assert r is not None, f"row {row_id} missing"
	return dict(r)


def _seed_live_open_row(
	store: SQLiteTradeStore,
	*,
	side: str = "yes",
	entry: int = 42,
	fill_size: int = 10,
	coid: str = "cid-d3",
	kalshi_id: str = "kx-d3",
	status: str = "open",
) -> int:
	"""Seed a realistic live ``open`` (or ``exit_pending``) row via the C1→C2
	production flow (record_intent INSERTs the pending row; record_trade
	CAS-transitions it pending→open with the real Kalshi id) — the same idiom
	tests/test_live_store_lifecycle.py uses, so the row shape (entry_fee_cents,
	entry_fee_remaining_cents, blended_entry_cents) is the genuine B-produced
	one, not hand-rolled SQL. Optionally drives the row to ``exit_pending``.

	CANONICAL B-FEE-DERIVATION ORACLE — the single source of truth for the
	entry_fee / entry_fee_remaining / won-lost-scratch pnl literals asserted
	below is ``tests/test_live_store_lifecycle.py::_seed_open_live_row``
	(≈:792-819) and its fee-derivation docstring on
	``test_exit_trade_routes_to_B_record_close_full_close`` (≈:891-895) /
	``test_exit_trade_loss_and_scratch_outcomes`` (≈:944-959). The B-fee
	arithmetic literals in THIS file (entry_fee=18, won pnl=145, lost pnl=-250,
	scratch pnl=-36) are kept cross-checkable against that ONE authority by
	matching its canonical seed values exactly: the C1 ``record_intent`` carries
	the SAME INTENT-shaped intent (entry_price_cents=5, intended_size=5,
	stop_loss_distance_cents=3 — the Signal intent, deliberately distinct from
	the fill so a live row legitimately has entry_price≠blended_entry), and the
	C2 ``record_trade`` records the SAME authoritative fill
	(entry_price/blended_entry=42, fill_size/intended_size=10) that the oracle
	documents as producing ``entry_fee_cents = int(round(STANDARD_FEE.calculate(
	42,10))) = 18`` with ``entry_fee_remaining_cents`` seeded = 18. Changing
	these here without re-deriving from B's arithmetic (an independent oracle)
	would silently drift the two files apart — DO NOT."""
	store.record_intent(
		ticker="KXSOL15M-26MAY18H12",
		series="KXSOL15M",
		strategy="debut_fade",
		side=side,
		# Canonical Signal-intent values (oracle's INTENT, test_live_store_
		# lifecycle.py:34-37): intended_size=5, entry_price_cents=5,
		# stop_loss_distance_cents=3. These are the pre-place Signal intent,
		# NOT the fill — C2's record_trade below records the real fill
		# (42/10) and does NOT overwrite entry_price_cents, so a live row
		# legitimately has entry_price(5) ≠ blended_entry(42). No assertion
		# in this file reads entry_price; every pnl literal keys off the
		# fill (blended_entry=42, fill_size=10) recorded by record_trade.
		intended_size=5,
		entry_price_cents=5,
		stop_loss_distance_cents=3,
		client_order_id=coid,
		placed_at_utc=_NOW_ISO,
	)
	tid = store.record_trade(
		ticker="KXSOL15M-26MAY18H12",
		# Authoritative fill — the SAME values the canonical oracle records
		# (test_live_store_lifecycle.py:809-815): entry_price=blended_entry=42,
		# fill_size=intended_size=10. The oracle's docstring (≈:799-804)
		# documents this produces entry_fee_cents =
		# int(round(STANDARD_FEE.calculate(42,10))) = 18 and seeds
		# entry_fee_remaining_cents = 18 — the single derivation all won/lost/
		# scratch pnl literals in this file reference.
		entry_price=entry,
		strategy="debut_fade",
		side=side,
		series_ticker="KXSOL15M",
		intended_size=fill_size,
		fill_size=fill_size,
		blended_entry=entry,
		fill_pct=1.0,
		slippage_cents=0,
		now=_NOW,
		client_order_id=coid,
		kalshi_order_id=kalshi_id,
	)
	if status == "exit_pending":
		# Drive open→exit_pending. B exposes no standalone live.state writer
		# for this transition (it is set inside the WS exit-POST-accepted
		# handler), so use the SAME established seeding idiom the B state
		# suite uses for this exact case (tests/test_live_state.py:380-382 —
		# a direct status UPDATE on the store's own held connection).
		store._conn.execute(
			"UPDATE live_trades SET status='exit_pending' WHERE id=?", (tid,)
		)
		store._conn.commit()
	return tid


def _seed_paper_open_row(
	store: TradeStore | InMemoryTradeStore,
	*,
	side: str = "yes",
	entry: int = 42,
	fill_size: int = 10,
) -> int:
	"""Seed a paper/InMemory ``open`` row via the production record_trade
	(INSERTs literal 'open' — the G-parity basis). Identical call for both the
	SQLite paper store and the replay in-memory twin."""
	return store.record_trade(
		ticker="KXSOL15M-26MAY18H12",
		entry_price=entry,
		strategy="debut_fade",
		side=side,
		series_ticker="KXSOL15M",
		intended_size=fill_size,
		fill_size=fill_size,
		blended_entry=entry,
		fill_pct=1.0,
		slippage_cents=0,
		now=_NOW,
	)


# ---------------------------------------------------------------------------
# Regression — a live exit whose IOC sell does NOT fill must NOT book a phantom
# close. On a thin book the IOC sell finds no resting bid at the limit and gets
# 0-fill; the position was never sold and rides to settlement. The engine
# previously DISCARDED the executor result and booked store.exit_trade at the
# ctx bid regardless, fabricating a stop/TP exit (exit_reason='ws_exit_fill')
# for a sale that never happened. Confirmed on real money 2026-05-26: the live
# db read -$8.53 (phantom closes) vs Kalshi settlements -$3.53; several true
# settlement wins were recorded as small stop-losses.
# ---------------------------------------------------------------------------


class _ZeroFillExecutor:
	"""Live-style executor whose IOC sell finds no liquidity at the limit and
	returns a 0-fill rejection — byte-identical to
	``LiveExecutor._translate_order``'s ``ioc_zero_fill`` OrderResult
	(executors/live.py:179-189). Used in place of PaperExecutor (which always
	returns a full fill) to drive the real live no-fill path through dispatch."""

	async def place(self, req: OrderRequest) -> OrderResult:
		return OrderResult(
			status="rejected",
			intended_size=req.size_contracts,
			filled_size=0,
			blended_entry_cents=0,
			fill_pct=0.0,
			slippage_cents=0,
			rejection_reason="ioc_zero_fill",
			order_id=None,
		)


def test_live_exit_zero_fill_does_not_book_phantom_close(tmp_path: Path) -> None:
	"""Funds-at-risk / observability: when the live IOC exit sell gets 0 fill,
	the position was NOT sold — it rides to settlement, where the settlement
	poller books the true outcome (exit_reason='settlement'). The row MUST stay
	``open``; booking a close here fabricates a stop/TP at the bid that never
	executed — the 2026-05-26 phantom-exit bug (db -$8.53 vs true Kalshi
	-$3.53). Drives the REAL ``dispatch._handle_exit`` against the REAL live
	``SQLiteTradeStore`` with a live-style 0-fill executor seam."""
	store = SQLiteTradeStore(tmp_path / "live_trades.db")
	try:
		tid = _seed_live_open_row(store, side="no", entry=28, fill_size=2)
		conn = store._conn
		assert _row(conn, tid)["status"] == "open"

		# No-side row → would sell into ctx.no_bid=37 (the TP target). The IOC
		# finds no buyer at 37 → 0-fill. The close MUST be skipped.
		asyncio.run(
			_handle_exit(
				_exit_signal(tid, "no"), _ctx(no_bid=37), store, now=_LATER,
				executor=_ZeroFillExecutor(), config={},
			)
		)

		row = _row(conn, tid)
		assert row["status"] == "open", (
			"a 0-fill IOC exit did not sell the position — the row must stay "
			"open for settlement, NOT be booked as a phantom ws_exit_fill close"
		)
		assert row["pnl_cents"] is None, "no fill → no realized P&L may be booked"
		assert row["exit_reason"] is None
		assert row["exit_price_cents"] is None
		# Exactly one row, untouched (no split, no terminal transition).
		assert conn.execute("SELECT COUNT(*) FROM live_trades").fetchone()[0] == 1
	finally:
		store.close()


class _TimeoutExecutor:
	"""Live-style executor whose place() never returns within the cap — models
	a Kalshi exit POST that hangs (no response before
	_ENTRY_PLACEMENT_TIMEOUT_SECONDS). asyncio.wait_for cancels the sleep and
	raises asyncio.TimeoutError into dispatch; the post-sleep line is
	unreachable. Distinct from _ZeroFillExecutor (a prompt 0-fill rejection):
	here the executor gives NO answer at all, driving dispatch's
	``except asyncio.TimeoutError`` branch rather than the fill gate."""

	async def place(self, req: OrderRequest) -> OrderResult:
		await asyncio.sleep(30)
		raise AssertionError("unreachable — wait_for must cancel this first")


def test_live_exit_timeout_leaves_row_open(
	tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
	"""Funds-at-risk / observability: when the exit place() exceeds the
	_ENTRY_PLACEMENT_TIMEOUT_SECONDS cap, dispatch catches asyncio.TimeoutError,
	leaves exit_result None, and SKIPS the close — the sale was never confirmed,
	so the row stays ``open`` and the settlement poller books the true outcome
	(exit_reason='settlement'). Pins the OTHER non-fill branch (the 0-fill
	rejection is covered by test_live_exit_zero_fill_does_not_book_phantom_close);
	pre-fix, a timed-out exit fell through to the unconditional store.exit_trade
	and booked a phantom close at the bid. The cap is monkeypatched low (the real
	60s would stall the suite) and the executor never returns in time, so
	asyncio.wait_for raises a genuine TimeoutError through the real path."""
	monkeypatch.setattr(_dispatch_mod, "_ENTRY_PLACEMENT_TIMEOUT_SECONDS", 0.05)
	store = SQLiteTradeStore(tmp_path / "live_trades.db")
	try:
		tid = _seed_live_open_row(store, side="no", entry=28, fill_size=2)
		conn = store._conn
		assert _row(conn, tid)["status"] == "open"

		asyncio.run(
			_handle_exit(
				_exit_signal(tid, "no"), _ctx(no_bid=37), store, now=_LATER,
				executor=_TimeoutExecutor(), config={},
			)
		)

		row = _row(conn, tid)
		assert row["status"] == "open", (
			"an exit whose place() timed out is NOT a confirmed sale — the row "
			"must stay open for the settlement poller, NOT be booked as a close"
		)
		assert row["pnl_cents"] is None, "no confirmed fill → no realized P&L"
		assert row["exit_reason"] is None
		assert row["exit_price_cents"] is None
		assert conn.execute("SELECT COUNT(*) FROM live_trades").fetchone()[0] == 1
	finally:
		store.close()


# ===========================================================================
# (1) LIVE exit — mode-agnostic + B-CAS-correct + idempotent vs B's async
# ===========================================================================


def test_live_exit_via_dispatch_routes_to_b_record_close_cas(
	tmp_path: Path,
) -> None:
	"""Failure mode prevented (funds-at-risk): a strategy/TP-SL exit on a
	real-money live position does NOT book through B's authoritative
	``record_close`` CAS — the durable row never closes, P&L is lost, or
	dispatch grew a mode branch to special-case live.

	Drives the REAL ``dispatch._handle_exit`` (the production engine handler,
	UNCHANGED by D3) with an exit Signal against the REAL live
	``SQLiteTradeStore``. Asserts the seam books the close via B's
	``record_close`` CAS: ``exit_reason='ws_exit_fill'``, the correct
	won/lost/scratch + pnl (B's full-close arithmetic: pre-fee compare vs the
	blended entry; pnl = fill_size*(exit-blended) - entry_fee_remaining -
	exit_fee), ``exit_time`` set, the entry-fee-remainder consumed into
	``entry_fee_cents`` and then zeroed — exactly ONE row, UPDATE-in-place
	(no split child). No mode branch is involved; dispatch calls the same
	``store.exit_trade`` for paper and live (test (3) proves paper unchanged)."""
	store = SQLiteTradeStore(tmp_path / "live_trades.db")
	try:
		tid = _seed_live_open_row(store, side="yes", entry=42, fill_size=10)
		conn = store._conn

		# Pre-state: B's record_trade CAS seeded entry_fee_cents +
		# entry_fee_remaining_cents (the parent's still-owed allocation).
		pre = _row(conn, tid)
		assert pre["status"] == "open"
		entry_fee = int(pre["entry_fee_cents"])
		entry_fee_remaining = int(pre["entry_fee_remaining_cents"])
		assert entry_fee_remaining == entry_fee  # never-split row

		# --- Drive the REAL dispatch exit handler. yes-side row → sells into
		# ctx.yes_bid=60 (60 > 42 entry → won). _handle_exit calls
		# store.exit_trade(trade_id, 60, now) UNCONDITIONALLY (no executor /
		# config / mode in scope — re-confirmed Step A).
		_run_exit(_exit_signal(tid, "yes"), _ctx(yes_bid=60), store, now=_LATER)

		row = _row(conn, tid)
		# Booked through B's record_close CAS — not a paper-shaped UPDATE.
		assert row["status"] == "won", (
			"60¢ exit on a 42¢ yes entry must be a won close via B record_close"
		)
		assert row["exit_reason"] == "ws_exit_fill", (
			"live exit must carry B's full-close exit_reason (C5 store.exit_trade"
			" → record_close), proving the live store routed to the B CAS"
		)
		assert row["exit_price_cents"] == 60
		assert row["exit_time"] == _LATER.isoformat()
		# B's DDL pnl contract: fill_size*(exit-blended) - entry_fee_remaining
		#   - exit_fee  (record_close does NOT recompute; the C5 store does).
		from edge_catcher.adapters.kalshi.fees import STANDARD_FEE

		exit_fee = int(round(STANDARD_FEE.calculate(60, 10)))
		expected_pnl = 10 * (60 - 42) - entry_fee_remaining - exit_fee
		assert row["pnl_cents"] == expected_pnl, (
			f"pnl must follow B's full-close arithmetic; "
			f"expected {expected_pnl} got {row['pnl_cents']}"
		)
		# Entry-fee-remainder consumed into entry_fee_cents then zeroed
		# (record_close's COALESCE(entry_fee_remaining_cents, entry_fee_cents)).
		assert row["entry_fee_cents"] == entry_fee_remaining
		assert row["entry_fee_remaining_cents"] == 0
		# Exactly one row — a full close UPDATEs in place, no split child.
		assert (
			conn.execute("SELECT COUNT(*) FROM live_trades").fetchone()[0] == 1
		)
	finally:
		store.close()


def test_live_exit_is_idempotent_vs_bs_async_authoritative_close(
	tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
	"""Failure mode prevented (THE D3 funds-at-risk race property): D3's sync
	``_handle_exit`` close and B's E3-wired async ``on_fill_event`` / reconciler
	BOTH act on the same live row (the WS exit fill arrives ~concurrently with
	the strategy-driven sync exit). If the second close were not idempotent it
	would raise (halting the real-money engine) or clobber the first
	authoritative close / double-count P&L.

	Asserts: after the sync ``_handle_exit`` books the close, a SECOND close on
	the SAME row (B's authoritative async path — modelled by calling B's
	``record_close`` directly, exactly the writer ``on_fill_event`` uses) is an
	IDEMPOTENT no-op: it does NOT raise, the row stays the FIRST authoritative
	close (status/exit_price/pnl UNCHANGED), still exactly one row, and B's
	lost-CAS WARNING is logged (the benign §4.2-sound race outcome). This is
	precisely why D3 can leave the sync exit path in place — B/Kalshi-truth is
	the authority and the loser no-ops."""
	store = SQLiteTradeStore(tmp_path / "live_trades.db")
	try:
		tid = _seed_live_open_row(store, side="yes", entry=42, fill_size=10)
		conn = store._conn

		# --- 1. D3's sync exit lands the close first (won @ 60¢).
		_run_exit(_exit_signal(tid, "yes"), _ctx(yes_bid=60), store, now=_LATER)
		first = _row(conn, tid)
		assert first["status"] == "won"

		# --- 2. B's authoritative async path (on_fill_event's writer is
		# live.state.record_close) races and ALSO tries to close the now-
		# terminal row at a different price. The CAS precondition
		# status IN ('open','exit_pending') no longer holds → idempotent
		# no-op, MUST NOT raise (B's _cas_update returns False + WARNs).
		with caplog.at_level(logging.WARNING, logger="edge_catcher.live.state"):
			# record_close returns None whether or not the CAS applied — the
			# no-op is silent to the caller (never an exception).
			result = record_close(
				conn,
				tid,
				status="lost",  # B would compute its own; the point is the CAS no-ops
				exit_price_cents=5,
				exit_time=_LATER.isoformat(),
				exit_reason="ws_exit_fill",
				pnl_cents=-999,
				exit_fee_cents=0,
			)
		assert result is None  # never raises; returns None on the lost CAS

		# Row is UNCHANGED — still the FIRST authoritative close (full-row
		# equality: the lost CAS must touch NO column, not just the projected
		# few).
		after = _row(conn, tid)
		assert after == first, (
			"the second (B-async) close must IDEMPOTENTLY no-op — the row must "
			f"stay the first authoritative close; first={first} after={after}"
		)
		assert (
			conn.execute("SELECT COUNT(*) FROM live_trades").fetchone()[0] == 1
		), "the lost-CAS second close must not insert a second row"
		# B's lost-CAS WARNING was logged (the benign §4.2-sound race outcome).
		assert any(
			"CAS lost race" in r.getMessage() and f"id={tid}" in r.getMessage()
			for r in caplog.records
		), (
			"B's _cas_update must log the lost-race WARNING for the second "
			f"close; records={[r.getMessage() for r in caplog.records]!r}"
		)
	finally:
		store.close()


def test_live_exit_no_side_sells_into_no_bid_and_books_loss(
	tmp_path: Path,
) -> None:
	"""Failure mode prevented: a no-side live exit prices off the wrong book
	(yes_bid instead of no_bid) — wrong P&L on a real-money close. Pins the
	bid-side selection (``_handle_exit``: ``exit_price = ctx.yes_bid if
	side=='yes' else ctx.no_bid``) AND the lost (worse-than-entry) B-CAS path
	for completeness of the won/lost/scratch coverage."""
	store = SQLiteTradeStore(tmp_path / "live_trades.db")
	try:
		tid = _seed_live_open_row(
			store, side="no", entry=42, fill_size=10, coid="cid-d3-no",
			kalshi_id="kx-d3-no",
		)
		conn = store._conn
		# no-side row → sells into ctx.no_bid=30 (30 < 42 entry → lost).
		_run_exit(
			_exit_signal(tid, "no"), _ctx(yes_bid=99, no_bid=30), store,
			now=_LATER,
		)
		row = _row(conn, tid)
		assert row["status"] == "lost", (
			"no-side exit must sell into no_bid (30¢ < 42¢ entry → lost), not "
			"yes_bid (99¢) — proves the correct book side is used"
		)
		assert row["exit_price_cents"] == 30
		assert row["exit_reason"] == "ws_exit_fill"
	finally:
		store.close()


def test_live_exit_scratch_books_scratch_status_via_b_cas(
	tmp_path: Path,
) -> None:
	"""Failure mode prevented (funds-at-risk + completeness): the won/lost/
	scratch coverage this file claims is incomplete without a SCRATCH exit, and
	— more importantly — the status-vs-notify-label DUALITY on a scratch close
	is undocumented, so a future change could silently collapse it.

	Drives the REAL ``dispatch._handle_exit`` with ``ctx.yes_bid == entry`` so
	the exit price EQUALS the blended entry → B's ``record_close`` books a
	pre-fee SCRATCH (``status='scratch'``), but fees push the net P&L NEGATIVE.

	Asserts the B-CAS scratch close, then explicitly PINS the dual-lens
	classification so it cannot drift:

	* DB ``status`` = the **gross-price-outcome** class (exit vs blended entry,
	  PRE-fee — exit==entry ⇒ scratch). This is what F / the H-phase reporting
	  aggregation buckets by. (Identical to the canonical oracle's rule —
	  tests/test_live_store_lifecycle.py::test_exit_trade_loss_and_scratch_
	  outcomes ≈:956-965: "fees push it negative; status still 'scratch' —
	  outcome is the pre-fee compare, exactly as B does it".)
	* dispatch's ``_pnl_label`` (the notify-outcome label fn, dispatch.py:89)
	  keys on the **net P&L sign**: it returns SCRATCH only when ``pnl == 0``;
	  here ``pnl < 0`` ⇒ it returns ``LOSS``. So the operator's notification
	  reads "LOSS −36¢" while the durable row's status is 'scratch'.

	This dual classification is DEFENSIBLE and INTENDED, not a bug: the two
	consumers want two different correct lenses on the same close — the durable
	row records the gross price outcome (for P&L aggregation by outcome class),
	the operator notification records the net cash sign (a fee-eroded scratch is
	a net loss to the operator). Pinning both halves here means a future edit
	that "fixes" one to match the other will trip this test and force a
	deliberate decision."""
	store = SQLiteTradeStore(tmp_path / "live_trades.db")
	try:
		# Canonical seed (oracle: blended_entry=42, fill_size=10,
		# entry_fee_cents=18, entry_fee_remaining_cents=18).
		tid = _seed_live_open_row(store, side="yes", entry=42, fill_size=10)
		conn = store._conn

		# yes-side row, exit price == the 42¢ blended entry (gross 0) → B's
		# record_close books a pre-fee SCRATCH.
		_run_exit(
			_exit_signal(tid, "yes"), _ctx(yes_bid=42), store, now=_LATER
		)

		row = _row(conn, tid)
		assert row["status"] == "scratch", (
			"exit price == blended entry (gross 0) must book a pre-fee SCRATCH "
			"via B's record_close CAS (outcome is the pre-fee compare — B's "
			"rule, mirrored by the canonical oracle)"
		)
		assert row["exit_reason"] == "ws_exit_fill", (
			"a strategy/TP-SL scratch exit still carries B's full-close "
			"exit_reason (C5 store.exit_trade → record_close)"
		)
		assert row["exit_price_cents"] == 42
		# Hand-derived from the ONE canonical oracle (test_live_store_
		# lifecycle.py::test_exit_trade_loss_and_scratch_outcomes ≈:956-959):
		# exit_fee = int(round(STANDARD_FEE.calculate(42,10))) = 18; gross =
		# fill_size*(exit-blended) = 10*(42-42) = 0; pnl = gross -
		# entry_fee_remaining(18) - exit_fee(18) = -36 (NEGATIVE — a fee-eroded
		# scratch). Independently confirmed below via the public fee model.
		from edge_catcher.adapters.kalshi.fees import STANDARD_FEE

		exit_fee = int(round(STANDARD_FEE.calculate(42, 10)))
		entry_fee_remaining = 18  # canonical oracle (record_trade fill 42/10)
		expected_pnl = 10 * (42 - 42) - entry_fee_remaining - exit_fee
		assert expected_pnl == -36  # pins the oracle arithmetic itself
		assert row["pnl_cents"] == expected_pnl, (
			f"scratch pnl is gross(0) − entry_fee_remaining − exit_fee = "
			f"{expected_pnl} (negative); got {row['pnl_cents']}"
		)
		assert row["pnl_cents"] < 0, "a fee-eroded scratch nets a LOSS in cash"

		# --- Pin the status-vs-label DUALITY (the load-bearing assertion of
		# this test). DB status = gross-price-outcome class ('scratch'); the
		# operator's notify label = net-P&L-sign class. dispatch._pnl_label
		# keys SCRATCH on pnl==0, so a pnl<0 scratch row labels as LOSS.
		outcome, pnl_str = _dispatch_mod._pnl_label(row["pnl_cents"])
		assert outcome == "LOSS", (
			"DEFENSIBLE DUAL-LENS (pinned, NOT a bug): the durable row's "
			"status is the gross-price-outcome class ('scratch', pre-fee, for "
			"F/H-phase P&L aggregation by outcome); _pnl_label is the "
			"operator-notification class keyed on the NET cash sign — it "
			"returns SCRATCH only at pnl==0, so this fee-eroded scratch "
			"(pnl=-36) correctly labels LOSS to the operator. The two "
			"consumers intentionally use two correct lenses on one close; this "
			"assertion pins both halves so a future edit collapsing one into "
			f"the other trips here. status={row['status']!r} "
			f"label_outcome={outcome!r}"
		)
		assert pnl_str == "-36¢", (
			"the operator sees the true net cash magnitude regardless of the "
			f"gross 'scratch' status; got {pnl_str!r}"
		)
	finally:
		store.close()


# ===========================================================================
# (2) SETTLEMENT — mode-agnostic + live B-CAS (exit_reason='settlement',
# exit_pending-supersede, entry-fee-remainder consumed)
# ===========================================================================


def test_live_settlement_routes_to_b_record_close_settlement_cas(
	tmp_path: Path,
) -> None:
	"""Failure mode prevented (funds-at-risk): market settlement on a live
	position does NOT book through B's settlement ``record_close`` CAS — wrong
	binary P&L, or the fee model diverges from B's so F double-books.

	Models the ``engine._settlement_poller`` store leg (the UNCONDITIONAL
	``store.settle_trade(id, result, now)`` → ``store.get_trade_by_id`` — the
	mode-agnostic seam re-confirmed in Step A) against the REAL live
	``SQLiteTradeStore``. yes-side row, market resolves YES → won at the binary
	100¢, ``exit_reason='settlement'``, NO exit fee at settlement, the
	entry-fee-remainder consumed."""
	store = SQLiteTradeStore(tmp_path / "live_trades.db")
	try:
		tid = _seed_live_open_row(store, side="yes", entry=42, fill_size=10)
		conn = store._conn
		entry_fee_remaining = int(_row(conn, tid)["entry_fee_remaining_cents"])

		# The settlement poller's exact store leg: settle_trade(id, raw market
		# result, now) — 'yes' = market resolved YES.
		store.settle_trade(tid, "yes", now=_LATER)
		settled = store.get_trade_by_id(tid)
		assert settled is not None

		row = _row(conn, tid)
		assert row["status"] == "won", "yes-side row, YES resolution → won"
		assert row["exit_reason"] == "settlement", (
			"live settlement must carry exit_reason='settlement' (C5 "
			"store.settle_trade → record_close settlement CAS)"
		)
		assert row["exit_price_cents"] == 100, "binary YES settles at 100¢"
		assert row["exit_fee_cents"] == 0, "Kalshi charges no fee at settlement"
		# pnl = fill_size*(payout - blended) - entry_fee_remaining (no exit fee).
		assert row["pnl_cents"] == 10 * (100 - 42) - entry_fee_remaining
		assert row["entry_fee_remaining_cents"] == 0  # consumed by record_close
		# get_trade_by_id is the mode-agnostic read the poller uses for notify.
		assert settled["status"] == "won"
		assert settled["pnl_cents"] == row["pnl_cents"]
	finally:
		store.close()


def test_live_settlement_supersedes_in_flight_exit_pending(
	tmp_path: Path,
) -> None:
	"""Failure mode prevented (funds-at-risk): a position whose exit POST was
	in flight (row in ``exit_pending``) settles at expiry, but the settlement
	close is rejected because the row left ``open`` — the real-money position
	never books its settlement P&L.

	B's ``record_close`` CAS precondition is ``status IN ('open',
	'exit_pending')`` — so settlement SUPERSEDES an in-flight exit (the exit
	attempt is moot at expiry; B's ``on_settlement_event`` behaviour). Pins
	that C5 ``store.settle_trade`` closes an ``exit_pending`` row won/lost at
	the binary price with ``exit_reason='settlement'``."""
	store = SQLiteTradeStore(tmp_path / "live_trades.db")
	try:
		tid = _seed_live_open_row(
			store, side="yes", entry=42, fill_size=10, status="exit_pending"
		)
		conn = store._conn
		assert (
			conn.execute(
				"SELECT status FROM live_trades WHERE id=?", (tid,)
			).fetchone()[0]
			== "exit_pending"
		)

		# Market resolves NO → a yes-side row loses, but settlement still
		# closes it (supersedes the in-flight exit).
		store.settle_trade(tid, "no", now=_LATER)
		row = _row(conn, tid)
		assert row["status"] == "lost", "yes-side row, NO resolution → lost"
		assert row["exit_reason"] == "settlement", (
			"settlement must supersede the in-flight exit_pending (CAS "
			"precondition includes 'exit_pending')"
		)
		assert row["exit_price_cents"] == 0, "binary NO settles a yes row at 0¢"
		# The supersede is recorded in notes (C5 sets it for exit_pending).
		assert row["notes"] == "settlement superseded in-flight exit"
	finally:
		store.close()


# ===========================================================================
# (3) PAPER BYTE-EXACT GUARD — the SAME exit + settlement flows against the
# paper TradeStore and the replay InMemoryTradeStore close EXACTLY as before
# the live path existed (mandatory K2 11/11 G-parity: D3 changed nothing for
# paper; the store/Protocol absorbs the difference, dispatch does not branch).
# ===========================================================================


@pytest.mark.parametrize("store_kind", ["paper_sqlite", "in_memory"])
def test_paper_exit_via_dispatch_is_byte_exact(
	store_kind: str, tmp_path: Path
) -> None:
	"""Failure mode prevented (G-parity BLOCKING — K2 11/11): D3 perturbs the
	paper/replay exit close (status/pnl/exit_price/timing). Drives the SAME
	REAL ``dispatch._handle_exit`` against the paper ``TradeStore`` (SQLite)
	and the replay ``InMemoryTradeStore`` and asserts the close is EXACTLY the
	pre-D3 paper arithmetic (paper ``exit_trade``: pnl = fill_size*(exit -
	effective_entry) - entry_fee - exit_fee; status won iff pnl>0). Because
	dispatch calls the identical ``store.exit_trade`` for paper and live (no
	mode branch — test (4)), proving paper is byte-unchanged proves the seam
	is genuinely mode-agnostic and D3 added zero paper-visible behaviour."""
	if store_kind == "paper_sqlite":
		store: TradeStore | InMemoryTradeStore = TradeStore(
			tmp_path / "paper_trades.db"
		)
	else:
		store = InMemoryTradeStore()
	tid = _seed_paper_open_row(store, side="yes", entry=42, fill_size=10)

	# Pre-D3 paper arithmetic, recomputed independently here from the public
	# fee model (NOT read back from the store) so this is a true oracle.
	from edge_catcher.adapters.kalshi.fees import STANDARD_FEE

	pre = store.get_trade_by_id(tid)
	assert pre is not None and pre["status"] == "open"
	entry_fee = int(pre["entry_fee_cents"])
	exit_fee = int(STANDARD_FEE.calculate(60, 10))
	expected_pnl = 10 * (60 - 42) - entry_fee - exit_fee
	expected_status = (
		"won" if expected_pnl > 0 else ("lost" if expected_pnl < 0 else "scratch")
	)

	# Same dispatch handler, same call — yes-side row sells into yes_bid=60.
	_run_exit(_exit_signal(tid, "yes"), _ctx(yes_bid=60), store, now=_LATER)

	closed = store.get_trade_by_id(tid)
	assert closed is not None
	assert closed["status"] == expected_status
	assert closed["pnl_cents"] == expected_pnl, (
		f"paper exit pnl must be byte-exact pre-D3 arithmetic ({store_kind}); "
		f"expected {expected_pnl} got {closed['pnl_cents']}"
	)
	assert closed["exit_price"] == 60
	assert closed["exit_time"] == _LATER.isoformat()
	# Paper schema has NO exit_reason column — proves this is the paper-shaped
	# UPDATE, NOT the live B-CAS path (which sets exit_reason='ws_exit_fill').
	assert "exit_reason" not in closed


@pytest.mark.parametrize("store_kind", ["paper_sqlite", "in_memory"])
def test_paper_settlement_is_byte_exact(
	store_kind: str, tmp_path: Path
) -> None:
	"""Failure mode prevented (G-parity BLOCKING — K2 11/11): D3 perturbs the
	paper/replay settlement close. Models the ``_settlement_poller`` store leg
	(``store.settle_trade(id, raw_result, now)`` → ``get_trade_by_id``) against
	the paper ``TradeStore`` and replay ``InMemoryTradeStore`` and asserts the
	pre-D3 paper settlement arithmetic byte-for-byte (paper ``settle_trade``:
	yes-side + 'yes' → exit 100¢/won; pnl = fill_size*(exit - effective_entry)
	- entry_fee; NO exit fee). Paper-unchanged ⇒ the settlement seam is
	mode-agnostic and D3 added zero paper-visible behaviour."""
	if store_kind == "paper_sqlite":
		store: TradeStore | InMemoryTradeStore = TradeStore(
			tmp_path / "paper_trades.db"
		)
	else:
		store = InMemoryTradeStore()
	tid = _seed_paper_open_row(store, side="yes", entry=42, fill_size=10)

	pre = store.get_trade_by_id(tid)
	assert pre is not None
	entry_fee = int(pre["entry_fee_cents"])
	# Pre-D3 paper settle arithmetic: yes-side + 'yes' result → 100¢ won.
	expected_pnl = 10 * (100 - 42) - entry_fee

	store.settle_trade(tid, "yes", now=_LATER)
	settled = store.get_trade_by_id(tid)
	assert settled is not None
	assert settled["status"] == "won", "yes-side + YES result → won (paper)"
	assert settled["pnl_cents"] == expected_pnl, (
		f"paper settlement pnl must be byte-exact pre-D3 arithmetic "
		f"({store_kind}); expected {expected_pnl} got {settled['pnl_cents']}"
	)
	assert settled["exit_price"] == 100
	assert settled["exit_time"] == _LATER.isoformat()
	assert "exit_reason" not in settled  # paper-shaped, NOT the B settlement CAS


def test_paper_exit_idempotent_double_close_is_noop(tmp_path: Path) -> None:
	"""Pins the paper-side idempotency parity: paper ``exit_trade``'s
	``WHERE id=? AND status='open'`` guard means a second close (a duplicate
	dispatch exit) is a silent no-op leaving the first close intact — the
	paper analogue of the live B-CAS no-op in test (1.idempotent). Confirms
	the idempotency property is uniform across the mode-agnostic seam (so D3's
	"races safely" guarantee is not a live-only artefact)."""
	store = TradeStore(tmp_path / "paper_trades.db")
	tid = _seed_paper_open_row(store, side="yes", entry=42, fill_size=10)
	# First close at _LATER (uniform with every other close in this file).
	_run_exit(_exit_signal(tid, "yes"), _ctx(yes_bid=60), store, now=_LATER)
	first = store.get_trade_by_id(tid)
	assert first is not None and first["status"] == "won"
	# Second close at a STRICTLY-LATER instant AND a different price — if the
	# guard were absent this would overwrite exit_time/exit_price/pnl; the
	# idempotent no-op (WHERE status='open') leaves the first close intact.
	_run_exit(_exit_signal(tid, "yes"), _ctx(yes_bid=5), store, now=_LATER2)
	after = store.get_trade_by_id(tid)
	assert after == first, (
		"a second paper exit must be an idempotent no-op (WHERE status='open')"
		f" — row must stay the first close; first={first} after={after}"
	)


# ===========================================================================
# (4) DISPATCH MODE-AGNOSTIC — structural assertion that the exit + settlement
# paths contain NO isinstance/mode/is_live branch (the §1 keystone: the
# live-vs-paper difference is WHICH store is wired at boot, never a per-call
# conditional). Mirrors how the sibling E tests pin the keystone structurally.
# ===========================================================================


def _branch_tests_a_mode(node: ast.AST) -> bool:
	"""True iff any If/IfExp test in ``node`` references a name/attr matching a
	mode/live/paper/isinstance discriminator — the structural shape SC-D3/§1
	forbids in the exit + settlement seam (a per-call live-vs-paper branch)."""
	_MODE_TOKENS = {
		"is_live", "live", "paper", "mode", "executor_kind", "isinstance",
		"sqlitetradestore", "inmemorytradestore", "tradestore",
	}

	class _V(ast.NodeVisitor):
		hit = False

		def _scan_test(self, test: ast.AST) -> None:
			for sub in ast.walk(test):
				if isinstance(sub, ast.Name) and sub.id.lower() in _MODE_TOKENS:
					self.hit = True
				elif (
					isinstance(sub, ast.Attribute)
					and sub.attr.lower() in _MODE_TOKENS
				):
					self.hit = True
				elif isinstance(sub, ast.Call) and isinstance(
					sub.func, ast.Name
				) and sub.func.id == "isinstance":
					self.hit = True

		def visit_If(self, n: ast.If) -> None:
			self._scan_test(n.test)
			self.generic_visit(n)

		def visit_IfExp(self, n: ast.IfExp) -> None:
			self._scan_test(n.test)
			self.generic_visit(n)

	v = _V()
	v.visit(node)
	return v.hit


def _func_ast(func) -> ast.AST:
	"""Parse a single function's source into an AST node (dedented)."""
	src = inspect.getsource(func)
	return ast.parse(src).body[0]


def test_handle_exit_has_no_mode_branch() -> None:
	"""Failure mode prevented (§1 keystone violation): a future edit adds an
	``if isinstance(store, SQLiteTradeStore)`` / ``if is_live:`` branch to
	``_handle_exit`` to special-case the live close. SC-D3/R1 is that the
	store/Protocol absorbs the difference — ``_handle_exit`` is mode-AGNOSTIC.
	Structurally asserts no If/IfExp in ``_handle_exit`` tests a mode/live/
	paper/isinstance discriminator (the legitimate ``signal.side``/``trade_id
	is None``/``blended`` branches are not mode discriminators)."""
	assert not _branch_tests_a_mode(_func_ast(_dispatch_mod._handle_exit)), (
		"dispatch._handle_exit must contain NO isinstance/mode/is_live branch "
		"— the §1 keystone: the store/Protocol absorbs the live-vs-paper "
		"difference; D3 must not (and does not) special-case the live close"
	)


def test_handle_signal_exit_dispatch_threads_executor_config_no_mode() -> None:
	"""SC-D3 (E3) end-state — REWRITTEN from the D3-era forcing-function
	``test_handle_signal_exit_dispatch_passes_no_executor_or_mode`` (the
	C6-precedent: a forcing-function test is rewritten by the PR that delivers
	the end-state it was guarding the absence of — exactly as the PR-5→PR-6
	strict-xfail twin was retired by C6). The OLD test pinned the D3-era state
	(``_handle_exit`` NOT yet executor-threaded); spec §10 SC-D3 explicitly
	defers the "place exit via executor" + executor/cfg threading TO E3, so
	the E3 end-state INVERTS that assertion.

	Failure mode prevented (SC-D3 + §1 keystone): the ``signal.action ==
	'exit'`` arm does NOT thread ``executor``/``config`` into ``_handle_exit``
	(the live exit would never be placed via the executor — funds-at-risk), OR
	it grows a mode/``isinstance`` discriminator around the exit call (§1
	violation — the executor IS the seam, not a per-call branch). Pins:
	``_handle_signal`` calls ``_handle_exit`` exactly once, passing
	``executor`` and ``config`` (the SC-D3 deliverable), passing NO ``risk``
	(exits bypass the entry gate), AND with no mode discriminator anywhere."""
	src = inspect.getsource(_dispatch_mod._handle_signal)
	tree = ast.parse(src).body[0]

	exit_calls: list[ast.Call] = []
	for node in ast.walk(tree):
		if (
			isinstance(node, ast.Call)
			and isinstance(node.func, ast.Name)
			and node.func.id == "_handle_exit"
		):
			exit_calls.append(node)
	assert len(exit_calls) == 1, (
		f"_handle_signal must call _handle_exit exactly once; got "
		f"{len(exit_calls)}"
	)
	call = exit_calls[0]
	# Collect arg names from BOTH positional and keyword forms (the exact call
	# style is an impl detail; the contract is executor+config ARE threaded).
	pos = [a.id if isinstance(a, ast.Name) else type(a).__name__
	       for a in call.args]
	kw = {k.arg for k in call.keywords}
	all_args = set(pos) | kw
	assert "executor" in all_args, (
		"the exit dispatch MUST thread `executor` into _handle_exit (SC-D3 / "
		f"spec §10 — E3's deliverable: place the exit via executor); got "
		f"positional={pos!r} kwargs={sorted(kw)!r}"
	)
	assert "config" in all_args, (
		"the exit dispatch MUST thread `config` into _handle_exit (SC-D3 — "
		f"the exit OrderRequest builder needs it); got positional={pos!r} "
		f"kwargs={sorted(kw)!r}"
	)
	# Exits bypass the entry gate (kills cap NEW exposure; they never trap
	# existing exposure — _handle_signal's own docstring) → no `risk` kwarg.
	assert "risk" not in kw, (
		f"_handle_exit must receive NO risk kwarg (exits bypass the entry "
		f"gate); got {sorted(kw)!r}"
	)
	# Still NO isinstance/mode/live/paper discriminator anywhere in
	# _handle_signal — the executor is the §1 seam, NOT a per-call branch.
	assert not _branch_tests_a_mode(tree), (
		"_handle_signal must not branch on mode/live/paper/isinstance around "
		"the exit dispatch (§1 keystone — the executor absorbs the difference)"
	)


def test_settlement_poller_store_leg_has_no_mode_branch() -> None:
	"""Failure mode prevented (§1 keystone violation): a future edit adds a
	mode/isinstance branch around the ``store.settle_trade`` /
	``store.get_trade_by_id`` leg of ``engine._settlement_poller`` to
	special-case live settlement. SC-D3/R1: that leg is UNCONDITIONAL /
	mode-agnostic (the store absorbs the live-vs-paper difference). The
	poller's only legitimate branches are on the DB ``status`` (won/lost
	counters) and the raw market ``result`` — never on store type / mode.
	Asserts no If/IfExp in ``_settlement_poller`` tests a mode discriminator
	AND that ``settle_trade`` is called unconditionally (not inside a
	mode-gated branch)."""
	poller = _func_ast(_engine_mod._settlement_poller)
	assert not _branch_tests_a_mode(poller), (
		"engine._settlement_poller must contain NO isinstance/mode/is_live "
		"branch — the store/Protocol absorbs the live-vs-paper settlement "
		"difference (§1 keystone / SC-D3)"
	)
	# settle_trade is invoked exactly once, as an unconditional store call.
	settle_calls = [
		n for n in ast.walk(poller)
		if isinstance(n, ast.Call)
		and isinstance(n.func, ast.Attribute)
		and n.func.attr == "settle_trade"
	]
	assert len(settle_calls) == 1, (
		f"_settlement_poller must call store.settle_trade exactly once "
		f"(unconditional, mode-agnostic); got {len(settle_calls)}"
	)
