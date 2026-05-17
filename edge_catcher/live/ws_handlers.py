"""Kalshi account-scope WebSocket event handlers — sub-project B / v1.6.0
PR 5, Agent 4.C.

Kalshi pushes three async events on the account scope that drive a live
``live_trades`` row through its lifecycle:

* ``fill``               — an order matched (entry fill: ``pending → open``;
  exit fill: ``open → won/lost/scratch`` full close, or a split-row partial
  exit when only M of N contracts closed).
* ``order_status``       — Kalshi rejected an order (``pending → rejected``).
* ``market_settlement``  — the market resolved at expiry (100¢ / 0¢ binary);
  every still-active row for that ticker closes, and a settlement that lands
  while an exit is in flight **supersedes** the exit.

These three coroutines are the async *shells* sub-project E's WS loop
dispatches to. They are deliberately thin: each resolves the affected
``live_trades`` row(s) and then calls 4.A's **sync** ``live.state`` write
functions over the passed :class:`sqlite3.Connection`. That is the entire
async/sync boundary — ``await`` only at the E-wired downstream callback;
every state mutation is a sub-millisecond local SQL write.

**Idempotency is NOT re-implemented here.** Every 4.A status-mutating write
is compare-and-swap by ``WHERE status IN (...)`` + a ``rowcount == 1`` check
(spec Risk #9). A duplicate WS event (Kalshi re-delivers on reconnect), an
out-of-order event, or a settlement-vs-fill race therefore resolves
correctly *because of that CAS*: the second/stale apply finds the
precondition no longer valid, the 4.A function logs a WARNING and no-ops,
and the row is never corrupted back to an earlier state. These handlers add
**no** pre-UPDATE status guard of their own — they call the 4.A transition
and trust the CAS. The matched-row lookups below are read-only; the only
mutations go through 4.A.

No deployment-specific references live here (operator-private notes own the
runtime topology); notification routing is E's / the operator's choice — a
settlement's downstream bankroll/peak effect is an *injected* async callback
(:class:`StoreCallbacks`), never an engine import.
"""
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Literal

from edge_catcher.adapters.kalshi.fees import STANDARD_FEE
from edge_catcher.engine.fill_math import FillEvent, blended_price_cents
from edge_catcher.live.state import (
	record_close,
	record_partial_exit,
	transition_pending_to_open,
	transition_pending_to_rejected,
)

log = logging.getLogger(__name__)


# Kalshi binary settlement is exactly 100¢ (the resolved side wins) or 0¢
# (the resolved side loses). Never a partial / scratch — that is a paper-side
# concept (spec §408 / transition table §243).
_SETTLED_YES_PRICE = 100
_SETTLED_NO_PRICE = 0


@dataclass(frozen=True, slots=True)
class StoreCallbacks:
	"""Downstream effects E wires; injected so the handlers never import
	engine internals (CR-6 / spec §428 / quality bar).

	Phase 1 needs exactly one: the post-settlement bankroll/peak hook. E
	composes it from C's ``BankrollCache.on_settlement`` + the peak tracker's
	``on_trade_close``; B's handler just ``await``\\ s it after the row
	closes. Kept as a frozen value object (slots) so a test can supply a
	trivial async stub and E can supply the real composite without either
	side reaching across the boundary.

	``on_settlement`` is awaited exactly once per ``on_settlement_event``
	**after** every affected row for the ticker has been closed (so a
	bankroll refresh observes the final state, not a half-applied one). A
	``None`` callback is a valid "no downstream effect wired yet" (early E
	bring-up / unit tests): the handler treats it as a no-op rather than
	forcing every caller to pass a stub.
	"""

	on_settlement: Callable[[], Awaitable[None]] | None = None


def _now_utc_iso() -> str:
	"""ISO-8601 aware-UTC now. Mirrors ``reconciliation._now_utc().isoformat()``
	and 4.A's ``placed_at_utc`` convention — ``ws_handlers`` never sources a
	bare/naive clock (zero-error lens: a naive timestamp would mis-bucket a
	close in ``read_daily_pnl_cents``' half-open UTC-day window)."""
	return datetime.now(timezone.utc).isoformat()


def _coerce_fills(raw: Any) -> list[FillEvent]:
	"""Validate Kalshi's per-fill array to the shared :class:`FillEvent`
	shape before it reaches ``fill_math``.

	Mirrors ``executors/live.py:_translate_order``'s light-touch validation
	(every element a dict with integer-ish ``price``+``size``) so a
	WS-recovered blended price is byte-consistent with the REST/dispatch
	path. A malformed/empty array yields ``[]``; the caller decides what an
	empty fills array means (an entry fill with no usable fills is treated
	as a no-op rather than fabricating a 0¢ cost basis — never lie about a
	price under the real-money lens).
	"""
	if not isinstance(raw, list):
		return []
	out: list[FillEvent] = []
	for f in raw:
		if not isinstance(f, dict) or "price" not in f or "size" not in f:
			return []
		try:
			out.append({"price": int(f["price"]), "size": int(f["size"])})
		except (TypeError, ValueError):
			return []
	return out


def _entry_fee_cents(blended_cents: int, fill_size: int) -> int:
	"""Entry/exit fee at WS-fill row-write time.

	Identical convention to ``reconciliation._resolve_matched_pending``
	(``int(round(STANDARD_FEE.calculate(price, size)))``) so a WS-path row's
	fee matches a reconcile-recovered row's fee exactly — F's P&L analytics
	must not diverge by which path booked the fill (spec §283; CR-7's
	``OrderResult.fees_cents`` will later be preferred, falling back to this).
	``calculate`` returns ceil'd cents as a float; the column is INTEGER.
	"""
	return int(round(STANDARD_FEE.calculate(blended_cents, fill_size)))


def _find_row_by_coid(
	db: sqlite3.Connection, client_order_id: str
) -> tuple[int, str, str, str] | None:
	"""``(id, status, ticker, side)`` for the row whose ``client_order_id``
	matches, or ``None``. ``client_order_id`` is ``UNIQUE`` so this is at
	most one row. Read-only (the only mutations in this module go through
	4.A's CAS writers)."""
	r = db.execute(
		"SELECT id, status, ticker, side FROM live_trades "
		"WHERE client_order_id = ?",
		(client_order_id,),
	).fetchone()
	return None if r is None else (int(r[0]), str(r[1]), str(r[2]), str(r[3]))


def _find_active_parent_for_exit(
	db: sqlite3.Connection, *, ticker: str, side: str
) -> tuple[int, int, int, int] | None:
	"""``(id, blended_entry_cents, fill_size, entry_fee_remaining_cents)`` of
	the single ``open`` row for ``ticker``+``side`` — the parent an exit fill
	closes. ``None`` when none is open (the position already closed:
	settlement won the race, or a prior exit already booked it) — the caller
	logs a no-op.

	An exit order's own ``client_order_id`` is a *fresh* idempotency key (D's
	``build_exit_order``), so an exit fill is matched to its parent by
	``ticker``+``side`` (the established row-lookup key — settlement uses the
	same, spec §402). Phase-1 single-position-per-ticker+side makes this
	unambiguous; ``ORDER BY id`` is a deterministic tiebreak only.

	``entry_fee_remaining_cents`` is carried so a FULL close subtracts the
	still-owed entry fee into ``pnl_cents`` — the DDL contract is
	``pnl = exit - entry - entry_fee - exit_fee`` and 4.A's ``record_close``
	does NOT recompute pnl (it only moves the remainder into
	``entry_fee_cents`` for audit); the caller owns the arithmetic, exactly
	as ``record_partial_exit``'s child-pnl and ``_settlement_pnl_cents`` do.
	"""
	r = db.execute(
		"SELECT id, blended_entry_cents, fill_size, "
		"COALESCE(entry_fee_remaining_cents, entry_fee_cents, 0) "
		"FROM live_trades "
		"WHERE ticker = ? AND side = ? AND status = 'open' "
		"ORDER BY id ASC",
		(ticker, side),
	).fetchone()
	if r is None:
		return None
	return int(r[0]), int(r[1]), int(r[2]), int(r[3] or 0)


def _settlement_outcome(side: str, settlement_price_cents: int) -> Literal["won", "lost"]:
	"""``won`` iff (yes-side & settled YES) or (no-side & settled NO); else
	``lost``. Binary settlement is 100¢ (YES resolved) or 0¢ (NO resolved) —
	never ``scratch`` (spec §405-408)."""
	settled_yes = settlement_price_cents >= _SETTLED_YES_PRICE
	if (side == "yes" and settled_yes) or (side == "no" and not settled_yes):
		return "won"
	return "lost"


def _settlement_pnl_cents(
	*,
	side: str,
	fill_size: int,
	blended_entry_cents: int,
	entry_fee_remaining_cents: int,
	settlement_price_cents: int,
) -> int:
	"""Realized P&L when the market settles.

	The contract pays 100¢ if the held side won, 0¢ if it lost. The exit
	"price" the position realizes is therefore the side-relative payout:
	a YES contract pays ``settlement_price``; a NO contract pays
	``100 - settlement_price`` (NO wins when YES settles 0¢). Cost basis is
	the blended entry; the entry fee still owed (the parent's remaining
	allocation — full size for a never-split row) is subtracted. Kalshi
	charges **no** fee at settlement (spec §423), so ``exit_fee = 0``.

	``pnl = size * (payout - blended_entry) - entry_fee_remaining``
	"""
	payout = (
		settlement_price_cents
		if side == "yes"
		else _SETTLED_YES_PRICE - settlement_price_cents
	)
	return fill_size * (payout - blended_entry_cents) - entry_fee_remaining_cents


# ---------------------------------------------------------------------------
# Handler shells (async — E's WS loop dispatches to these)
# ---------------------------------------------------------------------------


async def on_fill_event(
	msg: dict[str, Any],
	db: sqlite3.Connection,
	store_callbacks: StoreCallbacks,
) -> None:
	"""Kalshi ``fill`` event.

	Resolution:

	* The fill matches a ``pending`` row by ``client_order_id`` (the entry
	  order's idempotency key) → ``transition_pending_to_open`` (4.A CAS;
	  precondition ``status='pending'``). A duplicate entry-fill on a row
	  that already left ``pending`` (re-delivery after reconnect, or a
	  settlement closed it first) is matched by ``kalshi_order_id`` and the
	  4.A CAS no-ops it — *proven* by tests #22 (incl. the rowcount-0
	  lost-race where a concurrent settlement already closed the row).
	* The fill is an **exit** fill (its ``client_order_id`` matches no row,
	  but an ``open`` row exists for the ticker+side) → close the parent: a
	  full close (``filled_count >= parent.fill_size``) via ``record_close``;
	  a partial close via ``record_partial_exit`` (split row). A duplicate
	  partial WS event repeats the **same** ``kalshi_order_id`` — 4.A's
	  ``record_partial_exit`` dedups on exactly that id and no-ops (test #23).
	* No active row resolvable (settlement already closed it, etc.) → a
	  logged no-op; never a blind write (spec Risk #4).

	``store_callbacks`` is part of the shared handler signature (spec §726);
	a fill has no downstream bankroll effect until the position *closes*, so
	it is unused for the entry path and consumed only via ``record_close`` /
	``record_partial_exit`` on the exit path (the settlement handler owns the
	bankroll callback). ``RecordPendingFailed`` from 4.A is never caught here
	— it must propagate (ghost-reject defense, cross-PR contract #4).
	"""
	client_order_id = str(msg.get("client_order_id") or "")
	kalshi_order_id = str(msg.get("order_id") or "")
	filled_count = int(msg.get("filled_count") or 0)
	fills = _coerce_fills(msg.get("fills"))

	# --- Entry vs exit discriminator (spec §726 "match by client_order_id").
	# An ENTRY order's client_order_id IS the row's client_order_id (D's
	# _make_client_order_id produced it; record_pending/record_open stored
	# it). An EXIT order's client_order_id is a FRESH idempotency key (D's
	# build_exit_order) that matches NO row. Therefore:
	#   coid matches a row  → ENTRY fill for that row.
	#   coid matches no row → EXIT fill (resolve parent by ticker+side).
	# This single rule makes a duplicate entry-fill (row already 'open') and
	# a fill-after-settlement (row already terminal) both flow through 4.A's
	# transition_pending_to_open whose WHERE status='pending' CAS no-ops
	# them — never misrouting a re-delivered entry into the exit/close path.
	matched = (
		_find_row_by_coid(db, client_order_id) if client_order_id else None
	)

	if matched is not None:
		row_id, status, ticker, side = matched
		if status == "pending" and (not fills or filled_count <= 0):
			# A first-delivery entry fill with no usable fills array cannot
			# yield a trustworthy blended cost basis. Do NOT fabricate 0¢
			# (zero-error lens) — leave the pending row for B's reconciler to
			# resolve by client_order_id (same policy as
			# LiveExecutor._translate_order's malformed-fills → pending).
			log.warning(
				"on_fill_event: pending row id=%d coid=%s has "
				"filled_count=%d / %d fills — no trustworthy blended price; "
				"leaving pending for reconcile",
				row_id,
				client_order_id,
				filled_count,
				len(fills),
			)
			return
		blended = blended_price_cents(fills) if fills else 0
		entry_fee = _entry_fee_cents(blended, filled_count) if fills else 0
		if status == "pending":
			log.info(
				"on_fill_event: ENTRY fill row id=%d ticker=%s coid=%s "
				"pending→open filled=%d blended=%dc",
				row_id,
				ticker,
				client_order_id,
				filled_count,
				blended,
			)
		else:
			# Row already left 'pending' (duplicate/late entry fill after a
			# fill opened it, or after a concurrent settlement closed it).
			# Driven through the SAME 4.A CAS — its WHERE status='pending'
			# yields rowcount-0 → logged no-op (idempotent; Risk #9 /
			# Risk #2). Never corrupts the row back to 'open'.
			log.info(
				"on_fill_event: duplicate/late entry fill for row id=%d "
				"ticker=%s status=%s coid=%s — 4.A CAS will no-op "
				"(idempotent)",
				row_id,
				ticker,
				status,
				client_order_id,
			)
		# slippage is left 0: the entry order's limit is not on the fill
		# event; the dispatch/REST path records signed slippage at placement.
		# A fabricated value here would diverge from that path (cross-PR
		# contract #1 — never roll our own).
		transition_pending_to_open(
			db,
			row_id,
			kalshi_order_id=kalshi_order_id or "",
			fill_size=filled_count,
			blended_entry_cents=blended,
			slippage_cents=0,
			fill_pct=1.0,
			entry_time=_now_utc_iso(),
			entry_fee_cents=entry_fee,
		)
		return

	# --- Exit-fill path: coid matched no row → this is an exit order's fill.
	# Resolve the parent open row by ticker+side (the established lookup key;
	# settlement uses the same — spec §402).
	ticker = str(msg.get("ticker") or "")
	side = str(msg.get("side") or "")
	if not ticker or not side:
		log.warning(
			"on_fill_event: unmatched fill (coid=%s kalshi_id=%s) with no "
			"ticker/side — cannot resolve a parent; no-op",
			client_order_id,
			kalshi_order_id,
		)
		return

	parent = _find_active_parent_for_exit(db, ticker=ticker, side=side)
	if parent is None:
		# No open parent: the position already closed (settlement won the
		# race, or a prior exit fill already booked it). Idempotent no-op
		# (spec Risk #4 — a late exit fill for a now-closed row is dropped,
		# never blind-written). Exercised by test #24's "settlement then
		# late fill" variant.
		log.info(
			"on_fill_event: exit fill for %s/%s but no open parent row "
			"(already closed — settlement/prior-exit won the race); no-op "
			"(idempotent)",
			ticker,
			side,
		)
		return

	parent_id, parent_blended, parent_fill_size, parent_fee_remaining = parent
	if not fills or filled_count <= 0:
		log.warning(
			"on_fill_event: exit fill for parent id=%d %s/%s has "
			"filled_count=%d / %d fills — no trustworthy exit price; no-op "
			"(reconcile/next-tick retry owns recovery)",
			parent_id,
			ticker,
			side,
			filled_count,
			len(fills),
		)
		return

	exit_blended = blended_price_cents(fills)
	closed = min(filled_count, parent_fill_size)
	if closed >= parent_fill_size:
		# Full close. Outcome vs the parent's blended entry (scratch only
		# when exactly equal pre-fee; fees push a scratch to pnl<=0 — the
		# same rule 4.A's record_partial_exit applies).
		if exit_blended > parent_blended:
			outcome: Literal["won", "lost", "scratch"] = "won"
		elif exit_blended < parent_blended:
			outcome = "lost"
		else:
			outcome = "scratch"
		exit_fee = _entry_fee_cents(exit_blended, closed)
		# DDL contract: pnl = exit - entry - entry_fee - exit_fee. 4.A's
		# record_close moves the remaining entry fee into entry_fee_cents
		# for audit but does NOT recompute pnl — the caller owns the
		# arithmetic (same as record_partial_exit's child-pnl and
		# _settlement_pnl_cents). For a never-split open row this remainder
		# is the full entry fee; for a previously-partially-exited parent it
		# is exactly what is still owed.
		pnl = (
			closed * (exit_blended - parent_blended)
			- parent_fee_remaining
			- exit_fee
		)
		log.info(
			"on_fill_event: EXIT fill (full) parent id=%d ticker=%s "
			"%s→%s closed=%d exit=%dc pnl=%dc",
			parent_id,
			ticker,
			"open",
			outcome,
			closed,
			exit_blended,
			pnl,
		)
		# record_close consumes the parent's remaining entry-fee allocation
		# into this close's entry_fee_cents (no rounding fragment lost) and
		# CAS-guards on status IN ('open','exit_pending'); a settlement that
		# closed the row first → rowcount-0 logged no-op (Risk #9).
		record_close(
			db,
			parent_id,
			status=outcome,
			exit_price_cents=exit_blended,
			exit_time=_now_utc_iso(),
			exit_reason="ws_exit_fill",
			pnl_cents=pnl,
			exit_fee_cents=exit_fee,
		)
		return

	# Partial close → split row. 4.A's record_partial_exit dedups on
	# kalshi_exit_order_id (a duplicate WS event repeats the SAME id) and is
	# atomic (parent decrement + child INSERT in one txn). A bad/duplicate
	# event driving closed_size out of bounds is rejected inside 4.A. Test
	# #23 drives both the split and the same-id duplicate no-op.
	exit_fee = _entry_fee_cents(exit_blended, closed)
	log.info(
		"on_fill_event: EXIT fill (partial) parent id=%d ticker=%s closed=%d "
		"of %d exit=%dc kalshi_exit=%s — split row",
		parent_id,
		ticker,
		closed,
		parent_fill_size,
		exit_blended,
		kalshi_order_id,
	)
	record_partial_exit(
		db,
		parent_id,
		closed_size=closed,
		exit_price_cents=exit_blended,
		exit_reason="ws_exit_fill",
		now_utc=_now_utc_iso(),
		exit_fee_cents=exit_fee,
		kalshi_exit_order_id=kalshi_order_id,
	)


async def on_order_status_event(
	msg: dict[str, Any],
	db: sqlite3.Connection,
	store_callbacks: StoreCallbacks,
) -> None:
	"""Kalshi ``order_status`` event reporting a **rejection**.

	Kalshi rejected the order at its level (e.g. a post-submit validation
	failure Kalshi surfaces asynchronously rather than as a place-time 4xx).
	The matched local row is driven ``pending → rejected`` via 4.A's
	``transition_pending_to_rejected`` (CAS precondition ``status='pending'``).

	The lost-race that proves Risk #9 here: the order *filled* first (a
	``fill`` event already moved the row ``pending → open``) and the
	rejection event arrives stale — the CAS precondition ``status='pending'``
	is no longer met, 4.A logs the lost-race WARNING and no-ops, the row
	stays ``open`` (NOT corrupted to ``rejected``). Driven by test #25's
	lost-race variant.

	Only ``rejected``/``canceled``-flavoured statuses are actioned; any
	other status string (a benign lifecycle ping) is a logged no-op — the
	authoritative fill/settlement transitions own the happy path. No
	``RecordPendingFailed`` is raised by this terminal transition (the row
	exists; no Kalshi-side position to strand).
	"""
	client_order_id = str(msg.get("client_order_id") or "")
	status = str(msg.get("status") or "")
	rejection_reason = msg.get("rejection_reason")
	kalshi_order_id = msg.get("order_id")

	if not client_order_id:
		log.warning(
			"on_order_status_event: event with no client_order_id "
			"(status=%s) — cannot resolve a row; no-op",
			status,
		)
		return

	# Only a rejection/cancel is actionable here. Anything else (a resting/
	# executed lifecycle ping) is owned by the fill handler — no-op.
	if status not in ("rejected", "canceled", "cancelled"):
		log.info(
			"on_order_status_event: non-terminal status=%s coid=%s — no-op "
			"(fill/settlement own the active path)",
			status,
			client_order_id,
		)
		return

	matched = _find_row_by_coid(db, client_order_id)
	if matched is None:
		log.warning(
			"on_order_status_event: no row for coid=%s (status=%s) — no-op",
			client_order_id,
			status,
		)
		return

	row_id, row_status, ticker, _side = matched
	log.info(
		"on_order_status_event: Kalshi %s for row id=%d ticker=%s coid=%s "
		"(local status=%s) — driving pending→rejected via 4.A CAS",
		status,
		row_id,
		ticker,
		client_order_id,
		row_status,
	)
	# Unconditional 4.A call — its WHERE status='pending' CAS is the guard.
	# If a fill already moved the row to 'open', rowcount-0 → 4.A logs the
	# lost-race WARNING and no-ops (Risk #9; test #25 lost-race variant).
	transition_pending_to_rejected(
		db,
		row_id,
		kalshi_order_id=str(kalshi_order_id) if kalshi_order_id else None,
		rejection_reason=(
			str(rejection_reason)
			if rejection_reason
			else f"kalshi_order_status:{status}"
		),
	)


async def on_settlement_event(
	msg: dict[str, Any],
	db: sqlite3.Connection,
	store_callbacks: StoreCallbacks,
) -> None:
	"""Kalshi ``market_settlement`` event — the market resolved at expiry.

	Closes **every** still-active row for the settled ticker:
	``status IN ('open', 'exit_pending')``. Settlement price is 100¢ (the
	market resolved YES) or 0¢ (resolved NO); the per-row outcome is
	``won``/``lost`` by side (binary — never ``scratch``, spec §405-408).

	**Settlement supersedes an in-flight exit (spec §431):** a row in
	``exit_pending`` (D's NetworkError on the exit POST) still closes at the
	settlement price — the exit attempt is moot. The row's ``notes`` records
	``"settlement superseded in-flight exit"`` and the supersede is logged
	for audit. ``record_close``'s CAS precondition is
	``status IN ('open','exit_pending')`` so both active states are handled
	by the one writer; a row a concurrent fill closed first → rowcount-0
	logged no-op (Risk #9 — test #24's lost-race variant).

	After every affected row is closed, E's wired bankroll/peak callback is
	awaited exactly once (so a balance refresh observes the final, fully
	settled state). ``RecordPendingFailed`` is not raised by ``record_close``
	(terminal transition, no Kalshi-side position to strand);
	``asyncio.CancelledError`` from the awaited callback is never caught
	(clean shutdown must propagate).
	"""
	ticker = str(msg.get("ticker") or "")
	if not ticker:
		log.warning(
			"on_settlement_event: settlement event with no ticker — no-op"
		)
		return
	try:
		settlement_price = int(msg["settlement_price"])
	except (KeyError, TypeError, ValueError):
		log.error(
			"on_settlement_event: settlement for %s has missing/invalid "
			"settlement_price=%r — no-op (cannot close rows without the "
			"resolved price; reconcile/operator owns recovery)",
			ticker,
			msg.get("settlement_price"),
		)
		return
	if settlement_price not in (_SETTLED_NO_PRICE, _SETTLED_YES_PRICE):
		# Binary markets settle exactly 0¢/100¢. An out-of-band value is a
		# data-quality anomaly — refuse to fabricate P&L off it (zero-error
		# lens). Operator/reconcile investigates.
		log.error(
			"on_settlement_event: %s settlement_price=%d is not binary "
			"(0/100) — no-op; data-quality anomaly, manual investigation",
			ticker,
			settlement_price,
		)
		return

	rows = db.execute(
		"SELECT id, side, fill_size, blended_entry_cents, "
		"COALESCE(entry_fee_remaining_cents, entry_fee_cents, 0), status "
		"FROM live_trades "
		"WHERE ticker = ? AND status IN ('open', 'exit_pending') "
		"ORDER BY id ASC",
		(ticker,),
	).fetchall()

	if not rows:
		log.info(
			"on_settlement_event: %s settled @ %dc but no active "
			"(open/exit_pending) rows — no-op",
			ticker,
			settlement_price,
		)
		# Still fire the downstream callback: a settlement with no local
		# rows is a valid steady state (positions already closed); E's
		# bankroll refresh is harmless and keeps the contract simple
		# (callback fires once per settlement event, unconditionally).
		await _fire_settlement_callback(store_callbacks, ticker)
		return

	closed_ids: list[int] = []
	for row in rows:
		row_id = int(row[0])
		side = str(row[1])
		fill_size = int(row[2])
		blended_entry = int(row[3] or 0)
		entry_fee_remaining = int(row[4] or 0)
		row_status = str(row[5])

		outcome = _settlement_outcome(side, settlement_price)
		pnl = _settlement_pnl_cents(
			side=side,
			fill_size=fill_size,
			blended_entry_cents=blended_entry,
			entry_fee_remaining_cents=entry_fee_remaining,
			settlement_price_cents=settlement_price,
		)
		notes = None
		if row_status == "exit_pending":
			notes = "settlement superseded in-flight exit"
			log.info(
				"on_settlement_event: settlement supersedes exit_pending for "
				"%s id=%d (exit attempt moot — closing at settlement price)",
				ticker,
				row_id,
			)
		log.info(
			"on_settlement_event: %s id=%d %s→%s settled=%dc pnl=%dc "
			"(exit_reason=settlement, exit_fee=0)",
			ticker,
			row_id,
			row_status,
			outcome,
			settlement_price,
			pnl,
		)
		# record_close CAS-guards status IN ('open','exit_pending'); a row a
		# concurrent fill closed first → rowcount-0 logged no-op (Risk #9).
		# Kalshi charges no fee at settlement → exit_fee_cents=0 (spec §423).
		record_close(
			db,
			row_id,
			status=outcome,
			exit_price_cents=settlement_price,
			exit_time=_now_utc_iso(),
			exit_reason="settlement",
			pnl_cents=pnl,
			exit_fee_cents=0,
			notes=notes,
		)
		closed_ids.append(row_id)

	log.info(
		"on_settlement_event: %s settled @ %dc — closed %d row(s): %s",
		ticker,
		settlement_price,
		len(closed_ids),
		closed_ids,
	)
	# Downstream bankroll/peak effect, awaited AFTER every row is closed so a
	# refresh observes the final settled state (spec §428).
	await _fire_settlement_callback(store_callbacks, ticker)


async def _fire_settlement_callback(
	store_callbacks: StoreCallbacks, ticker: str
) -> None:
	"""Await E's wired post-settlement callback exactly once, if present.

	A ``None`` callback (early E bring-up / unit tests) is a valid no-op —
	B never forces a stub on the caller. ``asyncio.CancelledError`` from the
	callback propagates (clean shutdown); any other exception is logged at
	ERROR and swallowed: a bankroll-refresh failure must NOT roll back the
	settlement rows that are already durably closed (the rows are the truth
	of record; a stale cash cache self-heals on the next refresh, mirroring
	the live.client audit-write fault-isolation precedent).
	"""
	cb = store_callbacks.on_settlement
	if cb is None:
		return
	try:
		await cb()
	except Exception:
		log.exception(
			"on_settlement_event: downstream settlement callback failed for "
			"%s — settlement rows remain durably closed (truth of record); "
			"bankroll cache self-heals on next refresh",
			ticker,
		)
