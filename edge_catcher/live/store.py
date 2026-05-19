"""``SQLiteTradeStore`` — the live-only ``TradeStoreProtocol`` adapter.

Owned by sub-project B (v1.6.0 PR 5). This is the thin, stateful
connection-holding bridge between the engine's *structural*
``engine.trade_store.TradeStoreProtocol`` (what ``engine/dispatch.py`` calls)
and B's *pure-function* ``live.state`` writers over ``live_trades.db``.

``live.state`` is deliberately stateless free functions
(``record_pending(conn, *, ...)``) so the reconciler and WS handlers can call
them over whatever connection sub-project E owns. Dispatch, however, is written
against an *object* with methods. This class is the seam: it holds ONE
``sqlite3.Connection`` to ``live_trades.db`` (WAL + the ``0003`` migration
applied at construction via 4.A's ``connect_live_trades_db`` helper) and
delegates each Protocol method dispatch reaches on the LIVE path to the
matching ``live.state`` / ``engine.live_db`` function.

**Live-path surface (only what ``engine/dispatch.py`` invokes when the
executor is ``LiveExecutor`` — statuses rejected / pending):**

* ``record_rejected`` → CAS ``pending → rejected`` of the C1 row via
  :func:`live.state.transition_pending_to_rejected` (rejected branch,
  non-``stale_book`` only; ``stale_book`` is the paper-side reject path that
  ``dispatch.py`` short-circuits before the store). NOT an insert — spec §3
  supersedes B's CR-4 insert-on-outcome model; a CAS-miss / write failure is
  a logged ERROR audit gap, NOT fatal (§3.1: a rejected order holds no
  position ⇒ not funds-at-risk).
* ``record_pending`` → :func:`live.state.record_pending` (pending branch —
  NetworkError / malformed-fills / engine-timeout).
* ``get_open_trades`` / ``get_open_trades_for`` → ``live_trades`` open-row
  reads (dispatch builds ``TickContext.open_positions`` so live strategies
  can see their positions and emit exit Signals).
* ``close`` → close the held connection (idempotent).

So this adapter's live WRITE responsibility is **intent / pending /
rejected persistence + the filled-entry CAS transition + open-row reads**.
The post-fill lifecycle (exit / partial-exit / settlement / strategy
state) is still NOT this adapter's job (see ``PR-5 → PR-6 (E) CONTRACT``).

**``record_trade`` (E / C2) — the LIVE filled-entry write is a CAS
``pending → open`` TRANSITION of the C1 row, NOT an insert** (spec §3
``:400 filled`` row / §4.2 / §5). The entry model is
insert-pending-then-CAS-transition: C1's ``record_intent`` durably INSERTs
the ``pending`` row keyed by ``client_order_id`` *before* ``place()``;
dispatch's filled branch then calls ``record_trade(...)`` UNCONDITIONALLY
(it must never branch on paper-vs-live — spec §1 keystone). On the live
store ``record_trade`` locates that C1 row by ``client_order_id`` (B's
canonical lookup) and CAS-transitions it to ``open`` via
:func:`live.state.transition_pending_to_open` — exactly one row remains,
now ``status='open'`` with the real ``kalshi_order_id`` set and the fill
fields populated. ``client_order_id`` / ``kalshi_order_id`` reach it as
additive keyword-only Protocol args (paper / in-memory accept-and-ignore
them so their behaviour is byte-identical — G-parity-guarded; the live
store consumes them). No synthesized ids, no competing INSERT (§4.2): the
funds-at-risk row 4.B's reconciler / ``on_fill_event`` / phantom-pending
poller key off is the SAME C1 row, now transitioned.

**Post-fill lifecycle methods still deliberately NOT implemented**
(``settle_trade``, ``exit_trade``, ``get_trade_by_id``, ``save_state``,
``load_state``, ``load_all_states``): they raise
:class:`NotImplementedError` with an explanatory message rather than
silently no-op into a wrong real-money result. Rationale — on the live
path the post-fill lifecycle (exit → partial-exit → settlement / close) is
driven by 4.C's **WS handlers** + 4.B's **reconciliation** calling
``live.state``'s CAS-guarded ``transition_* / record_close /
record_partial_exit`` functions *directly against ``live_trades.db``*, NOT
through this store. Specifically:

* ``exit_trade`` / ``settle_trade`` — paper computes P&L in-store with a
  single ``status='open' → won/lost`` UPDATE on a ``paper_trades`` schema;
  the live equivalent is a CAS ``won/lost/scratch`` close with entry-fee
  remainder consumption keyed off the Kalshi exit fill — no faithful 1:1
  mapping, so a silent no-op here would be a real-money correctness hole
  (e.g. a strategy exit Signal silently not closing a live position).
* strategy state likewise lives in ``live_trades.db`` (rehydrated by the
  reconciler), not in a store-owned ``strategy_state`` table.

E wires the live engine so these are unreachable; the loud
``NotImplementedError`` is the fail-loud guard if a wiring change ever
routes one here before E's rewire lands.

----------------------------------------------------------------------------
**PR-5 → PR-6 (E) CONTRACT — read before wiring this store into a live run.**
----------------------------------------------------------------------------

``SQLiteTradeStore`` is the live **intent / pending / rejected persistence
+ filled-entry CAS + open-row read** boundary. As shipped in PR 5 the
merged ``engine/dispatch.py`` had **no live-vs-paper branching**:
``_handle_signal`` routes every exit Signal to ``_handle_exit``, which
unconditionally calls ``store.exit_trade(...)`` then
``store.get_trade_by_id(...)``; the filled branch unconditionally calls
``store.record_trade(...)``. E (C2) makes ``record_trade`` live-correct (a
CAS ``pending → open`` transition — above); the exit / settlement arms are
still **fail-loud** against this adapter until E's later phases redirect
them.

Therefore, **before any live run, E (PR 6) MUST also rewire dispatch** so
that, when the executor is ``LiveExecutor``:

(a) **filled-entry branch** → ``store.record_trade(...)`` now CAS-
    transitions the C1 ``pending`` row to ``open`` via
    :func:`live.state.transition_pending_to_open` with D's real
    ``kalshi_order_id`` (passed as the additive keyword-only arg) —
    DONE (E / C2). (``LiveExecutor.place`` returns ``status="filled"``
    synchronously for Kalshi IOC, so this is the common live entry path,
    not an edge case.)
(b) **exit Signal path** → route through D's executor → B's
    ``exit_pending`` / ``record_close`` / ``record_partial_exit`` against
    ``live_trades.db`` — NOT paper ``store.exit_trade`` /
    ``store.get_trade_by_id``. (still pending — later E phase)
(c) **settlement path** → B's settlement handler (CAS ``won/lost/scratch``
    close with entry-fee-remainder consumption) — NOT paper
    ``store.settle_trade``. (still pending — later E phase)

Until (b)/(c) land, ``exit_trade`` / ``get_trade_by_id`` /
``settle_trade`` are **deliberately fail-loud** so wiring this adapter
into a live engine without the rewire fails immediately and loudly rather
than silently not-closing a real-money position. The
``tests/test_live_store.py`` PR-5→PR-6 contract test pair tracks this
gap closing across E's phases (the strict-xfail twin XPASSes — CI-fail by
design — and the green-guard flips as the fail-loud methods are
implemented; both are retired in E's dedicated test-cleanup phase, which
also rewrites them to assert the implemented behaviour).

🚨 **Real-money invariant — ``RecordPendingFailed`` MUST propagate.**
``record_pending`` is the funds-at-risk INSERT this adapter performs on the
live path: a failed INSERT means a Kalshi-side order is stranded with no
local row for B's reconciler to find. ``live.state.record_pending`` raises
:class:`RecordPendingFailed` in that case; this adapter does **pure
delegation with no try/except around the call**, so the exception propagates
uncaught — which is what the three ``except RecordPendingFailed: raise``
ghost-reject clauses in ``dispatch.process_tick`` / ``engine._ws_loop`` /
``engine`` outer reconnect depend on to halt the engine.
(:func:`live.state.record_open` raises the SAME exception for the same
reason on the filled-entry INSERT, but that call is made by E's PR-6 wiring
directly against ``live.state`` — NOT through this adapter — so the
ghost-reject contract there is E's to preserve, not this store's.) The
``record_rejected`` audit-write best-effort carve-out (a failed
``pending → rejected`` CAS, or a CAS-miss because the C-gate rejected the
order *before* C1 inserted a pending row, strands at most an audit gap —
no Kalshi position, no money) is **caller-owned here, not inherited**: B's
``transition_pending_to_rejected`` owns only a lost-CAS-race WARNING no-op
(not a write-failure carve-out), so ``record_rejected`` wraps the whole
locate+CAS in its own ``try/except`` that logs ERROR and does NOT raise
(mirroring the PR#34 ``438d843`` precedent). This is a converged, locked
§3.1 tradeoff that partially supersedes B's CR-4 audit-completeness for the
positionless-rejected case — INTENTIONAL, not a regression.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from edge_catcher.adapters.kalshi.fees import STANDARD_FEE
from edge_catcher.live.state import (
	connect_live_trades_db,
	record_pending,
	transition_pending_to_open,
	transition_pending_to_rejected,
)

log = logging.getLogger(__name__)

# Columns selected for the paper-shaped open-trade dicts dispatch +
# strategies consume. The keys mirror engine.trade_store._row_to_dict's
# 15-column open-trade shape so a live strategy reading
# TickContext.open_positions sees the same dict keys it would under paper
# (id / ticker / entry_price / strategy / side / series_ticker /
# entry_fee_cents / intended_size / fill_size / blended_entry / fill_pct /
# slippage_cents / status / entry_time). live_trades' cent-suffixed columns
# (entry_price_cents / blended_entry_cents) are aliased to the paper names so
# strategy code stays venue/store agnostic; book_depth is absent in the live
# schema (live entries are IOC fills, not book walks) so it is reported as
# None to keep the shape stable.
_OPEN_ROW_SQL = (
	"SELECT id, ticker, entry_price_cents, strategy, side, series, "
	"entry_fee_cents, intended_size, fill_size, blended_entry_cents, "
	"fill_pct, slippage_cents, status, entry_time "
	"FROM live_trades WHERE status = 'open'"
)


def _backfill_pending_kalshi_order_id(
	conn: sqlite3.Connection, *, row_id: int, kalshi_order_id: str
) -> bool:
	"""Targeted, CAS-guarded ``kalshi_order_id`` backfill on a still-``pending``
	row (C4 / spec §3 / §3.1 / §5).

	B's ``live.state`` deliberately ships NO ``kalshi_order_id``-only backfill
	writer: its CAS writers either move OUT of ``pending``
	(``transition_pending_to_open`` → ``open`` and sets the fill fields;
	``transition_pending_to_rejected`` → ``rejected``/``rejected_post_hoc``,
	terminal). The executor-pending branch needs the row to STAY ``pending``
	(fill state is still UNKNOWN; B's reconciler resolves it later via
	``client_order_id``), with only ``kalshi_order_id`` learned. C4's task
	contract explicitly sanctions "a single targeted guarded UPDATE on the
	located row" as THE documented backfill mechanism when B has no writer.

	This is that single UPDATE, mirroring B's canonical CAS-predicate idiom
	verbatim (``WHERE id = ? AND status = '<precondition>'`` — identical shape
	to ``transition_pending_to_open`` / ``touch_reconciled`` in
	``live.state``): the ``status = 'pending'`` predicate makes it a
	compare-and-swap, so a row that concurrently left ``pending`` (B's
	reconciler / a fill landed) is an idempotent no-op (``rowcount == 0``),
	never a blind clobber of a transitioned row. ``B._cas_update`` is a private
	module helper not exported to this module; replicating its one-line
	``rowcount``-check here (rather than importing a private symbol) keeps the
	store↔state seam clean and is exactly the sanctioned "single targeted
	UPDATE". A module-level function (not an inline ``self._conn`` UPDATE) so
	C4's failure test can monkeypatch it at the ``edge_catcher.live.store``
	namespace it is resolved from (C1's stale-binding lesson).

	Returns ``True`` when the CAS won (the still-``pending`` row was
	backfilled), ``False`` when it lost the race (row no longer ``pending`` or
	absent) — the caller treats ``False`` as a benign idempotent no-op (the
	row already moved on; B's reconciler owns it). Never raises on a lost CAS;
	a genuine ``sqlite3.Error`` (disk/DB fault) propagates to the caller's
	§3.1 best-effort ``try/except`` (NOT fatal there — the durable pending row
	already exists from C1).
	"""
	if not kalshi_order_id:
		# Self-guard so the helper is safe even if a future caller forgets
		# the caller-side `if kalshi_order_id:` (defense-in-depth, both stay):
		# never run `SET kalshi_order_id = NULL/''` (would null out an id C1
		# or a prior call set). Makes the `kalshi_order_id: str` annotation
		# honest. A benign no-op (treated like a lost CAS by the caller).
		return False
	cur = conn.execute(
		"UPDATE live_trades SET kalshi_order_id = ? "
		"WHERE id = ? AND status = 'pending'",
		(kalshi_order_id, row_id),
	)
	conn.commit()
	return cur.rowcount == 1


def _open_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
	"""Map a live_trades open row to the paper open-trade dict shape.

	Keeps live strategy code shape-compatible with paper (see
	``engine.trade_store._row_to_dict``). ``book_depth`` is always ``None``
	(no book-walk concept for live IOC fills) — present so the key set is
	stable across stores.
	"""
	return {
		"id": row["id"],
		"ticker": row["ticker"],
		"entry_price": row["entry_price_cents"],
		"strategy": row["strategy"],
		"side": row["side"],
		"series_ticker": row["series"],
		"entry_fee_cents": row["entry_fee_cents"],
		"intended_size": row["intended_size"],
		"fill_size": row["fill_size"],
		"blended_entry": row["blended_entry_cents"],
		"book_depth": None,
		"fill_pct": row["fill_pct"],
		"slippage_cents": row["slippage_cents"],
		"status": row["status"],
		"entry_time": row["entry_time"],
	}


class SQLiteTradeStore:
	"""Live-only ``TradeStoreProtocol`` adapter backed by ``live_trades.db``.

	Structurally satisfies ``engine.trade_store.TradeStoreProtocol`` (nominal
	Protocol — no inheritance). Construction mirrors the paper ``TradeStore``
	(``__init__(db_path)``): it opens / migrates ``live_trades.db`` via 4.A's
	``connect_live_trades_db`` (WAL + ``0003``) and owns that single
	connection for its lifetime. ``check_same_thread=False`` is inherited from
	``connect_live_trades_db`` (matches paper ``TradeStore``'s choice). The
	caller (sub-project E) is responsible for calling :meth:`close` on
	shutdown.
	"""

	def __init__(self, db_path: Path) -> None:
		"""Open + migrate ``live_trades.db`` and hold the connection.

		Mirrors paper ``TradeStore.__init__(db_path)`` so E constructs the
		live store the same way it constructs the paper store, swapping only
		the class + db path. The WAL pragma + ``0003`` migration are applied
		by ``connect_live_trades_db`` (idempotent — re-running against an
		already-migrated DB is a no-op beyond the pragma).
		"""
		self._conn: sqlite3.Connection = connect_live_trades_db(db_path)
		self._closed = False

	# -------------------------------------------------------------------------
	# Live-path WRITE surface — delegate to live.state free functions
	# -------------------------------------------------------------------------

	def record_intent(
		self,
		*,
		ticker: str,
		series: str,
		strategy: str,
		side: str,
		intended_size: int,
		entry_price_cents: Optional[int],
		stop_loss_distance_cents: Optional[int],
		client_order_id: str,
		placed_at_utc: str,
	) -> None:
		"""LIVE pre-place durability hook (spec §3 / §3.1 / §4.2).

		Dispatch (E's later wiring) calls this UNCONDITIONALLY immediately
		BEFORE ``await executor.place(req)``. On the live store it durably
		INSERTs a ``pending`` row keyed by ``client_order_id`` BEFORE any
		order is sent, so a severed place→persist is recoverable by B's
		reconciler (it discriminates by ``client_order_id`` via Kalshi truth)
		and there is never an untracked real-money position. An un-sent
		order's row is indistinguishable-to-recovery from a never-received
		one — both TTL-expire safely — so a pre-place INSERT preceding the
		order is strictly safe (spec §4.2).

		Pure delegation to :func:`live.state.record_pending` over the held
		connection with ``kalshi_order_id=None`` (no order placed yet) and
		``rejection_reason=None`` (no rejection — this is the intent, not a
		terminal outcome). The 9-kwarg signature matches
		``TradeStoreProtocol.record_intent`` verbatim; the post-place outcome
		(open / rejected / pending-on-failure) is a later CAS transition on
		THIS row, not this method's concern.

		🚨 §3.1 NORMATIVE — FATAL on failure. ``live.state.record_pending``
		raises :class:`RecordPendingFailed` (chained from the underlying
		``sqlite3.Error``) on INSERT failure. There is intentionally **no**
		try/except around this call: the exception propagates UNCAUGHT so the
		entry aborts BEFORE ``place()`` (safe by construction — nothing was
		sent, no money at risk) and the engine's three
		``except RecordPendingFailed: raise`` ghost-reject clauses
		(``dispatch.process_tick`` / ``engine._ws_loop`` / ``engine`` outer
		reconnect) halt the engine rather than swallowing a failed
		pre-place persistence.
		"""
		record_pending(
			self._conn,
			ticker=ticker,
			series=series,
			strategy=strategy,
			side=side,  # type: ignore[arg-type]  # Protocol widens to str; live.state narrows to Literal["yes","no"] — value validated upstream (OrderRequest.side cast in dispatch); the side CHECK constraint is the runtime backstop
			intended_size=intended_size,
			entry_price_cents=entry_price_cents,
			stop_loss_distance_cents=stop_loss_distance_cents,
			client_order_id=client_order_id,
			kalshi_order_id=None,
			placed_at_utc=placed_at_utc,
			rejection_reason=None,
		)

	def record_pending(
		self,
		*,
		ticker: str,
		series: str,
		strategy: str,
		side: str,
		intended_size: int,
		entry_price_cents: Optional[int],
		stop_loss_distance_cents: Optional[int],
		client_order_id: str,
		kalshi_order_id: Optional[str],
		placed_at_utc: str,
		rejection_reason: Optional[str],
	) -> None:
		"""LIVE executor-pending write — an idempotent ``kalshi_order_id``
		BACKFILL on the C1 ``pending`` row located by ``client_order_id``,
		**NOT a 2nd insert** (spec §3 EXPLICITLY supersedes B's CR-4
		insert-on-outcome / one-row-per-attempt model: "the live entry model is
		insert-pending-then-CAS-transition, NOT insert-on-outcome").

		Dispatch's executor-pending branch (``dispatch.py:478`` — fires on
		D's NetworkError / timeout / malformed-fills OrderResult; the order MAY
		be live on Kalshi, fill state UNKNOWN) calls this UNCONDITIONALLY — it
		must never branch on paper-vs-live (spec §1 keystone) — so the
		paper-shaped Protocol signature is preserved verbatim (the locked
		11-kwarg signature; pinned by
		``tests/test_engine_dispatch_pending_branch.py``). The durable
		``pending`` row ALREADY EXISTS from C1's ``record_intent`` (INSERTed
		pre-place, keyed by ``client_order_id``); this method only learns the
		``kalshi_order_id`` (a NetworkError/timeout OrderResult may still carry
		one) and keeps ``status='pending'`` (still unknown — B's reconciler
		resolves it later via ``client_order_id``).

		Flow:

		1. Locate the C1 ``pending`` row by ``client_order_id`` using B's
		   canonical lookup query (the same ``WHERE client_order_id = ?``
		   SELECT ``live.state`` itself / the reconciler / ``record_trade`` /
		   ``record_rejected`` use — NOT hand-rolled SQL; §5). The select is
		   extended to ``SELECT id, status`` so the pre-state rides the SAME
		   round-trip. ``client_order_id`` is ``UNIQUE`` ⇒ at most one row.
		2. If a non-empty ``kalshi_order_id`` is now known, BACKFILL it onto
		   that row via :func:`_backfill_pending_kalshi_order_id` (the single
		   sanctioned targeted CAS UPDATE — B ships no ``kalshi_order_id``-only
		   backfill writer; ``status='pending'`` CAS predicate ⇒ idempotent,
		   no clobber of a row that concurrently transitioned). A NULL/empty id
		   (pure NetworkError, no id returned) is left as-is — never null out
		   an id C1 or a prior call already set (idempotent).

		**§3.1 NORMATIVE — caller-owned best-effort, NOT fatal (the INVERSE of
		C1's ``record_intent``, like C3's ``record_rejected``):** post-place
		``record_pending`` backfill failure ⇒ NOT fatal (log ERROR, continue).
		The durable row already exists from ``record_intent``; B's reconciler
		owns recovery via ``client_order_id``. Raising here would needlessly
		halt the engine over an audit-grade backfill miss while the
		funds-at-risk invariant is already satisfied. Contrast:

		* **C1 ``record_intent`` — FATAL.** Its INSERT precedes ``place()``;
		  the row does NOT exist yet, so a failure strands a funds-at-risk
		  order with no local row → ``RecordPendingFailed`` propagates uncaught
		  and the engine's ghost-reject clauses halt it. C4 is the opposite:
		  the row ALREADY exists, nothing to strand.
		* **C3 ``record_rejected`` — CAS ``pending → rejected``, best-effort.**
		  Same §3.1 not-fatal posture; C4 differs only in that the CAS keeps
		  ``status='pending'`` (state still unknown) instead of moving to a
		  terminal ``rejected``.

		Failure modes (all best-effort — log ERROR, never raise, never
		``RecordPendingFailed``; ghost-reject scope is funds-at-risk
		pre-place INSERTs only):

		* **Row-not-found (no preceding C1 ``record_intent`` — defense in
		  depth):** log ERROR audit gap and return. Do NOT fabricate a
		  competing INSERT (that would resurrect B's superseded
		  insert-on-outcome model; spec §3). B's reconciler is the backstop via
		  ``client_order_id`` (mirrors C3's CAS-miss posture).
		* **Backfill write failure — two carve-outs (both best-effort, never
		  raise), mirroring C3's ``record_rejected`` split so an operator can
		  triage faster:** ``sqlite3.Error`` is the transient/environmental
		  disk/DB fault (or a CAS lost-race no-op) — log ERROR and continue; a
		  non-DB ``Exception`` is flagged DISTINCTLY as an UNEXPECTED error (a
		  possible permanent B-API/signature drift) which would otherwise
		  log-and-continue forever with zero backfills — an operator should
		  escalate that class. The C1 row is intact either way (the targeted
		  CAS UPDATE applied or no-op'd — it never half-writes); B's reconciler
		  resolves the row via ``client_order_id`` regardless.

		This partially supersedes B's CR-4 insert-on-outcome for the
		executor-pending case — a converged, locked §3 / §3.1 tradeoff,
		INTENTIONAL, not a regression.
		"""
		try:
			# B's canonical by-client_order_id lookup (identical predicate to
			# live.state.py:807 / reconciliation / record_trade /
			# record_rejected) — NOT hand-rolled; UNIQUE ⇒ at most one row.
			# (id, status) in one round-trip: status discriminates the
			# row-not-found audit gap from the normal still-pending backfill.
			found = self._conn.execute(
				"SELECT id, status FROM live_trades WHERE client_order_id = ?",
				(client_order_id,),
			).fetchone()
			if found is None:
				# §3.1 accepted audit gap: the C1 record_intent row is absent
				# (defense-in-depth — should not happen; dispatch always
				# record_intent's before place()). Log ERROR and return — NOT
				# fatal, NOT a fabricated INSERT (spec §3 supersedes
				# insert-on-outcome); B's reconciler is the backstop via
				# client_order_id.
				log.error(
					"record_pending row-not-found: no C1 pending row for "
					"client_order_id=%r reason=%r — record_intent must have "
					"INSERTed it before the executor-pending write; §3.1 "
					"accepted audit gap, not fatal (the durable row's absence "
					"is recoverable by B's reconciler via client_order_id), "
					"NOT a fabricated insert (spec §3 supersedes "
					"insert-on-outcome)",
					client_order_id,
					rejection_reason,
				)
				return
			row_id = int(found[0])
			pre_status = found[1]
			# Only backfill when a non-empty id is now known. A pure
			# NetworkError returns no id (kalshi_order_id None/"") — leave the
			# C1 NULL as-is; never null out an id C1 or a prior call set
			# (idempotent: re-running with the same id is a no-op-equivalent —
			# the CAS UPDATE rewrites the same value or no-ops if the row
			# already left 'pending').
			if kalshi_order_id:
				backfilled = _backfill_pending_kalshi_order_id(
					self._conn, row_id=row_id, kalshi_order_id=kalshi_order_id
				)
				if not backfilled:
					# Lost CAS race: the row left 'pending' between the SELECT
					# and the UPDATE (B's reconciler resolved it, or a fill
					# landed). Idempotent no-op — surface DISTINCTLY on this
					# store's coid-keyed audit logger (B's _cas_update is not
					# in this path) so the audit trail records that the backfill
					# did not apply; NOT fatal, NOT re-applied (the row already
					# moved on; B owns it). pre_status is the row's status at
					# SELECT time — a useful triage hint for the race window.
					log.error(
						"record_pending backfill lost CAS race for "
						"client_order_id=%r reason=%r: row left 'pending' "
						"(status at lookup=%r) before the kalshi_order_id=%r "
						"backfill applied — §3.1 best-effort no-op, not fatal "
						"(row already transitioned; B's reconciler owns it), "
						"not re-applied",
						client_order_id,
						rejection_reason,
						pre_status,
						kalshi_order_id,
					)
		except sqlite3.Error as exc:
			# §3.1 caller-owned best-effort — TRANSIENT/ENVIRONMENTAL DB or
			# disk fault. The durable C1 pending row ALREADY exists (this is
			# the INVERSE of C1's FATAL record_intent: there the row did NOT
			# exist yet, here it does), so a failed kalshi_order_id backfill
			# strands at most an audit-grade detail — B's reconciler resolves
			# the row via client_order_id regardless. Log ERROR, do NOT raise
			# — never RecordPendingFailed (ghost-reject = funds-at-risk
			# pre-place INSERTs only; PR#34 438d843 best-effort precedent).
			log.error(
				"record_pending backfill failed (DB/disk fault) for "
				"client_order_id=%r reason=%r: %s — §3.1 best-effort, not "
				"fatal (the durable C1 pending row already exists; B's "
				"reconciler owns recovery via client_order_id; transient)",
				client_order_id,
				rejection_reason,
				exc,
			)
		except Exception as exc:
			# §3.1 caller-owned best-effort — UNEXPECTED non-DB error. This
			# is NOT the transient carve-out: it is most likely a PERMANENT
			# programming / B-API signature drift (e.g. a wrong kwarg to
			# _backfill_pending_kalshi_order_id) that would otherwise
			# log-and-continue FOREVER with zero kalshi_order_id backfills.
			# Still best-effort (never raise, never RecordPendingFailed — the
			# durable C1 pending row already exists & B's reconciler owns
			# recovery via client_order_id), but flagged DISTINCTLY so an
			# operator can escalate this class faster than the transient one.
			log.error(
				"record_pending UNEXPECTED non-DB error (possible B-API / "
				"signature drift — escalate; NOT a transient disk fault) for "
				"client_order_id=%r reason=%r: %r — §3.1 best-effort, not "
				"fatal (the durable C1 pending row already exists; B's "
				"reconciler owns recovery via client_order_id), engine not "
				"masked",
				client_order_id,
				rejection_reason,
				exc,
			)

	def record_rejected(
		self,
		*,
		ticker: str,
		series: str,
		strategy: str,
		side: str,
		intended_size: int,
		entry_price_cents: Optional[int],
		stop_loss_distance_cents: Optional[int],
		client_order_id: str,
		placed_at_utc: str,
		rejection_reason: str,
	) -> None:
		"""LIVE rejected write — a CAS ``pending → rejected`` TRANSITION of
		the C1 row located by ``client_order_id``, **NOT an insert** (spec §3
		EXPLICITLY supersedes B's CR-4 insert-on-outcome / one-row-per-attempt
		model: "the live entry model is insert-pending-then-CAS-transition,
		NOT insert-on-outcome").

		Dispatch's rejected branch (non-``stale_book`` only; ``stale_book`` is
		short-circuited before the store) calls this UNCONDITIONALLY — it must
		never branch on paper-vs-live (spec §1 keystone) — so the paper-shaped
		Protocol signature is preserved verbatim (the locked 10-kwarg
		signature, no ``kalshi_order_id``; ``rejection_reason`` REQUIRED;
		pinned by ``tests/test_engine_dispatch_pending_branch.py``).

		Flow:

		1. Locate the C1 ``pending`` row by ``client_order_id`` using B's
		   canonical lookup query (the same ``WHERE client_order_id = ?``
		   SELECT ``live.state`` itself / the reconciler / ``record_trade``
		   use — NOT hand-rolled SQL; §5). The select is extended to
		   ``SELECT id, status`` so the pre-CAS status is captured in the
		   SAME round-trip (no extra read vs B's ``_status_of``).
		   ``client_order_id`` is ``UNIQUE`` so this is at most one row.
		2. CAS ``pending → rejected`` via B's
		   :func:`live.state.transition_pending_to_rejected`
		   (``kalshi_order_id=None`` — a rejected order never got a Kalshi
		   id on this path). On a won CAS (pre-status was ``pending``) the
		   normal path stays quiet (B emits its own ``pending→rejected``
		   INFO). A LOST CAS race (the row had already left ``pending``) is
		   surfaced DISTINCTLY here on THIS module's coid-keyed audit logger
		   WITH the business keys (``client_order_id`` / ``rejection_reason``
		   / actual current status) — B's ``_cas_update`` only emits a
		   generic WARNING keyed by ``row_id`` on the ``edge_catcher.live.state``
		   logger (no coid, no reason), invisible on the store's audit trail.
		   The dangerous sub-case is a ``status='open'`` row (the order
		   FILLED): a subsequent ``record_rejected`` for the same coid means
		   the system believes one order both filled AND was rejected — a
		   fill/reject ORDERING ANOMALY (a real-money concern the zero-error
		   lens targets), logged explicitly as such; any other terminal
		   pre-status (``rejected`` / ``rejected_post_hoc`` / ``cancelled``)
		   is the benign late/duplicate-reject variant. Either way this is a
		   best-effort observability log, NOT a raise (spec §3.1 — a rejected
		   order holds no position).

		**§3.1 best-effort — caller-owned, NOT fatal (unlike C1's
		``record_intent``):** ``record_intent`` failure is FATAL
		(``RecordPendingFailed`` propagates, entry aborts BEFORE ``place()``
		— a funds-at-risk INSERT). ``record_rejected`` is the inverse: a
		rejected order **holds no position**, so per spec §3.1
		("``record_rejected`` CAS-miss/failure ⇒ NOT fatal,
		audit-best-effort, log ERROR … a rejected order holds no position")
		every failure mode here is a logged ERROR audit gap, never a raise:

		* **CAS-miss (no preceding C1 ``pending`` row):** the spec author
		  KNEW pre-place C-gate rejects (``absolute_max_exceeded`` /
		  ``invalid_intended_size``) reject the order BEFORE C1 inserts a
		  pending row, and DELIBERATELY accepted that as a logged audit gap,
		  NOT fatal. Log ERROR and return — do NOT raise, do NOT INSERT a
		  fabricated row (that would resurrect B's superseded
		  insert-on-outcome model; spec §3).
		* **Lost CAS race (row found but not ``pending``):** surfaced
		  DISTINCTLY on this store's coid-keyed audit logger with the
		  business keys + the actual current status, explicitly flagged as a
		  fill/reject ordering anomaly when the row is ``open`` (real-money
		  concern) vs benign late/duplicate reject otherwise. Best-effort log
		  only — never raise (the row exists and is terminal/filled; nothing
		  to strand).
		* **Write failure — categorized into two carve-outs (both
		  best-effort, never raise):** B's ``transition_pending_to_rejected``
		  owns only the lost-race WARNING no-op, NOT a write-failure
		  carve-out, so the CALLER owns the ``try/except`` (PR#34 ``438d843``
		  precedent). It is split so an operator can triage faster:
		  ``sqlite3.Error`` is the transient/environmental disk/DB fault (the
		  documented §3.1 carve-out — mirrors B's ``record_pending``
		  ``except sqlite3.Error``); a non-DB ``Exception`` is flagged
		  DISTINCTLY as an UNEXPECTED error (a possible permanent
		  B-API/signature drift, e.g. a wrong kwarg) which would otherwise
		  log-and-continue forever with zero rejected audit rows — an
		  operator should escalate that class. Neither raises
		  ``RecordPendingFailed`` (ghost-reject scope is funds-at-risk
		  INSERTs only).

		This partially supersedes B's CR-4 audit-completeness for the
		positionless-rejected case — a converged, locked §3.1 tradeoff,
		INTENTIONAL, not a regression.
		"""
		try:
			# B's canonical by-client_order_id lookup (identical predicate to
			# live.state.py:807 / reconciliation.py:706 / record_trade) —
			# NOT hand-rolled; UNIQUE ⇒ at most one row. The select is
			# extended to (id, status) so the pre-CAS status rides the SAME
			# round-trip (preferred over a separate _status_of read): a
			# pending pre-status ⇒ the CAS below wins; any other pre-status ⇒
			# the CAS is a no-op and the resulting status == this pre-status,
			# so this value is the authoritative lost-race status without a
			# second query.
			found = self._conn.execute(
				"SELECT id, status FROM live_trades WHERE client_order_id = ?",
				(client_order_id,),
			).fetchone()
			if found is None:
				# §3.1 accepted audit gap: pre-place C-gate reject (no C1
				# pending row). Log ERROR and return — NOT fatal, NOT a
				# fabricated INSERT (spec §3 supersedes insert-on-outcome).
				log.error(
					"record_rejected CAS-miss: no pending row for "
					"client_order_id=%r reason=%r — pre-place C-gate reject "
					"(no C1 record_intent); §3.1 accepted audit gap, not "
					"fatal (a rejected order holds no position)",
					client_order_id,
					rejection_reason,
				)
				return
			row_id = int(found[0])
			pre_status = found[1]
			# CAS pending → rejected via B's writer (no hand-rolled UPDATE;
			# §5). A rejected order never got a Kalshi id on this path.
			transition_pending_to_rejected(
				self._conn,
				row_id,
				kalshi_order_id=None,
				rejection_reason=rejection_reason,
			)
			# FIX 1 — lost-CAS-race observability. The CAS only fires when
			# the row was 'pending'; if pre_status was anything else the row
			# already left 'pending' (lost race) and is UNCHANGED, so
			# pre_status IS the resulting status. Surface that DISTINCTLY on
			# THIS store's coid-keyed audit logger with the business keys
			# (B's _cas_update only WARNs by row_id on a different logger).
			# Mirror B's `if changed:`-style gating (state.py:948) — keep the
			# normal won-CAS path quiet (B logs its own pending→rejected
			# INFO); only the lost race is noteworthy here.
			if pre_status != "pending":
				anomaly = (
					"FILL/REJECT ORDERING ANOMALY (real-money concern: the "
					"system believes this order both filled and was "
					"rejected)"
					if pre_status == "open"
					else "benign late/duplicate reject"
				)
				log.error(
					"record_rejected lost CAS race for client_order_id=%r "
					"reason=%r: row already left 'pending' (current "
					"status=%r) — %s; §3.1 best-effort, not fatal (a "
					"rejected order holds no position), not re-applied",
					client_order_id,
					rejection_reason,
					pre_status,
					anomaly,
				)
		except sqlite3.Error as exc:
			# §3.1 caller-owned best-effort — TRANSIENT/ENVIRONMENTAL DB or
			# disk fault (the documented carve-out; mirrors B's
			# record_pending `except sqlite3.Error`). A rejected order holds
			# no position, so this strands at most an audit gap. Log ERROR,
			# do NOT raise — never RecordPendingFailed (ghost-reject =
			# funds-at-risk INSERTs only), never mask the engine.
			log.error(
				"record_rejected audit-write failed (DB/disk fault) for "
				"client_order_id=%r reason=%r: %s — §3.1 best-effort, not "
				"fatal (a rejected order holds no position; transient; PR#34 "
				"438d843 precedent)",
				client_order_id,
				rejection_reason,
				exc,
			)
		except Exception as exc:
			# §3.1 caller-owned best-effort — UNEXPECTED non-DB error. This
			# is NOT the transient carve-out: it is most likely a PERMANENT
			# programming / B-API signature drift (e.g. a wrong kwarg to
			# transition_pending_to_rejected) that would otherwise
			# log-and-continue FOREVER with zero rejected audit rows. Still
			# best-effort (never raise, never RecordPendingFailed — a
			# rejected order holds no position), but flagged DISTINCTLY so an
			# operator can escalate this class faster than the transient one.
			log.error(
				"record_rejected UNEXPECTED non-DB error (possible B-API / "
				"signature drift — escalate; NOT a transient disk fault) for "
				"client_order_id=%r reason=%r: %r — §3.1 best-effort, not "
				"fatal (a rejected order holds no position), engine not "
				"masked",
				client_order_id,
				rejection_reason,
				exc,
			)

	def record_trade(
		self,
		ticker: str,
		entry_price: int,
		strategy: str,
		side: str,
		series_ticker: str,
		intended_size: int = 1,
		fill_size: int = 1,
		blended_entry: Optional[int] = None,
		book_depth: Optional[int] = None,
		fill_pct: Optional[float] = None,
		slippage_cents: Optional[float] = None,
		book_snapshot: Optional[str] = None,
		*,
		now: datetime,
		client_order_id: Optional[str] = None,
		kalshi_order_id: Optional[str] = None,
	) -> int:
		"""LIVE filled-entry write — a CAS ``pending → open`` TRANSITION of
		the C1 row, **NOT an insert** (spec §3 ``:400 filled`` row / §4.2 /
		§5).

		Dispatch's filled branch (E's later wiring) calls this
		UNCONDITIONALLY — it must never branch on paper-vs-live (spec §1
		keystone), so the paper-shaped Protocol signature is preserved and
		``client_order_id`` / ``kalshi_order_id`` are carried as additive
		keyword-only args (paper / in-memory accept-and-ignore; the live
		store consumes them). ``LiveExecutor.place`` returns
		``status="filled"`` synchronously for Kalshi IOC, so this is the
		common live entry path.

		Flow (every post-place outcome is a CAS on the C1 row, never a
		competing insert — §4.2):

		1. Locate the C1 ``pending`` row by ``client_order_id`` using B's
		   canonical lookup query (the same ``WHERE client_order_id = ?``
		   SELECT ``live.state`` itself / the reconciler / ``ws_handlers``
		   use — NOT hand-rolled SQL; §5). ``client_order_id`` is ``UNIQUE``
		   so this is at most one row.
		2. Compute the entry fee with B's canonical convention
		   ``int(round(STANDARD_FEE.calculate(blended_entry_cents,
		   fill_size)))`` — byte-identical to
		   ``ws_handlers._entry_fee_cents`` /
		   ``reconciliation._resolve_matched_pending`` so F's P&L does not
		   diverge by which path booked the fill (spec §283). ``blended`` is
		   D's already-resolved blended fill price; treat a falsy
		   ``blended_entry`` (None / sub-cent-rounds-to-0¢) as
		   ``entry_price`` for the cost basis, mirroring paper
		   ``record_trade``'s ``blended_entry or entry_price`` rule so the
		   fee is never computed off a 0¢ basis.
		3. CAS ``pending → open`` via B's
		   :func:`live.state.transition_pending_to_open` over the single
		   held connection (no hand-rolled UPDATE, no new thread/lock; §5).
		   ``slippage_cents`` is consumed verbatim (D's signed value — never
		   recomputed here); ``fill_pct`` verbatim. A lost CAS race (row
		   already left ``pending``) is a logged no-op inside
		   ``transition_pending_to_open`` — exactly one row remains either
		   way; this method never inserts a second row.

		Returns the transitioned row's id (paper parity:
		``record_trade -> int`` trade id) so dispatch's filled-branch
		bookkeeping is store-agnostic.

		``client_order_id`` is required on the live path (dispatch always
		generates D's idempotency key before ``record_intent``); a missing /
		unmatched one is a wiring bug and raises loudly rather than silently
		inserting an unreconcilable row (zero-error lens). ``kalshi_order_id``
		is symmetrically required on this filled path: D's ``place()`` always
		returns a real id, B's WS reconciler / ``on_fill_event`` key off it,
		so a missing one raises loudly rather than writing an empty,
		unreconcilable id (same zero-error lens, both identity keys).
		"""
		if not client_order_id:
			raise ValueError(
				"SQLiteTradeStore.record_trade requires client_order_id on "
				"the live path (dispatch must pass D's idempotency key — the "
				"C1 pending row is located by it); spec §1/§3."
			)
		if not kalshi_order_id:
			raise ValueError(
				"SQLiteTradeStore.record_trade requires a real kalshi_order_id "
				"on the live filled path (D's place() returns it; B's WS "
				"reconciler / on_fill_event key off it — an empty id is "
				"unreconcilable silent-bad-state); spec §3."
			)
		# B's canonical by-client_order_id lookup (identical query to
		# live.state.py:807 / reconciliation.py:706 / ws_handlers
		# _find_row_by_coid) — NOT hand-rolled; UNIQUE ⇒ at most one row.
		found = self._conn.execute(
			"SELECT id FROM live_trades WHERE client_order_id = ?",
			(client_order_id,),
		).fetchone()
		if found is None:
			raise ValueError(
				f"SQLiteTradeStore.record_trade: no pending row for "
				f"client_order_id={client_order_id!r} — C1 record_intent "
				f"must have inserted it before the filled write (spec §3/§4.2)."
			)
		row_id = int(found[0])

		# Cost basis: D's blended fill price; fall back to entry_price when
		# blended is falsy (None or sub-cent → 0¢), mirroring paper
		# record_trade's `blended_entry or entry_price` so the fee is never
		# taken off a 0¢ basis.
		blended_cents = blended_entry if blended_entry else entry_price
		# B's canonical entry-fee convention (ws_handlers._entry_fee_cents /
		# reconciliation._resolve_matched_pending) — keep byte-identical so
		# F's P&L does not diverge by which path booked the fill (spec §283).
		entry_fee_cents = int(
			round(STANDARD_FEE.calculate(blended_cents, fill_size))
		)

		transition_pending_to_open(
			self._conn,
			row_id,
			# guarded truthy above — pass D's real id directly
			kalshi_order_id=kalshi_order_id,
			fill_size=fill_size,
			blended_entry_cents=blended_cents,
			# D's signed slippage, consumed verbatim — never recomputed here
			# (cross-PR contract #1). None coalesces to 0 for the INTEGER
			# column (paper-path callers may omit it).
			slippage_cents=int(slippage_cents or 0),
			fill_pct=fill_pct if fill_pct is not None else 0.0,
			entry_time=now.isoformat(),
			entry_fee_cents=entry_fee_cents,
		)
		return row_id

	# -------------------------------------------------------------------------
	# Live-path READ surface — open-position reads for TickContext
	# -------------------------------------------------------------------------

	def get_open_trades(self) -> list[dict[str, Any]]:
		"""All ``open`` rows, mapped to the paper open-trade dict shape.

		Dispatch / strategy code consume this to build
		``TickContext.open_positions``. Ordered by ``id`` ASC for determinism
		(matches ``engine.live_db.read_open_positions``' ordering contract).
		"""
		# Save + restore the prior row_factory (NOT a hardcoded None): E's PR-6
		# wiring may share this connection and set a connection-level
		# row_factory; clobbering to None would silently reset it. This module
		# deliberately defends against cross-PR coupling — this closes the last
		# such gap. Behaviour-identical under every current caller (the factory
		# is None by default and no other code on this connection sets it);
		# purely forward-defensive.
		_prev_factory = self._conn.row_factory
		self._conn.row_factory = sqlite3.Row
		try:
			rows = self._conn.execute(
				f"{_OPEN_ROW_SQL} ORDER BY id ASC"
			).fetchall()
		finally:
			self._conn.row_factory = _prev_factory
		return [_open_row_to_dict(r) for r in rows]

	def get_open_trades_for(
		self, strategy: str, ticker: str
	) -> list[dict[str, Any]]:
		"""``open`` rows filtered by ``strategy`` + ``ticker``.

		Parameter name is ``strategy`` (NOT ``strat_name``) — matches the
		Protocol + paper ``TradeStore.get_open_trades_for`` so dispatch's
		``store.get_open_trades_for(strat.name, ticker)`` call binds.
		"""
		# Save + restore the prior row_factory (NOT a hardcoded None): same
		# cross-PR-coupling defense as get_open_trades — E's PR-6 wiring may
		# share this connection with its own connection-level row_factory.
		# Behaviour-identical under every current caller; forward-defensive.
		_prev_factory = self._conn.row_factory
		self._conn.row_factory = sqlite3.Row
		try:
			rows = self._conn.execute(
				f"{_OPEN_ROW_SQL} AND strategy = ? AND ticker = ? "
				"ORDER BY id ASC",
				(strategy, ticker),
			).fetchall()
		finally:
			self._conn.row_factory = _prev_factory
		return [_open_row_to_dict(r) for r in rows]

	# -------------------------------------------------------------------------
	# Paper-path Protocol methods — NOT reachable on the live dispatch path
	# -------------------------------------------------------------------------
	#
	# These complete the structural Protocol surface but raise loudly: the
	# live post-placement lifecycle is WS-handler / reconciliation driven via
	# live.state's CAS functions against live_trades.db directly, NOT through
	# the store. A silent no-op would be a real-money correctness hole (e.g. a
	# strategy exit Signal silently not closing a live position). E wires the
	# live engine so none of these are reached; this is the fail-loud backstop.

	def _live_only(self, method: str) -> NotImplementedError:
		return NotImplementedError(
			f"SQLiteTradeStore is live-only; {method} is a paper-path method "
			f"not reachable on the live dispatch path. The live post-placement "
			f"lifecycle (exit / settlement / partial-exit / strategy state) is "
			f"driven by live.state's CAS-guarded transition functions via the "
			f"WS handlers + reconciliation against live_trades.db directly — "
			f"see edge_catcher/live/store.py module docstring."
		)

	def settle_trade(self, trade_id: int, result: str, *, now: datetime) -> None:
		raise self._live_only("settle_trade")

	def exit_trade(self, trade_id: int, exit_price: int, *, now: datetime) -> None:
		raise self._live_only("exit_trade")

	def get_trade_by_id(self, trade_id: int) -> dict[str, Any] | None:
		raise self._live_only("get_trade_by_id")

	def save_state(self, strategy: str, state_dict: dict[str, Any]) -> None:
		raise self._live_only("save_state")

	def load_state(self, strategy: str) -> dict[str, Any]:
		raise self._live_only("load_state")

	def load_all_states(self) -> dict[str, dict[str, Any]]:
		raise self._live_only("load_all_states")

	# -------------------------------------------------------------------------
	# Lifecycle
	# -------------------------------------------------------------------------

	def close(self) -> None:
		"""Close the held connection. Idempotent — E's shutdown path (SIGTERM
		handler + a finally block) may call it more than once; a second call
		is a no-op rather than a ``ProgrammingError``."""
		if self._closed:
			return
		self._conn.close()
		self._closed = True
