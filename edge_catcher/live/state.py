"""Order state machine — sync write functions over ``live_trades.db``.

Owned by sub-project B (v1.6.0 PR 5). Called from D's dispatch path
(``record_pending`` / ``record_open`` / ``record_rejected``) and from B's
WS handlers + reconciliation loops (the ``transition_*`` / ``record_*``
functions).

All functions are **sync** — local SQLite writes are sub-millisecond, so
there is no need for async here. E owns the async event loop and calls
these synchronously from inside its async WS-handler / reconciliation
shells. The functions are pure SQL: no I/O outside the passed
``sqlite3.Connection``.

Every status-mutating UPDATE is **compare-and-swap by WHERE clause**
(spec Risk #9): the UPDATE carries an explicit
``WHERE status IN (<valid preconditions>)`` predicate and the function
checks ``cursor.rowcount == 1``. ``rowcount == 0`` means a concurrent
event already moved the row out of the precondition state — the function
logs a warning and returns WITHOUT blind-writing. This makes every
transition idempotent under the WS event-duplication / settlement-vs-fill
races Kalshi can produce (SQLite's serializable isolation under WAL +
single-writer B makes WHERE-clause CAS sufficient).
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Literal

from edge_catcher.storage.migrations import apply_migrations

log = logging.getLogger(__name__)


class RecordPendingFailed(Exception):
	"""Raised when record_pending/record_open INSERT fails — engine MUST stop.

	Mirror of KillSwitchTripFailed (C-spec L214 + PR #36 ghost-reject):
	silent INSERT failure = order stranded on Kalshi with no local row.
	dispatch.process_tick + engine._ws_loop + engine outer reconnect all
	catch Exception broadly but MUST re-raise this class so the engine
	loop terminates rather than swallowing a failed persistence.

	Scope: raised ONLY by ``record_pending`` and ``record_open`` — the two
	writes where a failed INSERT means a funds-at-risk Kalshi-side order is
	stranded with no local row for B's reconciler to find. ``record_rejected``
	(and any future terminal-state ``record_*`` with no Kalshi-side
	position) deliberately does NOT raise this: a failed audit-row INSERT
	for an already-rejected order strands only an audit row, not money —
	it is logged best-effort per the PR #34 audit-write precedent
	(commit 438d843).
	"""


# ---------------------------------------------------------------------------
# Connection helper
# ---------------------------------------------------------------------------


def connect_live_trades_db(db_path: Path) -> sqlite3.Connection:
	"""Open (creating if absent) ``live_trades.db`` with WAL mode and the
	``0003_create_live_trades`` migration applied.

	WAL is set at open time (spec Risk #5) so the optional reporting CLI can
	read the DB read-only while B writes; the single-writer constraint still
	holds (only B's process writes). The caller owns the connection lifecycle
	(E in production; the test harness in unit tests). The migration runner is
	idempotent, so calling this against an already-migrated DB is a no-op
	beyond the PRAGMA.
	"""
	db_path.parent.mkdir(parents=True, exist_ok=True)
	conn = sqlite3.connect(str(db_path), check_same_thread=False)
	conn.execute("PRAGMA journal_mode=WAL")
	conn.execute("PRAGMA busy_timeout=5000")
	apply_migrations(conn)
	return conn


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _cas_update(
	conn: sqlite3.Connection,
	*,
	row_id: int,
	sql: str,
	params: tuple[object, ...],
	transition: str,
	commit: bool = True,
) -> bool:
	"""Execute a status-mutating UPDATE that already carries its
	``WHERE id=? AND status IN (...)`` compare-and-swap predicate, then
	enforce ``rowcount == 1``.

	Returns True when exactly one row changed (the CAS won). On
	``rowcount == 0`` the precondition lost a concurrent-event race
	(settlement vs fill, duplicate WS event) — log a WARNING with the row id
	+ attempted transition and return False WITHOUT raising and WITHOUT
	blind-writing. Callers treat False as an idempotent no-op.

	``rowcount > 1`` is impossible (``id`` is the PRIMARY KEY) but is treated
	as a hard invariant violation if it ever occurs.

	``commit`` (default True) controls whether a winning CAS is committed
	immediately. The single-write callers (every transition_*/record_close/
	record_cancelled/etc.) keep the default. ``record_partial_exit`` passes
	``commit=False`` so its parent-decrement UPDATE and child INSERT land in
	ONE transaction with a single trailing ``conn.commit()`` — making the
	two-step split atomic (Critical #2: a child-INSERT failure must roll back
	the parent decrement, never leaving the parent silently short of
	contracts with no offsetting child row).
	"""
	cur = conn.execute(sql, params)
	if cur.rowcount == 0:
		log.warning(
			"live_trades CAS lost race: id=%d transition=%s — precondition "
			"status no longer valid (concurrent settlement/fill or duplicate "
			"WS event); no-op",
			row_id,
			transition,
		)
		return False
	if cur.rowcount != 1:  # pragma: no cover - impossible with PK predicate
		raise RuntimeError(
			f"live_trades CAS hit rowcount={cur.rowcount} for id={row_id} "
			f"transition={transition!r} — expected exactly 1 (id is PK)"
		)
	if commit:
		conn.commit()
	return True


def _status_of(conn: sqlite3.Connection, row_id: int) -> str | None:
	row = conn.execute(
		"SELECT status FROM live_trades WHERE id = ?", (row_id,)
	).fetchone()
	return None if row is None else row[0]


# ---------------------------------------------------------------------------
# INSERT writers — called from D's dispatch path
# ---------------------------------------------------------------------------


def record_pending(
	conn: sqlite3.Connection,
	*,
	ticker: str,
	series: str,
	strategy: str,
	side: Literal["yes", "no"],
	intended_size: int,
	entry_price_cents: int | None,
	stop_loss_distance_cents: int | None,
	client_order_id: str,
	kalshi_order_id: str | None,
	placed_at_utc: str,
	rejection_reason: str | None = None,
) -> int:
	"""INSERT a new ``pending`` row. Called by dispatch.py on D's pending
	OrderResult (NetworkError / malformed-fills / engine-timeout).

	On INSERT: ``original_intended_size = intended_size``; ``fill_size = 0``;
	``status = 'pending'``. ``kalshi_order_id`` may be NULL (NetworkError).

	Raises ``RecordPendingFailed`` (chained from the underlying
	``sqlite3.Error``) on any DB failure — a silent INSERT failure here
	strands a funds-at-risk Kalshi-side order with no local row for B's
	reconciler to find. The engine MUST stop (ghost-reject defense).

	``entry_price_cents`` is the ORIGINAL Signal intent, NOT D's
	slippage-adjusted limit (caller's contract; pinned by
	tests/test_engine_dispatch_pending_branch.py). It is typed
	``int | None`` to match the locked cross-PR kwarg contract
	(``test_record_pending_kwarg_set_is_exactly_locked_eleven`` +
	``Signal.entry_price_cents: int | None``); a ``None`` intent is persisted
	as the inert sentinel ``0`` so the NOT-NULL DDL column stays satisfied
	(a pending row's entry_price_cents is overwritten by the real blended
	fill on transition_pending_to_open, and pending rows never feed P&L).
	"""
	try:
		cur = conn.execute(
			"INSERT INTO live_trades ("
			"ticker, series, strategy, side, intended_size, "
			"original_intended_size, fill_size, entry_price_cents, "
			"stop_loss_distance_cents, status, client_order_id, "
			"kalshi_order_id, placed_at_utc, rejection_reason"
			") VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, 'pending', ?, ?, ?, ?)",
			(
				ticker,
				series,
				strategy,
				side,
				intended_size,
				intended_size,
				# entry_price_cents is NOT NULL in the DDL; the locked Protocol
				# allows a None intent. Persist the inert sentinel 0 (pending
				# rows never feed P&L; the real price lands on fill).
				entry_price_cents if entry_price_cents is not None else 0,
				stop_loss_distance_cents,
				client_order_id,
				kalshi_order_id,
				placed_at_utc,
				rejection_reason,
			),
		)
		conn.commit()
	except sqlite3.Error as exc:
		log.error(
			"record_pending INSERT FAILED for client_order_id=%r: %s — engine "
			"MUST stop (ghost-reject defense; Kalshi order may be stranded)",
			client_order_id,
			exc,
		)
		raise RecordPendingFailed(
			f"live_trades pending INSERT failed for "
			f"client_order_id={client_order_id!r}: {exc}"
		) from exc
	row_id = int(cur.lastrowid or 0)
	log.info(
		"live_trades id=%d ε→pending ticker=%s strategy=%s size=%d coid=%s",
		row_id,
		ticker,
		strategy,
		intended_size,
		client_order_id,
	)
	return row_id


def record_open(
	conn: sqlite3.Connection,
	*,
	ticker: str,
	series: str,
	strategy: str,
	side: str,
	intended_size: int,
	fill_size: int,
	entry_price_cents: int,
	blended_entry_cents: int,
	slippage_cents: int,
	fill_pct: float,
	stop_loss_distance_cents: int,
	client_order_id: str,
	kalshi_order_id: str,
	placed_at_utc: str,
	entry_time: str,
	entry_fee_cents: int,
) -> int:
	"""INSERT a new ``open`` row. Called on D's filled / partial-filled
	OrderResult (Kalshi confirmed the entry at submission).

	On INSERT: ``original_intended_size = intended_size``;
	``entry_fee_remaining_cents = entry_fee_cents`` (the full immutable entry
	fee, decremented later by partial-exit splits and consumed at close);
	``status = 'open'``.

	Raises ``RecordPendingFailed`` (chained) on any DB failure — same
	funds-at-risk rationale as ``record_pending``: Kalshi already filled the
	entry, so a missing local row strands a live position.

	``slippage_cents`` is consumed verbatim from D's OrderResult (produced by
	``engine.fill_math.signed_slippage_cents``: positive = worse than limit,
	any side). This function never rolls its own slippage subtraction.
	"""
	try:
		cur = conn.execute(
			"INSERT INTO live_trades ("
			"ticker, series, strategy, side, intended_size, "
			"original_intended_size, fill_size, entry_price_cents, "
			"blended_entry_cents, slippage_cents, fill_pct, "
			"stop_loss_distance_cents, status, client_order_id, "
			"kalshi_order_id, placed_at_utc, entry_time, entry_fee_cents, "
			"entry_fee_remaining_cents"
			") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?, ?, ?, ?, ?, ?)",
			(
				ticker,
				series,
				strategy,
				side,
				intended_size,
				intended_size,
				fill_size,
				entry_price_cents,
				blended_entry_cents,
				slippage_cents,
				fill_pct,
				stop_loss_distance_cents,
				client_order_id,
				kalshi_order_id,
				placed_at_utc,
				entry_time,
				entry_fee_cents,
				entry_fee_cents,
			),
		)
		conn.commit()
	except sqlite3.Error as exc:
		log.error(
			"record_open INSERT FAILED for client_order_id=%r: %s — engine "
			"MUST stop (ghost-reject defense; live position has no local row)",
			client_order_id,
			exc,
		)
		raise RecordPendingFailed(
			f"live_trades open INSERT failed for "
			f"client_order_id={client_order_id!r}: {exc}"
		) from exc
	row_id = int(cur.lastrowid or 0)
	log.info(
		"live_trades id=%d ε→open ticker=%s strategy=%s fill=%d/%d coid=%s",
		row_id,
		ticker,
		strategy,
		fill_size,
		intended_size,
		client_order_id,
	)
	return row_id


def record_rejected(
	conn: sqlite3.Connection,
	*,
	ticker: str,
	series: str,
	strategy: str,
	side: str,
	intended_size: int,
	entry_price_cents: int | None,
	stop_loss_distance_cents: int | None,
	client_order_id: str,
	placed_at_utc: str,
	rejection_reason: str,
) -> int:
	"""INSERT a new ``rejected`` row (audit trail). CR-4 — every D place
	attempt produces exactly one row.

	On INSERT: ``original_intended_size = intended_size``;
	``status = 'rejected'``.

	**Audit-write carve-out (PR #34 precedent, commit 438d843; spec §661):**
	a rejected row represents an order Kalshi ALREADY rejected — there is no
	Kalshi-side position and no money at risk, so a failed INSERT strands
	only an audit row. This function therefore does NOT raise
	``RecordPendingFailed``; on ``sqlite3.Error`` it logs the audit gap at
	ERROR (operator-visible, mirrors live.client's
	``audit_write_failed_after_*`` log keys) and returns ``0`` (sentinel:
	"row not written"). Operators accept best-effort audit for
	already-rejected orders; the engine continues. This carve-out is pinned
	by the round-2 spec-review regression test so the ghost-reject scope
	cannot be over-broadened to cover terminal no-position states.

	Returns the new row id, or ``0`` when the best-effort INSERT failed.
	"""
	try:
		cur = conn.execute(
			"INSERT INTO live_trades ("
			"ticker, series, strategy, side, intended_size, "
			"original_intended_size, fill_size, entry_price_cents, "
			"stop_loss_distance_cents, status, client_order_id, "
			"placed_at_utc, rejection_reason"
			") VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, 'rejected', ?, ?, ?)",
			(
				ticker,
				series,
				strategy,
				side,
				intended_size,
				intended_size,
				# entry_price_cents is NOT NULL in the DDL; rejected rows may
				# carry a None intent (locked Protocol). Persist the sentinel
				# 0 so the audit row still writes — rejected rows never feed
				# P&L (read_daily_pnl_cents filters them out), so a 0 here is
				# inert and the column constraint stays satisfied.
				entry_price_cents if entry_price_cents is not None else 0,
				stop_loss_distance_cents,
				client_order_id,
				placed_at_utc,
				rejection_reason,
			),
		)
		conn.commit()
	except sqlite3.Error as exc:
		# Carve-out: log + swallow (NOT RecordPendingFailed). No Kalshi-side
		# position to strand — only an audit row is lost.
		log.exception(
			"rejected_audit_write_failed client_order_id=%s reason=%s: %s — "
			"audit row NOT persisted (best-effort; engine continues)",
			client_order_id,
			rejection_reason,
			exc,
		)
		return 0
	row_id = int(cur.lastrowid or 0)
	log.info(
		"live_trades id=%d ε→rejected ticker=%s strategy=%s reason=%s coid=%s",
		row_id,
		ticker,
		strategy,
		rejection_reason,
		client_order_id,
	)
	return row_id


# ---------------------------------------------------------------------------
# Terminal-state UPDATE writers (CAS-guarded)
# ---------------------------------------------------------------------------


def record_close(
	conn: sqlite3.Connection,
	row_id: int,
	*,
	status: Literal["won", "lost", "scratch"],
	exit_price_cents: int,
	exit_time: str,
	exit_reason: str,
	pnl_cents: int,
	exit_fee_cents: int,
	notes: str | None = None,
) -> None:
	"""UPDATE an existing row to a closed terminal state (won/lost/scratch)
	on a natural exit — D's exit IOC filled, settlement, or the final
	partial-exit child becoming the close.

	Consumes whatever remains in ``entry_fee_remaining_cents`` into this
	close's ``entry_fee_cents`` so multi-split rounding fragments are not
	lost, then zeroes the remainder. CAS precondition: status IN
	('open', 'exit_pending'). A lost race (settlement already closed the row)
	is a logged no-op.
	"""
	before = _status_of(conn, row_id)
	changed = _cas_update(
		conn,
		row_id=row_id,
		sql=(
			"UPDATE live_trades SET "
			"status = ?, exit_price_cents = ?, exit_time = ?, "
			"exit_reason = ?, pnl_cents = ?, exit_fee_cents = ?, "
			"entry_fee_cents = COALESCE(entry_fee_remaining_cents, entry_fee_cents), "
			"entry_fee_remaining_cents = 0, "
			"notes = COALESCE(?, notes) "
			"WHERE id = ? AND status IN ('open', 'exit_pending')"
		),
		params=(
			status,
			exit_price_cents,
			exit_time,
			exit_reason,
			pnl_cents,
			exit_fee_cents,
			notes,
			row_id,
		),
		transition=f"{before}->{status}",
	)
	# Important #6: only log the transition when the CAS actually applied.
	# On a lost race _cas_update already logged the WARNING — emitting an
	# "open→won" INFO here would falsely claim a transition that did not
	# happen (TOCTOU: `before` was read before the no-op UPDATE).
	if changed:
		log.info(
			"live_trades id=%d %s→%s exit=%dc pnl=%dc reason=%s",
			row_id,
			before,
			status,
			exit_price_cents,
			pnl_cents,
			exit_reason,
		)


def record_cancelled(
	conn: sqlite3.Connection,
	row_id: int,
	*,
	exit_time: str,
	exit_price_cents: int | None,
	pnl_cents: int,
	notes: str,
) -> None:
	"""UPDATE → ``cancelled`` (terminal). Operator-CLI path.

	Distinct from ``record_close`` because cancellation may occur from ANY
	active state (pending, open, exit_pending) and the status set differs
	('cancelled' only). ``exit_price_cents`` may be NULL when cancellation
	occurred before any fill (row was pending). Carries the same
	terminal-no-Kalshi-position carve-out as ``record_rejected`` — a CAS
	loss is a logged no-op, never a RecordPendingFailed.
	"""
	before = _status_of(conn, row_id)
	changed = _cas_update(
		conn,
		row_id=row_id,
		sql=(
			"UPDATE live_trades SET "
			"status = 'cancelled', exit_time = ?, exit_price_cents = ?, "
			"pnl_cents = ?, notes = ? "
			"WHERE id = ? AND status IN ('pending', 'open', 'exit_pending')"
		),
		params=(exit_time, exit_price_cents, pnl_cents, notes, row_id),
		transition=f"{before}->cancelled",
	)
	# Important #6: gate the transition log on the CAS result (see
	# record_close) — a lost race already logged its WARNING in _cas_update.
	if changed:
		log.info(
			"live_trades id=%d %s→cancelled pnl=%dc notes=%s",
			row_id,
			before,
			pnl_cents,
			notes,
		)


def record_partial_exit(
	conn: sqlite3.Connection,
	parent_id: int,
	*,
	closed_size: int,
	exit_price_cents: int,
	exit_reason: str,
	now_utc: str,
	exit_fee_cents: int,
	kalshi_exit_order_id: str,
) -> int:
	"""Split-row partial exit: M of N contracts closed, (N-M) still alive.

	1. UPDATE parent: ``fill_size -= M``, ``intended_size -= M``,
	   ``reconciled_at_utc = now``. Status stays 'open' (remaining position
	   alive). CAS precondition: status='open'.
	2. INSERT a closed child row (status won/lost/scratch per entry vs exit)
	   for the M closed contracts, inheriting the parent's cost basis +
	   identity, with an allocated share of the parent's entry fee.

	**Allocated entry fee:** ``round(parent.entry_fee_cents * M /
	parent.original_intended_size)``, clamped to the parent's
	``entry_fee_remaining_cents`` so multi-split rounding never over-allocates;
	the parent's remainder is decremented by the allocated amount. The final
	close (record_close) consumes whatever remains.

	**Idempotency (Critical #1):** a duplicate WS fill event repeats the
	SAME ``kalshi_exit_order_id``. Before any write, this function looks up
	an existing child of THIS parent already bearing that exit order id; if
	found, it returns that child's id as a pure no-op (parent NOT
	decremented again, no phantom child). The duplicate-detection key is the
	repeated Kalshi identity — NOT a count-derived ``-split-N`` (a fresh,
	non-colliding split-id is exactly what a count derives on the second
	call, which is why the old UNIQUE-collision scheme could not detect a
	success-then-duplicate). The ``-split-N`` label is retained purely as
	the child's monotonic row identifier (Test #8 / spec §293); its
	``UNIQUE(client_order_id)`` is kept as a defence-in-depth backstop only.

	**Atomicity (Critical #2):** the parent-decrement UPDATE and the child
	INSERT run in ONE transaction (parent CAS uses ``commit=False``; a
	single ``conn.commit()`` lands both). ANY failure rolls back BOTH — a
	child-INSERT error can never leave the parent durably short of
	contracts with no offsetting child row.

	Returns the child row id (new, or the existing one on idempotent retry).
	Returns ``0`` if the parent is absent, the parent CAS lost its race
	(parent no longer 'open'), or the call was rejected by a precondition
	guard (``closed_size`` out of bounds, or an empty
	``kalshi_exit_order_id`` — an un-keyed exit cannot be made idempotent)
	— every ``0`` path is logged unambiguously so "parent absent" vs
	"CAS-lost" vs "bad-size" vs "missing-exit-id" are distinguishable in the
	operator log.
	"""
	parent = conn.execute(
		"SELECT ticker, series, strategy, side, blended_entry_cents, "
		"entry_time, entry_fee_cents, entry_fee_remaining_cents, "
		"original_intended_size, client_order_id, status, fill_size "
		"FROM live_trades WHERE id = ?",
		(parent_id,),
	).fetchone()
	if parent is None:
		log.warning(
			"record_partial_exit: parent id=%d not found — no-op (return 0: "
			"parent absent)",
			parent_id,
		)
		return 0
	(
		p_ticker,
		p_series,
		p_strategy,
		p_side,
		p_blended_entry,
		p_entry_time,
		p_entry_fee,
		p_entry_fee_remaining,
		p_orig_size,
		p_coid,
		p_status,
		p_fill_size,
	) = parent

	# --- Precondition guard (Important #5): a bad/duplicate WS event with
	# closed_size <= 0 or > the parent's current fill_size would drive
	# fill_size / intended_size negative (silent real-money under-count) or
	# mint a zero/negative child. Reject with a log + return 0; never write.
	if not 0 < closed_size <= p_fill_size:
		log.error(
			"record_partial_exit: parent id=%d closed_size=%d out of bounds "
			"(must be 0 < M <= parent.fill_size=%d) — REJECTED, no write "
			"(return 0: bad-size)",
			parent_id,
			closed_size,
			p_fill_size,
		)
		return 0

	# --- Precondition guard: an exit fill with no Kalshi order_id has no
	# idempotency identity. The Critical #1 dedup below keys on
	# kalshi_exit_order_id; an empty key would let two genuinely-distinct
	# un-keyed fills collapse into one (silent real-money under-count) or a
	# reconnect re-delivery double-decrement the parent. A fill without a
	# Kalshi order id is itself anomalous (Kalshi always assigns one) —
	# reject loud and never write; the position stays 'open' for reconcile /
	# next-tick recovery (same posture as on_fill_event's other "no
	# trustworthy data → no-op, reconcile owns recovery" guards).
	if not kalshi_exit_order_id or not kalshi_exit_order_id.strip():
		log.error(
			"record_partial_exit: parent id=%d exit fill has no "
			"kalshi_exit_order_id (empty) — REJECTED, no write (return 0: "
			"missing-exit-id); un-dedupable, left 'open' for reconcile",
			parent_id,
		)
		return 0

	# --- Idempotency pre-check (Critical #1): a duplicate WS event repeats
	# the same kalshi_exit_order_id. If a child of THIS parent already
	# carries it, the logical fill was already booked — return that child as
	# a no-op WITHOUT decrementing the parent again or inserting a phantom.
	dup = conn.execute(
		"SELECT id, client_order_id FROM live_trades "
		"WHERE client_order_id LIKE ? AND kalshi_order_id = ?",
		(f"{p_coid}-split-%", kalshi_exit_order_id),
	).fetchone()
	if dup is not None:
		log.info(
			"record_partial_exit: duplicate WS event for parent=%d "
			"kalshi_exit_order_id=%s — child %s (id=%d) already booked, "
			"no-op (idempotent; parent NOT decremented again)",
			parent_id,
			kalshi_exit_order_id,
			dup[1],
			int(dup[0]),
		)
		return int(dup[0])

	# Cost basis is the parent's blended entry. A NULL blended_entry_cents on
	# an 'open' row is an invariant violation (transition_pending_to_open /
	# record_open always set it at fill) — under the real-money lens a 0
	# cost-basis would silently mislabel won/lost and corrupt P&L, so this is
	# a hard error, not a silent fallback (Minor (a)).
	if p_blended_entry is None:
		raise RuntimeError(
			f"record_partial_exit: parent id={parent_id} has NULL "
			f"blended_entry_cents on a partial-exitable row — cannot compute "
			f"cost basis (invariant violation: open rows always have a "
			f"blended entry)"
		)
	entry_basis = p_blended_entry

	# Allocated entry fee (proportional, clamped to remaining). Guard the
	# original_intended_size == 0 case (Critical #3): an engine-timeout
	# pending row is synthesized with intended_size=0 (dispatch.py defers
	# sizing); transition_pending_to_open does NOT mutate
	# original_intended_size (spec-locked immutability), so a timeout-pending
	# row reconciled to open then partially exited would hit ZeroDivision.
	# When the proportional denominator is unusable, allocate the full
	# remaining entry fee to this child (record_close then sees a 0
	# remainder; no fragment lost) and log loudly.
	entry_fee_total = p_entry_fee or 0
	remaining = (
		p_entry_fee_remaining
		if p_entry_fee_remaining is not None
		else entry_fee_total
	)
	if p_orig_size and p_orig_size > 0:
		child_entry_fee = round(entry_fee_total * closed_size / p_orig_size)
		child_entry_fee = min(child_entry_fee, remaining)
	else:
		child_entry_fee = remaining
		log.warning(
			"record_partial_exit: parent id=%d original_intended_size=%r "
			"(<=0; engine-timeout sizing-deferred row) — cannot allocate "
			"entry fee proportionally; assigning full remaining fee %dc to "
			"this child",
			parent_id,
			p_orig_size,
			child_entry_fee,
		)

	# Outcome: won if exit beat entry, lost if worse, scratch if equal
	# (before fees — fees push a scratch to pnl <= 0).
	if exit_price_cents > entry_basis:
		outcome: str = "won"
	elif exit_price_cents < entry_basis:
		outcome = "lost"
	else:
		outcome = "scratch"
	pnl_cents = (
		closed_size * (exit_price_cents - entry_basis)
		- child_entry_fee
		- exit_fee_cents
	)

	# child_seq = (# existing -split-% children for this parent) + 1.
	# Monotonic row label only (Test #8 / spec §293) — NOT the dedup key;
	# duplicate detection already happened above on kalshi_exit_order_id.
	seq_row = conn.execute(
		"SELECT COUNT(*) FROM live_trades WHERE client_order_id LIKE ?",
		(f"{p_coid}-split-%",),
	).fetchone()
	child_seq = int(seq_row[0]) + 1
	child_coid = f"{p_coid}-split-{child_seq}"

	# Atomic two-step (Critical #2): parent decrement (commit=False) + child
	# INSERT under ONE transaction; single commit at the end; ANY failure
	# rolls BOTH back. The IntegrityError "already split" path also rolls
	# back the parent decrement (the old code's rollback was a no-op there
	# because _cas_update had already committed the parent).
	try:
		# Step 1: decrement the parent (CAS — parent must still be 'open').
		parent_changed = _cas_update(
			conn,
			row_id=parent_id,
			sql=(
				"UPDATE live_trades SET "
				"fill_size = fill_size - ?, intended_size = intended_size - ?, "
				"entry_fee_remaining_cents = entry_fee_remaining_cents - ?, "
				"reconciled_at_utc = ? "
				"WHERE id = ? AND status = 'open'"
			),
			params=(
				closed_size,
				closed_size,
				child_entry_fee,
				now_utc,
				parent_id,
			),
			transition=f"{p_status}->open(partial -{closed_size})",
			commit=False,
		)
		if not parent_changed:
			# Parent already left 'open' (settlement closed it, or a
			# concurrent split). Roll back (nothing committed yet) and do
			# NOT insert an orphan child row. _cas_update already logged the
			# lost-race WARNING; add the return-0 disambiguation.
			conn.rollback()
			log.warning(
				"record_partial_exit: parent id=%d CAS lost (no longer "
				"'open') — no child inserted (return 0: CAS-lost)",
				parent_id,
			)
			return 0

		# Step 2: INSERT the closed child.
		cur = conn.execute(
			"INSERT INTO live_trades ("
			"ticker, series, strategy, side, intended_size, "
			"original_intended_size, fill_size, entry_price_cents, "
			"blended_entry_cents, status, client_order_id, kalshi_order_id, "
			"placed_at_utc, entry_time, exit_time, exit_price_cents, "
			"pnl_cents, entry_fee_cents, entry_fee_remaining_cents, "
			"exit_fee_cents, exit_reason"
			") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)",
			(
				p_ticker,
				p_series,
				p_strategy,
				p_side,
				closed_size,
				closed_size,
				closed_size,
				entry_basis,
				entry_basis,
				outcome,
				child_coid,
				kalshi_exit_order_id,
				now_utc,
				p_entry_time,
				now_utc,
				exit_price_cents,
				pnl_cents,
				child_entry_fee,
				exit_fee_cents,
				exit_reason,
			),
		)
		conn.commit()
	except sqlite3.IntegrityError:
		# Backstop only: the kalshi_exit_order_id pre-check above already
		# handles genuine duplicate WS events. Reaching here means the
		# -split-N label collided (e.g. a concurrent writer — spec-stated
		# impossible under single-writer B, kept defensively). Roll back
		# BOTH parent decrement and the failed INSERT.
		conn.rollback()
		existing = conn.execute(
			"SELECT id FROM live_trades WHERE client_order_id = ?",
			(child_coid,),
		).fetchone()
		existing_id = int(existing[0]) if existing else 0
		log.warning(
			"record_partial_exit: split-id %s collided on UNIQUE despite "
			"kalshi_exit_order_id pre-check (concurrent writer?) — rolled "
			"back parent decrement, no-op (existing id=%d)",
			child_coid,
			existing_id,
		)
		return existing_id
	except sqlite3.Error:
		# Critical #2: any other DB failure (disk full, OperationalError,
		# I/O) on the child INSERT or the commit. The parent decrement is
		# NOT yet committed — roll it back so the parent keeps its full
		# contract count (no silent real-money under-accounting). Re-raise:
		# a partial-exit persistence failure must not be swallowed.
		conn.rollback()
		log.error(
			"record_partial_exit: DB failure during atomic split for "
			"parent=%d (child_coid=%s) — rolled back parent decrement (no "
			"silent contract loss); re-raising",
			parent_id,
			child_coid,
		)
		raise

	child_id = int(cur.lastrowid or 0)
	log.info(
		"live_trades id=%d partial-exit child of parent=%d %s closed=%d "
		"exit=%dc pnl=%dc alloc_fee=%dc coid=%s kalshi_exit=%s",
		child_id,
		parent_id,
		outcome,
		closed_size,
		exit_price_cents,
		pnl_cents,
		child_entry_fee,
		child_coid,
		kalshi_exit_order_id,
	)
	return child_id


# ---------------------------------------------------------------------------
# Transition writers (CAS-guarded) — driven by WS handlers / reconcile
# ---------------------------------------------------------------------------


def transition_pending_to_open(
	conn: sqlite3.Connection,
	row_id: int,
	*,
	kalshi_order_id: str,
	fill_size: int,
	blended_entry_cents: int,
	slippage_cents: int,
	fill_pct: float,
	entry_time: str,
	entry_fee_cents: int,
) -> None:
	"""UPDATE ``pending`` → ``open``. Called from the WS fill handler or
	reconcile when Kalshi confirms the entry filled.

	Sets ``entry_fee_remaining_cents = entry_fee_cents`` (initialized at fill
	time; decremented by record_partial_exit; consumed at record_close). Does
	NOT mutate ``original_intended_size`` (immutable after INSERT).
	``slippage_cents`` is consumed verbatim (from D's signed_slippage_cents);
	never recomputed here. CAS precondition: status='pending'. A lost race is
	a logged no-op.
	"""
	changed = _cas_update(
		conn,
		row_id=row_id,
		sql=(
			"UPDATE live_trades SET "
			"status = 'open', kalshi_order_id = ?, fill_size = ?, "
			"blended_entry_cents = ?, slippage_cents = ?, fill_pct = ?, "
			"entry_time = ?, entry_fee_cents = ?, "
			"entry_fee_remaining_cents = ? "
			"WHERE id = ? AND status = 'pending'"
		),
		params=(
			kalshi_order_id,
			fill_size,
			blended_entry_cents,
			slippage_cents,
			fill_pct,
			entry_time,
			entry_fee_cents,
			entry_fee_cents,
			row_id,
		),
		transition="pending->open",
	)
	# Important #6: gate the transition log on the CAS result (a lost race
	# already logged its WARNING in _cas_update — don't double-claim).
	if changed:
		log.info(
			"live_trades id=%d pending→open kalshi_id=%s fill=%d blended=%dc",
			row_id,
			kalshi_order_id,
			fill_size,
			blended_entry_cents,
		)


def transition_pending_to_rejected(
	conn: sqlite3.Connection,
	row_id: int,
	*,
	kalshi_order_id: str | None,
	rejection_reason: str,
) -> None:
	"""UPDATE ``pending`` → ``rejected`` (Kalshi rejected) or
	``rejected_post_hoc`` (TTL elapsed, no Kalshi order found).

	The terminal state is inferred from ``rejection_reason``: the reconciler
	passes ``'ttl_no_kalshi_order'`` for the TTL path (→ rejected_post_hoc);
	any other reason → rejected. CAS precondition: status='pending'. A lost
	race is a logged no-op. No RecordPendingFailed — the row exists; this is
	a terminal transition with no Kalshi-side position to strand.
	"""
	terminal = (
		"rejected_post_hoc"
		if rejection_reason == "ttl_no_kalshi_order"
		else "rejected"
	)
	changed = _cas_update(
		conn,
		row_id=row_id,
		sql=(
			"UPDATE live_trades SET "
			"status = ?, kalshi_order_id = COALESCE(?, kalshi_order_id), "
			"rejection_reason = ? "
			"WHERE id = ? AND status = 'pending'"
		),
		params=(terminal, kalshi_order_id, rejection_reason, row_id),
		transition=f"pending->{terminal}",
	)
	# Important #6: gate the transition log on the CAS result.
	if changed:
		log.info(
			"live_trades id=%d pending→%s reason=%s",
			row_id,
			terminal,
			rejection_reason,
		)


def transition_exit_pending_to_open(
	conn: sqlite3.Connection,
	row_id: int,
	*,
	notes: str = "exit reverted: TTL or rejected",
) -> None:
	"""UPDATE ``exit_pending`` → ``open`` (revert). The exit POST TTL'd with
	no Kalshi order, or reconcile found it rejected — the position is still
	alive and the strategy will retry the exit on the next tick.

	CAS precondition: status='exit_pending'. A lost race (settlement already
	closed the row, or the fill landed) is a logged no-op.
	"""
	changed = _cas_update(
		conn,
		row_id=row_id,
		sql=(
			"UPDATE live_trades SET status = 'open', "
			"notes = COALESCE(?, notes) "
			"WHERE id = ? AND status = 'exit_pending'"
		),
		params=(notes, row_id),
		transition="exit_pending->open",
	)
	# Important #6: gate the transition log on the CAS result.
	if changed:
		log.info(
			"live_trades id=%d exit_pending→open (revert) notes=%s",
			row_id,
			notes,
		)


def touch_reconciled(
	conn: sqlite3.Connection,
	row_id: int,
	*,
	now_utc: str,
) -> bool:
	"""UPDATE only ``reconciled_at_utc`` — the matrix row 6 "both agree on
	position" observability bump (spec §332: ``UPDATE reconciled_at_utc;
	continue``). Called from the reconciler whenever Kalshi confirms a local
	row is still legitimately active (a matched in-flight order, or an open
	row whose ticker Kalshi still holds a position for) and NO money-state
	transition is warranted — this is purely a last-verified timestamp, not a
	lifecycle move.

	``now_utc`` is the caller-supplied ISO-8601 UTC string (mirrors
	``record_partial_exit``'s ``now_utc`` convention — ``state.py`` never
	sources its own clock; the reconciler passes ``_now_utc().isoformat()``,
	exactly as it already does for ``transition_pending_to_open``'s
	``entry_time``).

	CAS precondition: status IN ('open', 'pending', 'exit_pending') — the same
	active-state set as :func:`mark_lost_truth`. ``reconciled_at_utc`` is only
	meaningful while a row can still be reconciled against a live Kalshi
	order/position; terminal rows (won/lost/scratch/rejected/
	rejected_post_hoc/cancelled/lost_truth) are NOT bumped. A precondition
	miss (the row went terminal between the reconciler's read and this write,
	or the row id does not exist) is a logged no-op — ``_cas_update`` emits
	the shared ``CAS lost race`` WARNING and returns ``False``; the reconciler
	treats that as the correct idempotent outcome (a row a concurrent
	settlement/fill already closed needs no reconcile stamp), never an error.

	Idempotent: calling it twice just rewrites the timestamp. Returns the
	``_cas_update`` boolean verbatim — ``True`` when the active-row stamp
	applied, ``False`` on the precondition-miss no-op.
	"""
	changed = _cas_update(
		conn,
		row_id=row_id,
		sql=(
			"UPDATE live_trades SET reconciled_at_utc = ? "
			"WHERE id = ? AND status IN ('open', 'pending', 'exit_pending')"
		),
		params=(now_utc, row_id),
		transition="reconciled_at_utc bump (row6 both-agree)",
	)
	# Important #6 parity: only log the bump when the CAS actually applied
	# (a precondition miss already logged its WARNING in _cas_update — an
	# unconditional INFO here would falsely claim a stamp that did not land).
	if changed:
		log.info(
			"live_trades id=%d reconciled_at_utc bumped (row6 both-agree) "
			"at=%s",
			row_id,
			now_utc,
		)
	return changed


def mark_lost_truth(
	conn: sqlite3.Connection,
	row_id: int,
	*,
	notes: str,
) -> None:
	"""UPDATE → ``lost_truth`` (terminal, alert). Reconcile-time only:
	startup reconcile found Kalshi has no record of a position we believe is
	open — manual investigation required.

	CAS precondition: status IN ('open', 'pending', 'exit_pending') — only an
	active row can become lost_truth. A lost race is a logged no-op.
	"""
	changed = _cas_update(
		conn,
		row_id=row_id,
		sql=(
			"UPDATE live_trades SET status = 'lost_truth', "
			"notes = COALESCE(?, notes) "
			"WHERE id = ? AND status IN ('open', 'pending', 'exit_pending')"
		),
		params=(notes, row_id),
		transition="active->lost_truth",
	)
	# Important #6: only raise the manual-investigation alarm when the row
	# actually moved to lost_truth (a lost race already logged its WARNING;
	# falsely demanding investigation on a no-op would page the operator).
	if changed:
		log.warning(
			"live_trades id=%d →lost_truth (Kalshi has no record) notes=%s — "
			"manual investigation required",
			row_id,
			notes,
		)
