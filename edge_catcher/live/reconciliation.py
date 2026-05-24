"""Ambiguous-state recovery for ``live_trades.db`` — sub-project B / v1.6.0
PR 5, Agent 4.B.

Three async surfaces, all scheduled by sub-project E on its event loop:

* :func:`startup_reconcile` — T0, before the WS subscribes. Pulls the
  authoritative Kalshi state (``positions()`` + a recency-bounded
  ``list_orders(min_ts=...)``) and applies the spec's 6-case decision
  matrix to every divergence (orphan position, lost-truth row, phantom
  pending, TTL'd exit).
* :func:`reconnect_reconcile` — the FAST subset run on WS reconnect. Resolves
  only ``pending`` / ``exit_pending`` rows; deliberately skips the
  ``positions()`` orphan scan (caught on the next full startup reconcile).
* :func:`poll_pending_rows_loop` — the continuous phantom-pending poller.
  ONE ``list_orders()`` call per cycle (never one-per-row), matched locally
  by ``client_order_id``; TTL'd rows resolve to their terminal/revert state.

Async/sync boundary (spec §11): the coroutines ``await`` the async
``KalshiOrderClient`` for I/O, then call sub-project 4.A's **sync**
``live.state`` write functions over the passed ``sqlite3.Connection``. Every
4.A transition is compare-and-swap by ``WHERE status IN (...)`` + a
``rowcount == 1`` check, so this module never re-implements precondition
checks — it calls the transition and trusts the CAS (a no-op because a
concurrent WS event already moved the row is correct behaviour, not an
error). The reconciler is **read-only with respect to ``client_order_id``s**
(locked PR #38 contract): it only *matches* existing
``live_trades.client_order_id`` values against Kalshi's ``list_orders()``
results — it never fabricates an id or emits a POST. Any actual retry /
forced-close is the strategy's normal next-tick path through D's builders.
"""
from __future__ import annotations

import asyncio
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Literal, Protocol

from edge_catcher.adapters.kalshi.fees import STANDARD_FEE
from edge_catcher.live.client import Order, Position
from edge_catcher.live.state import (
	mark_lost_truth,
	record_open,
	touch_reconciled,
	transition_exit_pending_to_open,
	transition_pending_to_open,
	transition_pending_to_rejected,
)

log = logging.getLogger(__name__)

# Own timeout constant (cross-PR contract #3 — do NOT reuse dispatch's
# ``_ENTRY_PLACEMENT_TIMEOUT_SECONDS``; that value is calibrated for entry
# placement and reusing its headroom would be a latent coupling bug).
#
# Recency bound for ``startup_reconcile``'s ``list_orders(min_ts=...)`` scan.
# ``list_orders`` is single-page; without a recency bound a genuine pending
# row's matching (possibly *filled*) Kalshi order can fall off page 1 after
# extended downtime → the matrix sees "no order" → marks it
# ``rejected_post_hoc`` → a stranded real-money position + phantom rejection
# (spec §316-324, client.py docstring). 6h comfortably covers a long process
# outage while staying well inside Kalshi's documented page size at Phase-1
# volume. Unix-seconds, matching ``adapters/kalshi/adapter.py``'s convention.
_RECONCILE_LOOKBACK_SECONDS: int = 6 * 60 * 60

# Kalshi resolved/terminal order states (``Order.status`` enum; client.py
# documents the wire values pending/resting/executed/canceled/rejected).
_KALSHI_FILLED = "executed"
_KALSHI_REJECTED_STATES = frozenset({"rejected", "canceled"})
_KALSHI_INFLIGHT_STATES = frozenset({"resting", "pending"})

# Passed to ``transition_pending_to_rejected`` on the TTL-no-order path; 4.A
# infers the ``rejected_post_hoc`` terminal state from exactly this string
# (live/state.py ``transition_pending_to_rejected``).
_TTL_NO_ORDER_REASON = "ttl_no_kalshi_order"

# Strategy/series sentinels for an orphan position Kalshi reports but we have
# no local row for. We cannot know which strategy placed it (the row that
# would carry that is exactly the one that is missing), so the recovered row
# is tagged unambiguously for the operator. Series is best-effort the Kalshi
# ticker prefix (``SERIES-...``) — the repo-wide convention
# (engine/dispatch.py ``ticker.startswith(series)``).
_ORPHAN_STRATEGY = "reconcile-orphan"


class _OrderClient(Protocol):
	"""Structural subset of ``KalshiOrderClient`` this module consumes.

	Declared as a Protocol so the production client AND the test counting
	double satisfy it without a shared base — and so mypy checks the exact
	kwargs (notably ``min_ts``) at every call site.
	"""

	async def positions(self) -> list[Position]: ...

	async def list_orders(
		self,
		*,
		status: str | None = ...,
		limit: int = ...,
		cursor: str | None = ...,
		min_ts: int | None = ...,
	) -> list[Order]: ...


class _BankrollCache(Protocol):
	"""Structural subset of ``engine.risk.BankrollCache`` — only ``refresh``
	is needed at reconcile time (E seeds C's cash before the WS subscribes)."""

	async def refresh(self) -> None: ...


@dataclass(frozen=True, slots=True)
class StartupReconcileReport:
	"""Immutable summary of one :func:`startup_reconcile` pass.

	Counts are per-call (a fully idempotent second pass returns all-zero
	action counts — the rows are already consistent). ``alerts`` is the
	operator-attention total: orphan recoveries + lost-truth marks (both
	demand manual investigation per the spec's matrix).
	"""

	pending_resolved: int = 0
	pending_post_hoc_rejected: int = 0
	orphan_positions_recovered: int = 0
	lost_truth: int = 0
	mismatches: int = 0
	alerts: int = 0


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _now_utc() -> datetime:
	return datetime.now(timezone.utc)


def _parse_iso(ts: str) -> datetime:
	"""Parse a ``placed_at_utc`` ISO-8601 string to an aware UTC datetime.

	Mirrors ``adapters/kalshi/adapter.py``'s convention (``fromisoformat``
	after normalising a trailing ``Z``). A naive value is assumed UTC (4.A
	always writes aware-UTC ``.isoformat()``; the coercion is defensive).
	"""
	dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
	if dt.tzinfo is None:
		dt = dt.replace(tzinfo=timezone.utc)
	return dt


def _clamp_fill_pct(fill_size: int, intended_size: int) -> float:
	"""Reconcile-side mirror of ``executors/live.py:_clamp_fill_pct``.

	Replicated (NOT imported) deliberately: importing the executor helper
	into the live reconciler would couple this layer to the engine's
	execution layer for a three-line pure function — the cross-PR contract
	keeps reconcile self-contained. The SEMANTICS must match that helper
	byte-for-byte so a reconcile-recovered row's ``fill_pct`` is consistent
	with the live-executor (WS) path:

	* ``intended_size <= 0`` → ``0.0`` (defence in depth; never a NaN from a
	  div-by-zero — the matched-pending guard already rejects a zero-fill
	  before this is called, but a zero ``intended_size`` must still be safe).
	* otherwise the **raw** ratio, NOT rounded (the executor helper returns
	  ``filled_count / size_contracts`` unrounded; the column is ``REAL`` and
	  F's slippage/partial-fill analytics read it as a probability — rounding
	  here would diverge from every WS-path row).
	* clamp the upper bound to ``1.0`` and WARNING on overfill (an IOC must
	  cap at ``count``; ``> 1.0`` would corrupt the analytics, mirroring the
	  executor's overfill log; the raw ``fill_size`` is still the truth of
	  record — only the derived ratio is clamped).
	"""
	if intended_size <= 0:
		return 0.0
	raw = fill_size / intended_size
	if raw > 1.0:
		log.warning(
			"reconcile: fill overfill fill_size=%d > intended_size=%d "
			"(fill_pct clamped to 1.0; fill_size preserved as truth of "
			"record)",
			fill_size,
			intended_size,
		)
		return 1.0
	return raw


def _kalshi_outcome(order: Order) -> Literal["open", "rejected"]:
	"""Map a Kalshi ``Order`` to the local resolution for a matched pending
	row, per the spec's matrix row 3.

	* ``executed`` (or any ``filled_count >= count``) → ``open``.
	* ``rejected`` / ``canceled`` → ``rejected``.
	* ``resting`` / ``pending`` → rare with our IOC entries (spec §338): log
	  a WARNING and treat as ``rejected`` defensively (a still-resting entry
	  we did not get a fill event for is safer un-booked than phantom-open;
	  the strategy re-enters on the next tick if the edge still holds).
	"""
	if order.status == _KALSHI_FILLED or (
		order.count > 0 and order.filled_count >= order.count
	):
		return "open"
	if order.status in _KALSHI_REJECTED_STATES:
		return "rejected"
	if order.status in _KALSHI_INFLIGHT_STATES:
		log.warning(
			"reconcile: Kalshi order %s for coid=%s is still %r (rare with "
			"IOC entries) — treating as rejected defensively",
			order.order_id,
			order.client_order_id,
			order.status,
		)
		return "rejected"
	# Unknown future status — fail safe to rejected (never phantom-open a
	# row off a status we do not understand; real-money zero-error lens).
	log.warning(
		"reconcile: Kalshi order %s for coid=%s has unrecognised status %r "
		"— treating as rejected defensively",
		order.order_id,
		order.client_order_id,
		order.status,
	)
	return "rejected"


def _resolve_matched_pending(
	conn: sqlite3.Connection,
	*,
	row_id: int,
	status: str,
	order: Order,
	intended_size: int,
) -> Literal["resolved", "noop"]:
	"""Apply the matched-order branch (matrix row 3) for one local row.

	``pending``: Kalshi-filled → :func:`transition_pending_to_open`;
	Kalshi-rejected → :func:`transition_pending_to_rejected`.
	``exit_pending``: a Kalshi-rejected exit order → revert to ``open`` (the
	position is still alive; the strategy retries the exit). A *filled* exit
	order is the WS fill-handler's job (4.C) — the reconciler does not
	synthesise the won/lost/scratch close P&L; it leaves a filled exit for
	the WS path / settlement and only handles the revert side here.

	``intended_size`` is the pending row's original size (carried by
	:func:`_read_rows`); the recovered ``fill_pct`` is the TRUE
	``fill_size / intended_size`` fraction (:func:`_clamp_fill_pct`) so a
	partial IOC fill — still reported ``status='executed'`` by Kalshi — is
	not mis-recorded as a clean 100% fill. The row's ``intended_size`` itself
	is never mutated (the original value is correct; only the derived ratio
	is computed from it).

	Returns ``"resolved"`` when this call performed the resolving action,
	``"noop"`` when nothing actionable applied (lets the caller skip the
	stale-pending TTL branch for an already-handled row).
	"""
	outcome = _kalshi_outcome(order)
	if status == "pending":
		if outcome == "open":
			# M1 — zero-fill defense. ``_kalshi_outcome`` returns ``"open"``
			# off ``status=='executed'`` BEFORE its ``filled_count >= count``
			# check, so ANY ``executed`` order that genuinely filled zero
			# contracts (``filled_count == 0`` — whether ``count`` is 0 OR
			# positive) reaches here with ``outcome == "open"``. ``fill_size``
			# is therefore the TRUE ``filled_count`` and NEVER ``or
			# order.count``: that fallback evaluated ``0 or count == count``
			# for a real zero-fill, booking a phantom count-sized ``open``
			# that never drains — no WS event is emitted for a zero-fill
			# order, and every later reconcile re-matches it ``executed`` so
			# the TTL branch is never reached: an unbounded MAX_OPEN slot
			# leak with wrong mark-to-market equity and no operator signal.
			# An effective zero fill is routed to a defensive rejection
			# instead (clear, distinct reason; operator WARNING) so the slot
			# is freed and the anomaly is visible.
			fill_size = order.filled_count
			if fill_size <= 0:
				log.warning(
					"reconcile: matched Kalshi order %s for coid=%s is "
					"'executed' but has an effective zero fill "
					"(filled_count=%d count=%d) — rejecting defensively "
					"(reconcile_zero_fill) to avoid a never-draining "
					"phantom 'open' (MAX_OPEN slot leak)",
					order.order_id,
					order.client_order_id,
					order.filled_count,
					order.count,
				)
				transition_pending_to_rejected(
					conn,
					row_id,
					kalshi_order_id=order.order_id,
					rejection_reason="reconcile_zero_fill",
				)
				return "resolved"
			# Cost basis: prefer Kalshi's TRUE volume-weighted fill price
			# (``avg_fill_price_cents``, derived from the order's aggregate
			# ``taker_fill_cost_dollars`` — present on the list_orders / status
			# responses the reconciler reads), and fall back to the IOC
			# ``limit_price_cents`` only when the response carried no usable
			# fill cost.
			#
			# A1 — untrustworthy-price defense. A live binary-market fill is
			# never ``0``¢ (valid prices are 1–99¢; 0/100¢ are settlement), so a
			# non-positive basis means "no trustworthy price". Booking
			# ``pending→open`` at a ``0``¢ basis would silently mislabel
			# won/lost and corrupt P&L on every reconcile-recovered row — and
			# ``record_partial_exit``'s ``blended is None`` guard does NOT catch
			# a non-NULL ``0``. Never fabricate a basis: leave the row
			# ``pending`` (return ``"noop"``) — NOT rejected (the order matched
			# and filled, so Kalshi holds the contracts; rejecting would orphan
			# a real position). The caller leaves a young row untouched (it
			# retries on the next reconcile / its WS fill) and TTLs a stale one,
			# after which the orphan/``positions()`` path recovers the real
			# position with Kalshi's trustworthy ``average_price_cents``.
			# Mirrors ``on_fill_event``'s "no trustworthy price → no-op,
			# reconcile owns recovery" posture (zero-error lens).
			blended = order.avg_fill_price_cents or order.limit_price_cents
			if blended <= 0:
				log.warning(
					"reconcile: matched Kalshi order %s for coid=%s is "
					"'executed' (filled_count=%d) but exposes no trustworthy "
					"price (avg_fill=%d limit=%d) — leaving the row 'pending' "
					"(NOT phantom-opening at a 0c basis, NOT rejecting a "
					"really-held position); recovered later via the WS fill / "
					"next reconcile / positions()",
					order.order_id,
					order.client_order_id,
					order.filled_count,
					order.avg_fill_price_cents,
					order.limit_price_cents,
				)
				return "noop"
			# Entry fee MUST be computed here (spec §283 — B computes the
			# fee via STANDARD_FEE at row-write time; this resolution IS the
			# entry-fill write that seeds entry_fee_cents /
			# entry_fee_remaining_cents). Passing 0 would under-charge the
			# entry and overstate P&L on every reconcile-recovered fill.
			# ``calculate`` already returns ceil'd cents; the column is
			# INTEGER so round to int.
			entry_fee = int(round(STANDARD_FEE.calculate(blended, fill_size)))
			# fill_pct is the TRUE fraction (spec ~L126 / DDL 0003:
			# ``fill_size / intended_size``), mirroring
			# ``executors/live.py:_clamp_fill_pct`` so a reconcile-recovered
			# row is consistent with the WS-path row. A hardcoded ``1.0``
			# here mis-reports a partial IOC fill (Kalshi marks a partial
			# 'executed' too) as a clean 100% fill and defeats F's
			# slippage/partial-fill analytics on exactly the rows
			# reconciliation exists to recover.
			fill_pct = _clamp_fill_pct(fill_size, intended_size)
			# slippage is left 0: the REST Order exposes no fill-vs-limit
			# delta, so a non-zero value would be fabricated. The WS path
			# records real signed slippage; reconcile cannot measure it.
			transition_pending_to_open(
				conn,
				row_id,
				kalshi_order_id=order.order_id,
				fill_size=fill_size,
				blended_entry_cents=blended,
				slippage_cents=0,
				fill_pct=fill_pct,
				entry_time=_now_utc().isoformat(),
				entry_fee_cents=entry_fee,
			)
		else:
			transition_pending_to_rejected(
				conn,
				row_id,
				kalshi_order_id=order.order_id,
				rejection_reason=f"reconcile_kalshi_status:{order.status}",
			)
		return "resolved"
	if status == "exit_pending":
		# Only the rejected/canceled exit reverts here. A filled exit is the
		# WS handler's close path (4.C) — not reconstructed in 4.B.
		if outcome == "rejected":
			transition_exit_pending_to_open(
				conn,
				row_id,
				notes=(
					f"exit reverted: reconcile found exit order "
					f"{order.order_id} {order.status}"
				),
			)
			return "resolved"
		return "noop"
	return "noop"


def _series_from_ticker(ticker: str) -> str:
	"""Best-effort series for an orphan-recovered row: the Kalshi ticker
	prefix before the first ``-`` (repo convention — engine/dispatch.py
	matches strategies by ``ticker.startswith(series)``)."""
	return ticker.split("-", 1)[0] if "-" in ticker else ticker


def _read_rows(
	conn: sqlite3.Connection, statuses: tuple[str, ...]
) -> list[tuple[int, str, str, str, int]]:
	"""``(id, status, client_order_id, placed_at_utc, intended_size)`` for the
	given statuses. The reconciler only ever needs these five columns.

	``intended_size`` is carried so the matched-pending→open resolution can
	write the TRUE ``fill_pct = fill_size / intended_size`` (DDL 0003 / spec
	~L126) instead of a hardcoded ``1.0`` — a partial IOC fill is still
	reported ``status='executed'`` by Kalshi, so without the real fraction a
	3-of-10 reconcile-recovered fill is mis-reported as a clean 100% fill and
	slippage/partial-fill analysis on exactly the rows reconciliation exists
	to recover is defeated. A ``pending`` row is pre-fill (``fill_size=0``,
	never partial-exited), so its ``intended_size`` still equals the
	INSERT-time ``original_intended_size`` — the spec-locked-immutable value
	the live-executor path also divides by (``req.size_contracts``)."""
	placeholders = ",".join("?" for _ in statuses)
	return [
		(int(r[0]), str(r[1]), str(r[2]), str(r[3]), int(r[4]))
		for r in conn.execute(
			f"SELECT id, status, client_order_id, placed_at_utc, "
			f"intended_size "
			f"FROM live_trades WHERE status IN ({placeholders})",
			statuses,
		).fetchall()
	]


def _reconcile_rows_against_orders(
	conn: sqlite3.Connection,
	rows: list[tuple[int, str, str, str, int]],
	orders_by_coid: dict[str, Order],
	*,
	ttl_seconds: float,
	now: datetime,
	ttl_log_level: int = logging.INFO,
) -> tuple[int, int]:
	"""Core matrix rows 3-5 for ``pending`` / ``exit_pending`` rows.

	For each row: if a Kalshi order matches its ``client_order_id``, resolve
	per :func:`_resolve_matched_pending`. Otherwise, if the row is older than
	``ttl_seconds``, TTL it (``pending`` → ``rejected_post_hoc`` via the
	``ttl_no_kalshi_order`` reason; ``exit_pending`` → revert to ``open``).
	A row younger than its TTL with no match is left untouched — it may
	still get its WS event (real-money: never reject a young in-flight row).

	``ttl_log_level`` (M2): the pending TTL→``rejected_post_hoc`` line is
	logged at this level. The steady-state 30s poller / reconnect leaves it
	at the ``INFO`` default (a routine TTL is not anomalous); only
	:func:`startup_reconcile` raises it to ``WARNING`` — a pending row still
	stale at *boot* is operator-actionable (it never got its event across a
	full process lifetime, not just one poll gap). State transition,
	counters and control flow are identical regardless of the level.

	Returns ``(pending_resolved, pending_ttl_actioned)``.
	"""
	resolved = 0
	ttl_actioned = 0
	for row_id, status, coid, placed_at, intended_size in rows:
		order = orders_by_coid.get(coid)
		if order is not None:
			outcome = _resolve_matched_pending(
				conn,
				row_id=row_id,
				status=status,
				order=order,
				intended_size=intended_size,
			)
			# Matrix row 6 ("both agree on position"): a matched Kalshi order
			# is Kalshi *confirming* this row's order exists — the spec's
			# "both agree → UPDATE reconciled_at_utc; continue". Stamp the
			# last-verified timestamp via 4.A's CAS-guarded helper. It is
			# safe to call unconditionally for every matched row: the
			# helper's own WHERE status IN ('open','pending','exit_pending')
			# predicate stamps a still-active row (pending→open resolution,
			# an exit_pending whose exit is still working) and is a logged
			# no-op when the resolution drove the row terminal
			# (pending→rejected) — exactly the row-6 semantics, with zero
			# change to the resolve/TTL control flow below.
			touch_reconciled(conn, row_id, now_utc=now.isoformat())
			if outcome == "resolved":
				resolved += 1
				continue
			# Matched but not actionable (e.g. exit_pending whose exit is
			# still working) — fall through to the TTL check below so a
			# genuinely stale exit_pending can still revert.
		age = (now - _parse_iso(placed_at)).total_seconds()
		if age < ttl_seconds:
			continue
		if status == "pending":
			transition_pending_to_rejected(
				conn,
				row_id,
				kalshi_order_id=None,
				rejection_reason=_TTL_NO_ORDER_REASON,
			)
			# M2: WARNING on the startup path (stale at boot is
			# operator-actionable), INFO on the steady-state poller.
			log.log(
				ttl_log_level,
				"reconcile: pending id=%d past TTL (%.0fs > %.0fs) with no "
				"Kalshi order — rejected_post_hoc",
				row_id,
				age,
				ttl_seconds,
			)
			ttl_actioned += 1
		elif status == "exit_pending":
			transition_exit_pending_to_open(
				conn,
				row_id,
				notes="exit reverted: TTL elapsed, no Kalshi order found",
			)
			log.info(
				"reconcile: exit_pending id=%d past TTL (%.0fs > %.0fs) with "
				"no Kalshi order — reverted to open (strategy retries)",
				row_id,
				age,
				ttl_seconds,
			)
			ttl_actioned += 1
	return resolved, ttl_actioned


async def _reconcile_pending_batch(
	client: _OrderClient,
	conn: sqlite3.Connection,
	ttl_seconds: float,
) -> tuple[int, int]:
	"""ONE ``list_orders()`` call, then match every local ``pending`` /
	``exit_pending`` row locally by ``client_order_id``.

	The single-call invariant is the heart of the phantom-pending design
	(spec §367-388 / test #19): N pending rows must NOT produce N REST calls
	— it is exactly one batched scan per cycle, matched in-process.

	``min_ts`` is intentionally omitted here: the reconnect path and the 30s
	poller operate on a working set that is small by construction at Phase-1
	volume (far below one Kalshi page over a few minutes), so the default
	unbounded-but-newest-first page is sufficient. Only ``startup_reconcile``
	(which may run after extended downtime) MUST bound the scan with
	``min_ts`` — see :func:`startup_reconcile`.

	Returns ``(pending_resolved, pending_ttl_actioned)``.
	"""
	rows = _read_rows(conn, ("pending", "exit_pending"))
	# Always issue the single batched call even when there are no rows: keeps
	# the per-cycle REST shape invariant (test #19) and is one cheap GET.
	orders = await client.list_orders()
	orders_by_coid: dict[str, Order] = {
		o.client_order_id: o
		for o in orders
		if o.client_order_id is not None
	}
	return _reconcile_rows_against_orders(
		conn,
		rows,
		orders_by_coid,
		ttl_seconds=ttl_seconds,
		now=_now_utc(),
	)


# ---------------------------------------------------------------------------
# (a) Startup reconcile
# ---------------------------------------------------------------------------


async def startup_reconcile(
	client: _OrderClient,
	db: sqlite3.Connection,
	bankroll_cache: _BankrollCache,
) -> StartupReconcileReport:
	"""Pull the authoritative Kalshi state at T0 (before the WS subscribes)
	and resolve every divergence via the spec's 6-case decision matrix.

	Steps (spec §305-329):

	1. ``await bankroll_cache.refresh()`` — seed C's cash. **FATAL** if it
	   fails: a live engine that cannot read its balance must not proceed
	   (the exception propagates; the engine bootstrap aborts before any
	   Kalshi state is touched).
	2. ``positions = await client.positions()``.
	3. ``orders = await client.list_orders(min_ts=<now − lookback>)`` — the
	   recency bound is **mandatory** here (single-page scan; an unbounded
	   scan after extended downtime can drop a filled order off page 1 →
	   phantom ``rejected_post_hoc`` + stranded real-money position).
	4. Matrix rows 3-5: resolve / TTL every ``pending`` / ``exit_pending``
	   row against the matched order.
	5. Matrix row 1: a Kalshi position with no local row → INSERT ``open``
	   (orphan recovery) + alert.
	6. Matrix row 2: a local ``open`` row whose ticker Kalshi has no
	   position for → :func:`mark_lost_truth` + alert.
	7. Matrix row 6: rows that already agree need no action (the all-zero
	   second-pass guarantees idempotency).

	Returns a :class:`StartupReconcileReport` with per-call counts.
	"""
	# Step 1 — cash seed. FATAL on failure (propagate; do NOT swallow).
	await bankroll_cache.refresh()

	# Steps 2-3 — authoritative Kalshi state. The recency bound is the
	# real-money mandate (cross-PR contract): Unix seconds, matching
	# ``adapters/kalshi/adapter.py``'s ``int(dt.timestamp())`` convention.
	now = _now_utc()
	min_ts = int(
		(now - timedelta(seconds=_RECONCILE_LOOKBACK_SECONDS)).timestamp()
	)
	positions = await client.positions()
	orders = await client.list_orders(min_ts=min_ts)

	report = _apply_startup_matrix(
		db, positions=positions, orders=orders, now=now
	)
	log.info(
		"startup_reconcile complete: pending_resolved=%d "
		"rejected_post_hoc=%d orphans_recovered=%d lost_truth=%d alerts=%d",
		report.pending_resolved,
		report.pending_post_hoc_rejected,
		report.orphan_positions_recovered,
		report.lost_truth,
		report.alerts,
	)
	return report


def _apply_startup_matrix(
	db: sqlite3.Connection,
	*,
	positions: list[Position],
	orders: list[Order],
	now: datetime,
) -> StartupReconcileReport:
	"""The sync core of :func:`startup_reconcile` (all six matrix rows).

	Split out so the I/O (``await``) and the pure SQL stay on opposite sides
	of a clean boundary and the matrix is unit-testable in isolation.
	"""
	orders_by_coid: dict[str, Order] = {
		o.client_order_id: o
		for o in orders
		if o.client_order_id is not None
	}

	# --- Matrix rows 3-5: pending / exit_pending resolution + TTL.
	pending_rows = _read_rows(db, ("pending", "exit_pending"))
	resolved, ttl_actioned = _reconcile_rows_against_orders(
		db,
		pending_rows,
		orders_by_coid,
		# Startup reconcile uses the same 90s phantom TTL as the poller
		# (spec §326-327 "older than TTL"); a row this old at boot truly
		# never got its event.
		ttl_seconds=90.0,
		now=now,
		# M2: a pending row still stale at *boot* (TTL'd by startup) is
		# operator-actionable — surface it at WARNING. The steady-state
		# poller path keeps the INFO default.
		ttl_log_level=logging.WARNING,
	)

	# --- Matrix row 6 (both agree on position): "UPDATE reconciled_at_utc;
	# continue" — a last-verified OBSERVABILITY timestamp, NOT a money-state
	# action (rows 1-5 carry all the correctness-critical transitions). The
	# bump goes through 4.A's CAS-guarded ``touch_reconciled`` helper so this
	# module still never reimplements a 4.A write. It is applied in the
	# lost-truth loop below: an open row whose ticker Kalshi DOES still hold
	# a position for is precisely the both-agree case (the else of the
	# lost-truth branch). Stamping it there touches each agreeing open row
	# exactly once per pass and is idempotent (a 2nd pass just re-touches).
	#
	# --- Matrix row 1: Kalshi has a position, we have no row → INSERT open.
	# --- Matrix row 2: we have an open row, Kalshi has no position → lost.
	# Build the local-open ticker set ONCE.
	open_rows = [
		(int(r[0]), str(r[1]))
		for r in db.execute(
			"SELECT id, ticker FROM live_trades WHERE status = 'open'"
		).fetchall()
	]
	local_open_tickers = {t for _, t in open_rows}
	kalshi_tickers = {p.ticker for p in positions}

	orphans_recovered = 0
	for pos in positions:
		if pos.ticker in local_open_tickers:
			continue  # matrix row 6 — both agree, no action.
		# Orphan: Kalshi holds a position we have no open row for. INSERT an
		# ``open`` row so C's gate counts it and the operator can see it.
		#
		# Idempotency / false-halt guard: the synthetic id is deterministic
		# (``_orphan_coid``), so a re-run normally short-circuits on the
		# ``local_open_tickers`` check above. But if an operator has since
		# closed/cancelled the recovered row (status no longer 'open') while
		# Kalshi still reports the position, the ticker check would miss it
		# and ``record_open`` would hit the ``UNIQUE(client_order_id)``
		# constraint → ``RecordPendingFailed`` → the engine would HALT on a
		# benign operator action (a false-positive outage on live money).
		# Guard with an explicit any-status existence check on the synthetic
		# id: if this orphan was already recovered once, recovery is a pure
		# no-op — reconcile recovers an orphan exactly once and never
		# re-opens (or crashes on) a row the operator has already actioned.
		coid = _orphan_coid(pos)
		if (
			db.execute(
				"SELECT 1 FROM live_trades WHERE client_order_id = ?",
				(coid,),
			).fetchone()
			is not None
		):
			log.info(
				"startup_reconcile: orphan %s already recovered "
				"(coid=%s exists, any status) — idempotent no-op",
				pos.ticker,
				coid,
			)
			continue
		# Entry fee from STANDARD_FEE (spec §283) on the position's average
		# cost basis — an orphan still incurred Kalshi's taker fee; a 0 here
		# would overstate P&L when the operator later reconciles it.
		orphan_entry_fee = int(
			round(
				STANDARD_FEE.calculate(pos.average_price_cents, pos.count)
			)
		)
		record_open(
			db,
			ticker=pos.ticker,
			series=_series_from_ticker(pos.ticker),
			strategy=_ORPHAN_STRATEGY,
			side=pos.side,
			intended_size=pos.count,
			fill_size=pos.count,
			entry_price_cents=pos.average_price_cents,
			blended_entry_cents=pos.average_price_cents,
			slippage_cents=0,
			fill_pct=1.0,
			stop_loss_distance_cents=0,
			client_order_id=coid,
			kalshi_order_id=coid,
			placed_at_utc=now.isoformat(),
			entry_time=now.isoformat(),
			entry_fee_cents=orphan_entry_fee,
		)
		orphans_recovered += 1
		log.warning(
			"startup_reconcile: ORPHAN position recovered — Kalshi holds "
			"%s %s x%d @ %dc but no local row existed; inserted an 'open' "
			"row (strategy=%s). MANUAL INVESTIGATION: which strategy placed "
			"this?",
			pos.ticker,
			pos.side,
			pos.count,
			pos.average_price_cents,
			_ORPHAN_STRATEGY,
		)

	lost_truth_count = 0
	for row_id, ticker in open_rows:
		if ticker in kalshi_tickers:
			# Matrix row 6 — both agree (Kalshi still holds this ticker's
			# position). Bump the last-verified observability timestamp via
			# 4.A's CAS-guarded helper, then continue (no money-state move).
			touch_reconciled(db, row_id, now_utc=now.isoformat())
			continue
		mark_lost_truth(
			db,
			row_id,
			notes=(
				f"startup reconcile: Kalshi reports no position for "
				f"{ticker} but local row was 'open' — manual investigation"
			),
		)
		lost_truth_count += 1

	alerts = orphans_recovered + lost_truth_count
	return StartupReconcileReport(
		pending_resolved=resolved,
		pending_post_hoc_rejected=ttl_actioned,
		orphan_positions_recovered=orphans_recovered,
		lost_truth=lost_truth_count,
		mismatches=lost_truth_count,
		alerts=alerts,
	)


def _orphan_coid(pos: Position) -> str:
	"""Deterministic synthetic ``client_order_id`` for an orphan-recovered
	row so a second startup pass is idempotent (the row is found by ticker
	on re-run; this id keeps the UNIQUE constraint satisfied and the row
	self-describing as reconcile-sourced). NOT POSTed to Kalshi — orphan
	rows are recovery records, never re-placed (locked PR #38 read-only-id
	contract; this id never reaches ``KalshiOrderClient.place``)."""
	return f"reconcile-orphan-{pos.ticker}-{pos.side}"


# ---------------------------------------------------------------------------
# (b) WS reconnect reconcile
# ---------------------------------------------------------------------------


async def reconnect_reconcile(
	client: _OrderClient,
	db: sqlite3.Connection,
) -> None:
	"""FAST subset run on WS reconnect (spec §343-353).

	Resolves only ``pending`` / ``exit_pending`` rows that fired during the
	dropout. **Deliberately skips** the ``positions()`` orphan scan — a
	short reconnect cannot have produced a brand-new untracked Kalshi
	position, and the scan is caught on the next full
	:func:`startup_reconcile` (typically the next process restart). This
	keeps reconnect cheap (one ``list_orders`` call, no ``positions`` call)
	so a flapping WS does not hammer the REST API. Awaited by 4.C's WS
	reconnect handler.
	"""
	resolved, ttl_actioned = await _reconcile_pending_batch(
		client, db, ttl_seconds=90.0
	)
	log.info(
		"reconnect_reconcile complete: pending_resolved=%d "
		"ttl_actioned=%d (position-orphan scan skipped — deferred to next "
		"full startup reconcile)",
		resolved,
		ttl_actioned,
	)


# ---------------------------------------------------------------------------
# (c) Phantom-pending poller
# ---------------------------------------------------------------------------


async def poll_pending_rows_loop(
	client: _OrderClient,
	db: sqlite3.Connection,
	poll_interval: float = 30.0,
	ttl_seconds: float = 90.0,
) -> None:
	"""Continuous phantom-pending poller (spec §355-384). Scheduled by E as
	a background task on its event loop.

	Every ``poll_interval`` seconds: sleep, then ONE
	:func:`_reconcile_pending_batch` cycle (exactly one ``list_orders()``
	REST call, matched locally). Pending rows older than ``ttl_seconds`` with
	no Kalshi match resolve to ``rejected_post_hoc``; ``exit_pending`` rows
	revert to ``open`` (the strategy retries the exit on the next tick).

	The loop NEVER dies from a single bad cycle: each cycle body is wrapped
	in ``try/except Exception`` that logs and continues to the next cycle.
	``asyncio.CancelledError`` is NOT caught (clean shutdown must propagate);
	``RecordPendingFailed`` is NOT caught (it is never raised by the
	transition functions this loop calls — only by ``record_pending`` /
	``record_open`` in the dispatch path — but were it ever to surface here
	it must propagate as the ghost-reject defense, never be swallowed).
	"""
	log.info(
		"phantom-pending poller started (interval=%.0fs ttl=%.0fs)",
		poll_interval,
		ttl_seconds,
	)
	while True:
		await asyncio.sleep(poll_interval)
		try:
			resolved, ttl_actioned = await _reconcile_pending_batch(
				client, db, ttl_seconds
			)
			if resolved or ttl_actioned:
				log.info(
					"phantom-pending poll cycle: resolved=%d ttl_actioned=%d",
					resolved,
					ttl_actioned,
				)
		except asyncio.CancelledError:
			# Clean shutdown — propagate, do NOT swallow.
			log.info("phantom-pending poller cancelled — exiting cleanly")
			raise
		except Exception:
			# One bad cycle must not kill the poller; the next cycle retries
			# (spec §379-384). exc_info for the operator log.
			log.error(
				"phantom-pending poller cycle error — continuing to next "
				"cycle",
				exc_info=True,
			)
