"""Offline maker-fill model + resting-order lifecycle (Phase 2a).

CLAIM BOUNDARY (binding — SPEC §7): this model is a conservative LOWER BOUND
on fill attainability (fill rate, time-to-fill, fillable size) plus a
mark-out instrument. Its verdicts may ONLY be used to (a) REJECT a maker
lead offline or (b) size a live probe's expectations. It must NEVER be
cited as evidence that a maker lead is profitable or that its fills are
benign — adverse selection and true fill quality are live-only questions
(fill_realism_gate is the GRADUATE/REJECT authority).

This module is the engine-level home for resting (maker/GTC) order state,
shared by paper, replay, and — in Phase 2b — live:

  - ``Print`` — one normalized taker trade print (yes-terms). Dispatch
    constructs these from WS trade messages; nothing here sees wire shapes.
  - ``RestingOrder`` — the in-memory record of one in-flight resting order,
    including the SPEC §5.1 ``deadline_ts`` fill-eligibility boundary.
  - ``QueueFillModel`` — the queue-honest FIFO fill model (SPEC §7). A pure
    per-order print-consumer; lifecycle/deadline enforcement belongs to the
    tracker (SPEC §5.1 validity window), NOT the model.

``RestingOrderTracker`` (SPEC §5 state machine, ledger, serialization) lands
in this module next — the model below is deliberately tracker-agnostic so
the two compose without circular knowledge.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from typing import Callable, Literal, cast

log = logging.getLogger(__name__)

# Mark-out sampling offsets after each fill, in seconds (SPEC §7.5).
MARKOUT_OFFSETS_S: tuple[int, ...] = (30, 120, 300)

# Terminal RestingOrder states — absorb duplicate events (SPEC §5.4).
_TERMINAL_STATES = frozenset({"filled", "cancelled", "cancelled_partial", "errored"})


@dataclass
class Print:
	"""One normalized taker trade print, in yes-terms.

	Dispatch (the paper/replay drivers) normalizes each WS trade message
	into this shape before handing it to the tracker; the model never sees
	raw wire fields. ``taker_side`` is ``str | None`` deliberately — the
	wire field is optional (``market_state`` reads it via ``.get``), and a
	missing side must reach the model so it can be COUNTED as degenerate
	rather than silently guessed (SPEC §7.8).
	"""

	ts: float
	yes_price_cents: int
	size: float
	taker_side: str | None


@dataclass
class RestingOrder:
	"""One in-flight resting (maker/GTC) order — engine-level state.

	In-memory in paper/replay (bundle-snapshotted at rotation, SPEC §5.5);
	Phase 2b maps the live analogue onto ``live_trades`` ``pending``+``gtc``
	rows (SPEC §4.5). ``queue_ahead`` is INITIALIZED by dispatch at
	registration from the book's visible resting depth at the order's level;
	the model only ever DECREMENTS it via at-level prints (SPEC §7.3
	pessimism — never via book-delta shrinkage, so cancels ahead of us are
	deliberately not credited).
	"""

	client_order_id: str
	order_id: str
	ticker: str
	series: str
	strategy: str
	side: str                       # "yes" | "no" — validated upstream (invalid_maker_signal:side)
	rest_price_cents: int
	intended_size: int
	filled_size: int
	placed_ts: float
	expires_ts: float
	market_close_ts: float | None   # None => TTL-only deadline (SPEC §5 internals)
	cancel_before_close_seconds: int | None
	trade_id: int | None            # set by dispatch on first booked fill
	queue_ahead: float              # model state (paper/replay only)
	state: str                      # resting|partially_filled|filled|cancelled|cancelled_partial|errored

	@property
	def deadline_ts(self) -> float:
		"""The SPEC §5.1 fill-eligibility boundary.

		``min(expires_ts, market_close_ts − cancel_before_close_seconds*,
		market_close_ts)`` — the ``*`` term participates only when
		``cancel_before_close_seconds`` is set, and both close terms only
		when ``market_close_ts`` is known. A close-window signal without a
		close_ts is REJECTED upstream (``invalid_maker_signal:no_close_ts``)
		so that combination never reaches here. A print with
		``ts >= deadline_ts`` can NEVER fill this order (tracker-enforced);
		the boundary itself is conservative — ``ts == deadline_ts`` does
		not fill.
		"""
		candidates = [self.expires_ts]
		if self.market_close_ts is not None:
			candidates.append(self.market_close_ts)
			if self.cancel_before_close_seconds is not None:
				candidates.append(self.market_close_ts - self.cancel_before_close_seconds)
		return min(candidates)

	@property
	def remaining(self) -> int:
		"""Unfilled contract count (``intended_size − filled_size``)."""
		return self.intended_size - self.filled_size


class QueueFillModel:
	"""SPEC §7 queue-honest FIFO fill model.

	A pure per-order print-consumer: ``consume`` mutates ONLY the order's
	``queue_ahead`` and returns the whole-contract fill count for the print.
	Fill BOOKKEEPING (``filled_size``, state transitions, the ledger) is the
	tracker's job. Deadline checks are the tracker's job too (SPEC §5.1
	validity window) — ``consume`` never looks at time, so the model stays a
	deterministic pure function of (order params, print stream).

	Pessimism locked (SPEC §7.3): ``queue_ahead`` decrements ONLY via prints
	at our level — never via book-delta shrinkage — and no latency credit is
	given anywhere. Degenerate prints (SPEC §7.8: unknown taker side,
	non-positive size, out-of-band price) NEVER fill and are counted on
	``degenerate_count`` for the report's data-quality table.

	Level arithmetic (SPEC §6/§7.2, both orientations unit-tested): a resting
	bid for ``side`` at price ``q`` is filled by takers on the COUNTER side
	crossing our implied-ask level ``L = 100 − q`` (counter-side cents). A
	print at exactly ``L`` trades AT our level — FIFO: the visible queue
	ahead of us consumes first, the remainder fills us. A print STRICTLY
	beyond ``L`` means deeper levels traded, so ours must have been swept —
	the order's remaining size fills in full.
	"""

	def __init__(self) -> None:
		self.degenerate_count = 0

	def consume(self, order: RestingOrder, p: Print) -> int:
		"""Whole contracts of *order* filled by print *p* (0 if none).

		Never mutates ``order.filled_size``; never returns a negative count
		or more than ``order.remaining``. Mutates ``order.queue_ahead``
		(downward only) when the print trades at our level.
		"""
		if p.taker_side not in ("yes", "no") or p.size <= 0 or not (1 <= p.yes_price_cents <= 99):
			self.degenerate_count += 1
			return 0
		counter_side = "yes" if order.side == "no" else "no"
		if p.taker_side != counter_side:
			return 0
		level = 100 - order.rest_price_cents          # our level in counter-side terms
		paid = p.yes_price_cents if counter_side == "yes" else 100 - p.yes_price_cents
		if paid < level:
			return 0
		if paid > level:                              # swept through our level
			return order.remaining
		# At-level: FIFO — the queue ahead of us consumes the print first.
		available = p.size - order.queue_ahead
		order.queue_ahead = max(0.0, order.queue_ahead - p.size)
		if available <= 0:
			return 0
		return min(int(available), order.remaining)


@dataclass
class TrackerEvent:
	"""One lifecycle event emitted by ``RestingOrderTracker.step``/``cancel``.

	``order`` is a live reference so dispatch can book/notify without a
	lookup round-trip. ``kind``:
	  - ``"fill"``:   ``size`` contracts filled at the order's rest price;
	                  ``first_fill`` marks the booking-vs-augment split.
	  - ``"cancel"``: the order (or its unfilled remainder) ended; ``cause``
	                  is the §5.3 label (``"expired"`` = TTL was the binding
	                  deadline term, ``"cancelled"`` = any other cause) and
	                  ``ts`` is BACKDATED to ``deadline_ts`` for clock
	                  cancels (SPEC §5.1 — the ledger is timer-independent).
	  - ``"error"``:  the per-order isolation path fired (SPEC §5 internals);
	                  the order is terminal ``errored``, the step continued.
	                  (Mark-outs are ledger-only instrumentation, not events.)
	"""

	kind: Literal["fill", "cancel", "error"]
	order: RestingOrder
	ts: float
	size: int = 0
	first_fill: bool = False
	cause: str | None = None


@dataclass
class LedgerRow:
	"""Per-order instrumentation record (SPEC §11).

	``disposition`` is ``None`` while in-flight, then exactly one of
	``filled | partial | expired | cancelled | censored_stream_end |
	errored`` — fill-completeness wins over cause (SPEC §5.3): a partially
	filled order that ends for ANY reason is ``partial`` with the cause
	preserved in ``end_cause``. ``mark_outs`` maps offset-seconds to the
	sampled reference price (``None`` when the provider had no price).
	"""

	client_order_id: str
	ticker: str
	side: str
	rest_price_cents: int
	intended_size: int
	queue_ahead_at_place: float
	placed_ts: float
	fills: list[tuple[float, int]] = field(default_factory=list)
	time_to_first_fill: float | None = None
	disposition: str | None = None
	end_cause: str | None = None
	mark_outs: dict[int, int | None] = field(default_factory=dict)


class RestingOrderTracker:
	"""Engine-level resting-order lifecycle state machine (SPEC §5).

	One tracker instance serves paper, replay, and (2b) live — only the
	fill-event SOURCE differs (offline: ``QueueFillModel`` over the print
	stream; live 2b: venue polling feeding the same transitions). ``step``
	is side-effect-free beyond internal state + returned events: dispatch
	owns ALL external side effects (booking, notify, metrics) — this is
	what lets 2b wrap a durable write between event emission and state
	commit (SPEC §5.6, frozen contract §15.11).

	Internals pinned by SPEC §5:
	  - ticker-keyed index: ``step`` touches only tickers with in-flight
	    orders — O(orders-at-ticker) per print, never O(all-orders).
	  - empty short-circuit: with no in-flight orders ``step`` returns
	    immediately (the taker hot path pays one emptiness check).
	  - model-time determinism (§5.1): a print with ``ts >= deadline_ts``
	    NEVER fills; clock cancels are backdated to ``deadline_ts`` — the
	    ledger is a pure function of the event stream, so paper (timered)
	    and replay (timerless) produce byte-identical ledgers.
	  - per-order error isolation: an exception processing one order moves
	    THAT order to ``errored`` (loud log + event) and the step continues.

	``mid_provider`` supplies the mark-out reference price for a ticker
	(mid, or last-trade fallback; ``None`` = no price available). Injected
	by the driver so the tracker stays I/O-free.
	"""

	def __init__(
		self,
		model: QueueFillModel,
		mid_provider: Callable[[str], int | None],
	) -> None:
		self._model = model
		self._mid_provider = mid_provider
		self._orders: dict[str, RestingOrder] = {}          # coid -> order (insertion-ordered)
		self._by_ticker: dict[str, list[str]] = {}          # ticker -> [coid, ...]
		self._rows: dict[str, LedgerRow] = {}               # coid -> ledger row
		# Pending mark-out samples: (coid, offset_s, scheduled_ts), FIFO.
		self._pending_markouts: list[tuple[str, int, float]] = []

	# ------------------------------------------------------------------
	# Registration + guard data sources (SPEC §5 API)
	# ------------------------------------------------------------------

	def register(self, order: RestingOrder) -> None:
		"""Track a newly placed resting order (dispatch's ``resting`` branch)."""
		self._orders[order.client_order_id] = order
		self._by_ticker.setdefault(order.ticker, []).append(order.client_order_id)
		self._rows[order.client_order_id] = LedgerRow(
			client_order_id=order.client_order_id,
			ticker=order.ticker,
			side=order.side,
			rest_price_cents=order.rest_price_cents,
			intended_size=order.intended_size,
			queue_ahead_at_place=order.queue_ahead,
			placed_ts=order.placed_ts,
		)

	def has_level(self, strategy: str, ticker: str, side: str, price_cents: int) -> bool:
		"""Data source for dispatch's ``duplicate_level`` guard (SPEC §8.2)."""
		for coid in self._by_ticker.get(ticker, ()):
			o = self._orders[coid]
			if (o.state not in _TERMINAL_STATES
					and o.strategy == strategy and o.side == side
					and o.rest_price_cents == price_cents):
				return True
		return False

	def in_flight_count(self, strategy: str | None = None) -> int:
		"""Count of non-terminal resting orders (per-strategy cap input)."""
		return sum(
			1 for o in self._orders.values()
			if o.state not in _TERMINAL_STATES
			and (strategy is None or o.strategy == strategy)
		)

	@property
	def ledger(self) -> list[LedgerRow]:
		"""All ledger rows, registration-ordered (SPEC §11 instrument)."""
		return list(self._rows.values())

	# ------------------------------------------------------------------
	# The pure step (SPEC §5.1)
	# ------------------------------------------------------------------

	def step(
		self,
		now: float,
		prints_by_ticker: dict[str, list[Print]],
	) -> list[TrackerEvent]:
		"""Advance the state machine: consume prints, apply deadline cancels,
		sample due mark-outs. Returns the events for dispatch to act on.

		Phase order is load-bearing (SPEC §5.1): prints are processed FIRST
		on their OWN timestamps (a print with ``ts < deadline_ts`` fills even
		when ``now`` is already past the deadline — model time, not step
		time), THEN deadline cancels apply, backdated to ``deadline_ts``.
		"""
		if not self._orders:
			return []
		has_live = any(o.state not in _TERMINAL_STATES for o in self._orders.values())
		if not has_live and not self._pending_markouts:
			return []

		events: list[TrackerEvent] = []

		# Phase 1 — prints, per ticker, orders in registration order.
		for ticker, prints in prints_by_ticker.items():
			for coid in self._by_ticker.get(ticker, ()):
				order = self._orders[coid]
				if order.state in _TERMINAL_STATES:
					continue
				row = self._rows[coid]
				try:
					for p in prints:
						if order.state in _TERMINAL_STATES:
							break
						if p.ts >= order.deadline_ts:
							continue  # §5.1: outside the validity window
						got = self._model.consume(order, p)
						if got <= 0:
							continue
						first = not row.fills
						order.filled_size += got
						row.fills.append((p.ts, got))
						if first:
							row.time_to_first_fill = p.ts - order.placed_ts
						for offset in MARKOUT_OFFSETS_S:
							self._pending_markouts.append((coid, offset, p.ts + offset))
						if order.remaining <= 0:
							self._finalize(order, row, "filled", None)
						else:
							order.state = "partially_filled"
						events.append(TrackerEvent(
							kind="fill", order=order, ts=p.ts, size=got,
							first_fill=first,
						))
				except Exception:
					log.exception(
						"RestingOrderTracker: error processing order %s on %s — "
						"isolating as errored, step continues", coid, ticker,
					)
					self._finalize(order, row, "errored", None)
					events.append(TrackerEvent(kind="error", order=order, ts=now))

		# Phase 2 — deadline cancels (clock conditions), backdated (§5.1).
		for coid, order in self._orders.items():
			if order.state in _TERMINAL_STATES:
				continue
			deadline = order.deadline_ts
			if deadline <= now:
				row = self._rows[coid]
				cause = "expired" if deadline == order.expires_ts else "cancelled"
				self._finalize(order, row, cause, None)
				events.append(TrackerEvent(
					kind="cancel", order=order, ts=deadline, cause=cause,
				))

		# Phase 3 — due mark-out samples (§7.5 pending-sample scheduling).
		if self._pending_markouts:
			due = [m for m in self._pending_markouts if m[2] <= now]
			if due:
				self._pending_markouts = [m for m in self._pending_markouts if m[2] > now]
				for coid, offset, _scheduled in due:
					mrow = self._rows.get(coid)
					if mrow is None:
						continue
					try:
						mrow.mark_outs[offset] = self._mid_provider(mrow.ticker)
					except Exception:
						log.exception(
							"RestingOrderTracker: mark-out sample failed for %s", coid,
						)
						mrow.mark_outs[offset] = None

		return events

	# ------------------------------------------------------------------
	# External transitions
	# ------------------------------------------------------------------

	def cancel(self, client_order_id: str, cause: str, now: float) -> TrackerEvent | None:
		"""Strategy- or operator-initiated cancel (e.g. exit-while-resting,
		SPEC §8.2). NOT backdated — this is a genuine now-decision, unlike
		deadline cancels. Idempotent: terminal/unknown orders are a no-op.
		Fills already booked stay booked (fills-beat-cancels, §5.2)."""
		order = self._orders.get(client_order_id)
		if order is None or order.state in _TERMINAL_STATES:
			return None
		row = self._rows[client_order_id]
		self._finalize(order, row, "cancelled" if cause != "expired" else cause, None)
		return TrackerEvent(kind="cancel", order=order, ts=now, cause=cause)

	def censor_open(self, ts: float) -> int:
		"""Stream-end censoring (SPEC §11): mark every still-in-flight order's
		ledger row ``censored_stream_end``. A REPORTING disposition, not a
		state transition — called only by the replay driver at end-of-stream.
		Returns the censored count."""
		censored = 0
		for coid, order in self._orders.items():
			if order.state in _TERMINAL_STATES:
				continue
			row = self._rows[coid]
			if row.disposition is None:
				row.disposition = "censored_stream_end"
				censored += 1
		return censored

	# ------------------------------------------------------------------
	# Serialization (SPEC §5.5 — bundle step; JSON-plain data only)
	# ------------------------------------------------------------------

	def to_snapshot(self) -> list[dict[str, object]]:
		"""Serialize IN-FLIGHT state (non-terminal orders + their open ledger
		rows + pending mark-outs). Terminal rows are session-local reporting
		and are NOT carried across days."""
		snapshot: list[dict[str, object]] = []
		for coid, order in self._orders.items():
			if order.state in _TERMINAL_STATES:
				continue
			row = self._rows[coid]
			if row.disposition == "censored_stream_end":
				continue
			snapshot.append({
				"order": asdict(order),
				"fills": [[ts, size] for ts, size in row.fills],
				"time_to_first_fill": row.time_to_first_fill,
				"mark_outs": {str(k): v for k, v in row.mark_outs.items()},
				"pending_markouts": [
					[offset, scheduled]
					for c, offset, scheduled in self._pending_markouts if c == coid
				],
			})
		return snapshot

	def from_snapshot(self, snapshot: list[dict[str, object]]) -> None:
		"""Seed from a prior ``to_snapshot`` (or its JSON round-trip — string
		mark-out keys are coerced back to int). Additive: call on a fresh
		tracker at boot/replay-seed time (SPEC §5.5)."""
		for entry in snapshot:
			order = RestingOrder(**cast("dict[str, object]", entry["order"]))  # type: ignore[arg-type]
			self.register(order)
			row = self._rows[order.client_order_id]
			fills = cast("list[list[float]]", entry.get("fills") or [])
			row.fills = [(float(ts), int(size)) for ts, size in fills]
			ttff = cast("float | None", entry.get("time_to_first_fill"))
			row.time_to_first_fill = float(ttff) if ttff is not None else None
			mark_outs = cast("dict[str, int | None]", entry.get("mark_outs") or {})
			row.mark_outs = {int(k): v for k, v in mark_outs.items()}
			pending = cast("list[list[float]]", entry.get("pending_markouts") or [])
			for offset, scheduled in pending:
				self._pending_markouts.append(
					(order.client_order_id, int(offset), float(scheduled)))

	# ------------------------------------------------------------------
	# Internal
	# ------------------------------------------------------------------

	def _finalize(
		self,
		order: RestingOrder,
		row: LedgerRow,
		outcome: str,
		_unused: None,
	) -> None:
		"""Apply the terminal transition + §5.3 canonical disposition rule.
		Idempotent: terminal states absorb duplicates (SPEC §5.4)."""
		if order.state in _TERMINAL_STATES:
			return
		if outcome == "filled":
			order.state = "filled"
			row.disposition = "filled"
		elif outcome == "errored":
			order.state = "errored"
			row.disposition = "errored"
		else:  # "expired" | "cancelled" — cause labels (§5.3)
			if order.filled_size > 0:
				order.state = "cancelled_partial"
				row.disposition = "partial"
				row.end_cause = outcome
			else:
				order.state = "cancelled"
				row.disposition = outcome
