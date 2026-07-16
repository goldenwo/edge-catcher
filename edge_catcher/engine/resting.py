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

from dataclasses import dataclass


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
