"""Engine-facing execution contract.

Defines the typed `OrderRequest`/`OrderResult` value objects and the `Executor`
Protocol that dispatch holds a reference to. PaperExecutor and (later) LiveExecutor
implement the protocol; dispatch doesn't know which is wired.

**Protocol growth invariant (binding for B/C/D/E/F and beyond).** New fields on
OrderRequest/OrderResult MUST be additive — default-None (or zero-value) optional
fields only. No reordering of existing fields. No removal of existing fields once
they ship outside G. New OrderResult.status literal values require an explicit
dispatch-side branch update — call out in the PR description and update
engine/dispatch.py to handle the new status. Adding a method to the Executor
Protocol (e.g. cancel, query) is additive when current implementers can structurally
satisfy a default no-op — but keep the protocol minimal; B/D/E grow their own narrow
protocols rather than fattening Executor.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol


@dataclass(frozen=True, slots=True)
class OrderRequest:
	"""The engine's typed instruction to the executor."""
	ticker: str
	series: str
	side: Literal["yes", "no"]
	size_contracts: int           # post-sizing contract count
	limit_price_cents: int        # 1..99
	strategy: str                 # for audit/correlation
	client_order_id: str          # idempotency (live); recorded but not enforced (paper)
	# default "buy" — all current strategies (paper + Phase 1 live) only
	# buy entries; sell-side execution lands with PR 4 (D) when LiveExecutor
	# constructs sell-orders for exit-pending paths. Per the protocol-growth
	# invariant above, "buy" serves as the zero-value default for this
	# binary-action field.
	action: Literal["buy", "sell"] = "buy"
	# Set by the builders (which know entry-vs-exit and taker-vs-maker);
	# executors pass it through. "ioc" default preserves all current sites.
	time_in_force: Literal["ioc", "gtc"] = "ioc"


@dataclass(frozen=True, slots=True)
class OrderResult:
	"""The executor's response to a place() request.

	`status` discriminates the engine's downstream handling:
	- "filled":   trade is booked synchronously (paper); or live IOC filled at
	              submission. `filled_size == intended_size` (or close to it for
	              partial-IOC, with `fill_pct` reflecting the partial).
	- "pending":  fill/placement state is UNKNOWN (network error, 5xx, or a
	              missing fill cost prevents the engine from confirming what
	              happened). Engine writes a pending row; B's state machine
	              (the reconciler) resolves it. `filled_size` MAY be > 0 if a
	              partial fill amount is known despite the ambiguity.
	- "resting":  the venue (or paper, for maker sims) ACCEPTED the order and
	              it now rests on the book unfilled or partially filled.
	              `order_id` is known. `filled_size` MAY be > 0 — a partial
	              fill at placement time, live only. `fill_pct` reflects the
	              partial. `blended_entry_cents` is the partial's blended
	              price (or 0 sentinel if no fill yet).
	- "rejected": order rejected at executor level (orderbook stale, budget too
	              small, Kalshi 4xx, etc.). No trade row written. `filled_size == 0`.

	Two fields the paper path does NOT need (and G therefore omits):
	  - `order_id`: paper_trades has no order_id column; D adds it when LiveExecutor
	    lands.
	  - `fees_cents`: paper computes fees inside trade_store.record_trade
	    (STANDARD_FEE.calculate); D adds it when live REST responses carry an
	    explicit fee value Kalshi reports.
	"""
	status: Literal["filled", "pending", "rejected", "resting"]
	intended_size: int
	filled_size: int
	blended_entry_cents: int      # 0-sentinel preserved from FillResult.blended_price_cents
	fill_pct: float
	slippage_cents: int            # DEPRECATED — see market_impact_cents / limit_slippage_cents below
	book_depth: int | None = None
	book_snapshot: str | None = None
	rejection_reason: str | None = None
	order_id: str | None = None
	# Dual-slippage diagnostics, reporting-only (spec §4.2). Paper populates
	# on filled entries; live leaves both None (live computes at
	# transition_pending_to_open).
	market_impact_cents: int | None = None  # vs top-of-book best
	limit_slippage_cents: int | None = None  # vs the order's limit


@dataclass(frozen=True, slots=True)
class OpenPosition:
	"""A resolved open position held by the engine.

	``side`` is typed ``Literal["yes","no"]`` for Phase 1's binary-prediction-
	market scope (Kalshi today; Polymarket-binary later — same type works).
	Engine code treats the value as an opaque label: no business logic
	switches on "yes" vs "no" (sizing/exit logic is direction-agnostic via
	``Signal.action``). Future continuous-payoff or multi-outcome venues
	require their own ``OpenPosition`` type per CR-6, not a widening of
	this one.
	"""
	ticker: str
	side: Literal["yes", "no"]
	fill_size: int
	blended_entry_cents: int


class Executor(Protocol):
	"""Engine-facing execution contract.

	Async by design — `LiveExecutor` (sub-project D) issues HTTPX requests
	to Kalshi inside `place()`; PaperExecutor's body is pure CPU but adopts
	the same async signature so dispatch can `await executor.place(...)`
	without branching on executor flavor. Replay parity is preserved because
	the captured WS message stream still drives a deterministic async
	dispatch — only the call mechanism changes, not the logic.
	"""
	async def place(self, req: OrderRequest) -> OrderResult: ...
