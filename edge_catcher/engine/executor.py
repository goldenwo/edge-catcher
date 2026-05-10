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
	action: Literal["buy", "sell"] = "buy"


@dataclass(frozen=True, slots=True)
class OrderResult:
	"""The executor's response to a place() request.

	`status` discriminates the engine's downstream handling:
	- "filled":   trade is booked synchronously (paper); or live IOC filled at
	              submission. `filled_size == intended_size` (or close to it for
	              partial-IOC, with `fill_pct` reflecting the partial).
	- "pending":  order accepted upstream but fill not yet confirmed (live GTC).
	              Engine writes a pending row; B's state machine resolves it.
	              `filled_size` MAY be > 0 — Kalshi's "partially filled and resting"
	              GTC case maps to `status="pending" AND filled_size > 0`. `fill_pct`
	              reflects the partial. `blended_entry_cents` is the partial's
	              blended price (or 0 sentinel if no fill yet).
	- "rejected": order rejected at executor level (orderbook stale, budget too
	              small, Kalshi 4xx, etc.). No trade row written. `filled_size == 0`.

	Two fields the paper path does NOT need (and G therefore omits):
	  - `order_id`: paper_trades has no order_id column; D adds it when LiveExecutor
	    lands.
	  - `fees_cents`: paper computes fees inside trade_store.record_trade
	    (STANDARD_FEE.calculate); D adds it when live REST responses carry an
	    explicit fee value Kalshi reports.
	"""
	status: Literal["filled", "pending", "rejected"]
	intended_size: int
	filled_size: int
	blended_entry_cents: int      # 0-sentinel preserved from FillResult.blended_price_cents
	fill_pct: float
	slippage_cents: int
	book_depth: int | None = None
	book_snapshot: str | None = None
	rejection_reason: str | None = None
	order_id: str | None = None


@dataclass(frozen=True, slots=True)
class OpenPosition:
	"""A resolved open position held by the engine.

	side semantics are venue-defined ("yes"/"no" for Kalshi binary);
	engine treats as opaque label per CR-6 (multi-adapter extensibility).
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
