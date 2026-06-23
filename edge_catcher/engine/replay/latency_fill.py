"""Replay-only latency-aware fill deferral (spec 2026-06-23). REPLAY ONLY —
never imported by the live engine at module top level."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class PendingFill:
	seq: int                # monotonic enqueue order — deterministic drain key
	arrival_time: datetime
	req: Any                # OrderRequest
	entry_price: int
	signal: Any             # Signal


@dataclass
class PendingFillQueue:
	"""Deferred entries; drains those whose arrival_time <= now in ENQUEUE (seq)
	order. `total_enqueued` is the lifetime count (= fill-rate denominator: the
	drain records only fills, so non-fills = total_enqueued - filled rows)."""
	# init=False: the only valid construction is PendingFillQueue() — structurally
	# enforces the invariant `_seq == lifetime enqueue count` (T6's fill-rate
	# denominator) by keeping the private state out of the generated __init__.
	_pending: list[PendingFill] = field(default_factory=list, init=False)
	_seq: int = field(default=0, init=False)

	def enqueue(self, *, req: Any, entry_price: int, signal: Any, arrival_time: datetime) -> None:
		self._seq += 1
		self._pending.append(PendingFill(self._seq, arrival_time, req, entry_price, signal))

	def drain(self, now: datetime) -> list[PendingFill]:
		matured = sorted((p for p in self._pending if p.arrival_time <= now), key=lambda p: p.seq)
		if matured:
			ready = {p.seq for p in matured}
			self._pending = [p for p in self._pending if p.seq not in ready]
		return matured

	@property
	def total_enqueued(self) -> int:
		return self._seq

	def __len__(self) -> int:
		return len(self._pending)


@dataclass
class LatencyReplayExecutor:
	"""Replay-only wrapper. latency_ms==0 => transparent passthrough (byte-exact).
	latency_ms>0 => carries base + queue; _handle_enter enqueues and the replay
	loop drains via resolve_matured_fills(). Satisfies Executor (place is the only method)."""
	base: Any
	latency_ms: int
	pending_queue: PendingFillQueue = field(default_factory=PendingFillQueue)

	async def place(self, req: Any) -> Any:
		return await self.base.place(req)	# only reached at Delta=0 (Delta>0 enqueues upstream)


async def resolve_matured_fills(
	queue: PendingFillQueue,
	now: datetime,
	base_executor: Any,
	store: Any,
) -> None:
	"""Drain entries matured by `now`; place each against the in-place-evolved book
	and record FILLS via record_filled_entry. Non-fills write no row (matching
	replay's InMemory record_rejected no-op) and are counted as
	queue.total_enqueued - len(store.all_trades()). No metrics (replay has no
	Metrics instance). Lazy import avoids a top-level cycle with dispatch."""
	matured = queue.drain(now)
	if not matured:
		return
	from edge_catcher.engine.dispatch import record_filled_entry
	for pf in matured:
		result = await base_executor.place(pf.req)
		if result.status == "filled":
			record_filled_entry(
				store,
				signal=pf.signal,
				entry_price=pf.entry_price,
				req=pf.req,
				result=result,
				now=pf.arrival_time,
			)
		# rejected/pending: no row in replay (InMemory no-op); counted via total_enqueued.
