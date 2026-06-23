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
	_pending: list[PendingFill] = field(default_factory=list)
	_seq: int = 0

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
