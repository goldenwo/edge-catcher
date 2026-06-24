"""Live fill-realism gate (spec 2026-06-24). PURE: no I/O, no edge_catcher.live
imports — exactly cross_check.py's isolation contract. All DB/ledger I/O lives in the
gitignored scripts/fill_realism_gate_cli.py wrapper. Decides GRADUATE/REJECT/INCONCLUSIVE
on a strategy's settled live fills, to gate scaling real money."""
from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping, Optional

# live_trades status partition (DDL 0003 CHECK enum):
FILLED_TERMINAL = frozenset({"won", "lost", "scratch"})                  # enter the P&L sample
NO_POSITION = frozenset({"rejected", "rejected_post_hoc", "cancelled"})  # denominator only
IN_FLIGHT = frozenset({"pending", "open", "exit_pending"})               # unresolved
ALERT_STATUS = frozenset({"lost_truth"})                                 # surfaced, never silently dropped


class Decision(str, Enum):
	GRADUATE = "GRADUATE"
	REJECT = "REJECT"
	INCONCLUSIVE = "INCONCLUSIVE"
	RUNNING = "RUNNING"  # not yet terminal (n<N, no kill/ceiling, CI sign undetermined)


@dataclass(frozen=True)
class GateVerdict:
	decision: Decision
	n_positions: int
	n_orders_placed: int
	observed_fill_rate: float
	mean_pnl_cents: float
	ci_low: float
	ci_high: float
	per_contract_ci_low: float
	per_contract_ci_high: float
	n_in_flight: int
	n_lost_truth: int
	ceiling_exceeded: bool
	attempt_num: int
	requires_signoff: bool
	outcome_reason: str


# ---------------------------------------------------------------------------
# Position aggregation + dual-column windowing (spec section 4)
# ---------------------------------------------------------------------------

_SPLIT_RE = re.compile(r"-split-\d+$")


def _position_key(client_order_id: str) -> str:
	"""Group key: strip a trailing -split-{N} so a parent + its children share a key."""
	return _SPLIT_RE.sub("", client_order_id)


def _in_window(ts: Optional[str], since: Optional[str], until: Optional[str]) -> bool:
	"""ISO-8601 lexical window [since, until). NULL ts is outside any window.
	Ported from scripts/cross_check_live._in_window (gitignored; cannot import)."""
	if not ts:
		return False
	if since and ts < since:
		return False
	if until and ts >= until:
		return False
	return True


@dataclass(frozen=True)
class Position:
	key: str
	pnl_cents: int
	position_size: int
	entry_time: str


@dataclass(frozen=True)
class Aggregation:
	positions: list[Position]	# filled, in-window, ordered by entry_time (ties by key)
	n_orders_placed: int		# filled + no-position rows in the placed-window
	n_in_flight: int
	n_lost_truth: int

	@property
	def n_positions(self) -> int:
		return len(self.positions)

	@property
	def observed_fill_rate(self) -> float:
		return (self.n_positions / self.n_orders_placed) if self.n_orders_placed else 0.0


def aggregate_positions(
	rows: list[Mapping[str, Any]],
	*,
	since: Optional[str],
	until: Optional[str],
) -> Aggregation:
	"""Collapse rows into logical positions and partition by status (spec section 4).

	Filled sample: status in FILLED_TERMINAL, windowed by entry_time, parent+children summed.
	Placed denominator: filled + NO_POSITION rows, windowed by placed_at_utc.
	lost_truth / in-flight: counted, excluded from the P&L sample, never silently dropped."""
	groups: dict[str, dict[str, Any]] = {}
	n_in_flight = 0
	n_lost_truth = 0
	n_placed = 0

	for r in rows:
		status = r.get("status")
		if status in ALERT_STATUS:
			n_lost_truth += 1
			continue
		if status in IN_FLIGHT:
			n_in_flight += 1
			continue
		if status in NO_POSITION:
			if _in_window(r.get("placed_at_utc"), since, until):
				n_placed += 1
			continue
		if status in FILLED_TERMINAL:
			if not _in_window(r.get("entry_time"), since, until):
				continue
			n_placed += 1  # a fill is also a placed order
			key = _position_key(str(r.get("client_order_id")))
			g = groups.setdefault(key, {"pnl": 0, "size": 0, "entry_time": r.get("entry_time")})
			g["pnl"] += int(r.get("pnl_cents") or 0)
			g["size"] += int(r.get("fill_size") or 0)
			# all rows of a position share entry_time; keep the earliest defensively
			if r.get("entry_time") and r["entry_time"] < g["entry_time"]:
				g["entry_time"] = r["entry_time"]
		else:
			# unknown/unexpected status: surfaced as lost_truth, never silently dropped
			n_lost_truth += 1

	positions = [
		Position(key=k, pnl_cents=g["pnl"], position_size=g["size"], entry_time=g["entry_time"])
		for k, g in groups.items()
	]
	positions.sort(key=lambda p: (p.entry_time, p.key))
	return Aggregation(positions=positions, n_orders_placed=n_placed,
	                   n_in_flight=n_in_flight, n_lost_truth=n_lost_truth)
