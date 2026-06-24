"""Live fill-realism gate (spec 2026-06-24). PURE: no I/O, no edge_catcher.live
imports — exactly cross_check.py's isolation contract. All DB/ledger I/O lives in the
gitignored scripts/fill_realism_gate_cli.py wrapper. Decides GRADUATE/REJECT/INCONCLUSIVE
on a strategy's settled live fills, to gate scaling real money."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

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
