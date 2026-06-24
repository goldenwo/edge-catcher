"""Live fill-realism gate (spec 2026-06-24). PURE: no I/O, no edge_catcher.live
imports — exactly cross_check.py's isolation contract. All DB/ledger I/O lives in the
gitignored scripts/fill_realism_gate_cli.py wrapper. Decides GRADUATE/REJECT/INCONCLUSIVE
on a strategy's settled live fills, to gate scaling real money."""
from __future__ import annotations

import random
import re
import statistics
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
	n_orders_placed: int		# distinct filled positions + no-position rows (placed entry-orders)
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
	Placed denominator: distinct filled positions (entry counted once) + NO_POSITION rows windowed by placed_at_utc.
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
				# A filled-terminal row must carry entry_time; NULL = data loss → surface it.
				# A non-null entry_time merely outside the window is a legitimate skip.
				if r.get("entry_time") is None:
					n_lost_truth += 1
				continue
			key = _position_key(str(r.get("client_order_id")))
			if key not in groups:
				n_placed += 1  # count each logical position's entry ONCE (split children are exits, not new placements)
			g = groups.setdefault(key, {"pnl": 0, "size": 0, "entry_time": r.get("entry_time")})
			g["pnl"] += int(r.get("pnl_cents") or 0)
			g["size"] += int(r.get("fill_size") or 0)
			# all rows of a position share entry_time; keep the earliest defensively
			if r.get("entry_time") and r["entry_time"] < g["entry_time"]:
				g["entry_time"] = r["entry_time"]
		else:
			n_lost_truth += 1  # unknown status — surfaced, not dropped

	positions = [
		Position(key=k, pnl_cents=g["pnl"], position_size=g["size"], entry_time=g["entry_time"])
		for k, g in groups.items()
	]
	positions.sort(key=lambda p: (p.entry_time, p.key))
	return Aggregation(positions=positions, n_orders_placed=n_placed,
	                   n_in_flight=n_in_flight, n_lost_truth=n_lost_truth)


# ---------------------------------------------------------------------------
# Bootstrap confidence interval (spec section 4)
# ---------------------------------------------------------------------------

def bootstrap_ci(
	values: list[float],
	*,
	seed: int,
	resamples: int = 10_000,
	conf: float = 0.95,
) -> tuple[float, float]:
	"""Percentile bootstrap CI of the mean. Deterministic under `seed`
	(random.Random — stdlib, no numpy per house convention). Returns (lo, hi);
	(0.0, 0.0) for an empty sample."""
	n = len(values)
	if n == 0:
		return (0.0, 0.0)
	rng = random.Random(seed)
	means: list[float] = []
	for _ in range(resamples):
		total = 0.0
		for _ in range(n):
			total += values[rng.randrange(n)]
		means.append(total / n)
	means.sort()
	lo_idx = int((1.0 - conf) / 2.0 * resamples)
	hi_idx = min(resamples - 1, int((1.0 + conf) / 2.0 * resamples))
	return (means[lo_idx], means[hi_idx])


# ---------------------------------------------------------------------------
# Asymmetric decision rule (spec section 4)
# ---------------------------------------------------------------------------

def decide(
	*,
	n: int,
	n_target: int,
	pt_lo: float, pt_hi: float,		# per-trade CI
	pc_lo: float, pc_hi: float,		# per-contract CI (rule branches on pc_lo; pc_hi accepted for symmetry, reported in the verdict)
	ceiling: bool,
) -> tuple[Decision, str]:
	"""Asymmetric rule (spec section 4): graduate ONLY at exactly N with both CIs' lower bound > 0;
	reject continuously (full CI below 0) — the safe direction. Kills are enforced by the
	operator/live-trader, not here; this evaluates the rows it is given."""
	# Sub-cap statistical REJECT — takes precedence over ALL other branches, including n==n_target.
	# SAFETY: reorder this AFTER the n==n_target block and a fully-negative CI at exactly N could graduate.
	if pt_hi < 0:
		return Decision.REJECT, "per-trade CI fully below 0 (ci_high<0)"
	if n == n_target:
		if pt_lo > 0 and pc_lo > 0:
			return Decision.GRADUATE, "per-trade & per-contract ci_low>0 at N"
		if pt_lo > 0:  # per-trade passes but per-contract spans 0
			return Decision.INCONCLUSIVE, "size-dependent edge (per-contract ci_low<=0)"
		return Decision.REJECT, "per-trade ci_low<=0 at N (marginal != scale-worthy)"
	if n > n_target:
		# graduation is strictly at the first 50; never graduate a hand-picked larger n
		return Decision.REJECT, "n>N: graduation only at exactly N (caller must pass first-50)"
	# n < n_target, sign not fully negative
	if ceiling:
		return Decision.INCONCLUSIVE, "ceiling before N, CI sign undetermined"
	return Decision.RUNNING, "accumulating to N"


# ---------------------------------------------------------------------------
# evaluate() — pure orchestrator (Task 5)
# ---------------------------------------------------------------------------

def evaluate(
	rows: list[Mapping[str, Any]],
	*,
	since: Optional[str],
	until: Optional[str],
	n_target: int = 50,
	seed: int = 1729,
	resamples: int = 10_000,
	ceiling_exceeded: bool = False,
	prior_rejected: bool = False,
	attempt_num: int = 1,
) -> GateVerdict:
	"""Pure orchestration: aggregate → (first n_target positions) → CIs → decide.

	`ceiling_exceeded` / `prior_rejected` / `attempt_num` are supplied by the caller
	(the gitignored CLI reads them from run state + the ledger). No I/O here.

	n_orders_placed / observed_fill_rate are full-window (pre-truncation) diagnostics;
	the CI decision uses only the first n_target positions (len(sample))."""
	agg = aggregate_positions(rows, since=since, until=until)
	# Evaluate strictly the first n_target positions (graduate-only-at-exactly-N, spec section 4).
	sample = agg.positions[:n_target]
	pnls = [float(p.pnl_cents) for p in sample]
	per_contract = [p.pnl_cents / p.position_size for p in sample if p.position_size > 0]

	pt_lo, pt_hi = bootstrap_ci(pnls, seed=seed, resamples=resamples)
	pc_lo, pc_hi = bootstrap_ci(per_contract, seed=seed, resamples=resamples)
	mean_pnl = statistics.fmean(pnls) if pnls else 0.0

	decision, reason = decide(
		n=len(sample), n_target=n_target,
		pt_lo=pt_lo, pt_hi=pt_hi, pc_lo=pc_lo, pc_hi=pc_hi,
		ceiling=ceiling_exceeded,
	)
	if agg.n_lost_truth:
		reason = f"{reason} | ALERT: {agg.n_lost_truth} lost_truth row(s) excluded — investigate"
	requires_signoff = prior_rejected and decision is Decision.GRADUATE

	return GateVerdict(
		decision=decision,
		n_positions=len(sample),
		n_orders_placed=agg.n_orders_placed,
		observed_fill_rate=agg.observed_fill_rate,
		mean_pnl_cents=mean_pnl,
		ci_low=pt_lo, ci_high=pt_hi,
		per_contract_ci_low=pc_lo, per_contract_ci_high=pc_hi,
		n_in_flight=agg.n_in_flight,
		n_lost_truth=agg.n_lost_truth,
		ceiling_exceeded=ceiling_exceeded,
		attempt_num=attempt_num,
		requires_signoff=requires_signoff,
		outcome_reason=reason,
	)
