from __future__ import annotations

import dataclasses

import pytest

from edge_catcher.fill_realism_gate import (
	Decision,
	GateVerdict,
	FILLED_TERMINAL,
	NO_POSITION,
	IN_FLIGHT,
	ALERT_STATUS,
	aggregate_positions,
	bootstrap_ci,
	decide,
	evaluate,
)


def test_status_sets_match_ddl():
	assert FILLED_TERMINAL == frozenset({"won", "lost", "scratch"})
	assert NO_POSITION == frozenset({"rejected", "rejected_post_hoc", "cancelled"})
	assert IN_FLIGHT == frozenset({"pending", "open", "exit_pending"})
	assert ALERT_STATUS == frozenset({"lost_truth"})

	# partition-integrity: union = all 10 DDL statuses, sets are pairwise disjoint
	all_sets = [FILLED_TERMINAL, NO_POSITION, IN_FLIGHT, ALERT_STATUS]
	union = frozenset().union(*all_sets)
	assert len(union) == 10
	assert sum(len(s) for s in all_sets) == 10  # equal sizes => pairwise disjoint


def test_gate_verdict_is_frozen():
	v = GateVerdict(decision=Decision.REJECT, n_positions=0, n_orders_placed=0,
	                observed_fill_rate=0.0, mean_pnl_cents=0.0, ci_low=0.0, ci_high=0.0,
	                per_contract_ci_low=0.0, per_contract_ci_high=0.0, n_in_flight=0,
	                n_lost_truth=0, ceiling_exceeded=False, attempt_num=1,
	                requires_signoff=False, outcome_reason="empty")
	assert v.decision is Decision.REJECT
	with pytest.raises(dataclasses.FrozenInstanceError):
		v.decision = Decision.GRADUATE  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Task 2: aggregate_positions
# ---------------------------------------------------------------------------

def _row(coid: str, status: str = "won", pnl: int = 100, fill_size: int = 2,
	entry_time: str = "2026-06-22T05:00:00+00:00",
	placed: str = "2026-06-22T05:00:00+00:00", strategy: str = "s") -> dict:
	return {"client_order_id": coid, "status": status, "pnl_cents": pnl,
	        "fill_size": fill_size, "entry_time": entry_time, "placed_at_utc": placed,
	        "strategy": strategy}


def test_partial_exit_children_collapse_to_one_position():
	rows = [
		_row("a", status="won", pnl=30, fill_size=3),            # parent residual (3 contracts)
		_row("a-split-1", status="lost", pnl=-10, fill_size=2),  # child (2 contracts)
	]
	agg = aggregate_positions(rows, since=None, until=None)
	assert len(agg.positions) == 1
	p = agg.positions[0]
	assert p.pnl_cents == 20            # 30 + (-10), no double-count
	assert p.position_size == 5         # 3 + 2 (parent residual decremented, child slice)
	assert agg.n_orders_placed == 1     # one logical position = one placed entry (splits are exits)
	assert agg.observed_fill_rate == 1.0


def test_denominator_counts_rejected_by_placed_at_utc_not_entry_time():
	rows = [
		_row("f1", status="won", pnl=50, fill_size=1, entry_time="2026-06-22T05:00:00+00:00"),
		# a 0-fill IOC: status rejected, NULL entry_time, placed inside window
		{"client_order_id": "r1", "status": "rejected", "pnl_cents": None, "fill_size": 0,
		 "entry_time": None, "placed_at_utc": "2026-06-22T05:01:00+00:00", "strategy": "s"},
	]
	agg = aggregate_positions(rows, since="2026-06-22T00:00:00+00:00",
	                          until="2026-06-23T00:00:00+00:00")
	assert agg.n_positions == 1                 # only the filled one
	assert agg.n_orders_placed == 2             # filled + rejected (rejected NOT dropped)
	assert agg.observed_fill_rate == 0.5


def test_lost_truth_excluded_and_counted():
	rows = [
		_row("f1", status="won", pnl=50, fill_size=1),
		_row("lt", status="lost_truth", pnl=None, fill_size=1),
	]
	agg = aggregate_positions(rows, since=None, until=None)
	assert agg.n_positions == 1
	assert agg.n_lost_truth == 1                # surfaced, not in the P&L sample


def test_first_n_positions_ordered_by_entry_time():
	rows = [
		_row("late", entry_time="2026-06-22T09:00:00+00:00", pnl=1),
		_row("early", entry_time="2026-06-22T05:00:00+00:00", pnl=2),
	]
	agg = aggregate_positions(rows, since=None, until=None)
	assert [p.pnl_cents for p in agg.positions] == [2, 1]  # early first


def test_in_flight_rows_excluded_and_counted():
	rows = [
		{"client_order_id": "p1", "status": "pending", "pnl_cents": None, "fill_size": 0,
		 "entry_time": None, "placed_at_utc": "2026-06-22T05:00:00+00:00", "strategy": "s"},
		{"client_order_id": "o1", "status": "open", "pnl_cents": None, "fill_size": 1,
		 "entry_time": "2026-06-22T05:00:00+00:00", "placed_at_utc": "2026-06-22T05:00:00+00:00",
		 "strategy": "s"},
	]
	agg = aggregate_positions(rows, since=None, until=None)
	assert agg.positions == []
	assert agg.n_in_flight == 2
	assert agg.n_orders_placed == 0     # in-flight rows are not resolved placements
	assert agg.n_positions == 0


def test_zero_fill_size_position_aggregates_without_error():
	# Defensive: a filled-terminal row with fill_size=0 (should not happen, but must not crash).
	# Task 5's per-contract math must guard position_size==0 (it filters position_size>0).
	rows = [_row("z", status="scratch", pnl=0, fill_size=0)]
	agg = aggregate_positions(rows, since=None, until=None)
	assert agg.n_positions == 1
	assert agg.positions[0].position_size == 0


def test_filled_row_with_null_entry_time_is_flagged_not_dropped():
	rows = [
		{"client_order_id": "bad", "status": "won", "pnl_cents": 50, "fill_size": 1,
		 "entry_time": None, "placed_at_utc": "2026-06-22T05:00:00+00:00", "strategy": "s"},
	]
	agg = aggregate_positions(rows, since=None, until=None)
	assert agg.n_positions == 0          # not in the P&L sample (no entry_time)
	assert agg.n_lost_truth == 1         # surfaced as an anomaly, not silently dropped


# ---------------------------------------------------------------------------
# Task 3: bootstrap_ci
# ---------------------------------------------------------------------------


def test_bootstrap_ci_is_deterministic_under_seed():
	vals = [10, 12, -3, 8, 15, 9, 11, 7, 13, 6]
	a = bootstrap_ci(vals, seed=42, resamples=2000)
	b = bootstrap_ci(vals, seed=42, resamples=2000)
	assert a == b                       # same seed → identical
	assert a[0] < a[1]                  # lo < hi


def test_bootstrap_ci_clearly_positive_excludes_zero():
	vals = [50, 55, 48, 60, 52, 58, 49, 61, 53, 57]  # all strongly positive
	lo, hi = bootstrap_ci(vals, seed=1, resamples=5000)
	assert lo > 0


def test_bootstrap_ci_clearly_negative_hi_below_zero():
	# REJECT direction: a strongly-negative sample must yield ci_high < 0
	# (the branch Task 6's stage-0 mirage relies on to reject a losing strategy).
	vals = [-50.0, -55.0, -48.0, -60.0, -52.0, -58.0, -49.0, -61.0, -53.0, -57.0]
	lo, hi = bootstrap_ci(vals, seed=2, resamples=5000)
	assert hi < 0


def test_bootstrap_ci_empty_is_zero():
	assert bootstrap_ci([], seed=1) == (0.0, 0.0)


# ---------------------------------------------------------------------------
# Task 4: decide — asymmetric decision rule
# ---------------------------------------------------------------------------


def test_decide_graduate_requires_both_cis_positive_at_N():
	d, _ = decide(n=50, n_target=50, pt_lo=5, pt_hi=20, pc_lo=2, pc_hi=8, ceiling=False)
	assert d is Decision.GRADUATE


def test_decide_per_trade_pass_but_per_contract_spans_zero_is_inconclusive():
	d, reason = decide(n=50, n_target=50, pt_lo=5, pt_hi=20, pc_lo=-1, pc_hi=4, ceiling=False)
	assert d is Decision.INCONCLUSIVE
	assert "size-dependent" in reason


def test_decide_sub_cap_full_negative_ci_rejects():
	d, _ = decide(n=17, n_target=50, pt_lo=-40, pt_hi=-5, pc_lo=-20, pc_hi=-2, ceiling=False)
	assert d is Decision.REJECT


def test_decide_at_N_ci_low_not_positive_rejects():
	d, _ = decide(n=50, n_target=50, pt_lo=-2, pt_hi=10, pc_lo=-1, pc_hi=5, ceiling=False)
	assert d is Decision.REJECT


def test_decide_sub_cap_undetermined_keeps_running():
	d, _ = decide(n=20, n_target=50, pt_lo=-2, pt_hi=10, pc_lo=-1, pc_hi=5, ceiling=False)
	assert d is Decision.RUNNING


def test_decide_ceiling_with_undetermined_sign_is_inconclusive():
	d, _ = decide(n=20, n_target=50, pt_lo=-2, pt_hi=10, pc_lo=-1, pc_hi=5, ceiling=True)
	assert d is Decision.INCONCLUSIVE


def test_decide_n_above_target_cannot_graduate_on_streak():
	# n>50: graduation is only at exactly N — n>N never graduates (caller passes first-50 CIs,
	# but guard here too): treat n>n_target with otherwise-graduating CIs as REJECT-not-graduate.
	d, _ = decide(n=63, n_target=50, pt_lo=5, pt_hi=20, pc_lo=2, pc_hi=8, ceiling=False)
	assert d is Decision.REJECT


def test_decide_reject_via_full_negative_ci_takes_precedence_at_N():
	# Even at exactly N, a fully-negative CI must fire branch 1 (ci_high<0),
	# NOT fall through to the n==n_target block. Asserting the REASON pins which
	# branch fired — a branch reorder would change the reason, not just the outcome.
	d, reason = decide(n=50, n_target=50, pt_lo=-40, pt_hi=-5,
	                   pc_lo=2, pc_hi=8, ceiling=False)
	assert d is Decision.REJECT
	assert "ci_high" in reason  # came from branch 1, not "ci_low<=0 at N"


# ---------------------------------------------------------------------------
# Task 5: evaluate() — orchestration + truncation + re-gate sign-off
# ---------------------------------------------------------------------------


def _filled(coid: str, pnl: int, size: int = 1,
            t: str = "2026-06-22T05:00:00+00:00") -> dict:
	return _row(coid, status="won" if pnl >= 0 else "lost", pnl=pnl, fill_size=size, entry_time=t)


def test_evaluate_graduates_strong_positive_at_N():
	# 50 strongly +EV positions, 1 contract each → both CIs positive
	rows = [_filled(f"c{i}", pnl=40, size=1, t=f"2026-06-22T05:{i:02d}:00+00:00") for i in range(50)]
	v = evaluate(rows, since=None, until=None, n_target=50, seed=7)
	assert v.decision is Decision.GRADUATE
	assert v.n_positions == 50
	assert v.requires_signoff is False


def test_evaluate_surfaces_signoff_when_prior_rejected_and_would_graduate():
	rows = [_filled(f"c{i}", pnl=40, t=f"2026-06-22T05:{i:02d}:00+00:00") for i in range(50)]
	v = evaluate(rows, since=None, until=None, n_target=50, seed=7,
	             prior_rejected=True, attempt_num=2)
	assert v.decision is Decision.GRADUATE
	assert v.requires_signoff is True            # operator must sign off (spec re-gate guard)
	assert v.attempt_num == 2


def test_evaluate_truncates_to_first_n_positions():
	rows = [_filled(f"c{i}", pnl=40, t=f"2026-06-22T05:{i:02d}:00+00:00") for i in range(63)]
	v = evaluate(rows, since=None, until=None, n_target=50, seed=7)
	assert v.n_positions == 50                   # only the first 50 evaluated
	assert v.decision is Decision.GRADUATE        # decided AT N, not on the n=63 streak


def test_evaluate_prior_rejected_but_now_rejects_needs_no_signoff():
	# A re-gate that FAILS again does not require sign-off — there's nothing to graduate.
	rows = [_filled(f"c{i}", pnl=-40, size=1, t=f"2026-06-22T05:{i:02d}:00+00:00") for i in range(50)]
	v = evaluate(rows, since=None, until=None, n_target=50, seed=7,
	             prior_rejected=True, attempt_num=3)
	assert v.decision is Decision.REJECT
	assert v.requires_signoff is False
	assert v.attempt_num == 3


def test_evaluate_surfaces_lost_truth_alert_in_reason():
	# A lost_truth row is excluded from the P&L sample but surfaced as an ALERT in the reason.
	rows = [_filled(f"c{i}", pnl=40, size=1, t=f"2026-06-22T05:{i:02d}:00+00:00") for i in range(50)]
	rows.append({"client_order_id": "lt", "status": "lost_truth", "pnl_cents": None,
	             "fill_size": 1, "entry_time": "2026-06-22T06:00:00+00:00",
	             "placed_at_utc": "2026-06-22T06:00:00+00:00", "strategy": "s"})
	v = evaluate(rows, since=None, until=None, n_target=50, seed=7)
	assert v.n_lost_truth == 1
	assert "ALERT" in v.outcome_reason and "lost_truth" in v.outcome_reason


def test_evaluate_ceiling_subcap_undetermined_is_inconclusive():
	# Below N, sign undetermined, but the calendar/order ceiling was hit → INCONCLUSIVE (not RUNNING).
	rows = [_filled(f"c{i}", pnl=(40 if i % 2 else -40), size=1, t=f"2026-06-22T05:{i:02d}:00+00:00")
	        for i in range(20)]
	v = evaluate(rows, since=None, until=None, n_target=50, seed=7, ceiling_exceeded=True)
	assert v.n_positions == 20
	assert v.ceiling_exceeded is True
	assert v.decision is Decision.INCONCLUSIVE


def test_stage0_known_mirage_rejects():
	"""The real stage-0 run: 17 filled positions, 4 wins / 13 losses, net -$5.55 (-555c),
	mean ~= -33c/trade. The gate MUST REJECT on its own statistics (ci_high<0), sub-cap
	(17 < 50), independent of any kill (spec regression fixture)."""
	# 4 wins + 13 losses summing to -555c, ~ -33c mean; 1 contract each.
	wins = [60, 55, 50, 45]                       # +210
	losses = [-60] * 12 + [-45]                   # -765  -> total -555
	pnls = wins + losses
	assert sum(pnls) == -555 and len(pnls) == 17
	rows = [_filled(f"s0-{i}", pnl=p, size=1, t=f"2026-06-22T05:{i:02d}:00+00:00")
	        for i, p in enumerate(pnls)]
	v = evaluate(rows, since=None, until=None, n_target=50, seed=7)
	assert v.n_positions == 17
	assert v.ci_high < 0                          # full CI below 0
	assert v.decision is Decision.REJECT
