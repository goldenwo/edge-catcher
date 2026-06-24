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
