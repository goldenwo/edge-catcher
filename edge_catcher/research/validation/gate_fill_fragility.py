"""Fill-fragility gate — flags candidates whose edge is realized on fills that
are not reliably takeable on the real exchange (the spot-fair failure mode).

SOFT gate: it NEVER fails a candidate (never routes to 'explore'). When a
candidate's execution archetype is fragile it sets ``tier="review"`` so the
candidate cannot auto-promote without live scrutiny. This is a coarse archetype
screen, NOT a fill simulator — the real takeability bar is the live
fill-realism gate (edge_catcher/fill_realism_gate.py).
"""

from __future__ import annotations

import logging

from edge_catcher.research.execution_archetype import (
	is_fragile,
	resolve_execution_archetype,
)
from edge_catcher.research.hypothesis import HypothesisResult

from .gate import Gate, GateContext, GateResult

logger = logging.getLogger(__name__)


class FillFragilityGate(Gate):
	"""Down-rank fill-fragile archetypes to the review tier (never fail)."""

	name = "fill_fragility"

	def check(self, result: HypothesisResult, context: GateContext) -> GateResult:
		archetype = resolve_execution_archetype(context.hypothesis.strategy)
		details = {"execution_archetype": archetype}

		if is_fragile(archetype):
			return GateResult(
				passed=True, gate_name=self.name,
				reason=(
					f"execution archetype '{archetype}' is fill-fragile "
					f"(not reliably takeable live) — flagged for review; "
					f"graduate only via the live fill-realism gate"
				),
				details=details,
				tier="review",
			)

		return GateResult(
			passed=True, gate_name=self.name,
			reason=f"execution archetype '{archetype}' is fill-robust",
			details=details,
		)
