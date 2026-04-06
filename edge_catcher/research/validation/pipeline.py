"""ValidationPipeline: runs gates in order, short-circuits on first failure."""

from __future__ import annotations

import logging

from edge_catcher.research.hypothesis import HypothesisResult

from .gate import Gate, GateContext, GateResult

logger = logging.getLogger(__name__)


class ValidationPipeline:
	"""Chain of validation gates. Short-circuits on first failure."""

	def __init__(self, gates: list[Gate]) -> None:
		self.gates = gates

	def validate(
		self,
		result: HypothesisResult,
		context: GateContext,
	) -> tuple[str, str, list[GateResult]]:
		"""Run gates in order, short-circuit on first failure.

		Returns (verdict, reason, gate_results).
		verdict is 'promote' if all pass, 'explore' if any fail.
		"""
		gate_results: list[GateResult] = []

		for gate in self.gates:
			logger.info("Running validation gate: %s", gate.name)
			gate_result = gate.check(result, context)
			gate_results.append(gate_result)

			if not gate_result.passed:
				reason = (
					f"failed gate '{gate_result.gate_name}': {gate_result.reason}"
				)
				logger.info("  Gate '%s' FAILED: %s", gate.name, gate_result.reason)
				return "explore", reason, gate_results

			logger.info("  Gate '%s' PASSED: %s", gate.name, gate_result.reason)

		# All gates passed
		gate_names = ", ".join(g.gate_name for g in gate_results)
		reason = f"passed all validation gates ({gate_names})" if gate_names else "no gates configured"
		return "promote", reason, gate_results
