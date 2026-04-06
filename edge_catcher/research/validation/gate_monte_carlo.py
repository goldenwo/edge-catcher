"""Monte Carlo sign-flip permutation test gate."""

from __future__ import annotations

import logging
import random
import statistics

from edge_catcher.research.hypothesis import HypothesisResult

from .gate import Gate, GateContext, GateResult

logger = logging.getLogger(__name__)


class MonteCarloGate(Gate):
	"""Fail strategies whose mean return is not significantly different from zero."""

	name = "monte_carlo"

	def __init__(
		self,
		n_permutations: int = 1000,
		p_threshold: float = 0.05,
	) -> None:
		self.n_permutations = n_permutations
		self.p_threshold = p_threshold

	def check(self, result: HypothesisResult, context: GateContext) -> GateResult:
		pnl = context.pnl_values
		T = len(pnl)

		if T < 10:
			return GateResult(
				passed=False, gate_name=self.name,
				reason=f"only {T} trades, need ≥10 for Monte Carlo",
				details={"T": T},
			)

		obs_std = statistics.stdev(pnl)
		observed_sharpe = statistics.mean(pnl) / obs_std if obs_std > 0 else 0.0

		# Seed from dedup_key for reproducibility
		seed = hash(context.hypothesis.dedup_key())
		rng = random.Random(seed)

		count_ge = 0
		permuted_sharpes: list[float] = []

		for _ in range(self.n_permutations):
			flipped = [v * rng.choice((1, -1)) for v in pnl]
			std = statistics.stdev(flipped)
			s = statistics.mean(flipped) / std if std > 0 else 0.0
			permuted_sharpes.append(s)
			if s >= observed_sharpe:
				count_ge += 1

		# Standard permutation p-value includes the observed statistic itself
		# to avoid p=0, which would overstate significance.
		p_value = (count_ge + 1) / (self.n_permutations + 1)
		null_mean = statistics.mean(permuted_sharpes)
		null_std = statistics.stdev(permuted_sharpes) if len(permuted_sharpes) >= 2 else 0.0

		details = {
			"p_value": round(p_value, 4),
			"observed_sharpe": round(observed_sharpe, 4),
			"n_permutations": self.n_permutations,
			"null_mean": round(null_mean, 4),
			"null_std": round(null_std, 4),
			"T": T,
		}

		if p_value < self.p_threshold:
			return GateResult(
				passed=True, gate_name=self.name,
				reason=f"p-value {p_value:.3f} < {self.p_threshold}",
				details=details,
			)
		return GateResult(
			passed=False, gate_name=self.name,
			reason=f"p-value {p_value:.3f} ≥ {self.p_threshold}",
			details=details,
		)
