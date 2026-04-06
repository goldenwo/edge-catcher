"""Deflated Sharpe Ratio gate — adjusts observed Sharpe for multiple testing."""

from __future__ import annotations

import logging
import math
import statistics

from scipy.stats import norm

from edge_catcher.research.hypothesis import HypothesisResult

from .gate import Gate, GateContext, GateResult

logger = logging.getLogger(__name__)

# Euler-Mascheroni constant
_GAMMA = 0.5772156649


class DeflatedSharpeGate(Gate):
	"""Fail strategies whose Sharpe is not significant after correcting for N trials."""

	name = "deflated_sharpe"

	def __init__(self, threshold: float = 0.95) -> None:
		self.threshold = threshold

	def check(self, result: HypothesisResult, context: GateContext) -> GateResult:
		pnl = context.pnl_values
		T = len(pnl)

		if T < 10:
			return GateResult(
				passed=False, gate_name=self.name,
				reason=f"only {T} trades, need ≥10 for DSR",
				details={"T": T},
			)

		# Non-annualized per-trade Sharpe: mean/std (no sqrt(N) scaling)
		mu = statistics.mean(pnl)
		std = statistics.stdev(pnl)
		if std == 0:
			return GateResult(
				passed=False, gate_name=self.name,
				reason="zero variance in returns",
				details={"T": T, "std": 0},
			)
		sr_observed = mu / std

		# Skewness and excess kurtosis of the return series
		skew = _skewness(pnl)
		kurt = _kurtosis(pnl)  # excess kurtosis: normal = 0

		# Count distinct strategies and collect all Sharpe values from tracker
		if context.tracker is None:
			return GateResult(
				passed=False, gate_name=self.name,
				reason="no tracker available for DSR computation",
				details={},
			)
		all_results = context.tracker.list_results()
		ok_sharpes = [r["sharpe"] for r in all_results if r.get("status") == "ok"]
		strategy_names = {r["strategy"] for r in all_results if r.get("status") == "ok"}
		N = len(strategy_names)

		if N < 2 or len(ok_sharpes) < 2:
			return GateResult(
				passed=False, gate_name=self.name,
				reason=f"only {N} strategies tested, need ≥2 for DSR",
				details={"n_strategies": N},
			)

		# SR0: expected maximum Sharpe from noise
		sr_var = statistics.variance(ok_sharpes)
		if sr_var <= 0:
			sr_var = 1e-6  # avoid sqrt(0)
		sr0 = math.sqrt(sr_var) * (
			(1 - _GAMMA) * norm.ppf(1 - 1 / N)
			+ _GAMMA * norm.ppf(1 - 1 / (N * math.e))
		)

		# DSR formula (kurt is excess kurtosis, so raw kurtosis = kurt + 3)
		# denominator = sqrt(1 - skew*SR0 + (raw_kurt - 1)/4 * SR0^2)
		#             = sqrt(1 - skew*SR0 + (kurt + 2)/4 * SR0^2)
		denom = math.sqrt(abs(1 - skew * sr0 + (kurt + 2) / 4 * sr0 ** 2))
		if denom <= 0:
			denom = 1e-6
		dsr = norm.cdf((sr_observed - sr0) * math.sqrt(T - 1) / denom)

		details = {
			"dsr": round(dsr, 4),
			"sr_observed": round(sr_observed, 4),
			"sr0": round(sr0, 4),
			"n_strategies": N,
			"n_sharpes": len(ok_sharpes),
			"sr_var": round(sr_var, 4),
			"skewness": round(skew, 4),
			"kurtosis": round(kurt, 4),
			"T": T,
		}

		if dsr >= self.threshold:
			return GateResult(
				passed=True, gate_name=self.name,
				reason=f"DSR {dsr:.3f} ≥ {self.threshold}",
				details=details,
			)
		return GateResult(
			passed=False, gate_name=self.name,
			reason=f"DSR {dsr:.3f} < {self.threshold}",
			details=details,
		)


def _skewness(values: list[float]) -> float:
	"""Sample skewness using scipy."""
	from scipy.stats import skew
	return float(skew(values, bias=False))


def _kurtosis(values: list[float]) -> float:
	"""Sample excess kurtosis using scipy (normal = 0)."""
	from scipy.stats import kurtosis as sp_kurtosis
	return float(sp_kurtosis(values, bias=False))
