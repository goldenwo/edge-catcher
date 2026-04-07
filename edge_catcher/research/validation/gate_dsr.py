"""Deflated Sharpe Ratio gate — adjusts observed Sharpe for multiple testing."""

from __future__ import annotations

import logging
import math
import re
import statistics

from scipy.stats import norm

from edge_catcher.research.hypothesis import HypothesisResult

from .gate import Gate, GateContext, GateResult

logger = logging.getLogger(__name__)

# Euler-Mascheroni constant
_GAMMA = 0.5772156649

# Regex to strip all trailing V\d+ suffixes: FooV1 → Foo, MomentumV2V3 → Momentum
_FAMILY_RE = re.compile(r"(V\d+)+$")


def _strategy_family(name: str) -> str:
	"""Strip all trailing V\\d+ suffixes to get the strategy family root."""
	return _FAMILY_RE.sub("", name) or name


class DeflatedSharpeGate(Gate):
	"""Fail strategies whose Sharpe is not significant after correcting for N trials."""

	name = "deflated_sharpe"

	def __init__(
		self,
		threshold: float = 0.95,
		review_floor: float = 0.80,
	) -> None:
		self.threshold = threshold
		self.review_floor = review_floor

	def check(self, result: HypothesisResult, context: GateContext) -> GateResult:
		pnl = context.pnl_values
		T = len(pnl)

		if T < 10:
			return GateResult(
				passed=False, gate_name=self.name,
				reason=f"only {T} trades, need ≥10 for DSR",
				details={"T": T},
			)

		# Low trade count: DSR has no statistical power, defer to other gates.
		if T < 50:
			return GateResult(
				passed=True, gate_name=self.name,
				reason=f"DSR skipped: only {T} trades (< 50), deferring to other gates",
				details={"T": T, "skipped": True},
			)

		# Per-trade Sharpe: mean(pnl) / stdev(pnl), no sqrt(N) scaling.
		# The backtester computes sharpe = mean/std * sqrt(N), so historical
		# Sharpes below are divided by sqrt(trades) to match this scale.
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

		# Count distinct strategy families for N (independent research ideas)
		if context.tracker is None:
			return GateResult(
				passed=False, gate_name=self.name,
				reason="no tracker available for DSR computation",
				details={},
			)
		all_results = context.tracker.list_results()
		ok_results = [r for r in all_results if r.get("status") == "ok"]
		families: set[str] = set()
		for r in ok_results:
			if r.get("total_trades", 0) >= 1:
				families.add(_strategy_family(r["strategy"]))
		N = len(families)

		if N < 2:
			return GateResult(
				passed=False, gate_name=self.name,
				reason=f"only {N} strategy families tested, need >=2 for DSR",
				details={"n_strategies": N},
			)

		# SR0: expected maximum per-trade Sharpe from noise.
		# Use the theoretical null variance 1/(T-1) rather than observed
		# variance of family-mean Sharpes. The observed approach breaks when
		# strategies have heterogeneous trade counts (low-trade strategies
		# inflate variance) and families are tested across many series
		# (family means bury signal under cross-series noise).
		sr_std = 1.0 / math.sqrt(T - 1)
		sr0 = sr_std * (
			(1 - _GAMMA) * norm.ppf(1 - 1 / N)
			+ _GAMMA * norm.ppf(1 - 1 / (N * math.e))
		)

		# DSR formula (kurt is excess kurtosis, so raw kurtosis = kurt + 3)
		# denominator = sqrt(1 - skew*SR0 + (raw_kurt - 1)/4 * SR0^2)
		#             = sqrt(1 - skew*SR0 + (kurt + 2)/4 * SR0^2)
		denom_inner = 1 - skew * sr0 + (kurt + 2) / 4 * sr0 ** 2
		if denom_inner <= 0:
			# Pathological case: extreme skew makes SE undefined.
			# The DSR statistic has no valid interpretation here.
			return GateResult(
				passed=False, gate_name=self.name,
				reason=(
					f"DSR denominator non-positive ({denom_inner:.4f}); "
					f"extreme skew ({skew:.2f}) invalidates the test"
				),
				details={
					"sr_observed": round(sr_observed, 4),
					"sr0": round(sr0, 4),
					"n_strategies": N,
					"skewness": round(skew, 4),
					"kurtosis": round(kurt, 4),
					"denom_inner": round(denom_inner, 4),
					"T": T,
				},
			)
		denom = math.sqrt(denom_inner)
		dsr = norm.cdf((sr_observed - sr0) * math.sqrt(T - 1) / denom)

		details = {
			"dsr": round(dsr, 4),
			"sr_observed": round(sr_observed, 4),
			"sr0": round(sr0, 4),
			"n_strategies": N,
			"sr_std": round(sr_std, 4),
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

		if dsr >= self.review_floor:
			return GateResult(
				passed=True, gate_name=self.name,
				reason=f"DSR {dsr:.3f} in review band [{self.review_floor}, {self.threshold})",
				details=details,
				tier="review",
			)

		return GateResult(
			passed=False, gate_name=self.name,
			reason=f"DSR {dsr:.3f} < {self.review_floor}",
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
