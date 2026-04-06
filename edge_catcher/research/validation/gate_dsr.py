"""Deflated Sharpe Ratio gate — adjusts observed Sharpe for multiple testing."""

from __future__ import annotations

import logging
import math
import re
import statistics
from collections import defaultdict

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

		# Count distinct strategies and collect all Sharpe values from tracker
		if context.tracker is None:
			return GateResult(
				passed=False, gate_name=self.name,
				reason="no tracker available for DSR computation",
				details={},
			)
		all_results = context.tracker.list_results()

		# Normalize backtester Sharpes to per-trade scale: bt_sharpe / sqrt(N).
		# This MUST match sr_observed above (mean/std without sqrt(N) scaling).
		# Group by strategy family (e.g. FooV1, FooV2 → Foo) so N counts
		# independent research ideas, not parameter variants.
		ok_results = [r for r in all_results if r.get("status") == "ok"]
		sharpes_by_family: dict[str, list[float]] = defaultdict(list)
		for r in ok_results:
			bt_sharpe = r["sharpe"]
			trades = r.get("total_trades", 0)
			if trades >= 1:
				family = _strategy_family(r["strategy"])
				sharpes_by_family[family].append(
					bt_sharpe / math.sqrt(trades)
				)

		# One representative Sharpe per family (mean across its backtests)
		ok_sharpes = [
			statistics.mean(v) for v in sharpes_by_family.values()
		]
		N = len(ok_sharpes)

		if N < 2:
			return GateResult(
				passed=False, gate_name=self.name,
				reason=f"only {N} strategy families tested, need ≥2 for DSR",
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
			"n_sharpes": sum(len(v) for v in sharpes_by_family.values()),
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
