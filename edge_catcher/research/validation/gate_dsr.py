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

		# N for the Bailey multiple-testing correction = number of distinct
		# experimental trials in the tracker. A trial is a unique
		# (strategy family, series, fee_pct) tuple: each one is an
		# independent pull from the null Sharpe distribution we're
		# implicitly selecting the best from.
		#
		# Rationale for family collapsing: LLM-generated variants (FooV1,
		# FooV2, ...) tested on the same series are closer to repeat
		# refinements of one idea than independent trials, so they collapse
		# into one trial via _strategy_family().
		#
		# Note: N is cumulative over the tracker's lifetime, so it grows
		# monotonically during a sweep. This means hypotheses evaluated
		# later in a sweep see a slightly larger N than earlier ones —
		# a known limitation; fixing it would require pre-snapshotting N.
		if context.tracker is None:
			return GateResult(
				passed=False, gate_name=self.name,
				reason="no tracker available for DSR computation",
				details={},
			)
		all_results = context.tracker.list_results()
		trials: set[tuple] = set()
		for r in all_results:
			if r.get("status") != "ok":
				continue
			trades = r.get("total_trades") or 0  # None → 0
			if trades < 1:
				continue
			strat = r.get("strategy")
			if not strat:
				continue
			trial_key = (
				_strategy_family(strat),
				r.get("series"),
				r.get("fee_pct"),
			)
			trials.add(trial_key)
		N = len(trials)

		if N < 2:
			return GateResult(
				passed=False, gate_name=self.name,
				reason=f"only {N} distinct trials tested, need >=2 for DSR",
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

		# Bailey & Lopez de Prado (2014), Eq. 9 — the standard error of the
		# Sharpe estimator under non-normality uses the *observed* Sharpe:
		#     denom = sqrt(1 - skew*SR_hat + ((raw_kurt - 1)/4) * SR_hat^2)
		#           = sqrt(1 - skew*SR_hat + ((kurt + 2)/4) * SR_hat^2)
		# (``kurt`` is excess kurtosis, raw kurtosis = kurt + 3.)
		denom_inner = 1 - skew * sr_observed + (kurt + 2) / 4 * sr_observed ** 2
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
					"denom_inner": round(denom_inner, 6),
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
			"denom_inner": round(denom_inner, 6),
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
