"""Shared statistical utilities for hypothesis testing."""

from __future__ import annotations

import math
from collections import defaultdict

from edge_catcher.fees import FeeModel


def proportions_ztest(wins: int, n: int, p0: float) -> tuple[float, float]:
	"""One-sample proportions z-test. Returns (z_stat, p_value).

	Tests whether observed win rate differs from null proportion p0.
	"""
	if n == 0 or p0 <= 0 or p0 >= 1:
		return (0.0, 1.0)
	from statsmodels.stats.proportion import proportions_ztest as _ztest
	z, p = _ztest(wins, n, p0)
	return (float(z), float(p))


def clustered_z(
	rows: list[tuple[float, bool, str | None]],
) -> tuple[float, float, int]:
	"""Compute clustered z-statistic grouped by cluster key.

	Each row is (implied_prob, won: bool, cluster_key: str|None).
	Returns (z_stat, p_value, n_clusters).
	"""
	from scipy.stats import norm

	clusters: dict[str, dict] = defaultdict(lambda: {"wins": 0, "n": 0, "implied": []})
	for implied, won, cluster_key in rows:
		key = cluster_key or "__no_key__"
		clusters[key]["wins"] += int(won)
		clusters[key]["n"] += 1
		clusters[key]["implied"].append(implied)

	if len(clusters) < 2:
		return (0.0, 1.0, len(clusters))

	excess = []
	for c in clusters.values():
		mean_implied = sum(c["implied"]) / len(c["implied"])
		excess.append(c["wins"] / c["n"] - mean_implied)

	k = len(excess)
	mean_exc = sum(excess) / k
	var = sum((x - mean_exc) ** 2 for x in excess) / (k - 1)
	se = math.sqrt(var / k)
	if se == 0:
		# All clusters show identical excess — effect is real but variance is zero.
		# Return a large z with sign matching the direction of the effect.
		if mean_exc == 0.0:
			return (0.0, 1.0, k)
		z = math.copysign(100.0, mean_exc)
		p = 0.0
		return (float(z), float(p), k)

	z = mean_exc / se
	p = 2 * (1 - norm.cdf(abs(z)))
	return (float(z), float(p), k)


def wilson_ci(wins: int, n: int, z: float = 1.96) -> tuple[float, float]:
	"""Wilson score confidence interval — better than Wald near 0 and 1."""
	if n == 0:
		return (0.0, 0.0)
	p = wins / n
	denom = 1 + z * z / n
	centre = (p + z * z / (2 * n)) / denom
	margin = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
	lo = centre - margin
	hi = centre + margin
	# Round to 14 sig-fig precision before clamping to avoid sub-ULP surprises
	# (e.g. 0.9999999999999999 when true value is exactly 1.0).
	lo = round(lo, 14)
	hi = round(hi, 14)
	return (max(0.0, lo), min(1.0, hi))


def fee_adjusted_edge(raw_edge: float, implied_prob: float, maker_fee_rate: float) -> float:
	"""Subtract maker fee impact from raw edge (flat-rate approximation).

	Fee = maker_fee_rate * (1 - implied_prob) per contract.

	NOTE: this is a flat-rate *approximation*, not Kalshi's real per-contract fee.
	The exchange charges ceil(0.07 * p * (1-p) * 100) cents/contract per side
	(see edge_catcher/adapters/kalshi/fees.py); the linear `rate * (1 - p)` form
	used here is a stand-in calibrated near the longshot bucket. Prefer
	fee_adjusted_edge_curve() for live-fidelity gating; this flat form remains for
	the legacy hypothesis template (ai/formalizer.py) and
	hypotheses/kalshi/price_efficiency.py.
	"""
	return raw_edge - maker_fee_rate * (1.0 - implied_prob)


def fee_adjusted_edge_curve(raw_edge: float, implied_prob: float, fee_model: FeeModel) -> float:
	"""Subtract the exchange's real per-contract entry fee from the raw edge.

	Unlike fee_adjusted_edge (a flat-rate approximation), this charges the
	exchange's actual fee curve via FeeModel.calculate(), so the gate matches live
	execution. The edge is a per-$1-notional quantity (win-rate minus implied
	price), so we charge the single-contract fee at the implied price and convert
	cents → dollars. The fee is charged once on entry; a buy-to-settlement
	position pays no exit fee.
	"""
	price_cents = round(implied_prob * 100)
	fee_cents = fee_model.calculate(price_cents, 1)
	return raw_edge - fee_cents / 100.0
