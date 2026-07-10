"""Shared statistical utilities for hypothesis testing."""

from __future__ import annotations

import math
from collections import defaultdict

from edge_catcher.fees import FeeModel

# z_over_excesses' zero-variance sentinel: identical excess in every cluster
# with a nonzero mean returns ±this value. mc_null_pvalue mirrors it for
# simulated statistics, and the MC extreme-count comparison |z_sim| >= |z_obs|
# relies on BOTH sides using the SAME magnitude — an observed sentinel must tie
# with simulated sentinels (the documented conservative tie-counting), never
# out-run them. Always reference this constant; never inline the literal.
ZERO_VARIANCE_Z_SENTINEL = 100.0


def proportions_ztest(wins: int, n: int, p0: float) -> tuple[float, float]:
	"""One-sample proportions z-test. Returns (z_stat, p_value).

	Tests whether observed win rate differs from null proportion p0.
	"""
	if n == 0 or p0 <= 0 or p0 >= 1:
		return (0.0, 1.0)
	from statsmodels.stats.proportion import proportions_ztest as _ztest
	z, p = _ztest(wins, n, p0)
	return (float(z), float(p))


def z_over_excesses(excess: list[float]) -> tuple[float, float, int]:
	"""z-statistic over per-cluster excess values (shared clustered-z core).

	Public: clustered_z and clustered_z_from_stats wrap it, and lifecycle's paired
	early-vs-late differential feeds precomputed per-day contrasts directly. Its
	degenerate-branch semantics are a cross-module contract.

	Returns (z_stat, p_value, n_clusters). Fewer than 2 clusters → (0, 1, k).
	Zero between-cluster variance with a nonzero mean excess → ±100 with the
	effect's sign (identical excess in every cluster: real effect, zero variance).
	"""
	from scipy.stats import norm

	k = len(excess)
	if k < 2:
		return (0.0, 1.0, k)

	mean_exc = sum(excess) / k
	var = sum((x - mean_exc) ** 2 for x in excess) / (k - 1)
	se = math.sqrt(var / k)
	if se == 0:
		if mean_exc == 0.0:
			return (0.0, 1.0, k)
		return (math.copysign(ZERO_VARIANCE_Z_SENTINEL, mean_exc), 0.0, k)

	z = mean_exc / se
	p = 2 * (1 - norm.cdf(abs(z)))
	return (float(z), float(p), k)


def t_pvalue(stat: float, df: int) -> float:
	"""Two-sided Student-t p-value for a mean/SE statistic over df+1 clusters.

	z_over_excesses' statistic is a sample mean over k cluster excesses divided
	by its estimated SE — under the null that is t-distributed with k−1 df, not
	normal. Grading small-k statistics on the normal reference overstates
	significance (artifact class (e), proven by real adversarial refutations of
	findings that cleared their Bonferroni alpha under the normal reference and
	failed under t). df < 1 means no usable reference → 1.0 (never significant).
	"""
	if df < 1:
		return 1.0
	from scipy.stats import t

	return float(2.0 * t.sf(abs(stat), df))


def exact_binom_pvalue(wins: int, n: int, p: float) -> float:
	"""Two-sided exact binomial p-value for `wins` successes in `n` trials at `p`.

	The cross-check for artifact class (c): when a band's market-level expected
	wins or losses are tiny, the clustered z degenerates to price dispersion and
	only the exact outcome-count test is trustworthy. Degenerate inputs (n == 0,
	p outside (0, 1)) have no testable null → 1.0 (never significant).
	"""
	if n <= 0 or p <= 0.0 or p >= 1.0:
		return 1.0
	from scipy.stats import binomtest

	return float(binomtest(wins, n, p).pvalue)


def clustered_z(
	rows: list[tuple[float, bool, str | None]],
) -> tuple[float, float, int]:
	"""Compute clustered z-statistic grouped by cluster key.

	Each row is (implied_prob, won: bool, cluster_key: str|None).
	Returns (z_stat, p_value, n_clusters).
	"""
	clusters: dict[str, dict] = defaultdict(lambda: {"wins": 0, "n": 0, "implied": []})
	for implied, won, cluster_key in rows:
		key = cluster_key or "__no_key__"
		clusters[key]["wins"] += int(won)
		clusters[key]["n"] += 1
		clusters[key]["implied"].append(implied)

	excess = [
		c["wins"] / c["n"] - sum(c["implied"]) / len(c["implied"])
		for c in clusters.values()
	]
	return z_over_excesses(excess)


def clustered_z_from_stats(
	clusters: list[tuple[int, int, float]],
) -> tuple[float, float, int]:
	"""clustered_z's aggregate-input twin: one (n, wins, sum_implied) per cluster.

	Identical to clustered_z on the same population **iff each aggregate summarizes
	one row per observation** (n = row count, sum_implied = Σ per-row implied), since
	clustered_z's per-cluster mean_implied is an unweighted mean over its rows:
	cluster excess = wins/n − sum_implied/n. Callers that aggregate in SQL (one
	GROUP BY row per cluster) use this to avoid materializing millions of per-trade
	rows in Python. Returns (z_stat, p_value, n_clusters).
	"""
	excess = [wins / n - sum_implied / n for n, wins, sum_implied in clusters if n > 0]
	return z_over_excesses(excess)


def mc_null_pvalue(
	market_stats: list[tuple[str | None, int, float]],
	z_obs: float,
	n_sims: int = 10_000,
	seed: int = 20260703,
) -> float:
	"""Monte-Carlo null p-value for a day-clustered band statistic (class (d)).

	`market_stats` is one (day, n_trades, sum_price) per in-band MARKET (prices
	on the 0–1 scale). Each simulation redraws every market's settlement as an
	independent Bernoulli at its own in-band mean traded price — the fair-priced
	null — while keeping the day-cluster structure and per-market print counts,
	then recomputes the exact z_over_excesses statistic over per-day excesses
	(including its ±100 zero-variance sentinel). Returns the add-one-corrected
	two-sided p: (1 + #{|z_sim| >= |z_obs|}) / (n_sims + 1) — never exactly 0.

	This is the honest reference when E[outcome-flips] is small: with few
	clusters, extreme prices, and lumpy per-day counts, the day-excess
	distribution is discrete and skewed, and the normal/t machinery overstates
	significance by orders of magnitude (proven by real adversarial refutations
	where the honest MC p sat many orders above the nominal claim). Fixed seed
	→ reproducible verdicts; fewer than 2 day clusters → 1.0 (no null to
	simulate).

	Known conservative limit: when z_obs is itself the ±100 sentinel, the
	comparison degenerates to "how often does the null also produce identical
	day excesses", which can be small for lumpy cells; the class (c) gate and
	the exact-binomial cross-check remain the guards for that shape.
	"""
	import numpy as np

	if not market_stats:
		return 1.0
	day_keys = sorted({d if d is not None else "__no_key__" for d, _, _ in market_stats})
	k = len(day_keys)
	if k < 2:
		return 1.0
	day_id = {d: i for i, d in enumerate(day_keys)}

	n_m = np.array([n for _, n, _ in market_stats], dtype=np.float64)
	p_m = np.clip(
		np.array([sp / n for _, n, sp in market_stats], dtype=np.float64), 0.0, 1.0
	)
	sp_m = np.array([sp for _, _, sp in market_stats], dtype=np.float64)
	day_idx = np.array(
		[day_id[d if d is not None else "__no_key__"] for d, _, _ in market_stats]
	)
	# Sort markets by day once so per-sim day aggregation is a segment sum
	# (np.add.reduceat, O(M) per sim) — a dense M×k one-hot matmul is O(M·k)
	# memory and O(M·k) flops per sim, which OOMs/hangs on long-history
	# high-frequency series (tens of thousands of markets × hundreds of days).
	order = np.argsort(day_idx, kind="stable")
	n_sorted = n_m[order]
	p_sorted = p_m[order]
	# Every day id occurs (day_keys came from market_stats), so the segment
	# boundaries are strictly increasing and reduceat is well-defined.
	boundaries = np.searchsorted(day_idx[order], np.arange(k))
	day_n = np.bincount(day_idx, weights=n_m, minlength=k)
	day_sum_price = np.bincount(day_idx, weights=sp_m, minlength=k)
	day_implied = day_sum_price / day_n

	rng = np.random.default_rng(seed)
	# Chunk sims to bound the (chunk × M) draw matrix at ~64 MB of float64.
	chunk = max(1, min(n_sims, 8_000_000 // len(market_stats)))
	extreme = 0
	done = 0
	while done < n_sims:
		c = min(chunk, n_sims - done)
		wins = (rng.random((c, len(market_stats))) < p_sorted) * n_sorted
		excess = np.add.reduceat(wins, boundaries, axis=1) / day_n - day_implied
		mean = excess.mean(axis=1)
		se = np.sqrt(excess.var(axis=1, ddof=1) / k)
		with np.errstate(divide="ignore", invalid="ignore"):
			z_sim = np.where(
				se > 0.0,
				mean / np.where(se > 0.0, se, 1.0),
				np.where(
					mean == 0.0, 0.0, np.copysign(ZERO_VARIANCE_Z_SENTINEL, mean)
				),
			)
		extreme += int((np.abs(z_sim) >= abs(z_obs) - 1e-12).sum())
		done += c

	return (1 + extreme) / (n_sims + 1)


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
