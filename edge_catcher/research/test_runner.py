"""Generic statistical test runner for hypothesis validation."""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from typing import ClassVar, Optional

from edge_catcher.adapters.kalshi.fees import INDEX_FEE, STANDARD_FEE
from edge_catcher.fees import ZERO_FEE, FeeModel
from edge_catcher.research.stats_utils import (
	clustered_z,
	fee_adjusted_edge_curve,
	proportions_ztest,
	wilson_ci,
)

logger = logging.getLogger(__name__)

# Verdict constants
EDGE_EXISTS = "EDGE_EXISTS"
NO_EDGE = "NO_EDGE"
INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
EDGE_NOT_TRADEABLE = "EDGE_NOT_TRADEABLE"

# Fee model mapping: name → FeeModel. The gate applies each model's real
# per-contract fee curve via fee_adjusted_edge_curve() (see stats_utils.py), so
# fee accounting matches live execution rather than a flat approximation.
FEE_MODELS: dict[str, FeeModel] = {
	"zero": ZERO_FEE,
	"standard": STANDARD_FEE,
	"index": INDEX_FEE,
}
# "kalshi" is the name research configs pass for the live Kalshi taker fee; it
# resolves to the standard fee model. Aliased (not a second entry) so the two
# can't silently drift apart.
FEE_MODELS["kalshi"] = FEE_MODELS["standard"]


def _resolve_fee_model(fee_model: str) -> FeeModel:
	"""Map a fee_model name to its FeeModel, failing loud on unknown names.

	An unknown name used to fall back to 0.0 silently, which ran the fee-adjusted
	edge gate with ZERO fees — a false-positive risk where a small raw edge could
	pass a gate that real fees would have killed. Unknown names now raise.
	"""
	if fee_model not in FEE_MODELS:
		raise ValueError(
			f"Unknown fee_model {fee_model!r}. Valid options: {sorted(FEE_MODELS)}"
		)
	return FEE_MODELS[fee_model]


def _normalize_buckets(buckets: list[list[float]]) -> list[tuple[float, float]]:
	"""Normalize bucket bounds to the 0–1 implied-probability scale, PER BUCKET.

	Fail-safe for #1: the LLM ideator historically emitted cents-scale buckets
	(e.g. [[1,30]]) against 0–1 implied data, so `1 <= implied` was unsatisfiable
	→ every bucket n=0 → a silent INSUFFICIENT_DATA total-drop.

	Normalization is PER BUCKET (not whole-list): a single cents-scale bucket must
	not corrupt valid 0–1 siblings. For each `[lo, hi]`, if EITHER bound > 1.0 the
	bucket is treated as cents-scale and both its bounds are divided by 100; ≤1
	bounds are left as-is (a `[0, 1]` bucket is the full 0–1 probability range —
	the correct default, not cents to disambiguate). We warn once per call listing
	which buckets were rescaled — turning a silent drop into a corrected run plus a
	visible warning (mirrors the fail-loud stance on fee models).

	After normalization we VALIDATE and raise ValueError (fail loud, consistent with
	the fee_model handling) if any bucket has `lo >= hi`, `lo < 0`, or `hi > 1.0`.
	"""
	normalized: list[tuple[float, float]] = []
	rescaled: list[list[float]] = []
	for lo, hi in buckets:
		if lo > 1.0 or hi > 1.0:
			rescaled.append([lo, hi])
			normalized.append((lo / 100.0, hi / 100.0))
		else:
			normalized.append((lo, hi))

	if rescaled:
		logger.warning(
			"Bucket(s) %r appear to be cents-scale (a bound > 1.0); "
			"auto-normalizing those to the 0–1 implied-probability scale by dividing by 100. "
			"Configs should pass buckets on a 0–1 scale (e.g. [[0.01,0.30]]).",
			rescaled,
		)

	for lo, hi in normalized:
		if lo >= hi or lo < 0.0 or hi > 1.0:
			raise ValueError(
				f"Invalid bucket [{lo}, {hi}] after normalization: require "
				f"0.0 <= lo < hi <= 1.0."
			)

	return normalized


def _trade_price_cluster_rows(
	cursor: sqlite3.Cursor,
	series: str,
	lo: float,
	hi: float,
	ticker_filter: Optional[set[str]] = None,
) -> list[tuple[float, bool, str | None]]:
	"""Return one calibration row per market that traded in price band [lo, hi).

	Conditions on the price each trade was actually placed at (not a per-market
	lifetime VWAP). For each market, the count-weighted mean yes_price over trades
	whose price falls in [lo, hi) (0–1 scale) is the market's implied price in the
	band; the market's settled outcome is the realization. Each row's CLUSTER KEY is
	the market's close_date (day), so feeding these to clustered_z gives effective
	N = #independent days in the band, not #markets. Clustering by day (not ticker)
	is critical: 15-min markets produce ~96 correlated markets/day, and one-cluster-
	per-market understates the SE and fabricates edges (25–59% false-positive rate
	under intraday correlation). Matches the sibling
	hypotheses/kalshi/price_efficiency.py, which also clusters by close_date.

	Per-trade calibration is unbiased ONLY if the trade price is a fair belief at
	trade time. Systematic price drift on structurally inefficient markets (e.g. a
	market whose price predictably drifts toward its outcome after entry) can leave a
	real-looking calibration residual inside a band and surface as an apparent signal
	— it is a genuine inefficiency, not a per-trade-VWAP artifact, but callers should
	not read it as a pure calibration result.

	`lo`/`hi` are on the 0–1 scale; the SQL band filter compares INTEGER cents. We
	round to integer cent bounds (`round(lo*100)`, `round(hi*100)`) rather than
	binding floats, because `0.07*100 == 7.0000000000000009 > 7` silently drops/
	misfiles boundary cents against the INTEGER `yes_price` column.
	`ticker_filter`, when given, restricts to those tickers (used by the volume
	terciles in VolumeMispricingTest).

	The cluster key (close_date) is a real string when close_time is populated and
	None otherwise; the return type is `str | None` to match clustered_z's signature
	(which we deliberately do not modify). GROUP BY t.ticker is kept — the count-
	weighted mean price is still per-market; only the returned cluster KEY changed
	from ticker to close_date.
	"""
	lo_c = round(lo * 100)
	hi_c = round(hi * 100)
	params: list[object] = [series, lo_c, hi_c]
	ticker_clause = ""
	if ticker_filter is not None:
		if not ticker_filter:
			return []
		placeholders = ",".join("?" for _ in ticker_filter)
		ticker_clause = f" AND t.ticker IN ({placeholders})"
		params.extend(sorted(ticker_filter))

	cursor.execute(
		"SELECT t.ticker, m.result, m.close_time, "
		"       SUM(CAST(t.yes_price AS REAL) * t.count) / SUM(t.count) / 100.0 AS mean_price "
		"FROM trades t JOIN markets m ON t.ticker = m.ticker "
		"WHERE m.series_ticker = ? AND m.result IS NOT NULL "
		"  AND t.yes_price >= ? AND t.yes_price < ?"
		f"{ticker_clause} "
		"GROUP BY t.ticker",
		params,
	)
	rows: list[tuple[float, bool, str | None]] = []
	for ticker, result, close_time, mean_price in cursor.fetchall():
		if mean_price is None:
			continue
		close_date = close_time[:10] if close_time else None
		rows.append((float(mean_price), result == "yes", close_date))
	return rows


def _bonferroni_z_threshold(z_threshold: float, k: int) -> float:
	"""Bonferroni-correct a z-threshold for K simultaneously evaluated buckets.

	Converts the per-test z-threshold to its two-sided alpha, divides alpha by K,
	and returns the z-threshold for that corrected alpha. Conservative and simple
	(no assumption about bucket independence beyond Bonferroni's worst case).
	"""
	from scipy.stats import norm

	if k <= 1:
		return z_threshold
	alpha = 2.0 * (1.0 - norm.cdf(z_threshold))
	if alpha <= 0.0:
		return z_threshold
	return float(norm.ppf(1.0 - (alpha / k) / 2.0))


def _bucket_bonferroni_verdict(
	bucket_results: list[dict],
	z_threshold: float,
	min_fee_adj: float,
	any_bucket_met_min_n: bool,
) -> tuple[str, Optional[dict], float, float]:
	"""Un-pooled per-bucket verdict with Bonferroni multiple-testing correction.

	Each entry in `bucket_results` must carry "z" (clustered z) and "fee_adj"
	(fee_adjusted_edge_curve). K = number of evaluated buckets (those that met
	min_n). A bucket *qualifies* iff |z| >= z_corr AND fee_adj > max(min_fee_adj,
	0.0), where z_corr is the Bonferroni-corrected threshold for K buckets. The floor
	is clamped at 0 so EDGE_EXISTS always requires a genuinely net-positive fee-
	adjusted edge even if a config passes a negative `min_fee_adjusted_edge`. Verdict:
	  - EDGE_EXISTS        if ≥1 bucket qualifies;
	  - EDGE_NOT_TRADEABLE if ≥1 significant (|z|>=z_corr) bucket is fee-walled
	                       (fee_adj <= 0);
	  - NO_EDGE            if ≥1 bucket met min_n but none qualifies;
	  - INSUFFICIENT_DATA  if no bucket met min_n.

	Does not pool opposite-sign buckets, so a +edge longshot and a −edge favorite
	cannot cancel. Returns (verdict, driver_bucket, z_stat, fee_adjusted_edge),
	where the driver is the qualifying bucket with the largest |fee_adj| (most
	economically meaningful), or the max-|z| bucket if none qualifies.
	"""
	if not any_bucket_met_min_n or not bucket_results:
		return (INSUFFICIENT_DATA, None, 0.0, 0.0)

	k = len(bucket_results)
	z_corr = _bonferroni_z_threshold(z_threshold, k)

	fee_floor = max(min_fee_adj, 0.0)
	qualifying = [b for b in bucket_results if abs(b["z"]) >= z_corr and b["fee_adj"] > fee_floor]
	significant_fee_walled = [
		b for b in bucket_results if abs(b["z"]) >= z_corr and b["fee_adj"] <= 0
	]

	if qualifying:
		driver = max(qualifying, key=lambda b: abs(b["fee_adj"]))
		verdict = EDGE_EXISTS
	elif significant_fee_walled:
		driver = max(significant_fee_walled, key=lambda b: abs(b["z"]))
		verdict = EDGE_NOT_TRADEABLE
	else:
		driver = max(bucket_results, key=lambda b: abs(b["z"]))
		verdict = NO_EDGE

	return (verdict, driver, float(driver["z"]), float(driver["fee_adj"]))


@dataclass
class TestResult:
	"""Result of a statistical hypothesis test."""

	# Tell pytest this is a domain class, not a test class. Without this,
	# pytest tries to collect TestResult (and its `__init__`) wherever it's
	# imported into a test module and emits a PytestCollectionWarning.
	__test__ = False

	verdict: str
	z_stat: float
	fee_adjusted_edge: float
	detail: dict


class StatisticalTest:
	"""Base class for all statistical test types."""
	name: ClassVar[str] = ""

	def run(
		self,
		conn: sqlite3.Connection,
		series: str,
		params: dict,
		thresholds: dict,
	) -> TestResult:
		raise NotImplementedError


def _compute_vwap(cursor: sqlite3.Cursor, ticker: str, last_price: Optional[float]) -> Optional[float]:
	"""Return volume-weighted average price (0-1 scale) for a market.

	Falls back to last_price when no trades exist.
	Returns None when no price signal is available.
	"""
	cursor.execute(
		"SELECT SUM(CAST(yes_price AS REAL) * count) / SUM(count) FROM trades WHERE ticker = ?",
		(ticker,),
	)
	row = cursor.fetchone()
	vwap_cents = row[0] if row else None

	if vwap_cents is not None:
		return vwap_cents / 100.0

	if last_price is not None and last_price > 0:
		return last_price / 100.0

	return None


class PriceBucketBiasTest(StatisticalTest):
	"""Test whether settlement rates deviate from implied probability across price buckets.

	Generalization of the longshot bias analysis in price_efficiency.py.
	Works with any set of probability buckets, not just longshots.
	"""
	name: ClassVar[str] = "price_bucket_bias"

	def run(
		self,
		conn: sqlite3.Connection,
		series: str,
		params: dict,
		thresholds: dict,
	) -> TestResult:
		buckets: list[list[float]] = params.get("buckets", [[0.01, 0.30]])
		min_n: int = params.get("min_n_per_bucket", 30)
		min_clusters: int = thresholds.get("min_clusters", 2)
		fee_model: FeeModel = _resolve_fee_model(params.get("fee_model", "zero"))
		z_threshold: float = thresholds.get("clustered_z_stat", 3.0)
		min_fee_adj: float = thresholds.get("min_fee_adjusted_edge", 0.0)

		cursor = conn.cursor()

		# 1. Confirm the series has settled markets at all (graceful early exit).
		cursor.execute(
			"SELECT COUNT(*) FROM markets WHERE series_ticker = ? AND result IS NOT NULL",
			(series,),
		)
		if (cursor.fetchone() or [0])[0] == 0:
			return TestResult(
				verdict=INSUFFICIENT_DATA, z_stat=0.0, fee_adjusted_edge=0.0,
				detail={"reason": "no_settled_markets", "n": 0},
			)

		bucket_tuples = _normalize_buckets(buckets)

		# 2-4. Per bucket: trade-price calibration (one row per market that traded
		# in the band), clustered by close_date so effective N = #independent days.
		bucket_results: list[dict] = []
		any_bucket_met_min_n = False
		all_rows: list[tuple[float, bool, str | None]] = []
		cluster_floor_skipped: list[dict] = []

		for lo, hi in bucket_tuples:
			rows = _trade_price_cluster_rows(cursor, series, lo, hi)
			n_markets = len(rows)
			if n_markets < min_n:
				continue

			wins = sum(1 for _, won, _ in rows if won)
			mean_price = sum(p for p, _, _ in rows) / n_markets
			win_rate = wins / n_markets
			edge = win_rate - mean_price

			z_naive, p_naive = proportions_ztest(wins, n_markets, mean_price)
			z_clust, p_clust, n_clust = clustered_z(rows)
			# FIX A3: charge the fee on the edge MAGNITUDE. edge < 0 means the bucket
			# is overpriced (a short-side edge); the tradeable edge is |edge| - fee,
			# so a fee applied to the signed (negative) edge would wrongly push a real
			# short-side edge further negative and never grade EDGE_EXISTS. Keep the
			# SIGNED edge in the detail for direction.
			fee_adj = fee_adjusted_edge_curve(abs(edge), mean_price, fee_model)
			ci_lo, ci_hi = wilson_ci(wins, n_markets)

			bucket_entry = {
				"bucket_lo": lo, "bucket_hi": hi,
				"n_markets": n_markets, "n_clusters": n_clust,
				"mean_price": mean_price,
				"win_rate": win_rate,
				"edge": edge,
				"z": float(z_clust),
				"z_stat_naive": float(z_naive),
				"p": float(p_clust),
				"p_value_naive": float(p_naive),
				"fee_adj": fee_adj,
				"ci_lower": ci_lo, "ci_upper": ci_hi,
			}

			# FIX A1 min-cluster floor: a bucket is eligible for the verdict only if
			# it clears min_clusters independent days. Buckets below the floor are NOT
			# added to the evaluated results (they can't drive a verdict) but are noted
			# in the detail. Understating the cluster count is exactly what fabricates
			# edges under intraday correlation, so a thin-day bucket must not score.
			if n_clust < min_clusters:
				cluster_floor_skipped.append(bucket_entry)
				continue

			any_bucket_met_min_n = True
			bucket_results.append(bucket_entry)
			all_rows.extend(rows)

		verdict, driver, z_stat, fee_adj_result = _bucket_bonferroni_verdict(
			bucket_results, z_threshold, min_fee_adj, any_bucket_met_min_n,
		)

		if verdict == INSUFFICIENT_DATA:
			return TestResult(
				verdict=INSUFFICIENT_DATA, z_stat=0.0, fee_adjusted_edge=0.0,
				detail={
					"reason": "no_bucket_met_min_n", "buckets": bucket_results,
					"cluster_floor_skipped": cluster_floor_skipped,
				},
			)

		# Aggregate descriptors (back-compat detail keys); the VERDICT is per-bucket.
		total_n = len(all_rows)
		total_wins = sum(1 for _, won, _ in all_rows if won)
		total_implied = sum(p for p, _, _ in all_rows) / total_n
		driver_key = (
			(driver["bucket_lo"], driver["bucket_hi"]) if driver is not None else None
		)

		return TestResult(
			verdict=verdict,
			z_stat=z_stat,
			fee_adjusted_edge=fee_adj_result,
			detail={
				"n": total_n,
				"overall_implied": total_implied,
				"overall_win_rate": total_wins / total_n,
				"overall_edge": total_wins / total_n - total_implied,
				"z_threshold_bonferroni": _bonferroni_z_threshold(z_threshold, len(bucket_results)),
				"driver_bucket": driver,
				"driver_bucket_band": driver_key,
				"buckets": bucket_results,
				"cluster_floor_skipped": cluster_floor_skipped,
			},
		)


class LifecycleBiasTest(StatisticalTest):
	"""Test whether pricing behavior varies by market age (time since open)."""
	name: ClassVar[str] = "lifecycle_bias"

	def run(
		self,
		conn: sqlite3.Connection,
		series: str,
		params: dict,
		thresholds: dict,
	) -> TestResult:
		lifecycle_window_minutes: int = params.get("lifecycle_window_minutes", 30)
		buckets: list[list[float]] = params.get("buckets", [[0.40, 0.60]])
		min_n: int = params.get("min_n_per_bucket", 30)
		fee_model: FeeModel = _resolve_fee_model(params.get("fee_model", "zero"))
		z_threshold: float = thresholds.get("clustered_z_stat", 3.0)
		min_fee_adj: float = thresholds.get("min_fee_adjusted_edge", 0.0)

		cursor = conn.cursor()

		# 1. Query settled markets for the series with open_time and close_time
		cursor.execute(
			"SELECT ticker, result, last_price, open_time "
			"FROM markets WHERE series_ticker = ? AND result IS NOT NULL",
			(series,),
		)
		markets = cursor.fetchall()

		if not markets:
			return TestResult(
				verdict=INSUFFICIENT_DATA, z_stat=0.0, fee_adjusted_edge=0.0,
				detail={"reason": "no_settled_markets", "n": 0},
			)

		bucket_tuples = _normalize_buckets(buckets)

		# Per segment: early/late → per bucket → list of (implied, won, ticker)
		# cluster key = ticker (each market is one observation)
		early_bucket_data: dict[tuple[float, float], list[tuple[float, bool, Optional[str]]]] = {
			(lo, hi): [] for lo, hi in bucket_tuples
		}
		late_bucket_data: dict[tuple[float, float], list[tuple[float, bool, Optional[str]]]] = {
			(lo, hi): [] for lo, hi in bucket_tuples
		}

		# 2. For each market, query trades and split into early/late
		for row in markets:
			ticker, result, last_price, open_time = row[0], row[1], row[2], row[3]
			won = (result == "yes")

			if open_time is None:
				continue

			# Parse open_time as seconds since epoch using SQLite's strftime
			cursor.execute(
				"SELECT strftime('%s', ?)", (open_time,)
			)
			open_ts_row = cursor.fetchone()
			if open_ts_row is None or open_ts_row[0] is None:
				continue
			open_ts = float(open_ts_row[0])
			cutoff_ts = open_ts + lifecycle_window_minutes * 60.0

			# Fetch all trades for this market
			cursor.execute(
				"SELECT yes_price, count, strftime('%s', created_time) "
				"FROM trades WHERE ticker = ?",
				(ticker,),
			)
			trades = cursor.fetchall()

			early_price_sum = 0.0
			early_count = 0
			late_price_sum = 0.0
			late_count = 0

			for trade_row in trades:
				yes_price, count, created_ts_str = trade_row
				if created_ts_str is None:
					continue
				created_ts = float(created_ts_str)
				count = count or 1
				if created_ts <= cutoff_ts:
					early_price_sum += yes_price * count
					early_count += count
				else:
					late_price_sum += yes_price * count
					late_count += count

			# Compute implied probability per segment; fall back to last_price if needed
			def _implied_from_segment(price_sum: float, count: int) -> Optional[float]:
				if count > 0:
					return (price_sum / count) / 100.0
				if last_price is not None and last_price > 0:
					return last_price / 100.0
				return None

			early_implied = _implied_from_segment(early_price_sum, early_count)
			late_implied = _implied_from_segment(late_price_sum, late_count)

			# 3-4. Assign to bucket per segment
			for implied, bucket_store in (
				(early_implied, early_bucket_data),
				(late_implied, late_bucket_data),
			):
				if implied is None:
					continue
				for lo, hi in bucket_tuples:
					if lo <= implied < hi:
						bucket_store[(lo, hi)].append((implied, won, ticker))
						break

		# 5. Compare early vs late settlement deviations
		# Strategy: compute clustered z for early segment; if significantly more mispriced
		# than late, report lifecycle bias.
		# We compute the differential signal: (early_edge - late_edge) across markets.
		# We use a single "diff" row per market ticker that appears in both segments.

		# Collect per-bucket analysis
		bucket_results: list[dict] = []
		any_bucket_has_data = False
		all_diff_rows: list[tuple[float, bool, Optional[str]]] = []

		for lo, hi in bucket_tuples:
			early_rows = early_bucket_data[(lo, hi)]
			late_rows = late_bucket_data[(lo, hi)]

			if len(early_rows) < min_n or len(late_rows) < min_n:
				continue

			any_bucket_has_data = True

			# Early segment stats
			e_wins = sum(1 for _, won, _ in early_rows if won)
			e_n = len(early_rows)
			e_implied = sum(imp for imp, _, _ in early_rows) / e_n
			e_win_rate = e_wins / e_n
			e_edge = e_win_rate - e_implied

			# Late segment stats
			l_wins = sum(1 for _, won, _ in late_rows if won)
			l_n = len(late_rows)
			l_implied = sum(imp for imp, _, _ in late_rows) / l_n
			l_win_rate = l_wins / l_n
			l_edge = l_win_rate - l_implied

			e_z, e_p, e_nc = clustered_z(early_rows)
			l_z, l_p, l_nc = clustered_z(late_rows)

			bucket_results.append({
				"bucket_lo": lo, "bucket_hi": hi,
				"early_n": e_n, "early_n_clusters": e_nc,
				"early_implied": e_implied, "early_win_rate": e_win_rate, "early_edge": e_edge,
				"early_z_stat": float(e_z), "early_p_value": float(e_p),
				"late_n": l_n, "late_n_clusters": l_nc,
				"late_implied": l_implied, "late_win_rate": l_win_rate, "late_edge": l_edge,
				"late_z_stat": float(l_z), "late_p_value": float(l_p),
				"edge_differential": e_edge - l_edge,
			})

			# For overall signal: use early rows to measure lifecycle bias
			# The differential approach: use early segment rows only, weighted by the deviation
			# from what the late segment looks like. For simplicity, use early segment directly.
			all_diff_rows.extend(early_rows)

		if not any_bucket_has_data:
			return TestResult(
				verdict=INSUFFICIENT_DATA, z_stat=0.0, fee_adjusted_edge=0.0,
				detail={"reason": "no_bucket_met_min_n", "buckets": []},
			)

		# 6. Aggregate: cluster by market ticker over early segment rows
		total_n = len(all_diff_rows)
		total_wins = sum(1 for _, won, _ in all_diff_rows if won)
		total_implied = sum(imp for imp, _, _ in all_diff_rows) / total_n
		overall_edge = total_wins / total_n - total_implied

		overall_z_clust, overall_p_clust, overall_n_clust = clustered_z(all_diff_rows)
		overall_fee_adj = fee_adjusted_edge_curve(overall_edge, total_implied, fee_model)

		# 7. Verdict
		if abs(overall_z_clust) >= z_threshold and overall_fee_adj > min_fee_adj:
			verdict = EDGE_EXISTS
		elif abs(overall_z_clust) >= z_threshold and overall_fee_adj <= 0:
			verdict = EDGE_NOT_TRADEABLE
		else:
			verdict = NO_EDGE

		return TestResult(
			verdict=verdict,
			z_stat=float(overall_z_clust),
			fee_adjusted_edge=overall_fee_adj,
			detail={
				"n_early": total_n,
				"n_clusters": overall_n_clust,
				"overall_early_implied": total_implied,
				"overall_early_win_rate": total_wins / total_n,
				"overall_early_edge": overall_edge,
				"lifecycle_window_minutes": lifecycle_window_minutes,
				"buckets": bucket_results,
			},
		)


class VolumeMispricingTest(StatisticalTest):
	"""Test whether thin (low-volume) markets have wider mispricing than liquid ones.

	Splits settled markets into volume terciles and checks whether the low-volume
	tercile shows significantly more mispricing than the medium/high terciles.
	"""
	name: ClassVar[str] = "volume_mispricing"

	def run(
		self,
		conn: sqlite3.Connection,
		series: str,
		params: dict,
		thresholds: dict,
	) -> TestResult:
		buckets: list[list[float]] = params.get("buckets", [[0.40, 0.60]])
		min_n: int = params.get("min_n_per_bucket", 30)
		min_clusters: int = thresholds.get("min_clusters", 2)
		fee_model: FeeModel = _resolve_fee_model(params.get("fee_model", "zero"))
		z_threshold: float = thresholds.get("clustered_z_stat", 3.0)
		min_fee_adj: float = thresholds.get("min_fee_adjusted_edge", 0.0)

		cursor = conn.cursor()

		# 1. Query all settled markets for the series with volume data
		cursor.execute(
			"SELECT ticker, result, last_price, volume, close_time "
			"FROM markets WHERE series_ticker = ? AND result IS NOT NULL",
			(series,),
		)
		markets = cursor.fetchall()

		if not markets:
			return TestResult(
				verdict=INSUFFICIENT_DATA, z_stat=0.0, fee_adjusted_edge=0.0,
				detail={"reason": "no_settled_markets", "n": 0},
			)

		# 2. Split into volume terciles
		volumes = [row[3] or 0 for row in markets]
		sorted_volumes = sorted(volumes)
		n_total = len(sorted_volumes)
		t1 = sorted_volumes[n_total // 3]      # upper bound of low tercile
		t2 = sorted_volumes[2 * n_total // 3]  # upper bound of medium tercile

		bucket_tuples = _normalize_buckets(buckets)

		# Split tickers into volume terciles (membership only — the per-bucket
		# implied price now comes from trade-price calibration, not lifetime VWAP).
		low_tickers: set[str] = set()
		med_tickers: set[str] = set()
		high_tickers: set[str] = set()
		for row in markets:
			ticker, vol = row[0], (row[3] or 0)
			if vol <= t1:
				low_tickers.add(ticker)
			elif vol <= t2:
				med_tickers.add(ticker)
			else:
				high_tickers.add(ticker)

		# 3. Analyze the LOW-volume tercile per bucket via trade-price calibration.
		def _tercile_summary(rows: list[tuple[float, bool, str | None]]) -> dict:
			if not rows:
				return {"n_markets": 0}
			wins = sum(1 for _, won, _ in rows if won)
			n = len(rows)
			mean_price = sum(p for p, _, _ in rows) / n
			z, p, nc = clustered_z(rows)
			return {
				"n_markets": n, "n_clusters": nc,
				"mean_price": mean_price,
				"win_rate": wins / n,
				"edge": wins / n - mean_price,
				"z_stat_clustered": float(z),
				"p_value_clustered": float(p),
			}

		bucket_results: list[dict] = []
		any_bucket_met_min_n = False
		all_low_rows: list[tuple[float, bool, str | None]] = []
		cluster_floor_skipped: list[dict] = []

		for lo, hi in bucket_tuples:
			low_rows = _trade_price_cluster_rows(cursor, series, lo, hi, ticker_filter=low_tickers)
			med_summary = _tercile_summary(
				_trade_price_cluster_rows(cursor, series, lo, hi, ticker_filter=med_tickers)
			)
			hi_rows = _trade_price_cluster_rows(cursor, series, lo, hi, ticker_filter=high_tickers)
			hi_summary = _tercile_summary(hi_rows)

			if len(low_rows) < min_n:
				continue

			lv_wins = sum(1 for _, won, _ in low_rows if won)
			lv_n = len(low_rows)
			lv_mean_price = sum(p for p, _, _ in low_rows) / lv_n
			lv_win_rate = lv_wins / lv_n
			lv_edge = lv_win_rate - lv_mean_price

			lv_z, lv_p, lv_nc = clustered_z(low_rows)
			lv_z_naive, lv_p_naive = proportions_ztest(lv_wins, lv_n, lv_mean_price)
			lv_ci_lo, lv_ci_hi = wilson_ci(lv_wins, lv_n)
			# FIX A3: charge the fee on the edge MAGNITUDE (see PriceBucketBiasTest).
			lv_fee_adj = fee_adjusted_edge_curve(abs(lv_edge), lv_mean_price, fee_model)

			bucket_entry = {
				"bucket_lo": lo, "bucket_hi": hi,
				# Verdict keys (consumed by _bucket_bonferroni_verdict):
				"z": float(lv_z),
				"fee_adj": lv_fee_adj,
				"n_markets": lv_n,
				"n_clusters": lv_nc,
				"mean_price": lv_mean_price,
				"win_rate": lv_win_rate,
				"edge": lv_edge,
				"p": float(lv_p),
				"ci_lower": lv_ci_lo,
				"ci_upper": lv_ci_hi,
				# Detail block:
				"low_volume": {
					"n_markets": lv_n, "n_clusters": lv_nc,
					"mean_price": lv_mean_price,
					"win_rate": lv_win_rate,
					"edge": lv_edge,
					"z_stat_naive": float(lv_z_naive),
					"z_stat_clustered": float(lv_z),
					"p_value_naive": float(lv_p_naive),
					"p_value_clustered": float(lv_p),
					"fee_adjusted_edge": lv_fee_adj,
					"ci_lower": lv_ci_lo,
					"ci_upper": lv_ci_hi,
				},
				"medium_volume": med_summary,
				"high_volume": hi_summary,
				"edge_differential_low_vs_high": (
					lv_edge - hi_summary.get("edge", 0.0) if hi_rows else None
				),
			}

			# FIX A1 min-cluster floor: only buckets clearing min_clusters independent
			# days are eligible for the verdict (see PriceBucketBiasTest).
			if lv_nc < min_clusters:
				cluster_floor_skipped.append(bucket_entry)
				continue

			any_bucket_met_min_n = True
			bucket_results.append(bucket_entry)
			all_low_rows.extend(low_rows)

		verdict, driver, z_stat, fee_adj_result = _bucket_bonferroni_verdict(
			bucket_results, z_threshold, min_fee_adj, any_bucket_met_min_n,
		)

		if verdict == INSUFFICIENT_DATA:
			return TestResult(
				verdict=INSUFFICIENT_DATA, z_stat=0.0, fee_adjusted_edge=0.0,
				detail={
					"reason": "no_bucket_met_min_n", "buckets": bucket_results,
					"tercile_bounds": (t1, t2),
					"cluster_floor_skipped": cluster_floor_skipped,
				},
			)

		# Aggregate descriptors (back-compat detail keys); the VERDICT is per-bucket.
		total_n = len(all_low_rows)
		total_wins = sum(1 for _, won, _ in all_low_rows if won)
		total_implied = sum(p for p, _, _ in all_low_rows) / total_n
		driver_key = (
			(driver["bucket_lo"], driver["bucket_hi"]) if driver is not None else None
		)

		return TestResult(
			verdict=verdict,
			z_stat=z_stat,
			fee_adjusted_edge=fee_adj_result,
			detail={
				"n_low_volume": total_n,
				"overall_implied": total_implied,
				"overall_win_rate": total_wins / total_n,
				"overall_edge": total_wins / total_n - total_implied,
				"z_threshold_bonferroni": _bonferroni_z_threshold(z_threshold, len(bucket_results)),
				"driver_bucket": driver,
				"driver_bucket_band": driver_key,
				"tercile_bounds": (t1, t2),
				"buckets": bucket_results,
				"cluster_floor_skipped": cluster_floor_skipped,
			},
		)


class MomentumAlignmentTest(StatisticalTest):
	"""Test whether contract prices lag or lead external spot price movements.

	Uses OHLC data from an external SQLite DB to classify each trade into a
	momentum regime (up/down/flat), then checks whether contracts in "up"
	momentum are systematically underpriced (actual win rate > implied) and
	contracts in "down" momentum are overpriced.

	Requires params["ohlc_config"] = {"db_path": str, "table": str, "asset": str}.
	Falls back to INSUFFICIENT_DATA gracefully if OHLC data is unavailable.
	"""
	name: ClassVar[str] = "momentum_alignment"

	def run(
		self,
		conn: sqlite3.Connection,
		series: str,
		params: dict,
		thresholds: dict,
	) -> TestResult:
		ohlc_config: Optional[dict] = params.get("ohlc_config")
		lookback: int = params.get("lookback_candles", 5)
		buckets: list[list[float]] = params.get("buckets", [[0.30, 0.70]])
		min_n: int = params.get("min_n_per_bucket", 30)
		fee_model: FeeModel = _resolve_fee_model(params.get("fee_model", "zero"))
		z_threshold: float = thresholds.get("clustered_z_stat", 3.0)
		min_fee_adj: float = thresholds.get("min_fee_adjusted_edge", 0.0)

		# 1. Validate ohlc_config and open OHLC connection
		if not ohlc_config:
			return TestResult(
				verdict=INSUFFICIENT_DATA, z_stat=0.0, fee_adjusted_edge=0.0,
				detail={"reason": "no_ohlc_config"},
			)

		ohlc_db_path: str = ohlc_config.get("db_path", "")
		ohlc_table: str = ohlc_config.get("table", "")

		import os
		# #2: data_source_config.ohlc_for_series hands back a BARE db_file
		# ("kalshi-altcrypto.db") but the actual file lives under data/. If the path
		# doesn't exist as given, fall back to data/<path> (the convention in
		# data_source_resolver.py). Keep this local to the test (smallest change).
		if not os.path.exists(ohlc_db_path):
			data_dir_candidate = os.path.join("data", ohlc_db_path)
			if os.path.exists(data_dir_candidate):
				ohlc_db_path = data_dir_candidate
			else:
				# Still missing — stay graceful, reporting the RESOLVED candidate path.
				return TestResult(
					verdict=INSUFFICIENT_DATA, z_stat=0.0, fee_adjusted_edge=0.0,
					detail={"reason": "ohlc_db_not_found", "db_path": data_dir_candidate},
				)

		try:
			ohlc_conn = sqlite3.connect(ohlc_db_path)
			ohlc_cursor = ohlc_conn.cursor()
			# Check the table exists
			ohlc_cursor.execute(
				"SELECT name FROM sqlite_master WHERE type='table' AND name=?",
				(ohlc_table,),
			)
			if ohlc_cursor.fetchone() is None:
				ohlc_conn.close()
				return TestResult(
					verdict=INSUFFICIENT_DATA, z_stat=0.0, fee_adjusted_edge=0.0,
					detail={"reason": "ohlc_table_not_found", "table": ohlc_table},
				)
		except Exception as exc:
			return TestResult(
				verdict=INSUFFICIENT_DATA, z_stat=0.0, fee_adjusted_edge=0.0,
				detail={"reason": "ohlc_open_error", "error": str(exc)},
			)

		try:
			return self._run_with_ohlc(
				conn=conn,
				ohlc_conn=ohlc_conn,
				ohlc_table=ohlc_table,
				series=series,
				lookback=lookback,
				buckets=buckets,
				min_n=min_n,
				fee_model=fee_model,
				z_threshold=z_threshold,
				min_fee_adj=min_fee_adj,
			)
		finally:
			ohlc_conn.close()

	def _run_with_ohlc(
		self,
		conn: sqlite3.Connection,
		ohlc_conn: sqlite3.Connection,
		ohlc_table: str,
		series: str,
		lookback: int,
		buckets: list[list[float]],
		min_n: int,
		fee_model: FeeModel,
		z_threshold: float,
		min_fee_adj: float,
	) -> TestResult:
		cursor = conn.cursor()
		ohlc_cursor = ohlc_conn.cursor()

		# 3. Query settled markets for the series with trades
		cursor.execute(
			"SELECT ticker, result, last_price, close_time "
			"FROM markets WHERE series_ticker = ? AND result IS NOT NULL",
			(series,),
		)
		markets = cursor.fetchall()

		if not markets:
			return TestResult(
				verdict=INSUFFICIENT_DATA, z_stat=0.0, fee_adjusted_edge=0.0,
				detail={"reason": "no_settled_markets", "n": 0},
			)

		bucket_tuples = [(lo, hi) for lo, hi in buckets]

		# Per regime → per bucket → list of (implied, won, close_date)
		regime_bucket_data: dict[str, dict[tuple[float, float], list[tuple[float, bool, Optional[str]]]]] = {
			"up":   {(lo, hi): [] for lo, hi in bucket_tuples},
			"down": {(lo, hi): [] for lo, hi in bucket_tuples},
			"flat": {(lo, hi): [] for lo, hi in bucket_tuples},
		}

		# 4-6. For each market, fetch trades, look up OHLC, classify regime
		for row in markets:
			ticker, result, last_price, close_time = row[0], row[1], row[2], row[3]
			won = (result == "yes")
			close_date = close_time[:10] if close_time else None

			# Fetch trades for this market
			cursor.execute(
				"SELECT yes_price, count, created_time FROM trades WHERE ticker = ?",
				(ticker,),
			)
			trade_rows = cursor.fetchall()
			if not trade_rows:
				continue

			# Compute VWAP from trades
			implied = _compute_vwap(cursor, ticker, last_price)
			if implied is None:
				continue

			# Use the earliest trade time as the reference point for OHLC lookup
			# (we want the candle *before* the first trade)
			trade_times = [t[2] for t in trade_rows if t[2] is not None]
			if not trade_times:
				continue
			ref_time = min(trade_times)

			# 4. Look up the most recent OHLC candle before the trade time
			ohlc_cursor.execute(
				f"SELECT timestamp, close FROM \"{ohlc_table}\" "
				"WHERE timestamp < ? ORDER BY timestamp DESC LIMIT 1",
				(ref_time,),
			)
			current_candle = ohlc_cursor.fetchone()
			if current_candle is None:
				continue
			current_close = current_candle[1]

			# Look up candle N steps back from the current candle
			ohlc_cursor.execute(
				f"SELECT timestamp, close FROM \"{ohlc_table}\" "
				"WHERE timestamp < ? ORDER BY timestamp DESC LIMIT 1 OFFSET ?",
				(ref_time, lookback),
			)
			lookback_candle = ohlc_cursor.fetchone()
			if lookback_candle is None:
				continue
			lookback_close = lookback_candle[1]

			# 5. Compute momentum and classify regime
			if lookback_close == 0:
				continue
			momentum = (current_close - lookback_close) / lookback_close

			flat_threshold = 0.001  # 0.1% threshold for "flat"
			if momentum > flat_threshold:
				regime = "up"
			elif momentum < -flat_threshold:
				regime = "down"
			else:
				regime = "flat"

			# 7. Assign to bucket by implied price
			for lo, hi in bucket_tuples:
				if lo <= implied < hi:
					regime_bucket_data[regime][(lo, hi)].append((implied, won, close_date))
					break

		# 8. Test for underreaction: in "up" momentum, win_rate > implied;
		#    in "down" momentum, win_rate < implied.
		# We focus on "up" + "down" regimes combined, signed by hypothesis direction.
		# Build analysis rows: for "up" regime keep as-is (edge = win_rate - implied > 0 is the signal)
		# For "down" regime, invert (edge = implied - win_rate > 0 is the signal)
		# We compute the aggregate signal across both momentum regimes.

		bucket_results: list[dict] = []
		any_bucket_has_data = False
		all_signal_rows: list[tuple[float, bool, Optional[str]]] = []

		for lo, hi in bucket_tuples:
			up_rows = regime_bucket_data["up"][(lo, hi)]
			down_rows = regime_bucket_data["down"][(lo, hi)]
			flat_rows = regime_bucket_data["flat"][(lo, hi)]

			# Need enough data in at least one directional regime
			if len(up_rows) < min_n and len(down_rows) < min_n:
				continue

			any_bucket_has_data = True

			def _regime_summary(rows: list) -> dict:
				if not rows:
					return {"n": 0}
				wins = sum(1 for _, won, _ in rows if won)
				n = len(rows)
				imp = sum(i for i, _, _ in rows) / n
				z, p, nc = clustered_z(rows)
				return {
					"n": n, "n_clusters": nc,
					"implied_prob": imp,
					"win_rate": wins / n,
					"edge": wins / n - imp,
					"z_stat_clustered": float(z),
					"p_value_clustered": float(p),
				}

			bucket_results.append({
				"bucket_lo": lo, "bucket_hi": hi,
				"up_regime": _regime_summary(up_rows),
				"down_regime": _regime_summary(down_rows),
				"flat_regime": _regime_summary(flat_rows),
			})

			# Aggregate signal: use "up" rows directly (up momentum → contracts underpriced → win_rate > implied)
			if len(up_rows) >= min_n:
				all_signal_rows.extend(up_rows)

		if not any_bucket_has_data:
			return TestResult(
				verdict=INSUFFICIENT_DATA, z_stat=0.0, fee_adjusted_edge=0.0,
				detail={"reason": "no_bucket_met_min_n", "buckets": []},
			)

		# 9-10. Aggregate statistics
		total_n = len(all_signal_rows)
		if total_n == 0:
			return TestResult(
				verdict=INSUFFICIENT_DATA, z_stat=0.0, fee_adjusted_edge=0.0,
				detail={"reason": "no_up_regime_rows", "buckets": bucket_results},
			)

		total_wins = sum(1 for _, won, _ in all_signal_rows if won)
		total_implied = sum(imp for imp, _, _ in all_signal_rows) / total_n
		overall_edge = total_wins / total_n - total_implied

		overall_z_clust, overall_p_clust, overall_n_clust = clustered_z(all_signal_rows)
		overall_fee_adj = fee_adjusted_edge_curve(overall_edge, total_implied, fee_model)

		# Verdict
		if abs(overall_z_clust) >= z_threshold and overall_fee_adj > min_fee_adj:
			verdict = EDGE_EXISTS
		elif abs(overall_z_clust) >= z_threshold and overall_fee_adj <= 0:
			verdict = EDGE_NOT_TRADEABLE
		else:
			verdict = NO_EDGE

		return TestResult(
			verdict=verdict,
			z_stat=float(overall_z_clust),
			fee_adjusted_edge=overall_fee_adj,
			detail={
				"n": total_n,
				"n_clusters": overall_n_clust,
				"overall_implied": total_implied,
				"overall_win_rate": total_wins / total_n,
				"overall_edge": overall_edge,
				"lookback_candles": lookback,
				"buckets": bucket_results,
			},
		)


class TestRunner:
	"""Dispatches hypothesis configs to the appropriate StatisticalTest."""

	# Tell pytest this is a domain class, not a test class.
	__test__ = False

	def __init__(self) -> None:
		self.test_types: dict[str, StatisticalTest] = {}
		self._register_defaults()

	def _register_defaults(self) -> None:
		"""Register built-in test types. Called during __init__."""
		self.register("price_bucket_bias", PriceBucketBiasTest())
		self.register("lifecycle_bias", LifecycleBiasTest())
		self.register("volume_mispricing", VolumeMispricingTest())
		self.register("momentum_alignment", MomentumAlignmentTest())

	def register(self, name: str, test: StatisticalTest) -> None:
		self.test_types[name] = test

	def run(
		self,
		test_type: str,
		conn: sqlite3.Connection,
		series: str,
		params: dict,
		thresholds: dict,
	) -> TestResult:
		if test_type not in self.test_types:
			raise ValueError(f"Unknown test type: {test_type}. Available: {list(self.test_types.keys())}")
		return self.test_types[test_type].run(conn, series, params, thresholds)
