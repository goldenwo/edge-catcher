"""Generic statistical test runner for hypothesis validation."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import ClassVar, Optional

from edge_catcher.research.stats_utils import (
	clustered_z,
	fee_adjusted_edge,
	proportions_ztest,
	wilson_ci,
)

# Verdict constants
EDGE_EXISTS = "EDGE_EXISTS"
NO_EDGE = "NO_EDGE"
INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
EDGE_NOT_TRADEABLE = "EDGE_NOT_TRADEABLE"

# Fee model mapping: name → maker fee rate
FEE_MODELS: dict[str, float] = {
	"zero": 0.0,
	"standard": 0.0175,
	"index": 0.00875,
}


@dataclass
class TestResult:
	"""Result of a statistical hypothesis test."""
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
		maker_fee: float = FEE_MODELS.get(params.get("fee_model", "zero"), 0.0)
		z_threshold: float = thresholds.get("clustered_z_stat", 3.0)
		min_fee_adj: float = thresholds.get("min_fee_adjusted_edge", 0.0)

		cursor = conn.cursor()

		# 1. Query all settled markets for the series
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

		# 2-3. Compute VWAP and assign to buckets
		bucket_tuples = [(lo, hi) for lo, hi in buckets]
		bucket_data: dict[tuple[float, float], list[tuple[float, bool, Optional[str]]]] = {
			(lo, hi): [] for lo, hi in bucket_tuples
		}

		for row in markets:
			ticker, result, last_price, close_time = row[0], row[1], row[2], row[3]
			implied = _compute_vwap(cursor, ticker, last_price)
			if implied is None:
				continue

			# Find matching bucket
			for lo, hi in bucket_tuples:
				if lo <= implied < hi:
					won = (result == "yes")
					close_date = close_time[:10] if close_time else None
					bucket_data[(lo, hi)].append((implied, won, close_date))
					break

		# 4. Per-bucket statistics
		bucket_results: list[dict] = []
		any_bucket_has_data = False
		worst_z: float = 0.0
		worst_fee_adj: float = 0.0
		all_rows: list[tuple[float, bool, Optional[str]]] = []

		for (lo, hi) in bucket_tuples:
			rows = bucket_data[(lo, hi)]
			n = len(rows)

			if n < min_n:
				continue

			any_bucket_has_data = True
			wins = sum(1 for _, won, _ in rows if won)
			implied_vals = [imp for imp, _, _ in rows]
			mean_implied = sum(implied_vals) / len(implied_vals)
			actual_win_rate = wins / n
			edge = actual_win_rate - mean_implied

			z_naive, p_naive = proportions_ztest(wins, n, mean_implied)
			z_clust, p_clust, n_clust = clustered_z(rows)
			fee_adj = fee_adjusted_edge(edge, mean_implied, maker_fee)
			ci_lo, ci_hi = wilson_ci(wins, n)

			bucket_results.append({
				"bucket_lo": lo, "bucket_hi": hi,
				"n": n, "n_clusters": n_clust,
				"implied_prob": mean_implied,
				"actual_win_rate": actual_win_rate,
				"edge": edge,
				"z_stat_naive": float(z_naive),
				"z_stat_clustered": float(z_clust),
				"p_value_naive": float(p_naive),
				"p_value_clustered": float(p_clust),
				"fee_adjusted_edge": fee_adj,
				"ci_lower": ci_lo, "ci_upper": ci_hi,
			})

			all_rows.extend(rows)

		if not any_bucket_has_data:
			return TestResult(
				verdict=INSUFFICIENT_DATA, z_stat=0.0, fee_adjusted_edge=0.0,
				detail={"reason": "no_bucket_met_min_n", "buckets": []},
			)

		# 5. Aggregate across buckets using all qualifying rows
		total_n = len(all_rows)
		total_wins = sum(1 for _, won, _ in all_rows if won)
		total_implied = sum(imp for imp, _, _ in all_rows) / total_n
		overall_edge = total_wins / total_n - total_implied

		overall_z_clust, overall_p_clust, overall_n_clust = clustered_z(all_rows)
		overall_fee_adj = fee_adjusted_edge(overall_edge, total_implied, maker_fee)

		# 6. Verdict logic
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
				"buckets": bucket_results,
			},
		)


class LifecycleBiasTest(StatisticalTest):
	"""Test whether mispricing varies by market age (time since open).

	Detects 'debut fade' patterns where newly listed markets are systematically
	mispriced in their early trading period vs later.
	"""
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
		maker_fee: float = FEE_MODELS.get(params.get("fee_model", "zero"), 0.0)
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

		bucket_tuples = [(lo, hi) for lo, hi in buckets]

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
		overall_fee_adj = fee_adjusted_edge(overall_edge, total_implied, maker_fee)

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


class TestRunner:
	"""Dispatches hypothesis configs to the appropriate StatisticalTest."""

	def __init__(self) -> None:
		self.test_types: dict[str, StatisticalTest] = {}
		self._register_defaults()

	def _register_defaults(self) -> None:
		"""Register built-in test types. Called during __init__."""
		self.register("price_bucket_bias", PriceBucketBiasTest())
		self.register("lifecycle_bias", LifecycleBiasTest())

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
