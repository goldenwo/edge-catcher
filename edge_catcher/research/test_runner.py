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


class TestRunner:
	"""Dispatches hypothesis configs to the appropriate StatisticalTest."""

	def __init__(self) -> None:
		self.test_types: dict[str, StatisticalTest] = {}
		self._register_defaults()

	def _register_defaults(self) -> None:
		"""Register built-in test types. Called during __init__."""
		self.register("price_bucket_bias", PriceBucketBiasTest())

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
