"""Tests for shared statistical utilities."""

import pytest
from edge_catcher.research.stats_utils import (
	proportions_ztest, clustered_z, wilson_ci, fee_adjusted_edge,
)


class TestProportionsZtest:
	def test_fair_coin(self):
		"""50 wins out of 100 at p0=0.5 → z ≈ 0."""
		z, p = proportions_ztest(50, 100, 0.5)
		assert abs(z) < 0.5
		assert p > 0.5

	def test_significant_deviation(self):
		"""30 wins out of 100 at p0=0.5 → significant negative z."""
		z, p = proportions_ztest(30, 100, 0.5)
		assert z < -3.0
		assert p < 0.01

	def test_zero_n_returns_zero(self):
		z, p = proportions_ztest(0, 0, 0.5)
		assert z == 0.0
		assert p == 1.0


class TestClusteredZ:
	def test_no_excess_returns_zero(self):
		"""All clusters match implied → z ≈ 0."""
		rows = [
			(0.5, True, "2026-01-01"), (0.5, False, "2026-01-01"),
			(0.5, True, "2026-01-02"), (0.5, False, "2026-01-02"),
		]
		z, p, k = clustered_z(rows)
		assert abs(z) < 1.0
		assert k == 2

	def test_single_cluster_returns_zero(self):
		"""Need ≥ 2 clusters for variance estimate."""
		rows = [(0.5, True, "2026-01-01"), (0.5, False, "2026-01-01")]
		z, p, k = clustered_z(rows)
		assert z == 0.0
		assert k == 1

	def test_strong_deviation(self):
		"""All clusters show wins well below implied → negative z."""
		rows = []
		for day in range(20):
			date = f"2026-01-{day+1:02d}"
			for _ in range(10):
				rows.append((0.5, False, date))  # 0% win rate, 50% implied
		z, p, k = clustered_z(rows)
		assert z < -3.0
		assert k == 20


class TestWilsonCI:
	def test_basic_interval(self):
		lo, hi = wilson_ci(50, 100)
		assert 0.39 < lo < 0.42
		assert 0.58 < hi < 0.61

	def test_zero_n(self):
		lo, hi = wilson_ci(0, 0)
		assert lo == 0.0
		assert hi == 0.0

	def test_all_wins(self):
		lo, hi = wilson_ci(100, 100)
		assert lo > 0.95
		assert hi == 1.0


class TestFeeAdjustedEdge:
	def test_positive_edge_survives(self):
		"""5% raw edge, 1.75% maker fee at 10% implied → edge - 0.0175 * 0.9."""
		result = fee_adjusted_edge(0.05, 0.10, 0.0175)
		assert result == pytest.approx(0.05 - 0.0175 * 0.90, abs=0.001)

	def test_edge_killed_by_fees(self):
		"""1% raw edge, 1.75% maker fee at 5% implied → negative."""
		result = fee_adjusted_edge(0.01, 0.05, 0.0175)
		assert result < 0

	def test_zero_fee(self):
		result = fee_adjusted_edge(0.05, 0.50, 0.0)
		assert result == 0.05
