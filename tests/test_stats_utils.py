"""Tests for shared statistical utilities."""

import math

import pytest
from edge_catcher.adapters.kalshi.fees import INDEX_FEE, STANDARD_FEE
from edge_catcher.fees import ZERO_FEE
from edge_catcher.research.stats_utils import (
	proportions_ztest, clustered_z, wilson_ci, fee_adjusted_edge,
	fee_adjusted_edge_curve,
)


def _real_fee_dollars(rate: float, p: float) -> float:
	"""The exchange's real per-contract fee: ceil(rate * p * (1-p) * 100) cents → $."""
	return math.ceil(rate * p * (1 - p) * 100) / 100.0


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


class TestFeeAdjustedEdgeCurve:
	"""The real-fee gate: subtracts the exchange's actual per-contract fee curve."""

	@pytest.mark.parametrize("implied", [0.05, 0.10, 0.25, 0.50, 0.75, 0.90])
	def test_matches_kalshi_standard_curve(self, implied):
		"""Curve fee equals the exchange's ceil(0.07*p*(1-p)*100) cents/contract."""
		raw = 0.10
		result = fee_adjusted_edge_curve(raw, implied, STANDARD_FEE)
		assert result == pytest.approx(raw - _real_fee_dollars(0.07, implied))

	def test_index_curve_uses_index_rate(self):
		"""The index fee model applies the 0.035 rate, not standard's 0.07."""
		raw = 0.10
		result = fee_adjusted_edge_curve(raw, 0.50, INDEX_FEE)
		assert result == pytest.approx(raw - _real_fee_dollars(0.035, 0.50))

	def test_midprice_fee_exceeds_flat_approximation(self):
		"""At p≈0.5 the real curve charges materially more than the old flat rate.

		Flat: 0.0175 * (1 - 0.5) = 0.00875. Curve: ceil(0.07*0.5*0.5*100) = 2c = 0.02.
		The flat approximation understated mid-priced fees — a false-positive risk.
		"""
		raw = 0.10
		flat_fee = raw - fee_adjusted_edge(raw, 0.50, 0.0175)
		curve_fee = raw - fee_adjusted_edge_curve(raw, 0.50, STANDARD_FEE)
		assert curve_fee == pytest.approx(0.02)
		assert curve_fee > flat_fee
		assert curve_fee / flat_fee > 1.5

	def test_zero_fee_model_subtracts_nothing(self):
		assert fee_adjusted_edge_curve(0.05, 0.50, ZERO_FEE) == 0.05
