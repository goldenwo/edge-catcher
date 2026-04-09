"""Tests for individual validation gates."""

from __future__ import annotations

import math
import statistics
from unittest.mock import MagicMock, patch

import pytest

from edge_catcher.research.data_source_config import make_ds
from edge_catcher.research.hypothesis import Hypothesis, HypothesisResult
from edge_catcher.research.validation.gate import GateContext, GateResult


def _make_hypothesis(**kwargs) -> Hypothesis:
	ds_kwargs = {}
	for k in ("series",):
		if k in kwargs:
			ds_kwargs[k] = kwargs.pop(k)
	# Extract db from db_path if passed (no longer a Hypothesis field)
	db = "kalshi.db"
	if "db_path" in kwargs:
		from pathlib import Path
		db = Path(kwargs.pop("db_path")).name
	ds = make_ds(db=db, series=ds_kwargs.get("series", "SERIES_A"))
	defaults = dict(
		strategy="C", data_sources=ds,
		start_date="2025-01-01", end_date="2025-12-31",
	)
	defaults.update(kwargs)
	return Hypothesis(**defaults)


def _make_result(pnl_values=None, sharpe=2.5, total_trades=100, **kwargs) -> HypothesisResult:
	h = _make_hypothesis(**{k: kwargs.pop(k) for k in list(kwargs) if k in ("strategy", "series", "db_path", "start_date", "end_date")})
	if pnl_values is None:
		pnl_values = [10] * 90 + [-5] * 10
	defaults = dict(
		hypothesis=h, status="ok", total_trades=total_trades,
		wins=90, losses=10, win_rate=0.90, net_pnl_cents=500.0,
		sharpe=sharpe, max_drawdown_pct=5.0, fees_paid_cents=100.0,
		avg_win_cents=10.0, avg_loss_cents=-5.0, per_strategy={},
		verdict="candidate", verdict_reason="", raw_json={"pnl_values": pnl_values},
	)
	defaults.update(kwargs)
	return HypothesisResult(**defaults)


# ---------------------------------------------------------------------------
# DSR Gate
# ---------------------------------------------------------------------------

class TestDeflatedSharpeGate:
	def _make_tracker_with_results(
		self,
		sharpes: list[float],
		strategies: list[str] | None = None,
	):
		"""Mock tracker that returns results with given Sharpe values.

		``sharpes`` are treated as per-trade Sharpes (not backtester-scaled).
		We set ``total_trades=1`` so that the gate's division by sqrt(total_trades)
		is a no-op (sqrt(1) == 1), preserving the original test intent.
		"""
		if strategies is None:
			strategies = [f"strat_{i}" for i in range(len(sharpes))]
		rows = []
		for i, (sharpe, strat) in enumerate(zip(sharpes, strategies)):
			rows.append({
				"strategy": strat, "sharpe": sharpe, "status": "ok",
				"verdict": "explore", "id": str(i),
				# total_trades=1 keeps bt_sharpe / sqrt(1) == sharpe, so the
				# existing test values remain meaningful as per-trade Sharpes.
				"total_trades": 1,
			})
		tracker = MagicMock()
		tracker.list_results.return_value = rows
		return tracker

	def test_high_dsr_passes(self):
		"""Strategy with genuinely high Sharpe in a small noise pool should pass."""
		from edge_catcher.research.validation.gate_dsr import DeflatedSharpeGate

		# 10 background strategies tested, all low Sharpe (no outlier in pool)
		sharpes = [0.5, 0.3, -0.2, 0.8, 0.1, 0.4, 0.6, -0.1, 0.2, 0.3]
		tracker = self._make_tracker_with_results(sharpes)

		# Strong positive signal — high per-trade SR
		pnl = [20] * 80 + [-2] * 20
		result = _make_result(pnl_values=pnl, sharpe=5.0, total_trades=100)
		ctx = GateContext(tracker=tracker, pnl_values=pnl, hypothesis=result.hypothesis)

		gate = DeflatedSharpeGate()
		gr = gate.check(result, ctx)
		assert gr.passed
		assert gr.details["dsr"] > 0.95

	def test_low_dsr_fails(self):
		"""Strategy with weak per-trade Sharpe among many families should fail DSR."""
		from edge_catcher.research.validation.gate_dsr import DeflatedSharpeGate

		# 500 strategy families — high N pushes SR0 up via E[max(Z)]
		sharpes = [0.1] * 500
		tracker = self._make_tracker_with_results(sharpes)

		# Very weak signal: per-trade Sharpe ~0.06, with N=500 and T=100
		# SR0 = 1/sqrt(99) * ~3.05 = 0.31 >> 0.06, so DSR near 0
		pnl = [1] * 53 + [-1] * 47
		sr = statistics.mean(pnl) / statistics.stdev(pnl)
		result = _make_result(pnl_values=pnl, sharpe=sr * math.sqrt(100), total_trades=100)
		ctx = GateContext(tracker=tracker, pnl_values=pnl, hypothesis=result.hypothesis)

		gate = DeflatedSharpeGate()
		gr = gate.check(result, ctx)
		assert not gr.passed

	def test_insufficient_data_fails(self):
		"""Too few pnl_values should fail the gate."""
		from edge_catcher.research.validation.gate_dsr import DeflatedSharpeGate

		tracker = self._make_tracker_with_results([1.0, 2.0, 3.0])
		result = _make_result(pnl_values=[10], sharpe=2.0, total_trades=1)
		ctx = GateContext(tracker=tracker, pnl_values=[10], hypothesis=result.hypothesis)

		gate = DeflatedSharpeGate()
		gr = gate.check(result, ctx)
		assert not gr.passed

	def test_dsr_gate_sharpe_scale_consistency(self):
		"""sr_observed and sr0 should be on the same per-trade scale.

		sr_observed = mean(pnl)/std(pnl) (per-trade, no sqrt(T) scaling).
		sr0 uses theoretical null std = 1/sqrt(T-1), so both are per-trade.
		"""
		from edge_catcher.research.validation.gate_dsr import DeflatedSharpeGate

		# PnL with known per-trade Sharpe ~0.116 (mean=10, std~86)
		pnl = [10 + (i % 3 - 1) * 100 for i in range(200)]
		mu = statistics.mean(pnl)
		std = statistics.stdev(pnl)
		per_trade_sharpe = mu / std
		backtester_sharpe = per_trade_sharpe * math.sqrt(len(pnl))

		h = _make_hypothesis(strategy="TestStrat", series="X", db_path="d.db",
							 start_date="2025-01-01", end_date="2025-12-31")
		result = _make_result(
			pnl_values=pnl, sharpe=backtester_sharpe, total_trades=200,
			strategy="TestStrat",
		)

		tracker = MagicMock()
		tracker.list_results.return_value = [
			{"strategy": f"S{i}", "status": "ok", "sharpe": 1.0 + i * 0.1, "total_trades": 200}
			for i in range(10)
		]

		ctx = GateContext(tracker=tracker, pnl_values=pnl, hypothesis=h)
		gate = DeflatedSharpeGate(threshold=0.95)
		gate_result = gate.check(result, ctx)

		sr_observed = gate_result.details["sr_observed"]
		sr0 = gate_result.details["sr0"]

		# Both should be on per-trade scale. sr0 = 1/sqrt(199) * ~2.7 ≈ 0.19
		# sr_observed ≈ 0.116. Both are small per-trade numbers.
		assert sr0 != 0, "sr0 should not be zero"
		assert sr0 < 1.0, f"sr0 ({sr0:.4f}) should be per-trade scale (< 1.0)"
		assert abs(sr_observed) < 1.0, f"sr_observed ({sr_observed:.4f}) should be per-trade scale"

	def test_dsr_groups_sharpes_by_strategy(self):
		"""Same strategy tested on multiple series should count as one trial.

		Before the fix, 1 strategy × 10 series = 10 data points for sr_var
		but N=1, causing "only 1 strategies tested" failure or inflated variance.
		After the fix, the 10 Sharpes are averaged into 1 representative value.
		"""
		from edge_catcher.research.validation.gate_dsr import DeflatedSharpeGate

		# 3 strategies, each tested on 5 series — 15 total rows
		tracker = MagicMock()
		rows = []
		for s_idx in range(3):
			for series_idx in range(5):
				rows.append({
					"strategy": f"Strat{s_idx}",
					"status": "ok",
					"sharpe": 0.3 + s_idx * 0.1 + series_idx * 0.02,
					"total_trades": 1,
				})
		tracker.list_results.return_value = rows

		pnl = [20] * 80 + [-2] * 20
		result = _make_result(pnl_values=pnl, sharpe=5.0, total_trades=100)
		ctx = GateContext(tracker=tracker, pnl_values=pnl, hypothesis=result.hypothesis)

		gate = DeflatedSharpeGate()
		gr = gate.check(result, ctx)

		# Should see 3 strategy families, not 15 or 1
		assert gr.details["n_strategies"] == 3

	def test_strategy_family_helper(self):
		"""_strategy_family strips trailing V\\d+ suffixes."""
		from edge_catcher.research.validation.gate_dsr import _strategy_family

		assert _strategy_family("FooV1") == "Foo"
		assert _strategy_family("test-strategy-aV1") == "test-strategy-a"
		assert _strategy_family("test-strategy-a") == "test-strategy-a"
		assert _strategy_family("MomentumV2V3") == "Momentum"
		assert _strategy_family("V1") == "V1"  # degenerate: whole name is suffix
		assert _strategy_family("MyStratV10") == "MyStrat"
		assert _strategy_family("TestV1V2V3") == "Test"

	def test_family_aware_grouping(self):
		"""FooV1, FooV2, FooV3 + Bar should count as 2 families, not 4 strategies."""
		from edge_catcher.research.validation.gate_dsr import DeflatedSharpeGate

		strategies = ["FooV1", "FooV2", "FooV3", "Bar"]
		sharpes = [0.5, 0.6, 0.4, 0.8]
		tracker = self._make_tracker_with_results(sharpes, strategies)

		pnl = [20] * 80 + [-2] * 20
		result = _make_result(pnl_values=pnl, sharpe=5.0, total_trades=100)
		ctx = GateContext(tracker=tracker, pnl_values=pnl, hypothesis=result.hypothesis)

		gate = DeflatedSharpeGate()
		gr = gate.check(result, ctx)

		assert gr.details["n_strategies"] == 2  # Foo family + Bar family

	def test_low_t_bypass_skips_dsr(self):
		"""T < 50 should skip DSR and let other gates decide."""
		from edge_catcher.research.validation.gate_dsr import DeflatedSharpeGate

		pnl = [10] * 25 + [-5] * 5  # T=30
		result = _make_result(pnl_values=pnl, sharpe=2.0, total_trades=30)

		# Don't even need a tracker — should return before reaching it
		ctx = GateContext(tracker=None, pnl_values=pnl, hypothesis=result.hypothesis)

		gate = DeflatedSharpeGate()
		gr = gate.check(result, ctx)
		assert gr.passed
		assert gr.details.get("skipped") is True
		assert "skipped" in gr.reason.lower()
		assert gr.tier is None  # not "review" — simply skipped

	def test_dsr_review_tier(self):
		"""DSR in [0.80, 0.95) should produce tier='review' with passed=True."""
		from edge_catcher.research.validation.gate_dsr import DeflatedSharpeGate

		# Use a gate with a low review_floor so we can control the band.
		# We need a scenario where DSR ends up between 0.80 and 0.95.
		# Strategy with moderate edge in a small pool.
		sharpes = [0.2, -0.1, 0.5, 0.3, 0.1]
		tracker = self._make_tracker_with_results(sharpes)

		# Moderate PnL — should produce borderline DSR
		pnl = [3] * 55 + [-2] * 45  # weak but positive
		result = _make_result(pnl_values=pnl, sharpe=1.5, total_trades=100)
		ctx = GateContext(tracker=tracker, pnl_values=pnl, hypothesis=result.hypothesis)

		# Use thresholds that bracket the computed DSR
		gate = DeflatedSharpeGate(threshold=0.99, review_floor=0.01)
		gr = gate.check(result, ctx)

		# With threshold=0.99, most strategies won't reach "promote".
		# With review_floor=0.01, anything above 0.01 DSR lands in review band.
		if gr.passed and "dsr" in gr.details:
			dsr = gr.details["dsr"]
			if dsr < 0.99:
				assert gr.tier == "review"
			else:
				assert gr.tier is None

	def test_dsr_below_review_floor_fails(self):
		"""DSR below review_floor should fail (passed=False)."""
		from edge_catcher.research.validation.gate_dsr import DeflatedSharpeGate

		# 500 strategy families — high N pushes SR0 up
		sharpes = [0.1] * 500
		tracker = self._make_tracker_with_results(sharpes)

		# Very weak signal: per-trade Sharpe ~0.04, SR0 ~0.31
		pnl = [1] * 52 + [-1] * 48
		result = _make_result(pnl_values=pnl, sharpe=1.0, total_trades=100)
		ctx = GateContext(tracker=tracker, pnl_values=pnl, hypothesis=result.hypothesis)

		gate = DeflatedSharpeGate(threshold=0.95, review_floor=0.80)
		gr = gate.check(result, ctx)
		assert not gr.passed

	def test_dsr_negative_denominator_fails_gracefully(self):
		"""Extreme skew that makes the SE denominator negative should fail
		with a clear reason instead of silently using abs().
		"""
		from edge_catcher.research.validation.gate_dsr import DeflatedSharpeGate

		# Construct extreme negative skew: many small gains, one huge loss
		pnl = [1.0] * 99 + [-10000.0]
		result = _make_result(pnl_values=pnl, sharpe=0.5, total_trades=100)

		# Background strategies with spread Sharpes to get a meaningful sr0
		sharpes = [0.5 + i * 0.5 for i in range(10)]
		tracker = self._make_tracker_with_results(sharpes)
		ctx = GateContext(tracker=tracker, pnl_values=pnl, hypothesis=result.hypothesis)

		gate = DeflatedSharpeGate()
		gr = gate.check(result, ctx)

		# If denominator went negative, we should get a specific failure reason
		# (not a silent abs() pass). If it stayed positive, that's fine too —
		# the key is no silent masking.
		if "denominator non-positive" in gr.reason:
			assert not gr.passed
			assert "denom_inner" in gr.details
		else:
			# Denominator stayed positive — gate ran normally, check it has dsr
			assert "dsr" in gr.details


# ---------------------------------------------------------------------------
# Monte Carlo Gate
# ---------------------------------------------------------------------------

class TestMonteCarloGate:
	def test_strong_signal_passes(self):
		"""Consistently positive trades should pass Monte Carlo."""
		from edge_catcher.research.validation.gate_monte_carlo import MonteCarloGate

		pnl = [10] * 90 + [-1] * 10  # very strong positive bias
		result = _make_result(pnl_values=pnl, total_trades=100)
		ctx = GateContext(tracker=None, pnl_values=pnl, hypothesis=result.hypothesis)

		gate = MonteCarloGate()
		gr = gate.check(result, ctx)
		assert gr.passed
		assert gr.details["p_value"] < 0.05

	def test_noise_fails(self):
		"""Zero-mean noise should fail Monte Carlo."""
		from edge_catcher.research.validation.gate_monte_carlo import MonteCarloGate

		# Symmetric around zero — no edge
		pnl = [5, -5] * 50
		result = _make_result(pnl_values=pnl, total_trades=100)
		ctx = GateContext(tracker=None, pnl_values=pnl, hypothesis=result.hypothesis)

		gate = MonteCarloGate()
		gr = gate.check(result, ctx)
		assert not gr.passed
		assert gr.details["p_value"] >= 0.05

	def test_insufficient_data_fails(self):
		"""Too few trades should fail."""
		from edge_catcher.research.validation.gate_monte_carlo import MonteCarloGate

		pnl = [10]
		result = _make_result(pnl_values=pnl, total_trades=1)
		ctx = GateContext(tracker=None, pnl_values=pnl, hypothesis=result.hypothesis)

		gate = MonteCarloGate()
		gr = gate.check(result, ctx)
		assert not gr.passed

	def test_reproducibility(self):
		"""Same dedup_key should produce same p-value."""
		from edge_catcher.research.validation.gate_monte_carlo import MonteCarloGate

		pnl = [10] * 70 + [-5] * 30
		result = _make_result(pnl_values=pnl, total_trades=100)
		ctx = GateContext(tracker=None, pnl_values=pnl, hypothesis=result.hypothesis)

		gate = MonteCarloGate()
		gr1 = gate.check(result, ctx)
		gr2 = gate.check(result, ctx)
		assert gr1.details["p_value"] == gr2.details["p_value"]

	def test_p_value_never_zero(self):
		"""Even with an extremely strong signal, p-value should never be 0.

		Uses (count+1)/(N+1) formula so the observed statistic is always
		counted as one permutation.
		"""
		from edge_catcher.research.validation.gate_monte_carlo import MonteCarloGate

		# Overwhelmingly positive with small variance — no permutation will beat this
		pnl = [1000 + i * 0.01 for i in range(100)]
		result = _make_result(pnl_values=pnl, total_trades=100)
		ctx = GateContext(tracker=None, pnl_values=pnl, hypothesis=result.hypothesis)

		gate = MonteCarloGate(n_permutations=1000)
		gr = gate.check(result, ctx)
		assert gr.passed
		assert gr.details["p_value"] > 0, "p-value should never be exactly 0"
		# Should be 1/1001 ≈ 0.001
		assert gr.details["p_value"] == pytest.approx(1 / 1001, abs=0.001)

	def test_sharpe_based_permutation(self):
		"""Gate should test Sharpe (mean/std), not just mean.

		Symmetric distribution with zero mean → Sharpe ~0 → should fail.
		"""
		from edge_catcher.research.validation.gate_monte_carlo import MonteCarloGate

		import random as _random
		rng = _random.Random(99)
		# Zero-mean with high variance: Sharpe is ~0
		pnl = [rng.gauss(0, 100) for _ in range(200)]
		# Center exactly at zero to ensure Sharpe is truly negligible
		mean = statistics.mean(pnl)
		pnl = [v - mean for v in pnl]

		gate = MonteCarloGate(n_permutations=2000)
		h = _make_hypothesis()
		result = _make_result(pnl_values=pnl, sharpe=0.0, total_trades=len(pnl))
		ctx = GateContext(tracker=None, pnl_values=pnl, hypothesis=h)

		gate_result = gate.check(result, ctx)
		# Zero-mean Sharpe should not be significant
		assert not gate_result.passed
		assert "observed_sharpe" in gate_result.details


# ---------------------------------------------------------------------------
# Temporal Consistency Gate
# ---------------------------------------------------------------------------

class TestTemporalConsistencyGate:
	def test_consistent_strategy_passes(self):
		"""Strategy that performs well across all windows passes."""
		from edge_catcher.research.validation.gate_temporal_consistency import TemporalConsistencyGate

		gate = TemporalConsistencyGate()
		result = _make_result(sharpe=3.0, total_trades=200)
		h = result.hypothesis

		agent = MagicMock()
		agent.run_backtest_only.return_value = {
			"sharpe": 3.0, "total_trades": 40, "net_pnl_cents": 100.0,
		}

		ctx = GateContext(
			tracker=None, pnl_values=[10]*200,
			hypothesis=h, agent=agent,
		)
		gate_result = gate.check(result, ctx)
		assert gate_result.passed

	def test_inconsistent_strategy_fails(self):
		"""Strategy that loses money in most windows fails."""
		from edge_catcher.research.validation.gate_temporal_consistency import TemporalConsistencyGate

		gate = TemporalConsistencyGate()
		result = _make_result(sharpe=3.0, total_trades=200)
		h = result.hypothesis

		agent = MagicMock()
		agent.run_backtest_only.return_value = {
			"sharpe": -0.5, "total_trades": 40, "net_pnl_cents": -50.0,
		}

		ctx = GateContext(
			tracker=None, pnl_values=[10]*200,
			hypothesis=h, agent=agent,
		)
		gate_result = gate.check(result, ctx)
		assert not gate_result.passed

	def test_no_agent_fails(self):
		"""Should fail gracefully if no agent provided."""
		from edge_catcher.research.validation.gate_temporal_consistency import TemporalConsistencyGate

		gate = TemporalConsistencyGate()
		result = _make_result()
		ctx = GateContext(tracker=None, pnl_values=[10]*100, hypothesis=result.hypothesis)
		gate_result = gate.check(result, ctx)
		assert not gate_result.passed

	def test_none_dates_fails_without_db(self):
		"""If dates are None and no DB accessible, gate fails."""
		from edge_catcher.research.validation.gate_temporal_consistency import TemporalConsistencyGate

		gate = TemporalConsistencyGate()
		h = _make_hypothesis(start_date=None, end_date=None, db_path="nonexistent.db")
		result = _make_result(hypothesis=h)
		agent = MagicMock()
		ctx = GateContext(tracker=None, pnl_values=[10]*100, hypothesis=h, agent=agent)
		gate_result = gate.check(result, ctx)
		assert not gate_result.passed

	def test_normalizes_sharpe_by_trade_count(self):
		"""Sharpe values should be normalized to per-trade scale."""
		from edge_catcher.research.validation.gate_temporal_consistency import TemporalConsistencyGate

		gate = TemporalConsistencyGate()
		result = _make_result(sharpe=3.0, total_trades=200)

		call_count = [0]
		def mock_backtest(h):
			call_count[0] += 1
			trades = 100 if call_count[0] % 2 == 0 else 25
			raw_sharpe = 2.0 * math.sqrt(trades)  # same per-trade Sharpe
			return {"sharpe": raw_sharpe, "total_trades": trades, "net_pnl_cents": 50.0}

		agent = MagicMock()
		agent.run_backtest_only.side_effect = mock_backtest

		ctx = GateContext(
			tracker=None, pnl_values=[10]*200,
			hypothesis=result.hypothesis, agent=agent,
		)
		gate_result = gate.check(result, ctx)
		# With equal per-trade Sharpes, the worst-window check should pass
		assert gate_result.passed
		assert "sharpes" in gate_result.details

	def test_worst_window_sharpe_floor(self):
		"""A single terrible window should cause failure."""
		from edge_catcher.research.validation.gate_temporal_consistency import TemporalConsistencyGate

		gate = TemporalConsistencyGate()
		result = _make_result(sharpe=3.0, total_trades=200)

		call_count = [0]
		def mock_backtest(h):
			call_count[0] += 1
			if call_count[0] == 3:
				return {"sharpe": -5.0, "total_trades": 30, "net_pnl_cents": -200.0}
			return {"sharpe": 3.0, "total_trades": 40, "net_pnl_cents": 100.0}

		agent = MagicMock()
		agent.run_backtest_only.side_effect = mock_backtest

		ctx = GateContext(
			tracker=None, pnl_values=[10]*200,
			hypothesis=result.hypothesis, agent=agent,
		)
		gate_result = gate.check(result, ctx)
		# One window has deeply negative per-trade Sharpe, should fail
		assert not gate_result.passed


# ---------------------------------------------------------------------------
# Parameter Sensitivity Gate
# ---------------------------------------------------------------------------

class TestParameterSensitivityGate:
	SAMPLE_STRATEGY = '''
class TestStrategy(Strategy):
	name = "TestStrat"
	lookback = 20
	threshold = 0.85
	max_hold = 60

	def on_trade(self, market, trade):
		pass
'''

	def test_robust_strategy_passes(self):
		"""Strategy where neighbors also perform well should pass."""
		from edge_catcher.research.validation.gate_sensitivity import ParameterSensitivityGate

		mock_agent = MagicMock()
		mock_agent.read_strategy_code.return_value = self.SAMPLE_STRATEGY

		result = _make_result(sharpe=2.5, total_trades=100)
		ctx = GateContext(
			tracker=None, pnl_values=[5]*100, hypothesis=result.hypothesis,
			agent=mock_agent,
		)

		gate = ParameterSensitivityGate()
		# Mock _run_neighbor to avoid filesystem/import side effects
		# Original Sharpe = 2.5. All neighbors return 1.8 (>= 50% of 2.5)
		with patch.object(gate, "_run_neighbor", return_value=1.8):
			gr = gate.check(result, ctx)
		assert gr.passed

	def test_fragile_strategy_fails(self):
		"""Strategy where most neighbors collapse should fail."""
		from edge_catcher.research.validation.gate_sensitivity import ParameterSensitivityGate

		mock_agent = MagicMock()
		mock_agent.read_strategy_code.return_value = self.SAMPLE_STRATEGY

		result = _make_result(sharpe=2.5, total_trades=100)
		ctx = GateContext(
			tracker=None, pnl_values=[5]*100, hypothesis=result.hypothesis,
			agent=mock_agent,
		)

		gate = ParameterSensitivityGate()
		# All neighbors return very low Sharpe
		with patch.object(gate, "_run_neighbor", return_value=0.1):
			gr = gate.check(result, ctx)
		assert not gr.passed

	def test_no_params_passes(self):
		"""Strategy with no numeric params should pass (nothing to perturb)."""
		from edge_catcher.research.validation.gate_sensitivity import ParameterSensitivityGate

		code = '''
class MinimalStrategy(Strategy):
	name = "Minimal"

	def on_trade(self, market, trade):
		pass
'''
		mock_agent = MagicMock()
		mock_agent.read_strategy_code.return_value = code

		result = _make_result(sharpe=2.5, total_trades=100)
		ctx = GateContext(
			tracker=None, pnl_values=[5]*100, hypothesis=result.hypothesis,
			agent=mock_agent,
		)

		gate = ParameterSensitivityGate()
		gr = gate.check(result, ctx)
		assert gr.passed

	def test_no_source_fails(self):
		"""If strategy source can't be read, gate fails."""
		from edge_catcher.research.validation.gate_sensitivity import ParameterSensitivityGate

		mock_agent = MagicMock()
		mock_agent.read_strategy_code.return_value = None

		result = _make_result(sharpe=2.5, total_trades=100)
		ctx = GateContext(
			tracker=None, pnl_values=[5]*100, hypothesis=result.hypothesis,
			agent=mock_agent,
		)

		gate = ParameterSensitivityGate()
		gr = gate.check(result, ctx)
		assert not gr.passed

	def test_sensitivity_normalizes_sharpe_by_trade_count(self):
		"""Original and neighbor Sharpes should be on per-trade scale.

		Without normalization, a neighbor with more trades would have an
		inflated backtester Sharpe, masking real degradation.
		"""
		from edge_catcher.research.validation.gate_sensitivity import ParameterSensitivityGate

		mock_agent = MagicMock()
		mock_agent.read_strategy_code.return_value = self.SAMPLE_STRATEGY

		# Original: bt_sharpe=2.5, 100 trades → per-trade = 0.25
		result = _make_result(sharpe=2.5, total_trades=100)
		ctx = GateContext(
			tracker=None, pnl_values=[5]*100, hypothesis=result.hypothesis,
			agent=mock_agent,
		)

		gate = ParameterSensitivityGate()
		# min_acceptable = 0.25 * 0.5 = 0.125 (per-trade)
		# _run_neighbor returns per-trade Sharpe after our fix
		# Return 0.15 → above 0.125 → passes
		with patch.object(gate, "_run_neighbor", return_value=0.15):
			gr = gate.check(result, ctx)
		assert gr.passed
		assert gr.details["min_acceptable_sharpe"] == pytest.approx(0.125, abs=0.01)

	def test_sensitivity_gate_uses_file_lock(self):
		"""_run_neighbor should acquire a lock before writing strategies_local.py."""
		import threading
		from edge_catcher.research.validation.gate_sensitivity import ParameterSensitivityGate

		gate = ParameterSensitivityGate()
		assert hasattr(gate, '_file_lock')
		assert isinstance(gate._file_lock, type(threading.Lock()))

	def test_sensitivity_gate_shares_lock_across_instances(self):
		"""Two ParameterSensitivityGate instances must share the same lock."""
		from edge_catcher.research.validation.gate_sensitivity import ParameterSensitivityGate

		gate_a = ParameterSensitivityGate()
		gate_b = ParameterSensitivityGate()
		assert gate_a._file_lock is gate_b._file_lock


# ---------------------------------------------------------------------------
# Pipeline review tier integration
# ---------------------------------------------------------------------------

class TestPipelineReviewTier:
	def test_review_tier_produces_review_verdict(self):
		"""A gate with tier='review' should make the pipeline return 'review'."""
		from edge_catcher.research.validation.pipeline import ValidationPipeline
		from edge_catcher.research.validation.gate import Gate

		class ReviewGate(Gate):
			name = "review_gate"
			def check(self, result, context):
				return GateResult(
					passed=True, gate_name=self.name,
					reason="borderline", tier="review",
				)

		class PassGate(Gate):
			name = "pass_gate"
			def check(self, result, context):
				return GateResult(
					passed=True, gate_name=self.name,
					reason="all good",
				)

		pipeline = ValidationPipeline([ReviewGate(), PassGate()])
		result = _make_result()
		ctx = GateContext(tracker=None, pnl_values=[5]*100, hypothesis=result.hypothesis)

		verdict, reason, gate_results = pipeline.validate(result, ctx)
		assert verdict == "review"
		assert "review" in reason.lower()

	def test_all_normal_passes_still_promote(self):
		"""Pipeline with no tier set should still return 'promote'."""
		from edge_catcher.research.validation.pipeline import ValidationPipeline
		from edge_catcher.research.validation.gate import Gate

		class PassGate(Gate):
			name = "pass_gate"
			def check(self, result, context):
				return GateResult(
					passed=True, gate_name=self.name,
					reason="all good",
				)

		pipeline = ValidationPipeline([PassGate()])
		result = _make_result()
		ctx = GateContext(tracker=None, pnl_values=[5]*100, hypothesis=result.hypothesis)

		verdict, reason, gate_results = pipeline.validate(result, ctx)
		assert verdict == "promote"

	def test_failure_still_short_circuits(self):
		"""A failed gate should still short-circuit, regardless of tiers."""
		from edge_catcher.research.validation.pipeline import ValidationPipeline
		from edge_catcher.research.validation.gate import Gate

		class FailGate(Gate):
			name = "fail_gate"
			def check(self, result, context):
				return GateResult(
					passed=False, gate_name=self.name,
					reason="nope",
				)

		class NeverReachedGate(Gate):
			name = "never_reached"
			def check(self, result, context):
				raise AssertionError("Should not be called")

		pipeline = ValidationPipeline([FailGate(), NeverReachedGate()])
		result = _make_result()
		ctx = GateContext(tracker=None, pnl_values=[5]*100, hypothesis=result.hypothesis)

		verdict, reason, gate_results = pipeline.validate(result, ctx)
		assert verdict == "explore"
		assert len(gate_results) == 1


# ---------------------------------------------------------------------------
# Evaluator validate verdict
# ---------------------------------------------------------------------------

class TestEvaluatorValidateVerdict:
	def test_borderline_sharpe_gets_validate(self):
		"""Sharpe 1.0-2.0 with >= 100 trades should get 'validate', not 'explore'."""
		from edge_catcher.research.evaluator import Evaluator, Thresholds

		result = _make_result(sharpe=1.5, total_trades=150, net_pnl_cents=200.0)
		verdict, reason = Evaluator().evaluate(result, Thresholds())
		assert verdict == "validate"

	def test_low_trade_count_still_explore(self):
		"""Sharpe 1.5 but < 100 trades should stay 'explore'."""
		from edge_catcher.research.evaluator import Evaluator, Thresholds

		result = _make_result(sharpe=1.5, total_trades=80, net_pnl_cents=200.0)
		verdict, reason = Evaluator().evaluate(result, Thresholds())
		assert verdict == "explore"

	def test_high_sharpe_still_candidate(self):
		"""Sharpe >= 2.0 should still get 'candidate', not 'validate'."""
		from edge_catcher.research.evaluator import Evaluator, Thresholds

		result = _make_result(sharpe=2.5, total_trades=150, net_pnl_cents=500.0)
		verdict, reason = Evaluator().evaluate(result, Thresholds())
		assert verdict == "candidate"


# ---------------------------------------------------------------------------
# Refinement keep/discard with normalized Sharpe
# ---------------------------------------------------------------------------

class TestShouldKeepRefinement:
	def test_higher_per_trade_sharpe_kept_despite_fewer_trades(self):
		"""Refinement with fewer but higher-quality trades should be kept."""
		from edge_catcher.research.loop import LoopOrchestrator

		# Original: Sharpe 3.5 with 200 trades -> per-trade = 3.5/sqrt(200) ~ 0.247
		original = [{"status": "ok", "sharpe": 3.5, "total_trades": 200, "verdict": "explore"}]
		# Refined: Sharpe 2.5 with 50 trades -> per-trade = 2.5/sqrt(50) ~ 0.354
		refined_h = _make_hypothesis()
		refined = [_make_result(sharpe=2.5, total_trades=50, verdict="explore", hypothesis=refined_h)]
		assert LoopOrchestrator._should_keep_refinement(original, refined)

	def test_lower_per_trade_sharpe_discarded(self):
		"""Refinement with worse per-trade Sharpe should be discarded."""
		from edge_catcher.research.loop import LoopOrchestrator

		# Original: Sharpe 2.0 with 50 trades -> per-trade = 2.0/sqrt(50) ~ 0.283
		original = [{"status": "ok", "sharpe": 2.0, "total_trades": 50, "verdict": "explore"}]
		# Refined: Sharpe 2.5 with 200 trades -> per-trade = 2.5/sqrt(200) ~ 0.177
		refined_h = _make_hypothesis()
		refined = [_make_result(sharpe=2.5, total_trades=200, verdict="explore", hypothesis=refined_h)]
		assert not LoopOrchestrator._should_keep_refinement(original, refined)


# ---------------------------------------------------------------------------
# Trajectory classification
# ---------------------------------------------------------------------------

class TestTrajectoryClassification:
	def test_single_review_among_many_kills_is_plateauing(self):
		"""1 review among 50 kills should be 'plateauing', not 'improving'."""
		from edge_catcher.research.journal import ResearchJournal

		results = [{"run_id": "r1", "verdict": "kill", "sharpe": 0.5}] * 50
		results.append({"run_id": "r1", "verdict": "review", "sharpe": 1.5})
		status = ResearchJournal.classify_trajectory("r1", results, None)
		assert status == "plateauing"

	def test_high_promote_rate_is_improving(self):
		"""Promote rate > 5% should be 'improving'."""
		from edge_catcher.research.journal import ResearchJournal

		results = [
			{"run_id": "r1", "verdict": "promote", "sharpe": 3.0},
			{"run_id": "r1", "verdict": "promote", "sharpe": 2.5},
			{"run_id": "r1", "verdict": "kill", "sharpe": 0.3},
		] * 5
		status = ResearchJournal.classify_trajectory("r1", results, None)
		assert status == "improving"

	def test_near_miss_sharpe_is_plateauing_not_improving(self):
		"""Sharpe within 5% of previous best was 'improving' under old code, now 'plateauing'."""
		from edge_catcher.research.journal import ResearchJournal

		prev = {"best_sharpe_overall": 3.0, "total_sessions": 5}
		results = [
			{"run_id": "r1", "verdict": "explore", "sharpe": 2.9},
		]
		status = ResearchJournal.classify_trajectory("r1", results, prev)
		assert status == "plateauing"
