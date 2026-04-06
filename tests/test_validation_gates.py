"""Tests for individual validation gates."""

from __future__ import annotations

import math
import statistics
from unittest.mock import MagicMock, patch

import pytest

from edge_catcher.research.hypothesis import Hypothesis, HypothesisResult
from edge_catcher.research.validation.gate import GateContext, GateResult


def _make_hypothesis(**kwargs) -> Hypothesis:
	defaults = dict(
		strategy="C", series="KXBTCD", db_path="data/kalshi.db",
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
		"""Strategy with Sharpe 2.0 among 500 tested should fail DSR."""
		from edge_catcher.research.validation.gate_dsr import DeflatedSharpeGate

		# 500 strategies with similar Sharpe distribution
		import random
		rng = random.Random(42)
		sharpes = [rng.gauss(0.5, 1.0) for _ in range(500)]
		tracker = self._make_tracker_with_results(sharpes)

		# Mediocre signal — Sharpe 2.0 is plausible from noise with 500 trials
		pnl = [5] * 60 + [-3] * 40
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
		"""sr_observed and sr0 should be on the same scale.

		The backtester reports Sharpe as mean/std * sqrt(N) (scaled).
		sr_observed is computed as mean/std (per-trade, unscaled).
		ok_sharpes from tracker must be normalized to per-trade scale too,
		or sr0 ends up ~sqrt(N) times larger than sr_observed, breaking DSR.
		"""
		from edge_catcher.research.validation.gate_dsr import DeflatedSharpeGate

		# PnL with known per-trade Sharpe ~0.116 (mean=10, std~86)
		# Backtester reports: 0.116 * sqrt(200) ~= 1.64
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

		# Tracker rows use backtester-scale Sharpes (1.0–1.9) with 200 trades each.
		# Per-trade equivalents are ~0.07–0.13.
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

		# sr_observed is per-trade (~0.116). After fix, ok_sharpes are normalized
		# to per-trade scale, so sr0 is also ~0.03–0.05. The ratio sr_observed/sr0
		# should be in a reasonable range (0.5–10).
		#
		# Before the fix, ok_sharpes are raw backtester Sharpes (~1.0–1.9 for 200
		# trades), making sr0 ~0.48, so ratio = 0.116/0.48 = 0.24 — below 0.5.
		assert sr0 != 0, "sr0 should not be zero"
		ratio = abs(sr_observed) / abs(sr0)
		assert ratio >= 0.5, (
			f"sr_observed ({sr_observed:.4f}) and sr0 ({sr0:.4f}) are on different "
			f"scales (ratio={ratio:.3f} < 0.5). ok_sharpes must be divided by "
			f"sqrt(total_trades) to match the per-trade sr_observed scale."
		)

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

		# Should see 3 strategies, not 15 or 1
		assert gr.details["n_strategies"] == 3
		# n_sharpes still reports total backtests for transparency
		assert gr.details["n_sharpes"] == 15

	def test_strategy_family_helper(self):
		"""_strategy_family strips trailing V\\d+ suffixes."""
		from edge_catcher.research.validation.gate_dsr import _strategy_family

		assert _strategy_family("FooV1") == "Foo"
		assert _strategy_family("CvolV1") == "Cvol"
		assert _strategy_family("Cvol") == "Cvol"
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
		assert gr.details["n_sharpes"] == 4  # total backtests

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

		# 500 strategies → very high sr0 → any moderate Sharpe will fail
		import random
		rng = random.Random(42)
		sharpes = [rng.gauss(0.5, 1.0) for _ in range(500)]
		tracker = self._make_tracker_with_results(sharpes)

		pnl = [5] * 60 + [-3] * 40
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

		# Overwhelmingly positive — no permutation will beat this
		pnl = [1000] * 100
		result = _make_result(pnl_values=pnl, total_trades=100)
		ctx = GateContext(tracker=None, pnl_values=pnl, hypothesis=result.hypothesis)

		gate = MonteCarloGate(n_permutations=1000)
		gr = gate.check(result, ctx)
		assert gr.passed
		assert gr.details["p_value"] > 0, "p-value should never be exactly 0"
		# Should be 1/1001 ≈ 0.001
		assert gr.details["p_value"] == pytest.approx(1 / 1001, abs=0.001)


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
