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


# ---------------------------------------------------------------------------
# Walk-Forward Gate
# ---------------------------------------------------------------------------

class TestWalkForwardGate:
	def test_good_oos_performance_passes(self):
		"""When OOS Sharpe >= 50% of IS and majority profitable, should pass."""
		from edge_catcher.research.validation.gate_walkforward import WalkForwardGate

		mock_agent = MagicMock()
		# 5 windows × 2 (IS + OOS) = 10 calls
		# IS: high Sharpe; OOS: moderate but >= 50% of IS
		is_data = {"sharpe": 2.0, "net_pnl_cents": 100, "total_trades": 20, "status": "ok", "pnl_values": [5]*20}
		oos_data = {"sharpe": 1.2, "net_pnl_cents": 50, "total_trades": 15, "status": "ok", "pnl_values": [3]*15}
		mock_agent.run_backtest_only.side_effect = [is_data, oos_data] * 5

		result = _make_result(total_trades=100)
		ctx = GateContext(
			tracker=None, pnl_values=[5]*100, hypothesis=result.hypothesis,
			agent=mock_agent,
		)

		gate = WalkForwardGate()
		gr = gate.check(result, ctx)
		assert gr.passed
		assert gr.details["sharpe_ratio"] >= 0.5

	def test_poor_oos_fails(self):
		"""When OOS Sharpe drops to near-zero, should fail."""
		from edge_catcher.research.validation.gate_walkforward import WalkForwardGate

		mock_agent = MagicMock()
		is_data = {"sharpe": 3.0, "net_pnl_cents": 200, "total_trades": 20, "status": "ok", "pnl_values": [10]*20}
		oos_data = {"sharpe": 0.1, "net_pnl_cents": -10, "total_trades": 15, "status": "ok", "pnl_values": [-1]*15}
		mock_agent.run_backtest_only.side_effect = [is_data, oos_data] * 5

		result = _make_result(total_trades=100)
		ctx = GateContext(
			tracker=None, pnl_values=[5]*100, hypothesis=result.hypothesis,
			agent=mock_agent,
		)

		gate = WalkForwardGate()
		gr = gate.check(result, ctx)
		assert not gr.passed

	def test_no_agent_fails(self):
		"""Should fail gracefully if no agent provided."""
		from edge_catcher.research.validation.gate_walkforward import WalkForwardGate

		result = _make_result(total_trades=100)
		ctx = GateContext(tracker=None, pnl_values=[5]*100, hypothesis=result.hypothesis, agent=None)

		gate = WalkForwardGate()
		gr = gate.check(result, ctx)
		assert not gr.passed

	def test_none_dates_fails_without_db(self):
		"""If dates are None and no DB accessible, gate fails."""
		from edge_catcher.research.validation.gate_walkforward import WalkForwardGate

		h = _make_hypothesis(start_date=None, end_date=None, db_path="/nonexistent/path.db")
		result = _make_result(total_trades=100)
		result.hypothesis = h
		mock_agent = MagicMock()
		ctx = GateContext(tracker=None, pnl_values=[5]*100, hypothesis=h, agent=mock_agent)

		gate = WalkForwardGate()
		gr = gate.check(result, ctx)
		assert not gr.passed
		assert "date range" in gr.reason.lower()

	def test_walkforward_no_date_overlap(self):
		"""IS end date and OOS start date should not overlap."""
		from edge_catcher.research.validation.gate_walkforward import WalkForwardGate

		gate = WalkForwardGate(n_windows=5, oos_ratio=0.3)
		windows = gate._make_windows("2025-01-01", "2025-12-31")

		for is_start, is_end, oos_start, oos_end in windows:
			# OOS should start the day after IS ends
			from datetime import datetime, timedelta
			is_end_dt = datetime.fromisoformat(is_end)
			oos_start_dt = datetime.fromisoformat(oos_start)
			assert oos_start_dt > is_end_dt, (
				f"OOS start {oos_start} should be after IS end {is_end}"
			)


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
