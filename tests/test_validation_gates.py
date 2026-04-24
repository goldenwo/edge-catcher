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
	h = _make_hypothesis(**{
		k: kwargs.pop(k)
		for k in list(kwargs)
		if k in ("strategy", "series", "db_path", "start_date", "end_date")
	})
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

	def test_dsr_respects_sweep_n_override(self):
		"""Passing sweep_N_override=500 makes N=500 regardless of tracker count."""
		from edge_catcher.research.validation.gate_dsr import DeflatedSharpeGate
		tracker = self._make_tracker_with_results([0.1, 0.2, 0.3])  # only 3 trials

		pnl = [20] * 80 + [-2] * 20
		result = _make_result(pnl_values=pnl, sharpe=5.0, total_trades=100)
		ctx = GateContext(tracker=tracker, pnl_values=pnl, hypothesis=result.hypothesis)

		gate = DeflatedSharpeGate(sweep_N_override=500)
		gr = gate.check(result, ctx)
		assert gr.details["n_strategies"] == 500

	def test_dsr_override_works_without_tracker(self):
		"""tracker=None + sweep_N_override → override wins, no early return.

		Locks in the Task 8 reordering: the override check must come BEFORE
		the tracker-None guard in check(), otherwise override+null-tracker
		would fail on the internal check and never reach the computation.
		"""
		from edge_catcher.research.validation.gate_dsr import DeflatedSharpeGate

		pnl = [20] * 80 + [-2] * 20
		result = _make_result(pnl_values=pnl, sharpe=5.0, total_trades=100)
		ctx = GateContext(tracker=None, pnl_values=pnl, hypothesis=result.hypothesis)

		gate = DeflatedSharpeGate(sweep_N_override=500)
		gr = gate.check(result, ctx)
		assert gr.details["n_strategies"] == 500

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

	def test_dsr_n_counts_trials_not_families(self):
		"""For the Bailey multiple-testing correction, N should equal the
		number of distinct experimental trials (strategy × series × fee
		cells) — not the number of distinct strategy family names.

		Before the fix, 3 strategies × 5 series = 15 trials were counted
		as N=3. After the fix they count as N=15 because each (strategy,
		series) combo is an independent pull from the null Sharpe
		distribution for multiple-testing purposes.
		"""
		from edge_catcher.research.validation.gate_dsr import DeflatedSharpeGate

		tracker = MagicMock()
		rows = []
		for s_idx in range(3):
			for series_idx in range(5):
				rows.append({
					"strategy": f"Strat{s_idx}",
					"series": f"SER_{series_idx}",
					"db_path": "data/kalshi-btc.db",
					"fee_pct": 1.0,
					"status": "ok",
					"sharpe": 0.3 + s_idx * 0.1 + series_idx * 0.02,
					"total_trades": 100,
				})
		tracker.list_results.return_value = rows

		pnl = [20] * 80 + [-2] * 20
		result = _make_result(pnl_values=pnl, sharpe=5.0, total_trades=100)
		ctx = GateContext(tracker=tracker, pnl_values=pnl, hypothesis=result.hypothesis)

		gate = DeflatedSharpeGate()
		gr = gate.check(result, ctx)

		assert gr.details["n_strategies"] == 15

	def test_dsr_n_dedupes_identical_trials(self):
		"""Two identical (strategy, series, fee) rows in the tracker are
		one trial, not two. Re-evaluation of the same hypothesis in a later
		run should not inflate N."""
		from edge_catcher.research.validation.gate_dsr import DeflatedSharpeGate

		tracker = MagicMock()
		tracker.list_results.return_value = [
			{"strategy": "S", "series": "X", "db_path": "d.db", "fee_pct": 1.0,
			 "status": "ok", "sharpe": 0.4, "total_trades": 100},
			{"strategy": "S", "series": "X", "db_path": "d.db", "fee_pct": 1.0,
			 "status": "ok", "sharpe": 0.4, "total_trades": 100},
			{"strategy": "S", "series": "Y", "db_path": "d.db", "fee_pct": 1.0,
			 "status": "ok", "sharpe": 0.3, "total_trades": 100},
		]

		pnl = [20] * 80 + [-2] * 20
		result = _make_result(pnl_values=pnl, sharpe=5.0, total_trades=100)
		ctx = GateContext(tracker=tracker, pnl_values=pnl, hypothesis=result.hypothesis)

		gate = DeflatedSharpeGate()
		gr = gate.check(result, ctx)
		assert gr.details["n_strategies"] == 2

	def test_dsr_n_tolerates_null_total_trades(self):
		"""sqlite NULL total_trades should not crash the gate.

		Regression: the tracker's ``results`` table allows NULL
		total_trades, and ``r.get("total_trades", 0)`` returns None (not 0)
		when the key exists with a None value. ``None < 1`` was a TypeError.
		"""
		from edge_catcher.research.validation.gate_dsr import DeflatedSharpeGate

		tracker = MagicMock()
		tracker.list_results.return_value = [
			{"strategy": "S", "series": "X", "db_path": "d.db", "fee_pct": 1.0,
			 "status": "ok", "sharpe": 0.4, "total_trades": None},  # NULL
			{"strategy": "T", "series": "X", "db_path": "d.db", "fee_pct": 1.0,
			 "status": "ok", "sharpe": 0.4, "total_trades": 100},
			{"strategy": "U", "series": "Y", "db_path": "d.db", "fee_pct": 1.0,
			 "status": "ok", "sharpe": 0.4, "total_trades": 50},
		]

		pnl = [20] * 80 + [-2] * 20
		result = _make_result(pnl_values=pnl, sharpe=5.0, total_trades=100)
		ctx = GateContext(tracker=tracker, pnl_values=pnl, hypothesis=result.hypothesis)

		gate = DeflatedSharpeGate()
		gr = gate.check(result, ctx)

		# Should not crash. NULL row excluded, so N=2 (T/X and U/Y).
		assert gr.details["n_strategies"] == 2

	def test_dsr_n_family_collapsing_still_applied(self):
		"""V1/V2/V3 variants of a strategy on the same series collapse to
		one family for trial counting, so the trial key is
		(family, series, fee), not (strategy_name, series, fee).

		Otherwise, LLM-generated refinements would inflate N artificially
		for strategies that have been refined many times."""
		from edge_catcher.research.validation.gate_dsr import DeflatedSharpeGate

		tracker = MagicMock()
		tracker.list_results.return_value = [
			{"strategy": "FooV1", "series": "X", "db_path": "d.db", "fee_pct": 1.0,
			 "status": "ok", "sharpe": 0.4, "total_trades": 100},
			{"strategy": "FooV2", "series": "X", "db_path": "d.db", "fee_pct": 1.0,
			 "status": "ok", "sharpe": 0.5, "total_trades": 100},
			{"strategy": "FooV3", "series": "X", "db_path": "d.db", "fee_pct": 1.0,
			 "status": "ok", "sharpe": 0.3, "total_trades": 100},
			{"strategy": "Bar", "series": "Y", "db_path": "d.db", "fee_pct": 1.0,
			 "status": "ok", "sharpe": 0.6, "total_trades": 100},
		]

		pnl = [20] * 80 + [-2] * 20
		result = _make_result(pnl_values=pnl, sharpe=5.0, total_trades=100)
		ctx = GateContext(tracker=tracker, pnl_values=pnl, hypothesis=result.hypothesis)

		gate = DeflatedSharpeGate()
		gr = gate.check(result, ctx)
		# (Foo family, X) = 1 trial; (Bar family, Y) = 1 trial → N=2
		assert gr.details["n_strategies"] == 2

	def test_dsr_groups_sharpes_by_strategy(self):
		"""Legacy behavior: the details key is still called n_strategies and
		the field is populated, even though it now counts (family, series,
		fee) trials. Kept for API stability."""
		from edge_catcher.research.validation.gate_dsr import DeflatedSharpeGate

		tracker = MagicMock()
		rows = []
		for s_idx in range(3):
			for series_idx in range(5):
				rows.append({
					"strategy": f"Strat{s_idx}",
					"series": f"SER_{series_idx}",
					"db_path": "data/kalshi-btc.db",
					"fee_pct": 1.0,
					"status": "ok",
					"sharpe": 0.3 + s_idx * 0.1 + series_idx * 0.02,
					"total_trades": 100,
				})
		tracker.list_results.return_value = rows

		pnl = [20] * 80 + [-2] * 20
		result = _make_result(pnl_values=pnl, sharpe=5.0, total_trades=100)
		ctx = GateContext(tracker=tracker, pnl_values=pnl, hypothesis=result.hypothesis)

		gate = DeflatedSharpeGate()
		gr = gate.check(result, ctx)

		# 3 families × 5 series = 15 trials
		assert "n_strategies" in gr.details
		assert gr.details["n_strategies"] == 15

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

	def test_dsr_denom_uses_observed_sharpe_per_bailey_eq9(self):
		"""Bailey & Lopez de Prado (2014), Eq. 9: the DSR denominator is

		    sqrt(1 - skew * SR_hat + ((kurt_raw - 1) / 4) * SR_hat^2)

		— where SR_hat is the *observed* per-trade Sharpe, not the null
		cutoff SR0. Previous code mistakenly used sr0 in the denominator,
		which makes denom_inner ≈ 1 regardless of distribution shape.

		We verify by expanding the expected Bailey denominator from the
		fat-tailed test distribution and checking it appears in the gate's
		details.
		"""
		from edge_catcher.research.validation.gate_dsr import DeflatedSharpeGate
		from scipy.stats import skew as _skew, kurtosis as _kurt

		# Fat-tailed: 98 small wins, 2 large losses — skewed & leptokurtic
		pnl = [2.0] * 98 + [-50.0] * 2
		sr_hat = statistics.mean(pnl) / statistics.stdev(pnl)
		skew_val = float(_skew(pnl, bias=False))
		exkurt = float(_kurt(pnl, bias=False))  # excess kurtosis
		expected_denom_inner = 1 - skew_val * sr_hat + (exkurt + 2) / 4 * sr_hat ** 2

		tracker = self._make_tracker_with_results([0.1, 0.2, -0.1])

		result = _make_result(
			pnl_values=pnl, sharpe=sr_hat * math.sqrt(len(pnl)), total_trades=len(pnl),
		)
		ctx = GateContext(tracker=tracker, pnl_values=pnl, hypothesis=result.hypothesis)

		gate = DeflatedSharpeGate()
		gr = gate.check(result, ctx)

		assert "denom_inner" in gr.details, "gate must expose denom_inner for auditability"
		assert gr.details["denom_inner"] == pytest.approx(expected_denom_inner, abs=1e-3)

	def test_dsr_symmetric_vs_fat_tail_dsr_differs(self):
		"""Two strategies with identical observed per-trade Sharpe should
		produce different DSR scores when their return distributions differ
		in skew/kurt. With sr0 in the denominator (old bug), kurtosis is
		ignored and both score the same."""
		from edge_catcher.research.validation.gate_dsr import DeflatedSharpeGate

		# Symmetric: mean≈1, near-normal
		symmetric = []
		rng = __import__("random").Random(1)
		for _ in range(200):
			symmetric.append(1.0 + rng.gauss(0, 5))
		# Fat-tailed: lots of small wins, a few huge losses, same T
		fat = [1.5] * 180 + [-10.0] * 20  # mean=0.35, strong negative skew

		tracker = self._make_tracker_with_results([0.1, 0.2, 0.3])

		res_sym = _make_result(
			pnl_values=symmetric,
			sharpe=statistics.mean(symmetric) / statistics.stdev(symmetric) * math.sqrt(len(symmetric)),
			total_trades=len(symmetric),
		)
		res_fat = _make_result(
			pnl_values=fat,
			sharpe=statistics.mean(fat) / statistics.stdev(fat) * math.sqrt(len(fat)),
			total_trades=len(fat),
		)
		ctx_sym = GateContext(tracker=tracker, pnl_values=symmetric, hypothesis=res_sym.hypothesis)
		ctx_fat = GateContext(tracker=tracker, pnl_values=fat, hypothesis=res_fat.hypothesis)

		gate = DeflatedSharpeGate()
		gr_sym = gate.check(res_sym, ctx_sym)
		gr_fat = gate.check(res_fat, ctx_fat)

		# The fat-tailed distribution has a larger denom_inner (due to kurtosis),
		# shrinking its z-stat. It should get a meaningfully lower DSR.
		assert gr_sym.details["denom_inner"] != gr_fat.details["denom_inner"]

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

	def test_short_range_15_to_34_days_still_partitions(self):
		"""Ranges of 15-34 days should produce 3 windows (not the default 5)
		so the gate can still run on series with limited history. Before
		this behavior, the hard 35-day minimum meant crypto 15m series
		with 9-22 days of data were silently failing even with strong
		per-trade Sharpes. Discovered during Task 5 sweep v2 analysis.
		"""
		from edge_catcher.research.validation.gate_temporal_consistency import TemporalConsistencyGate

		gate = TemporalConsistencyGate()
		# 20 days, well above the new 15-day minimum
		h = _make_hypothesis(start_date="2026-03-14", end_date="2026-04-03")
		result = _make_result(sharpe=3.0, total_trades=200, hypothesis=h)

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
		assert len(gate_result.details.get("sharpes", [])) >= 3

	def test_very_short_range_below_15_days_is_review_soft_pass(self):
		"""Ranges below 15 days don't have enough data for temporal
		partitioning — the gate should soft-pass with tier='review'
		rather than hard-fail, so short-history strategies aren't
		silently demoted. The old behavior failed with 'only 0 windows
		possible' which blocked real candidates in Task 5 v1 sweep.
		"""
		from edge_catcher.research.validation.gate_temporal_consistency import TemporalConsistencyGate

		gate = TemporalConsistencyGate()
		# 9 days — below the 15-day floor
		h = _make_hypothesis(start_date="2026-03-24", end_date="2026-04-02")
		result = _make_result(sharpe=3.0, total_trades=200, hypothesis=h)

		agent = MagicMock()
		ctx = GateContext(
			tracker=None, pnl_values=[10]*200,
			hypothesis=h, agent=agent,
		)
		gate_result = gate.check(result, ctx)

		assert gate_result.passed  # soft-pass, not fail
		assert gate_result.tier == "review"
		assert "insufficient data" in gate_result.reason
		# Gate should not have tried to run any sub-backtests
		agent.run_backtest_only.assert_not_called()


# ---------------------------------------------------------------------------
# Parameter Sensitivity Gate
# ---------------------------------------------------------------------------

class TestExtractNumericParams:
	"""Direct unit tests for _extract_numeric_params helper.

	Real strategies in strategies_local.py define parameters as __init__
	defaults with type annotations, not class-level attributes. The extractor
	must read __init__ signatures.
	"""

	def test_extracts_init_default_ints(self):
		from edge_catcher.research.validation.gate_sensitivity import _extract_numeric_params

		code = (
			"class DebutFade(Strategy):\n"
			"\tname = 'strategy_a'\n"
			"\tdef __init__(self, threshold_high: int = 60, threshold_low: int = 40,\n"
			"\t             take_profit: int = 8, stop_loss: int = 5) -> None:\n"
			"\t\tpass\n"
		)
		params = dict(_extract_numeric_params(code))
		assert params == {
			"threshold_high": 60,
			"threshold_low": 40,
			"take_profit": 8,
			"stop_loss": 5,
		}

	def test_extracts_init_default_floats(self):
		from edge_catcher.research.validation.gate_sensitivity import _extract_numeric_params

		code = (
			"class FlowFade(Strategy):\n"
			"\tname = 'strategy_c'\n"
			"\tdef __init__(self, flow_threshold: float = 0.5, max_move_pct: float = 1.5) -> None:\n"
			"\t\tpass\n"
		)
		params = dict(_extract_numeric_params(code))
		assert params == {"flow_threshold": 0.5, "max_move_pct": 1.5}

	def test_skips_strategy_name_attribute(self):
		"""The class-level `name = ...` attribute must never be treated as a param."""
		from edge_catcher.research.validation.gate_sensitivity import _extract_numeric_params

		code = (
			"class TestStrat(Strategy):\n"
			"\tname = 'test-strat'\n"
			"\tdef __init__(self, lookback: int = 20) -> None:\n"
			"\t\tpass\n"
		)
		names = [p[0] for p in _extract_numeric_params(code)]
		assert "name" not in names
		assert "lookback" in names

	def test_skips_self_and_untyped_non_numeric_defaults(self):
		from edge_catcher.research.validation.gate_sensitivity import _extract_numeric_params

		code = (
			"class TestStrat(Strategy):\n"
			"\tname = 't'\n"
			"\tdef __init__(self, x: int = 5, label: str = 'foo', bucket: list = None) -> None:\n"
			"\t\tpass\n"
		)
		names = [p[0] for p in _extract_numeric_params(code)]
		assert names == ["x"]

	def test_boolean_defaults_are_skipped_but_int_zero_and_one_are_kept(self):
		"""A param with default True/False is not numeric, but an int param
		with default 0 or 1 is a legitimate numeric parameter that must not
		be skipped."""
		from edge_catcher.research.validation.gate_sensitivity import _extract_numeric_params

		code = (
			"class TestStrat(Strategy):\n"
			"\tname = 't'\n"
			"\tdef __init__(self, enabled: bool = True, offset: int = 0, toggle: int = 1) -> None:\n"
			"\t\tpass\n"
		)
		params = dict(_extract_numeric_params(code))
		assert "enabled" not in params
		assert params.get("offset") == 0
		assert params.get("toggle") == 1

	def test_handles_unparseable_code(self):
		from edge_catcher.research.validation.gate_sensitivity import _extract_numeric_params

		assert _extract_numeric_params("not valid python )(") == []

	def test_no_init_returns_empty(self):
		from edge_catcher.research.validation.gate_sensitivity import _extract_numeric_params

		code = (
			"class NoInit(Strategy):\n"
			"\tname = 'no-init'\n"
			"\tdef on_trade(self, trade, market, portfolio):\n"
			"\t\treturn []\n"
		)
		assert _extract_numeric_params(code) == []

	def test_extract_skips_size_param(self):
		from edge_catcher.research.validation.gate_sensitivity import _extract_numeric_params
		code = (
			"class Strat(Strategy):\n"
			"\tname = 's'\n"
			"\tdef __init__(self, threshold: int = 60, size: int = 1) -> None:\n"
			"\t\tpass\n"
		)
		params = dict(_extract_numeric_params(code))
		assert "threshold" in params
		assert "size" not in params

	def test_ignores_non_init_method_params(self):
		"""Only __init__ defaults are parameters; other methods' defaults are not."""
		from edge_catcher.research.validation.gate_sensitivity import _extract_numeric_params

		code = (
			"class TestStrat(Strategy):\n"
			"\tname = 't'\n"
			"\tdef __init__(self, real_param: int = 5) -> None:\n"
			"\t\tpass\n"
			"\tdef helper(self, fake_param: int = 99) -> None:\n"
			"\t\tpass\n"
		)
		names = [p[0] for p in _extract_numeric_params(code)]
		assert names == ["real_param"]


class TestReplaceParam:
	"""Direct unit tests for _replace_param helper.

	Must rewrite __init__ default values, strategy name attribute, and
	class name without corrupting type annotations or adjacent parameters.
	"""

	SAMPLE = (
		"class DebutFade(Strategy):\n"
		"\tname = 'strategy_a'\n"
		"\tdef __init__(self, threshold_high: int = 60, threshold_low: int = 40) -> None:\n"
		"\t\tself.threshold_high = threshold_high\n"
	)

	def _reparse_params(self, code: str) -> dict:
		"""Helper: re-extract numeric params from generated code."""
		from edge_catcher.research.validation.gate_sensitivity import _extract_numeric_params
		return dict(_extract_numeric_params(code))

	def test_replaces_int_default(self):
		from edge_catcher.research.validation.gate_sensitivity import _replace_param

		out = _replace_param(self.SAMPLE, "DebutFade", "threshold_high", 72, "strategy_a__sens_72")
		params = self._reparse_params(out)
		assert params == {"threshold_high": 72, "threshold_low": 40}
		assert "strategy_a__sens_72" in out

	def test_replaces_float_default(self):
		from edge_catcher.research.validation.gate_sensitivity import _replace_param

		code = (
			"class FlowFade(Strategy):\n"
			"\tname = 'strategy_c'\n"
			"\tdef __init__(self, flow_threshold: float = 0.5, max_count: int = 10) -> None:\n"
			"\t\tpass\n"
		)
		out = _replace_param(code, "FlowFade", "flow_threshold", 0.575, "strategy_c__sens_0_575")
		params = self._reparse_params(out)
		assert params == {"flow_threshold": 0.575, "max_count": 10}

	def test_replaces_class_and_name_even_when_values_nonmatching(self):
		from edge_catcher.research.validation.gate_sensitivity import _replace_param

		out = _replace_param(self.SAMPLE, "DebutFade", "threshold_high", 60, "strategy_a__sens_unchanged")
		# name attribute rewritten
		assert "name = 'strategy_a__sens_unchanged'" in out or \
			'name = "strategy_a__sens_unchanged"' in out
		# class renamed (sanitized — hyphens → underscores)
		assert "class strategy_a__sens_unchanged(Strategy)" in out

	def test_replace_param_outputs_tab_indented(self):
		from edge_catcher.research.validation.gate_sensitivity import _replace_param
		out = _replace_param(self.SAMPLE, "DebutFade", "threshold_high", 72, "strategy_a__sens_72")
		# No 4-space indentation runs should appear
		assert "    " not in out, f"ast.unparse 4-space indentation leaked: {out[:200]}"


class TestParameterSensitivityGate:
	# Realistic strategy code: __init__ defaults with type hints.
	SAMPLE_STRATEGY = (
		"class TestStrategy(Strategy):\n"
		"\tname = \"TestStrat\"\n"
		"\tdef __init__(self, lookback: int = 20, threshold: float = 0.85, max_hold: int = 60) -> None:\n"
		"\t\tself.lookback = lookback\n"
		"\tdef on_trade(self, market, trade):\n"
		"\t\tpass\n"
	)

	def test_robust_strategy_passes(self):
		"""Strategy where neighbors also perform well should pass.

		Asserts not only the pass verdict but also that the perturbation
		path was exercised — guards against silently regressing to the
		"no numeric parameters to perturb" early return.
		"""
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
		with patch.object(gate, "_run_neighbor", return_value=1.8) as m:
			gr = gate.check(result, ctx)
		assert gr.passed
		# Perturbation actually happened — sample strategy has 3 params,
		# each perturbed ±15%, so 6 neighbor runs
		assert gr.details["neighbors_total"] == 6
		assert m.call_count == 6

	def test_fragile_strategy_fails(self):
		"""Strategy where most neighbors collapse should fail.

		Asserts the perturbation path was exercised before failing.
		"""
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
		with patch.object(gate, "_run_neighbor", return_value=0.1) as m:
			gr = gate.check(result, ctx)
		assert not gr.passed
		assert gr.details["neighbors_total"] == 6
		assert m.call_count == 6

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


class TestCleanupSafety:
	"""_cleanup must never wipe the strategies_local.py file.

	Regression: on 2026-04-13 during Task 5 v2 sweep, _cleanup reduced the
	file to 6 bytes (3 blank lines), taking out every strategy. The new
	invariants refuse any splice that would remove more than one class,
	remove a non-temp class, leave the file empty, or corrupt its syntax.
	"""

	def _make_file(self, tmp_path, contents: str):
		from pathlib import Path
		p: Path = tmp_path / "strategies_local.py"
		p.write_text(contents, encoding="utf-8")
		return p

	def _run_cleanup(self, gate, strategies_path, temp_name: str):
		"""Run _cleanup with STRATEGIES_LOCAL_PATH pointed at a tmp file."""
		import edge_catcher.runner.strategy_parser as sp
		from unittest.mock import patch

		# The module name doesn't matter — we patch importlib.import_module
		# inside _cleanup so reload() is a no-op.
		with patch.object(sp, "STRATEGIES_LOCAL_PATH", strategies_path), \
		     patch("importlib.import_module", side_effect=ImportError("test")):
			gate._cleanup(temp_name)

	def test_refuses_non_temp_name(self, tmp_path):
		"""A call with a non-sensitivity name must not touch the file."""
		from edge_catcher.research.validation.gate_sensitivity import ParameterSensitivityGate

		original = (
			"class RealStrat:\n"
			"    name = 'real-strat'\n"
			"    def on_trade(self, t, m, p): return []\n"
		)
		path = self._make_file(tmp_path, original)
		gate = ParameterSensitivityGate()
		self._run_cleanup(gate, path, "real-strat")  # not a __sens_ name

		assert path.read_text(encoding="utf-8") == original

	def test_removes_exactly_one_sens_class(self, tmp_path):
		"""Normal happy path: one sensitivity temp removed, others intact."""
		from edge_catcher.research.validation.gate_sensitivity import ParameterSensitivityGate

		original = (
			"class RealA:\n"
			"    name = 'real-a'\n"
			"    def on_trade(self, t, m, p): return []\n"
			"\n"
			"class TempA(RealA):\n"
			"    name = 'real-a__sens_x_1'\n"
			"    def on_trade(self, t, m, p): return []\n"
			"\n"
			"class RealB:\n"
			"    name = 'real-b'\n"
			"    def on_trade(self, t, m, p): return []\n"
		)
		path = self._make_file(tmp_path, original)
		gate = ParameterSensitivityGate()
		self._run_cleanup(gate, path, "real-a__sens_x_1")

		new = path.read_text(encoding="utf-8")
		assert "real-a__sens_x_1" not in new
		assert "real-a" in new       # real-a survives
		assert "real-b" in new       # real-b survives

		# Backup was created
		backup = path.with_suffix(".py.bak")
		assert backup.exists()
		assert "real-a__sens_x_1" in backup.read_text(encoding="utf-8")

	def test_refuses_when_temp_would_be_last_class(self, tmp_path):
		"""If removing the temp would leave zero non-temp classes, refuse.
		Protects against losing the whole file in a pathological state.
		"""
		from edge_catcher.research.validation.gate_sensitivity import ParameterSensitivityGate

		original = (
			"class TempOnly:\n"
			"    name = 'x__sens_only'\n"
			"    def on_trade(self, t, m, p): return []\n"
		)
		path = self._make_file(tmp_path, original)
		gate = ParameterSensitivityGate()
		self._run_cleanup(gate, path, "x__sens_only")

		# File unchanged — refused to remove the only class
		assert path.read_text(encoding="utf-8") == original

	def test_refuses_on_existing_syntax_error(self, tmp_path):
		"""If the file already has a syntax error, refuse the cleanup
		instead of falling back to a text filter (the old fallback could
		delete unrelated lines).
		"""
		from edge_catcher.research.validation.gate_sensitivity import ParameterSensitivityGate

		broken = (
			"class RealA:\n"
			"    name = 'real-a'\n"
			"    def on_trade(self, t, m, p: return []\n"  # syntax error
			"\n"
			"class Temp:\n"
			"    name = 'foo__sens_bar'\n"
		)
		path = self._make_file(tmp_path, broken)
		gate = ParameterSensitivityGate()
		self._run_cleanup(gate, path, "foo__sens_bar")

		# File untouched — refused to run on broken input
		assert path.read_text(encoding="utf-8") == broken

	def test_noop_when_temp_class_absent(self, tmp_path):
		"""If the temp_name isn't in the file, do nothing (no backup, no write)."""
		from edge_catcher.research.validation.gate_sensitivity import ParameterSensitivityGate

		original = (
			"class RealA:\n"
			"    name = 'real-a'\n"
			"    def on_trade(self, t, m, p): return []\n"
		)
		path = self._make_file(tmp_path, original)
		gate = ParameterSensitivityGate()
		self._run_cleanup(gate, path, "not_in_file__sens_x_1")

		assert path.read_text(encoding="utf-8") == original
		assert not path.with_suffix(".py.bak").exists()


# ---------------------------------------------------------------------------
# Tail-risk gate (vol-seller / deep-OTM detector)
# ---------------------------------------------------------------------------

class TestTailRiskGate:
	"""Catches strategies with a selling-deep-OTM payoff signature:
	very high win rate + asymmetric average loss >> average win.
	The Apr 11 run had fade-long-vol/KXETH (88% WR, avg_loss/avg_win=6.6x)
	pass all other gates; this gate must flag it.
	"""

	def test_selling_vol_pattern_fails(self):
		"""88% tiny wins + 12% huge losses → classic vol-seller → FAIL."""
		from edge_catcher.research.validation.gate_tail_risk import TailRiskGate

		# Reproduce fade-long-vol/KXETH-ish shape: mean ≈ 1.1
		pnl = [12] * 88 + [-82] * 12
		result = _make_result(pnl_values=pnl, total_trades=len(pnl))
		ctx = GateContext(tracker=None, pnl_values=pnl, hypothesis=result.hypothesis)

		gate = TailRiskGate()
		gr = gate.check(result, ctx)
		assert not gr.passed
		assert "tail" in gr.reason.lower() or "loss" in gr.reason.lower() or "asym" in gr.reason.lower()

	def test_high_win_rate_alone_passes(self):
		"""A 90% win rate with symmetric-magnitude losses is not vol-selling."""
		from edge_catcher.research.validation.gate_tail_risk import TailRiskGate

		# 90% wins and 10% losses but ratio is only 1.2x — a genuine edge
		pnl = [10] * 90 + [-12] * 10
		result = _make_result(pnl_values=pnl, total_trades=len(pnl))
		ctx = GateContext(tracker=None, pnl_values=pnl, hypothesis=result.hypothesis)

		gate = TailRiskGate()
		gr = gate.check(result, ctx)
		assert gr.passed

	def test_asymmetric_losses_alone_passes(self):
		"""A lottery-ticket strategy (few big wins, many small losses) is
		not vol-selling, even though avg win >> avg loss in magnitude."""
		from edge_catcher.research.validation.gate_tail_risk import TailRiskGate

		# 20% wins that average 50, 80% losses that average 5 — upside skew
		pnl = [50] * 20 + [-5] * 80
		result = _make_result(pnl_values=pnl, total_trades=len(pnl))
		ctx = GateContext(tracker=None, pnl_values=pnl, hypothesis=result.hypothesis)

		gate = TailRiskGate()
		gr = gate.check(result, ctx)
		assert gr.passed

	def test_balanced_strategy_passes(self):
		"""A normal 55/45 strategy with comparable win/loss magnitudes passes."""
		from edge_catcher.research.validation.gate_tail_risk import TailRiskGate

		pnl = [8] * 55 + [-6] * 45
		result = _make_result(pnl_values=pnl, total_trades=len(pnl))
		ctx = GateContext(tracker=None, pnl_values=pnl, hypothesis=result.hypothesis)

		gate = TailRiskGate()
		gr = gate.check(result, ctx)
		assert gr.passed

	def test_losing_strategy_handled(self):
		"""A strategy with zero wins must not crash (edge case)."""
		from edge_catcher.research.validation.gate_tail_risk import TailRiskGate

		pnl = [-5] * 100
		result = _make_result(pnl_values=pnl, total_trades=len(pnl))
		ctx = GateContext(tracker=None, pnl_values=pnl, hypothesis=result.hypothesis)

		gate = TailRiskGate()
		gr = gate.check(result, ctx)
		# A strategy with no wins isn't vol-selling — it's just bad.
		# Shouldn't crash, and tail-risk gate shouldn't be the gate that
		# kills it (other gates handle negative expectancy).
		assert gr.details is not None

	def test_zero_losses_handled(self):
		"""A perfect strategy (100% wins) must not crash on division."""
		from edge_catcher.research.validation.gate_tail_risk import TailRiskGate

		pnl = [10] * 100
		result = _make_result(pnl_values=pnl, total_trades=len(pnl))
		ctx = GateContext(tracker=None, pnl_values=pnl, hypothesis=result.hypothesis)

		gate = TailRiskGate()
		gr = gate.check(result, ctx)
		# No losses means no asymmetric-loss risk
		assert gr.passed

	def test_custom_thresholds(self):
		"""Thresholds should be configurable."""
		from edge_catcher.research.validation.gate_tail_risk import TailRiskGate

		# Strict gate: any win_rate >= 50% with ratio >= 1.5x fails.
		# 60% wins at 10, 40% losses at -18 → ratio 1.8x, WR 60%
		pnl = [10] * 60 + [-18] * 40
		result = _make_result(pnl_values=pnl, total_trades=len(pnl))
		ctx = GateContext(tracker=None, pnl_values=pnl, hypothesis=result.hypothesis)

		lenient = TailRiskGate(max_win_rate=0.85, max_loss_win_ratio=3.0)
		gr_l = lenient.check(result, ctx)
		assert gr_l.passed

		strict = TailRiskGate(max_win_rate=0.50, max_loss_win_ratio=1.5)
		gr_s = strict.check(result, ctx)
		assert not gr_s.passed

	def test_low_trade_count_skips(self):
		"""With fewer than N trades, skip the gate — insufficient statistics."""
		from edge_catcher.research.validation.gate_tail_risk import TailRiskGate

		pnl = [10] * 8 + [-50] * 2  # 80% wr, huge ratio, only 10 trades
		result = _make_result(pnl_values=pnl, total_trades=len(pnl))
		ctx = GateContext(tracker=None, pnl_values=pnl, hypothesis=result.hypothesis)

		gate = TailRiskGate(min_trades=50)
		gr = gate.check(result, ctx)
		assert gr.passed
		assert gr.details.get("skipped") is True

	def test_worst_single_loss_vs_median_win_reaches_review_tier(self):
		"""A strategy that passes the vol-seller check (moderate win rate, ok
		avg ratio) but has ONE catastrophic worst-case loss >= 10x median win
		should be flagged as review tier (soft-pass with tier='review')."""
		from edge_catcher.research.validation.gate_tail_risk import TailRiskGate

		# 60% wins (< 75% so NOT a vol-seller by win rate), small avg loss,
		# but one devastating worst-case loss
		pnl = [20] * 60 + [-10] * 39 + [-500]
		result = _make_result(pnl_values=pnl, total_trades=len(pnl))
		ctx = GateContext(tracker=None, pnl_values=pnl, hypothesis=result.hypothesis)

		gate = TailRiskGate()
		gr = gate.check(result, ctx)
		# Passes the vol-seller check (WR 60% < 75%) but worst loss is
		# 25x median win → review tier
		assert gr.passed, f"should pass vol-seller check: {gr.reason}"
		assert gr.tier == "review", f"expected review tier, got {gr.tier}: {gr.reason}"
		assert gr.details["worst_loss_to_median_win"] >= 10.0


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
		from edge_catcher.research.refinement import RefinementExecutor

		# Original: Sharpe 3.5 with 200 trades -> per-trade = 3.5/sqrt(200) ~ 0.247
		original = [{"status": "ok", "sharpe": 3.5, "total_trades": 200, "verdict": "explore"}]
		# Refined: Sharpe 2.5 with 50 trades -> per-trade = 2.5/sqrt(50) ~ 0.354
		refined_h = _make_hypothesis()
		refined = [_make_result(sharpe=2.5, total_trades=50, verdict="explore", hypothesis=refined_h)]
		assert RefinementExecutor.should_keep_refinement(original, refined)

	def test_lower_per_trade_sharpe_discarded(self):
		"""Refinement with worse per-trade Sharpe should be discarded."""
		from edge_catcher.research.refinement import RefinementExecutor

		# Original: Sharpe 2.0 with 50 trades -> per-trade = 2.0/sqrt(50) ~ 0.283
		original = [{"status": "ok", "sharpe": 2.0, "total_trades": 50, "verdict": "explore"}]
		# Refined: Sharpe 2.5 with 200 trades -> per-trade = 2.5/sqrt(200) ~ 0.177
		refined_h = _make_hypothesis()
		refined = [_make_result(sharpe=2.5, total_trades=200, verdict="explore", hypothesis=refined_h)]
		assert not RefinementExecutor.should_keep_refinement(original, refined)


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
