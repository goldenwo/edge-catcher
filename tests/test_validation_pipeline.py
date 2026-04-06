"""Tests for the validation pipeline and gate interface."""

from __future__ import annotations

import pytest

from edge_catcher.research.hypothesis import Hypothesis, HypothesisResult
from edge_catcher.research.validation.gate import Gate, GateContext, GateResult
from edge_catcher.research.validation.pipeline import ValidationPipeline


def _make_result(sharpe=2.5, total_trades=100, **kwargs) -> HypothesisResult:
	h = Hypothesis(
		strategy="C", series="KXBTCD", db_path="data/kalshi.db",
		start_date="2025-01-01", end_date="2025-12-31",
	)
	defaults = dict(
		hypothesis=h, status="ok", total_trades=total_trades,
		wins=90, losses=10, win_rate=0.90, net_pnl_cents=500.0,
		sharpe=sharpe, max_drawdown_pct=5.0, fees_paid_cents=100.0,
		avg_win_cents=10.0, avg_loss_cents=-5.0, per_strategy={},
		verdict="candidate", verdict_reason="", raw_json={},
	)
	defaults.update(kwargs)
	return HypothesisResult(**defaults)


class _PassGate(Gate):
	name = "always_pass"
	def check(self, result, context):
		return GateResult(passed=True, gate_name=self.name, reason="ok", details={})


class _FailGate(Gate):
	name = "always_fail"
	def check(self, result, context):
		return GateResult(passed=False, gate_name=self.name, reason="nope", details={})


class TestValidationPipeline:
	def test_all_gates_pass_promotes(self):
		pipeline = ValidationPipeline([_PassGate(), _PassGate()])
		ctx = GateContext(tracker=None, pnl_values=[1, 2, 3], hypothesis=_make_result().hypothesis)
		verdict, reason, gate_results = pipeline.validate(_make_result(), ctx)
		assert verdict == "promote"
		assert len(gate_results) == 2
		assert all(g.passed for g in gate_results)

	def test_first_gate_fails_short_circuits(self):
		pipeline = ValidationPipeline([_FailGate(), _PassGate()])
		ctx = GateContext(tracker=None, pnl_values=[1, 2, 3], hypothesis=_make_result().hypothesis)
		verdict, reason, gate_results = pipeline.validate(_make_result(), ctx)
		assert verdict == "explore"
		assert len(gate_results) == 1  # short-circuited, second gate not run
		assert not gate_results[0].passed

	def test_second_gate_fails(self):
		pipeline = ValidationPipeline([_PassGate(), _FailGate()])
		ctx = GateContext(tracker=None, pnl_values=[1, 2, 3], hypothesis=_make_result().hypothesis)
		verdict, reason, gate_results = pipeline.validate(_make_result(), ctx)
		assert verdict == "explore"
		assert len(gate_results) == 2
		assert gate_results[0].passed
		assert not gate_results[1].passed

	def test_empty_pipeline_promotes(self):
		pipeline = ValidationPipeline([])
		ctx = GateContext(tracker=None, pnl_values=[], hypothesis=_make_result().hypothesis)
		verdict, reason, gate_results = pipeline.validate(_make_result(), ctx)
		assert verdict == "promote"
		assert gate_results == []


class TestPipelineWithRealGates:
	def test_default_gates_instantiate(self):
		"""default_gates() should return 4 gates in the correct order."""
		from edge_catcher.research.validation.pipeline import default_gates
		gates = default_gates()
		assert len(gates) == 4
		assert gates[0].name == "deflated_sharpe"
		assert gates[1].name == "monte_carlo"
		assert gates[2].name == "walk_forward"
		assert gates[3].name == "param_sensitivity"
