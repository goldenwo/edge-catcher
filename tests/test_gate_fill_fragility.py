"""Tests for the soft fill-fragility gate."""

from edge_catcher.research.data_source_config import make_ds
from edge_catcher.research.hypothesis import Hypothesis, HypothesisResult
from edge_catcher.research.validation import gate_fill_fragility as gff
from edge_catcher.research.validation.gate import GateContext
from edge_catcher.research.validation.gate_fill_fragility import FillFragilityGate
from edge_catcher.research.validation.pipeline import default_gates


def _ctx(strategy: str):
	h = Hypothesis(strategy=strategy, data_sources=make_ds(db="x.db", series="S1"))
	result = HypothesisResult.error(h, "n/a")  # gate ignores result fields
	ctx = GateContext(tracker=None, pnl_values=[1.0] * 60, hypothesis=h)
	return result, ctx


def test_fragile_archetype_flags_review(monkeypatch):
	monkeypatch.setattr(gff, "resolve_execution_archetype", lambda name: "taker_synthetic")
	result, ctx = _ctx("frag")
	gr = FillFragilityGate().check(result, ctx)
	assert gr.passed is True          # NEVER fails the candidate
	assert gr.tier == "review"
	assert gr.details["execution_archetype"] == "taker_synthetic"


def test_unknown_archetype_flags_review(monkeypatch):
	monkeypatch.setattr(gff, "resolve_execution_archetype", lambda name: "unknown")
	result, ctx = _ctx("mystery")
	gr = FillFragilityGate().check(result, ctx)
	assert gr.passed is True
	assert gr.tier == "review"


def test_robust_archetype_passes_clean(monkeypatch):
	monkeypatch.setattr(gff, "resolve_execution_archetype", lambda name: "maker")
	result, ctx = _ctx("mk")
	gr = FillFragilityGate().check(result, ctx)
	assert gr.passed is True
	assert gr.tier is None
	assert gr.details["execution_archetype"] == "maker"


def test_gate_registered_in_default_pipeline():
	assert any(g.name == "fill_fragility" for g in default_gates())
