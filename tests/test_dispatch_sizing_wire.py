"""Tests for sizing-wire dispatch helpers (Task C1+).

C1: _inc_gate_metric — translates a GateDecision into a Metrics counter
increment. The gate holds no Metrics handle (spec §4.2); dispatch is the
translation layer.
"""
from __future__ import annotations

import typing

from edge_catcher.engine.metrics import Metrics
from edge_catcher.engine.risk import Allow, Reject, GateRejectReason, SizingBreakdown
from edge_catcher.engine.dispatch import _inc_gate_metric


def _bd() -> SizingBreakdown:
	"""Minimal SizingBreakdown for Allow construction."""
	return SizingBreakdown(
		fixed_fraction_contracts=5,
		quarter_kelly_contracts=2**31,  # sentinel: no edge config
		absolute_max_contracts=10,
		bound_by="fixed_fraction",
	)


def test_inc_gate_metric_allow_and_every_reject_reason() -> None:
	m = Metrics()
	_inc_gate_metric(m, Allow(size_contracts=5, sizing_breakdown=_bd()))
	assert m.snapshot()["risk_gate_allowed"] == 1

	for reason in typing.get_args(GateRejectReason):
		_inc_gate_metric(m, Reject(reason=reason, detail="x"))  # must NEVER raise KeyError
