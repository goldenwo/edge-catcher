# tests/test_agent.py
"""Tests for edge_catcher.research.agent module."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from edge_catcher.research.agent import ResearchAgent
from edge_catcher.research.data_source_config import make_ds
from edge_catcher.research.hypothesis import Hypothesis


def _mock_resolver():
	mock_resolved = MagicMock()
	mock_resolved.ohlc_config = None
	mock_resolver = MagicMock()
	mock_resolver.resolve.return_value = mock_resolved
	return mock_resolver


def test_run_backtest_only_passes_slippage_when_set():
	"""Hypothesis with slippage_cents=6 must forward --slippage 6 to the subprocess."""
	h = Hypothesis(
		strategy="strategy_a",
		data_sources=make_ds(db="kalshi.db", series="X"),
		start_date="2025-01-01",
		end_date="2025-12-31",
		fee_pct=1.0,
		slippage_cents=6,
	)
	agent = ResearchAgent.__new__(ResearchAgent)  # bypass init

	with patch("edge_catcher.research.agent._get_resolver", return_value=_mock_resolver()), \
	     patch("subprocess.run") as mock_run:
		mock_run.return_value = MagicMock(
			stdout='{"status":"ok","total_trades":0,"pnl_values":[]}',
			returncode=0,
			stderr="",
		)
		agent.run_backtest_only(h)
		cmd = mock_run.call_args[0][0]
		assert "--slippage" in cmd
		slippage_idx = cmd.index("--slippage")
		assert cmd[slippage_idx + 1] == "6"


def test_run_backtest_only_omits_slippage_when_none():
	"""Hypothesis with slippage_cents=None must NOT add --slippage to the cmd."""
	h = Hypothesis(
		strategy="strategy_a",
		data_sources=make_ds(db="kalshi.db", series="X"),
		start_date="2025-01-01",
		end_date="2025-12-31",
		fee_pct=1.0,
	)
	agent = ResearchAgent.__new__(ResearchAgent)

	with patch("edge_catcher.research.agent._get_resolver", return_value=_mock_resolver()), \
	     patch("subprocess.run") as mock_run:
		mock_run.return_value = MagicMock(
			stdout='{"status":"ok","total_trades":0,"pnl_values":[]}',
			returncode=0,
			stderr="",
		)
		agent.run_backtest_only(h)
		cmd = mock_run.call_args[0][0]
		assert "--slippage" not in cmd


def test_run_hypothesis_passes_slippage_when_set():
	"""run_hypothesis must also forward --slippage when slippage_cents is set."""
	h = Hypothesis(
		strategy="strategy_a",
		data_sources=make_ds(db="kalshi.db", series="X"),
		start_date="2025-01-01",
		end_date="2025-12-31",
		fee_pct=1.0,
		slippage_cents=8,
	)
	agent = ResearchAgent.__new__(ResearchAgent)
	agent.force = True  # skip dedup check
	agent.tracker = MagicMock()
	agent.tracker.is_tested.return_value = None
	agent.evaluator = MagicMock()
	agent.evaluator.evaluate.return_value = ("kill", "low trades")
	agent.thresholds = MagicMock()

	with patch("edge_catcher.research.agent._get_resolver", return_value=_mock_resolver()), \
	     patch("subprocess.run") as mock_run:
		mock_run.return_value = MagicMock(
			stdout='{"status":"ok","total_trades":0,"pnl_values":[]}',
			returncode=0,
			stderr="",
		)
		agent.run_hypothesis(h)
		cmd = mock_run.call_args[0][0]
		assert "--slippage" in cmd
		slippage_idx = cmd.index("--slippage")
		assert cmd[slippage_idx + 1] == "8"


def test_default_gates_forwards_sweep_n_override():
	"""Smoke test: sweep_N_override reaches DeflatedSharpeGate via default_gates().

	This catches regressions where a parameter is added to the outer API but
	not plumbed through to the gate constructor.
	"""
	from edge_catcher.research.validation.gate_dsr import DeflatedSharpeGate
	from edge_catcher.research.validation.pipeline import default_gates

	gates = default_gates(sweep_N_override=500)
	dsr_gates = [g for g in gates if isinstance(g, DeflatedSharpeGate)]
	assert len(dsr_gates) == 1
	assert dsr_gates[0].sweep_N_override == 500

	# Default (no override) should yield None, preserving live-tracker behavior
	# for non-grid phases (LLM, expand, refine).
	default = default_gates()
	default_dsr = [g for g in default if isinstance(g, DeflatedSharpeGate)][0]
	assert default_dsr.sweep_N_override is None
