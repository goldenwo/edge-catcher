"""Tests for edge_catcher.research.agent — dedup and warning behaviour."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest

from edge_catcher.research.agent import ResearchAgent
from edge_catcher.research.data_source_config import make_ds
from edge_catcher.research.hypothesis import Hypothesis


def test_run_hypothesis_no_false_warning_on_new(monkeypatch, caplog):
	"""When hypothesis is genuinely new, no 'Existing record' warning should appear."""
	tracker = MagicMock()
	tracker.is_tested.return_value = None  # not tested before

	agent = ResearchAgent(tracker=tracker, force=False)

	h = Hypothesis(strategy="A", data_sources=make_ds(db="test.db", series="KXBTCD"),
	               start_date="2025-01-01", end_date="2025-12-31")

	# Mock subprocess to return valid JSON
	fake_proc = MagicMock()
	fake_proc.stdout = '{"status":"ok","total_trades":100,"wins":60,"losses":40,"win_rate":0.6,"net_pnl_cents":500,"sharpe":1.5,"max_drawdown_pct":5.0,"total_fees_paid":100,"avg_win_cents":20,"avg_loss_cents":-10,"per_strategy":{},"pnl_values":[]}'
	fake_proc.stderr = ""

	with patch("subprocess.run", return_value=fake_proc), \
	     caplog.at_level(logging.WARNING, logger="edge_catcher.research.agent"):
		agent.run_hypothesis(h)

	assert "Existing record" not in caplog.text
