"""Regression tests for the research loop CLI date defaulting.

LoopOrchestrator + GridPlanner require concrete ISO date strings; the CLI
previously documented `--start` / `--end` as defaulting to None ("all data")
but None reached loop.py:53 and raised ValueError. _resolve_loop_dates now
fills in a rolling 1-year window when either flag is omitted.
"""

from __future__ import annotations

from datetime import date, timedelta

from edge_catcher.cli.research import _resolve_loop_dates


def test_both_none_resolves_to_rolling_year():
	start, end = _resolve_loop_dates(None, None)
	today = date.today()
	assert end == today.isoformat()
	assert start == (today - timedelta(days=365)).isoformat()


def test_explicit_start_preserved():
	start, end = _resolve_loop_dates("2025-06-01", None)
	assert start == "2025-06-01"
	assert end == date.today().isoformat()


def test_explicit_end_preserved():
	start, end = _resolve_loop_dates(None, "2026-03-15")
	assert start == (date.today() - timedelta(days=365)).isoformat()
	assert end == "2026-03-15"


def test_both_explicit_pass_through():
	start, end = _resolve_loop_dates("2025-01-01", "2026-05-03")
	assert start == "2025-01-01"
	assert end == "2026-05-03"
