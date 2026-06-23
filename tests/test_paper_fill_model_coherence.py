"""Post-PR#57 paper_fill_model boot-coherence gate.

Replaces the regression guard from the deleted tests/test_honest_paper_coherence.py.
The 'fixed' slippage model was retired 2026-06-23; honest fills are now the
replay-only fill_latency_ms. The boot gate (_assert_mode_coherence) must accept
ONLY 'optimistic' (or an absent key) and reject everything else — it is a
money-path safety net, so it stays tested.
"""
from __future__ import annotations

import pytest

from edge_catcher.engine.engine import _assert_mode_coherence


def _paper_base() -> dict:
	# Minimal paper config that passes the other coherence checks (paper mode
	# early-returns after the fill-model check, skipping the live-only gates).
	return {"executor": "paper", "db_path": "data/paper_trades.db"}


def test_optimistic_paper_fill_model_accepted() -> None:
	# Explicit 'optimistic' and the absent-key default both pass (happy path,
	# byte-unchanged paper boot).
	_assert_mode_coherence({**_paper_base(), "paper_fill_model": "optimistic"})
	_assert_mode_coherence(_paper_base())  # absent -> defaults to optimistic


def test_retired_fixed_paper_fill_model_rejected() -> None:
	# The retired 'fixed' model must fail boot and point at the replacement so an
	# upgrading operator gets an actionable error, not a silent behavior change.
	cfg = {**_paper_base(), "paper_fill_model": "fixed"}
	with pytest.raises(RuntimeError, match="fill_latency_ms"):
		_assert_mode_coherence(cfg)


def test_unknown_paper_fill_model_fails_boot() -> None:
	# Regression guard preserved from the deleted test_honest_paper_coherence.py:
	# any non-'optimistic' value aborts boot.
	cfg = {**_paper_base(), "paper_fill_model": "bogus"}
	with pytest.raises(RuntimeError, match="paper_fill_model"):
		_assert_mode_coherence(cfg)
