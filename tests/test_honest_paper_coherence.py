"""_assert_mode_coherence validates paper_fill_model + honest_paper block (spec §4.6)."""
from __future__ import annotations

import pytest

from edge_catcher.engine.engine import _assert_mode_coherence


def _paper_base() -> dict:
	# Minimal paper config that passes the existing coherence checks.
	return {"executor": "paper", "db_path": "data/paper_trades.db"}


def test_unknown_paper_fill_model_fails_boot():
	cfg = {**_paper_base(), "paper_fill_model": "bogus"}
	with pytest.raises(RuntimeError, match="paper_fill_model"):
		_assert_mode_coherence(cfg)


def test_fixed_without_honest_paper_block_fails_boot():
	cfg = {**_paper_base(), "paper_fill_model": "fixed"}
	with pytest.raises(RuntimeError, match="honest_paper"):
		_assert_mode_coherence(cfg)


def test_fixed_with_malformed_block_fails_boot():
	cfg = {**_paper_base(), "paper_fill_model": "fixed",
	       "honest_paper": {"default_slippage_cents": "two", "per_strategy": {}}}
	with pytest.raises(RuntimeError, match="honest_paper"):
		_assert_mode_coherence(cfg)


def test_fixed_with_bool_slippage_fails_boot():
	# yaml `true` parses to a bool, an int subclass — must be rejected (spec §4.6
	# footgun) so `default_slippage_cents: true` never silently becomes a 1c penalty.
	cfg = {**_paper_base(), "paper_fill_model": "fixed",
	       "honest_paper": {"default_slippage_cents": True, "per_strategy": {}}}
	with pytest.raises(RuntimeError, match="honest_paper"):
		_assert_mode_coherence(cfg)


def test_fixed_with_unknown_per_strategy_key_fails_boot(monkeypatch):
	# Force a known-strategy set so the test is independent of strategies_local.py.
	import edge_catcher.engine.engine as eng

	class _S:
		def __init__(self, name): self.name = name

	monkeypatch.setattr(eng, "discover_strategies", lambda: [_S("debut_fade")])
	cfg = {**_paper_base(), "paper_fill_model": "fixed",
	       "honest_paper": {"default_slippage_cents": 2, "per_strategy": {"nonexistent": 5}}}
	with pytest.raises(RuntimeError, match="per_strategy|honest_paper"):
		_assert_mode_coherence(cfg)


def test_fixed_with_non_int_per_strategy_value_fails_boot(monkeypatch):
	# per_strategy VALUES must be non-bool ints. Monkeypatch so the key is KNOWN,
	# isolating the value check from the unknown-key check: a str override would
	# TypeError and a float would silently corrupt cents at fill time.
	import edge_catcher.engine.engine as eng

	class _S:
		def __init__(self, name): self.name = name

	monkeypatch.setattr(eng, "discover_strategies", lambda: [_S("debut_fade")])
	for bad in ("five", 1.5, True):
		hp = {"default_slippage_cents": 2, "per_strategy": {"debut_fade": bad}}
		cfg = {**_paper_base(), "paper_fill_model": "fixed", "honest_paper": hp}
		with pytest.raises(RuntimeError, match="per_strategy"):
			_assert_mode_coherence(cfg)


def test_fixed_with_valid_config_passes(monkeypatch):
	import edge_catcher.engine.engine as eng

	class _S:
		def __init__(self, name): self.name = name

	monkeypatch.setattr(eng, "discover_strategies", lambda: [_S("debut_fade")])
	cfg = {**_paper_base(), "paper_fill_model": "fixed",
	       "honest_paper": {"default_slippage_cents": 2, "per_strategy": {"debut_fade": 5}}}
	_assert_mode_coherence(cfg)  # must not raise


def test_optimistic_default_skips_honest_paper_checks():
	cfg = {**_paper_base()}  # no paper_fill_model, no honest_paper
	_assert_mode_coherence(cfg)  # must not raise (byte-unchanged default path)
