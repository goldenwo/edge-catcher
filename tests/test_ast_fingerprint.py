"""Tests for AST fingerprinting and code hash dedup."""

import pytest

from edge_catcher.runner.strategy_parser import compute_code_hash, compute_ast_fingerprint
from edge_catcher.research.tracker import Tracker


STRATEGY_A = '''
class MomentumFlip:
	name = "momentum_flip"
	def setup(self):
		self.threshold = 0.5
		self.window = 10
	def on_market(self, market):
		if market.last_price > self.threshold:
			return "yes"
		return None
'''

# Same logic, different class name
STRATEGY_A_RENAMED = '''
class PriceReversal:
	name = "price_reversal"
	def setup(self):
		self.threshold = 0.5
		self.window = 10
	def on_market(self, market):
		if market.last_price > self.threshold:
			return "yes"
		return None
'''

# Same structure, different parameter values
STRATEGY_A_DIFF_PARAMS = '''
class MomentumFlipV2:
	name = "momentum_flip_v2"
	def setup(self):
		self.threshold = 0.7
		self.window = 20
	def on_market(self, market):
		if market.last_price > self.threshold:
			return "yes"
		return None
'''

# Completely different logic
STRATEGY_B = '''
class VolumeSpike:
	name = "volume_spike"
	def setup(self):
		self.min_volume = 1000
	def on_market(self, market):
		if market.volume > self.min_volume:
			return "no"
		return None
'''


class TestCodeHash:
	def test_identical_code_same_hash(self):
		assert compute_code_hash(STRATEGY_A) == compute_code_hash(STRATEGY_A)

	def test_renamed_class_same_hash(self):
		"""Renaming the class should produce the same code hash."""
		assert compute_code_hash(STRATEGY_A) == compute_code_hash(STRATEGY_A_RENAMED)

	def test_different_logic_different_hash(self):
		assert compute_code_hash(STRATEGY_A) != compute_code_hash(STRATEGY_B)

	def test_different_params_different_hash(self):
		"""Different parameter values should produce different code hashes."""
		assert compute_code_hash(STRATEGY_A) != compute_code_hash(STRATEGY_A_DIFF_PARAMS)


class TestASTFingerprint:
	def test_identical_code_same_fingerprint(self):
		assert compute_ast_fingerprint(STRATEGY_A) == compute_ast_fingerprint(STRATEGY_A)

	def test_renamed_class_same_fingerprint(self):
		assert compute_ast_fingerprint(STRATEGY_A) == compute_ast_fingerprint(STRATEGY_A_RENAMED)

	def test_different_params_different_fingerprint(self):
		"""Different numeric literals should produce different fingerprints."""
		assert compute_ast_fingerprint(STRATEGY_A) != compute_ast_fingerprint(STRATEGY_A_DIFF_PARAMS)

	def test_different_logic_different_fingerprint(self):
		assert compute_ast_fingerprint(STRATEGY_A) != compute_ast_fingerprint(STRATEGY_B)

	def test_invalid_code_returns_none(self):
		assert compute_ast_fingerprint("this is not python{{{") is None


@pytest.fixture
def tracker(tmp_path):
	return Tracker(str(tmp_path / "test.db"))


class TestFingerprintStorage:
	def test_save_and_check_fingerprint(self, tracker):
		tracker.save_fingerprint("abc123", "StratA", "hash1")
		assert tracker.check_fingerprint("abc123") == "StratA"
		assert tracker.check_fingerprint("nonexistent") is None

	def test_check_code_hash(self, tracker):
		tracker.save_fingerprint("abc123", "StratA", "hash1")
		assert tracker.check_code_hash("hash1") == "StratA"
		assert tracker.check_code_hash("nonexistent") is None
