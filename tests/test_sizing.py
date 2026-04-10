"""Tests for edge_catcher.monitors.sizing."""

import pytest

from edge_catcher.monitors.sizing import compute_raw_size


class TestComputeRawSize:
	def test_basic_division(self) -> None:
		assert compute_raw_size(200, 3) == 66

	def test_exact_division(self) -> None:
		assert compute_raw_size(200, 20) == 10

	def test_one_cent_entry(self) -> None:
		assert compute_raw_size(200, 1) == 200

	def test_budget_too_small(self) -> None:
		"""Risk budget smaller than entry price → 0 contracts."""
		assert compute_raw_size(200, 201) == 0

	def test_zero_price_raises(self) -> None:
		with pytest.raises(ValueError, match="entry_price_cents"):
			compute_raw_size(200, 0)

	def test_negative_price_raises(self) -> None:
		with pytest.raises(ValueError, match="entry_price_cents"):
			compute_raw_size(200, -5)
