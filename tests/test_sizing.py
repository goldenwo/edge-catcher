"""Tests for edge_catcher.monitors.sizing."""

import pytest

from edge_catcher.monitors.sizing import compute_raw_size
from edge_catcher.monitors.market_state import OrderbookSnapshot, FillResult
from edge_catcher.monitors.sizing import walk_book_with_ceiling


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


class TestWalkBookWithCeiling:
	def test_fills_within_ceiling(self) -> None:
		"""Book at 3c, 4c, 5c, 6c with ceiling 2c — fills up to 5c only."""
		book = OrderbookSnapshot(
			yes_levels=[(0.03, 10), (0.04, 10), (0.05, 10), (0.06, 10)],
			no_levels=[],
		)
		fill = walk_book_with_ceiling(book, "yes", 40, max_slippage_cents=2)
		assert fill.fill_size == 30  # 10+10+10, stops before 6c
		assert fill.intended_size == 40
		assert fill.slippage_cents == 1  # blended(3+4+5)/3=4 minus best=3 = 1
		assert fill.fill_pct == pytest.approx(30 / 40)

	def test_zero_ceiling_only_best_level(self) -> None:
		"""Ceiling 0c — only the best price level fills."""
		book = OrderbookSnapshot(
			yes_levels=[(0.03, 5), (0.04, 20)],
			no_levels=[],
		)
		fill = walk_book_with_ceiling(book, "yes", 20, max_slippage_cents=0)
		assert fill.fill_size == 5
		assert fill.blended_price_cents == 3
		assert fill.slippage_cents == 0

	def test_empty_book(self) -> None:
		book = OrderbookSnapshot(yes_levels=[], no_levels=[])
		fill = walk_book_with_ceiling(book, "yes", 10, max_slippage_cents=2)
		assert fill.fill_size == 0
		assert fill.fill_pct == 0.0

	def test_no_side(self) -> None:
		"""Walking the no side uses no_levels."""
		book = OrderbookSnapshot(
			yes_levels=[],
			no_levels=[(0.05, 10), (0.06, 10)],
		)
		fill = walk_book_with_ceiling(book, "no", 20, max_slippage_cents=1)
		assert fill.fill_size == 20

	def test_partial_fill_at_boundary(self) -> None:
		"""Size exceeds book depth — partial fill."""
		book = OrderbookSnapshot(
			yes_levels=[(0.03, 5)],
			no_levels=[],
		)
		fill = walk_book_with_ceiling(book, "yes", 20, max_slippage_cents=5)
		assert fill.fill_size == 5
		assert fill.intended_size == 20
		assert fill.fill_pct == pytest.approx(5 / 20)

	def test_all_levels_beyond_ceiling(self) -> None:
		"""Best is 10c, next is 15c, ceiling 2c — only fills best level."""
		book = OrderbookSnapshot(
			yes_levels=[(0.10, 5), (0.15, 50)],
			no_levels=[],
		)
		fill = walk_book_with_ceiling(book, "yes", 30, max_slippage_cents=2)
		assert fill.fill_size == 5
		assert fill.blended_price_cents == 10
