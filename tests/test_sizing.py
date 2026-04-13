"""Tests for edge_catcher.monitors.sizing."""

import pytest

from edge_catcher.monitors.sizing import (
	compute_raw_size,
	validate_sizing_config,
	resolve_fill,
	FillSkip,
)
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

	def test_negative_risk_raises(self) -> None:
		with pytest.raises(ValueError, match="risk_cents"):
			compute_raw_size(-100, 5)


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


class TestValidateSizingConfig:
	def test_valid_config(self) -> None:
		config = {"sizing": {"risk_per_trade_cents": 200, "max_slippage_cents": 2, "min_fill": 3}}
		validate_sizing_config(config)  # should not raise

	def test_missing_risk_per_trade(self) -> None:
		config = {"sizing": {"max_slippage_cents": 2, "min_fill": 3}}
		with pytest.raises(ValueError, match="risk_per_trade_cents"):
			validate_sizing_config(config)

	def test_missing_max_slippage(self) -> None:
		config = {"sizing": {"risk_per_trade_cents": 200, "min_fill": 3}}
		with pytest.raises(ValueError, match="max_slippage_cents"):
			validate_sizing_config(config)

	def test_missing_min_fill(self) -> None:
		config = {"sizing": {"risk_per_trade_cents": 200, "max_slippage_cents": 2}}
		with pytest.raises(ValueError, match="min_fill"):
			validate_sizing_config(config)

	def test_missing_sizing_section(self) -> None:
		with pytest.raises(ValueError, match="sizing"):
			validate_sizing_config({})

	def test_zero_risk_raises(self) -> None:
		config = {"sizing": {"risk_per_trade_cents": 0, "max_slippage_cents": 2, "min_fill": 3}}
		with pytest.raises(ValueError, match="risk_per_trade_cents"):
			validate_sizing_config(config)

	def test_zero_min_fill_raises(self) -> None:
		config = {"sizing": {"risk_per_trade_cents": 200, "max_slippage_cents": 2, "min_fill": 0}}
		with pytest.raises(ValueError, match="min_fill"):
			validate_sizing_config(config)

	def test_negative_slippage_raises(self) -> None:
		config = {"sizing": {"risk_per_trade_cents": 200, "max_slippage_cents": -1, "min_fill": 3}}
		with pytest.raises(ValueError, match="max_slippage_cents"):
			validate_sizing_config(config)


class TestResolveFill:
	@pytest.fixture
	def config(self):
		return {"sizing": {"risk_per_trade_cents": 200, "max_slippage_cents": 2, "min_fill": 3}}

	def test_happy_path(self, config) -> None:
		book = OrderbookSnapshot(
			yes_levels=[(0.05, 20), (0.06, 20)],
			no_levels=[],
		)
		fill = resolve_fill(config, entry_price_cents=5, side="yes", book=book)
		assert fill is not None
		assert fill.intended_size == 40  # 200 // 5
		assert fill.fill_size == 40  # book has 40 within ceiling
		assert fill.blended_price_cents == 6  # (20*5 + 20*6) / 40 = 5.5 → round(5.5) = 6

	def test_empty_book_uses_entry_price_fallback(self, config) -> None:
		"""Empty book → startup fallback: returns fill at entry_price (blended=0).

		An empty book is the legitimate startup case (strategy_a fires on
		the first tick of a new market before the orderbook has been seeded).
		"""
		book = OrderbookSnapshot(yes_levels=[], no_levels=[])
		fill = resolve_fill(config, entry_price_cents=5, side="yes", book=book)
		assert fill is not None
		assert fill.blended_price_cents == 0  # signals stale book; trade_store uses entry_price
		assert fill.fill_size == 40  # 200 // 5

	def test_empty_book_below_min_fill_returns_skip(self, config) -> None:
		"""Empty book + entry too expensive to meet min_fill → FillSkip."""
		book = OrderbookSnapshot(yes_levels=[], no_levels=[])
		# 200 // 99 = 2, below min_fill=3
		fill = resolve_fill(config, entry_price_cents=99, side="yes", book=book)
		assert isinstance(fill, FillSkip)

	def test_populated_but_stale_book_falls_back_by_default(self) -> None:
		"""Populated book whose best is > 10c from entry_price → stale.

		With ``require_fresh_book: false`` explicitly set, the fallback
		path returns a FillResult with blended=0 so the trade enters at
		the tick price. This preserves the Apr 11 fix semantics for users
		who explicitly opt out of strict fresh-book checking.
		"""
		config = {
			"sizing": {
				"risk_per_trade_cents": 200,
				"max_slippage_cents": 2,
				"min_fill": 3,
				"require_fresh_book": False,
			}
		}
		# Book has real liquidity at 1c but strategy sees entry_price=42
		book = OrderbookSnapshot(
			yes_levels=[],
			no_levels=[(0.01, 500), (0.02, 100)],
		)
		fill = resolve_fill(config, entry_price_cents=42, side="no", book=book)
		assert fill is not None
		assert fill.blended_price_cents == 0  # stale fallback
		assert fill.fill_size > 0

	def test_require_fresh_book_defaults_to_true(self) -> None:
		"""Flag should default True — stale books must be skipped by default."""
		config = {"sizing": {"risk_per_trade_cents": 200, "max_slippage_cents": 2, "min_fill": 3}}
		# Note: no require_fresh_book key at all
		book = OrderbookSnapshot(yes_levels=[], no_levels=[(0.01, 500)])
		result = resolve_fill(config, entry_price_cents=42, side="no", book=book)
		# Default is now True → stale book → FillSkip(reason="stale_book")
		assert isinstance(result, FillSkip)
		assert result.reason == "stale_book"

	def test_populated_but_stale_book_skipped_when_require_fresh_book(self) -> None:
		"""With ``require_fresh_book: true``, a populated-but-stale book
		returns None so the trade is skipped entirely.

		This prevents the bookkeeping-artifact wins we saw in the Apr 11-12
		paper trader run where 94% of crypto 15m / KXXRP entries filled
		against phantom 1c liquidity that didn't reflect the tradeable market.
		"""
		config = {
			"sizing": {
				"risk_per_trade_cents": 200,
				"max_slippage_cents": 2,
				"min_fill": 3,
				"require_fresh_book": True,
			}
		}
		book = OrderbookSnapshot(
			yes_levels=[],
			no_levels=[(0.01, 500), (0.02, 100)],
		)
		fill = resolve_fill(config, entry_price_cents=42, side="no", book=book)
		assert isinstance(fill, FillSkip), "stale populated book must be skipped when require_fresh_book is set"

	def test_empty_book_still_allowed_under_require_fresh_book(self) -> None:
		"""require_fresh_book only filters populated-but-stale books.

		Empty books are the legitimate startup case for strategy_a's first-tick
		entries and must still fall back to entry_price even when strict
		fresh-book checking is enabled.
		"""
		config = {
			"sizing": {
				"risk_per_trade_cents": 200,
				"max_slippage_cents": 2,
				"min_fill": 3,
				"require_fresh_book": True,
			}
		}
		book = OrderbookSnapshot(yes_levels=[], no_levels=[])
		fill = resolve_fill(config, entry_price_cents=5, side="yes", book=book)
		assert fill is not None
		assert fill.blended_price_cents == 0
		assert fill.fill_size == 40

	def test_min_fill_gate(self, config) -> None:
		"""Book has only 2 contracts, min_fill is 3 → FillSkip."""
		book = OrderbookSnapshot(
			yes_levels=[(0.05, 2)],
			no_levels=[],
		)
		fill = resolve_fill(config, entry_price_cents=5, side="yes", book=book)
		assert isinstance(fill, FillSkip)

	def test_slippage_caps_fill(self, config) -> None:
		"""Book has 100 contracts but spread across wide prices."""
		book = OrderbookSnapshot(
			yes_levels=[(0.05, 10), (0.06, 10), (0.07, 10), (0.08, 10), (0.10, 60)],
			no_levels=[],
		)
		fill = resolve_fill(config, entry_price_cents=5, side="yes", book=book)
		assert fill is not None
		# Ceiling = 5+2 = 7c, so fills 10@5 + 10@6 + 10@7 = 30
		assert fill.fill_size == 30

	def test_budget_too_small_returns_skip(self, config) -> None:
		"""risk=200c, price=201c → raw_size=0 → FillSkip."""
		book = OrderbookSnapshot(
			yes_levels=[(2.01, 100)],
			no_levels=[],
		)
		fill = resolve_fill(config, entry_price_cents=201, side="yes", book=book)
		assert isinstance(fill, FillSkip)

	def test_no_side(self, config) -> None:
		book = OrderbookSnapshot(
			yes_levels=[],
			no_levels=[(0.03, 50)],
		)
		fill = resolve_fill(config, entry_price_cents=3, side="no", book=book)
		assert fill is not None
		assert fill.intended_size == 66  # 200 // 3
		assert fill.fill_size == 50  # book only has 50

	def test_fillskip_stale_book(self) -> None:
		"""Populated-but-stale book with require_fresh_book → FillSkip(stale_book)."""
		config = {
			"sizing": {
				"risk_per_trade_cents": 200,
				"max_slippage_cents": 2,
				"min_fill": 3,
				"require_fresh_book": True,
			},
		}
		book = OrderbookSnapshot(
			yes_levels=[],
			no_levels=[(0.01, 500)],
		)
		result = resolve_fill(config, entry_price_cents=42, side="no", book=book)
		assert isinstance(result, FillSkip)
		assert result.reason == "stale_book"

	def test_fillskip_budget_too_small(self, config) -> None:
		"""Risk budget smaller than entry price → FillSkip(budget_too_small)."""
		book = OrderbookSnapshot(
			yes_levels=[(2.01, 100)],
			no_levels=[],
		)
		result = resolve_fill(config, entry_price_cents=201, side="yes", book=book)
		assert isinstance(result, FillSkip)
		assert result.reason == "budget_too_small"

	def test_fillskip_below_min_fill_walk_book(self, config) -> None:
		"""Walked fill below min_fill → FillSkip(below_min_fill)."""
		book = OrderbookSnapshot(
			yes_levels=[(0.05, 2)],
			no_levels=[],
		)
		result = resolve_fill(config, entry_price_cents=5, side="yes", book=book)
		assert isinstance(result, FillSkip)
		assert result.reason == "below_min_fill"

	def test_fillskip_below_min_fill_empty_book(self) -> None:
		"""Empty book with raw_size < min_fill → FillSkip(below_min_fill)."""
		config = {"sizing": {"risk_per_trade_cents": 200, "max_slippage_cents": 2, "min_fill": 3}}
		book = OrderbookSnapshot(yes_levels=[], no_levels=[])
		# 200 // 99 = 2, below min_fill=3
		result = resolve_fill(config, entry_price_cents=99, side="yes", book=book)
		assert isinstance(result, FillSkip)
		assert result.reason == "below_min_fill"
