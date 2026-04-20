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
		assert fill.intended_size == 40  # 200 // 5 — pre-walk target
		# Walker fills 20@5¢ (cost 100) then caps at 16@6¢ (cost 96) because
		# adding a 17th contract at 6¢ would push total cost to 202 > 200c risk.
		# Without the risk-budget cap the walker would have taken 20@6, spending
		# 220c → 10% over the configured budget. See resolve_fill docstring.
		assert fill.fill_size == 36
		assert fill.blended_price_cents == 5  # round((20*5 + 16*6) / 36) = round(5.44) = 5
		# Verify the real cost stays strictly within the risk budget
		assert fill.fill_size * fill.blended_price_cents <= config["sizing"]["risk_per_trade_cents"]

	def test_empty_book_fallback_when_require_fresh_book_false(self) -> None:
		"""Legacy fallback: entry_price used as fake fill price when
		require_fresh_book=False AND the fill side is empty.

		Historically this was strategy_a's first-tick entry path. Data from
		2026-04-12..19 showed these fallback entries had 0% win rate and
		-204c avg P&L — the entry_price from the ticker msg is a derived
		value, not a fillable offer. The new default is require_fresh_book=True
		which correctly skips these (see test_empty_book_skipped_...).
		This test exercises the backward-compat path only.
		"""
		config = {
			"sizing": {
				"risk_per_trade_cents": 200,
				"max_slippage_cents": 2,
				"min_fill": 3,
				"require_fresh_book": False,
			}
		}
		book = OrderbookSnapshot(yes_levels=[], no_levels=[])
		fill = resolve_fill(config, entry_price_cents=5, side="yes", book=book)
		assert isinstance(fill, FillResult)
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

	def test_empty_book_skipped_under_require_fresh_book_basic(self) -> None:
		"""require_fresh_book=True also filters empty fill sides (floor filter).

		This replaces the old behavior where empty books fell through to an
		entry_price fallback. The fallback produced phantom fills (0% win rate
		on 82 strategy_a entries, -204c avg) because the ticker-derived
		entry_price isn't a fillable offer. Now empty books → FillSkip("empty_book")
		by default. See test_empty_book_skipped_when_require_fresh_book for
		the authoritative version; this one locks the default behavior."""
		config = {
			"sizing": {
				"risk_per_trade_cents": 200,
				"max_slippage_cents": 2,
				"min_fill": 3,
				"require_fresh_book": True,
			}
		}
		book = OrderbookSnapshot(yes_levels=[], no_levels=[])
		result = resolve_fill(config, entry_price_cents=5, side="yes", book=book)
		assert isinstance(result, FillSkip)
		assert result.reason == "empty_book"

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

	def test_fillskip_empty_book_precedes_min_fill(self) -> None:
		"""Empty book under require_fresh_book=True skips as 'empty_book'
		regardless of would-be fill size. The empty-book check fires before
		the min_fill gate because there's nothing fillable on that side."""
		config = {"sizing": {"risk_per_trade_cents": 200, "max_slippage_cents": 2, "min_fill": 3}}
		book = OrderbookSnapshot(yes_levels=[], no_levels=[])
		# 200 // 99 = 2 — would be below_min_fill under fallback, but empty_book fires first.
		result = resolve_fill(config, entry_price_cents=99, side="yes", book=book)
		assert isinstance(result, FillSkip)
		assert result.reason == "empty_book"

	def test_empty_book_skipped_when_require_fresh_book(self) -> None:
		"""Empty fill side MUST be skipped when require_fresh_book=True.

		An empty side means nobody is offering on the side the strategy wants
		to buy. The previous "stale fallback" entered at the ticker's reported
		entry_price — but that's a phantom fill that never actually executes.
		Data from 2026-04-12..19 showed 82 such entries on strategy_a, all
		losses with 0% win rate and -204c avg (vs +71c avg on real walked
		fills). This is the "floor filter" the
		project_debut_fade_viability.md memory predicted.
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
		result = resolve_fill(config, entry_price_cents=5, side="yes", book=book)
		assert isinstance(result, FillSkip)
		assert result.reason == "empty_book"

	def test_empty_book_side_asymmetric_is_skipped(self) -> None:
		"""Empty fill side skipped even if the OTHER side has depth.

		strategy_a's is_first_observation can fire when YES side is populated
		but NO side has no real asks (or vice versa). The fill side's depth is
		what matters — ctx.orderbook.depth sums both sides and would incorrectly
		let one-sided books through."""
		config = {
			"sizing": {
				"risk_per_trade_cents": 200,
				"max_slippage_cents": 2,
				"min_fill": 3,
				"require_fresh_book": True,
			}
		}
		# Deep YES side, empty NO side. Strategy wants to buy NO.
		book = OrderbookSnapshot(yes_levels=[(0.95, 500)], no_levels=[])
		result = resolve_fill(config, entry_price_cents=5, side="no", book=book)
		assert isinstance(result, FillSkip)
		assert result.reason == "empty_book"

	def test_stale_book_does_not_fire_on_small_absolute_drift(self) -> None:
		"""1-2c drifts must NOT be flagged stale even when relative % is high.

		entry=2 with book=3 is 50% relative, but 1c absolute is normal market
		spread movement — the walker handles sizing via the budget cap. Without
		this floor, every normal longshot tick with a 1c market move would
		wrongly trigger stale_book.
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
			yes_levels=[(0.03, 30), (0.04, 100)],
			no_levels=[],
		)
		result = resolve_fill(config, entry_price_cents=2, side="yes", book=book)
		# Must be a FillResult (proceed), NOT FillSkip.
		assert isinstance(result, FillResult)


class TestRiskBudgetCap:
	"""Regression tests for the 2026-04-14 longshot oversizing bug.

	The `compute_raw_size` sizer computes contract count from the
	signal's entry_price, but the real book walk fills at the book's
	current prices — which can be 2-5c higher (within the stale-book
	10c tolerance). For longshot strategies like strategy_b that enter at
	1-15c, this previously doubled or tripled the configured per-trade
	risk. Fix: walk_book_with_ceiling now takes max_cost_cents and
	trims the take at each level so total cost never exceeds it.
	"""

	@pytest.fixture
	def config(self):
		return {"sizing": {"risk_per_trade_cents": 200, "max_slippage_cents": 2, "min_fill": 3}}

	def test_longshot_walk_divergence_is_capped_at_budget(self, config) -> None:
		"""Signal entry 2c but book's best_ask is already 3c with thin
		depth forcing the walk into the 4c level. Without the cap, sizer
		would buy 100 contracts (200 // 2) and the walker would fill
		them at ~3.5c avg → total cost ~350c, 1.75x the 200c risk.
		With the cap, total cost is strictly ≤ 200c.
		"""
		book = OrderbookSnapshot(
			yes_levels=[(0.03, 30), (0.04, 100)],   # best=3c; 2c signal ≠ book best
			no_levels=[],
		)
		fill = resolve_fill(config, entry_price_cents=2, side="yes", book=book)
		assert isinstance(fill, FillResult)
		# intended_size still sized from signal price (that's the contract)
		assert fill.intended_size == 100  # 200 // 2

		# Real cost stays within budget, not ≈350c
		real_cost = fill.fill_size * fill.blended_price_cents
		assert real_cost <= config["sizing"]["risk_per_trade_cents"]

		# Specifically: walker takes 30@3c (cost 90) + 27@4c (cost 108) = 198c
		# (28@4c would push cost to 202, past the 200c cap)
		assert fill.fill_size == 57
		assert fill.blended_price_cents == 3  # round(198/57) = round(3.47) = 3

	def test_normal_entry_unchanged_by_budget_cap(self, config) -> None:
		"""Sweet-spot-style entry where signal and book agree: the
		cap is not load-bearing and the walker fills the full raw_size.
		"""
		book = OrderbookSnapshot(
			yes_levels=[(0.50, 100)],   # plenty of depth at best
			no_levels=[],
		)
		fill = resolve_fill(config, entry_price_cents=50, side="yes", book=book)
		assert isinstance(fill, FillResult)
		assert fill.fill_size == 4  # 200 // 50 = 4
		assert fill.blended_price_cents == 50
		assert fill.fill_size * fill.blended_price_cents == 200  # exactly at budget

	def test_budget_cap_respects_min_fill(self, config) -> None:
		"""If the budget cap trims the fill below min_fill, the trade is
		skipped rather than entered under-sized. Prevents a 1-contract
		runt fill that wouldn't have covered its own fees.
		"""
		# Budget 200c, signal 2c → raw_size 100. Book walks entirely at
		# 99c (extreme slippage, stale_book wouldn't catch a 2→99 jump
		# because it's over the 10c threshold — simulate with a normal
		# small book instead). Pick an entry price + book combo where
		# 200c budget only fits 2 contracts at the walked price:
		# 200c // 99c = 2 (below min_fill=3).
		book = OrderbookSnapshot(
			yes_levels=[(0.90, 100)],
			no_levels=[],
		)
		# Signal at 90c matches book (no divergence). raw_size = 200//90 = 2.
		# Walker fills 2@90c = 180c (under budget). But 2 < min_fill=3.
		result = resolve_fill(config, entry_price_cents=90, side="yes", book=book)
		assert isinstance(result, FillSkip)
		assert result.reason == "below_min_fill"

	def test_walker_accepts_none_budget_for_backward_compat(self) -> None:
		"""Direct callers of walk_book_with_ceiling (e.g. existing tests)
		can omit max_cost_cents to get the unbounded pre-fix behavior.
		"""
		book = OrderbookSnapshot(
			yes_levels=[(0.03, 30), (0.04, 100)],
			no_levels=[],
		)
		fill = walk_book_with_ceiling(book, "yes", 100, max_slippage_cents=2)
		# Unbounded walker fills 30@3 + 70@4 = 370c cost, 100 contracts
		assert fill.fill_size == 100
		assert fill.blended_price_cents == 4  # round(370/100) = 4
		# (this is exactly the amount the resolve_fill cap prevents —
		# the caller is responsible for enforcing budget at a higher layer
		# if they don't pass max_cost_cents)
