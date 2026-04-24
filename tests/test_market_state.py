"""Tests for market state models: OrderbookSnapshot, FillResult, TickContext, MarketState."""

import pytest
from edge_catcher.monitors.market_state import (
	OrderbookSnapshot,
	TickContext,
	MarketState,
	derive_event_ticker,
)


class TestOrderbookSnapshotDepth:
	def test_depth_empty_book(self):
		snap = OrderbookSnapshot(yes_levels=[], no_levels=[])
		assert snap.depth == 0

	def test_depth_with_levels(self):
		# yes: 10+5=15, no: 20+8=28 → total=43
		snap = OrderbookSnapshot(
			yes_levels=[(0.50, 10), (0.55, 5)],
			no_levels=[(0.45, 20), (0.40, 8)],
		)
		assert snap.depth == 43


class TestOrderbookSnapshotSpread:
	def test_spread_with_levels(self):
		# yes_ask=50, no_ask=45, spread = 50+45-100 = -5
		snap = OrderbookSnapshot(
			yes_levels=[(0.50, 10)],
			no_levels=[(0.45, 10)],
		)
		assert snap.spread == -5

	def test_spread_empty_book(self):
		snap = OrderbookSnapshot(yes_levels=[], no_levels=[])
		assert snap.spread == 0

	def test_spread_empty_yes(self):
		snap = OrderbookSnapshot(yes_levels=[], no_levels=[(0.45, 10)])
		assert snap.spread == 0

	def test_spread_empty_no(self):
		snap = OrderbookSnapshot(yes_levels=[(0.50, 10)], no_levels=[])
		assert snap.spread == 0


class TestOrderbookSnapshotWalkBook:
	def test_walk_book_full_fill_single_level(self):
		snap = OrderbookSnapshot(
			yes_levels=[(0.50, 10)],
			no_levels=[],
		)
		result = snap.walk_book("yes", 5)
		assert result.fill_size == 5
		assert result.blended_price_cents == 50
		assert result.slippage_cents == 0

	def test_walk_book_crossing_levels(self):
		# 3@0.50 + 2@0.55, want 5
		# cost = 3*50 + 2*55 = 150 + 110 = 260
		# blended = 260/5 = 52
		# slippage = 52 - 50 = 2
		snap = OrderbookSnapshot(
			yes_levels=[(0.50, 3), (0.55, 10)],
			no_levels=[],
		)
		result = snap.walk_book("yes", 5)
		assert result.fill_size == 5
		assert result.blended_price_cents == 52
		assert result.slippage_cents == 2

	def test_walk_book_partial_fill(self):
		# only 3 available, want 10
		snap = OrderbookSnapshot(
			yes_levels=[(0.50, 3)],
			no_levels=[],
		)
		result = snap.walk_book("yes", 10)
		assert result.fill_size == 3
		assert pytest.approx(result.fill_pct, abs=0.01) == 0.3

	def test_walk_book_empty_book(self):
		snap = OrderbookSnapshot(yes_levels=[], no_levels=[])
		result = snap.walk_book("yes", 5)
		assert result.fill_size == 0
		assert result.blended_price_cents == 0

	def test_walk_book_no_side(self):
		snap = OrderbookSnapshot(
			yes_levels=[(0.50, 10)],
			no_levels=[],
		)
		result = snap.walk_book("no", 5)
		assert result.fill_size == 0
		assert result.blended_price_cents == 0

	def test_walk_book_float_price_rounding(self):
		# 0.29 * 100 = 28.999... should round to 29
		snap = OrderbookSnapshot(
			yes_levels=[(0.29, 10)],
			no_levels=[],
		)
		result = snap.walk_book("yes", 5)
		assert result.blended_price_cents == 29


class TestTickContext:
	def test_basic_construction(self):
		snap = OrderbookSnapshot(yes_levels=[(0.50, 10)], no_levels=[(0.45, 10)])
		ctx = TickContext(
			ticker="KXBTC15M-26APR10-T1234",
			event_ticker="KXBTC15M-26APR10",
			yes_bid=48,
			yes_ask=50,
			no_bid=48,
			no_ask=45,
			orderbook=snap,
			price_history=[50, 51, 52],
			open_positions=[],
			persisted_state={},
			market_metadata={},
		)
		assert ctx.ticker == "KXBTC15M-26APR10-T1234"
		assert ctx.event_ticker == "KXBTC15M-26APR10"
		assert ctx.yes_ask == 50
		assert ctx.orderbook is snap
		assert ctx.price_history == [50, 51, 52]


class TestDeriveEventTicker:
	def test_strips_strike_suffix(self):
		assert derive_event_ticker("KXBTC15M-26APR10-T1234") == "KXBTC15M-26APR10"

	def test_no_suffix_unchanged(self):
		assert derive_event_ticker("KXBTC15M-26APR10") == "KXBTC15M-26APR10"

	def test_non_numeric_t_segment_unchanged(self):
		assert derive_event_ticker("FOO-BAR-TBAZ") == "FOO-BAR-TBAZ"

	def test_multiple_segments_only_strips_last(self):
		# Only last -Tnnnn should be stripped
		assert derive_event_ticker("FOO-T999-T1234") == "FOO-T999"


class TestMarketState:
	def test_register_and_get_price_history(self):
		ms = MarketState()
		ms.register_ticker("KXBTC15M-26APR10-T1234")
		series = ms.get_price_history("KXBTC15M-26APR10-T1234")
		assert series is not None
		assert len(series) == 0

	def test_update_price_returns_true_first_time(self):
		ms = MarketState()
		ms.register_ticker("TICKER-A")
		is_first = ms.update_price("TICKER-A", 55)
		assert is_first is True

	def test_update_price_returns_false_second_time(self):
		ms = MarketState()
		ms.register_ticker("TICKER-A")
		ms.update_price("TICKER-A", 55)
		is_first = ms.update_price("TICKER-A", 56)
		assert is_first is False

	def test_price_history_bounded(self):
		ms = MarketState(limit=5)
		ms.register_ticker("TICKER-B")
		for i in range(10):
			ms.update_price("TICKER-B", i)
		series = ms.get_price_history("TICKER-B")
		assert len(series) == 5
		# Should contain the last 5 values
		assert list(series) == [5, 6, 7, 8, 9]

	def test_unregister_clears_state(self):
		ms = MarketState()
		ms.register_ticker("TICKER-C")
		ms.update_price("TICKER-C", 50)
		ms.unregister_ticker("TICKER-C")
		assert ms.get_price_history("TICKER-C") is None

	def test_seed_orderbook(self):
		ms = MarketState()
		ms.register_ticker("TICKER-D")
		snap = OrderbookSnapshot(
			yes_levels=[(0.50, 10)],
			no_levels=[(0.45, 8)],
		)
		ms.seed_orderbook("TICKER-D", snap)
		ob = ms.get_orderbook("TICKER-D")
		assert ob is snap

	def test_apply_orderbook_delta_add(self):
		ms = MarketState()
		ms.register_ticker("TICKER-E")
		ms.seed_orderbook(
			"TICKER-E",
			OrderbookSnapshot(yes_levels=[(0.50, 10)], no_levels=[]),
		)
		# Adding new level
		ms.apply_orderbook_delta("TICKER-E", side="yes", price=0.55, delta=5)
		ob = ms.get_orderbook("TICKER-E")
		prices = [p for p, _ in ob.yes_levels]
		assert 0.55 in prices

	def test_apply_orderbook_delta_ignores_sub_cent_prices(self):
		"""Sub-cent WS deltas must be silently dropped.

		Kalshi trades only at integer cents (1¢–99¢); a delta at 0.1¢/0.7¢
		is never tradeable and must not enter the in-memory book, or the
		first-level "best price" seen by downstream logic would be 0c.
		"""
		ms = MarketState()
		ms.register_ticker("TEST-TICKER")
		seeded = OrderbookSnapshot(
			yes_levels=[(0.50, 10)],
			no_levels=[(0.50, 10)],
		)
		ms.seed_orderbook("TEST-TICKER", seeded)

		ms.apply_orderbook_delta("TEST-TICKER", side="yes", price=0.001, delta=100)
		ms.apply_orderbook_delta("TEST-TICKER", side="no", price=0.007, delta=50)

		ob = ms.get_orderbook("TEST-TICKER")
		assert ob is not None
		assert ob.yes_levels == [(0.50, 10)]
		assert ob.no_levels == [(0.50, 10)]

	def test_apply_orderbook_delta_remove(self):
		ms = MarketState()
		ms.register_ticker("TICKER-F")
		ms.seed_orderbook(
			"TICKER-F",
			OrderbookSnapshot(yes_levels=[(0.50, 10)], no_levels=[]),
		)
		# Setting quantity to 0 removes level
		ms.apply_orderbook_delta("TICKER-F", side="yes", price=0.50, delta=-10)
		ob = ms.get_orderbook("TICKER-F")
		assert ob.yes_levels == [] or all(q > 0 for _, q in ob.yes_levels)
