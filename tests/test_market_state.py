"""Tests for market state models: OrderbookSnapshot, FillResult, TickContext, MarketState."""

import pytest
from edge_catcher.engine.market_state import (
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
		# best_yes_bid=87, best_no_bid=12 → spread = 100 − (87+12) = 1.
		# (Pre-fix this read the penny floors as asks and returned −98-ish
		# garbage — the live spread gate could never fire on it.)
		snap = OrderbookSnapshot(
			yes_levels=[(0.01, 900), (0.87, 497)],
			no_levels=[(0.01, 500), (0.12, 60)],
		)
		assert snap.spread == 1

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
	"""walk_book consumes IMPLIED asks (opposite side's bids at 100−p).

	Rewritten 2026-06-11 (spec §5.7): the original suite walked same-side
	ladders as asks, encoding the bids-as-asks bug.  Each original case is
	preserved on a corrected two-sided ladder: the opposite bid p' = 1 − ask
	reproduces the same expected prices.
	"""

	def test_walk_book_full_fill_single_level(self):
		# NO bid 0.50×10 implies YES ask 50¢×10.
		snap = OrderbookSnapshot(yes_levels=[], no_levels=[(0.50, 10)])
		result = snap.walk_book("yes", 5)
		assert result.fill_size == 5
		assert result.blended_price_cents == 50
		assert result.slippage_cents == 0

	def test_walk_book_crossing_levels(self):
		# NO bids 0.45×10 (→ask 55¢), 0.50×3 (→ask 50¢, best).
		# Walk 5: 3@50 + 2@55 = 260 → blended 52, slippage +2.
		snap = OrderbookSnapshot(
			yes_levels=[],
			no_levels=[(0.45, 10), (0.50, 3)],
		)
		result = snap.walk_book("yes", 5)
		assert result.fill_size == 5
		assert result.blended_price_cents == 52
		assert result.slippage_cents == 2

	def test_walk_book_partial_fill(self):
		# Only 3 implied-ask contracts available, want 10.
		snap = OrderbookSnapshot(yes_levels=[], no_levels=[(0.50, 3)])
		result = snap.walk_book("yes", 10)
		assert result.fill_size == 3
		assert pytest.approx(result.fill_pct, abs=0.01) == 0.3

	def test_walk_book_empty_book(self):
		snap = OrderbookSnapshot(yes_levels=[], no_levels=[])
		result = snap.walk_book("yes", 5)
		assert result.fill_size == 0
		assert result.blended_price_cents == 0

	def test_walk_book_empty_opposite_side(self):
		# Buying NO crosses YES bids; none resting → no implied liquidity,
		# even though the NO side itself has resting bids.
		snap = OrderbookSnapshot(yes_levels=[], no_levels=[(0.50, 10)])
		result = snap.walk_book("no", 5)
		assert result.fill_size == 0
		assert result.blended_price_cents == 0

	def test_walk_book_float_price_rounding(self):
		# NO bid 0.71 → implied YES ask 100 − round(71.0) = 29¢.
		snap = OrderbookSnapshot(yes_levels=[], no_levels=[(0.71, 10)])
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


# ---------------------------------------------------------------------------
# Test 1.c — MarketState.clear() must reset _first_seen so the next price
# observation is treated as the first again. This is a regression lock for
# Change 3 in docs/superpowers/plans/replay-first-seen-fix.md (clear discipline).
# ---------------------------------------------------------------------------


class TestClearResetsFirstSeen:
	def test_clear_empties_first_seen_and_resets_first_observation(self) -> None:
		"""After clear(), the next update_price for a previously-seen ticker
		MUST return True, indicating the ticker is being observed for the
		first time again. Both checks are required:
		  - _first_seen is empty (state inspection).
		  - update_price returns True (behavioral contract that strategies see).
		"""
		ms = MarketState()
		assert ms.update_price("KXFOO-T1", 50) is True, "first observation"
		assert ms.update_price("KXFOO-T1", 51) is False, "second observation"

		ms.clear()

		# Bare state assertion — _first_seen must be empty.
		assert ms._first_seen == set(), (  # noqa: SLF001
			f"clear() left _first_seen populated: {ms._first_seen!r}"  # noqa: SLF001
		)
		# Behavioural assertion — the very next update_price re-fires first-seen.
		assert ms.update_price("KXFOO-T1", 52) is True, (
			"post-clear, the ticker must be treated as first-seen again"
		)


class TestImpliedAsks:
	"""implied_asks: the single source of truth for crossing the book.

	yes_levels/no_levels are resting BIDS (Kalshi wire shape, ascending).
	Buying side S crosses the OPPOSITE side's bids: a bid at p implies an
	ask at 100−p, so the cheapest ask comes from the HIGHEST opposite bid.
	"""

	def test_yes_asks_derived_from_no_bids_cheapest_first(self):
		# NO bids: penny floor 1¢×500, best 45¢×8.
		# Implied YES asks: 100−45=55¢×8 (cheapest), then 100−1=99¢×500.
		snap = OrderbookSnapshot(
			yes_levels=[(0.01, 900), (0.50, 10)],
			no_levels=[(0.01, 500), (0.45, 8)],
		)
		assert snap.implied_asks("yes") == [(55, 8), (99, 500)]

	def test_no_asks_derived_from_yes_bids(self):
		snap = OrderbookSnapshot(
			yes_levels=[(0.01, 900), (0.50, 10)],
			no_levels=[(0.45, 8)],
		)
		assert snap.implied_asks("no") == [(50, 10), (99, 900)]

	def test_unsorted_input_still_cheapest_first(self):
		# Defensive: implied_asks must sort, not trust input order.
		snap = OrderbookSnapshot(
			yes_levels=[],
			no_levels=[(0.45, 8), (0.01, 500)],
		)
		assert snap.implied_asks("yes") == [(55, 8), (99, 500)]

	def test_empty_opposite_side_no_implied_liquidity(self):
		snap = OrderbookSnapshot(yes_levels=[(0.50, 10)], no_levels=[])
		assert snap.implied_asks("yes") == []
		# the populated YES side still implies NO asks
		assert snap.implied_asks("no") == [(50, 10)]


class TestBestAccessors:
	def _two_sided(self) -> OrderbookSnapshot:
		# Real shape from the June 2026 live run (trade 98 reconstruction):
		# best_yes_bid=87 (depth 497), best_no_bid=12 → spread 1¢.
		return OrderbookSnapshot(
			yes_levels=[(0.01, 900), (0.87, 497)],
			no_levels=[(0.01, 500), (0.12, 60)],
		)

	def test_best_bids_are_highest_own_side(self):
		snap = self._two_sided()
		assert snap.best_yes_bid == 87
		assert snap.best_no_bid == 12

	def test_best_asks_are_implied_from_opposite(self):
		snap = self._two_sided()
		assert snap.best_yes_ask == 88   # 100 − best_no_bid(12)
		assert snap.best_no_ask == 13    # 100 − best_yes_bid(87)

	def test_empty_sides_return_none(self):
		snap = OrderbookSnapshot(yes_levels=[], no_levels=[(0.12, 60)])
		assert snap.best_yes_bid is None    # own side empty
		assert snap.best_no_ask is None     # needs yes_levels
		assert snap.best_no_bid == 12
		assert snap.best_yes_ask == 88


class TestMarketStateBidAskAccessors:
	def _seeded(self) -> MarketState:
		ms = MarketState()
		ms.register_ticker("T")
		ms.seed_orderbook("T", OrderbookSnapshot(
			yes_levels=[(0.01, 900), (0.87, 497)],
			no_levels=[(0.01, 500), (0.12, 60)],
		))
		return ms

	def test_get_yes_ask_is_implied_from_best_no_bid(self):
		# Pre-fix this returned yes_levels[0] = the 1¢ penny floor — the
		# root cause of the entry_best=1 recording corruption.
		assert self._seeded().get_yes_ask("T") == 88

	def test_get_yes_bid_is_best_own_side_bid(self):
		# Pre-fix this returned 100 − no_levels[0] ≈ 99.
		assert self._seeded().get_yes_bid("T") == 87

	def test_unknown_ticker_returns_none(self):
		ms = MarketState()
		assert ms.get_yes_ask("X") is None
		assert ms.get_yes_bid("X") is None

	def test_empty_no_side_means_no_yes_ask(self):
		ms = MarketState()
		ms.register_ticker("T")
		ms.seed_orderbook("T", OrderbookSnapshot(
			yes_levels=[(0.87, 497)], no_levels=[],
		))
		assert ms.get_yes_ask("T") is None
		assert ms.get_yes_bid("T") == 87
