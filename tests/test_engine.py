"""Tests for the paper trading engine — process_tick pipeline and WS message handlers."""

from datetime import datetime, timezone

import pytest


def _now() -> datetime:
	"""Timezone-aware wall-clock timestamp for `now=` kwargs in engine tests.

	These tests don't assert anything about timestamps; they just need a
	valid value to satisfy the required parameter. See tests/test_trade_store_now.py
	for the contract that these timestamps are checked against.
	"""
	return datetime.now(timezone.utc)

from edge_catcher.monitors.market_state import (
	MarketState,
	OrderbookSnapshot,
	TickContext,
)
from edge_catcher.monitors.metrics import Metrics
from edge_catcher.monitors.strategy_base import PaperStrategy, Signal
from edge_catcher.monitors.trade_store import TradeStore
from edge_catcher.monitors.dispatch import (
	_format_close_message,
	_format_enter_message,
	_handle_orderbook_delta,
	_handle_orderbook_snapshot,
	_handle_ticker_msg,
	_handle_trade_msg,
	_pnl_label,
	process_tick,
)
from edge_catcher.monitors.engine import (
	_collect_active_series,
	_series_for_strategy,
)


# ---------------------------------------------------------------------------
# Stub strategies
# ---------------------------------------------------------------------------

class StubStrategy(PaperStrategy):
	"""Enters on first observation."""
	name = "stub"
	supported_series = ["TEST"]
	default_params = {}

	def on_tick(self, ctx: TickContext) -> list[Signal]:
		if ctx.is_first_observation:
			return [Signal(
				action="enter", ticker=ctx.ticker, side="yes",
				series=ctx.series, strategy=self.name,
				intended_size=10, reason="test",
			)]
		return []


class ExitStrategy(PaperStrategy):
	"""Exits any open position."""
	name = "exit-stub"
	supported_series = ["TEST"]
	default_params = {}

	def on_tick(self, ctx: TickContext) -> list[Signal]:
		return [Signal(
			action="exit", ticker=ctx.ticker, side=pos["side"],
			series=ctx.series, strategy=self.name,
			intended_size=0, reason="test exit", trade_id=pos["id"],
		) for pos in ctx.open_positions]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def store(tmp_path):
	s = TradeStore(tmp_path / "test.db")
	yield s
	s.close()


@pytest.fixture
def config():
	return {
		"sizing": {"risk_per_trade_cents": 500, "max_slippage_cents": 5, "min_fill": 1},
		"strategies": {
			"stub": {"enabled": True, "series": ["TEST"]},
			"exit-stub": {"enabled": True, "series": ["TEST"]},
		},
	}


def _make_ctx(
	orderbook: OrderbookSnapshot,
	is_first: bool = False,
	open_positions: list | None = None,
	yes_ask: int = 50,
	yes_bid: int = 48,
) -> TickContext:
	return TickContext(
		ticker="TEST-TICKER-T100",
		event_ticker="TEST-TICKER",
		yes_bid=yes_bid,
		yes_ask=yes_ask,
		no_bid=100 - yes_ask,
		no_ask=100 - yes_bid,
		orderbook=orderbook,
		price_history=[50, 51],
		open_positions=open_positions or [],
		persisted_state={},
		market_metadata={},
		series="TEST",
		is_first_observation=is_first,
	)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestProcessTick:
	def test_enter_signal_records_trade(self, store, config):
		"""StubStrategy fires on first observation, trade is recorded."""
		ob = OrderbookSnapshot(yes_levels=[(0.50, 20)], no_levels=[(0.45, 20)])
		ctx = _make_ctx(ob, is_first=True)
		strategies = [StubStrategy()]

		process_tick(ctx, strategies, store, config, now=_now())

		trades = store.get_open_trades()
		assert len(trades) == 1
		t = trades[0]
		assert t["ticker"] == "TEST-TICKER-T100"
		assert t["strategy"] == "stub"
		assert t["side"] == "yes"
		assert t["fill_size"] == 10  # 500c risk / 50c price = 10, book has 20 at 50c
		assert t["series_ticker"] == "TEST"

	def test_enter_signal_on_empty_book_is_skipped(self, store, config):
		"""Empty orderbook with require_fresh_book=true (default) skips the entry.

		This replaces the old "entry_price fallback" which produced phantom
		fills (0% win rate on 82 strategy_a trades, -204c avg) because the
		ticker-derived entry_price isn't a fillable offer. The floor filter
		now requires a real book for every entry.
		"""
		ob = OrderbookSnapshot(yes_levels=[], no_levels=[])
		ctx = _make_ctx(ob, is_first=True)
		strategies = [StubStrategy()]

		process_tick(ctx, strategies, store, config, now=_now())

		trades = store.get_open_trades()
		assert trades == []

	def test_exit_signal_closes_trade(self, store, config):
		"""ExitStrategy exits open positions at the bid price (selling)."""
		trade_id = store.record_trade(
			ticker="TEST-TICKER-T100",
			entry_price=50,
			strategy="exit-stub",
			side="yes",
			series_ticker="TEST",
			intended_size=10,
			fill_size=10,
			blended_entry=50,
			now=datetime.now(timezone.utc),
		)
		ob = OrderbookSnapshot(yes_levels=[(0.55, 20)], no_levels=[(0.45, 20)])
		open_pos = [{"id": trade_id, "side": "yes", "ticker": "TEST-TICKER-T100"}]
		ctx = _make_ctx(ob, open_positions=open_pos, yes_ask=55, yes_bid=52)
		strategies = [ExitStrategy()]

		process_tick(ctx, strategies, store, config, now=_now())

		# Trade should be closed at the bid price (52c), not the ask (55c)
		open_trades = store.get_open_trades()
		assert len(open_trades) == 0
		row = store._conn.execute(
			"SELECT exit_price FROM paper_trades WHERE id=?", (trade_id,)
		).fetchone()
		assert row[0] == 52  # yes_bid, not yes_ask

	def test_strategy_exception_does_not_crash(self, store, config):
		"""A strategy that raises should not prevent other strategies from running."""

		class CrashStrategy(PaperStrategy):
			name = "crash"
			supported_series = ["TEST"]
			default_params = {}
			def on_tick(self, ctx):
				raise RuntimeError("boom")

		ob = OrderbookSnapshot(yes_levels=[(0.50, 20)], no_levels=[(0.45, 20)])
		ctx = _make_ctx(ob, is_first=True)
		strategies = [CrashStrategy(), StubStrategy()]

		# Should not raise
		process_tick(ctx, strategies, store, config, now=_now())

		# StubStrategy should still have recorded its trade
		trades = store.get_open_trades()
		assert len(trades) == 1

	def test_rejects_100c_no_entry(self, store, config):
		"""Entry at no_ask=100 (yes_bid=0) should be silently skipped."""
		# yes_bid=0 → no_ask=100 — degenerate entry with zero upside
		ob = OrderbookSnapshot(yes_levels=[(0.90, 20)], no_levels=[(1.00, 20)])

		class NoSideStub(PaperStrategy):
			name = "no-stub"
			supported_series = ["TEST"]
			default_params = {}
			def on_tick(self, ctx):
				from edge_catcher.monitors.strategy_base import Signal
				if ctx.is_first_observation:
					return [Signal(action="enter", ticker=ctx.ticker, side="no",
						series=ctx.series, strategy=self.name, reason="test")]
				return []

		ctx = _make_ctx(ob, is_first=True, yes_ask=90, yes_bid=0)
		strategies = [NoSideStub()]

		process_tick(ctx, strategies, store, config, now=_now())
		assert len(store.get_open_trades()) == 0

	def test_rejects_0c_yes_entry(self, store, config):
		"""Entry at yes_ask=0 should be silently skipped."""
		ob = OrderbookSnapshot(yes_levels=[(0.0, 20)], no_levels=[(0.90, 20)])
		ctx = _make_ctx(ob, is_first=True, yes_ask=0, yes_bid=0)
		strategies = [StubStrategy()]

		process_tick(ctx, strategies, store, config, now=_now())
		assert len(store.get_open_trades()) == 0


# ---------------------------------------------------------------------------
# _handle_ticker_msg tests
# ---------------------------------------------------------------------------

class TestHandleTickerMsg:
	"""Tests for the synchronous WS ticker message handler."""

	@pytest.fixture
	def setup(self, tmp_path):
		"""Common setup: market state, store, strategy, config."""
		ms = MarketState()
		ms.register_ticker("KXBTC15M-26APR10-T100")
		ms.seed_orderbook("KXBTC15M-26APR10-T100", OrderbookSnapshot(
			yes_levels=[(0.50, 20)], no_levels=[(0.45, 20)],
		))
		store = TradeStore(tmp_path / "test.db")
		strat = StubStrategy()
		strat.supported_series = ["KXBTC15M"]
		config = {
			"sizing": {"risk_per_trade_cents": 500, "max_slippage_cents": 5, "min_fill": 1},
			"strategies": {"stub": {"enabled": True, "series": ["KXBTC15M"]}},
		}
		strat_by_series = {"KXBTC15M": [strat]}
		pending_states = {"stub": {}}
		yield ms, store, [strat], strat_by_series, pending_states, config
		store.close()

	def _make_msg(self, ticker: str, yes_ask: int, yes_bid: int | None = None) -> dict:
		# Match real Kalshi WS format: prices as 'yes_ask_dollars' strings
		msg_data = {"market_ticker": ticker, "yes_ask_dollars": f"{yes_ask / 100:.4f}"}
		if yes_bid is not None:
			msg_data["yes_bid_dollars"] = f"{yes_bid / 100:.4f}"
		return {"type": "ticker", "msg": msg_data}

	def test_routes_tick_to_matching_strategy(self, setup):
		ms, store, strategies, strat_by_series, pending_states, config = setup
		msg = self._make_msg("KXBTC15M-26APR10-T100", yes_ask=50)

		_handle_ticker_msg(msg, config, ms, store, strategies, strat_by_series, pending_states, set(), now=_now())

		# StubStrategy fires on first observation
		trades = store.get_open_trades()
		assert len(trades) == 1
		assert trades[0]["strategy"] == "stub"

	def test_ignores_unmatched_series(self, setup):
		ms, store, strategies, strat_by_series, pending_states, config = setup
		# Ticker from a different series
		ms.register_ticker("KXXRP-26APR10-T200")
		msg = self._make_msg("KXXRP-26APR10-T200", yes_ask=50)

		_handle_ticker_msg(msg, config, ms, store, strategies, strat_by_series, pending_states, set(), now=_now())

		# No strategy matched KXXRP
		assert len(store.get_open_trades()) == 0

	def test_rejects_price_outside_range(self, setup):
		ms, store, strategies, strat_by_series, pending_states, config = setup
		msg = self._make_msg("KXBTC15M-26APR10-T100", yes_ask=0)

		_handle_ticker_msg(msg, config, ms, store, strategies, strat_by_series, pending_states, set(), now=_now())

		assert len(store.get_open_trades()) == 0

	def test_ignores_missing_yes_ask(self, setup):
		ms, store, strategies, strat_by_series, pending_states, config = setup
		msg = {"type": "ticker", "msg": {"market_ticker": "KXBTC15M-26APR10-T100"}}

		# Should not raise
		_handle_ticker_msg(msg, config, ms, store, strategies, strat_by_series, pending_states, set(), now=_now())

		assert len(store.get_open_trades()) == 0

	def test_yes_bid_read_separately(self, setup):
		"""yes_bid and yes_ask should produce different no_ask values."""
		ms, store, strategies, strat_by_series, pending_states, config = setup

		# Use a no-side entry strategy instead
		class NoSideStub(PaperStrategy):
			name = "no-stub"
			supported_series = ["KXBTC15M"]
			default_params = {}
			def on_tick(self, ctx):
				if ctx.is_first_observation:
					# Verify bid/ask are different
					assert ctx.yes_ask == 60
					assert ctx.yes_bid == 55
					assert ctx.no_ask == 100 - 55  # 100 - yes_bid = 45
					assert ctx.no_bid == 100 - 60  # 100 - yes_ask = 40
				return []

		no_strat = NoSideStub()
		strat_by_series["KXBTC15M"] = [no_strat]
		pending_states["no-stub"] = {}
		msg = self._make_msg("KXBTC15M-26APR10-T100", yes_ask=60, yes_bid=55)

		# Should not raise (assertions inside on_tick verify correctness)
		_handle_ticker_msg(msg, config, ms, store, [no_strat], strat_by_series, pending_states, set(), now=_now())

	def test_derives_event_ticker(self, setup):
		"""TickContext.event_ticker should strip the -Tnnnn suffix."""
		ms, store, strategies, strat_by_series, pending_states, config = setup

		class EventCheckStub(PaperStrategy):
			name = "event-check"
			supported_series = ["KXBTC15M"]
			default_params = {}
			def on_tick(self, ctx):
				assert ctx.event_ticker == "KXBTC15M-26APR10"
				return []

		strat = EventCheckStub()
		strat_by_series["KXBTC15M"] = [strat]
		pending_states["event-check"] = {}
		msg = self._make_msg("KXBTC15M-26APR10-T100", yes_ask=50)

		_handle_ticker_msg(msg, config, ms, store, [strat], strat_by_series, pending_states, set(), now=_now())

	def test_second_tick_not_first_observation(self, setup):
		ms, store, strategies, strat_by_series, pending_states, config = setup
		msg = self._make_msg("KXBTC15M-26APR10-T100", yes_ask=50)

		# First tick — StubStrategy enters
		_handle_ticker_msg(msg, config, ms, store, strategies, strat_by_series, pending_states, set(), now=_now())
		assert len(store.get_open_trades()) == 1

		# Second tick — StubStrategy should NOT enter again (not first observation)
		_handle_ticker_msg(msg, config, ms, store, strategies, strat_by_series, pending_states, set(), now=_now())
		assert len(store.get_open_trades()) == 1


# ---------------------------------------------------------------------------
# _handle_orderbook_delta tests
# ---------------------------------------------------------------------------

class TestHandleOrderbookDelta:
	def test_applies_delta_to_market_state(self):
		ms = MarketState()
		ms.register_ticker("T1")
		ms.seed_orderbook("T1", OrderbookSnapshot(
			yes_levels=[(0.50, 10)], no_levels=[],
		))

		msg = {
			"type": "orderbook_delta",
			"msg": {
				"market_ticker": "T1",
				"yes": [["0.50", 5]],  # add 5 at 50c
				"no": [],
			},
		}
		_handle_orderbook_delta(ms, msg)

		ob = ms.get_orderbook("T1")
		assert ob.yes_levels == [(0.50, 15)]  # 10 + 5

	def test_ignores_delta_for_unseeded_ticker(self):
		ms = MarketState()
		ms.register_ticker("T1")
		# No seed_orderbook — delta should be silently ignored

		msg = {
			"type": "orderbook_delta",
			"msg": {
				"market_ticker": "T1",
				"yes": [["0.50", 5]],
				"no": [],
			},
		}
		_handle_orderbook_delta(ms, msg)

		assert ms.get_orderbook("T1") is None

	def test_ignores_missing_ticker(self):
		ms = MarketState()
		msg = {"type": "orderbook_delta", "msg": {}}

		# Should not raise
		_handle_orderbook_delta(ms, msg)


# ---------------------------------------------------------------------------
# _handle_orderbook_snapshot tests
# ---------------------------------------------------------------------------

class TestHandleOrderbookSnapshot:
	def test_installs_snapshot_from_ws_message(self):
		ms = MarketState()
		ms.register_ticker("T1")

		msg = {
			"type": "orderbook_snapshot",
			"msg": {
				"market_ticker": "T1",
				"yes": [["0.50", 10], ["0.45", 20]],
				"no": [["0.30", 5]],
			},
		}
		_handle_orderbook_snapshot(ms, msg)

		ob = ms.get_orderbook("T1")
		assert ob is not None
		assert ob.yes_levels == [(0.45, 20), (0.50, 10)]  # sorted ascending
		assert ob.no_levels == [(0.30, 5)]

	def test_replaces_existing_book(self):
		ms = MarketState()
		ms.register_ticker("T1")
		ms.seed_orderbook("T1", OrderbookSnapshot(
			yes_levels=[(0.70, 99)], no_levels=[(0.20, 99)],
		))

		msg = {
			"type": "orderbook_snapshot",
			"msg": {
				"market_ticker": "T1",
				"yes": [["0.55", 8]],
				"no": [["0.40", 12]],
			},
		}
		_handle_orderbook_snapshot(ms, msg)

		ob = ms.get_orderbook("T1")
		assert ob.yes_levels == [(0.55, 8)]  # old 0.70 level gone
		assert ob.no_levels == [(0.40, 12)]  # old 0.20 level gone

	def test_filters_sub_cent_ghost_levels(self):
		ms = MarketState()
		ms.register_ticker("T1")

		msg = {
			"type": "orderbook_snapshot",
			"msg": {
				"market_ticker": "T1",
				"yes": [
					["0.007", 100],  # sub-cent ghost
					["0.50", 10],    # legit
					["0.009", 50],   # sub-cent ghost
				],
				"no": [["0.001", 200]],  # sub-cent ghost
			},
		}
		_handle_orderbook_snapshot(ms, msg)

		ob = ms.get_orderbook("T1")
		assert ob.yes_levels == [(0.50, 10)]
		assert ob.no_levels == []

	def test_filters_non_positive_quantities(self):
		ms = MarketState()
		ms.register_ticker("T1")

		msg = {
			"type": "orderbook_snapshot",
			"msg": {
				"market_ticker": "T1",
				"yes": [["0.50", 0], ["0.45", -5], ["0.40", 10]],
				"no": [],
			},
		}
		_handle_orderbook_snapshot(ms, msg)

		ob = ms.get_orderbook("T1")
		assert ob.yes_levels == [(0.40, 10)]

	def test_empty_sides_install_empty_book(self):
		ms = MarketState()
		ms.register_ticker("T1")

		msg = {
			"type": "orderbook_snapshot",
			"msg": {
				"market_ticker": "T1",
				"yes": [],
				"no": [],
			},
		}
		_handle_orderbook_snapshot(ms, msg)

		ob = ms.get_orderbook("T1")
		assert ob is not None
		assert ob.yes_levels == []
		assert ob.no_levels == []

	def test_handles_fp_field_shape(self):
		"""Kalshi's public schema documents yes_dollars_fp / no_dollars_fp."""
		ms = MarketState()
		ms.register_ticker("T1")

		msg = {
			"type": "orderbook_snapshot",
			"msg": {
				"market_ticker": "T1",
				"yes_dollars_fp": [["0.50", "10.00"]],
				"no_dollars_fp": [["0.30", "5.00"]],
			},
		}
		_handle_orderbook_snapshot(ms, msg)

		ob = ms.get_orderbook("T1")
		assert ob.yes_levels == [(0.50, 10)]
		assert ob.no_levels == [(0.30, 5)]

	def test_ignores_missing_ticker(self):
		ms = MarketState()
		msg = {"type": "orderbook_snapshot", "msg": {}}

		# Should not raise, should not install any book
		_handle_orderbook_snapshot(ms, msg)
		assert ms.get_orderbook("T1") is None

	def test_ignores_malformed_entries(self):
		ms = MarketState()
		ms.register_ticker("T1")

		msg = {
			"type": "orderbook_snapshot",
			"msg": {
				"market_ticker": "T1",
				"yes": [
					["not_a_price", 10],    # bad price
					["0.50", "not_qty"],    # bad qty
					[],                      # empty entry
					["0.45", 15],            # legit
				],
				"no": [],
			},
		}
		_handle_orderbook_snapshot(ms, msg)

		ob = ms.get_orderbook("T1")
		assert ob.yes_levels == [(0.45, 15)]


# ---------------------------------------------------------------------------
# _handle_trade_msg tests
# ---------------------------------------------------------------------------

class TestHandleTradeMsg:
	"""Tests for the synchronous WS trade message handler."""

	@pytest.fixture
	def setup(self, tmp_path):
		ms = MarketState()
		ms.register_ticker("KXBTC15M-26APR10-T100")
		ms.seed_orderbook("KXBTC15M-26APR10-T100", OrderbookSnapshot(
			yes_levels=[(0.50, 20)], no_levels=[(0.45, 20)],
		))
		store = TradeStore(tmp_path / "test.db")
		config = {
			"sizing": {"risk_per_trade_cents": 500, "max_slippage_cents": 5, "min_fill": 1},
			"strategies": {"stub": {"enabled": True, "series": ["KXBTC15M"]}},
		}

		class TradeAwareStub(PaperStrategy):
			"""Records taker_side and trade_count seen on each tick."""
			name = "trade-stub"
			supported_series = ["KXBTC15M"]
			default_params = {}
			observed: list[tuple] = []

			def on_tick(self, ctx: TickContext) -> list[Signal]:
				self.observed.append((ctx.taker_side, ctx.trade_count))
				return []

		strat = TradeAwareStub()
		strat_by_series = {"KXBTC15M": [strat]}
		pending_states = {"trade-stub": {}}
		yield ms, store, [strat], strat_by_series, pending_states, config, strat
		store.close()

	def _make_trade_msg(
		self,
		ticker: str,
		yes_price: float | str,
		no_price: float | str | None = None,
		taker_side: str = "yes",
		count: int | str = 5,
	) -> dict:
		msg_data: dict = {
			"market_ticker": ticker,
			"yes_price": yes_price,
			"taker_side": taker_side,
			"count": count,
		}
		if no_price is not None:
			msg_data["no_price"] = no_price
		return {"type": "trade", "msg": msg_data}

	def test_routes_trade_to_strategy(self, setup):
		"""Trade message is dispatched to matching strategies."""
		ms, store, strategies, strat_by_series, pending_states, config, strat = setup
		msg = self._make_trade_msg("KXBTC15M-26APR10-T100", yes_price=0.60, taker_side="yes", count=3)

		_handle_trade_msg(msg, config, ms, store, strategies, strat_by_series, pending_states, set(), now=_now())

		assert len(strat.observed) == 1
		assert strat.observed[0] == ("yes", 3)

	def test_trade_populates_taker_side_and_count(self, setup):
		"""TickContext passed to strategy has correct taker_side and trade_count."""
		ms, store, strategies, strat_by_series, pending_states, config, strat = setup
		msg = self._make_trade_msg("KXBTC15M-26APR10-T100", yes_price=0.40, taker_side="no", count=7)

		_handle_trade_msg(msg, config, ms, store, strategies, strat_by_series, pending_states, set(), now=_now())

		assert strat.observed[0] == ("no", 7)

	def test_trade_updates_price_history(self, setup):
		"""Trade message updates market state price history."""
		ms, store, strategies, strat_by_series, pending_states, config, strat = setup
		msg = self._make_trade_msg("KXBTC15M-26APR10-T100", yes_price=0.55, taker_side="yes", count=1)

		_handle_trade_msg(msg, config, ms, store, strategies, strat_by_series, pending_states, set(), now=_now())

		history = list(ms.get_price_history("KXBTC15M-26APR10-T100") or [])
		assert 55 in history

	def test_ignores_unregistered_ticker(self, setup):
		"""Trade for a ticker not in market_state is silently skipped."""
		ms, store, strategies, strat_by_series, pending_states, config, strat = setup
		msg = self._make_trade_msg("KXBTC15M-26APR10-T999", yes_price=0.50, taker_side="yes", count=1)

		# Should not raise
		_handle_trade_msg(msg, config, ms, store, strategies, strat_by_series, pending_states, set(), now=_now())
		assert strat.observed == []

	def test_ignores_missing_yes_price(self, setup):
		"""Trade without yes_price is silently skipped."""
		ms, store, strategies, strat_by_series, pending_states, config, strat = setup
		msg = {"type": "trade", "msg": {"market_ticker": "KXBTC15M-26APR10-T100", "taker_side": "yes"}}

		_handle_trade_msg(msg, config, ms, store, strategies, strat_by_series, pending_states, set(), now=_now())
		assert strat.observed == []

	def test_ignores_price_outside_range(self, setup):
		"""yes_price of 0 or 100 is rejected."""
		ms, store, strategies, strat_by_series, pending_states, config, strat = setup
		msg = self._make_trade_msg("KXBTC15M-26APR10-T100", yes_price=0.0, taker_side="yes", count=1)

		_handle_trade_msg(msg, config, ms, store, strategies, strat_by_series, pending_states, set(), now=_now())
		assert strat.observed == []

	def test_handles_string_price_and_count(self, setup):
		"""Kalshi sends prices as strings like '0.360' and count as '136.00'."""
		ms, store, strategies, strat_by_series, pending_states, config, strat = setup
		msg = self._make_trade_msg(
			"KXBTC15M-26APR10-T100",
			yes_price="0.360",
			no_price="0.640",
			taker_side="no",
			count="136.00",
		)

		_handle_trade_msg(msg, config, ms, store, strategies, strat_by_series, pending_states, set(), now=_now())

		assert len(strat.observed) == 1
		assert strat.observed[0] == ("no", 136)

	def test_bid_ask_sourced_from_orderbook_not_trade(self, setup):
		"""TickContext bid/ask come from the orderbook, not the trade price.

		Kalshi's trade WS carries `yes_price` = executed price of a completed
		trade, which can land off-book (late limit orders, aggressive fills).
		Strategies must see the current orderbook-sourced quote so they don't
		fire on phantom prices. See project_open_bugs_trade_channel.md.
		"""
		ms, store, strategies, strat_by_series, pending_states, config, strat = setup

		class BidAskCheckStub(PaperStrategy):
			name = "bid-check"
			supported_series = ["KXBTC15M"]
			default_params = {}
			seen_ctx: TickContext | None = None

			def on_tick(self, ctx: TickContext) -> list[Signal]:
				self.seen_ctx = ctx
				return []

		check_strat = BidAskCheckStub()
		strat_by_series["KXBTC15M"] = [check_strat]
		pending_states["bid-check"] = {}

		# Fixture orderbook: yes_levels=[(0.50, 20)], no_levels=[(0.45, 20)]
		# Expected ctx quotes: yes_ask=50, no_ask=45, yes_bid=100-45=55, no_bid=100-50=50.
		# Trade reports yes_price=0.30, no_price=0.60 — both off-book. Must be ignored for quotes.
		msg = self._make_trade_msg(
			"KXBTC15M-26APR10-T100", yes_price=0.30, no_price=0.60, taker_side="yes",
		)
		_handle_trade_msg(msg, config, ms, store, [check_strat], strat_by_series, pending_states, set(), now=_now())

		assert check_strat.seen_ctx is not None
		assert check_strat.seen_ctx.yes_ask == 50
		assert check_strat.seen_ctx.no_ask == 45
		assert check_strat.seen_ctx.yes_bid == 55  # 100 - best no_ask
		assert check_strat.seen_ctx.no_bid == 50   # 100 - best yes_ask

	def test_skips_strategy_when_orderbook_not_populated(self, setup):
		"""If orderbook hasn't been seeded for the ticker, don't fire strategies.

		Without book state, we can't evaluate entry criteria correctly. The
		previous behavior spoofed bid/ask from the trade price, which caused
		strategy_b to enter phantom trades on 2026-04-14. New policy: skip the
		strategy if orderbook isn't populated, but still record the trade in
		price_history (legitimate event data)."""
		ms, store, strategies, strat_by_series, pending_states, config, strat = setup

		# Register a fresh ticker but do NOT seed its orderbook
		ms.register_ticker("KXBTC15M-26APR10-T999")

		msg = self._make_trade_msg("KXBTC15M-26APR10-T999", yes_price=0.50, taker_side="yes", count=1)
		_handle_trade_msg(msg, config, ms, store, strategies, strat_by_series, pending_states, set(), now=_now())

		assert strat.observed == [], "strategy must not fire without an orderbook"
		# Trade itself is still recorded in price history
		assert 50 in list(ms.get_price_history("KXBTC15M-26APR10-T999") or [])


# ---------------------------------------------------------------------------
# Helper function tests
# ---------------------------------------------------------------------------

class TestPnlLabel:
	def test_positive_pnl(self):
		assert _pnl_label(7) == ("WIN", "+7¢")

	def test_negative_pnl(self):
		assert _pnl_label(-3) == ("LOSS", "-3¢")

	def test_zero_pnl(self):
		assert _pnl_label(0) == ("SCRATCH", "0¢")

	def test_none_pnl(self):
		assert _pnl_label(None) == ("?", "?")


class TestDirtyTracking:
	"""Verify that ticker and trade handlers mark strategies as dirty."""

	@pytest.fixture
	def setup(self, tmp_path):
		ms = MarketState()
		ms.register_ticker("KXBTC15M-26APR10-T100")
		ms.seed_orderbook("KXBTC15M-26APR10-T100", OrderbookSnapshot(
			yes_levels=[(0.50, 20)], no_levels=[(0.45, 20)],
		))
		store = TradeStore(tmp_path / "test.db")
		strat = StubStrategy()
		strat.supported_series = ["KXBTC15M"]
		config = {
			"sizing": {"risk_per_trade_cents": 500, "max_slippage_cents": 5, "min_fill": 1},
			"strategies": {"stub": {"enabled": True, "series": ["KXBTC15M"]}},
		}
		strat_by_series = {"KXBTC15M": [strat]}
		pending_states = {"stub": {}}
		yield ms, store, [strat], strat_by_series, pending_states, config
		store.close()

	def test_ticker_msg_marks_dirty(self, setup):
		ms, store, strategies, strat_by_series, pending_states, config = setup
		dirty: set[str] = set()
		msg = {"type": "ticker", "msg": {
			"market_ticker": "KXBTC15M-26APR10-T100",
			"yes_ask_dollars": "0.5000",
		}}
		_handle_ticker_msg(msg, config, ms, store, strategies, strat_by_series, pending_states, dirty, now=_now())
		assert "stub" in dirty

	def test_trade_msg_marks_dirty(self, setup):
		ms, store, strategies, strat_by_series, pending_states, config = setup
		dirty: set[str] = set()
		msg = {"type": "trade", "msg": {
			"market_ticker": "KXBTC15M-26APR10-T100",
			"yes_price": 0.50, "taker_side": "yes", "count": 1,
		}}
		_handle_trade_msg(msg, config, ms, store, strategies, strat_by_series, pending_states, dirty, now=_now())
		assert "stub" in dirty


class TestCollectActiveSeries:
	def test_collects_from_enabled_strategies(self):
		config = {
			"strategies": {
				"s1": {"enabled": True, "series": ["KXBTC15M", "KXXRP"]},
				"s2": {"enabled": False, "series": ["KXSOLD"]},
				"s3": {"enabled": True, "series": ["KXXRP", "KXNBA"]},
			},
		}
		result = _collect_active_series(config)
		assert set(result) == {"KXBTC15M", "KXXRP", "KXNBA"}

	def test_empty_config(self):
		assert _collect_active_series({}) == []

	def test_capture_extra_series_unioned_when_capture_enabled(self):
		"""capture.extra_series tickers get observed but never dispatched to
		any strategy. The union with strategy series happens at subscription
		time so REST recovery + WS subscription pick them up automatically."""
		config = {
			"strategies": {
				"s1": {"enabled": True, "series": ["KXETH15M"]},
			},
			"capture": {
				"enabled": True,
				"extra_series": ["KXBTC15M", "KXDOGE15M"],
			},
		}
		result = _collect_active_series(config)
		assert set(result) == {"KXETH15M", "KXBTC15M", "KXDOGE15M"}

	def test_capture_extra_series_ignored_when_capture_disabled(self):
		"""No point subscribing to extra tickers when capture is off — they
		generate WS load without being recorded for any purpose."""
		config = {
			"strategies": {
				"s1": {"enabled": True, "series": ["KXETH15M"]},
			},
			"capture": {
				"enabled": False,
				"extra_series": ["KXBTC15M"],
			},
		}
		result = _collect_active_series(config)
		assert set(result) == {"KXETH15M"}

	def test_capture_extra_series_dedupe_with_strategy_series(self):
		"""If a strategy already covers a series and capture lists it again,
		the union dedupes — no double subscription."""
		config = {
			"strategies": {
				"s1": {"enabled": True, "series": ["KXETH15M"]},
			},
			"capture": {
				"enabled": True,
				"extra_series": ["KXETH15M", "KXBTC15M"],
			},
		}
		result = _collect_active_series(config)
		assert set(result) == {"KXETH15M", "KXBTC15M"}
		assert result == sorted(set(result))  # also confirm sorted output

	def test_capture_extra_series_handles_none_or_missing(self):
		"""Missing or null ``extra_series`` key under capture is a no-op."""
		# capture present, no extra_series key
		config_a = {
			"strategies": {"s1": {"enabled": True, "series": ["KXETH15M"]}},
			"capture": {"enabled": True},
		}
		assert set(_collect_active_series(config_a)) == {"KXETH15M"}

		# capture present, extra_series explicitly null
		config_b = {
			"strategies": {"s1": {"enabled": True, "series": ["KXETH15M"]}},
			"capture": {"enabled": True, "extra_series": None},
		}
		assert set(_collect_active_series(config_b)) == {"KXETH15M"}


class TestSeriesForStrategy:
	def test_returns_configured_series(self):
		config = {"strategies": {"s1": {"series": ["A", "B"]}}}
		assert _series_for_strategy(config, "s1") == {"A", "B"}

	def test_missing_strategy_returns_empty(self):
		config = {"strategies": {}}
		assert _series_for_strategy(config, "nonexistent") == set()


class TestProcessTickMetrics:
	"""Engine-level integration tests for the operational metrics counter."""

	def test_entry_increments_attempted_and_filled(self, store, config):
		"""Happy path: a fillable entry bumps attempted and filled by one each."""
		metrics = Metrics()
		config["_metrics"] = metrics
		ob = OrderbookSnapshot(yes_levels=[(0.50, 20)], no_levels=[(0.45, 20)])
		ctx = _make_ctx(ob, is_first=True)
		strategies = [StubStrategy()]

		process_tick(ctx, strategies, store, config, now=_now())

		snap = metrics.snapshot()
		assert snap["entries_attempted"] == 1
		assert snap["entries_filled"] == 1
		assert snap["entries_skipped_stale"] == 0
		assert snap["entries_skipped_other"] == 0

	def test_stale_book_skip_increments_counter(self, store, config):
		"""require_fresh_book=true + populated-but-stale book → stale_skipped."""
		metrics = Metrics()
		config["_metrics"] = metrics
		# Opt into the fresh-book gate so divergence becomes a hard skip
		config["sizing"] = {**config["sizing"], "require_fresh_book": True}
		# Best yes level (80c) diverges from entry_price (50c) by >10c → stale
		ob = OrderbookSnapshot(yes_levels=[(0.80, 20)], no_levels=[(0.45, 20)])
		ctx = _make_ctx(ob, is_first=True, yes_ask=50, yes_bid=48)
		strategies = [StubStrategy()]

		process_tick(ctx, strategies, store, config, now=_now())

		snap = metrics.snapshot()
		assert snap["entries_attempted"] == 1
		assert snap["entries_filled"] == 0
		assert snap["entries_skipped_stale"] == 1
		assert snap["entries_skipped_other"] == 0
		# And no trade was recorded
		assert len(store.get_open_trades()) == 0


# ---------------------------------------------------------------------------
# _format_enter_message / _format_close_message tests
# ---------------------------------------------------------------------------

class TestFormatEnterMessage:
	"""The ENTER log line must expose fill_size + entry_price + cost so a
	reader can spot-check risk exposure against risk_per_trade_cents
	without consulting the DB.
	"""

	def test_longshot_yes_entry_includes_size_and_cost(self):
		log_line, notify_line = _format_enter_message(
			strategy="strategy_b", series="KXATPSETWINNER",
			ticker="KXATPSETWINNER-26APR12LANMUS-2-LAN",
			side="yes", fill_size=100, entry_price=2,
			trade_id=1234, bullet="🟣",
		)
		# Log line is grep-stable + includes the three pieces of math
		assert "ENTER strategy_b yes" in log_line
		assert "100x@2c" in log_line
		assert "cost=200c" in log_line
		assert "[id=1234]" in log_line
		# Notify shows the cost explicitly
		assert "PAPER BUY YES" in notify_line
		assert "100 @ 2¢" in notify_line
		assert "(200¢ cost)" in notify_line

	def test_no_side_label(self):
		_, notify_line = _format_enter_message(
			strategy="strategy_a", series="KXSPOTIFYARTISTD",
			ticker="KXSPOTIFYARTISTD-xyz",
			side="no", fill_size=5, entry_price=40,
			trade_id=42, bullet="🔵",
		)
		assert "PAPER BUY NO" in notify_line
		assert "5 @ 40¢" in notify_line
		assert "(200¢ cost)" in notify_line


class TestFormatCloseMessage:
	"""EXIT and SETTLED log lines must include enough detail to verify
	the PnL arithmetic from the line alone: contracts × (exit − entry) − fee = pnl.
	"""

	def test_settled_loss_includes_result_and_arithmetic(self):
		log_line, notify_line = _format_close_message(
			event="SETTLED", outcome="LOSS",
			strategy="strategy_b", series="KXATPSETWINNER",
			ticker="KXATPSETWINNER-26APR12LANMUS-2-LAN",
			side="yes", fill_size=100, effective_entry=2,
			exit_price=0, pnl_cents=-202, fee_cents=2,
			settled_result="no", trade_id=1234, bullet="🟣",
		)
		# Log line: 100 × (0 - 2) - 2 = -202 is derivable
		assert "SETTLED strategy_b yes" in log_line
		assert "100x 2c->0c" in log_line
		assert "result=no" in log_line
		assert "LOSS pnl=-202c" in log_line
		assert "fee=-2c" in log_line
		# Notify shows settled side
		assert "LOSS (settled NO)" in notify_line
		assert "100 YES 2¢ → 0¢" in notify_line
		assert "-202¢ pnl" in notify_line
		assert "(−2¢ fee)" in notify_line

	def test_settled_win_yes_resolves_yes(self):
		log_line, notify_line = _format_close_message(
			event="SETTLED", outcome="WIN",
			strategy="strategy_b", series="KXATPSETWINNER",
			ticker="KXATP-xyz",
			side="yes", fill_size=100, effective_entry=2,
			exit_price=100, pnl_cents=9798, fee_cents=2,
			settled_result="yes", trade_id=5, bullet="🟣",
		)
		# 100 × (100 - 2) - 2 = 9798 ✓
		assert "result=yes" in log_line
		assert "WIN pnl=+9798c" in log_line
		assert "WIN (settled YES)" in notify_line
		assert "+9798¢ pnl" in notify_line

	def test_exit_tp_has_no_settled_result(self):
		"""Manual exits (TP/SL) don't have a market-settled side."""
		log_line, notify_line = _format_close_message(
			event="EXIT", outcome="WIN",
			strategy="strategy_a", series="KXSPOTIFYARTISTD",
			ticker="KXSP-xyz",
			side="yes", fill_size=3, effective_entry=40,
			exit_price=48, pnl_cents=23, fee_cents=1,
			settled_result=None, trade_id=99, bullet="🔵",
		)
		# 3 × (48 - 40) - 1 = 23 ✓
		assert "EXIT strategy_a yes" in log_line
		assert "3x 40c->48c" in log_line
		assert "(exit)" in log_line
		assert "WIN pnl=+23c" in log_line
		assert "WIN (exit)" in notify_line
		assert "3 YES 40¢ → 48¢" in notify_line

	def test_fee_zero_is_omitted(self):
		"""Clean lines when no fee — avoids '(-0¢ fee)' noise."""
		_, notify_line = _format_close_message(
			event="SETTLED", outcome="SCRATCH",
			strategy="strategy_a", series="KXETH15M",
			ticker="KXETH15M-xyz",
			side="no", fill_size=5, effective_entry=50,
			exit_price=50, pnl_cents=0, fee_cents=0,
			settled_result=None, trade_id=1, bullet="🔵",
		)
		assert "fee" not in notify_line
		assert "(settled" not in notify_line  # no settled_result → (exit) path
