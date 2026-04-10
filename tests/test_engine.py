"""Tests for the paper trading engine — process_tick pipeline and WS message handlers."""

import pytest

from edge_catcher.monitors.market_state import (
	MarketState,
	OrderbookSnapshot,
	TickContext,
)
from edge_catcher.monitors.strategy_base import PaperStrategy, Signal
from edge_catcher.monitors.trade_store import TradeStore
from edge_catcher.monitors.engine import (
	_collect_active_series,
	_handle_orderbook_delta,
	_handle_ticker_msg,
	_handle_trade_msg,
	_series_for_strategy,
	process_tick,
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

		process_tick(ctx, strategies, store, config)

		trades = store.get_open_trades()
		assert len(trades) == 1
		t = trades[0]
		assert t["ticker"] == "TEST-TICKER-T100"
		assert t["strategy"] == "stub"
		assert t["side"] == "yes"
		assert t["fill_size"] == 10  # 500c risk / 50c price = 10, book has 20 at 50c
		assert t["series_ticker"] == "TEST"

	def test_enter_skips_on_no_liquidity(self, store, config):
		"""Empty orderbook means fill_size=0 — no trade recorded."""
		ob = OrderbookSnapshot(yes_levels=[], no_levels=[])
		ctx = _make_ctx(ob, is_first=True)
		strategies = [StubStrategy()]

		process_tick(ctx, strategies, store, config)

		trades = store.get_open_trades()
		assert len(trades) == 0

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
		)
		ob = OrderbookSnapshot(yes_levels=[(0.55, 20)], no_levels=[(0.45, 20)])
		open_pos = [{"id": trade_id, "side": "yes", "ticker": "TEST-TICKER-T100"}]
		ctx = _make_ctx(ob, open_positions=open_pos, yes_ask=55, yes_bid=52)
		strategies = [ExitStrategy()]

		process_tick(ctx, strategies, store, config)

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
		process_tick(ctx, strategies, store, config)

		# StubStrategy should still have recorded its trade
		trades = store.get_open_trades()
		assert len(trades) == 1


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
		msg_data = {"market_ticker": ticker, "yes_ask": yes_ask}
		if yes_bid is not None:
			msg_data["yes_bid"] = yes_bid
		return {"type": "ticker", "msg": msg_data}

	def test_routes_tick_to_matching_strategy(self, setup):
		ms, store, strategies, strat_by_series, pending_states, config = setup
		msg = self._make_msg("KXBTC15M-26APR10-T100", yes_ask=50)

		_handle_ticker_msg(msg, config, ms, store, strategies, strat_by_series, pending_states)

		# StubStrategy fires on first observation
		trades = store.get_open_trades()
		assert len(trades) == 1
		assert trades[0]["strategy"] == "stub"

	def test_ignores_unmatched_series(self, setup):
		ms, store, strategies, strat_by_series, pending_states, config = setup
		# Ticker from a different series
		ms.register_ticker("KXXRP-26APR10-T200")
		msg = self._make_msg("KXXRP-26APR10-T200", yes_ask=50)

		_handle_ticker_msg(msg, config, ms, store, strategies, strat_by_series, pending_states)

		# No strategy matched KXXRP
		assert len(store.get_open_trades()) == 0

	def test_rejects_price_outside_range(self, setup):
		ms, store, strategies, strat_by_series, pending_states, config = setup
		msg = self._make_msg("KXBTC15M-26APR10-T100", yes_ask=0)

		_handle_ticker_msg(msg, config, ms, store, strategies, strat_by_series, pending_states)

		assert len(store.get_open_trades()) == 0

	def test_ignores_missing_yes_ask(self, setup):
		ms, store, strategies, strat_by_series, pending_states, config = setup
		msg = {"type": "ticker", "msg": {"market_ticker": "KXBTC15M-26APR10-T100"}}

		# Should not raise
		_handle_ticker_msg(msg, config, ms, store, strategies, strat_by_series, pending_states)

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
		_handle_ticker_msg(msg, config, ms, store, [no_strat], strat_by_series, pending_states)

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

		_handle_ticker_msg(msg, config, ms, store, [strat], strat_by_series, pending_states)

	def test_second_tick_not_first_observation(self, setup):
		ms, store, strategies, strat_by_series, pending_states, config = setup
		msg = self._make_msg("KXBTC15M-26APR10-T100", yes_ask=50)

		# First tick — StubStrategy enters
		_handle_ticker_msg(msg, config, ms, store, strategies, strat_by_series, pending_states)
		assert len(store.get_open_trades()) == 1

		# Second tick — StubStrategy should NOT enter again (not first observation)
		_handle_ticker_msg(msg, config, ms, store, strategies, strat_by_series, pending_states)
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

		_handle_trade_msg(msg, config, ms, store, strategies, strat_by_series, pending_states)

		assert len(strat.observed) == 1
		assert strat.observed[0] == ("yes", 3)

	def test_trade_populates_taker_side_and_count(self, setup):
		"""TickContext passed to strategy has correct taker_side and trade_count."""
		ms, store, strategies, strat_by_series, pending_states, config, strat = setup
		msg = self._make_trade_msg("KXBTC15M-26APR10-T100", yes_price=0.40, taker_side="no", count=7)

		_handle_trade_msg(msg, config, ms, store, strategies, strat_by_series, pending_states)

		assert strat.observed[0] == ("no", 7)

	def test_trade_updates_price_history(self, setup):
		"""Trade message updates market state price history."""
		ms, store, strategies, strat_by_series, pending_states, config, strat = setup
		msg = self._make_trade_msg("KXBTC15M-26APR10-T100", yes_price=0.55, taker_side="yes", count=1)

		_handle_trade_msg(msg, config, ms, store, strategies, strat_by_series, pending_states)

		history = list(ms.get_price_history("KXBTC15M-26APR10-T100") or [])
		assert 55 in history

	def test_ignores_unregistered_ticker(self, setup):
		"""Trade for a ticker not in market_state is silently skipped."""
		ms, store, strategies, strat_by_series, pending_states, config, strat = setup
		msg = self._make_trade_msg("KXBTC15M-26APR10-T999", yes_price=0.50, taker_side="yes", count=1)

		# Should not raise
		_handle_trade_msg(msg, config, ms, store, strategies, strat_by_series, pending_states)
		assert strat.observed == []

	def test_ignores_missing_yes_price(self, setup):
		"""Trade without yes_price is silently skipped."""
		ms, store, strategies, strat_by_series, pending_states, config, strat = setup
		msg = {"type": "trade", "msg": {"market_ticker": "KXBTC15M-26APR10-T100", "taker_side": "yes"}}

		_handle_trade_msg(msg, config, ms, store, strategies, strat_by_series, pending_states)
		assert strat.observed == []

	def test_ignores_price_outside_range(self, setup):
		"""yes_price of 0 or 100 is rejected."""
		ms, store, strategies, strat_by_series, pending_states, config, strat = setup
		msg = self._make_trade_msg("KXBTC15M-26APR10-T100", yes_price=0.0, taker_side="yes", count=1)

		_handle_trade_msg(msg, config, ms, store, strategies, strat_by_series, pending_states)
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

		_handle_trade_msg(msg, config, ms, store, strategies, strat_by_series, pending_states)

		assert len(strat.observed) == 1
		assert strat.observed[0] == ("no", 136)

	def test_derives_no_ask_from_no_price(self, setup):
		"""When no_price is provided, no_ask is derived from it (not 100-yes_ask)."""
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

		# yes_price=0.60, no_price=0.38 → no_ask=38, yes_bid=100-38=62
		msg = self._make_trade_msg(
			"KXBTC15M-26APR10-T100", yes_price=0.60, no_price=0.38, taker_side="yes",
		)
		_handle_trade_msg(msg, config, ms, store, [check_strat], strat_by_series, pending_states)

		assert check_strat.seen_ctx is not None
		assert check_strat.seen_ctx.yes_ask == 60
		assert check_strat.seen_ctx.no_ask == 38
		assert check_strat.seen_ctx.yes_bid == 62  # 100 - no_ask


# ---------------------------------------------------------------------------
# Helper function tests
# ---------------------------------------------------------------------------

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


class TestSeriesForStrategy:
	def test_returns_configured_series(self):
		config = {"strategies": {"s1": {"series": ["A", "B"]}}}
		assert _series_for_strategy(config, "s1") == {"A", "B"}

	def test_missing_strategy_returns_empty(self):
		config = {"strategies": {}}
		assert _series_for_strategy(config, "nonexistent") == set()
