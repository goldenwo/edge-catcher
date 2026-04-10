"""Tests for the paper trading engine's synchronous process_tick pipeline."""

import pytest

from edge_catcher.monitors.market_state import OrderbookSnapshot, TickContext
from edge_catcher.monitors.strategy_base import PaperStrategy, Signal
from edge_catcher.monitors.trade_store import TradeStore
from edge_catcher.monitors.engine import process_tick


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
		"sizing": {"default": 10},
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
) -> TickContext:
	return TickContext(
		ticker="TEST-TICKER-T100",
		event_ticker="TEST-TICKER",
		yes_bid=48,
		yes_ask=yes_ask,
		no_bid=48,
		no_ask=100 - yes_ask,
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
		assert t["fill_size"] == 10
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
		"""ExitStrategy exits open positions."""
		# Record a trade first
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
		ctx = _make_ctx(ob, open_positions=open_pos, yes_ask=55)
		strategies = [ExitStrategy()]

		process_tick(ctx, strategies, store, config)

		# Trade should now be closed (not open)
		open_trades = store.get_open_trades()
		assert len(open_trades) == 0

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
