"""Tests for the tutorial/example strategy `LongshotFadeExample`.

The example strategy ships as a public, tracked file so new users can see a
complete end-to-end working strategy without reading private code. These
tests exercise:
  1. Class metadata (name, discoverability, base-class contract).
  2. Unit behavior of on_trade at entry + exit branches.
  3. An integration run through EventBacktester against the bundled
     ``demo_markets.db`` fixture so the full path (DB → strategy → P&L)
     has test coverage that breaks loudly if any layer regresses.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from edge_catcher.runner.event_backtest import EventBacktester, Portfolio
from edge_catcher.runner.strategies import Signal, Strategy
from edge_catcher.runner.strategies_example import LongshotFadeExample
from edge_catcher.storage.models import Market, Trade
from tests.fixtures.build_demo_markets_db import build as build_demo_markets


# ---------------------------------------------------------------------------
# Fixture — build the demo_markets DB in a tmp dir so the test is hermetic.
# The committed fixture at edge_catcher/data/examples/demo_markets.db is
# exercised by a separate CLI smoke test; unit tests build their own copy
# to avoid coupling test state to the committed file.
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def demo_markets_db(tmp_path_factory) -> Path:
	db_path = tmp_path_factory.mktemp("example_fixture") / "demo_markets.db"
	build_demo_markets(db_path)
	return db_path


# ---------------------------------------------------------------------------
# Metadata / contract tests
# ---------------------------------------------------------------------------

class TestMetadata:
	def test_name_attribute(self) -> None:
		assert LongshotFadeExample.name == "longshot_fade_example"

	def test_is_strategy_subclass(self) -> None:
		assert issubclass(LongshotFadeExample, Strategy)

	def test_has_on_trade(self) -> None:
		# `on_trade` is the abstract method on Strategy that the backtester
		# calls for every event. Instantiation would fail if it wasn't
		# implemented, but assert the attribute explicitly for clarity.
		assert callable(LongshotFadeExample.on_trade)

	def test_supported_series_empty_by_default(self) -> None:
		# Empty list signals "allowed on all series" per the module docstring.
		strat = LongshotFadeExample()
		assert strat.supported_series == []

	def test_cli_auto_discovery(self) -> None:
		"""The CLI's build_strategy_map must pick up the example class."""
		from edge_catcher.cli.backtest import build_strategy_map
		strategy_map, _ = build_strategy_map()
		assert "longshot_fade_example" in strategy_map
		assert strategy_map["longshot_fade_example"] is LongshotFadeExample


# ---------------------------------------------------------------------------
# on_trade unit tests — feed synthetic Trade/Market/Portfolio objects.
# ---------------------------------------------------------------------------

def _make_market(ticker: str = "DEMO_SERIES-X", result: str = "no") -> Market:
	return Market(
		ticker=ticker, event_ticker="EV", series_ticker="DEMO_SERIES",
		title="t", status="settled", result=result,
		yes_bid=None, yes_ask=None, last_price=None,
		open_interest=None, volume=100,
		expiration_time=None, close_time=None, created_time=None,
		settled_time=None, open_time=None,
		notional_value=None, floor_strike=None, cap_strike=None,
	)


def _make_trade(ticker: str, yes_price: int) -> Trade:
	return Trade(
		trade_id=f"tr-{ticker}-{yes_price}",
		ticker=ticker, yes_price=yes_price, no_price=100 - yes_price,
		count=1, taker_side="yes",
		created_time=datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc),
	)


class TestOnTrade:
	def test_fires_at_longshot_price(self) -> None:
		strat = LongshotFadeExample(entry_threshold=5)
		portfolio = Portfolio(1000.0, fee_fn=lambda p, s: 0.0)
		signals = strat.on_trade(_make_trade("T", 3), _make_market("T"), portfolio)
		assert len(signals) == 1
		sig = signals[0]
		assert sig.action == "buy"
		assert sig.side == "no"
		assert sig.price == 97  # 100 - 3

	def test_no_signal_above_threshold(self) -> None:
		strat = LongshotFadeExample(entry_threshold=5)
		portfolio = Portfolio(1000.0, fee_fn=lambda p, s: 0.0)
		signals = strat.on_trade(_make_trade("T", 10), _make_market("T"), portfolio)
		assert signals == []

	def test_no_double_entry(self) -> None:
		strat = LongshotFadeExample(entry_threshold=5)
		portfolio = Portfolio(1000.0, fee_fn=lambda p, s: 0.0)
		# Pre-seed an open position
		portfolio.open_position(
			Signal(action="buy", ticker="T", side="no", price=97, size=1, reason="test"),
			strat.name,
			datetime(2026, 4, 1, tzinfo=timezone.utc),
			slippage=0,
		)
		signals = strat.on_trade(_make_trade("T", 3), _make_market("T"), portfolio)
		assert signals == []  # already holding → no re-entry

	def test_exits_on_recovery(self) -> None:
		strat = LongshotFadeExample(entry_threshold=5, exit_threshold=10)
		portfolio = Portfolio(1000.0, fee_fn=lambda p, s: 0.0)
		portfolio.open_position(
			Signal(action="buy", ticker="T", side="no", price=97, size=1, reason="entry"),
			strat.name,
			datetime(2026, 4, 1, tzinfo=timezone.utc),
			slippage=0,
		)
		# NO-leg price is now 100 - yes_price = 100 - 2 = 98 >= 97 + 10? No, 98 < 107.
		# So a wider recovery is needed — entry NO=97 means YES must drop to 0 for
		# NO=100, and 100 >= 97+10=107 is still false. Set a lower entry scenario.
		pos = portfolio.positions[("T", strat.name)]
		pos.entry_price = 85  # simulate an entry at 85 (YES was 15 at entry)
		# Now YES=3 → NO=97 >= 85+10=95 → TP fires.
		signals = strat.on_trade(_make_trade("T", 3), _make_market("T"), portfolio)
		assert len(signals) == 1
		assert signals[0].action == "sell"
		assert signals[0].price == 97


# ---------------------------------------------------------------------------
# Integration — run the backtester on the bundled demo fixture.
# ---------------------------------------------------------------------------

class TestBacktestSmoke:
	def test_runs_without_errors(self, demo_markets_db: Path) -> None:
		"""The strategy runs to completion on the demo fixture."""
		backtester = EventBacktester()
		result = backtester.run(
			series="DEMO_SERIES",
			strategies=[LongshotFadeExample()],
			db_path=demo_markets_db,
			initial_cash=1000.0,
			slippage_cents=0,
			fee_fn=lambda p, s: 0.0,
		)
		# Smoke assertions — we don't pin exact P&L because the fixture is
		# allowed to evolve, but these invariants must always hold.
		assert result.total_trades >= 1, "fixture should produce >=1 entry at <=5c prices"
		assert result.wins + result.losses == result.total_trades
		assert "longshot_fade_example" in result.per_strategy

	def test_fires_on_longshot_entries_only(self, demo_markets_db: Path) -> None:
		"""All entries must be on the NO leg — the strategy never buys YES."""
		backtester = EventBacktester()
		result = backtester.run(
			series="DEMO_SERIES",
			strategies=[LongshotFadeExample()],
			db_path=demo_markets_db,
			initial_cash=1000.0,
			slippage_cents=0,
			fee_fn=lambda p, s: 0.0,
		)
		for ct in result.trade_sample:
			assert ct.side == "no", (
				f"strategy should only open NO positions, got side={ct.side}"
			)
