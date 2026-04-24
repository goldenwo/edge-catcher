"""Example strategy: LongshotFadeExample — a minimal tutorial strategy.

This module exists as a copy-paste starting point for users writing their
own strategies. It implements the classic "longshot fade" pattern: when a
binary contract is priced at <=5c on the YES leg (a "longshot"), buy the NO
leg to fade the implied probability, then exit when the NO leg recovers to
>=10c.

The longshot bias is a well-documented mispricing in prediction markets:
empirical win rates on far-out-of-the-money longshots are typically lower
than the price implies, so buying the opposite side tends to have positive
expected value. This example does NOT claim edge on any real market — it
is strictly a didactic scaffold.

Strategy interface
------------------
This file targets the **runner backtester** (``edge_catcher.runner``).
Strategies here subclass ``Strategy`` from ``edge_catcher.runner.strategies``
and implement ``on_trade(trade, market, portfolio) -> list[Signal]``. The
backtester streams trades from a ``markets``+``trades`` SQLite DB in time
order and calls ``on_trade`` for every tick.

The paper trader uses a *different* base class (``PaperStrategy`` in
``edge_catcher.monitors``) with a different callback (``on_tick``) — the two
are independent, not interchangeable. See ``docs/strategy-guide.md`` for a
side-by-side comparison.

Making it your own
------------------
1. Copy this file to ``edge_catcher/runner/strategies_local.py`` (which is
   gitignored — your edge stays private).
2. Rename the class and ``name`` attribute.
3. Edit the entry/exit logic.
4. Run ``edge-catcher backtest --strategy <your-name> --series <series>``.

See also ``strategies_local.py.example`` for a minimal template, and
``edge_catcher/runner/strategies.py`` for the ``Strategy`` base + reusable
``VolumeMixin`` / ``MomentumMixin`` filters.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from edge_catcher.runner.strategies import Signal, Strategy
from edge_catcher.storage.models import Market, Trade

if TYPE_CHECKING:
	from edge_catcher.runner.event_backtest import Portfolio


class LongshotFadeExample(Strategy):
	"""Buy NO when the YES leg trades at a deep longshot price; exit on recovery.

	Entry:  trade.yes_price <= ``entry_threshold`` (default 5c) → BUY NO at
	        ``100 - yes_price`` (i.e. buy the complementary leg cheaply).
	Exit:   a later trade on the same ticker sees a NO-leg price >=
	        ``exit_threshold`` (default 10c above entry) → SELL (take-profit).

	If no exit fires, the open position settles at market close via the
	backtester's settlement sweep.
	"""

	name = 'longshot_fade_example'
	# Empty list = strategy is allowed on any series. To restrict to certain
	# series, populate this list (the backtester's series filter is separate,
	# but strategies that care about series membership can check against this).
	supported_series: list[str] = []

	def __init__(
		self,
		entry_threshold: int = 5,
		exit_threshold: int = 10,
		size: int = 1,
	) -> None:
		self.entry_threshold = entry_threshold
		self.exit_threshold = exit_threshold
		self.size = size

	def on_trade(
		self, trade: Trade, market: Market, portfolio: 'Portfolio',
	) -> list[Signal]:
		# --- Exit branch: we already hold NO here → check TP trigger ---
		if portfolio.has_position(trade.ticker, self.name):
			pos = portfolio.positions.get((trade.ticker, self.name))
			if pos is not None:
				current_no_price = 100 - trade.yes_price
				if current_no_price >= pos.entry_price + self.exit_threshold:
					return [Signal(
						action='sell',
						ticker=trade.ticker,
						side=pos.side,
						price=current_no_price,
						size=pos.size,
						reason=(
							f'take_profit: no_price={current_no_price} '
							f'>= entry={pos.entry_price} + {self.exit_threshold}'
						),
					)]
			return []

		# --- Entry branch: YES leg is at longshot price → buy NO ---
		if trade.yes_price <= self.entry_threshold:
			no_price = 100 - trade.yes_price
			return [Signal(
				action='buy',
				ticker=trade.ticker,
				side='no',
				price=no_price,
				size=self.size,
				reason=f'longshot fade: yes_price={trade.yes_price} <= {self.entry_threshold}',
			)]

		return []
