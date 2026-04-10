"""Paper trading strategy base class and signal types."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

from edge_catcher.monitors.market_state import TickContext


@dataclass
class Signal:
	"""What a strategy wants to do — enter or exit."""
	action: str         # "enter" or "exit"
	ticker: str
	side: str           # "yes" or "no"
	series: str
	strategy: str
	intended_size: int  # from config sizing (ignored for exits)
	reason: str
	trade_id: Optional[int] = None  # required for "exit" signals


class PaperStrategy(ABC):
	"""Base class for all paper trading strategies."""

	name: str
	supported_series: list[str]
	default_params: dict

	@abstractmethod
	def on_tick(self, ctx: TickContext) -> list[Signal]:
		"""Called on every WS tick. Return entry/exit signals or empty list."""
		...

	def on_settle(self, trade: dict, state: dict) -> None:
		"""Optional — called when an open trade settles.
		state is the strategy's persisted state. Mutations flushed immediately.
		"""
		pass

	def on_startup(self, ctx: dict) -> None:
		"""Optional — called once after recovery, before WS loop starts.
		ctx: {"open_positions": [...], "active_tickers": [...], "state": {...}}
		"""
		pass
