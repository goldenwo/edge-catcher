"""Engine strategy base class and signal types — runs both paper and live trades."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Literal, Optional

from edge_catcher.engine.market_state import TickContext


# Named alias for the three Phase 1 exit-kinds. Hoisted out of the inline
# annotation on ``Signal.exit_kind`` so D's ``engine/execution.py`` can
# declare ``cfg.exit_slippage_cents: dict[ExitKind, int]`` and validators
# can iterate ``typing.get_args(ExitKind)`` to assert config completeness.
# The inline annotation on ``Signal.exit_kind`` below uses this alias so
# the two stay in lock-step — adding a new kind (e.g. ``partial_exit``)
# requires only updating this one line.
ExitKind = Literal["take_profit", "stop_loss", "time_exit"]


@dataclass
class Signal:
	"""What a strategy wants to do — enter or exit."""
	action: str         # "enter" or "exit"
	ticker: str
	side: str           # "yes" or "no"
	series: str
	strategy: str
	reason: str
	trade_id: Optional[int] = None  # required for "exit" signals
	intended_size: Optional[int] = None  # deprecated: engine resolves sizing via pipeline
	entry_price_cents: int | None = None
	target_price_cents: int | None = None
	exit_kind: ExitKind | None = None
	stop_loss_distance_cents: int | None = None
	protective_stop_cents: int | None = None  # strategy's real stop (TP/SL dist); gate input, NOT sizing basis above


class Strategy(ABC):
	"""Base class for engine strategies — runs in both paper and live modes."""

	name: str
	supported_series: list[str]
	default_params: dict
	emoji: str = "🔵"  # color bullet shown in notifications

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
