"""Backward-compatibility shim — market_state.py has moved to engine/market_state.py.

Re-exports all public names so existing monitors/ callers and tests continue to
work without change until they are migrated to the engine path.
"""

from edge_catcher.engine.market_state import (  # noqa: F401
	FillResult,
	MarketState,
	OrderbookSnapshot,
	TickContext,
	derive_event_ticker,
)
