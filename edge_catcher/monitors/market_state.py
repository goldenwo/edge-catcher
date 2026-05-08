"""Bundle-compat shim — re-exports for ``edge_catcher.engine.market_state``.

Pre-cutover daily bundles capture a copy of the running ``strategies_local.py``,
which at the time imported from ``edge_catcher.monitors.market_state``. Sub-
project G moves the canonical module to ``edge_catcher.engine.market_state``
without retiring the old import path, so the replay backtester can still load
those captured strategy files when sweeping historical R2 bundles for the
cutover-gate parity check.

Active code MUST import from ``edge_catcher.engine.market_state``. This shim
exists ONLY so old bundles + Pi-side rollback continue to resolve their imports
through the deferred-retirement window. After ``monitors/`` is fully retired
(follow-up PR after >=3 stable Pi days on the new engine), this file goes away.
"""

from edge_catcher.engine.market_state import (
	FillResult,
	MarketState,
	OrderbookSnapshot,
	TickContext,
)

__all__ = ["FillResult", "MarketState", "OrderbookSnapshot", "TickContext"]
