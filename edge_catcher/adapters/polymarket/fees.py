"""Polymarket fee model.

Polymarket charges no taker/maker trading fees on the CLOB itself — the
exchange runs on Polygon (USDC), so the only direct cost to a backtest is
the on-chain gas to settle. For backtesting purposes (where we model the
mid-quote economics, not the on-chain settlement leg), ZERO_FEE is the
correct model.

If a future strategy variant wants to model the gas-cost amortization,
override per-strategy via FeeOverrides on AdapterMeta.
"""
from __future__ import annotations

from edge_catcher.fees import ZERO_FEE, FeeModel

# Re-exported under a Polymarket-specific name so callers can write
# `from edge_catcher.adapters.polymarket.fees import POLYMARKET_FEE` and
# get a clear answer to "what's the cost model here?" without grepping.
POLYMARKET_FEE: FeeModel = ZERO_FEE
