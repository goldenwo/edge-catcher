"""Polymarket adapter package.

Polymarket is a prediction-market exchange (markets + trades shape, like Kalshi).
The adapter reads the public CLOB and Gamma APIs — no auth required for
read-only data.

Re-exports the adapter from .adapter so `from edge_catcher.adapters.polymarket
import PolymarketAdapter` works (parity with `edge_catcher.adapters.kalshi`).
"""
from edge_catcher.adapters.polymarket.adapter import PolymarketAdapter, SCHEMAS

__all__ = ["PolymarketAdapter", "SCHEMAS"]
