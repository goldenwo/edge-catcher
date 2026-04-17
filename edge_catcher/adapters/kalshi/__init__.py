"""Kalshi adapter package.

Re-exports the adapter from .adapter so `from edge_catcher.adapters.kalshi
import KalshiAdapter` keeps working after the kalshi.py -> kalshi/ package
transition made to accommodate per-exchange submodules (fees, etc.).
"""
from edge_catcher.adapters.kalshi.adapter import KalshiAdapter, SCHEMAS

__all__ = ["KalshiAdapter", "SCHEMAS"]
