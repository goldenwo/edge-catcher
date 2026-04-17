"""Coinbase adapter package.

`coinbase.py` was promoted to a package to house registry + adapter +
(later) fees. Re-exports below preserve backward compat for existing
`from edge_catcher.adapters.coinbase import CoinbaseAdapter` callers.
"""
from edge_catcher.adapters.coinbase.adapter import CoinbaseAdapter  # noqa: F401

__all__ = ["CoinbaseAdapter"]
