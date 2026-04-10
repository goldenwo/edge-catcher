"""Sizing pipeline for the paper trading engine.

Pure functions that convert a risk budget + orderbook state into a fill decision.
"""

from __future__ import annotations


def compute_raw_size(risk_cents: int, entry_price_cents: int) -> int:
	"""Convert a dollar risk budget to a contract count.

	Args:
		risk_cents:        Maximum cents to risk on this trade.
		entry_price_cents: Entry price per contract in cents.

	Returns:
		Number of contracts (floor division). May be 0 if budget < price.

	Raises:
		ValueError: If entry_price_cents <= 0.
	"""
	if entry_price_cents <= 0:
		raise ValueError(
			f"entry_price_cents must be > 0, got {entry_price_cents}"
		)
	return risk_cents // entry_price_cents
