"""Exchange fee models for backtesting."""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Callable

# The fee function signature: (price_cents, size) -> fee_cents
FeeCalc = Callable[[int, int], float]


@dataclass(frozen=True)
class FeeModel:
	"""Encapsulates an exchange's fee schedule with display metadata.

	Args for calculate():
		price: entry price in cents (0-100 for binary contracts)
		size: number of contracts
	Returns: fee in cents
	"""
	id: str
	name: str
	description: str
	formula: str
	_calc: FeeCalc  # type: ignore[misc]  # frozen dataclass + callable field

	def calculate(self, price: int, size: int) -> float:
		return self._calc(price, size)


def _make_kalshi_fee(rate: float) -> FeeCalc:
	"""Factory: Kalshi fee at given rate. ceil(rate × contracts × P × (1-P) × 100) cents."""
	def _fee(price: int, size: int) -> float:
		p = price / 100.0
		raw_fee = rate * size * p * (1 - p) * 100
		return math.ceil(raw_fee) if raw_fee > 0 else 0.0
	return _fee


def _zero_fee(price: int, size: int) -> float:
	return 0.0


KALSHI_FEE = FeeModel(
	id='kalshi',
	name='Kalshi Taker Fee',
	description='Kalshi taker fee: ceil(7% × P × (1-P)) per contract. Highest near 50¢ (2¢), minimum 1¢ at extremes.',
	formula='ceil(0.07 × contracts × P × (1-P) × 100) cents',
	_calc=_make_kalshi_fee(0.07),
)

KALSHI_INDEX_FEE = FeeModel(
	id='kalshi_index',
	name='Kalshi Index Fee',
	description='Kalshi fee for S&P 500 and Nasdaq-100: ceil(3.5% × P × (1-P)) per contract.',
	formula='ceil(0.035 × contracts × P × (1-P) × 100) cents',
	_calc=_make_kalshi_fee(0.035),
)

ZERO_FEE = FeeModel(
	id='zero',
	name='No Fee',
	description='No exchange fees applied.',
	formula='None',
	_calc=_zero_fee,
)

_INDEX_PREFIXES = ("KXINX", "KXNASDAQ100")


def get_fee_model_for_series(series: str) -> FeeModel:
	"""Return the fee model for a Kalshi series ticker.

	S&P 500 (KXINX*) and Nasdaq-100 (KXNASDAQ100*) use the halved 3.5% rate.
	All other series use the standard 7% rate.
	"""
	if series.startswith(_INDEX_PREFIXES):
		return KALSHI_INDEX_FEE
	return KALSHI_FEE
