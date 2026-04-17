"""Fee models for backtesting."""
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


def _make_proportional_fee(rate: float) -> FeeCalc:
	"""Factory: proportional fee at given rate. ceil(rate × contracts × P × (1-P) × 100) cents."""
	def _fee(price: int, size: int) -> float:
		p = price / 100.0
		raw_fee = rate * size * p * (1 - p) * 100
		return math.ceil(raw_fee) if raw_fee > 0 else 0.0
	return _fee


def _zero_fee(price: int, size: int) -> float:
	return 0.0


ZERO_FEE = FeeModel(
	id='zero',
	name='No Fee',
	description='No exchange fees applied.',
	formula='None',
	_calc=_zero_fee,
)

