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


def _kalshi_fee(price: int, size: int) -> float:
    """Kalshi taker fee: ceil(0.07 × contracts × P × (1-P)) where P = price/100.

    Uses math.ceil per Kalshi's actual implementation — fee is rounded UP
    to the nearest cent per contract. This matters most at extreme prices
    (1-15¢, 85-99¢) where the raw formula gives sub-cent values but Kalshi
    charges a minimum of 1¢ per contract.
    """
    # P as a fraction (0.0 to 1.0)
    p = price / 100.0
    raw_fee = 0.07 * size * p * (1 - p) * 100  # in cents
    return math.ceil(raw_fee) if raw_fee > 0 else 0.0


def _zero_fee(price: int, size: int) -> float:
    return 0.0


KALSHI_FEE = FeeModel(
    id='kalshi',
    name='Kalshi Taker Fee',
    description='Kalshi taker fee: ceil(7% × P × (1-P)) per contract. Highest near 50¢ (2¢), minimum 1¢ at extremes.',
    formula='ceil(0.07 × contracts × P × (1-P) × 100) cents',
    _calc=_kalshi_fee,
)

ZERO_FEE = FeeModel(
    id='zero',
    name='No Fee',
    description='No exchange fees applied.',
    formula='None',
    _calc=_zero_fee,
)
