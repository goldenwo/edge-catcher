"""Exchange fee models for backtesting."""
from __future__ import annotations

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
    return 0.07 * price * (100 - price) / 100 * size


def _zero_fee(price: int, size: int) -> float:
    return 0.0


KALSHI_FEE = FeeModel(
    id='kalshi',
    name='Kalshi Variable Fee',
    description='Kalshi charges a variable fee based on contract price, highest near 50¢ and zero at the extremes.',
    formula='7% × price × (100 − price) / 100 per contract',
    _calc=_kalshi_fee,
)

ZERO_FEE = FeeModel(
    id='zero',
    name='No Fee',
    description='No exchange fees applied.',
    formula='None',
    _calc=_zero_fee,
)
