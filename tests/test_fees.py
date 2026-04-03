"""Tests for FeeModel definitions."""
import math

import pytest
from edge_catcher.fees import KALSHI_FEE, ZERO_FEE


class TestKalshiFee:
    def test_fee_at_midpoint(self):
        # price=50, size=1: ceil(0.07 * 1 * 0.50 * 0.50 * 100) = ceil(1.75) = 2
        assert KALSHI_FEE.calculate(50, 1) == 2

    def test_fee_at_extreme(self):
        # price=95, size=1: ceil(0.07 * 1 * 0.95 * 0.05 * 100) = ceil(0.3325) = 1
        assert KALSHI_FEE.calculate(95, 1) == 1

    def test_fee_at_low_price(self):
        # price=5, size=1: ceil(0.07 * 1 * 0.05 * 0.95 * 100) = ceil(0.3325) = 1
        assert KALSHI_FEE.calculate(5, 1) == 1

    def test_fee_scales_with_size(self):
        # price=50, size=10: ceil(0.07 * 10 * 0.50 * 0.50 * 100) = ceil(17.5) = 18
        assert KALSHI_FEE.calculate(50, 10) == 18

    def test_fee_at_zero_price(self):
        assert KALSHI_FEE.calculate(0, 1) == 0.0

    def test_fee_at_hundred(self):
        assert KALSHI_FEE.calculate(100, 1) == 0.0

    def test_matches_kalshi_published_table(self):
        """Verify against Kalshi's published per-contract fee table."""
        # (price_cents, expected_fee_cents) for 1 contract
        published = [
            (1, 1), (5, 1), (10, 1), (15, 1), (20, 2), (25, 2),
            (30, 2), (35, 2), (40, 2), (45, 2), (50, 2), (55, 2),
            (60, 2), (65, 2), (70, 2), (75, 2), (80, 2), (85, 1),
            (90, 1), (95, 1), (99, 1),
        ]
        for price, expected in published:
            actual = KALSHI_FEE.calculate(price, 1)
            assert actual == expected, (
                f"Price {price}¢: expected {expected}¢ fee, got {actual}¢"
            )

    def test_ceil_rounding(self):
        """Fee is always rounded UP — no sub-cent fees."""
        for price in range(1, 100):
            fee = KALSHI_FEE.calculate(price, 1)
            assert fee == int(fee), f"Fee at {price}¢ is not integer: {fee}"
            assert fee >= 1, f"Fee at {price}¢ is less than 1¢: {fee}"

    def test_has_display_metadata(self):
        assert KALSHI_FEE.id == 'kalshi'
        assert KALSHI_FEE.name  # non-empty
        assert KALSHI_FEE.formula  # non-empty
        assert KALSHI_FEE.description  # non-empty


class TestZeroFee:
    def test_always_zero(self):
        assert ZERO_FEE.calculate(50, 10) == 0.0
        assert ZERO_FEE.calculate(0, 1) == 0.0
        assert ZERO_FEE.calculate(99, 100) == 0.0

    def test_has_display_metadata(self):
        assert ZERO_FEE.id == 'zero'
