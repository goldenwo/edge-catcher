"""Tests for FeeModel definitions."""
import pytest
from edge_catcher.fees import KALSHI_FEE, ZERO_FEE


class TestKalshiFee:
    def test_fee_at_midpoint(self):
        # price=50, size=1: 0.07 * 50 * 50 / 100 = 1.75
        assert KALSHI_FEE.calculate(50, 1) == pytest.approx(1.75)

    def test_fee_at_extreme(self):
        # price=95, size=1: 0.07 * 95 * 5 / 100 = 0.3325
        assert KALSHI_FEE.calculate(95, 1) == pytest.approx(0.3325)

    def test_fee_scales_with_size(self):
        assert KALSHI_FEE.calculate(50, 10) == pytest.approx(17.5)

    def test_fee_at_zero_price(self):
        assert KALSHI_FEE.calculate(0, 1) == 0.0

    def test_fee_at_hundred(self):
        assert KALSHI_FEE.calculate(100, 1) == 0.0

    def test_matches_old_hardcoded_formula(self):
        # Verify exact parity with the old Portfolio formula: 1.0 * 0.07 * price * (100-price) / 100 * size
        for price in [10, 25, 50, 75, 87, 95]:
            for size in [1, 5, 10]:
                old = 1.0 * 0.07 * price * (100 - price) / 100 * size
                assert KALSHI_FEE.calculate(price, size) == pytest.approx(old, rel=1e-9)

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
