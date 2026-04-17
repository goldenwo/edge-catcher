"""Kalshi-specific fee models.

See docs/adr/0001-adapter-registry.md for the layout rationale."""
from __future__ import annotations

from edge_catcher.fees import FeeModel, make_proportional_fee


STANDARD_FEE = FeeModel(
	id='standard',
	name='Standard Fee',
	description='Standard taker fee: ceil(7% x P x (1-P) x 100) cents per contract',
	formula='ceil(0.07 * contracts * P * (1-P) * 100)',
	_calc=make_proportional_fee(0.07),
)

INDEX_FEE = FeeModel(
	id='index',
	name='Index Fee',
	description='Reduced index fee: ceil(3.5% x P x (1-P) x 100) cents per contract',
	formula='ceil(0.035 * contracts * P * (1-P) * 100)',
	_calc=make_proportional_fee(0.035),
)
