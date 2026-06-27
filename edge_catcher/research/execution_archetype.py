"""Execution-archetype resolution + fill-fragility classification.

The execution archetype describes HOW a strategy realizes its edge on the real
exchange — the dimension the event backtester is blind to. It is the single
source of truth for the fill-fragility gate (gate_fill_fragility.py).

Archetypes:
  - "maker"           : posts/rests liquidity (least fill-fragile)
  - "taker_prints"    : acts on realized trade prints (moderate)
  - "taker_synthetic" : crosses the synthetic implied-ask 100 - opposite_bid
                        (spot-fair's failure mode — most fill-fragile)
  - "unknown"         : unclassified — treated CONSERVATIVELY as fragile

Source of truth: each Strategy subclass declares an ``execution_archetype``
class attribute (mirrors ``supported_series``). Strategies that omit it default
to "unknown", which the gate treats as fragile so nothing slips through
unscreened.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

VALID_ARCHETYPES = frozenset({"maker", "taker_prints", "taker_synthetic", "unknown"})

# The ONLY fill-robust archetypes. Anything else — taker_synthetic, unknown,
# OR any unrecognized/out-of-vocabulary value — is treated as fill-fragile so a
# typo or a future archetype can never silently bypass the gate.
ROBUST_ARCHETYPES = frozenset({"maker", "taker_prints"})


def resolve_execution_archetype(strategy_name: str) -> str:
	"""Return the declared execution archetype for ``strategy_name``.

	Reads the ``execution_archetype`` class attribute from the discovered
	Strategy subclass. Returns "unknown" if the strategy can't be found or
	declares no (or an invalid) archetype.
	"""
	from edge_catcher.cli.backtest import build_strategy_map

	try:
		strategy_map, _ = build_strategy_map()
	except Exception:  # discovery is best-effort; never crash a gate
		logger.debug("archetype: build_strategy_map failed; defaulting unknown")
		return "unknown"

	cls = strategy_map.get(strategy_name)
	if cls is None:
		return "unknown"

	archetype = getattr(cls, "execution_archetype", "unknown")
	if archetype not in VALID_ARCHETYPES:
		logger.warning(
			"archetype: strategy %r declares invalid archetype %r",
			strategy_name, archetype,
		)
		return "unknown"
	return archetype


def is_fragile(archetype: str) -> bool:
	"""True if the archetype's fills are not reliably live-takeable.

	Fail-safe: anything not explicitly in ROBUST_ARCHETYPES (including
	"unknown" and any unrecognized value) is considered fragile.
	"""
	return archetype not in ROBUST_ARCHETYPES
