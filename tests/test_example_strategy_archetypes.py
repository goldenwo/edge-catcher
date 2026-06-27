"""The tracked example strategies declare a valid, non-default archetype.

Locks the convention that authored strategies classify their execution style;
new authored strategies should follow suit (the resolver defaults to 'unknown'
which the gate treats conservatively).
"""

from edge_catcher.research.execution_archetype import VALID_ARCHETYPES
from edge_catcher.runner.strategies import ExampleStrategy
from edge_catcher.runner.strategies_example import LongshotFadeExample


def test_example_strategies_declare_valid_archetype():
	for cls in (ExampleStrategy, LongshotFadeExample):
		assert cls.execution_archetype in VALID_ARCHETYPES
		assert cls.execution_archetype != "unknown"
