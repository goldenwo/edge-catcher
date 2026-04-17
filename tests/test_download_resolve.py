"""Tests for _resolve_db_from_markets_yaml in edge_catcher/cli/download.py.

Locks in the behavior that ADAPTERS lookups match on Path.name (filename),
not full path — so both `config/markets-*.yaml` and `config.local/markets-*.yaml`
resolve to the same db_file.
"""

import pytest


def test_resolve_db_from_markets_yaml_matches_canonical_path():
	from edge_catcher.cli.download import _resolve_db_from_markets_yaml
	assert _resolve_db_from_markets_yaml("config/markets-altcrypto.yaml") == "data/kalshi-altcrypto.db"


def test_resolve_db_from_markets_yaml_matches_local_override():
	"""config.local/ overrides should map to the same db_file as config/."""
	from edge_catcher.cli.download import _resolve_db_from_markets_yaml
	assert _resolve_db_from_markets_yaml("config.local/markets-altcrypto.yaml") == "data/kalshi-altcrypto.db"


def test_resolve_db_from_markets_yaml_unknown_raises():
	from edge_catcher.cli.download import _resolve_db_from_markets_yaml
	with pytest.raises(ValueError):
		_resolve_db_from_markets_yaml("config/markets-nonexistent.yaml")
