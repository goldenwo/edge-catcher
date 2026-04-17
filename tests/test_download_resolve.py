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


# ---------------------------------------------------------------------------
# Drift guards: Coinbase CLI subcommand defaults must match the registry.
# If someone changes coinbase_btc.db_file in adapters/coinbase/registry.py
# without updating the CLI default, these tests fail — catching silent drift
# between two sources of truth.
# ---------------------------------------------------------------------------

def _build_cli_parser():
	import argparse
	from edge_catcher.cli.download import register

	parser = argparse.ArgumentParser()
	sub = parser.add_subparsers(dest="command")
	register(sub)
	return parser


def _default_for(parser, subcommand: str, dest: str):
	sub_parsers = parser._subparsers._group_actions[0].choices  # type: ignore[attr-defined]
	sp = sub_parsers[subcommand]
	for action in sp._actions:
		if action.dest == dest:
			return action.default
	raise AssertionError(f"no --{dest} on subcommand {subcommand}")


def test_download_btc_default_db_matches_coinbase_btc_registry():
	from api.adapter_registry import get_adapter

	parser = _build_cli_parser()
	cli_default = _default_for(parser, "download-btc", "db")
	registry_db = get_adapter("coinbase_btc").db_file
	assert cli_default == registry_db, (
		f"CLI --db default ({cli_default!r}) drifted from coinbase_btc.db_file "
		f"({registry_db!r}). Keep them in sync."
	)


def test_download_altcoin_ohlc_defaults_match_registry():
	from api.adapter_registry import get_adapter

	parser = _build_cli_parser()
	# db default matches every altcoin entry (they all share data/ohlc.db)
	cli_db = _default_for(parser, "download-altcoin-ohlc", "db")
	assert cli_db == get_adapter("coinbase_eth").db_file

	# start-date default must match the altcoin registry default
	cli_start = _default_for(parser, "download-altcoin-ohlc", "start_date")
	assert cli_start == get_adapter("coinbase_eth").default_start_date
