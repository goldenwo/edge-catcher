"""Tests for _resolve_db_from_markets_yaml in edge_catcher/cli/download.py.

Locks in the behavior that ADAPTERS lookups match on Path.name (filename),
not full path — so both `config/markets-*.yaml` and `config.local/markets-*.yaml`
resolve to the same db_file.
"""

import argparse
from unittest.mock import MagicMock, patch

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
# Multi-exchange dispatch — _resolve_meta_from_markets_yaml + _run_download
# routes by meta.exchange. Locks in the fix for the bug where
# `download --markets config/markets-polymarket.yaml` instantiated
# KalshiAdapter and crashed with KeyError: 'kalshi'.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
	"markets_yaml, expected_id, expected_exchange",
	[
		("config/markets-btc.yaml", "kalshi_btc", "kalshi"),
		("config/markets-altcrypto.yaml", "kalshi_crypto", "kalshi"),
		("config/markets-polymarket.yaml", "polymarket_default", "polymarket"),
	],
)
def test_resolve_meta_from_markets_yaml(markets_yaml, expected_id, expected_exchange):
	"""Returns the AdapterMeta whose markets_yaml matches by Path.name."""
	from edge_catcher.cli.download import _resolve_meta_from_markets_yaml
	meta = _resolve_meta_from_markets_yaml(markets_yaml)
	assert meta.id == expected_id
	assert meta.exchange == expected_exchange


def test_resolve_meta_from_markets_yaml_unknown_raises():
	from edge_catcher.cli.download import _resolve_meta_from_markets_yaml
	with pytest.raises(ValueError):
		_resolve_meta_from_markets_yaml("config/markets-nonexistent.yaml")


def _download_args(**overrides):
	"""Build a minimal argparse Namespace matching the download subcommand."""
	defaults = dict(
		db_path=None,
		config="config",
		markets=None,
		dry_run=True,
		skip_market_scan=False,
		max_trade_markets=None,
		priority_series=None,
	)
	defaults.update(overrides)
	return argparse.Namespace(**defaults)


def test_run_download_polymarket_yaml_uses_polymarket_adapter(tmp_path):
	"""Regression: --markets config/markets-polymarket.yaml must route to
	PolymarketAdapter, not KalshiAdapter. Previously crashed with
	KeyError: 'kalshi' because KalshiAdapter was hardcoded.
	"""
	from edge_catcher.cli import download as dl

	mock_conn = MagicMock()
	mock_conn.execute.return_value.fetchone.return_value = (0,)
	mock_conn.execute.return_value.__iter__ = lambda self: iter([])

	mock_kalshi = MagicMock(name="KalshiAdapter")
	mock_polymarket = MagicMock(name="PolymarketAdapter")
	mock_polymarket.return_value.collect_markets.return_value = []
	mock_polymarket.return_value.series = []

	with patch("edge_catcher.storage.db.init_db"), \
		patch("edge_catcher.storage.db.get_connection", return_value=mock_conn), \
		patch.object(dl, "_resolve_db_from_markets_yaml", return_value=str(tmp_path / "polymarket.db")), \
		patch("edge_catcher.adapters.kalshi.KalshiAdapter", mock_kalshi), \
		patch("edge_catcher.adapters.polymarket.adapter.PolymarketAdapter", mock_polymarket):

		args = _download_args(
			db_path=str(tmp_path / "polymarket.db"),
			markets="config/markets-polymarket.yaml",
		)
		dl._run_download(args)

	mock_kalshi.assert_not_called()
	mock_polymarket.assert_called_once()


def test_run_download_kalshi_yaml_still_uses_kalshi_adapter(tmp_path):
	"""Regression guard: kalshi yaml must continue to use KalshiAdapter
	after the multi-exchange dispatch refactor.
	"""
	from edge_catcher.cli import download as dl

	mock_conn = MagicMock()
	mock_conn.execute.return_value.fetchone.return_value = (0,)
	mock_conn.execute.return_value.__iter__ = lambda self: iter([])

	mock_kalshi = MagicMock(name="KalshiAdapter")
	mock_kalshi.return_value.iter_market_pages.return_value = iter([])
	mock_kalshi.return_value.get_configured_series.return_value = []
	mock_polymarket = MagicMock(name="PolymarketAdapter")

	with patch("edge_catcher.storage.db.init_db"), \
		patch("edge_catcher.storage.db.get_connection", return_value=mock_conn), \
		patch.object(dl, "_resolve_db_from_markets_yaml", return_value=str(tmp_path / "kalshi-btc.db")), \
		patch("edge_catcher.adapters.kalshi.KalshiAdapter", mock_kalshi), \
		patch("edge_catcher.adapters.polymarket.adapter.PolymarketAdapter", mock_polymarket):

		args = _download_args(
			db_path=str(tmp_path / "kalshi-btc.db"),
			markets="config/markets-btc.yaml",
		)
		dl._run_download(args)

	mock_kalshi.assert_called_once()
	mock_polymarket.assert_not_called()


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
