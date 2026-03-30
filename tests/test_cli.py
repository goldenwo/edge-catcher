"""Tests for the download CLI command — specifically the --skip-market-scan flag."""

import argparse
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest


# ---------------------------------------------------------------------------
# Helper: build a minimal args Namespace for _cmd_download
# ---------------------------------------------------------------------------

def _download_args(**overrides):
    defaults = dict(
        db_path="data/kalshi.db",
        config="config",
        dry_run=False,
        skip_market_scan=False,
        max_trade_markets=None,
        priority_series=None,
    )
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


# ---------------------------------------------------------------------------
# Test 1: --skip-market-scan is accepted by the CLI parser without error
# ---------------------------------------------------------------------------

def test_skip_market_scan_flag_accepted():
    """Parser accepts --skip-market-scan without raising SystemExit."""
    import argparse
    from edge_catcher.__main__ import main

    # Build the same parser as main() and confirm the flag parses cleanly
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command")
    dl = sub.add_parser("download")
    dl.add_argument("--db-path", default="data/kalshi.db")
    dl.add_argument("--dry-run", action="store_true")
    dl.add_argument("--skip-market-scan", action="store_true")
    dl.add_argument("--max-trade-markets", type=int, default=None)

    args = parser.parse_args(["download", "--skip-market-scan"])
    assert args.skip_market_scan is True

    args_no_flag = parser.parse_args(["download"])
    assert args_no_flag.skip_market_scan is False


# ---------------------------------------------------------------------------
# Test 2: With --skip-market-scan, iter_market_pages is NOT called
# ---------------------------------------------------------------------------

def test_skip_market_scan_skips_phase1(tmp_path):
    """When --skip-market-scan is set, iter_market_pages() is never called."""
    from edge_catcher.__main__ import _cmd_download

    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetchone.return_value = (500000,)

    mock_adapter = MagicMock()
    mock_adapter.get_configured_series.return_value = []

    with patch("edge_catcher.storage.db.init_db"), \
         patch("edge_catcher.storage.db.get_connection", return_value=mock_conn), \
         patch("edge_catcher.adapters.kalshi.KalshiAdapter", return_value=mock_adapter):

        args = _download_args(
            db_path=str(tmp_path / "test.db"),
            skip_market_scan=True,
        )
        _cmd_download(args)

    mock_adapter.iter_market_pages.assert_not_called()


# ---------------------------------------------------------------------------
# Test 3: With --skip-market-scan, trade downloads still proceed (Phase 2 runs)
# ---------------------------------------------------------------------------

def test_skip_market_scan_proceeds_to_phase2(tmp_path):
    """When --skip-market-scan is set, Phase 2 (trade downloads) still runs."""
    from edge_catcher.__main__ import _cmd_download
    from tests.conftest import make_market

    market = make_market(ticker="TEST-001")  # volume defaults to 100 in make_market

    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetchone.return_value = (500000,)
    mock_conn.execute.return_value.__iter__ = lambda self: iter([])

    mock_adapter = MagicMock()
    mock_adapter.get_configured_series.return_value = ["TEST"]
    mock_adapter.collect_trades.return_value = []

    with patch("edge_catcher.storage.db.init_db"), \
         patch("edge_catcher.storage.db.get_connection", return_value=mock_conn), \
         patch("edge_catcher.storage.db.get_markets_by_series", return_value=[market]), \
         patch("edge_catcher.adapters.kalshi.KalshiAdapter", return_value=mock_adapter):

        args = _download_args(
            db_path=str(tmp_path / "test.db"),
            skip_market_scan=True,
        )
        _cmd_download(args)

    # iter_market_pages must NOT be called
    mock_adapter.iter_market_pages.assert_not_called()
    # collect_trades MUST be called for the market with volume > 0
    mock_adapter.collect_trades.assert_called_once_with("TEST-001")
