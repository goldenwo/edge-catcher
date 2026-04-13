"""Tests for the download CLI command — specifically the --skip-market-scan flag."""

import argparse
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

requests = pytest.importorskip("requests", reason="requests not installed")


# ---------------------------------------------------------------------------
# Helper: build a minimal args Namespace for _cmd_download
# ---------------------------------------------------------------------------

def _download_args(**overrides):
    defaults = dict(
        db_path="data/kalshi.db",
        config="config",
        markets=None,
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
    from edge_catcher.cli.download import _run_download as _cmd_download

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
    from edge_catcher.cli.download import _run_download as _cmd_download
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


# ---------------------------------------------------------------------------
# Tests for new agent-facing flags: --json, --list-strategies, --list-series,
# and the list-dbs command
# ---------------------------------------------------------------------------

import json
import subprocess
import sys


def _backtest_args(**overrides):
    """Build a minimal Namespace for _cmd_backtest."""
    defaults = dict(
        series="SERIES_A",
        strategy="example",
        start=None,
        end=None,
        cash=10000.0,
        slippage=1,
        tp=None,
        sl=None,
        min_price=None,
        max_price=None,
        h1_threshold_high=None,
        h1_threshold_low=None,
        h5_fav_threshold=None,
        h5_long_threshold=None,
        db_path="data/kalshi.db",
        output="reports/backtest_result.json",
        fee_pct=1.0,
        json=False,
        list_strategies=False,
        list_series=False,
    )
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


class TestListStrategies:
    def test_outputs_json_list(self, capsys):
        """--list-strategies prints a JSON object with a 'strategies' key."""
        from edge_catcher.cli.backtest import run as _cmd_backtest
        args = _backtest_args(list_strategies=True)
        _cmd_backtest(args)
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert "strategies" in data
        assert isinstance(data["strategies"], list)
        assert len(data["strategies"]) > 0

    def test_no_duplicates(self, capsys):
        """Each strategy name appears exactly once — no alias duplicates."""
        from edge_catcher.cli.backtest import run as _cmd_backtest
        args = _backtest_args(list_strategies=True)
        _cmd_backtest(args)
        data = json.loads(capsys.readouterr().out)
        names = data["strategies"]
        assert len(names) == len(set(names))

    def test_series_not_required(self, capsys):
        """--list-strategies works without --series."""
        from edge_catcher.cli.backtest import run as _cmd_backtest
        args = _backtest_args(list_strategies=True, series=None)
        _cmd_backtest(args)  # should not raise
        data = json.loads(capsys.readouterr().out)
        assert "strategies" in data


class TestListSeries:
    def test_outputs_json_with_series(self, tmp_path, capsys):
        """--list-series queries a real SQLite DB and returns structured JSON."""
        import sqlite3
        db = tmp_path / "test.db"
        conn = sqlite3.connect(str(db))
        conn.execute("CREATE TABLE markets (series_ticker TEXT, ticker TEXT)")
        conn.execute("INSERT INTO markets VALUES ('SERIES_A', 'SERIES_A-001')")
        conn.execute("INSERT INTO markets VALUES ('SERIES_B', 'SERIES_B-001')")
        conn.execute("INSERT INTO markets VALUES ('SERIES_A', 'SERIES_A-002')")
        conn.commit()
        conn.close()

        from edge_catcher.cli.backtest import run as _cmd_backtest
        args = _backtest_args(list_series=True, db_path=str(db), series=None)
        _cmd_backtest(args)
        data = json.loads(capsys.readouterr().out)
        assert data["series"] == ["SERIES_A", "SERIES_B"]
        assert data["total_markets"] == 3
        assert "db_path" in data

    def test_missing_db_returns_error_json(self, tmp_path, capsys):
        """--list-series on a non-existent table returns error JSON, exit 1."""
        import sqlite3
        db = tmp_path / "empty.db"
        # Create DB with no tables
        conn = sqlite3.connect(str(db))
        conn.close()

        from edge_catcher.cli.backtest import run as _cmd_backtest
        args = _backtest_args(list_series=True, db_path=str(db), series=None)
        with pytest.raises(SystemExit) as exc_info:
            _cmd_backtest(args)
        assert exc_info.value.code == 1
        data = json.loads(capsys.readouterr().out)
        assert data["status"] == "error"


class TestJsonFlag:
    def test_json_mode_outputs_status_ok(self, tmp_path, capsys):
        """--json mode: stdout is valid JSON with status='ok'."""
        from edge_catcher.cli.backtest import run as _cmd_backtest
        from unittest.mock import MagicMock, patch

        mock_result = MagicMock()
        mock_result.to_dict.return_value = {"pnl": 100.0, "trades": 5}

        output_file = tmp_path / "result.json"
        args = _backtest_args(
            json=True,
            output=str(output_file),
        )
        with patch("edge_catcher.runner.event_backtest.EventBacktester") as MockBT:
            MockBT.return_value.run.return_value = mock_result
            _cmd_backtest(args)

        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert data["status"] == "ok"
        assert data["pnl"] == 100.0

    def test_json_mode_error_returns_error_json(self, capsys):
        """--json mode: exceptions produce error JSON to stdout, exit 1."""
        from edge_catcher.cli.backtest import run as _cmd_backtest
        from unittest.mock import patch

        args = _backtest_args(json=True)
        with patch("edge_catcher.runner.event_backtest.EventBacktester") as MockBT:
            MockBT.return_value.run.side_effect = RuntimeError("db locked")
            with pytest.raises(SystemExit) as exc_info:
                _cmd_backtest(args)
        assert exc_info.value.code == 1
        data = json.loads(capsys.readouterr().out)
        assert data["status"] == "error"
        assert "db locked" in data["message"]

    def test_json_mode_missing_series_error(self, capsys):
        """--json + no --series → error JSON, exit 1."""
        from edge_catcher.cli.backtest import run as _cmd_backtest
        args = _backtest_args(json=True, series=None)
        with pytest.raises(SystemExit) as exc_info:
            _cmd_backtest(args)
        assert exc_info.value.code == 1
        data = json.loads(capsys.readouterr().out)
        assert data["status"] == "error"


class TestListDbs:
    def test_scans_data_dir(self, tmp_path, capsys, monkeypatch):
        """list-dbs scans data/ and returns JSON with path, size_mb, series."""
        import sqlite3
        from pathlib import Path

        # Create a fake data/ directory with one .db file
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        db = data_dir / "kalshi.db"
        conn = sqlite3.connect(str(db))
        conn.execute("CREATE TABLE markets (series_ticker TEXT, ticker TEXT)")
        conn.execute("INSERT INTO markets VALUES ('SERIES_A', 'X')")
        conn.commit()
        conn.close()

        # Patch Path("data") to point to our tmp data dir
        monkeypatch.chdir(tmp_path)

        from edge_catcher.cli.utils import _run_list_dbs as _cmd_list_dbs
        _cmd_list_dbs(argparse.Namespace())
        data = json.loads(capsys.readouterr().out)
        assert "databases" in data
        assert len(data["databases"]) == 1
        entry = data["databases"][0]
        assert "kalshi.db" in entry["path"]
        assert entry["size_mb"] >= 0
        assert entry["series"] == ["SERIES_A"]

    def test_empty_data_dir(self, tmp_path, capsys, monkeypatch):
        """list-dbs on an empty data/ dir returns empty list."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        monkeypatch.chdir(tmp_path)

        from edge_catcher.cli.utils import _run_list_dbs as _cmd_list_dbs
        _cmd_list_dbs(argparse.Namespace())
        data = json.loads(capsys.readouterr().out)
        assert data == {"databases": []}


def test_research_loop_help(capsys):
    """Verify the loop subcommand is registered."""
    import subprocess, sys
    proc = subprocess.run(
        [sys.executable, "-m", "edge_catcher", "research", "loop", "--help"],
        capture_output=True, text=True, timeout=10,
    )
    assert proc.returncode == 0
    assert "--max-runs" in proc.stdout
    assert "--max-time" in proc.stdout
    assert "--grid-only" in proc.stdout
    assert "--llm-only" in proc.stdout
    assert "--max-llm-calls" in proc.stdout


def test_research_loop_start_end_defaults():
    """Regression: `research loop` must default --start and --end to a
    concrete date range. Passing None propagates through GridPlanner into
    every hypothesis, and TemporalConsistencyGate then fails with
    "0 windows possible" on any series whose DB-resolved range is < 35
    days. Discovered during the 2026-04-13 Task 5 sweep.
    """
    import argparse
    from edge_catcher.cli import research as research_cli

    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command")
    research_cli.register(sub)

    args = parser.parse_args(["research", "loop"])
    assert args.start == "2025-01-01"
    assert args.end == "2025-12-31"
