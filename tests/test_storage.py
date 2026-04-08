"""Tests for storage layer: CRUD operations and WAL mode."""

import pytest
from datetime import datetime, timezone
from pathlib import Path

from edge_catcher.storage.db import (
    get_connection,
    get_db,
    get_db_stats,
    get_markets_by_series,
    get_settled_markets,
    get_trades_for_ticker,
    init_db,
    save_analysis_result,
    upsert_market,
    upsert_trade,
    upsert_trades_batch,
)
from edge_catcher.storage.models import HypothesisResult, Market, Trade
from tests.conftest import make_market, make_trade


# ---------------------------------------------------------------------------
# Init / WAL
# ---------------------------------------------------------------------------

def test_init_creates_tables(tmp_db_path):
    conn = get_connection(tmp_db_path)
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    conn.close()
    assert "markets" in tables
    assert "trades" in tables
    assert "analysis_results" in tables
    assert "hypothesis_runs" in tables


def test_wal_mode_enabled(tmp_db_conn):
    mode = tmp_db_conn.execute("PRAGMA journal_mode").fetchone()[0]
    assert mode == "wal"


def test_schema_migration_recorded(tmp_db_conn):
    row = tmp_db_conn.execute(
        "SELECT version FROM schema_migrations WHERE version = 1"
    ).fetchone()
    assert row is not None


# ---------------------------------------------------------------------------
# Market CRUD
# ---------------------------------------------------------------------------

def test_upsert_market_insert(tmp_db_conn):
    market = make_market(ticker="TEST-001")
    upsert_market(tmp_db_conn, market)
    tmp_db_conn.commit()

    rows = get_markets_by_series(tmp_db_conn, "TEST")
    assert len(rows) == 1
    assert rows[0].ticker == "TEST-001"


def test_upsert_market_update(tmp_db_conn):
    market = make_market(ticker="TEST-001", result="yes")
    upsert_market(tmp_db_conn, market)
    tmp_db_conn.commit()

    updated = make_market(ticker="TEST-001", result="no")
    upsert_market(tmp_db_conn, updated)
    tmp_db_conn.commit()

    rows = get_markets_by_series(tmp_db_conn, "TEST")
    assert len(rows) == 1
    assert rows[0].result == "no"


def test_get_settled_markets(tmp_db_conn):
    upsert_market(tmp_db_conn, make_market(ticker="TEST-SETTLED", status="settled"))
    upsert_market(tmp_db_conn, make_market(ticker="TEST-OPEN", status="open"))
    tmp_db_conn.commit()

    settled = get_settled_markets(tmp_db_conn)
    tickers = {m.ticker for m in settled}
    assert "TEST-SETTLED" in tickers
    assert "TEST-OPEN" not in tickers


def test_get_settled_markets_by_series(tmp_db_conn):
    upsert_market(tmp_db_conn, make_market(ticker="TEST-001", series_ticker="TEST", status="settled"))
    upsert_market(tmp_db_conn, make_market(ticker="TESTD-001", series_ticker="TESTD", status="settled"))
    tmp_db_conn.commit()

    results = get_settled_markets(tmp_db_conn, series_ticker="TEST")
    assert all(m.series_ticker == "TEST" for m in results)


# ---------------------------------------------------------------------------
# Trade CRUD
# ---------------------------------------------------------------------------

def test_upsert_trade_insert(tmp_db_conn):
    upsert_market(tmp_db_conn, make_market())
    trade = make_trade()
    upsert_trade(tmp_db_conn, trade)
    tmp_db_conn.commit()

    trades = get_trades_for_ticker(tmp_db_conn, "TEST-MKT-001")
    assert len(trades) == 1
    assert trades[0].trade_id == "trade-001"


def test_upsert_trade_ignore_duplicate(tmp_db_conn):
    upsert_market(tmp_db_conn, make_market())
    trade = make_trade()
    upsert_trade(tmp_db_conn, trade)
    upsert_trade(tmp_db_conn, trade)  # duplicate — should be silently ignored
    tmp_db_conn.commit()

    trades = get_trades_for_ticker(tmp_db_conn, "TEST-MKT-001")
    assert len(trades) == 1


def test_upsert_trades_batch(tmp_db_conn):
    upsert_market(tmp_db_conn, make_market())
    trades = [make_trade(trade_id=f"t-{i}") for i in range(5)]
    inserted = upsert_trades_batch(tmp_db_conn, trades)
    tmp_db_conn.commit()
    assert inserted == 5


def test_upsert_trades_batch_empty(tmp_db_conn):
    inserted = upsert_trades_batch(tmp_db_conn, [])
    assert inserted == 0


# ---------------------------------------------------------------------------
# Analysis results
# ---------------------------------------------------------------------------

def test_save_analysis_result(tmp_db_conn):
    result = HypothesisResult(
        hypothesis_id="kalshi_hypothesis",
        run_id="test-run-id-001",
        run_timestamp=datetime(2025, 6, 1, tzinfo=timezone.utc),
        market="kalshi",
        status="exploratory",
        naive_n=100,
        naive_z_stat=-2.5,
        naive_p_value=0.012,
        naive_edge=-0.10,
        clustered_n=30,
        clustered_z_stat=-2.0,
        clustered_p_value=0.045,
        clustered_edge=-0.10,
        fee_adjusted_edge=-0.11,
        confidence_interval_low=0.20,
        confidence_interval_high=0.40,
        verdict="NO_EDGE",
        warnings=[],
    )
    save_analysis_result(tmp_db_conn, result)
    tmp_db_conn.commit()

    row = tmp_db_conn.execute(
        "SELECT run_id, verdict FROM analysis_results WHERE run_id = ?",
        ("test-run-id-001",),
    ).fetchone()
    assert row is not None
    assert row["verdict"] == "NO_EDGE"

    run_row = tmp_db_conn.execute(
        "SELECT hypothesis_id FROM hypothesis_runs WHERE run_id = ?",
        ("test-run-id-001",),
    ).fetchone()
    assert run_row["hypothesis_id"] == "kalshi_hypothesis"


def test_get_db_stats(tmp_db_conn):
    upsert_market(tmp_db_conn, make_market())
    tmp_db_conn.commit()
    stats = get_db_stats(tmp_db_conn)
    assert stats["markets"] == 1
    assert stats["trades"] == 0


def test_cache_size_pragma_set(tmp_db_conn):
    """_configure_connection() sets cache_size to -262144 (~256 MB)."""
    cache_size = tmp_db_conn.execute("PRAGMA cache_size").fetchone()[0]
    assert cache_size == -262144


# ---------------------------------------------------------------------------
# backtest_results table
# ---------------------------------------------------------------------------

def test_backtest_results_table_exists(tmp_db_conn):
    """backtest_results table is created by init_db."""
    row = tmp_db_conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='backtest_results'"
    ).fetchone()
    assert row is not None, "backtest_results table should exist after init_db"


def test_backtest_results_insert_and_query(tmp_db_conn):
    """Can insert and query a backtest result row."""
    tmp_db_conn.execute(
        """INSERT INTO backtest_results
           (task_id, series, strategies, run_timestamp, total_trades, wins, losses,
            net_pnl_cents, sharpe, max_drawdown_pct, win_rate, result_path)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        ("task-001", "SERIES_A", '["example"]', "2026-03-31T00:00:00Z",
         100, 60, 40, 500, 1.5, 5.0, 0.6, "reports/backtest_task-001.json"),
    )
    tmp_db_conn.commit()
    row = tmp_db_conn.execute(
        "SELECT * FROM backtest_results WHERE task_id = ?", ("task-001",)
    ).fetchone()
    assert row["series"] == "SERIES_A"
    assert row["sharpe"] == 1.5
