"""Tests for init_ohlc_table and backward-compat init_btc_ohlc_table."""

import sqlite3
import pytest

from edge_catcher.storage.db import init_ohlc_table, init_btc_ohlc_table


@pytest.fixture
def mem_conn():
    """In-memory SQLite connection for fast table creation tests."""
    conn = sqlite3.connect(":memory:")
    yield conn
    conn.close()


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,)
    ).fetchone()
    return row is not None


def _index_exists(conn: sqlite3.Connection, index_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name=?", (index_name,)
    ).fetchone()
    return row is not None


def test_init_ohlc_table_creates_sol_ohlc(mem_conn):
    init_ohlc_table(mem_conn, "sol_ohlc")
    assert _table_exists(mem_conn, "sol_ohlc")


def test_init_ohlc_table_creates_index(mem_conn):
    init_ohlc_table(mem_conn, "sol_ohlc")
    assert _index_exists(mem_conn, "idx_sol_ohlc_ts")


def test_init_ohlc_table_default_creates_btc_ohlc(mem_conn):
    init_ohlc_table(mem_conn)
    assert _table_exists(mem_conn, "btc_ohlc")


def test_init_ohlc_table_idempotent(mem_conn):
    """Calling init_ohlc_table twice should not raise."""
    init_ohlc_table(mem_conn, "eth_ohlc")
    init_ohlc_table(mem_conn, "eth_ohlc")
    assert _table_exists(mem_conn, "eth_ohlc")


def test_init_btc_ohlc_table_backward_compat(mem_conn):
    init_btc_ohlc_table(mem_conn)
    assert _table_exists(mem_conn, "btc_ohlc")
    assert _index_exists(mem_conn, "idx_btc_ohlc_ts")


def test_init_ohlc_table_correct_schema(mem_conn):
    init_ohlc_table(mem_conn, "xrp_ohlc")
    # Insert a row to verify schema matches expected columns
    mem_conn.execute(
        "INSERT INTO xrp_ohlc (timestamp, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?)",
        (1700000000, 0.5, 0.6, 0.4, 0.55, 1000.0),
    )
    row = mem_conn.execute("SELECT * FROM xrp_ohlc WHERE timestamp=1700000000").fetchone()
    assert row is not None
