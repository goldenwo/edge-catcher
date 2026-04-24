"""Tests for storage layer: CRUD operations and WAL mode."""


from edge_catcher.storage.db import (
    get_connection,
    get_db_stats,
    get_markets_by_series,
    get_settled_markets,
    get_trades_for_ticker,
    upsert_market,
    upsert_trade,
    upsert_trades_batch,
)
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



def test_get_db_stats(tmp_db_conn):
    upsert_market(tmp_db_conn, make_market())
    tmp_db_conn.commit()
    stats = get_db_stats(tmp_db_conn)
    assert stats["markets"] == 1
    assert stats["trades"] == 0
    assert "results" not in stats


def test_cache_size_pragma_set(tmp_db_conn):
    """_configure_connection() sets cache_size to -262144 (~256 MB)."""
    cache_size = tmp_db_conn.execute("PRAGMA cache_size").fetchone()[0]
    assert cache_size == -262144
