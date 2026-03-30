import sqlite3
import json
import logging
from pathlib import Path
from typing import Optional, List
from contextlib import contextmanager
from datetime import datetime, timezone

from edge_catcher.storage.models import Market, Trade, HypothesisResult

logger = logging.getLogger(__name__)

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS markets (
    ticker TEXT PRIMARY KEY,
    event_ticker TEXT,
    series_ticker TEXT,
    title TEXT,
    status TEXT,
    result TEXT,
    yes_bid REAL,
    yes_ask REAL,
    last_price REAL,
    open_interest INTEGER,
    volume INTEGER,
    expiration_time TEXT,
    close_time TEXT,
    created_time TEXT,
    settled_time TEXT,
    open_time TEXT,
    notional_value REAL,
    floor_strike REAL,
    cap_strike REAL,
    raw_data TEXT,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS trades (
    trade_id TEXT PRIMARY KEY,
    ticker TEXT NOT NULL,
    yes_price INTEGER NOT NULL,
    no_price INTEGER NOT NULL,
    count INTEGER NOT NULL,
    taker_side TEXT NOT NULL,
    created_time TEXT NOT NULL,
    raw_data TEXT,
    FOREIGN KEY (ticker) REFERENCES markets(ticker)
);

CREATE TABLE IF NOT EXISTS candlesticks (
    ticker TEXT NOT NULL,
    period_start TEXT NOT NULL,
    period_end TEXT NOT NULL,
    open_price INTEGER,
    close_price INTEGER,
    high_price INTEGER,
    low_price INTEGER,
    volume INTEGER,
    vwap REAL,
    PRIMARY KEY (ticker, period_start)
);

CREATE TABLE IF NOT EXISTS analysis_results (
    run_id TEXT PRIMARY KEY,
    hypothesis_id TEXT NOT NULL,
    run_timestamp TEXT NOT NULL,
    market TEXT NOT NULL,
    status TEXT NOT NULL,
    naive_n INTEGER,
    naive_z_stat REAL,
    naive_p_value REAL,
    naive_edge REAL,
    clustered_n INTEGER,
    clustered_z_stat REAL,
    clustered_p_value REAL,
    clustered_edge REAL,
    fee_adjusted_edge REAL,
    confidence_interval_low REAL,
    confidence_interval_high REAL,
    verdict TEXT,
    warnings TEXT,
    total_markets_seen INTEGER,
    delisted_or_cancelled INTEGER,
    raw_bucket_data TEXT
);

CREATE TABLE IF NOT EXISTS hypothesis_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    hypothesis_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    run_timestamp TEXT NOT NULL,
    verdict TEXT,
    UNIQUE(run_id)
);
"""

_INDEXES_SQL = """
CREATE INDEX IF NOT EXISTS idx_trades_ticker ON trades(ticker);
CREATE INDEX IF NOT EXISTS idx_trades_ticker_time ON trades(ticker, created_time);
CREATE INDEX IF NOT EXISTS idx_markets_series ON markets(series_ticker);
CREATE INDEX IF NOT EXISTS idx_markets_status ON markets(status);
CREATE INDEX IF NOT EXISTS idx_analysis_hypothesis ON analysis_results(hypothesis_id, run_timestamp);
CREATE INDEX IF NOT EXISTS idx_hypothesis_runs_id ON hypothesis_runs(hypothesis_id);
"""

_BTC_OHLC_SQL = """
CREATE TABLE IF NOT EXISTS btc_ohlc (
    timestamp INTEGER PRIMARY KEY,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_btc_ohlc_ts ON btc_ohlc (timestamp);
"""


def init_btc_ohlc_table(conn: sqlite3.Connection) -> None:
    """Create btc_ohlc table and index if they don't exist."""
    conn.executescript(_BTC_OHLC_SQL)


def _configure_connection(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size = -32768")  # Cap page cache at 32 MB
    conn.row_factory = sqlite3.Row


def _dt_to_str(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        # Treat naive datetimes as UTC
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


def _str_to_dt(s: Optional[str]) -> Optional[datetime]:
    if s is None:
        return None
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def init_db(db_path: Path) -> None:
    """Create all tables, indexes, enable WAL, and record migration version 1."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Initializing database at %s", db_path)
    conn = sqlite3.connect(str(db_path))
    try:
        _configure_connection(conn)
        # executescript() issues an implicit COMMIT before running — only call on a fresh connection
        conn.executescript(_SCHEMA_SQL)
        # executescript() issues an implicit COMMIT before running — only call on a fresh connection
        conn.executescript(_INDEXES_SQL)
        init_btc_ohlc_table(conn)
        # Record migration version 1 if not already present
        conn.execute(
            "INSERT OR IGNORE INTO schema_migrations (version, applied_at) VALUES (1, ?)",
            (datetime.now(timezone.utc).isoformat(),),
        )
        conn.commit()
        logger.info("Database initialized successfully")
    finally:
        conn.close()


def get_connection(db_path: Path) -> sqlite3.Connection:
    """Open a connection with WAL configuration applied."""
    conn = sqlite3.connect(str(db_path), timeout=30)
    _configure_connection(conn)
    return conn


@contextmanager
def get_db(db_path: Path):
    """Context manager yielding a configured connection; commits on exit, rolls back on exception."""
    conn = get_connection(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def upsert_market(conn: sqlite3.Connection, market: Market) -> None:
    """Insert or replace a market record (markets are mutable)."""
    conn.execute(
        """
        INSERT OR REPLACE INTO markets (
            ticker, event_ticker, series_ticker, title, status, result,
            yes_bid, yes_ask, last_price, open_interest, volume,
            expiration_time, close_time, created_time, settled_time, open_time,
            notional_value, floor_strike, cap_strike, raw_data,
            updated_at
        ) VALUES (
            ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?,
            datetime('now')
        )
        """,
        (
            market.ticker,
            market.event_ticker,
            market.series_ticker,
            market.title,
            market.status,
            market.result,
            market.yes_bid,
            market.yes_ask,
            market.last_price,
            market.open_interest,
            market.volume,
            _dt_to_str(market.expiration_time),
            _dt_to_str(market.close_time),
            _dt_to_str(market.created_time),
            _dt_to_str(market.settled_time),
            _dt_to_str(market.open_time),
            market.notional_value,
            market.floor_strike,
            market.cap_strike,
            market.raw_data,
        ),
    )
    logger.debug("Upserted market %s", market.ticker)


def upsert_trade(conn: sqlite3.Connection, trade: Trade) -> None:
    """Insert a trade; silently ignore if trade_id already exists (trades are immutable)."""
    conn.execute(
        """
        INSERT OR IGNORE INTO trades (
            trade_id, ticker, yes_price, no_price, count, taker_side, created_time, raw_data
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            trade.trade_id,
            trade.ticker,
            trade.yes_price,
            trade.no_price,
            trade.count,
            trade.taker_side,
            _dt_to_str(trade.created_time),
            trade.raw_data,
        ),
    )
    logger.debug("Upserted trade %s", trade.trade_id)


def upsert_trades_batch(conn: sqlite3.Connection, trades: List[Trade]) -> int:
    """Batch insert trades using INSERT OR IGNORE. Returns the count of newly inserted rows."""
    if not trades:
        return 0
    rows = [
        (
            t.trade_id,
            t.ticker,
            t.yes_price,
            t.no_price,
            t.count,
            t.taker_side,
            _dt_to_str(t.created_time),
            t.raw_data,
        )
        for t in trades
    ]
    cursor = conn.executemany(
        """
        INSERT OR IGNORE INTO trades (
            trade_id, ticker, yes_price, no_price, count, taker_side, created_time, raw_data
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    inserted = cursor.rowcount
    logger.debug("Batch upserted %d/%d trades", inserted, len(trades))
    return inserted


def get_trades_for_ticker(
    conn: sqlite3.Connection,
    ticker: str,
    since: Optional[datetime] = None,
) -> List[Trade]:
    """Return all trades for a ticker, optionally filtered to those after `since`."""
    if since is not None:
        cursor = conn.execute(
            """
            SELECT trade_id, ticker, yes_price, no_price, count, taker_side, created_time, raw_data
            FROM trades
            WHERE ticker = ? AND created_time >= ?
            ORDER BY created_time ASC
            """,
            (ticker, _dt_to_str(since)),
        )
    else:
        cursor = conn.execute(
            """
            SELECT trade_id, ticker, yes_price, no_price, count, taker_side, created_time, raw_data
            FROM trades
            WHERE ticker = ?
            ORDER BY created_time ASC
            """,
            (ticker,),
        )
    rows = cursor.fetchall()
    return [
        Trade(
            trade_id=row["trade_id"],
            ticker=row["ticker"],
            yes_price=row["yes_price"],
            no_price=row["no_price"],
            count=row["count"],
            taker_side=row["taker_side"],
            created_time=_str_to_dt(row["created_time"]),
            raw_data=row["raw_data"],
        )
        for row in rows
    ]


def _row_to_market(row: sqlite3.Row) -> Market:
    return Market(
        ticker=row["ticker"],
        event_ticker=row["event_ticker"],
        series_ticker=row["series_ticker"],
        title=row["title"],
        status=row["status"],
        result=row["result"],
        yes_bid=row["yes_bid"],
        yes_ask=row["yes_ask"],
        last_price=row["last_price"],
        open_interest=row["open_interest"],
        volume=row["volume"],
        expiration_time=_str_to_dt(row["expiration_time"]),
        close_time=_str_to_dt(row["close_time"]),
        created_time=_str_to_dt(row["created_time"]),
        settled_time=_str_to_dt(row["settled_time"]),
        open_time=_str_to_dt(row["open_time"]),
        notional_value=row["notional_value"],
        floor_strike=row["floor_strike"],
        cap_strike=row["cap_strike"],
        raw_data=row["raw_data"],
    )


def get_markets_by_series(conn: sqlite3.Connection, series_ticker: str) -> List[Market]:
    """Return all markets for a given series ticker."""
    cursor = conn.execute(
        "SELECT * FROM markets WHERE series_ticker = ?",
        (series_ticker,),
    )
    return [_row_to_market(row) for row in cursor.fetchall()]


def get_settled_markets(
    conn: sqlite3.Connection,
    series_ticker: Optional[str] = None,
) -> List[Market]:
    """Return all settled markets, optionally filtered to a series."""
    if series_ticker is not None:
        cursor = conn.execute(
            "SELECT * FROM markets WHERE status = 'settled' AND series_ticker = ?",
            (series_ticker,),
        )
    else:
        cursor = conn.execute(
            "SELECT * FROM markets WHERE status = 'settled'",
        )
    return [_row_to_market(row) for row in cursor.fetchall()]


def save_analysis_result(conn: sqlite3.Connection, result: HypothesisResult) -> None:
    """Persist a HypothesisResult to analysis_results and hypothesis_runs."""
    warnings_json = json.dumps(result.warnings) if result.warnings else "[]"

    conn.execute(
        """
        INSERT OR REPLACE INTO analysis_results (
            run_id, hypothesis_id, run_timestamp, market, status,
            naive_n, naive_z_stat, naive_p_value, naive_edge,
            clustered_n, clustered_z_stat, clustered_p_value, clustered_edge,
            fee_adjusted_edge, confidence_interval_low, confidence_interval_high,
            verdict, warnings, total_markets_seen, delisted_or_cancelled, raw_bucket_data
        ) VALUES (
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?, ?, ?
        )
        """,
        (
            result.run_id,
            result.hypothesis_id,
            _dt_to_str(result.run_timestamp),
            result.market,
            result.status,
            result.naive_n,
            result.naive_z_stat,
            result.naive_p_value,
            result.naive_edge,
            result.clustered_n,
            result.clustered_z_stat,
            result.clustered_p_value,
            result.clustered_edge,
            result.fee_adjusted_edge,
            result.confidence_interval_low,
            result.confidence_interval_high,
            result.verdict,
            warnings_json,
            result.total_markets_seen,
            result.delisted_or_cancelled,
            result.raw_bucket_data,
        ),
    )

    conn.execute(
        """
        INSERT OR IGNORE INTO hypothesis_runs (hypothesis_id, run_id, run_timestamp, verdict)
        VALUES (?, ?, ?, ?)
        """,
        (
            result.hypothesis_id,
            result.run_id,
            _dt_to_str(result.run_timestamp),
            result.verdict,
        ),
    )
    logger.debug("Saved analysis result %s for hypothesis %s", result.run_id, result.hypothesis_id)


def get_analysis_history(
    conn: sqlite3.Connection,
    hypothesis_id: str,
    limit: int = 20,
) -> List[dict]:
    """Return the most recent analysis results for a hypothesis as plain dicts."""
    cursor = conn.execute(
        """
        SELECT * FROM analysis_results
        WHERE hypothesis_id = ?
        ORDER BY run_timestamp DESC
        LIMIT ?
        """,
        (hypothesis_id, limit),
    )
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


def get_db_stats(conn: sqlite3.Connection) -> dict:
    """Return basic database statistics."""
    markets_count = conn.execute("SELECT COUNT(*) FROM markets").fetchone()[0]
    trades_count = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
    results_count = conn.execute("SELECT COUNT(*) FROM analysis_results").fetchone()[0]

    # Determine DB file size via PRAGMA database_list
    db_path_row = conn.execute("PRAGMA database_list").fetchone()
    db_size_mb = 0.0
    if db_path_row:
        db_file = Path(db_path_row[2])
        if db_file.exists():
            db_size_mb = db_file.stat().st_size / (1024 * 1024)

    return {
        "markets": markets_count,
        "trades": trades_count,
        "results": results_count,
        "db_size_mb": round(db_size_mb, 4),
    }
