import gzip
import csv
import logging
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger(__name__)


def archive_old_trades(
    conn,
    archive_dir: Path,
    days_to_keep: int = 90,
) -> dict:
    """
    Export trades older than `days_to_keep` to a gzip CSV in `archive_dir`,
    delete them from the DB, and return summary stats.

    Returns: {rows_archived: int, rows_deleted: int, archive_file: str}
    """
    archive_dir = Path(archive_dir)
    archive_dir.mkdir(parents=True, exist_ok=True)

    exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='trades'"
    ).fetchone()
    if not exists:
        return {"rows_archived": 0, "rows_deleted": 0, "archive_file": ""}

    cutoff = (datetime.now(timezone.utc) - timedelta(days=days_to_keep)).isoformat()
    cursor = conn.execute(
        """
        SELECT trade_id, ticker, yes_price, no_price, count, taker_side, created_time, raw_data
        FROM trades
        WHERE created_time < ?
        ORDER BY created_time ASC
        """,
        (cutoff,),
    )
    rows = cursor.fetchall()

    if not rows:
        logger.info("No trades older than %d days to archive", days_to_keep)
        return {"rows_archived": 0, "rows_deleted": 0, "archive_file": ""}

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    archive_path = archive_dir / f"trades_{timestamp}.csv.gz"
    tmp_path = archive_path.with_suffix(".csv.gz.tmp")

    fieldnames = ["trade_id", "ticker", "yes_price", "no_price", "count", "taker_side", "created_time", "raw_data"]

    try:
        with gzip.open(str(tmp_path), "wt", newline="", encoding="utf-8") as gz:
            writer = csv.DictWriter(gz, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow({k: row[k] for k in fieldnames})

        rows_archived = len(rows)

        trade_ids = [row["trade_id"] for row in rows]
        # Delete in batches to avoid hitting SQLite's variable limit
        batch_size = 500
        rows_deleted = 0
        try:
            for i in range(0, len(trade_ids), batch_size):
                batch = trade_ids[i : i + batch_size]
                placeholders = ",".join("?" * len(batch))
                result = conn.execute(
                    f"DELETE FROM trades WHERE trade_id IN ({placeholders})", batch
                )
                rows_deleted += result.rowcount
            conn.commit()
        except Exception:
            conn.rollback()
            raise

        tmp_path.rename(archive_path)
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink()
        raise

    logger.info(
        "Archived %d trades to %s, deleted %d rows",
        rows_archived,
        archive_path,
        rows_deleted,
    )
    return {
        "rows_archived": rows_archived,
        "rows_deleted": rows_deleted,
        "archive_file": str(archive_path),
    }


def archive_old_markets(
    conn,
    archive_dir: Path,
    days_to_keep: int = 90,
) -> dict:
    """
    Export settled/closed markets whose updated_at is older than `days_to_keep`
    to a gzip CSV in `archive_dir`, delete them from the DB, and return summary stats.

    Returns: {rows_archived: int, rows_deleted: int, archive_file: str}
    """
    archive_dir = Path(archive_dir)
    archive_dir.mkdir(parents=True, exist_ok=True)

    exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='markets'"
    ).fetchone()
    if not exists:
        return {"rows_archived": 0, "rows_deleted": 0, "archive_file": ""}

    cutoff = (datetime.now(timezone.utc) - timedelta(days=days_to_keep)).isoformat()
    cursor = conn.execute(
        """
        SELECT ticker, event_ticker, series_ticker, title, status, result,
               yes_bid, yes_ask, last_price, open_interest, volume,
               expiration_time, close_time, created_time, settled_time, open_time,
               notional_value, floor_strike, cap_strike, raw_data, updated_at
        FROM markets
        WHERE status IN ('settled', 'closed') AND updated_at < ?
        ORDER BY updated_at ASC
        """,
        (cutoff,),
    )
    rows = cursor.fetchall()

    if not rows:
        logger.info("No settled/closed markets older than %d days to archive", days_to_keep)
        return {"rows_archived": 0, "rows_deleted": 0, "archive_file": ""}

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    archive_path = archive_dir / f"markets_{timestamp}.csv.gz"
    tmp_path = archive_path.with_suffix(".csv.gz.tmp")

    fieldnames = [
        "ticker", "event_ticker", "series_ticker", "title", "status", "result",
        "yes_bid", "yes_ask", "last_price", "open_interest", "volume",
        "expiration_time", "close_time", "created_time", "settled_time", "open_time",
        "notional_value", "floor_strike", "cap_strike", "raw_data", "updated_at",
    ]

    try:
        with gzip.open(str(tmp_path), "wt", newline="", encoding="utf-8") as gz:
            writer = csv.DictWriter(gz, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow({k: row[k] for k in fieldnames})

        rows_archived = len(rows)

        tickers = [row["ticker"] for row in rows]
        batch_size = 500
        rows_deleted = 0
        try:
            for i in range(0, len(tickers), batch_size):
                batch = tickers[i : i + batch_size]
                placeholders = ",".join("?" * len(batch))
                result = conn.execute(
                    f"DELETE FROM markets WHERE ticker IN ({placeholders})", batch
                )
                rows_deleted += result.rowcount
            conn.commit()
        except Exception:
            conn.rollback()
            raise

        tmp_path.rename(archive_path)
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink()
        raise

    logger.info(
        "Archived %d markets to %s, deleted %d rows",
        rows_archived,
        archive_path,
        rows_deleted,
    )
    return {
        "rows_archived": rows_archived,
        "rows_deleted": rows_deleted,
        "archive_file": str(archive_path),
    }


def vacuum_db(conn: sqlite3.Connection) -> None:
    """Run VACUUM to reclaim free pages and defragment the database file."""
    logger.info("Running VACUUM on database")
    old_isolation = conn.isolation_level
    try:
        conn.isolation_level = None  # autocommit mode required for VACUUM
        conn.execute("VACUUM")
        logger.info("VACUUM complete")
    finally:
        conn.isolation_level = old_isolation


def get_size_report(db_path: Path, archive_dir: Optional[Path] = None) -> dict:
    """
    Return a size report for the database and optional archive directory.

    Returns: {db_size_mb: float, archive_size_mb: float, total_mb: float}
    """
    db_path = Path(db_path)
    db_size_bytes = db_path.stat().st_size if db_path.exists() else 0

    archive_size_bytes = 0
    if archive_dir is not None:
        archive_dir = Path(archive_dir)
        if archive_dir.exists():
            for f in archive_dir.iterdir():
                if f.is_file():
                    archive_size_bytes += f.stat().st_size

    db_size_mb = db_size_bytes / (1024 * 1024)
    archive_size_mb = archive_size_bytes / (1024 * 1024)

    return {
        "db_size_mb": round(db_size_mb, 4),
        "archive_size_mb": round(archive_size_mb, 4),
        "total_mb": round(db_size_mb + archive_size_mb, 4),
    }
