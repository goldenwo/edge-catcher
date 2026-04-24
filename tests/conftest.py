"""Shared fixtures for edge_catcher tests."""

from datetime import datetime, timezone

import pytest

from edge_catcher.storage.db import get_connection, init_db
from edge_catcher.storage.models import Market, Trade


# ---------------------------------------------------------------------------
# DB fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_db_path(tmp_path):
    """Return a path to a freshly initialised SQLite DB."""
    db_path = tmp_path / "test.db"
    init_db(db_path)
    return db_path


@pytest.fixture
def tmp_db_conn(tmp_db_path):
    """Open connection to a fresh DB; close after test."""
    conn = get_connection(tmp_db_path)
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Sample data factories
# ---------------------------------------------------------------------------

def make_market(
    ticker: str = "TEST-MKT-001",
    series_ticker: str = "TEST",
    status: str = "settled",
    result: str = "yes",
    last_price: float = 50.0,
    expiration_time: datetime = None,
) -> Market:
    if expiration_time is None:
        expiration_time = datetime(2025, 6, 1, tzinfo=timezone.utc)
    return Market(
        ticker=ticker,
        event_ticker=f"EVT-{ticker}",
        series_ticker=series_ticker,
        title=f"Test market {ticker}",
        status=status,
        result=result,
        yes_bid=49,
        yes_ask=51,
        last_price=last_price,
        open_interest=100,
        volume=100,
        expiration_time=expiration_time,
        close_time=expiration_time,
        created_time=datetime(2025, 1, 1, tzinfo=timezone.utc),
        settled_time=expiration_time,
        open_time=datetime(2025, 1, 1, tzinfo=timezone.utc),
        notional_value=None,
        floor_strike=None,
        cap_strike=None,
    )


def make_trade(
    trade_id: str = "trade-001",
    ticker: str = "TEST-MKT-001",
    yes_price: int = 50,
    count: int = 1,
) -> Trade:
    return Trade(
        trade_id=trade_id,
        ticker=ticker,
        yes_price=yes_price,
        no_price=100 - yes_price,
        count=count,
        taker_side="yes",
        created_time=datetime(2025, 1, 15, tzinfo=timezone.utc),
    )
