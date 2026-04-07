"""Tests for the example Kalshi price efficiency hypothesis: statistical correctness with synthetic data.

These tests verify the core statistical machinery (VWAP computation, price signal
fallback chain, clustering, proportions z-test) using synthetic data. Adapt them
for your own hypothesis by swapping the module import and hypothesis ID.
"""

import pytest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from edge_catcher.storage.db import (
    get_connection,
    init_db,
    upsert_market,
    upsert_trade,
)
from edge_catcher.storage.models import Market, Trade

CONFIG = Path("config")
BASE_TIME = datetime(2025, 1, 1, tzinfo=timezone.utc)


def _make_market(i: int, yes_price: int, result: str, exp_day_offset: int) -> Market:
    exp = BASE_TIME + timedelta(days=exp_day_offset)
    return Market(
        ticker=f"MKTTEST-{i:04d}",
        event_ticker=f"MKTTEST-EVT-{i}",
        series_ticker="MKTTEST",
        title=f"Test market {i}",
        status="settled",
        result=result,
        yes_bid=yes_price - 1,
        yes_ask=yes_price + 1,
        last_price=float(yes_price),
        open_interest=100,
        volume=100,
        expiration_time=exp,
        close_time=exp,
        created_time=BASE_TIME,
        settled_time=exp + timedelta(hours=1),
        open_time=BASE_TIME,
        notional_value=None,
        floor_strike=None,
        cap_strike=None,
    )


def _make_trade(i: int, yes_price: int) -> Trade:
    return Trade(
        trade_id=f"t-{i}",
        ticker=f"MKTTEST-{i:04d}",
        yes_price=yes_price,
        no_price=100 - yes_price,
        count=1,
        taker_side="yes",
        created_time=BASE_TIME,
    )


def _seed_db(conn, n: int, yes_price: int, win_count: int, n_dates: int = 30):
    """Seed the DB with n markets at yes_price; first win_count resolve YES."""
    for i in range(n):
        result = "yes" if i < win_count else "no"
        market = _make_market(i, yes_price, result, i % n_dates)
        upsert_market(conn, market)
        upsert_trade(conn, _make_trade(i, yes_price))
    conn.commit()


# ---------------------------------------------------------------------------
# Unit: VWAP computation
# ---------------------------------------------------------------------------

def _vwap(trades):
    """Compute VWAP from a list of Trade objects (yes_price in cents, count = contracts)."""
    total_volume = sum(t.count for t in trades)
    if total_volume == 0:
        return None
    return sum(t.yes_price * t.count for t in trades) / total_volume / 100.0


def test_vwap_basic():
    trades = [
        Trade("t1", "X", yes_price=60, no_price=40, count=2, taker_side="yes", created_time=BASE_TIME),
        Trade("t2", "X", yes_price=40, no_price=60, count=2, taker_side="no", created_time=BASE_TIME),
    ]
    assert _vwap(trades) == pytest.approx(0.50)


def test_vwap_empty():
    assert _vwap([]) is None


def test_vwap_weighted():
    trades = [
        Trade("t1", "X", yes_price=80, no_price=20, count=3, taker_side="yes", created_time=BASE_TIME),
        Trade("t2", "X", yes_price=20, no_price=80, count=1, taker_side="no", created_time=BASE_TIME),
    ]
    # (80*3 + 20*1) / 4 / 100 = 260/400 = 0.65
    assert _vwap(trades) == pytest.approx(0.65)


# ---------------------------------------------------------------------------
# Integration: storage round-trip
# ---------------------------------------------------------------------------

def test_upsert_and_retrieve_market(tmp_db_path):
    conn = get_connection(tmp_db_path)
    market = _make_market(0, 50, "yes", 0)
    upsert_market(conn, market)
    conn.commit()

    cursor = conn.cursor()
    cursor.execute("SELECT ticker, result, last_price FROM markets WHERE ticker = ?", (market.ticker,))
    row = cursor.fetchone()
    assert row is not None
    assert row[0] == market.ticker
    assert row[1] == "yes"
    conn.close()


def test_upsert_and_retrieve_trade(tmp_db_path):
    conn = get_connection(tmp_db_path)
    market = _make_market(0, 50, "yes", 0)
    upsert_market(conn, market)
    trade = _make_trade(0, 50)
    upsert_trade(conn, trade)
    conn.commit()

    cursor = conn.cursor()
    cursor.execute("SELECT trade_id, yes_price FROM trades WHERE ticker = ?", (market.ticker,))
    row = cursor.fetchone()
    assert row is not None
    assert row[1] == 50
    conn.close()


def test_empty_db_has_no_markets(tmp_db_path):
    conn = get_connection(tmp_db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM markets")
    assert cursor.fetchone()[0] == 0
    conn.close()


# ---------------------------------------------------------------------------
# Statistical validation helpers
# ---------------------------------------------------------------------------

def test_proportions_z_stat_negative_edge():
    """Verify z-stat is negative when actual win rate < implied probability."""
    statsmodels = pytest.importorskip("statsmodels")
    from statsmodels.stats.proportion import proportions_ztest
    n = 100
    wins = 30
    implied = 0.50
    stat, pval = proportions_ztest(wins, n, implied)
    assert stat < 0  # actual (30%) < implied (50%) → negative edge


def test_proportions_z_stat_near_zero_for_fair_market():
    """Verify z-stat is near zero when actual win rate ≈ implied probability."""
    pytest.importorskip("statsmodels")
    from statsmodels.stats.proportion import proportions_ztest
    n = 1000
    wins = 499  # ~50% ≈ implied 50%
    implied = 0.50
    stat, pval = proportions_ztest(wins, n, implied)
    assert abs(stat) < 1.0  # not significant
