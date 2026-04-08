"""Tests for KalshiAdapter: schema validation with mock responses."""

import sys
import time
import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path

pytest.importorskip("requests", reason="requests not installed")
from edge_catcher.adapters.kalshi import KalshiAdapter


CONFIG_PATH = Path("config.local/markets.yaml")


@pytest.fixture
def adapter():
    return KalshiAdapter(config_path=CONFIG_PATH)


# ---------------------------------------------------------------------------
# validate_response
# ---------------------------------------------------------------------------

def test_validate_markets_list_passes(adapter):
    data = {
        "markets": [
            {"ticker": "KXBTC-001", "status": "settled", "yes_bid": 40, "yes_ask": 45}
        ],
        "cursor": "",
    }
    assert adapter.validate_response(data, "markets_list") is True


def test_validate_trades_list_passes(adapter):
    data = {
        "trades": [
            {
                "trade_id": "t1",
                "ticker": "KXBTC-001",
                "yes_price": 50,
                "no_price": 50,
                "count": 5,
                "taker_side": "yes",
                "created_time": "2025-01-01T00:00:00Z",
            }
        ],
        "cursor": "",
    }
    assert adapter.validate_response(data, "trades_list") is True


def test_validate_market_detail_passes(adapter):
    data = {"market": {"ticker": "KXBTC-001", "status": "settled"}}
    assert adapter.validate_response(data, "market_detail") is True


def test_validate_markets_list_missing_required_key(adapter):
    with pytest.raises(ValueError, match="missing required keys"):
        adapter.validate_response({"cursor": ""}, "markets_list")


def test_validate_trades_list_missing_required_key(adapter):
    with pytest.raises(ValueError, match="missing required keys"):
        adapter.validate_response({"cursor": ""}, "trades_list")


def test_validate_unknown_schema_raises(adapter):
    with pytest.raises(ValueError, match="Unknown schema key"):
        adapter.validate_response({}, "nonexistent_schema")


def test_validate_empty_list_does_not_crash(adapter):
    """An empty list passes top-level validation (no item fields to check)."""
    data = {"markets": [], "cursor": ""}
    assert adapter.validate_response(data, "markets_list") is True


def test_validate_missing_item_fields_logs_warning(adapter, caplog):
    """Missing item-level fields emit a warning, not an error."""
    import logging
    data = {"markets": [{"ticker": "KXBTC-001"}]}  # missing status, yes_bid, yes_ask
    with caplog.at_level(logging.WARNING):
        result = adapter.validate_response(data, "markets_list")
    assert result is True
    assert "missing expected fields" in caplog.text


# ---------------------------------------------------------------------------
# _parse_market
# ---------------------------------------------------------------------------

def test_parse_market_minimal(adapter):
    raw = {
        "ticker": "KXBTC-001",
        "event_ticker": "KXBTC-EVT",
        "series_ticker": "KXBTC",
        "title": "BTC test",
        "status": "settled",
        "result": "yes",
    }
    market = adapter._parse_market(raw)
    assert market.ticker == "KXBTC-001"
    assert market.result == "yes"
    assert market.expiration_time is None


def test_parse_market_with_datetime(adapter):
    raw = {
        "ticker": "KXBTC-001",
        "expiration_time": "2025-06-01T12:00:00Z",
    }
    market = adapter._parse_market(raw)
    assert market.expiration_time is not None
    assert market.expiration_time.year == 2025


# ---------------------------------------------------------------------------
# _parse_trade
# ---------------------------------------------------------------------------

def test_parse_trade_valid(adapter):
    # Kalshi API uses _dollars (string) and _fp (string) suffixes for numeric fields
    raw = {
        "trade_id": "t1",
        "ticker": "KXBTC-001",
        "yes_price_dollars": "0.5500",
        "no_price_dollars": "0.4500",
        "count_fp": "10.00",
        "taker_side": "yes",
        "created_time": "2025-01-15T10:00:00Z",
    }
    trade = adapter._parse_trade(raw)
    assert trade is not None
    assert trade.yes_price == 55
    assert trade.count == 10


def test_parse_trade_missing_created_time(adapter):
    """Trades with missing created_time return None (caller must handle)."""
    raw = {
        "trade_id": "t1",
        "ticker": "KXBTC-001",
        "yes_price": 55,
        "no_price": 45,
        "count": 10,
        "taker_side": "yes",
        "created_time": "",
    }
    assert adapter._parse_trade(raw) is None


def test_parse_trade_bad_datetime(adapter):
    raw = {
        "trade_id": "t1",
        "ticker": "KXBTC-001",
        "yes_price": 55,
        "no_price": 45,
        "count": 10,
        "taker_side": "yes",
        "created_time": "NOT_A_DATE",
    }
    assert adapter._parse_trade(raw) is None


# ---------------------------------------------------------------------------
# _check_memory
# ---------------------------------------------------------------------------

def test_check_memory_sleeps_when_ram_low(adapter):
    """_check_memory() calls time.sleep(30) when available RAM < threshold %."""
    mock_vm = MagicMock()
    mock_vm.total = 32 * 1024 ** 3  # 32 GB
    mock_vm.available = int(0.02 * mock_vm.total)  # 2% free — below 5% threshold

    mock_psutil = MagicMock()
    mock_psutil.virtual_memory.return_value = mock_vm

    with patch.dict(sys.modules, {"psutil": mock_psutil}):
        with patch("edge_catcher.adapters.kalshi.time.sleep") as mock_sleep:
            adapter._check_memory()

    mock_sleep.assert_called_once_with(30)


def test_check_memory_no_sleep_when_ram_ok(adapter):
    """_check_memory() does not sleep when available RAM >= threshold %."""
    mock_vm = MagicMock()
    mock_vm.total = 32 * 1024 ** 3  # 32 GB
    mock_vm.available = int(0.50 * mock_vm.total)  # 50% free — well above 5%

    mock_psutil = MagicMock()
    mock_psutil.virtual_memory.return_value = mock_vm

    with patch.dict(sys.modules, {"psutil": mock_psutil}):
        with patch("edge_catcher.adapters.kalshi.time.sleep") as mock_sleep:
            adapter._check_memory()

    mock_sleep.assert_not_called()


def test_check_memory_silent_when_psutil_missing(adapter):
    """_check_memory() passes silently when psutil is not installed."""
    with patch.dict(sys.modules, {"psutil": None}):
        # Should not raise
        adapter._check_memory()
