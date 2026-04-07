"""Tests for CoinbaseAdapter generalization."""

import pytest
pytest.importorskip("requests", reason="requests not installed")
from edge_catcher.adapters.coinbase import CoinbaseAdapter


def test_default_table_name():
    adapter = CoinbaseAdapter()
    assert adapter.table_name == "btc_ohlc"


def test_btc_usd_table_name():
    adapter = CoinbaseAdapter(product_id="BTC-USD")
    assert adapter.table_name == "btc_ohlc"


def test_sol_usd_table_name():
    adapter = CoinbaseAdapter(product_id="SOL-USD")
    assert adapter.table_name == "sol_ohlc"


def test_eth_usd_table_name():
    adapter = CoinbaseAdapter(product_id="ETH-USD")
    assert adapter.table_name == "eth_ohlc"


def test_xrp_usd_table_name():
    adapter = CoinbaseAdapter(product_id="XRP-USD")
    assert adapter.table_name == "xrp_ohlc"


def test_doge_usd_table_name():
    adapter = CoinbaseAdapter(product_id="DOGE-USD")
    assert adapter.table_name == "doge_ohlc"


def test_bnb_usd_table_name():
    adapter = CoinbaseAdapter(product_id="BNB-USD")
    assert adapter.table_name == "bnb_ohlc"


def test_url_contains_product_id():
    adapter = CoinbaseAdapter(product_id="SOL-USD")
    assert "SOL-USD" in adapter.base_url


def test_btc_url_contains_btc():
    adapter = CoinbaseAdapter(product_id="BTC-USD")
    assert "BTC-USD" in adapter.base_url


def test_product_id_stored():
    adapter = CoinbaseAdapter(product_id="XRP-USD")
    assert adapter.product_id == "XRP-USD"
