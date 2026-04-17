from edge_catcher.adapters.kalshi.registry import KALSHI_ADAPTERS
from edge_catcher.adapters.coinbase.registry import COINBASE_ADAPTERS


def test_kalshi_registry_contains_btc_adapter():
	ids = [a.id for a in KALSHI_ADAPTERS]
	assert "kalshi_btc" in ids


def test_coinbase_registry_contains_5_products():
	ids = [a.id for a in COINBASE_ADAPTERS]
	assert len(COINBASE_ADAPTERS) == 5
	assert "coinbase_btc" in ids


def test_all_adapters_have_exchange_tag():
	for meta in KALSHI_ADAPTERS:
		assert meta.exchange == "kalshi"
	for meta in COINBASE_ADAPTERS:
		assert meta.exchange == "coinbase"


def test_central_adapters_list_is_concat_of_per_exchange_lists():
	from api.adapter_registry import ADAPTERS
	from edge_catcher.adapters.kalshi.registry import KALSHI_ADAPTERS
	from edge_catcher.adapters.coinbase.registry import COINBASE_ADAPTERS

	expected_ids = [a.id for a in (*KALSHI_ADAPTERS, *COINBASE_ADAPTERS)]
	actual_ids = [a.id for a in ADAPTERS]
	assert actual_ids == expected_ids
