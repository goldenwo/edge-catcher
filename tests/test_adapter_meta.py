"""Tests for AdapterMeta — the shared adapter metadata dataclass.

See docs/superpowers/specs/2026-04-16-adapter-registry-design.md.
"""
from __future__ import annotations

import pytest

from edge_catcher.adapters.base import AdapterMeta
from edge_catcher.fees import ZERO_FEE


def test_adapter_meta_can_be_constructed_with_required_fields():
	meta = AdapterMeta(
		id="test_adapter",
		exchange="test",
		name="Test Adapter",
		description="Test",
		db_file="data/test.db",
		fee_model=ZERO_FEE,
	)
	assert meta.id == "test_adapter"
	assert meta.exchange == "test"
	assert meta.db_file == "data/test.db"
	assert meta.fee_model is ZERO_FEE
	assert meta.extra == {}


def test_adapter_meta_in_api_has_exchange_field():
	"""api/adapter_registry.py must have the new exchange field so the
	existing ADAPTERS list can be tagged during the transition."""
	from api.adapter_registry import AdapterMeta as ApiAdapterMeta
	meta = ApiAdapterMeta(
		id="test",
		exchange="test_exchange",
		name="T",
		description="T",
		markets_yaml="config/markets.yaml",
	)
	assert meta.exchange == "test_exchange"


def test_coinbase_product_id_syncs_with_extra():
	"""During transition both forms must work — old coinbase_product_id=
	and new extra={'product_id': ...}."""
	from api.adapter_registry import AdapterMeta as ApiAdapterMeta

	# New form
	m1 = ApiAdapterMeta(
		id="x", exchange="coinbase", name="X", description="X",
		db_file="data/x.db",
		extra={"product_id": "BTC-USD"},
	)
	assert m1.coinbase_product_id == "BTC-USD"

	# Old form
	m2 = ApiAdapterMeta(
		id="y", exchange="coinbase", name="Y", description="Y",
		db_file="data/y.db",
		coinbase_product_id="ETH-USD",
	)
	assert m2.extra.get("product_id") == "ETH-USD"
