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
	"""api/adapter_registry.py re-exports the clean AdapterMeta from base."""
	from api.adapter_registry import AdapterMeta as ApiAdapterMeta
	meta = ApiAdapterMeta(
		id="test",
		exchange="test_exchange",
		name="T",
		description="T",
		db_file="data/x.db",
		fee_model=ZERO_FEE,
		markets_yaml="config/markets.yaml",
	)
	assert meta.exchange == "test_exchange"


def test_adapter_meta_extra_stores_coinbase_product_id():
	"""Coinbase adapters stash product_id in extra rather than a typed field."""
	from edge_catcher.adapters.base import AdapterMeta
	meta = AdapterMeta(
		id="x", exchange="coinbase", name="X", description="X",
		db_file="data/x.db", fee_model=ZERO_FEE,
		extra={"product_id": "BTC-USD"},
	)
	assert meta.extra["product_id"] == "BTC-USD"


def test_adapter_meta_requires_exchange():
	from edge_catcher.adapters.base import AdapterMeta
	with pytest.raises(TypeError):
		AdapterMeta(  # type: ignore
			id="x", name="X", description="X",
			db_file="data/x.db", fee_model=ZERO_FEE,
		)


def test_adapter_meta_requires_db_file():
	from edge_catcher.adapters.base import AdapterMeta
	with pytest.raises(TypeError):
		AdapterMeta(  # type: ignore
			id="x", exchange="test", name="X", description="X",
			fee_model=ZERO_FEE,
		)


def test_adapter_meta_requires_fee_model():
	from edge_catcher.adapters.base import AdapterMeta
	with pytest.raises(TypeError):
		AdapterMeta(  # type: ignore
			id="x", exchange="test", name="X", description="X",
			db_file="data/x.db",
		)
