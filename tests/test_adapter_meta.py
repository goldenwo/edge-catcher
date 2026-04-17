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
