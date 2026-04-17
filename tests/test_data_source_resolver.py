"""Tests for DataSourceResolver."""

import pytest
from edge_catcher.research.data_source_resolver import (
	DataSourceResolver,
	PrimarySource,
	ResolvedSource,
)
from edge_catcher.research.data_source_config import PrimaryEntry, DataSourceConfig
from edge_catcher.adapters.kalshi.fees import STANDARD_FEE, INDEX_FEE
from edge_catcher.fees import ZERO_FEE


@pytest.fixture
def resolver():
	"""Resolver with a fake fee lookup."""
	fee_map = {
		"data/exchange.db": STANDARD_FEE,
		"data/exchange-alt.db": STANDARD_FEE,
		"data/ohlc.db": ZERO_FEE,
	}

	def _lookup(db_path: str, series: str | None = None) -> 'FeeModel':
		if db_path == "data/exchange-fin.db" and series and series.startswith("IDX"):
			return INDEX_FEE
		return fee_map.get(db_path, STANDARD_FEE)

	return DataSourceResolver(fee_model_lookup=_lookup, data_dir="data")


class TestResolveSinglePrimary:
	def test_basic_resolution(self, resolver):
		class MockHypothesis:
			data_sources = DataSourceConfig(
				primaries=[PrimaryEntry(db="exchange.db", series="SERIES_A")]
			)
		result = resolver.resolve(MockHypothesis())
		assert len(result.primaries) == 1
		assert result.primaries[0].db_path == "data/exchange.db"
		assert result.primaries[0].series == "SERIES_A"
		assert result.primaries[0].fee_model is STANDARD_FEE

	def test_no_ohlc(self, resolver):
		class MockHypothesis:
			data_sources = DataSourceConfig(
				primaries=[PrimaryEntry(db="exchange.db", series="SERIES_A")]
			)
		result = resolver.resolve(MockHypothesis())
		assert result.ohlc_config == {}

	def test_with_ohlc(self, resolver):
		class MockHypothesis:
			data_sources = DataSourceConfig(
				primaries=[PrimaryEntry(db="exchange.db", series="SERIES_A")],
				ohlc={"asset_x": ("ohlc.db", "asset_x_ohlc")},
			)
		result = resolver.resolve(MockHypothesis())
		assert result.ohlc_config["asset_x"] == ("data/ohlc.db", "asset_x_ohlc")


class TestResolveMultiPrimary:
	def test_two_primaries(self, resolver):
		class MockHypothesis:
			data_sources = DataSourceConfig(
				primaries=[
					PrimaryEntry(db="exchange.db", series="S1"),
					PrimaryEntry(db="exchange-alt.db", series="S2"),
				],
			)
		result = resolver.resolve(MockHypothesis())
		assert len(result.primaries) == 2
		series_set = {p.series for p in result.primaries}
		assert series_set == {"S1", "S2"}

	def test_fee_models_per_primary(self, resolver):
		class MockHypothesis:
			data_sources = DataSourceConfig(
				primaries=[
					PrimaryEntry(db="exchange.db", series="S1"),
					PrimaryEntry(db="ohlc.db", series="S2"),
				],
			)
		result = resolver.resolve(MockHypothesis())
		fee_by_db = {p.db_path: p.fee_model for p in result.primaries}
		assert fee_by_db["data/exchange.db"] is STANDARD_FEE
		assert fee_by_db["data/ohlc.db"] is ZERO_FEE

class TestFeeOverrides:
	def test_series_override_in_lookup(self, resolver):
		class MockHypothesis:
			data_sources = DataSourceConfig(
				primaries=[PrimaryEntry(db="exchange-fin.db", series="IDX_SP500")],
			)
		result = resolver.resolve(MockHypothesis())
		assert result.primaries[0].fee_model is INDEX_FEE

	def test_no_override_for_regular_series(self, resolver):
		class MockHypothesis:
			data_sources = DataSourceConfig(
				primaries=[PrimaryEntry(db="exchange-fin.db", series="REG_SERIES")],
			)
		result = resolver.resolve(MockHypothesis())
		assert result.primaries[0].fee_model is STANDARD_FEE
