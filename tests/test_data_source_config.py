"""Tests for DataSourceConfig, PrimaryEntry, make_ds, and ohlc_for_series."""

from unittest.mock import patch
from edge_catcher.research.data_source_config import (
	PrimaryEntry, DataSourceConfig, make_ds, ohlc_for_series,
)


class TestPrimaryEntry:
	def test_basic_construction(self):
		p = PrimaryEntry(db="exchange.db", series="SERIES_A")
		assert p.db == "exchange.db"
		assert p.series == "SERIES_A"

	def test_equality(self):
		a = PrimaryEntry(db="a.db", series="S1")
		b = PrimaryEntry(db="a.db", series="S1")
		assert a == b


class TestDataSourceConfig:
	def test_single_primary_no_ohlc(self):
		cfg = DataSourceConfig(
			primaries=[PrimaryEntry(db="exchange.db", series="SERIES_A")]
		)
		assert len(cfg.primaries) == 1
		assert cfg.ohlc is None

	def test_single_primary_with_ohlc(self):
		cfg = DataSourceConfig(
			primaries=[PrimaryEntry(db="exchange.db", series="SERIES_A")],
			ohlc={"asset_x": ("ohlc.db", "asset_x_ohlc")},
		)
		assert cfg.ohlc["asset_x"] == ("ohlc.db", "asset_x_ohlc")

	def test_multi_primary(self):
		cfg = DataSourceConfig(
			primaries=[
				PrimaryEntry(db="a.db", series="S1"),
				PrimaryEntry(db="b.db", series="S2"),
			],
		)
		assert len(cfg.primaries) == 2
		assert {p.series for p in cfg.primaries} == {"S1", "S2"}

	def test_ohlc_default_none(self):
		cfg = DataSourceConfig(primaries=[PrimaryEntry(db="a.db", series="S1")])
		assert cfg.ohlc is None


class TestMakeDs:
	@patch("edge_catcher.research.data_source_config.ohlc_for_series", return_value=None)
	def test_single_primary_no_ohlc(self, mock_ohlc):
		ds = make_ds(db="exchange.db", series="SERIES_A")
		assert len(ds.primaries) == 1
		assert ds.primaries[0].db == "exchange.db"
		assert ds.primaries[0].series == "SERIES_A"
		assert ds.ohlc is None

	@patch("edge_catcher.research.data_source_config.ohlc_for_series",
		   return_value={"asset_x": ("ohlc.db", "asset_x_ohlc")})
	def test_auto_populates_ohlc(self, mock_ohlc):
		ds = make_ds(db="exchange.db", series="SERIES_A")
		assert ds.ohlc == {"asset_x": ("ohlc.db", "asset_x_ohlc")}
		mock_ohlc.assert_called_once_with("SERIES_A")

	def test_explicit_ohlc_overrides_auto(self):
		ds = make_ds(db="exchange.db", series="SERIES_A",
					 ohlc={"custom": ("custom.db", "custom_ohlc")})
		assert ds.ohlc == {"custom": ("custom.db", "custom_ohlc")}


class TestOhlcForSeries:
	@patch("edge_catcher.research.data_source_config.get_series_to_asset",
		   return_value={"PFX": ("asset_x", "ohlc.db", "asset_x_ohlc")})
	@patch("edge_catcher.research.data_source_config.Path")
	def test_matching_prefix(self, mock_path, mock_mapping):
		mock_path.return_value.exists.return_value = True
		mock_path.__truediv__ = lambda self, other: mock_path.return_value
		result = ohlc_for_series("PFX_SERIES")
		assert result == {"asset_x": ("ohlc.db", "asset_x_ohlc")}

	@patch("edge_catcher.research.data_source_config.get_series_to_asset",
		   return_value={"PFX": ("asset_x", "ohlc.db", "asset_x_ohlc")})
	def test_no_matching_prefix(self, mock_mapping):
		result = ohlc_for_series("OTHER_SERIES")
		assert result is None

	@patch("edge_catcher.research.data_source_config.get_series_to_asset",
		   return_value={})
	def test_empty_mapping(self, mock_mapping):
		result = ohlc_for_series("SERIES_A")
		assert result is None
