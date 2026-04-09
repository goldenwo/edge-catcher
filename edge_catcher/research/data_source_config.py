"""Typed data source configuration for hypotheses."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from edge_catcher.research.context_engine import get_series_to_asset


@dataclass
class PrimaryEntry:
	"""A single trade-stream data source."""
	db: str          # relative to data/, e.g. "exchange.db"
	series: str      # series ticker in that DB


@dataclass
class DataSourceConfig:
	"""Everything needed to locate a hypothesis's data."""
	primaries: list[PrimaryEntry]                        # 1+ trade stream sources
	ohlc: dict[str, tuple[str, str]] | None = None       # asset -> (db_file, table) or None


def ohlc_for_series(series: str) -> dict[str, tuple[str, str]] | None:
	"""Look up OHLC config for a series from the series mapping config.

	Returns: {asset: (db_file, table)} or None if no mapping exists.
	Uses get_series_to_asset() which loads from config.local/series_mapping.yaml.
	"""
	for prefix, (asset, db_file, table) in get_series_to_asset().items():
		if series.startswith(prefix):
			if (Path("data") / db_file).exists():
				return {asset: (db_file, table)}
	return None


def make_ds(
	db: str,
	series: str,
	ohlc: dict[str, tuple[str, str]] | None = None,
) -> DataSourceConfig:
	"""Convenience factory for single-primary DataSourceConfig.

	If ohlc is not provided, auto-populates from series_mapping config.
	"""
	if ohlc is None:
		ohlc = ohlc_for_series(series)
	return DataSourceConfig(
		primaries=[PrimaryEntry(db=db, series=series)],
		ohlc=ohlc,
	)
