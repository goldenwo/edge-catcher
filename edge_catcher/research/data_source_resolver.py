"""DataSourceResolver — maps hypothesis data_sources to resolved paths and fee models."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from edge_catcher.fees import FeeModel, ZERO_FEE


@dataclass
class PrimarySource:
	"""Resolved primary — has absolute db_path and fee model attached."""
	db_path: str
	series: str
	fee_model: FeeModel


@dataclass
class ResolvedSource:
	"""Everything the backtester needs to run."""
	primaries: list[PrimarySource]
	ohlc_config: dict[str, tuple[str, str]]          # asset -> (resolved db_path, table)


FeeModelLookup = Callable[[str, str | None], FeeModel]


class DataSourceResolver:
	"""Resolves a Hypothesis's DataSourceConfig into concrete paths and fee models."""

	def __init__(
		self,
		fee_model_lookup: FeeModelLookup | None = None,
		data_dir: str = "data",
	) -> None:
		self._fee_lookup = fee_model_lookup or (lambda _db, _s: ZERO_FEE)
		self._data_dir = data_dir

	@classmethod
	def from_environment(cls) -> DataSourceResolver:
		"""Build a resolver from the live adapter registry."""
		from api.adapter_registry import get_fee_model_for_db
		return cls(fee_model_lookup=get_fee_model_for_db)

	def resolve(self, hypothesis) -> ResolvedSource:
		"""Resolve a Hypothesis to concrete data sources."""
		ds = hypothesis.data_sources
		primaries: list[PrimarySource] = []

		for entry in ds.primaries:
			db_path = (Path(self._data_dir) / entry.db).as_posix()
			fee_model = self._fee_lookup(db_path, entry.series)
			primaries.append(PrimarySource(db_path, entry.series, fee_model))

		ohlc_config: dict[str, tuple[str, str]] = {}
		if ds.ohlc:
			for asset, (db_file, table) in ds.ohlc.items():
				ohlc_config[asset] = ((Path(self._data_dir) / db_file).as_posix(), table)

		return ResolvedSource(primaries, ohlc_config)
