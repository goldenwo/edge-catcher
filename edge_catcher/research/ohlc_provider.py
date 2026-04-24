"""OHLCProvider — read-only external price data access for strategies."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime


@dataclass
class OHLC:
	"""Single OHLC candle."""
	timestamp: int
	open: float
	high: float
	low: float
	close: float
	volume: float


class OHLCProvider:
	"""Lightweight read-only wrapper for OHLC data in SQLite.

	Connections are opened lazily on first query and closed via close().
	"""

	def __init__(self, db_paths: dict[str, tuple[str, str]]) -> None:
		"""Initialize with asset → (db_file_path, table_name) mapping."""
		self._config = db_paths
		self._connections: dict[str, sqlite3.Connection] = {}

	def _get_conn(self, asset: str) -> sqlite3.Connection | None:
		if asset not in self._config:
			return None
		if asset not in self._connections:
			db_path, _ = self._config[asset]
			self._connections[asset] = sqlite3.connect(db_path)
		return self._connections[asset]

	def _get_table(self, asset: str) -> str | None:
		if asset not in self._config:
			return None
		return self._config[asset][1]

	def get_price(self, asset: str, timestamp: datetime) -> float | None:
		"""Latest close price at or before timestamp."""
		conn = self._get_conn(asset)
		if conn is None:
			return None
		table = self._get_table(asset)
		ts = int(timestamp.timestamp())
		row = conn.execute(
			f"SELECT close FROM {table} WHERE timestamp <= ? ORDER BY timestamp DESC LIMIT 1",
			(ts,),
		).fetchone()
		return row[0] if row else None

	def get_candle(self, asset: str, timestamp: datetime) -> OHLC | None:
		"""Nearest candle at or before timestamp."""
		conn = self._get_conn(asset)
		if conn is None:
			return None
		table = self._get_table(asset)
		ts = int(timestamp.timestamp())
		row = conn.execute(
			f"SELECT timestamp, open, high, low, close, volume FROM {table} "
			f"WHERE timestamp <= ? ORDER BY timestamp DESC LIMIT 1",
			(ts,),
		).fetchone()
		if row is None:
			return None
		return OHLC(*row)

	def get_recent(self, asset: str, timestamp: datetime, n_candles: int = 10) -> list[OHLC]:
		"""Last N candles ending at or before timestamp, in chronological order."""
		conn = self._get_conn(asset)
		if conn is None:
			return []
		table = self._get_table(asset)
		ts = int(timestamp.timestamp())
		rows = conn.execute(
			f"SELECT timestamp, open, high, low, close, volume FROM {table} "
			f"WHERE timestamp <= ? ORDER BY timestamp DESC LIMIT ?",
			(ts, n_candles),
		).fetchall()
		return [OHLC(*r) for r in reversed(rows)]

	def close(self) -> None:
		"""Close all open connections."""
		for conn in self._connections.values():
			try:
				conn.close()
			except Exception:
				pass
		self._connections.clear()
