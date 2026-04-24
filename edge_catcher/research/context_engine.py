"""Context Engine — auto-profiles market series from SQLite databases."""

from __future__ import annotations

import logging
import math
import sqlite3
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

_CONFIG_PATH = Path("config.local/series_mapping.yaml")

# Lazy-loaded caches (populated on first access via helpers below)
_series_to_asset_cache: dict[str, tuple[str, str, str]] | None = None
_db_to_asset_class_cache: dict[str, str] | None = None


def _load_series_mapping() -> tuple[dict[str, tuple[str, str, str]], dict[str, str]]:
	"""Load series mapping from config.local/series_mapping.yaml.

	Returns (series_to_asset, db_to_asset_class).  Falls back to empty dicts
	if the config file is missing (no hardcoded secrets in source).
	"""
	try:
		import yaml
		with open(_CONFIG_PATH) as f:
			data = yaml.safe_load(f) or {}
	except (FileNotFoundError, ImportError):
		logger.warning("Series mapping not found at %s — OHLC matching disabled", _CONFIG_PATH)
		return {}, {}

	s2a: dict[str, tuple[str, str, str]] = {}
	for prefix, vals in (data.get("series_to_asset") or {}).items():
		if isinstance(vals, list) and len(vals) == 3:
			s2a[prefix] = (vals[0], vals[1], vals[2])

	d2a: dict[str, str] = data.get("db_to_asset_class") or {}
	return s2a, d2a


def get_series_to_asset() -> dict[str, tuple[str, str, str]]:
	"""Return series prefix → (asset, db_file, table) mapping (cached)."""
	global _series_to_asset_cache
	if _series_to_asset_cache is None:
		_series_to_asset_cache, _ = _load_series_mapping()
	return _series_to_asset_cache


def get_db_to_asset_class() -> dict[str, str]:
	"""Return db filename → asset class mapping (cached)."""
	global _db_to_asset_class_cache
	if _db_to_asset_class_cache is None:
		_, _db_to_asset_class_cache = _load_series_mapping()
	return _db_to_asset_class_cache


@dataclass
class SeriesProfile:
	"""Auto-derived profile of a market series."""
	series_ticker: str
	db_path: str
	description: str
	settlement_frequency: str  # "15-minute", "hourly", "daily", "weekly", "unknown"
	market_count: int
	date_range: tuple[str | None, str | None]
	volume_stats: dict  # {"median": float, "mean": float, "p90": float}
	price_distribution: dict  # {"extreme": float, "mid": float, "moderate": float}
	result_distribution: dict  # {"yes": float, "no": float}
	strike_info: dict = field(default_factory=dict)
	asset_class: str = "Unknown"
	# OHLC fields (None if no matching external data)
	external_asset: str | None = None
	ohlc_db_path: str | None = None
	ohlc_table: str | None = None
	volatility_stats: dict | None = None
	price_level: float | None = None
	correlation_note: str | None = None


class ContextEngine:
	"""Profiles market series from SQLite databases and OHLC data."""

	def __init__(self, data_dir: str = "data") -> None:
		self.data_dir = Path(data_dir)

	def profile_all(self, db_paths: list[str]) -> list[SeriesProfile]:
		"""Scan all DBs and return a SeriesProfile per series.

		Skips DBs that are missing the markets table or have errors.
		"""
		profiles: list[SeriesProfile] = []
		for db_path in db_paths:
			try:
				profiles.extend(self._profile_db(db_path))
			except Exception as exc:
				logger.warning("Skipping %s: %s", db_path, exc)
		return profiles

	def _profile_db(self, db_path: str) -> list[SeriesProfile]:
		"""Profile all series in a single database."""
		with sqlite3.connect(db_path) as conn:
			conn.row_factory = sqlite3.Row
			# Check markets table exists
			tables = [
				r[0] for r in
				conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
			]
			if "markets" not in tables:
				raise ValueError(f"no markets table in {db_path}")

			series_rows = conn.execute(
				"SELECT DISTINCT series_ticker FROM markets WHERE series_ticker IS NOT NULL"
			).fetchall()

			db_name = Path(db_path).name
			asset_class = get_db_to_asset_class().get(db_name, "Unknown")

			profiles = []
			for row in series_rows:
				ticker = row[0]
				try:
					p = self._profile_series(conn, ticker, db_path, asset_class)
					profiles.append(p)
				except Exception as exc:
					logger.warning("Skipping series %s in %s: %s", ticker, db_path, exc)
			return profiles

	def _profile_series(
		self, conn: sqlite3.Connection, ticker: str, db_path: str, asset_class: str,
	) -> SeriesProfile:
		"""Build a SeriesProfile for one series."""
		# Basic counts
		row = conn.execute(
			"SELECT COUNT(*), MIN(open_time), MAX(close_time) FROM markets "
			"WHERE series_ticker = ?", (ticker,),
		).fetchone()
		market_count = row[0]
		date_start = row[1][:10] if row[1] else None
		date_end = row[2][:10] if row[2] else None

		# Description from titles
		title_row = conn.execute(
			"SELECT title FROM markets WHERE series_ticker = ? AND title IS NOT NULL LIMIT 1",
			(ticker,),
		).fetchone()
		description = title_row[0] if title_row else ticker

		# Settlement frequency from open_time/close_time deltas
		settlement_frequency = self._detect_frequency(conn, ticker)

		# Volume stats
		volumes = [
			r[0] for r in conn.execute(
				"SELECT volume FROM markets WHERE series_ticker = ? AND volume IS NOT NULL",
				(ticker,),
			).fetchall()
		]
		volume_stats = self._compute_volume_stats(volumes)

		# Price distribution
		prices = [
			r[0] for r in conn.execute(
				"SELECT last_price FROM markets WHERE series_ticker = ? AND last_price IS NOT NULL",
				(ticker,),
			).fetchall()
		]
		price_distribution = self._compute_price_distribution(prices)

		# Result distribution
		result_counts = conn.execute(
			"SELECT result, COUNT(*) FROM markets WHERE series_ticker = ? "
			"AND result IS NOT NULL GROUP BY result",
			(ticker,),
		).fetchall()
		total_settled = sum(c for _, c in result_counts)
		result_distribution = {
			r: round(c / total_settled, 3) if total_settled > 0 else 0.0
			for r, c in result_counts
		}

		# Strike info
		strike_row = conn.execute(
			"SELECT AVG(cap_strike - floor_strike) FROM markets "
			"WHERE series_ticker = ? AND floor_strike IS NOT NULL AND cap_strike IS NOT NULL",
			(ticker,),
		).fetchone()
		strike_info = {}
		if strike_row and strike_row[0] is not None:
			strike_info["typical_width"] = round(strike_row[0], 2)

		# OHLC matching
		external_asset = None
		ohlc_db_path = None
		ohlc_table = None
		volatility_stats = None
		price_level = None
		correlation_note = None

		for prefix, (asset, ohlc_db, table) in get_series_to_asset().items():
			if ticker.startswith(prefix):
				external_asset = asset
				ohlc_db_full = str(self.data_dir / ohlc_db)
				if Path(ohlc_db_full).exists():
					ohlc_db_path = ohlc_db_full
					ohlc_table = table
					volatility_stats, price_level = self._query_ohlc(
						ohlc_db_full, table, settlement_frequency,
					)
					correlation_note = (
						f"Settles based on {asset.upper()}/USD price "
						f"at {settlement_frequency} intervals"
					)
				break

		return SeriesProfile(
			series_ticker=ticker,
			db_path=db_path,
			description=description,
			settlement_frequency=settlement_frequency,
			market_count=market_count,
			date_range=(date_start, date_end),
			volume_stats=volume_stats,
			price_distribution=price_distribution,
			result_distribution=result_distribution,
			strike_info=strike_info,
			asset_class=asset_class,
			external_asset=external_asset,
			ohlc_db_path=ohlc_db_path,
			ohlc_table=ohlc_table,
			volatility_stats=volatility_stats,
			price_level=price_level,
			correlation_note=correlation_note,
		)

	@staticmethod
	def _detect_frequency(conn: sqlite3.Connection, ticker: str) -> str:
		"""Infer settlement frequency from open_time/close_time deltas."""
		rows = conn.execute(
			"SELECT open_time, close_time FROM markets "
			"WHERE series_ticker = ? AND open_time IS NOT NULL AND close_time IS NOT NULL "
			"LIMIT 50",
			(ticker,),
		).fetchall()
		if not rows:
			return "unknown"

		deltas: list[float] = []
		for open_t, close_t in rows:
			try:
				o = datetime.fromisoformat(open_t)
				c = datetime.fromisoformat(close_t)
				delta_hours = (c - o).total_seconds() / 3600
				if delta_hours > 0:
					deltas.append(delta_hours)
			except (ValueError, TypeError):
				continue

		if not deltas:
			return "unknown"

		median_hours = statistics.median(deltas)
		if median_hours <= 0.5:
			return "15-minute"
		elif median_hours <= 2:
			return "hourly"
		elif median_hours <= 36:
			return "daily"
		elif median_hours <= 192:
			return "weekly"
		else:
			return "monthly"

	@staticmethod
	def _compute_volume_stats(volumes: list[int]) -> dict:
		if not volumes:
			return {"median": 0, "mean": 0, "p90": 0}
		sorted_v = sorted(volumes)
		p90_idx = int(len(sorted_v) * 0.9)
		return {
			"median": round(statistics.median(sorted_v), 1),
			"mean": round(statistics.mean(sorted_v), 1),
			"p90": sorted_v[min(p90_idx, len(sorted_v) - 1)],
		}

	@staticmethod
	def _compute_price_distribution(prices: list[int]) -> dict:
		if not prices:
			return {"extreme": 0, "moderate": 0, "mid": 0}
		extreme = sum(1 for p in prices if p < 10 or p > 90)
		mid = sum(1 for p in prices if 40 <= p <= 60)
		moderate = len(prices) - extreme - mid
		total = len(prices)
		return {
			"extreme": round(extreme / total, 3),
			"moderate": round(moderate / total, 3),
			"mid": round(mid / total, 3),
		}

	def _query_ohlc(
		self, db_path: str, table: str, frequency: str,
	) -> tuple[dict | None, float | None]:
		"""Query OHLC table for volatility stats and current price."""
		try:
			with sqlite3.connect(db_path) as conn:
				# Latest price
				row = conn.execute(
					f"SELECT close FROM {table} ORDER BY timestamp DESC LIMIT 1"
				).fetchone()
				price_level = row[0] if row else None

				# Compute hourly returns for volatility
				rows = conn.execute(
					f"SELECT close FROM {table} ORDER BY timestamp DESC LIMIT 1500"
				).fetchall()
				if len(rows) < 100:
					return None, price_level

				closes = [r[0] for r in reversed(rows)]
				# Minute-level returns → hourly vol
				returns_60 = [
					(closes[i] - closes[i - 60]) / closes[i - 60]
					for i in range(60, len(closes))
					if closes[i - 60] != 0
				]
				hourly_vol = statistics.stdev(returns_60) if len(returns_60) >= 2 else 0.0

				# Daily vol (approximate: hourly × sqrt(24))
				daily_vol = hourly_vol * math.sqrt(24)

				return {
					"hourly_vol": round(hourly_vol, 5),
					"daily_vol": round(daily_vol, 4),
				}, price_level
		except Exception as exc:
			logger.warning("OHLC query failed for %s.%s: %s", db_path, table, exc)
			return None, None

	def build_context_block(self, profiles: list[SeriesProfile]) -> str:
		"""Format profiles into structured text for the LLM prompt."""
		if not profiles:
			return ""

		# Group by (asset_class, settlement_frequency)
		groups: dict[str, list[SeriesProfile]] = {}
		for p in profiles:
			key = f"{p.asset_class} — {p.settlement_frequency.title()} Settlement"
			groups.setdefault(key, []).append(p)

		parts: list[str] = ["## Market Profiles\n"]
		for group_name in sorted(groups):
			parts.append(f"### {group_name}")
			for p in sorted(groups[group_name], key=lambda x: x.series_ticker):
				db_name = Path(p.db_path).name
				parts.append(
					f"{p.series_ticker} ({db_name}): {p.description}"
				)
				parts.append(
					f"  {p.market_count:,} markets | "
					f"{p.date_range[0] or '?'} to {p.date_range[1] or '?'}"
				)
				parts.append(
					f"  Volume: median {p.volume_stats['median']}, "
					f"mean {p.volume_stats['mean']}, p90 {p.volume_stats['p90']}"
				)
				pct = p.price_distribution
				parts.append(
					f"  Price distribution: {pct.get('extreme', 0):.0%} at extremes "
					f"(<10¢ or >90¢), {pct.get('mid', 0):.0%} mid-range"
				)
				rd = p.result_distribution
				if rd:
					parts.append(
						f"  Result skew: " +
						", ".join(f"{k} {v:.0%}" for k, v in rd.items())
					)
				if p.strike_info:
					parts.append(
						f"  Strike width: {p.strike_info.get('typical_width', '?')}"
					)
				if p.external_asset:
					ext_parts = [f"  External: {p.external_asset.upper()}/USD"]
					if p.price_level is not None:
						ext_parts.append(f"current ~${p.price_level:,.2f}")
					if p.volatility_stats:
						ext_parts.append(
							f"hourly vol {p.volatility_stats['hourly_vol']:.2%}"
						)
					parts.append(" | ".join(ext_parts))
					if p.correlation_note:
						parts.append(f"  {p.correlation_note}")
				# Warnings
				if p.volume_stats["median"] < 5:
					parts.append(
						"  ⚠ Low volume — high fee impact at this frequency"
					)
				parts.append("")
		return "\n".join(parts)

	def find_related_series(
		self,
		series_ticker: str,
		profiles: list[SeriesProfile],
		same_asset_class: bool = True,
		same_settlement_freq: bool = False,
	) -> list[tuple[str, str]]:
		"""Find structurally related series for expansion.

		Returns (series_ticker, db_path) pairs ordered by similarity.
		"""
		source = None
		for p in profiles:
			if p.series_ticker == series_ticker:
				source = p
				break
		if source is None:
			return []

		# Score each other series by similarity
		candidates: list[tuple[float, str, str]] = []
		for p in profiles:
			if p.series_ticker == series_ticker:
				continue
			score = 0.0
			# Same asset (e.g. SERIES_H and SERIES_D) — highest similarity
			if p.external_asset and p.external_asset == source.external_asset:
				score += 3.0
			# Same asset class
			if same_asset_class and p.asset_class != source.asset_class:
				continue
			if p.asset_class == source.asset_class:
				score += 1.0
			# Same settlement frequency
			if p.settlement_frequency == source.settlement_frequency:
				score += 1.0
			elif same_settlement_freq:
				continue

			candidates.append((score, p.series_ticker, p.db_path))

		# Sort by score descending, then alphabetically
		candidates.sort(key=lambda x: (-x[0], x[1]))
		return [(ticker, db) for _, ticker, db in candidates]
