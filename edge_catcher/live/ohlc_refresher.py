"""Standalone ≤90s Coinbase OHLC refresher (Phase 2b). Keeps data/ohlc.db fresh
for the paper-trader engine's OHLCProvider. Synchronous poll loop; sole writer."""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from pathlib import Path

from edge_catcher.adapters.coinbase.adapter import CoinbaseAdapter, _candle_to_row, valid_candle_row
from edge_catcher.storage.db import get_connection, init_ohlc_table

logger = logging.getLogger(__name__)

_CARRY_FORWARD_AFTER_S = 25  # synthesize current-minute bar if forming bar lags past this


@dataclass
class RefreshConfig:
	db_path: str
	products: list[str]
	poll_interval_s: int = 20
	staleness_warn_s: int = 75
	startup_lookback_s: int = 7200
	carry_forward: bool = True

	@classmethod
	def from_yaml(cls, config: dict) -> "RefreshConfig | None":
		block = (config or {}).get("ohlc_refresh") or {}
		if not block.get("enabled"):
			return None
		return cls(
			db_path=block["db_path"],
			products=list(block["products"]),
			poll_interval_s=int(block.get("poll_interval_s", 20)),
			staleness_warn_s=int(block.get("staleness_warn_s", 75)),
			startup_lookback_s=int(block.get("startup_lookback_s", 7200)),
			carry_forward=bool(block.get("carry_forward", True)),
		)


def refresh_once(adapter, conn, now: int, cfg: RefreshConfig) -> int | None:
	"""One poll cycle for one product. Fetch last ~3 min, upsert; carry-forward a
	synthetic current-minute bar if the forming bar is absent past _CARRY_FORWARD_AFTER_S.
	Returns the newest bar timestamp in the DB, or None."""
	candles = adapter.fetch_candles(now - 180, now + 60)
	adapter.upsert_candles([_candle_to_row(c) for c in candles], conn)

	cur_min = (now // 60) * 60
	have_forming = conn.execute(
		f"SELECT 1 FROM {adapter.table_name} WHERE timestamp = ?", (cur_min,)
	).fetchone() is not None

	if cfg.carry_forward and not have_forming and (now - cur_min) >= _CARRY_FORWARD_AFTER_S:
		last = conn.execute(
			f"SELECT close FROM {adapter.table_name} WHERE timestamp < ? "
			"ORDER BY timestamp DESC LIMIT 1", (cur_min,)
		).fetchone()
		if last is not None:
			adapter.upsert_candles([(cur_min, last[0], last[0], last[0], last[0], 0.0)], conn)
			logger.info("carry-forward %s: synth bar @%d close=%.6f", adapter.table_name, cur_min, last[0])

	return conn.execute(f"SELECT MAX(timestamp) FROM {adapter.table_name}").fetchone()[0]


def backfill(adapter, conn, now: int, lookback_s: int) -> int:
	"""On boot, fill any gap (INSERT OR IGNORE, immutable history). Start from the
	latest existing bar, or now-lookback if empty. Returns rows considered."""
	last = conn.execute(f"SELECT MAX(timestamp) FROM {adapter.table_name}").fetchone()[0]
	start = last if last is not None else now - lookback_s
	candles = adapter.fetch_candles(start, now + 60)
	good = [r for r in (_candle_to_row(c) for c in candles) if valid_candle_row(r)]
	if good:
		conn.executemany(
			f"INSERT OR IGNORE INTO {adapter.table_name} (timestamp, open, high, low, close, volume) "
			"VALUES (?,?,?,?,?,?)", good)
		conn.commit()
	return len(good)


def staleness_age(newest_ts: int | None, now: int) -> int | None:
	"""Return seconds since newest_ts, or None if newest_ts is None."""
	return None if newest_ts is None else now - newest_ts


def should_warn_staleness(newest_ts: int | None, now: int, warn_s: int) -> bool:
	"""True if the DB is stale (age > warn_s) or has no data at all."""
	age = staleness_age(newest_ts, now)
	return age is None or age > warn_s


def run_refresher(cfg: RefreshConfig, *, _max_cycles: int | None = None) -> None:
	"""Backfill once per product, then poll forever. _max_cycles is for tests only.
	Idempotent upsert + Restart=always => a hard SIGKILL is safe (no drain)."""
	conn = get_connection(Path(cfg.db_path))  # applies PRAGMA journal_mode=WAL
	adapters = {p: CoinbaseAdapter(p) for p in cfg.products}
	for a in adapters.values():
		init_ohlc_table(conn, a.table_name)
		backfill(a, conn, int(time.time()), cfg.startup_lookback_s)

	cycles = 0
	while _max_cycles is None or cycles < _max_cycles:
		now = int(time.time())
		for a in adapters.values():
			try:
				newest = refresh_once(a, conn, now, cfg)
			except Exception as e:  # fail-safe: log + continue; next poll retries
				logger.warning("refresh_once %s failed: %s", a.table_name, e)
				newest = conn.execute(f"SELECT MAX(timestamp) FROM {a.table_name}").fetchone()[0]
			age = staleness_age(newest, now)
			if should_warn_staleness(newest, now, cfg.staleness_warn_s):
				logger.warning("STALE %s: newest=%s age=%ss (warn>%ss)",
					a.table_name, newest, age, cfg.staleness_warn_s)
			else:
				logger.info("ohlc %s: newest=%s age=%ss", a.table_name, newest, age)
		cycles += 1
		if _max_cycles is None or cycles < _max_cycles:
			time.sleep(cfg.poll_interval_s)
