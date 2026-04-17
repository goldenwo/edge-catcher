"""Download service functions for Edge Catcher API.

Contains Kalshi and Coinbase download logic, adapter data checks,
and API key management helpers. Extracted from api/main.py.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

from api.config_helpers import validate_db, markets_yaml
from api.tasks import save_adapter_history

logger = logging.getLogger(__name__)


def run_kalshi_download(
	adapter_id: str,
	state,
	start_date: str | None = None,
	markets_yaml_path: str | None = None,
	db_file: str | None = None,
) -> None:
	"""Run Kalshi download, updating the provided state object."""
	from datetime import datetime, timezone

	from edge_catcher.adapters.kalshi import KalshiAdapter
	from edge_catcher.storage.db import (
		get_connection,
		get_markets_by_series,
		init_db,
		upsert_market,
		upsert_trades_batch,
	)

	db = Path(db_file) if db_file else validate_db("kalshi-btc.db")

	state.running = True
	state.progress = "Initializing..."
	state.rows_fetched = 0
	state.error = None

	try:
		init_db(db)
		config_file = Path(markets_yaml_path) if markets_yaml_path else markets_yaml()
		adapter = KalshiAdapter(config_path=config_file)
		conn = get_connection(db)
		try:
			markets_count = 0
			for _series, page_markets in adapter.iter_market_pages():
				for m in page_markets:
					upsert_market(conn, m)
				conn.commit()
				markets_count += len(page_markets)
				state.progress = f"Markets: {markets_count} fetched"
				if hasattr(state, 'markets_fetched'):
					state.markets_fetched = markets_count

			existing_tickers = {
				r[0] for r in conn.execute("SELECT DISTINCT ticker FROM trades")
			}
			markets_with_vol: list = []
			for series in adapter.get_configured_series():
				for m in get_markets_by_series(conn, series):
					if (m.volume is None or (m.volume or 0) > 0) and m.ticker not in existing_tickers:
						markets_with_vol.append(m)
			markets_with_vol.sort(key=lambda m: m.volume or 0, reverse=True)

			total = len(markets_with_vol)
			trades_count = 0
			for i, market in enumerate(markets_with_vol, 1):
				state.progress = f"Trades: {i}/{total} markets"
				trades = adapter.collect_trades(market.ticker, since=start_date)
				if trades:
					upsert_trades_batch(conn, trades)
					conn.commit()
					trades_count += len(trades)
					if hasattr(state, 'trades_fetched'):
						state.trades_fetched = trades_count
			state.rows_fetched = trades_count
		finally:
			conn.close()

		state.last_run = datetime.now(timezone.utc).isoformat()
		state.progress = "Complete"
		save_adapter_history(adapter_id, state.last_run)
	except Exception as exc:
		logger.error("Kalshi adapter download failed: %s", exc)
		state.error = str(exc)
		state.progress = f"Error: {exc}"
	finally:
		state.running = False


def run_coinbase_download(
	adapter_id: str,
	state,
	start_date: str | None = None,
	product_id: str = "BTC-USD",
	db_file: str = "data/btc.db",
) -> None:
	"""Run Coinbase OHLC download, updating the provided state object."""
	from datetime import datetime, timezone

	from edge_catcher.adapters.coinbase import CoinbaseAdapter
	from edge_catcher.storage.db import get_connection, init_ohlc_table

	state.running = True
	state.progress = "Initializing..."
	state.error = None
	state.rows_fetched = 0
	try:
		db_path = Path(db_file)
		db_path.parent.mkdir(parents=True, exist_ok=True)
		conn = get_connection(db_path)
		adapter = CoinbaseAdapter(product_id=product_id)
		init_ohlc_table(conn, adapter.table_name)
		if start_date:
			start_dt = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)
		else:
			start_dt = datetime(2025, 3, 21, tzinfo=timezone.utc)
		start_ts = int(start_dt.timestamp())
		end_ts = int(datetime.now(timezone.utc).timestamp())

		def _on_progress(pct, page, total_pages, rows):
			state.progress = f"{pct}% — {rows:,} candles ({page}/{total_pages} pages)"

		n = adapter.download_range(start_ts, end_ts, conn, progress_callback=_on_progress)
		conn.close()
		state.rows_fetched = n
		state.progress = f"Complete — {n:,} new candles"
		state.last_run = datetime.now(timezone.utc).isoformat()
		save_adapter_history(adapter_id, state.last_run)
	except Exception as e:
		state.error = str(e)
		state.progress = "Error"
	finally:
		state.running = False


def run_legacy_download(download_state) -> None:
	"""Wrapper for the legacy /api/download endpoint.

	Passes download_state directly to run_kalshi_download so progress updates
	are reflected in real-time. The legacy state gains rows_fetched via
	duck-typing; trades_fetched is mirrored from it after completion.
	"""
	download_state.running = True
	download_state.progress = "Initializing..."
	download_state.markets_fetched = 0
	download_state.trades_fetched = 0
	download_state.error = None
	download_state.rows_fetched = 0  # duck-type compatibility with AdapterDownloadState

	run_kalshi_download("kalshi_btc", download_state)

	# Mirror adapter field name to legacy field name
	download_state.trades_fetched = getattr(download_state, 'rows_fetched', 0)


def adapter_has_data(meta) -> bool:
	"""Check whether an adapter's DB actually contains data."""
	import sqlite3
	import yaml

	db_file = Path(meta.db_file)
	if not db_file.exists():
		return False
	try:
		conn = sqlite3.connect(str(db_file), timeout=5)
		if meta.markets_yaml:
			cfg = yaml.safe_load(Path(meta.markets_yaml).read_text())
			series = cfg.get("adapters", {}).get("kalshi", {}).get("series", [])
			if not series:
				conn.close()
				return False
			placeholders = ",".join("?" for _ in series)
			count = conn.execute(
				f"SELECT COUNT(*) FROM markets WHERE series_ticker IN ({placeholders})", series
			).fetchone()[0]
			conn.close()
			return count > 0
		elif meta.exchange == "coinbase":
			product_id = meta.extra["product_id"]
			table = product_id.split("-")[0].lower() + "_ohlc"
			count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
			conn.close()
			return count > 0
		else:
			conn.close()
			return False
	except Exception:
		return False


def save_api_key(env_var: str, value: str) -> None:
	"""Append or update KEY=value in .env file."""
	env_path = Path(".env")
	lines = env_path.read_text().splitlines() if env_path.exists() else []
	updated = False
	for i, line in enumerate(lines):
		if line.startswith(f"{env_var}="):
			lines[i] = f"{env_var}={value}"
			updated = True
			break
	if not updated:
		lines.append(f"{env_var}={value}")
	env_path.write_text("\n".join(lines) + "\n")
	os.environ[env_var] = value  # also update current process


def clear_api_key(env_var: str) -> None:
	"""Remove KEY=value from .env file and unset from current process."""
	env_path = Path(".env")
	if env_path.exists():
		lines = [l for l in env_path.read_text().splitlines() if not l.startswith(f"{env_var}=")]
		env_path.write_text("\n".join(lines) + "\n" if lines else "")
	os.environ.pop(env_var, None)
