"""Download commands — Kalshi / Polymarket market data and Coinbase OHLC."""

import logging
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
	from edge_catcher.adapters.base import AdapterMeta


def _resolve_meta_from_markets_yaml(markets_yaml: str) -> "AdapterMeta":
	"""Look up the AdapterMeta from ADAPTERS by markets_yaml filename.

	Matches on Path.name so config/ and config.local/ variants of the
	same markets file resolve to the same adapter (users put private
	market overrides in config.local/; the registry mapping stays the
	same).

	Raises ValueError if no adapter declares this markets filename.
	"""
	from api.adapter_registry import ADAPTERS

	target_name = Path(markets_yaml).name
	for adapter in ADAPTERS:
		if adapter.markets_yaml is None:
			continue
		if Path(adapter.markets_yaml).name == target_name:
			return adapter
	raise ValueError(
		f"No adapter found for markets_yaml={markets_yaml!r}. "
		f"Declare it in the appropriate edge_catcher/adapters/<exchange>/registry.py"
	)


def _resolve_db_from_markets_yaml(markets_yaml: str) -> str:
	"""Convenience wrapper — returns just the db_file from the matched meta.

	Kept as a thin shim so existing callers and tests don't break after the
	multi-exchange dispatch refactor.
	"""
	return _resolve_meta_from_markets_yaml(markets_yaml).db_file


def _run_download(args) -> None:
	"""Multi-exchange download dispatcher.

	Resolves the AdapterMeta from --markets and routes to the per-exchange
	handler keyed off meta.exchange. Mirrors api/dispatchers.DOWNLOAD_DISPATCHERS
	but in CLI flavor (stdout output, no thread/state object).
	"""
	config_dir = getattr(args, 'config', 'config')
	markets_file = Path(args.markets) if args.markets else Path(config_dir) / "markets-btc.yaml"

	meta = _resolve_meta_from_markets_yaml(str(markets_file))

	# Derive DB path from the resolved adapter when not explicitly provided.
	if args.db_path:
		db_path = Path(args.db_path)
	else:
		db_path = Path(meta.db_file)

	if meta.exchange == "kalshi":
		_run_kalshi_download(args, markets_file, db_path)
	elif meta.exchange == "polymarket":
		_run_polymarket_download(args, markets_file, db_path)
	else:
		raise ValueError(
			f"`download` does not support exchange {meta.exchange!r} "
			f"(adapter={meta.id!r}). Use the adapter-specific subcommand "
			f"(e.g. `download-btc` for Coinbase OHLC)."
		)


def _run_kalshi_download(args, markets_file: Path, db_path: Path) -> None:
	"""Kalshi download flow — markets pages + trades by volume priority."""
	from edge_catcher.adapters.kalshi import KalshiAdapter
	from edge_catcher.storage.db import (
		get_connection,
		init_db,
		upsert_market,
		upsert_trades_batch,
		get_markets_by_series,
	)

	logger = logging.getLogger(__name__)

	init_db(db_path)
	adapter = KalshiAdapter(
		config_path=markets_file,
		dry_run=args.dry_run,
	)

	conn = get_connection(db_path)
	try:
		if not args.skip_market_scan:
			# --- Phase 1: Download markets incrementally, page by page ---
			total_markets = 0
			for series, page_markets in adapter.iter_market_pages():
				for m in page_markets:
					upsert_market(conn, m)
				conn.commit()
				total_markets += len(page_markets)
				print(
					f"  Markets: +{len(page_markets)} for {series} "
					f"(running total: {total_markets})"
				)

			logger.info(f"Market download complete: {total_markets} markets saved")
			print(f"Downloaded {total_markets} markets total.")
		else:
			# Count existing markets for logging
			total_markets = conn.execute("SELECT COUNT(*) FROM markets").fetchone()[0]
			print(f"Skipping market scan — using {total_markets:,} markets already in DB.")

		# --- Phase 2: Download trades for markets with volume > 0 ---
		# Prioritize short-duration contracts first — they are close-to-expiry by nature
		# and most valuable for time-decay analysis. Configure via CLI or config.
		# Override: set --priority-series on the command line.
		PRIORITY_SERIES = getattr(args, "priority_series", None) or []
		priority_markets = []
		other_markets = []
		for series in adapter.get_configured_series():
			series_markets = get_markets_by_series(conn, series)
			for m in series_markets:
				if m.volume is None or (m.volume or 0) > 0:
					if m.series_ticker in PRIORITY_SERIES:
						priority_markets.append(m)
					else:
						other_markets.append(m)

		# Within each group, sort by volume DESC
		priority_markets.sort(key=lambda m: m.volume or 0, reverse=True)
		other_markets.sort(key=lambda m: m.volume or 0, reverse=True)
		all_markets_with_vol = priority_markets + other_markets

		max_trade_markets = getattr(args, "max_trade_markets", None)
		if max_trade_markets:
			all_markets_with_vol = all_markets_with_vol[:max_trade_markets]

		# Skip markets that already have trades in DB (resume support)
		existing_trade_tickers = set()
		for row in conn.execute("SELECT DISTINCT ticker FROM trades"):
			existing_trade_tickers.add(row[0])
		before_skip = len(all_markets_with_vol)
		all_markets_with_vol = [
			m for m in all_markets_with_vol
			if m.ticker not in existing_trade_tickers
		]

		total_tickers = len(all_markets_with_vol)
		total_trades = 0
		skipped = before_skip - total_tickers
		logger.info(
			f"Downloading trades for {total_tickers} markets with volume > 0"
			+ (f" (skipped {skipped} already in DB)" if skipped else "")
			+ (f" (capped at {max_trade_markets})" if max_trade_markets else "")
		)

		for i, market in enumerate(all_markets_with_vol, 1):
			trades = adapter.collect_trades(market.ticker)
			if trades:
				upsert_trades_batch(conn, trades)
				conn.commit()
				total_trades += len(trades)
				print(
					f"  Market {i}/{total_tickers}: {market.ticker} "
					f"(vol={market.volume}) — {len(trades)} trades "
					f"(total: {total_trades})"
				)
			else:
				if i % 50 == 0:
					print(f"  Progress: {i}/{total_tickers} markets processed...")

	finally:
		conn.close()

	print(
		f"\nDownload complete: {total_markets} markets, {total_trades} trades"
	)


def _run_polymarket_download(args, markets_file: Path, db_path: Path) -> None:
	"""Polymarket download flow — Gamma markets + CLOB trades.

	Mirrors api/download_service.run_polymarket_download in shape but writes
	to stdout (no progress-state object). Polymarket has no auth and no
	priority-series concept; markets are collected in one Gamma call, then
	per-market trades fetched from CLOB.
	"""
	from edge_catcher.adapters.polymarket.adapter import PolymarketAdapter
	from edge_catcher.storage.db import (
		get_connection,
		get_markets_by_series,
		init_db,
		upsert_market,
		upsert_trades_batch,
	)

	logger = logging.getLogger(__name__)

	init_db(db_path)
	adapter = PolymarketAdapter(
		config_path=markets_file,
		dry_run=args.dry_run,
	)

	conn = get_connection(db_path)
	try:
		if not args.skip_market_scan:
			markets = adapter.collect_markets()
			for m in markets:
				upsert_market(conn, m)
			conn.commit()
			total_markets = len(markets)
			logger.info(f"Polymarket market download complete: {total_markets} markets saved")
			print(f"Downloaded {total_markets} markets total.")
		else:
			total_markets = conn.execute("SELECT COUNT(*) FROM markets").fetchone()[0]
			print(f"Skipping market scan — using {total_markets:,} markets already in DB.")

		# Collect markets-with-volume across the configured series. Polymarket
		# adapters track `series` as a list of category slugs on the adapter
		# itself; an empty list means "all categories", in which case we walk
		# every series_ticker we just upserted.
		series_list = adapter.series or sorted({
			row[0] for row in conn.execute("SELECT DISTINCT series_ticker FROM markets")
		})

		existing_trade_tickers = {
			row[0] for row in conn.execute("SELECT DISTINCT ticker FROM trades")
		}
		all_markets_with_vol = []
		for series in series_list:
			for m in get_markets_by_series(conn, series):
				if (m.volume is None or (m.volume or 0) > 0) and m.ticker not in existing_trade_tickers:
					all_markets_with_vol.append(m)
		all_markets_with_vol.sort(key=lambda m: m.volume or 0, reverse=True)

		max_trade_markets = getattr(args, "max_trade_markets", None)
		if max_trade_markets:
			all_markets_with_vol = all_markets_with_vol[:max_trade_markets]

		total_tickers = len(all_markets_with_vol)
		total_trades = 0
		logger.info(f"Downloading trades for {total_tickers} polymarket markets")

		for i, market in enumerate(all_markets_with_vol, 1):
			trades = adapter.collect_trades(market.ticker)
			if trades:
				upsert_trades_batch(conn, trades)
				conn.commit()
				total_trades += len(trades)
				print(
					f"  Market {i}/{total_tickers}: {market.ticker} "
					f"(vol={market.volume}) — {len(trades)} trades "
					f"(total: {total_trades})"
				)
			else:
				if i % 50 == 0:
					print(f"  Progress: {i}/{total_tickers} markets processed...")

	finally:
		conn.close()

	print(
		f"\nDownload complete: {total_markets} markets, {total_trades} trades"
	)


def _run_download_btc(args) -> None:
	from api.adapter_registry import get_adapter
	from edge_catcher.adapters.coinbase import CoinbaseAdapter
	from edge_catcher.storage.db import get_connection, init_btc_ohlc_table
	from datetime import datetime, timezone

	db_path = Path(args.db)
	db_path.parent.mkdir(parents=True, exist_ok=True)
	conn = get_connection(db_path)
	init_btc_ohlc_table(conn)

	# Start date from the coinbase_btc registry entry (keeps CLI + registry in sync).
	meta = get_adapter("coinbase_btc")
	assert meta is not None and meta.default_start_date is not None, (
		"coinbase_btc must be registered with a default_start_date"
	)
	start_ts = int(datetime.fromisoformat(meta.default_start_date).replace(tzinfo=timezone.utc).timestamp())
	end_ts = int(datetime.now(timezone.utc).timestamp())

	adapter = CoinbaseAdapter()
	n = adapter.download_range(start_ts, end_ts, conn)
	conn.close()
	print(f"Downloaded {n:,} new BTC-USD candles.")


def _run_download_altcoin_ohlc(args) -> None:
	from edge_catcher.adapters.coinbase import CoinbaseAdapter
	from edge_catcher.storage.db import get_connection, init_ohlc_table
	from datetime import datetime, timezone, date

	db_path = Path(args.db)
	db_path.parent.mkdir(parents=True, exist_ok=True)
	conn = get_connection(db_path)

	start_ts = int(datetime.fromisoformat(args.start_date).replace(tzinfo=timezone.utc).timestamp())
	end_date = args.end_date or date.today().isoformat()
	end_ts = int(datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc).timestamp())

	coins = [c.strip().upper() for c in args.coins.split(",") if c.strip()]
	for coin in coins:
		product_id = f"{coin}-USD"
		adapter = CoinbaseAdapter(product_id=product_id)
		init_ohlc_table(conn, adapter.table_name)
		n = adapter.download_range(start_ts, end_ts, conn)
		print(f"Downloading {product_id}: {n:,} candles")

	conn.close()


def register(subparsers) -> None:
	dl = subparsers.add_parser("download", help="Download market data from Kalshi")
	dl.add_argument("--db-path", default=None,
	                help="DB path (default: derived from --markets file, e.g. data/kalshi-crypto.db)")
	dl.add_argument("--dry-run", action="store_true", help="Fetch one page only")
	dl.add_argument(
		"--skip-market-scan",
		action="store_true",
		help="Skip Phase 1 market page scan and go directly to trade downloads. "
		     "Use on restart when markets are already in DB.",
	)
	dl.add_argument(
		"--markets",
		default=None,
		metavar="FILE",
		help="Path to markets YAML file (default: {config}/markets-btc.yaml). "
		     "Example: --markets config/markets-altcrypto.yaml",
	)
	dl.add_argument(
		"--max-trade-markets",
		type=int,
		default=None,
		metavar="N",
		help="Cap trades download to top N markets by volume (default: all volume>0 markets)",
	)
	dl.set_defaults(func=_run_download)

	btc = subparsers.add_parser("download-btc", help="Download BTC-USD 1-minute OHLC from Coinbase")
	btc.add_argument("--db", default="data/btc.db", help="Path to SQLite DB for BTC OHLC data")
	btc.set_defaults(func=_run_download_btc)

	altcoin = subparsers.add_parser("download-altcoin-ohlc", help="Download 1-minute OHLC for altcoins from Coinbase")
	altcoin.add_argument("--db", default="data/ohlc.db", help="Path to SQLite DB for altcoin OHLC data")
	altcoin.add_argument("--coins", default="SOL,ETH,XRP,DOGE,BNB",
	                     help="Comma-separated coin symbols (default: SOL,ETH,XRP,DOGE,BNB)")
	altcoin.add_argument("--start-date", default="2025-01-01", dest="start_date",
	                     help="Start date ISO format (default: 2025-01-01)")
	altcoin.add_argument("--end-date", default=None, dest="end_date",
	                     help="End date ISO format (default: today)")
	altcoin.set_defaults(func=_run_download_altcoin_ohlc)
