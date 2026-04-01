"""CLI entry point: python -m edge_catcher [download|analyze|archive|formalize|interpret]"""

import logging
import sys
from pathlib import Path


def _try_load_dotenv() -> None:
    """Load .env file if python-dotenv is installed (silently skip otherwise)."""
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass


def _setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )


def _cmd_download(args) -> None:
    from edge_catcher.adapters.kalshi import KalshiAdapter
    from edge_catcher.storage.db import (
        get_connection,
        init_db,
        upsert_market,
        upsert_trades_batch,
        get_markets_by_series,
    )

    logger = logging.getLogger(__name__)

    db_path = Path(args.db_path)
    init_db(db_path)

    adapter = KalshiAdapter(
        config_path=Path(args.config) / "markets.yaml",
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


def _cmd_download_btc(args) -> None:
    from edge_catcher.adapters.coinbase import CoinbaseAdapter
    from edge_catcher.storage.db import get_connection, init_db
    from datetime import datetime, timezone
    import time

    db_path = Path(args.db)
    init_db(db_path)
    conn = get_connection(db_path)

    # Default start: 2025-03-21T00:00:00 UTC (earliest Kalshi market open)
    start_ts = int(datetime(2025, 3, 21, tzinfo=timezone.utc).timestamp())
    end_ts = int(datetime.now(timezone.utc).timestamp())

    adapter = CoinbaseAdapter()
    n = adapter.download_range(start_ts, end_ts, conn)
    conn.close()
    print(f"Downloaded {n:,} new BTC-USD candles.")


def _cmd_analyze(args) -> None:
    from edge_catcher.runner.backtest import run_backtest
    from edge_catcher.reports.formatter import format_json_file

    run_backtest(
        hypothesis_id=getattr(args, "hypothesis", None),
        db_path=Path(args.db_path),
        config_path=Path(args.config),
        output_path=Path(args.output),
    )
    print(format_json_file(args.output))
    print(f"\nFull JSON saved to {args.output}")


def _cmd_formalize(args) -> None:
    _try_load_dotenv()
    from edge_catcher.ai.client import LLMClient, LLMError
    from edge_catcher.ai.formalizer import formalize

    client = LLMClient(provider=args.provider, model=args.model)
    try:
        result = formalize(args.description, client)
    except LLMError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)

    if result.get("error"):
        sys.exit(1)
    print(result["message"])


def _cmd_interpret(args) -> None:
    _try_load_dotenv()
    from edge_catcher.ai.client import LLMClient, LLMError
    from edge_catcher.ai.interpreter import interpret

    client = LLMClient(provider=args.provider, model=args.model)
    try:
        summary = interpret(args.report, client)
    except LLMError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
    except FileNotFoundError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
    print(summary)


def _cmd_paper_trade(args) -> None:
    from dotenv import load_dotenv
    load_dotenv()
    from edge_catcher.monitors.paper_trader import run_paper_trader
    import asyncio
    asyncio.run(run_paper_trader(
        db_path=Path(args.db),
        min_price=args.min_price,
        max_price=args.max_price,
        enable_strategy_b=args.enable_strategy_b,
    ))


def _cmd_paper_trade_15m(args) -> None:
    from dotenv import load_dotenv
    load_dotenv()
    from edge_catcher.monitors.paper_trader_15m import run_paper_trader_15m
    import asyncio
    asyncio.run(run_paper_trader_15m(
        db_path=Path(args.db),
        threshold_high=args.threshold_high,
        threshold_low=args.threshold_low,
    ))


def _cmd_backtest(args) -> None:
    import json
    from datetime import date
    from edge_catcher.runner.event_backtest import EventBacktester
    from edge_catcher.runner.strategies import ExampleStrategy
    try:
        from edge_catcher.runner.strategies_local import (
            BuyYesInRange, BuyNoOnDrop, BuyNoInRange, ActiveExitStub,
            FadeFirstTrade, ThresholdFade, REDACTED,
            REDACTED, REDACTED,
            REDACTED, REDACTED,
            REDACTED,
        )
        _has_local = True
    except ImportError:
        _has_local = False

    strategy_map: dict = {
        'example': ExampleStrategy,
    }
    if _has_local:
        strategy_map.update({
            'REDACTED': BuyYesInRange,
            'REDACTED': REDACTED,
            'REDACTED': BuyNoOnDrop,
            'REDACTED': BuyNoInRange,
            'REDACTED': REDACTED,
            'REDACTED': FadeFirstTrade,
            'TP': ActiveExitStub,
            'REDACTED': ThresholdFade,
            'A': BuyYesInRange, 'Avol': REDACTED,
            'B': BuyNoOnDrop, 'C': BuyNoInRange, 'Cvol': REDACTED,
            'D': FadeFirstTrade, 'H1': FadeFirstTrade,
            'Dvol': REDACTED, 'REDACTED': REDACTED,
            'Amom': REDACTED, 'REDACTED': REDACTED,
            'Cmom': REDACTED, 'REDACTED': REDACTED,
            'Cstack': REDACTED, 'REDACTED': REDACTED,
            'H5_15m': ThresholdFade, 'H5_15M': ThresholdFade,
        })
    strategy_names = [s.strip() for s in args.strategy.split(',')]

    strategies = []
    for name in strategy_names:
        cls = strategy_map.get(name)
        if cls is None:
            print(f"Unknown strategy: {name}. Available: REDACTED, REDACTED, REDACTED, REDACTED, TP, REDACTED", file=sys.stderr)
            sys.exit(1)
        kwargs: dict = {}
        if args.min_price is not None:
            kwargs['min_price'] = args.min_price
        if args.max_price is not None:
            kwargs['max_price'] = args.max_price
        if name in ('TP', 'A', 'Avol', 'REDACTED', 'REDACTED', 'C', 'Cvol', 'REDACTED', 'REDACTED',
                    'Amom', 'REDACTED', 'Cmom', 'REDACTED'):
            if args.tp is not None:
                kwargs['take_profit'] = args.tp
            if args.sl is not None:
                kwargs['stop_loss'] = args.sl
        if name in ('D', 'H1', 'REDACTED'):
            if args.tp is not None:
                kwargs['take_profit'] = args.tp
            if args.sl is not None:
                kwargs['stop_loss'] = args.sl
            if args.h1_threshold_high is not None:
                kwargs['threshold_high'] = args.h1_threshold_high
            if args.h1_threshold_low is not None:
                kwargs['threshold_low'] = args.h1_threshold_low
        if name in ('H5_15M', 'H5_15m', 'REDACTED'):
            if args.h5_fav_threshold is not None:
                kwargs['fav_threshold'] = args.h5_fav_threshold
            if args.h5_long_threshold is not None:
                kwargs['long_threshold'] = args.h5_long_threshold
        # Load BTC OHLC data for momentum-filtered strategies
        if name in ('Amom', 'REDACTED', 'Cmom', 'REDACTED', 'Cstack', 'REDACTED'):
            import sqlite3 as _sql
            _conn = _sql.connect(str(args.db_path))
            _conn.row_factory = _sql.Row
            _rows = _conn.execute('SELECT timestamp, close FROM btc_ohlc ORDER BY timestamp').fetchall()
            kwargs['btc_closes'] = {r['timestamp']: r['close'] for r in _rows}
            _conn.close()
            print(f'  Loaded {len(kwargs["btc_closes"])} BTC candles for momentum filter', file=sys.stderr)

        strategies.append(cls(**kwargs))

    start = date.fromisoformat(args.start) if args.start else None
    end = date.fromisoformat(args.end) if args.end else None

    backtester = EventBacktester()
    result = backtester.run(
        series=args.series,
        strategies=strategies,
        start=start,
        end=end,
        initial_cash=args.cash,
        slippage_cents=args.slippage,
        db_path=Path(args.db_path),
        fee_pct=args.fee_pct,
    )

    print(result.summary())

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(result.to_dict(), f, indent=2)
    print(f"\nJSON saved to {args.output}")


def _cmd_archive(args) -> None:
    from edge_catcher.storage.db import get_connection
    from edge_catcher.storage.archiver import archive_old_trades

    conn = get_connection(Path(args.db_path))
    try:
        result = archive_old_trades(conn, Path(args.archive_dir), days_to_keep=90)
        if result["rows_archived"]:
            print(
                f"Archived {result['rows_archived']} trades → {result['archive_file']}"
            )
        else:
            print("No trades old enough to archive.")
    finally:
        conn.close()


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Edge Catcher — prediction market statistical edge finder"
    )
    parser.add_argument("--config", default="config", help="Config directory")
    parser.add_argument("--verbose", "-v", action="store_true")

    sub = parser.add_subparsers(dest="command")

    dl = sub.add_parser("download", help="Download market data from Kalshi")
    dl.add_argument("--db-path", default="data/kalshi.db")
    dl.add_argument("--dry-run", action="store_true", help="Fetch one page only")
    dl.add_argument(
        "--skip-market-scan",
        action="store_true",
        help="Skip Phase 1 market page scan and go directly to trade downloads. "
             "Use on restart when markets are already in DB.",
    )
    dl.add_argument(
        "--max-trade-markets",
        type=int,
        default=None,
        metavar="N",
        help="Cap trades download to top N markets by volume (default: all volume>0 markets)",
    )

    btc = sub.add_parser("download-btc", help="Download BTC-USD 1-minute OHLC from Coinbase")
    btc.add_argument("--db", default="data/kalshi.db", help="Path to SQLite DB")
    btc.set_defaults(func=_cmd_download_btc)

    an = sub.add_parser("analyze", help="Run hypothesis analysis against local DB")
    an.add_argument("--hypothesis", default=None, help="Hypothesis ID (default: all)")
    an.add_argument("--db-path", default="data/kalshi.db")
    an.add_argument("--output", default="reports/latest_analysis.json")

    ar = sub.add_parser("archive", help="Archive trades older than 90 days")
    ar.add_argument("--db-path", default="data/kalshi.db")
    ar.add_argument("--archive-dir", default="data/archive")

    bt = sub.add_parser("backtest", help="Run event-driven backtest on historical trade data")
    bt.add_argument("--series", required=True, help="Series ticker (e.g. KXBTCD, KXBTC15M)")
    bt.add_argument("--strategy", default="A", help="Comma-separated strategy names: A,B,C,TP")
    bt.add_argument("--start", default=None, help="Start date ISO format (e.g. 2025-06-01)")
    bt.add_argument("--end", default=None, help="End date ISO format (e.g. 2026-03-30)")
    bt.add_argument("--cash", type=float, default=10000.0, help="Initial capital (default: 10000)")
    bt.add_argument("--slippage", type=int, default=1, help="Slippage in cents (default: 1)")
    bt.add_argument("--tp", type=int, default=None, help="Take profit cents for ActiveExitStub (default: 8)")
    bt.add_argument("--sl", type=int, default=None, help="Stop loss cents for ActiveExitStub (default: 5)")
    bt.add_argument("--min-price", type=int, default=None, dest="min_price", help="Override strategy min price")
    bt.add_argument("--max-price", type=int, default=None, dest="max_price", help="Override strategy max price")
    bt.add_argument("--h1-threshold-high", type=int, default=None, dest="h1_threshold_high",
                    help="H1 entry threshold for high (fade NO above this, default: 60)")
    bt.add_argument("--h1-threshold-low", type=int, default=None, dest="h1_threshold_low",
                    help="H1 entry threshold for low (fade YES below this, default: 40)")
    bt.add_argument("--h5-fav-threshold", type=int, default=None, dest="h5_fav_threshold",
                    help="H5_15m favorite threshold — buy NO at or above this (default: 85)")
    bt.add_argument("--h5-long-threshold", type=int, default=None, dest="h5_long_threshold",
                    help="H5_15m longshot threshold — buy YES at or below this (default: 15)")
    bt.add_argument("--db-path", default="data/kalshi.db", dest="db_path")
    bt.add_argument("--output", default="reports/backtest_result.json")
    bt.add_argument("--fee-pct", type=float, default=0.07, dest="fee_pct",
                    help="Fee as fraction of profit on wins (default: 0.07 = 7%% Kalshi taker fee)")

    pt = sub.add_parser("paper-trade", help="Run paper trading simulation via Kalshi WebSocket")
    pt.add_argument("--db", default="data/paper_trades.db")
    pt.add_argument("--min-price", type=int, default=70, help="Min yes_ask to enter for Strategy A (cents)")
    pt.add_argument("--max-price", type=int, default=99, help="Max yes_ask to enter for Strategy A (cents)")
    pt.add_argument("--enable-strategy-b", action="store_true", default=False,
                    help="Enable contrarian NO strategy (default: disabled — killed 2026-03-31 after historical backtest confirmed -$48.57 net, 32.4%% win rate)")
    pt.set_defaults(func=_cmd_paper_trade)

    pt15 = sub.add_parser('paper-trade-15m', help='Run 15-min BTC paper trading (Strategy D)')
    pt15.add_argument('--db', default='data/paper_trades.db')
    pt15.add_argument('--threshold-high', type=int, default=60,
                      help='Buy NO when first yes_ask > this (cents)', dest='threshold_high')
    pt15.add_argument('--threshold-low', type=int, default=40,
                      help='Buy YES when first yes_ask < this (cents)', dest='threshold_low')
    pt15.set_defaults(func=_cmd_paper_trade_15m)

    fm = sub.add_parser(
        "formalize",
        help="Formalize a hypothesis from plain English (requires AI)",
    )
    fm.add_argument("description", help="Your hypothesis in plain English")
    fm.add_argument(
        "--provider",
        choices=["anthropic", "openai", "openrouter"],
        default=None,
    )
    fm.add_argument("--model", default=None, help="Override model name")

    ip = sub.add_parser(
        "interpret",
        help="Interpret analysis results in plain English (requires AI)",
    )
    ip.add_argument(
        "report",
        nargs="?",
        default="reports/latest_analysis.json",
        help="Path to analysis JSON (default: reports/latest_analysis.json)",
    )
    ip.add_argument(
        "--provider",
        choices=["anthropic", "openai", "openrouter"],
        default=None,
    )
    ip.add_argument("--model", default=None, help="Override model name")

    args = parser.parse_args()
    _setup_logging(getattr(args, "verbose", False))

    if args.command == "backtest":
        _cmd_backtest(args)
    elif args.command == "download":
        _cmd_download(args)
    elif args.command == "download-btc":
        _cmd_download_btc(args)
    elif args.command == "analyze":
        _cmd_analyze(args)
    elif args.command == "archive":
        _cmd_archive(args)
    elif args.command == "formalize":
        _cmd_formalize(args)
    elif args.command == "interpret":
        _cmd_interpret(args)
    elif args.command == "paper-trade":
        _cmd_paper_trade(args)
    elif args.command == "paper-trade-15m":
        _cmd_paper_trade_15m(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
