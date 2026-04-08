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

    markets_file = Path(args.markets) if args.markets else Path(args.config) / "markets.yaml"

    # Derive DB path from markets file name when not explicitly provided
    # e.g. markets-crypto.yaml → data/kalshi-crypto.db, markets.yaml → data/kalshi.db
    if args.db_path:
        db_path = Path(args.db_path)
    else:
        stem = markets_file.stem  # e.g. "markets-crypto" or "markets"
        suffix = stem.removeprefix("markets")  # e.g. "-crypto" or ""
        db_path = Path(f"data/kalshi{suffix}.db")

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


def _cmd_download_btc(args) -> None:
    from edge_catcher.adapters.coinbase import CoinbaseAdapter
    from edge_catcher.storage.db import get_connection, init_btc_ohlc_table
    from datetime import datetime, timezone
    import time

    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = get_connection(db_path)
    init_btc_ohlc_table(conn)

    # Default start: 2025-03-21T00:00:00 UTC (earliest Kalshi market open)
    start_ts = int(datetime(2025, 3, 21, tzinfo=timezone.utc).timestamp())
    end_ts = int(datetime.now(timezone.utc).timestamp())

    adapter = CoinbaseAdapter()
    n = adapter.download_range(start_ts, end_ts, conn)
    conn.close()
    print(f"Downloaded {n:,} new BTC-USD candles.")


def _cmd_download_altcoin_ohlc(args) -> None:
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


def _cmd_paper_trade_v2(args) -> None:
    from dotenv import load_dotenv
    load_dotenv()
    from edge_catcher.monitors.paper_trader_v2 import run_paper_trader_v2, ALL_SERIES
    import asyncio
    series = [s.strip().upper() for s in args.series.split(",") if s.strip()] if args.series else list(ALL_SERIES)
    asyncio.run(run_paper_trader_v2(
        db_path=Path(args.db),
        active_series=series,
    ))


def _build_strategy_map():
    """Build the strategy name → class mapping. Returns (strategy_map, has_local)."""
    import importlib
    from edge_catcher.runner.strategy_parser import (
        STRATEGIES_PUBLIC_MODULE, STRATEGIES_LOCAL_MODULE, STRATEGIES_LOCAL_PATH,
    )

    strategy_map: dict = {}

    # Auto-discover public strategies
    pub_mod = importlib.import_module(STRATEGIES_PUBLIC_MODULE)
    for attr_name in dir(pub_mod):
        obj = getattr(pub_mod, attr_name)
        if isinstance(obj, type) and hasattr(obj, 'on_trade'):
            name_attr = getattr(obj, 'name', None)
            if isinstance(name_attr, str):
                strategy_map[name_attr] = obj

    # Auto-discover local strategies (override public if same name)
    local_mod = None
    if STRATEGIES_LOCAL_PATH.exists():
        try:
            local_mod = importlib.import_module(STRATEGIES_LOCAL_MODULE)
            for attr_name in dir(local_mod):
                obj = getattr(local_mod, attr_name)
                if isinstance(obj, type) and hasattr(obj, 'on_trade'):
                    name_attr = getattr(obj, 'name', None)
                    if isinstance(name_attr, str):
                        strategy_map[name_attr] = obj
        except ImportError:
            pass

    return strategy_map, local_mod is not None


def _auto_strategy_args(parser) -> None:
    """Auto-generate CLI args from strategy __init__ signatures.

    Inspects all discovered strategies and creates --param-name args
    for each unique parameter. No manual arg definitions needed.
    """
    import inspect

    strategy_map, _ = _build_strategy_map()

    # Params that are internal or handled specially (not CLI-configurable)
    SKIP = {'self', 'size', 'btc_closes', 'ohlc_provider'}

    seen: dict[str, tuple[type, object]] = {}
    for cls in strategy_map.values():
        sig = inspect.signature(cls.__init__)
        for name, param in sig.parameters.items():
            if name in SKIP or name in seen:
                continue
            if param.kind in (inspect.Parameter.VAR_KEYWORD, inspect.Parameter.VAR_POSITIONAL):
                continue
            default = param.default if param.default != inspect.Parameter.empty else None
            param_type = type(default) if default is not None else int
            seen[name] = (param_type, default)

    for name, (ptype, default) in sorted(seen.items()):
        cli_flag = f'--{name.replace("_", "-")}'
        help_text = f'(default: {default})' if default is not None else None
        try:
            parser.add_argument(cli_flag, type=ptype, default=None,
                                dest=name, help=help_text)
        except Exception:
            pass  # Already defined (e.g., by framework args)


def _load_coin_closes(series: str, btc_db: str, altcoin_db: str) -> dict:
    """Load price closes dict {timestamp: close} for the coin matching the series prefix."""
    import sqlite3 as _sql
    import logging as _logging
    _log = _logging.getLogger(__name__)

    # Map series prefix → (coin, db_path, table_name)
    _SERIES_MAP = [
        ("KXBTC", "BTC", btc_db, "btc_ohlc"),
        ("KXSOL", "SOL", altcoin_db, "sol_ohlc"),
        ("KXETH", "ETH", altcoin_db, "eth_ohlc"),
        ("KXXRP", "XRP", altcoin_db, "xrp_ohlc"),
        ("KXDOGE", "DOGE", altcoin_db, "doge_ohlc"),
        ("KXBNB", "BNB", altcoin_db, "bnb_ohlc"),
        ("KXHYPE", "HYPE", altcoin_db, "hype_ohlc"),
    ]

    db_path = btc_db
    table = "btc_ohlc"
    coin = "BTC"
    for prefix, c, db, tbl in _SERIES_MAP:
        if series.upper().startswith(prefix):
            coin, db_path, table = c, db, tbl
            break

    try:
        _conn = _sql.connect(str(db_path))
        _conn.row_factory = _sql.Row
        # Check table exists before querying
        exists = _conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        if not exists:
            _conn.close()
            _log.warning("_load_coin_closes: table %s not found in %s — skipping momentum filter", table, db_path)
            return {}
        _rows = _conn.execute(f"SELECT timestamp, close FROM {table} ORDER BY timestamp").fetchall()
        _conn.close()
        return {r["timestamp"]: r["close"] for r in _rows}
    except Exception as exc:
        _log.warning("_load_coin_closes: could not load %s from %s: %s — skipping momentum filter", table, db_path, exc)
        return {}


def _cmd_backtest(args) -> None:
    import json
    from datetime import date

    json_mode = getattr(args, 'json', False)
    ohlc_provider = None

    # --- --list-strategies: output unique strategy names and exit ---
    if getattr(args, 'list_strategies', False):
        strategy_map, _ = _build_strategy_map()
        # Deduplicate: keep first name per class (preserves logical ordering)
        seen_classes: set = set()
        unique_names: list = []
        for name, cls in strategy_map.items():
            if cls not in seen_classes:
                seen_classes.add(cls)
                unique_names.append(name)
        print(json.dumps({"strategies": sorted(unique_names)}))
        return

    # --- --list-series: query DB for distinct series and exit ---
    if getattr(args, 'list_series', False):
        import sqlite3
        db_path = args.db_path
        try:
            conn = sqlite3.connect(db_path)
            rows = conn.execute(
                "SELECT DISTINCT series_ticker FROM markets ORDER BY series_ticker"
            ).fetchall()
            total = conn.execute("SELECT COUNT(*) FROM markets").fetchone()[0]
            conn.close()
            series = [r[0] for r in rows]
            print(json.dumps({"series": series, "db_path": db_path, "total_markets": total}))
        except Exception as exc:
            print(json.dumps({"status": "error", "message": str(exc)}))
            sys.exit(1)
        return

    strategy_map, _has_local = _build_strategy_map()

    try:
        if not args.series:
            msg = "--series is required for backtest (e.g. --series KXBTCD)"
            if json_mode:
                print(json.dumps({"status": "error", "message": msg}))
            else:
                print(f"error: {msg}", file=sys.stderr)
            sys.exit(1)

        strategy_names = [s.strip() for s in args.strategy.split(',')]

        import inspect

        strategies = []
        for name in strategy_names:
            cls = strategy_map.get(name)
            if cls is None:
                msg = f"Unknown strategy: {name}. Available: {', '.join(sorted(strategy_map))}"
                if json_mode:
                    print(json.dumps({"status": "error", "message": msg}))
                else:
                    print(msg, file=sys.stderr)
                sys.exit(1)

            # Build kwargs via introspection — only pass params the class accepts
            sig = inspect.signature(cls.__init__)
            available: dict = {}
            for param_name, param in sig.parameters.items():
                if param_name == 'self' or param.kind in (
                    inspect.Parameter.VAR_KEYWORD, inspect.Parameter.VAR_POSITIONAL,
                ):
                    continue
                if param_name == 'btc_closes':
                    closes = _load_coin_closes(args.series, args.btc_db, args.altcoin_ohlc_db)
                    if closes:
                        available['btc_closes'] = closes
                        print(f'  Loaded {len(closes)} candles for momentum filter', file=sys.stderr)
                    continue
                val = getattr(args, param_name, None)
                if val is not None:
                    available[param_name] = val

            strategies.append(cls(**available))

        # Inject OHLC provider if --ohlc-config is provided
        ohlc_provider = None
        if getattr(args, 'ohlc_config', None):
            from edge_catcher.research.ohlc_provider import OHLCProvider
            ohlc_map = json.loads(args.ohlc_config)
            ohlc_provider = OHLCProvider({
                asset: (paths[0], paths[1]) for asset, paths in ohlc_map.items()
            })
            for s in strategies:
                s.ohlc = ohlc_provider

        start = date.fromisoformat(args.start) if args.start else None
        end = date.fromisoformat(args.end) if args.end else None

        from edge_catcher.runner.event_backtest import EventBacktester
        from edge_catcher.fees import get_fee_model_for_series
        _base_model = get_fee_model_for_series(args.series)
        _fee_pct = args.fee_pct
        fee_fn = lambda p, s: _fee_pct * _base_model.calculate(p, s)
        backtester = EventBacktester()
        result = backtester.run(
            series=args.series,
            strategies=strategies,
            start=start,
            end=end,
            initial_cash=args.cash,
            slippage_cents=args.slippage,
            db_path=Path(args.db_path),
            fee_fn=fee_fn,
        )

        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(result.to_dict(), f, indent=2)

        if json_mode:
            payload = result.to_dict()
            payload['status'] = 'ok'
            print(json.dumps(payload))
        else:
            print(result.summary())
            print(f"\nJSON saved to {args.output}")

    except SystemExit:
        raise
    except Exception as exc:
        if json_mode:
            print(json.dumps({"status": "error", "message": str(exc)}))
            sys.exit(1)
        raise
    finally:
        if ohlc_provider is not None:
            ohlc_provider.close()


def _cmd_research(args) -> None:
    import json as _json
    from edge_catcher.research import ResearchAgent, Hypothesis, Tracker, Reporter, Evaluator, Thresholds

    research_db = getattr(args, 'research_db', 'data/research.db')
    tracker = Tracker(research_db)
    force = getattr(args, 'force', False)
    agent = ResearchAgent(tracker=tracker, force=force)

    subcmd = getattr(args, 'research_command', None)

    if subcmd == 'run':
        h = Hypothesis(
            strategy=args.strategy,
            series=args.series,
            db_path=args.db_path,
            start_date=args.start,
            end_date=args.end,
            fee_pct=args.fee_pct,
        )
        result = agent.run_hypothesis(h)
        output = Reporter._result_to_dict(result)
        print(_json.dumps(output, indent=2))

    elif subcmd == 'sweep':
        results = agent.sweep_all_series(
            strategy=args.strategy,
            fee_pct=args.fee_pct,
            start=args.start,
            end=args.end,
            max_runs=args.max_runs,
        )
        reporter = Reporter()
        report = reporter.generate_report(results)
        if getattr(args, 'output', None):
            reporter.save(report, args.output)
            print(f"Report saved to {args.output}", file=sys.stderr)
        print(_json.dumps(report, indent=2))

    elif subcmd == 'sweep-all':
        from edge_catcher.research.agent import _STRATEGY_FAMILY
        strategy_map, _ = _build_strategy_map()
        all_strategies = [s for s in strategy_map if s not in ('example',)]
        all_results: list = []
        for strategy in all_strategies:
            results = agent.sweep_all_series(
                strategy=strategy,
                fee_pct=args.fee_pct,
                start=args.start,
                end=args.end,
                max_runs=max(1, args.max_runs // len(all_strategies)),
            )
            all_results.extend(results)
        reporter = Reporter()
        report = reporter.generate_report(all_results)
        if getattr(args, 'output', None):
            reporter.save(report, args.output)
            print(f"Report saved to {args.output}", file=sys.stderr)
        print(_json.dumps(report, indent=2))

    elif subcmd == 'status':
        stats = tracker.stats()
        rows = tracker.list_results()
        print(f"\nResearch DB: {research_db}")
        print(f"Total results: {stats['total']}")
        for verdict, count in sorted(stats['by_verdict'].items()):
            print(f"  {verdict}: {count}")
        if rows:
            print(f"\nRecent results (last 10):")
            print(f"  {'Verdict':<10} {'Strategy':<12} {'Series':<20} {'Sharpe':>7} {'WinRate':>8} {'PnL(¢)':>9}")
            print(f"  {'-'*10} {'-'*12} {'-'*20} {'-'*7} {'-'*8} {'-'*9}")
            for row in rows[:10]:
                print(
                    f"  {row['verdict']:<10} {row['strategy']:<12} {row['series']:<20} "
                    f"{row['sharpe']:>7.2f} {row['win_rate']:>7.1%} {row['net_pnl_cents']:>9.0f}"
                )

    elif subcmd == 'report':
        rows = tracker.list_results()
        if not rows:
            print("No results in tracker yet. Run some hypotheses first.", file=sys.stderr)
            sys.exit(1)

        # Reconstruct minimal HypothesisResult objects from tracker rows for reporting
        from edge_catcher.research.hypothesis import HypothesisResult
        results = []
        for row in rows:
            h = Hypothesis(
                id=row['id'],
                strategy=row['strategy'],
                series=row['series'],
                db_path=row['db_path'],
                start_date=row['start_date'],
                end_date=row['end_date'],
                fee_pct=row['fee_pct'],
            )
            results.append(HypothesisResult(
                hypothesis=h,
                status=row['status'],
                total_trades=row['total_trades'],
                wins=row['wins'],
                losses=row['losses'],
                win_rate=row['win_rate'],
                net_pnl_cents=row['net_pnl_cents'],
                sharpe=row['sharpe'],
                max_drawdown_pct=row['max_drawdown_pct'],
                fees_paid_cents=row['fees_paid_cents'],
                avg_win_cents=0.0,
                avg_loss_cents=0.0,
                per_strategy={},
                verdict=row['verdict'],
                verdict_reason=row['verdict_reason'],
                raw_json={},
            ))

        reporter = Reporter()
        report = reporter.generate_report(results)
        from edge_catcher.reports import RESEARCH_OUTPUT
        output_path = getattr(args, 'output', None) or str(RESEARCH_OUTPUT)
        reporter.save(report, output_path)
        print(f"Report saved to {output_path}.json and {output_path}.md")

    elif subcmd == 'loop':
        from edge_catcher.research.loop import LoopOrchestrator
        orch = LoopOrchestrator(
            research_db=research_db,
            start_date=args.start,
            end_date=args.end,
            max_runs=args.max_runs,
            max_time_minutes=args.max_time,
            parallel=args.parallel,
            fee_pct=args.fee_pct,
            max_llm_calls=args.max_llm_calls,
            grid_only=args.grid_only,
            llm_only=args.llm_only,
            output_path=args.output,
            force=force,
            max_refinements=args.max_refinements,
            refine_only=args.refine_only,
            max_stuck_runs=args.max_stuck_runs,
        )
        exit_code, results = orch.run()

        # Print summary
        verdicts = {}
        for r in results:
            verdicts[r.verdict] = verdicts.get(r.verdict, 0) + 1
        print(f"\nLoop complete: {len(results)} runs")
        for v, c in sorted(verdicts.items()):
            print(f"  {v}: {c}")
        if exit_code == 2:
            print("\nBudget exhausted — run again to continue.")
        if exit_code == 3:
            print("\nLoop terminated: stuck with no progress. Review kill-registry and data sources.")
        sys.exit(exit_code)

    elif subcmd == 'audit':
        from edge_catcher.research.audit import AuditLog
        audit_log = AuditLog(research_db)
        audit_type = getattr(args, 'audit_type', None)

        if audit_type == 'decisions':
            for d in audit_log.list_decisions()[:20]:
                print(f"  [{d['created_at']}] model={d['model']} hash={d['prompt_hash'][:12]}...")
        elif audit_type == 'integrity':
            for c in audit_log.list_integrity_checks():
                print(f"  [{c['created_at']}] {c['checkpoint']}: "
                      f"hash={c['result_hash'][:12]}... count={c['result_count']}")
        elif audit_type == 'trace':
            trace_id = getattr(args, 'trace_id', None)
            if not trace_id:
                print("Usage: research audit trace --id <hypothesis-id>")
                sys.exit(1)
            execs = [e for e in audit_log.list_executions() if e['hypothesis_id'] == trace_id]
            if execs:
                for e in execs:
                    print(f"  Phase: {e['phase']}, Verdict: {e['verdict']}, "
                          f"Status: {e['status']}, At: {e['completed_at']}")
            else:
                print(f"  No audit records for hypothesis {trace_id}")

    elif subcmd == 'kill-registry':
        action = getattr(args, 'kill_registry_action', None)
        if action == 'list':
            entries = tracker.list_kill_registry()
            if not entries:
                print("Kill registry is empty.")
            else:
                print(f"\nKill Registry ({len(entries)} entries):")
                for e in entries:
                    perm = "PERMANENT" if e["permanent"] else "reset"
                    print(f"  {e['strategy']:30s} kill_rate={e['kill_rate']:.0%} "
                          f"({e['kill_count']}/{e['series_tested']}) [{perm}] {e['reason_summary']}")
        elif action == 'reset':
            name = getattr(args, 'kill_registry_strategy', None)
            if not name:
                print("Usage: research kill-registry reset --strategy <name>")
                sys.exit(1)
            tracker.reset_kill_registry(name)
            print(f"Reset '{name}' — it can now be re-proposed by the ideator.")

    else:
        print("Usage: python -m edge_catcher research {run|sweep|sweep-all|status|report|loop|audit|kill-registry}")
        sys.exit(1)


def _cmd_list_dbs(args) -> None:
    import json
    import sqlite3

    data_dir = Path("data")
    databases = []
    for db_file in sorted(data_dir.glob("*.db")):
        size_mb = round(db_file.stat().st_size / (1024 * 1024), 1)
        try:
            conn = sqlite3.connect(str(db_file))
            rows = conn.execute(
                "SELECT DISTINCT series_ticker FROM markets ORDER BY series_ticker"
            ).fetchall()
            conn.close()
            series = [r[0] for r in rows]
        except Exception:
            series = []
        databases.append({"path": str(db_file), "size_mb": size_mb, "series": series})
    print(json.dumps({"databases": databases}))


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
        help="Path to markets YAML file (default: {config}/markets.yaml). "
             "Example: --markets config/markets-crypto.yaml",
    )
    dl.add_argument(
        "--max-trade-markets",
        type=int,
        default=None,
        metavar="N",
        help="Cap trades download to top N markets by volume (default: all volume>0 markets)",
    )

    btc = sub.add_parser("download-btc", help="Download BTC-USD 1-minute OHLC from Coinbase")
    btc.add_argument("--db", default="data/btc.db", help="Path to SQLite DB for BTC OHLC data")
    btc.set_defaults(func=_cmd_download_btc)

    altcoin = sub.add_parser("download-altcoin-ohlc", help="Download 1-minute OHLC for altcoins from Coinbase")
    altcoin.add_argument("--db", default="data/ohlc.db", help="Path to SQLite DB for altcoin OHLC data")
    altcoin.add_argument("--coins", default="SOL,ETH,XRP,DOGE,BNB",
                         help="Comma-separated coin symbols (default: SOL,ETH,XRP,DOGE,BNB)")
    altcoin.add_argument("--start-date", default="2025-01-01", dest="start_date",
                         help="Start date ISO format (default: 2025-01-01)")
    altcoin.add_argument("--end-date", default=None, dest="end_date",
                         help="End date ISO format (default: today)")
    altcoin.set_defaults(func=_cmd_download_altcoin_ohlc)

    an = sub.add_parser("analyze", help="Run hypothesis analysis against local DB")
    an.add_argument("--hypothesis", default=None, help="Hypothesis ID (default: all)")
    an.add_argument("--db-path", default="data/kalshi.db")
    from edge_catcher.reports import ANALYSIS_OUTPUT, BACKTEST_OUTPUT, RESEARCH_OUTPUT
    an.add_argument("--output", default=str(ANALYSIS_OUTPUT))

    ar = sub.add_parser("archive", help="Archive trades older than 90 days")
    ar.add_argument("--db-path", default="data/kalshi.db")
    ar.add_argument("--archive-dir", default="data/archive")

    bt = sub.add_parser("backtest", help="Run event-driven backtest on historical trade data")
    bt.add_argument("--series", default=None, help="Series ticker (e.g. KXBTCD, KXBTC15M)")
    bt.add_argument("--strategy", default="example", help="Comma-separated strategy names (use --list-strategies)")
    bt.add_argument("--start", default=None, help="Start date ISO format (e.g. 2025-06-01)")
    bt.add_argument("--end", default=None, help="End date ISO format (e.g. 2026-03-30)")
    bt.add_argument("--cash", type=float, default=10000.0, help="Initial capital (default: 10000)")
    bt.add_argument("--slippage", type=int, default=1, help="Slippage in cents (default: 1)")
    # Strategy-specific params auto-generated from __init__ signatures
    _auto_strategy_args(bt)
    bt.add_argument("--db-path", default="data/kalshi.db", dest="db_path")
    bt.add_argument("--output", default=str(BACKTEST_OUTPUT))
    bt.add_argument("--fee-pct", type=float, default=1.0, dest="fee_pct",
                    help="Multiplier on 0.07*P*(1-P) entry fee formula (default: 1.0 = full Kalshi taker fee; 0.25 = maker fee; 0.0 = no fee)")
    bt.add_argument("--json", action="store_true", default=False,
                    help="Output only valid JSON to stdout; progress goes to stderr")
    bt.add_argument("--list-strategies", action="store_true", default=False, dest="list_strategies",
                    help="Print available strategy names as JSON and exit")
    bt.add_argument("--list-series", action="store_true", default=False, dest="list_series",
                    help="Print distinct series_ticker values from the DB as JSON and exit")
    bt.add_argument("--btc-db", default="data/btc.db", dest="btc_db",
                    help="Path to BTC OHLC database (default: data/btc.db)")
    bt.add_argument("--altcoin-ohlc-db", default="data/ohlc.db", dest="altcoin_ohlc_db",
                    help="Path to altcoin OHLC database (default: data/ohlc.db)")
    bt.add_argument("--ohlc-config", default=None, dest="ohlc_config",
                    help='JSON mapping asset names to [db_path, table] pairs '
                         '(e.g. \'{"btc": ["data/kalshi.db", "btc_ohlc"]}\')')

    ldbs = sub.add_parser("list-dbs", help="Scan data/ for *.db files and list their series as JSON")
    ldbs.set_defaults(func=_cmd_list_dbs)

    rs = sub.add_parser("research", help="Automated hypothesis research across market categories")
    rs.add_argument("--research-db", default="data/research.db", dest="research_db",
                    help="Path to research tracker SQLite DB (default: data/research.db)")
    rs_sub = rs.add_subparsers(dest="research_command")

    rs_run = rs_sub.add_parser("run", help="Run a single hypothesis")
    rs_run.add_argument("--strategy", required=True, help="Strategy name (use backtest --list-strategies)")
    rs_run.add_argument("--series", required=True, help="Series ticker (e.g. KXBTCD)")
    rs_run.add_argument("--db-path", required=True, dest="db_path", help="Path to database")
    rs_run.add_argument("--start", required=True, help="Start date ISO (e.g. 2025-01-01)")
    rs_run.add_argument("--end", required=True, help="End date ISO (e.g. 2025-12-31)")
    rs_run.add_argument("--fee-pct", type=float, default=1.0, dest="fee_pct",
                        help="Fee multiplier (default: 1.0)")
    rs_run.add_argument("--force", action="store_true", default=False,
                        help="Re-run even if already tested (overwrite existing result)")

    rs_sweep = rs_sub.add_parser("sweep", help="Sweep one strategy across all available series/DBs")
    rs_sweep.add_argument("--strategy", required=True, help="Strategy name")
    rs_sweep.add_argument("--fee-pct", type=float, default=1.0, dest="fee_pct")
    rs_sweep.add_argument("--start", default="2025-01-01", help="Start date (default: 2025-01-01)")
    rs_sweep.add_argument("--end", default="2025-12-31", help="End date (default: 2025-12-31)")
    rs_sweep.add_argument("--max-runs", type=int, default=50, dest="max_runs",
                          help="Maximum hypotheses to run (default: 50)")
    rs_sweep.add_argument("--output", default=None, help="Save report to this base path")
    rs_sweep.add_argument("--force", action="store_true", default=False,
                          help="Re-run even if already tested (overwrite existing results)")

    rs_sweepall = rs_sub.add_parser("sweep-all", help="Sweep ALL strategies across ALL available data")
    rs_sweepall.add_argument("--fee-pct", type=float, default=1.0, dest="fee_pct")
    rs_sweepall.add_argument("--start", default="2025-01-01")
    rs_sweepall.add_argument("--end", default="2025-12-31")
    rs_sweepall.add_argument("--max-runs", type=int, default=200, dest="max_runs",
                             help="Total maximum hypotheses to run (default: 200)")
    rs_sweepall.add_argument("--output", default=str(RESEARCH_OUTPUT))
    rs_sweepall.add_argument("--force", action="store_true", default=False,
                             help="Re-run even if already tested (overwrite existing results)")

    rs_status = rs_sub.add_parser("status", help="Show what has been tested")

    rs_report = rs_sub.add_parser("report", help="Generate report from tracked results")
    rs_report.add_argument("--output", default=str(RESEARCH_OUTPUT),
                           help="Output base path (suffixes .json and .md added)")

    # Loop subcommand
    rs_loop = rs_sub.add_parser("loop", help="Autonomous research loop: grid sweep + LLM ideation")
    rs_loop.add_argument("--max-runs", type=int, default=0, dest="max_runs",
                         help="Max total backtests across both phases (default: 0 = unlimited)")
    rs_loop.add_argument("--max-time", type=float, default=None, dest="max_time",
                         help="Wall-clock timeout in minutes")
    rs_loop.add_argument("--parallel", type=int, default=1,
                         help="Concurrent backtests (default: 1)")
    rs_loop.add_argument("--fee-pct", type=float, default=1.0, dest="fee_pct")
    rs_loop.add_argument("--start", default=None, help="Start date ISO (default: all data)")
    rs_loop.add_argument("--end", default=None, help="End date ISO (default: all data)")
    rs_loop.add_argument("--max-llm-calls", type=int, default=10, dest="max_llm_calls",
                         help="Cap on LLM API calls in ideation phase (default: 10)")
    rs_loop.add_argument("--grid-only", action="store_true", dest="grid_only",
                         help="Skip LLM phase")
    rs_loop.add_argument("--llm-only", action="store_true", dest="llm_only",
                         help="Skip grid/expansion, ideate from context + existing results only")
    rs_loop.add_argument("--output", default=None, help="Save report to this base path")
    rs_loop.add_argument("--force", action="store_true", default=False,
                         help="Re-run even if already tested (overwrite existing results)")
    rs_loop.add_argument("--max-refinements", type=int, default=3, dest="max_refinements",
                         help="Max refinement iterations per strategy (default: 3)")
    rs_loop.add_argument("--refine-only", action="store_true", dest="refine_only",
                         help="Skip grid and LLM phases, only refine existing strategies")
    rs_loop.add_argument("--max-stuck-runs", type=int, default=3, dest="max_stuck_runs",
                         help="Auto-terminate after N stuck runs post budget-shift (0=disable, default: 3)")

    # Audit subcommand
    rs_audit = rs_sub.add_parser("audit", help="Query the research audit log")
    rs_audit.add_argument("audit_type", choices=["decisions", "integrity", "trace"],
                          help="What to query")
    rs_audit.add_argument("--id", default=None, dest="trace_id",
                          help="Hypothesis ID for trace queries")

    # Kill registry subcommand
    rs_killreg = rs_sub.add_parser("kill-registry", help="Manage the persistent kill registry")
    rs_killreg.add_argument("kill_registry_action", choices=["list", "reset"],
                             help="Action: list all entries or reset a strategy")
    rs_killreg.add_argument("--strategy", default=None, dest="kill_registry_strategy",
                             help="Strategy name (required for reset)")

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

    ptv2 = sub.add_parser('paper-trade-v2',
                          help='Unified paper trader — KXBTCD, KXBTC15M, KXXRP, KXNBAMENTION, KXSOLD')
    ptv2.add_argument('--db', default='data/paper_trades_v2.db',
                      help='SQLite DB path (default: data/paper_trades_v2.db)')
    ptv2.add_argument('--series', default=None,
                      help='Comma-separated series to subscribe (default: all 5)')
    ptv2.set_defaults(func=_cmd_paper_trade_v2)

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
        default=str(ANALYSIS_OUTPUT),
        help=f"Path to analysis JSON (default: {ANALYSIS_OUTPUT})",
    )
    ip.add_argument(
        "--provider",
        choices=["anthropic", "openai", "openrouter"],
        default=None,
    )
    ip.add_argument("--model", default=None, help="Override model name")

    args = parser.parse_args()
    _setup_logging(getattr(args, "verbose", False))

    if args.command == "research":
        _cmd_research(args)
    elif args.command == "backtest":
        _cmd_backtest(args)
    elif args.command == "list-dbs":
        _cmd_list_dbs(args)
    elif args.command == "download":
        _cmd_download(args)
    elif args.command == "download-btc":
        _cmd_download_btc(args)
    elif args.command == "download-altcoin-ohlc":
        _cmd_download_altcoin_ohlc(args)
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
    elif args.command == "paper-trade-v2":
        _cmd_paper_trade_v2(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
