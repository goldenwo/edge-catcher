"""CLI entry point: python -m edge_catcher [download|analyze|archive|formalize|interpret]"""

import sys
from pathlib import Path

# New thin shell — delegates to edge_catcher.cli command modules
from edge_catcher.cli import main, _try_load_dotenv, _setup_logging

__all__ = ["main", "_try_load_dotenv", "_setup_logging"]  # silence re-export lint hint


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
    from edge_catcher.monitors.paper_trader_v2 import run_paper_trader_v2, ALL_SERIES
    import asyncio
    series = [s.strip().upper() for s in args.series.split(",") if s.strip()] if args.series else list(ALL_SERIES)
    asyncio.run(run_paper_trader_v2(
        db_path=Path(args.db),
        active_series=series,
    ))



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


def _old_main() -> None:
    """Legacy main — kept for reference only. Use edge_catcher.cli.main instead."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Edge Catcher — prediction market statistical edge finder"
    )
    parser.add_argument("--config", default="config", help="Config directory")
    parser.add_argument("--verbose", "-v", action="store_true")

    sub = parser.add_subparsers(dest="command")

    an = sub.add_parser("analyze", help="Run hypothesis analysis against local DB")
    an.add_argument("--hypothesis", default=None, help="Hypothesis ID (default: all)")
    an.add_argument("--db-path", default="data/kalshi.db")
    from edge_catcher.reports import ANALYSIS_OUTPUT
    an.add_argument("--output", default=str(ANALYSIS_OUTPUT))

    ar = sub.add_parser("archive", help="Archive trades older than 90 days")
    ar.add_argument("--db-path", default="data/kalshi.db")
    ar.add_argument("--archive-dir", default="data/archive")

    from edge_catcher.cli.backtest import register as _register_backtest
    _register_backtest(sub)

    ldbs = sub.add_parser("list-dbs", help="Scan data/ for *.db files and list their series as JSON")
    ldbs.set_defaults(func=_cmd_list_dbs)

    from edge_catcher.cli.research import register as _register_research
    _register_research(sub)

    pt = sub.add_parser("paper-trade", help="Run paper trading via Kalshi WebSocket")
    pt.add_argument("--db", default="data/paper_trades_v2.db",
                    help="SQLite DB path (default: data/paper_trades_v2.db)")
    pt.add_argument("--series", default=None,
                    help="Comma-separated series to subscribe (default: all)")
    pt.set_defaults(func=_cmd_paper_trade)

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
        from edge_catcher.cli.research import run as _cmd_research
        _cmd_research(args)
    elif args.command == "backtest":
        from edge_catcher.cli.backtest import run as _cmd_backtest
        _cmd_backtest(args)
    elif args.command == "list-dbs":
        _cmd_list_dbs(args)
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
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    # TODO: switch to main() once all cli/* stubs are populated (Tasks 2-5)
    _old_main()
