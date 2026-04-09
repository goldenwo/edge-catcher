"""CLI: paper-trade command — run paper trader via Kalshi WebSocket."""

import sys
from pathlib import Path


def _run_paper_trade(args) -> None:
	from edge_catcher.monitors.paper_trader_v2 import run_paper_trader_v2, ALL_SERIES
	import asyncio

	series = (
		[s.strip().upper() for s in args.series.split(",") if s.strip()]
		if args.series
		else list(ALL_SERIES)
	)
	asyncio.run(run_paper_trader_v2(
		db_path=Path(args.db),
		active_series=series,
	))


def register(subparsers) -> None:
	p = subparsers.add_parser("paper-trade", help="Run paper trading via Kalshi WebSocket")
	p.add_argument(
		"--db",
		default="data/paper_trades_v2.db",
		help="SQLite DB path (default: data/paper_trades_v2.db)",
	)
	p.add_argument(
		"--series",
		default=None,
		help="Comma-separated series to subscribe (default: all)",
	)
	p.set_defaults(func=_run_paper_trade)
