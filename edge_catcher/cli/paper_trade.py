"""CLI: paper-trade command — run paper trader from config."""

from pathlib import Path


def _run_paper_trade(args) -> None:
	import asyncio
	from edge_catcher.monitors.engine import run_engine

	asyncio.run(run_engine(config_path=Path(args.config)))


def register(subparsers) -> None:
	p = subparsers.add_parser("paper-trade", help="Run paper trading via Kalshi WebSocket")
	p.add_argument(
		"--config",
		default="config.local/paper-trader.yaml",
		help="Path to paper trader config (default: config.local/paper-trader.yaml)",
	)
	p.set_defaults(func=_run_paper_trade)
