"""CLI: live-trade command — run the trading engine from config.

Thin wrapper over the SAME ``run_engine`` path the paper trader uses. Per
the converged spec §2, the execution mode is DATA carried by the config's
``executor:`` key and resolved downstream — this subcommand does NOT decide
or branch on mode; it only selects a different default config path. The
fail-closed mode-coherence invariant lives downstream (sub-project E2).
"""

from pathlib import Path


def _run_live_trade(args) -> None:
	import asyncio
	from edge_catcher.engine.engine import run_engine

	asyncio.run(run_engine(config_path=Path(args.config)))


def register(subparsers) -> None:
	p = subparsers.add_parser("live-trade", help="Run the trading engine via Kalshi WebSocket")
	p.add_argument(
		"--config",
		default="config.local/live-trader.yaml",
		help="Path to live trader config (default: config.local/live-trader.yaml)",
	)
	p.set_defaults(func=_run_live_trade)
