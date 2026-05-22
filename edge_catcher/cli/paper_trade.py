"""CLI: paper-trade command — run paper trader from config.

Thin wrapper over the shared ``run_engine`` run-path
(``cli/_engine_run.run_engine_with_signal_bridge``) — the single entrypoint
that installs the POSIX SIGTERM/SIGINT -> cancel bridge so ``run_engine``'s
graceful-shutdown ``finally:`` drain is reachable under ``systemctl stop``.
The bridge only fires on an actual signal (never in paper replay/backtest/CI),
so paper behaviour on the non-signal path is byte-exact (§9 G-parity).
"""

from pathlib import Path


def _run_paper_trade(args) -> None:
	import asyncio
	from edge_catcher.cli._engine_run import run_engine_with_signal_bridge

	asyncio.run(run_engine_with_signal_bridge(config_path=Path(args.config)))


def register(subparsers) -> None:
	p = subparsers.add_parser("paper-trade", help="Run paper trading via Kalshi WebSocket")
	p.add_argument(
		"--config",
		default="config.local/paper-trader.yaml",
		help="Path to paper trader config (default: config.local/paper-trader.yaml)",
	)
	p.set_defaults(func=_run_paper_trade)
