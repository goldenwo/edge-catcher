"""CLI: analyze command — run backtest and print formatted report."""

import sys
from pathlib import Path


def _run_analyze(args) -> None:
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


def register(subparsers) -> None:
	from edge_catcher.reports import ANALYSIS_OUTPUT

	p = subparsers.add_parser("analyze", help="Run hypothesis analysis against local DB")
	p.add_argument("--hypothesis", default=None, help="Hypothesis ID (default: all)")
	p.add_argument("--db-path", default="data/kalshi.db")
	p.add_argument("--output", default=str(ANALYSIS_OUTPUT))
	p.set_defaults(func=_run_analyze)
