"""CLI entry point — thin shell that delegates to command modules."""

import argparse
import logging
import sys


def _try_load_dotenv() -> None:
	"""Load .env file if python-dotenv is installed."""
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


def main() -> None:
	from edge_catcher.cli import (
		download, backtest, research,
		paper_trade, formalize, interpret, utils,
		replay_backtest,
	)

	parser = argparse.ArgumentParser(
		description="Edge Catcher — prediction market statistical edge finder"
	)
	parser.add_argument("--config", default="config", help="Config directory")
	parser.add_argument("--verbose", "-v", action="store_true")

	sub = parser.add_subparsers(dest="command")

	for module in [download, backtest, research,
	               paper_trade, formalize, interpret, utils,
	               replay_backtest]:
		module.register(sub)

	args = parser.parse_args()
	_setup_logging(getattr(args, "verbose", False))
	_try_load_dotenv()

	if hasattr(args, "func"):
		args.func(args)
	else:
		parser.print_help()
		sys.exit(1)
