"""Tests for the `live-trade` engine subcommand (sub-project E, Phase E1).

E1 adds a THIN CLI wrapper: `live-trade` mirrors `paper-trade` exactly,
changing only the default config path. Mode is DATA resolved from the
config's `executor:` key downstream (E2/E3) — the subcommand itself does
NOT decide or branch on mode (spec §1/§2 keystone).

E2 will extend this module with the fail-closed mode-coherence invariant.
"""
from __future__ import annotations

import argparse
import subprocess
import sys


def _build_parser() -> argparse.ArgumentParser:
	"""Reconstruct the top-level parser exactly as ``edge_catcher.cli.main``
	does: a subparser group with every command module registered against it.
	"""
	from edge_catcher.cli import (
		backtest,
		download,
		formalize,
		interpret,
		live_trade,
		paper_trade,
		replay_backtest,
		research,
		utils,
	)

	parser = argparse.ArgumentParser()
	sub = parser.add_subparsers(dest="command")
	for module in [
		download,
		backtest,
		research,
		paper_trade,
		live_trade,
		formalize,
		interpret,
		utils,
		replay_backtest,
	]:
		module.register(sub)
	return parser


def test_live_trade_subcommand_registered() -> None:
	"""`live-trade` parses, defaults --config to the gitignored live config,
	and binds a func handler (the thin run_engine wrapper)."""
	parser = _build_parser()
	args = parser.parse_args(["live-trade"])
	assert args.command == "live-trade"
	assert args.config == "config.local/live-trader.yaml"
	assert hasattr(args, "func")


def test_paper_trade_subcommand_unchanged() -> None:
	"""E1 must NOT disturb the existing `paper-trade` subcommand: it is still
	registered with its original default config path and func handler."""
	parser = _build_parser()
	args = parser.parse_args(["paper-trade"])
	assert args.command == "paper-trade"
	assert args.config == "config.local/paper-trader.yaml"
	assert hasattr(args, "func")


def test_live_trade_help_exits_zero() -> None:
	"""`python -m edge_catcher live-trade --help` exits 0 and the subcommand
	is discoverable (end-to-end through the real registration path)."""
	proc = subprocess.run(
		[sys.executable, "-m", "edge_catcher", "live-trade", "--help"],
		capture_output=True,
		text=True,
		timeout=30,
	)
	assert proc.returncode == 0, proc.stderr
	# argparse line-wraps long help text (even inserting a break mid-token);
	# strip ALL whitespace before matching the (space-free) config path.
	assert "--config" in proc.stdout
	squashed = "".join(proc.stdout.split())
	assert "config.local/live-trader.yaml" in squashed


def test_paper_trade_help_still_exits_zero() -> None:
	"""Regression: the `paper-trade` subcommand still works end-to-end and
	its default config path is byte-unchanged."""
	proc = subprocess.run(
		[sys.executable, "-m", "edge_catcher", "paper-trade", "--help"],
		capture_output=True,
		text=True,
		timeout=30,
	)
	assert proc.returncode == 0, proc.stderr
	# argparse line-wraps long help text (even inserting a break mid-token);
	# strip ALL whitespace before matching the (space-free) config path.
	assert "--config" in proc.stdout
	squashed = "".join(proc.stdout.split())
	assert "config.local/paper-trader.yaml" in squashed
