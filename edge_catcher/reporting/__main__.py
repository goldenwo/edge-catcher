"""CLI entry point for the reporting module.

Usage:
	python -m edge_catcher.reporting --db <path> [--date YYYY-MM-DD]
	python -m edge_catcher.reporting --db <path> --notify <channel> [--notify <channel> ...]
	    [--notify-config path/to/notifications.yaml] [--quiet]

Without --notify: prints the JSON report to stdout (indent=2 — backward compatible).
With --notify: also dispatches a Notification to each named channel.
With --quiet: suppresses the stdout JSON (only stderr table + exit code).

The default --notify-config is `config.local/notifications.yaml`, resolved
relative to the process working directory. For cron use, either invoke from
the repo root or pass --notify-config with an absolute path.

Exit codes:
	0 — report generated; if --notify was used, at least one channel succeeded
	1 — all configured channels failed at delivery; or generate_report() returned an error
	2 — setup error (config not found, malformed, unknown channel name)
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from edge_catcher.notifications import (
	DeliveryResult,
	NotificationConfigError,
	load_channels,
	send,
)
from edge_catcher.reporting import generate_report
from edge_catcher.reporting.notify import error_report_to_notification, report_to_notification


_DEFAULT_NOTIFY_CONFIG = Path("config.local") / "notifications.yaml"


def _build_parser() -> argparse.ArgumentParser:
	p = argparse.ArgumentParser(
		prog="edge_catcher.reporting",
		description="Daily P&L report from a paper_trades sqlite DB.",
	)
	p.add_argument("--db", required=True, type=Path, help="Path to the paper_trades sqlite DB.")
	p.add_argument("--date", default=None, help="YYYY-MM-DD for today bucket (UTC default).")
	p.add_argument(
		"--notify", action="append", default=[],
		help="Channel name from the notify config. May be repeated.",
	)
	p.add_argument(
		"--notify-config", default=str(_DEFAULT_NOTIFY_CONFIG),
		help=(
			f"Path to notifications YAML (default: {_DEFAULT_NOTIFY_CONFIG}, "
			"resolved relative to cwd). Use an absolute path for cron jobs."
		),
	)
	p.add_argument(
		"--quiet", action="store_true",
		help="Suppress the stdout JSON dump when --notify is in use.",
	)
	return p


def _print_results_table(results: dict[str, DeliveryResult]) -> None:
	"""Print the per-channel delivery results to stderr.

	Format spec (locked by tests):
	  channel              status  latency
	  -------------------- ------- -------
	  <name padded 20>     <OK|FAIL>     <latency or error truncated to 80>
	"""
	print(f"{'channel':<20} {'status':<7} {'latency':<7}", file=sys.stderr)
	print(f"{'-' * 20} {'-' * 7} {'-' * 7}", file=sys.stderr)
	for name, r in results.items():
		if r.success:
			tail = f"{int(r.latency_ms)}ms"
			status = "OK"
		else:
			err = (r.error or "").replace("\n", " ")
			tail = err[:80]
			status = "FAIL"
		print(f"{name:<20} {status:<7} {tail}", file=sys.stderr)


def main(argv: list[str] | None = None) -> int:
	args = _build_parser().parse_args(argv)
	if args.quiet and not args.notify:
		print(
			"warning: --quiet has no effect without --notify; ignoring",
			file=sys.stderr,
		)
	report = generate_report(args.db, date=args.date)
	if "error" in report:
		# If --notify is set, dispatch an error-severity notification before exiting.
		if args.notify:
			try:
				all_channels = load_channels(args.notify_config)
			except NotificationConfigError as exc:
				# Config broken AND report broken — surface both, prefer config exit code.
				print(f"notification config error: {exc}", file=sys.stderr)
				print(json.dumps(report, indent=2, default=str), file=sys.stdout)
				return 2
			missing = [n for n in args.notify if n not in all_channels]
			if missing:
				print(
					f"unknown channel(s): {missing}; defined: {sorted(all_channels)}",
					file=sys.stderr,
				)
				print(json.dumps(report, indent=2, default=str), file=sys.stdout)
				return 2
			notification = error_report_to_notification(report)
			selected = [all_channels[n] for n in args.notify]
			results = send(notification, selected)
			_print_results_table(results)
		if not args.quiet:
			print(json.dumps(report, indent=2, default=str), file=sys.stdout)
		return 1

	if not args.notify:
		# No notify requested — preserve v1.0.x behavior byte-for-byte.
		print(json.dumps(report, indent=2, default=str), file=sys.stdout)
		return 0

	# Notify path.
	try:
		all_channels = load_channels(args.notify_config)
	except NotificationConfigError as exc:
		print(f"notification config error: {exc}", file=sys.stderr)
		return 2

	missing = [n for n in args.notify if n not in all_channels]
	if missing:
		print(
			f"unknown channel(s): {missing}; defined: {sorted(all_channels)}",
			file=sys.stderr,
		)
		return 2

	notification = report_to_notification(report)
	selected = [all_channels[n] for n in args.notify]
	results = send(notification, selected)
	_print_results_table(results)

	if not args.quiet:
		# Same format as the no-notify branch (backward compat).
		print(json.dumps(report, indent=2, default=str), file=sys.stdout)

	# Exit code: 0 if any success, 1 if all failed.
	if any(r.success for r in results.values()):
		return 0
	return 1


if __name__ == "__main__":
	raise SystemExit(main())
