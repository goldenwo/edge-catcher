"""CLI entry: python -m edge_catcher.reporting --db <path> [--date YYYY-MM-DD]"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from edge_catcher.reporting import generate_report


def main() -> int:
	parser = argparse.ArgumentParser(
		prog="edge_catcher.reporting",
		description=__doc__,
	)
	parser.add_argument(
		"--db",
		required=True,
		type=Path,
		help="Path to paper_trades sqlite DB",
	)
	parser.add_argument(
		"--date",
		help="YYYY-MM-DD for the today bucket (default: UTC today)",
	)
	args = parser.parse_args()
	report = generate_report(args.db, date=args.date)
	print(json.dumps(report, indent=2, default=str))
	return 0 if "error" not in report else 1


if __name__ == "__main__":
	sys.exit(main())
