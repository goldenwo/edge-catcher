"""Regenerate strict-parity fixtures for tests/test_replay_parity_first_seen.py.

CLI: python tests/fixtures/replay_parity/regenerate.py [--day YYYY-MM-DD | --all]

Schema per fixture file: {"engine_commit": "<sha>", "generated_at": "<iso>",
"keys": [[strategy, ticker, side, entry_time_iso, fill_size, blended_entry, fill_price], ...]}

Goal-table cross-check: refuses to write if len(keys - allowlist) != EXPECTED_TRADE_COUNTS[day].

Exit codes: 0 success; 2 Goal-table mismatch; 3 bundle missing (local + R2).

NOTE: Step 1.d.i ships this as a stub. The real implementation lands in 1.d.ii
once Steps 2/3 have been merged and replay_capture produces the post-fix output.
"""
from __future__ import annotations

import argparse
import sys

PARITY_DAYS = [
	"2026-04-17", "2026-04-19", "2026-04-20", "2026-04-21",
	"2026-04-22", "2026-04-23", "2026-04-24", "2026-04-25",
	"2026-04-26", "2026-04-27",
]

# TODO(replay-first-seen-fix): populate from live bundles in 1.d.ii.
# Each value is the post-fix expected total replay-trade count (= live trade count for that day).
EXPECTED_TRADE_COUNTS: dict[str, int] = {}


def main(argv: list[str] | None = None) -> int:
	p = argparse.ArgumentParser(description=__doc__)
	g = p.add_mutually_exclusive_group()
	g.add_argument("--day", choices=PARITY_DAYS, help="regenerate one day")
	g.add_argument("--all", action="store_true", help="regenerate all days (default)")
	args = p.parse_args(argv)
	days = [args.day] if args.day else PARITY_DAYS

	print(
		"regenerate.py: Step 1.d.i stub — fixture generation requires Steps 2/3 to be merged.\n"
		"Will be implemented in Step 1.d.ii. Days requested: " + ", ".join(days),
		file=sys.stderr,
	)
	return 1


if __name__ == "__main__":
	sys.exit(main())
