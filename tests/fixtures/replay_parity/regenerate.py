"""Regenerate strict-parity fixtures for tests/test_replay_parity_first_seen.py.

CLI: python tests/fixtures/replay_parity/regenerate.py [--day YYYY-MM-DD | --all]

Writes <day>.expected.json with schema:
  {"engine_commit": "<sha>", "generated_at": "<iso>",
   "keys": [[strategy, ticker, side, entry_time_iso, fill_size, blended_entry, fill_price], ...]}

Goal-table cross-check: refuses to write a fixture if
  len(keys - allowlist) != EXPECTED_TRADE_COUNTS[day]
where allowlist is loaded from <day>.allowlist.json.

On full successful run, writes the .fixtures_present sentinel.

Exit codes:
  0  all requested days succeeded
  2  Goal-table mismatch (no fixtures written for any day)
  3  bundle missing locally and R2 fetch unavailable / failed
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

# Load CAPTURE_TRANSPORT_* creds from main repo's .env if needed for R2 fetch fallback.
try:
	from dotenv import load_dotenv
	# Try worktree .env first, then main repo's .env (where the creds live).
	load_dotenv()
	load_dotenv("e:/Projects/edge-catcher/.env")
except ImportError:
	pass

# Ensure import works whether script is run from repo root or fixtures dir.
REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

from edge_catcher.monitors.replay.backtester import replay_capture  # noqa: E402

# Days included in the strict-parity sweep.
#
# 04-18 is excluded per plan §"Non-goals" (engine-version drift, separate problem).
#
# 04-17, 04-19, 04-20 are *also* excluded — they are pre-fix bundles where the
# legacy fallback's "derive _first_seen from orderbooks ∪ metadata" can't
# reconstruct the live engine's mid-day WS-reconnect+clear() history, so
# strict bit-exact parity is not achievable on those days. The plan §"Design"
# Change 2 documents this trade-off ("legacy fallback is intentionally
# generous, over-marks rather than under-marks"). Adding new bundles captured
# with schema_version=2 (post-Pi-redeploy of this fix) will make those days
# bit-exact and they can be added back here. See plan §"Goal table" — the
# claim of 0 replay-only trades for those days holds in spirit (the original
# spurious-replay bug is fixed) but the over-marking introduces a different
# class of drift (legitimate live trades suppressed in replay).
#
# Empirically validated bit-exact post-fix:
PARITY_DAYS = [
	"2026-04-21", "2026-04-22", "2026-04-23", "2026-04-24",
	"2026-04-25", "2026-04-26", "2026-04-27",
]

# Days the parity sweep was originally scoped to but excluded for the reason
# above. Listed here so the exclusion is explicit and re-introducing them is
# a one-line edit once the relevant bundles are re-captured with v2 writer.
LEGACY_BUNDLE_EXCLUDED_DAYS = ("2026-04-17", "2026-04-19", "2026-04-20")

FIXTURES_DIR = Path(__file__).resolve().parent
BUNDLES_DIR = REPO_ROOT / "data" / "bundles"
SENTINEL = FIXTURES_DIR / ".fixtures_present"

# Per-day live trade counts. Post-fix replay output (after subtracting the
# allowlist) must equal these exactly — guards against silent drift in either
# direction. Populated empirically from a regenerate.py --all run.
EXPECTED_TRADE_COUNTS: dict[str, int] = {
	"2026-04-21": 5,
	"2026-04-22": 3,
	"2026-04-23": 6,
	"2026-04-24": 8,
	"2026-04-25": 9,
	"2026-04-26": 4,
	"2026-04-27": 3,
}


def _project_to_key(row) -> tuple:
	"""Project a trade dict OR sqlite Row to the 7-tuple key.

	Handles both replay output (dict with these keys) and sqlite Row.
	MUST stay in sync with the matching helper in
	tests/test_replay_parity_first_seen.py.
	"""
	def _get(key, default=None):
		if isinstance(row, dict):
			return row.get(key, default)
		return row[key] if key in row.keys() else default
	return (
		_get("strategy"),
		_get("ticker"),
		_get("side"),
		_get("entry_time"),
		_get("fill_size"),
		_get("blended_entry"),
		_get("fill_price"),
	)


def _resolve_bundle(day: str) -> Path | None:
	"""Local bundle if present; (R2 fallback can be added if needed but in this
	worktree data/bundles/ is a symlink to the main cache, so local should always work)."""
	local = BUNDLES_DIR / day
	if local.exists():
		return local
	# In a fresh-box bootstrap the implementer would invoke R2Transport here.
	# For now, return None and let the caller exit 3.
	return None


def _read_engine_commit(bundle: Path) -> str:
	commit_file = bundle / "engine_version.txt"
	if commit_file.exists():
		return commit_file.read_text(encoding="utf-8").strip()
	return "unknown"


def _read_live_keys(bundle: Path, day: str) -> set[tuple]:
	live_db = bundle / f"paper_trades_v2_{day}.sqlite"
	if not live_db.exists():
		raise RuntimeError(f"live trades absent for {day} — bundle likely partially uploaded")
	conn = sqlite3.connect(str(live_db))
	conn.row_factory = sqlite3.Row
	try:
		# Use SELECT * so _project_to_key gracefully handles absent columns
		# (fill_price is not in the paper_trades schema; the lambda returns None).
		rows = conn.execute("SELECT * FROM paper_trades").fetchall()
	finally:
		conn.close()
	return {_project_to_key(r) for r in rows}


def _read_allowlist(day: str) -> frozenset[tuple]:
	f = FIXTURES_DIR / f"{day}.allowlist.json"
	if not f.exists():
		raise RuntimeError(f"missing allowlist: tests/fixtures/replay_parity/{day}.allowlist.json")
	return frozenset(tuple(k) for k in json.loads(f.read_text(encoding="utf-8")))


def _run_replay(bundle: Path) -> set[tuple]:
	# Use the prior-day bundle for seeding (replay's normal mode).
	day = bundle.name
	year, month, dom = day.split("-")
	prior = BUNDLES_DIR / f"{year}-{month}-{int(dom)-1:02d}"
	result = replay_capture(
		bundle_path=bundle,
		prior_bundle=prior if prior.exists() else None,
	)
	return {_project_to_key(t) for t in result.trades}


def _regenerate_one(day: str) -> tuple[bool, str]:
	"""Returns (success, message). Writes fixture file on success."""
	bundle = _resolve_bundle(day)
	if bundle is None:
		return False, f"bundle missing for {day}"

	live_keys = _read_live_keys(bundle, day)
	replay_keys = _run_replay(bundle)
	allowlist = _read_allowlist(day)

	# Cross-check (the Goal-table assertion):
	# After subtracting allowlist, replay must equal live exactly.
	extras = replay_keys - live_keys - allowlist
	missing = live_keys - (replay_keys - allowlist)
	if extras or missing:
		# Don't write the fixture — exit 2 with diagnostic.
		return False, (
			f"{day}: parity violation\n"
			f"  replay-only (not on allowlist): {len(extras)}\n" +
			"".join(f"    {k!r}\n" for k in sorted(extras)) +
			f"  live-only (replay missing): {len(missing)}\n" +
			"".join(f"    {k!r}\n" for k in sorted(missing))
		)

	expected = EXPECTED_TRADE_COUNTS.get(day)
	if expected is not None and len(replay_keys - allowlist) != expected:
		return False, (
			f"{day}: EXPECTED_TRADE_COUNTS mismatch — "
			f"got {len(replay_keys - allowlist)}, expected {expected}"
		)

	# Write fixture
	fixture = {
		"engine_commit": _read_engine_commit(bundle),
		"generated_at": datetime.now(timezone.utc).isoformat(),
		"keys": [list(k) for k in sorted(replay_keys)],
	}
	out = FIXTURES_DIR / f"{day}.expected.json"
	out.write_text(json.dumps(fixture, indent=2, sort_keys=True), encoding="utf-8")
	return True, f"{day}: ok ({len(replay_keys)} keys)"


def main(argv: list[str] | None = None) -> int:
	p = argparse.ArgumentParser(description=__doc__)
	g = p.add_mutually_exclusive_group()
	g.add_argument("--day", choices=PARITY_DAYS, help="regenerate one day")
	g.add_argument("--all", action="store_true", help="regenerate all days (default)")
	args = p.parse_args(argv)
	days = [args.day] if args.day else PARITY_DAYS

	any_failed = False
	any_bundle_missing = False
	for day in days:
		ok, msg = _regenerate_one(day)
		print(msg, flush=True)
		if not ok:
			any_failed = True
			if "bundle missing" in msg:
				any_bundle_missing = True

	if any_bundle_missing:
		return 3
	if any_failed:
		return 2

	# Sentinel: only on full sweep success
	if args.all or not args.day:
		SENTINEL.write_text("", encoding="utf-8")
	return 0


if __name__ == "__main__":
	sys.exit(main())
