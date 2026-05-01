"""Strict-parity regression: replay_capture trade keys must match live
paper-trader trade keys, modulo a small allowlist for known artifacts.

Skip-marked in Step 1.d.i; activated post-Step 1.d.ii fixture regen.

Per docs/superpowers/plans/replay-first-seen-fix.md §"Step 1 — write tests"
(1.d.i).
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

# Days covered by the strict-parity sweep.
#
# Excluded days:
#   04-18                 — engine-version drift (plan §"Non-goals").
#   04-17, 04-19, 04-20   — pre-fix bundles where the legacy fallback
#                           ("derive _first_seen from orderbooks ∪ metadata")
#                           can't reconstruct the live engine's mid-day
#                           WS-reconnect+clear() history. Plan §"Design"
#                           Change 2 documents this trade-off ("legacy
#                           fallback is intentionally generous, over-marks
#                           rather than under-marks"). Bit-exact parity on
#                           those days is unachievable without re-capturing
#                           the bundle with the schema_version=2 writer.
#                           One-line edit to re-introduce them once Pi
#                           backfills the v2 bundles.
#
# Mirror this list in tests/fixtures/replay_parity/regenerate.py — they MUST
# stay in sync (the regenerate script writes the fixtures the test reads).
PARITY_DAYS = [
	"2026-04-21", "2026-04-22", "2026-04-23", "2026-04-24",
	"2026-04-25", "2026-04-26", "2026-04-27",
]

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "replay_parity"
SENTINEL = FIXTURES_DIR / ".fixtures_present"

CANONICAL_SKIP_REASON = "awaiting Step 3 + Step 1.d.ii fixture generation"


def _resolve_bundle(day: str) -> Path | None:
	"""Local bundle dir if present; otherwise None (R2 fetch not in Step 1.d.i scope)."""
	# Try data/bundles/<day>/ (relative to repo root)
	# In 1.d.ii the regenerate.py downloads via R2 if missing.
	repo_root = Path(__file__).resolve().parent.parent
	local = repo_root / "data" / "bundles" / day
	return local if local.exists() else None


def _check_sentinel_consistency() -> None:
	"""If fixtures are present (sentinel exists) but skip markers remain,
	someone forgot Step 1.d.ii's un-skip step. Fail loud."""
	if SENTINEL.exists():
		this_file = Path(__file__).read_text(encoding="utf-8")
		if f'@pytest.mark.skip(reason="{CANONICAL_SKIP_REASON}")' in this_file:
			pytest.fail(
				"fixtures regenerated but skip markers not removed — see Step 1.d.ii"
			)


@pytest.mark.requires_bundles
@pytest.mark.parametrize("day", PARITY_DAYS)
def test_parity(day: str) -> None:
	_check_sentinel_consistency()  # safety net for 1.d.ii oversight

	bundle = _resolve_bundle(day)
	if bundle is None:
		pytest.skip(f"bundle {day} not available locally — run regenerate.py to fetch from R2")

	# Gate 2: engine_commit freshness
	fixture_file = FIXTURES_DIR / f"{day}.expected.json"
	if not fixture_file.exists():
		pytest.fail(f"missing fixture: {fixture_file} — run regenerate.py --day {day}")
	fixture = json.loads(fixture_file.read_text(encoding="utf-8"))
	bundle_commit_file = bundle / "engine_version.txt"
	bundle_commit = bundle_commit_file.read_text(encoding="utf-8").strip() if bundle_commit_file.exists() else "unknown"
	if fixture["engine_commit"] != bundle_commit:
		pytest.fail(
			f"engine_commit mismatch (fixture={fixture['engine_commit']}, bundle={bundle_commit}): "
			f"regenerate fixtures: python tests/fixtures/replay_parity/regenerate.py --day {day}"
		)

	# Gate 3: allowlist must exist (do NOT default to frozenset on missing — masks typos)
	allowlist_file = FIXTURES_DIR / f"{day}.allowlist.json"
	if not allowlist_file.exists():
		pytest.fail(f"missing allowlist: tests/fixtures/replay_parity/{day}.allowlist.json (use [] for empty)")
	allowlist = frozenset(tuple(k) for k in json.loads(allowlist_file.read_text(encoding="utf-8")))

	# Live trades: paper_trades_v2_<day>.sqlite from the bundle
	live_db = bundle / f"paper_trades_v2_{day}.sqlite"
	if not live_db.exists():
		pytest.skip(f"live trades absent for {day} — bundle likely partially uploaded")

	# Build keys via a shared projection that handles both sqlite Row and
	# replay's dict-shape rows. The 7-tuple uses (strategy, ticker, side,
	# entry_time, fill_size, blended_entry, fill_price) per the plan; note
	# that fill_price is NOT a column in the paper_trades schema today —
	# both sides project to None for that slot, which is harmless and keeps
	# the tuple arity stable for any future column addition. MUST stay in
	# sync with regenerate.py::_project_to_key.
	def _project(row) -> tuple:
		def _get(key, default=None):
			# sqlite3.Row supports `key in row.keys()`; dict supports it too.
			if isinstance(row, dict):
				return row.get(key, default)
			return row[key] if key in row.keys() else default
		return (
			_get("strategy"), _get("ticker"), _get("side"), _get("entry_time"),
			_get("fill_size"), _get("blended_entry"), _get("fill_price"),
		)

	# Live keys
	conn = sqlite3.connect(str(live_db))
	conn.row_factory = sqlite3.Row
	try:
		live_rows = conn.execute("SELECT * FROM paper_trades").fetchall()
	finally:
		conn.close()
	live_keys = frozenset(_project(r) for r in live_rows)

	# Replay
	from edge_catcher.monitors.replay.backtester import replay_capture
	result = replay_capture(bundle)
	replay_keys = frozenset(_project(t) for t in result.trades)

	# Gate 4: comparator
	diff = replay_keys - live_keys - allowlist
	assert diff == frozenset(), (
		f"parity violation on {day}: {len(diff)} replay-only trades not on allowlist:\n"
		+ "\n".join(repr(k) for k in sorted(diff))
	)
	# Symmetric: replay should not be missing live trades either
	assert sorted(replay_keys - allowlist) == sorted(live_keys), (
		f"replay/live key set mismatch on {day}"
	)
