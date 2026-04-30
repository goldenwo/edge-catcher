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

# Days covered by the parity sweep (4-18 excluded per Non-goals)
PARITY_DAYS = [
	"2026-04-17", "2026-04-19", "2026-04-20", "2026-04-21",
	"2026-04-22", "2026-04-23", "2026-04-24", "2026-04-25",
	"2026-04-26", "2026-04-27",
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


# TODO(replay-first-seen-fix): un-skip after 1.d.ii regen
@pytest.mark.requires_bundles
@pytest.mark.parametrize("day", PARITY_DAYS)
@pytest.mark.skip(reason="awaiting Step 3 + Step 1.d.ii fixture generation")
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

	# Build live keys
	conn = sqlite3.connect(str(live_db))
	conn.row_factory = sqlite3.Row
	try:
		live_rows = conn.execute(
			"SELECT strategy, ticker, side, entry_time, fill_size, blended_entry, fill_price "
			"FROM paper_trades"
		).fetchall()
	finally:
		conn.close()
	live_keys = frozenset(
		(r["strategy"], r["ticker"], r["side"], r["entry_time"], r["fill_size"], r["blended_entry"], r["fill_price"])
		for r in live_rows
	)

	# Replay
	from edge_catcher.monitors.replay.backtester import replay_capture
	result = replay_capture(bundle)
	replay_keys = frozenset(
		(t["strategy"], t["ticker"], t["side"], t["entry_time"], t["fill_size"], t["blended_entry"], t["fill_price"])
		for t in result.trades
	)

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
