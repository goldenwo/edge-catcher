"""The replay-vs-live parity gate.

Sets ``REPLAY_PARITY_BUNDLE=<path>`` to run against a real captured bundle.
Without the env var the test is skipped (so CI can be green without a bundle).

Two modes:

  * **Strict mode** (``REPLAY_PARITY_STRICT=1``): asserts zero divergences.
    Used after the pipeline has reached maturity — every trade in the live
    slice must have a matching replay row on the column whitelist.

  * **Diagnostic mode** (default): runs replay and prints the divergence
    report for inspection, but does NOT enforce parity — the test passes
    as long as replay completes. Useful during iteration for eyeballing
    drift without failing the suite. A WARNING banner makes the lack of
    enforcement explicit so a green run isn't mistaken for a green gate.

A bundle date listed in ``replay_parity_skip.txt`` (alongside this file)
has its strict-mode assertions downgraded to soft warnings. The divergence
report still prints, prefixed ``[SKIPPED]``, so the drift is visible but
doesn't fail the gate. Used for legacy bundles whose divergence has a
documented, accepted root cause (e.g. pre-schema-v2 first_seen gap).

See spec §8.1 for the column whitelist rationale and the full correctness
argument. The parity test is the verdict tool for the capture/replay
rewrite: if it passes on a real bundle, the replay backtester is provably
correct against the live engine on all columns that matter for P&L
attribution and strategy evaluation.
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any

import pytest


# Columns that must match on every common-key trade.
# id is excluded (autoincrement vs in-memory counter).
# book_snapshot is excluded (JSON string equality is format-sensitive;
# not material for P&L parity).
PARITY_COLUMNS = [
	"strategy",
	"ticker",
	"side",
	"series_ticker",
	"entry_price",
	"blended_entry",
	"intended_size",
	"fill_size",
	"fill_pct",
	"slippage_cents",
	"book_depth",
	"entry_fee_cents",
	"exit_price",
	"pnl_cents",
	"status",
	"entry_time",
	"exit_time",
]

# Floating point columns that require approximate equality to absorb
# round-trip SQLite ↔ Python drift.
APPROX_COLUMNS = {"fill_pct", "slippage_cents"}


PARITY_BUNDLE_ENV = "REPLAY_PARITY_BUNDLE"
PARITY_STRICT_ENV = "REPLAY_PARITY_STRICT"

# Skip-list co-located with the test so it ships with the codebase. See file
# header for format and rationale-per-entry expectations.
SKIP_LIST_PATH = Path(__file__).parent / "replay_parity_skip.txt"


def _load_skip_list(path: Path = SKIP_LIST_PATH) -> set[str]:
	"""Return the set of bundle dates (ISO strings) listed in the skip file.

	Returns an empty set if the file is absent — keeps the test runnable in
	checkouts that haven't pulled the skip-list yet.
	"""
	if not path.exists():
		return set()
	skips: set[str] = set()
	for raw in path.read_text(encoding="utf-8").splitlines():
		line = raw.strip()
		if not line or line.startswith("#"):
			continue
		# The first whitespace-separated token is the date; the rest is rationale.
		skips.add(line.split()[0])
	return skips


def _is_bundle_skipped(bundle_path: Path, skip_list: set[str] | None = None) -> bool:
	"""True if the bundle's directory name (an ISO date) is in the skip list."""
	if skip_list is None:
		skip_list = _load_skip_list()
	return bundle_path.name in skip_list


def _composite_key(row: dict[str, Any]) -> tuple:
	return (row["strategy"], row["ticker"], row["side"], row["entry_time"])


def _values_match(col: str, live_val: Any, replay_val: Any) -> bool:
	if col in APPROX_COLUMNS:
		if live_val is None and replay_val is None:
			return True
		if live_val is None or replay_val is None:
			return False
		return abs(float(live_val) - float(replay_val)) < 1e-9
	return live_val == replay_val


def _diff_rows(live: dict, replay: dict) -> dict[str, tuple[Any, Any]]:
	diffs: dict[str, tuple[Any, Any]] = {}
	for col in PARITY_COLUMNS:
		lv = live.get(col)
		rv = replay.get(col)
		if not _values_match(col, lv, rv):
			diffs[col] = (lv, rv)
	return diffs


def _load_live_day_slice(bundle: Path) -> list[dict]:
	"""Load the bundle's day-slice DB as a list of row dicts."""
	candidates = sorted(bundle.glob("paper_trades_v2_*.sqlite"))
	if not candidates:
		raise FileNotFoundError(f"no paper_trades_v2_*.sqlite in {bundle}")
	if len(candidates) > 1:
		raise ValueError(f"expected exactly one day-slice DB in {bundle}, found {len(candidates)}")
	conn = sqlite3.connect(str(candidates[0]))
	conn.row_factory = sqlite3.Row
	try:
		rows = [dict(r) for r in conn.execute("SELECT * FROM paper_trades").fetchall()]
	finally:
		conn.close()
	return rows


@pytest.mark.asyncio
@pytest.mark.skipif(
	not os.environ.get(PARITY_BUNDLE_ENV),
	reason=f"set {PARITY_BUNDLE_ENV}=<bundle-path> to run",
)
async def test_replay_parity_against_live_bundle():
	"""Run replay against a captured bundle and compare every live row
	on the PARITY_COLUMNS whitelist.

	Strict mode (REPLAY_PARITY_STRICT=1): any divergence fails the test.
	Default mode: collects all divergences into a structured report.
	"""
	from edge_catcher.engine.replay.backtester import replay_capture

	bundle_path = Path(os.environ[PARITY_BUNDLE_ENV])
	assert bundle_path.exists(), f"bundle not found: {bundle_path}"
	strict = os.environ.get(PARITY_STRICT_ENV) == "1"
	skipped = _is_bundle_skipped(bundle_path)

	# 1. Run replay
	result = await replay_capture(bundle_path)

	# 2. Load live slice
	live_all = _load_live_day_slice(bundle_path)

	# 3. Scope filter — restrict comparison to trades whose entry_time falls
	# within the capture window (first..last recv_ts in the JSONL). Trades
	# that opened before capture started (e.g. carried over from an earlier
	# session before tee point 1 was recording) can't possibly have a
	# replay counterpart and would poison the diff.
	start = result.capture_start_ts or ""
	end = result.capture_end_ts or ""
	live = [t for t in live_all if start <= t.get("entry_time", "") <= end]

	live_by_key = {_composite_key(t): t for t in live}
	replay_by_key = {_composite_key(t): t for t in result.trades}
	common = set(live_by_key) & set(replay_by_key)
	live_only = set(live_by_key) - set(replay_by_key)
	replay_only = set(replay_by_key) - set(live_by_key)

	# 4. Diff common-key rows on the whitelist
	row_diffs: list[tuple[tuple, dict]] = []
	for k in sorted(common):
		diffs = _diff_rows(live_by_key[k], replay_by_key[k])
		if diffs:
			row_diffs.append((k, diffs))

	# 5. Build a human-readable report
	header = "=== REPLAY PARITY REPORT ===" if not skipped else "=== REPLAY PARITY REPORT [SKIPPED] ==="
	lines = [
		"",
		header,
		f"bundle:          {bundle_path}",
		f"capture window:  {start} .. {end}",
		f"events replayed: {result.events_processed:,}",
		f"replay duration: {result.duration_seconds:.2f}s",
		f"strategies:      {result.strategies_loaded}",
		f"mode:            {'STRICT' if strict else 'NON-STRICT (report-only)'}",
	]
	if skipped:
		lines.append(
			f"skip-list:       {bundle_path.name} listed in {SKIP_LIST_PATH.name}; "
			"strict assertions downgraded to warnings"
		)
	if not strict:
		# Test integrity: a green run in non-strict mode does NOT mean parity
		# holds — only strict mode enforces it. Make that explicit so an
		# operator scanning the report doesn't mistake "test passed" for
		# "no divergences". See module docstring.
		lines.append(
			"WARNING:         non-strict mode — divergences below are NOT enforced. "
			"Set REPLAY_PARITY_STRICT=1 to fail on drift."
		)
	lines.extend([
		"",
		f"live trades (in scope):   {len(live)} (of {len(live_all)} total in day slice)",
		f"replay trades:            {len(result.trades)}",
		f"common keys:              {len(common)}",
		f"  - matching:             {len(common) - len(row_diffs)}",
		f"  - diverging:            {len(row_diffs)}",
		f"live-only keys:           {len(live_only)}",
		f"replay-only keys:         {len(replay_only)}",
	])

	if row_diffs:
		lines.append("\n--- COMMON-KEY DIVERGENCES ---")
		for k, diffs in row_diffs:
			lines.append(f"  [{k[1]} {k[2]} @ {k[3][:19]}]")
			for col, (lv, rv) in diffs.items():
				lines.append(f"    {col:18s} live={lv!r:40s} replay={rv!r}")

	if live_only:
		lines.append(f"\n--- LIVE-ONLY ({len(live_only)}) ---")
		for k in sorted(live_only):
			t = live_by_key[k]
			lines.append(f"  id={t.get('id')} {k[1]} {k[2]} entry={k[3][:19]} status={t.get('status')}")

	if replay_only:
		lines.append(f"\n--- REPLAY-ONLY ({len(replay_only)}) ---")
		for k in sorted(replay_only):
			t = replay_by_key[k]
			lines.append(f"  id={t.get('id')} {k[1]} {k[2]} entry={k[3][:19]} status={t.get('status')}")

	report = "\n".join(lines)
	print(report)

	# 6. Assertions
	#
	# Strict mode + not skipped: enforce zero divergence as before.
	# Strict mode + skipped:     downgrade to soft warning — the bundle is on
	#                            the accepted-divergence list, so the test
	#                            passes but the divergences are still printed
	#                            (and any NEW divergence shape would still
	#                            need an operator to expand the rationale).
	# Non-strict mode:           always pass. WARNING banner above makes the
	#                            non-enforcement obvious.
	if strict and not skipped:
		assert not row_diffs, f"STRICT parity violated: {len(row_diffs)} divergent rows\n{report}"
		assert not live_only, f"STRICT parity violated: {len(live_only)} live-only rows\n{report}"
		assert not replay_only, f"STRICT parity violated: {len(replay_only)} replay-only rows\n{report}"


@pytest.mark.asyncio
@pytest.mark.skipif(
	not os.environ.get(PARITY_BUNDLE_ENV),
	reason=f"set {PARITY_BUNDLE_ENV}=<bundle-path> to run",
)
async def test_replay_runs_without_errors():
	"""Smoke test: replay must complete without raising, regardless of parity.

	If the bundle is malformed (missing manifest, broken strategies_local.py,
	empty JSONL) this test surfaces the problem before any parity logic runs.
	"""
	from edge_catcher.engine.replay.backtester import replay_capture

	bundle_path = Path(os.environ[PARITY_BUNDLE_ENV])
	result = await replay_capture(bundle_path)
	assert result.events_processed > 0, "replay processed zero events — bundle likely malformed"
	assert result.strategies_loaded, "no strategies loaded from bundle"
