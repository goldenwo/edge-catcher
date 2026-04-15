"""Daily bundle assembly for the orderbook capture pipeline.

Runs on the Pi at midnight UTC rotation (wired up in Task 13 via the
RawFrameWriter rotation_callback). Packages a day's capture artifacts into
a self-contained directory that the dev workstation can later replay
against — see capture/replay spec §4.3.

The resulting bundle must be complete enough that replay can:
  * Re-parse the JSONL of captured events
  * Load the exact strategies_local.py that was running live
  * Load the exact config that was running live
  * Seed MarketState from the snapshot taken at bundle-assembly time
  * Seed its InMemoryTradeStore with the open trades that existed at
    end-of-day so the next day's replay picks up carry-over positions
  * Compare its resulting trade rows against the day's live paper_trades slice
"""
from __future__ import annotations

import json
import logging
import shutil
import sqlite3
import subprocess
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

import zstandard as zstd

from edge_catcher.monitors.market_state import MarketState

log = logging.getLogger(__name__)


def assemble_daily_bundle(
	capture_date: date,
	capture_dir: Path,
	repo_root: Path,
	db_path: Path,
	market_state: Optional[MarketState] = None,
) -> Path:
	"""Build the bundle directory for ``capture_date``.

	Args:
		capture_date:  The UTC date whose events should be bundled.
		capture_dir:   Directory containing the raw ``kalshi_engine_<date>.jsonl`` file.
		repo_root:     Root of the edge-catcher checkout (for strategies + config).
		db_path:       Path to the live ``paper_trades_v2.db`` (read-only access).
		market_state:  Current MarketState. If None, the snapshot is omitted —
		               useful for catch-up runs where the live state isn't available.

	Returns:
		The path of the created bundle directory (``<capture_dir>/<capture_date>/``).
	"""
	bundle_dir = capture_dir / capture_date.isoformat()
	bundle_dir.mkdir(parents=True, exist_ok=True)
	day_str = capture_date.isoformat()

	# 1. Compress the JSONL (zstd level 19 — takes a few seconds at rotation
	#    but shrinks the overnight capture to ~10-20% of original).
	src_jsonl = capture_dir / f"kalshi_engine_{day_str}.jsonl"
	dst_jsonl = bundle_dir / f"kalshi_engine_{day_str}.jsonl.zst"
	_compress_zstd(src_jsonl, dst_jsonl)

	# 2. Copy strategies_local.py from the repo checkout.
	strategies_src = repo_root / "edge_catcher" / "monitors" / "strategies_local.py"
	if strategies_src.exists():
		shutil.copy2(strategies_src, bundle_dir / "strategies_local.py")
	else:
		log.warning(
			"assemble_daily_bundle: strategies_local.py not found at %s — bundle "
			"will be incomplete and replay will need to supply its own strategies",
			strategies_src,
		)

	# 3. Copy paper-trader.yaml from config.local.
	config_src = repo_root / "config.local" / "paper-trader.yaml"
	if config_src.exists():
		shutil.copy2(config_src, bundle_dir / "paper-trader.yaml")
	else:
		log.warning("assemble_daily_bundle: paper-trader.yaml not found at %s", config_src)

	# 4. engine_version.txt — git commit + dirty flag so replay can report
	#    exactly which engine the captured events ran against.
	commit, dirty = _git_state(repo_root)
	(bundle_dir / "engine_version.txt").write_text(
		f"commit: {commit}\ndirty: {dirty}\n", encoding="utf-8"
	)

	# 5. market_state_at_start.json — snapshot of current MarketState. Seeds
	#    NEXT day's replay (the bundle represents "end of day N" == "start of day N+1").
	if market_state is not None:
		_write_market_state_snapshot(bundle_dir / "market_state_at_start.json", market_state)

	# 6. open_trades_at_start.sqlite — only rows with status='open' at the
	#    moment of rotation. Next day's replay seeds its InMemoryTradeStore
	#    from this so carry-over positions' composite keys are present.
	_write_open_trades_slice(db_path, bundle_dir / "open_trades_at_start.sqlite")

	# 7. paper_trades_v2_<date>.sqlite — the full day's slice of the live DB.
	#    This is the ground truth that the parity test compares replay output
	#    against. Only includes rows whose entry_time is within the UTC day.
	_write_day_slice(db_path, bundle_dir / f"paper_trades_v2_{day_str}.sqlite", capture_date)

	# 8. manifest.json — metadata + file list. Written LAST so _write_manifest
	#    can enumerate all siblings.
	_write_manifest(bundle_dir, capture_date, commit, dirty)

	log.info("assemble_daily_bundle: bundle %s assembled", bundle_dir)
	return bundle_dir


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _compress_zstd(src: Path, dst: Path, level: int = 19) -> None:
	"""Stream-compress ``src`` to ``dst`` using zstd level 19 (slow compress,
	fast decompress, ~10% of original size for JSONL text)."""
	cctx = zstd.ZstdCompressor(level=level)
	with open(src, "rb") as fin, open(dst, "wb") as fout:
		cctx.copy_stream(fin, fout)


def _git_state(repo_root: Path) -> tuple[str, bool]:
	"""Return (commit_sha, dirty_flag). Returns ('unknown', False) if git is
	unavailable or the directory isn't a git repo — bundles can still be
	assembled off-repo."""
	try:
		commit = subprocess.check_output(
			["git", "rev-parse", "HEAD"],
			cwd=str(repo_root),
			text=True,
			stderr=subprocess.DEVNULL,
		).strip()
		porcelain = subprocess.check_output(
			["git", "status", "--porcelain"],
			cwd=str(repo_root),
			text=True,
			stderr=subprocess.DEVNULL,
		).strip()
		dirty = porcelain != ""
		return commit, dirty
	except (subprocess.CalledProcessError, FileNotFoundError):
		return "unknown", False


def _write_market_state_snapshot(path: Path, market_state: MarketState) -> None:
	"""Serialize orderbooks and ticker metadata to a JSON file that the
	replay's ``_seed_market_state`` can read back.

	Iterates ``_orderbooks`` and ``_metadata`` directly so we capture both
	seeded books and registered metadata even when they don't overlap (e.g.
	a ticker seeded from a REST snapshot before its metadata fetch lands).
	``all_tickers()`` only reflects registered tickers, not seeded books,
	which would drop snapshots from the bundle.
	"""
	orderbooks: dict[str, dict] = {}
	for ticker, ob in market_state._orderbooks.items():  # noqa: SLF001
		orderbooks[ticker] = {
			"yes_levels": [list(level) for level in ob.yes_levels],
			"no_levels": [list(level) for level in ob.no_levels],
		}
	metadata: dict[str, dict] = {}
	for ticker, meta in market_state._metadata.items():  # noqa: SLF001
		if meta:
			metadata[ticker] = meta
	state = {
		"captured_at": datetime.now(timezone.utc).isoformat(),
		"orderbooks": orderbooks,
		"metadata": metadata,
	}
	path.write_text(json.dumps(state), encoding="utf-8")


def _write_open_trades_slice(db_path: Path, dst: Path) -> None:
	"""Copy the live DB's schema + only the rows where ``status='open'``.

	Uses ATTACH + CREATE TABLE AS SELECT so we don't need VACUUM INTO (the
	dev workstation may run an older SQLite that lacks it, per the plan's
	VACUUM INTO version-fallback note). The attach-based approach works on
	SQLite 3.x across the board."""
	if dst.exists():
		dst.unlink()
	src = sqlite3.connect(str(db_path))
	try:
		src.execute("ATTACH DATABASE ? AS bundle", (str(dst),))
		src.execute(
			"CREATE TABLE bundle.paper_trades AS "
			"SELECT * FROM main.paper_trades WHERE status='open'"
		)
		src.execute("DETACH DATABASE bundle")
		src.commit()
	finally:
		src.close()


def _write_day_slice(db_path: Path, dst: Path, day: date) -> None:
	"""Copy the live DB's schema + rows whose entry_time falls in ``day``'s UTC window.

	The day window is ``[day 00:00:00 UTC, day+1 00:00:00 UTC)``.
	"""
	if dst.exists():
		dst.unlink()
	day_iso = day.isoformat()
	next_day_iso = date.fromordinal(day.toordinal() + 1).isoformat()
	src = sqlite3.connect(str(db_path))
	try:
		src.execute("ATTACH DATABASE ? AS bundle", (str(dst),))
		src.execute(
			"CREATE TABLE bundle.paper_trades AS "
			"SELECT * FROM main.paper_trades "
			"WHERE entry_time >= ? AND entry_time < ?",
			(day_iso, next_day_iso),
		)
		src.execute("DETACH DATABASE bundle")
		src.commit()
	finally:
		src.close()


def _write_manifest(bundle_dir: Path, capture_date: date, commit: str, dirty: bool) -> None:
	files = sorted(p.name for p in bundle_dir.iterdir() if p.name != "manifest.json")
	manifest = {
		"schema_version": 1,
		"exchange": "kalshi",
		"capture_date": capture_date.isoformat(),
		"engine_commit": commit,
		"engine_dirty": dirty,
		"files": files,
		"stats": {
			"rotation_started_at": datetime.now(timezone.utc).isoformat(),
		},
	}
	(bundle_dir / "manifest.json").write_text(
		json.dumps(manifest, indent=2), encoding="utf-8"
	)
