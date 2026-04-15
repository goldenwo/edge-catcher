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

This module ALSO owns bundle lifecycle helpers (``delete_raw_jsonl`` and
``prune_old_bundles``) used by the rotation callback to keep Pi disk usage
bounded. Without retention, raw JSONL accumulates at ~1.5 GB/day and fills
the disk in ~3 months. With retention (raw deleted after compression +
uploaded bundles pruned after N days) the steady state is ~420 MB.
"""
from __future__ import annotations

import json
import logging
import shutil
import sqlite3
import subprocess
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import zstandard as zstd

from edge_catcher.monitors.market_state import MarketState

log = logging.getLogger(__name__)

# Sentinel filename written inside a bundle dir after transport.upload_bundle
# returns successfully. prune_old_bundles refuses to delete a bundle that
# doesn't have this sentinel so silent upload failures can't cause data loss.
UPLOADED_SENTINEL = ".uploaded"


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


def _write_strategy_state_snapshot(db_path: Path, dst: Path) -> None:
	"""Snapshot the live DB's strategy_state table to a JSON envelope.

	Format: {"schema_version": 1, "captured_at": <iso>, "states": {strategy: {key: value}}}
	where each inner value is the json.loads'd Python object from the DB's
	value column. Byte-stable output via sort_keys=True on the states dict.

	Fails soft on missing table or per-row malformed JSON (logs a warning
	and continues) — strategy_state is a replay-fidelity enhancement, not
	a hard bundle requirement. See spec §7.1, §7.2.
	"""
	states: dict[str, dict[str, object]] = {}
	try:
		conn = sqlite3.connect(str(db_path))
		try:
			rows = conn.execute(
				"SELECT strategy, key, value FROM strategy_state"
			).fetchall()
		finally:
			conn.close()
	except sqlite3.OperationalError as e:
		log.warning(
			"_write_strategy_state_snapshot: could not read strategy_state table from %s: %s",
			db_path, e,
		)
		rows = []

	for strategy, key, value in rows:
		try:
			parsed = json.loads(value)
		except json.JSONDecodeError:
			log.warning(
				"_write_strategy_state_snapshot: skipping malformed value for %s.%s",
				strategy, key,
			)
			continue
		states.setdefault(strategy, {})[key] = parsed

	envelope = {
		"schema_version": 1,
		"captured_at": datetime.now(timezone.utc).isoformat(),
		"states": states,
	}
	dst.write_text(
		json.dumps(envelope, sort_keys=True, indent=2),
		encoding="utf-8",
	)


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


# ---------------------------------------------------------------------------
# Retention helpers — used by the rotation callback to keep Pi disk bounded
# ---------------------------------------------------------------------------


def mark_bundle_uploaded(bundle_dir: Path) -> None:
	"""Write the ``.uploaded`` sentinel file into ``bundle_dir``.

	Called by the rotation callback after ``transport.upload_bundle``
	returns successfully. ``prune_old_bundles`` uses this sentinel to
	distinguish safely-uploaded bundles (deletable) from stuck/failed
	uploads (must preserve for manual recovery).
	"""
	(bundle_dir / UPLOADED_SENTINEL).touch()


def delete_raw_jsonl(capture_dir: Path, capture_date: date) -> bool:
	"""Delete the raw ``kalshi_engine_<date>.jsonl`` file after its
	compressed copy has been placed in the bundle directory.

	Safe to call multiple times — missing file is a no-op. Returns True
	if a file was actually deleted, False otherwise.

	CRITICAL: this must only be called AFTER ``assemble_daily_bundle``
	has successfully produced the compressed ``<date>/kalshi_engine_<date>.jsonl.zst``
	inside the bundle dir. Deleting the raw JSONL without a verified
	compressed copy would lose the day's capture.
	"""
	raw = capture_dir / f"kalshi_engine_{capture_date.isoformat()}.jsonl"
	if not raw.exists():
		return False
	# Sanity check: confirm the compressed version exists before deleting.
	bundle_dir = capture_dir / capture_date.isoformat()
	compressed = bundle_dir / f"kalshi_engine_{capture_date.isoformat()}.jsonl.zst"
	if not compressed.exists():
		log.warning(
			"delete_raw_jsonl: refusing to delete %s — compressed copy %s is missing",
			raw, compressed,
		)
		return False
	try:
		raw.unlink()
	except OSError as e:
		log.warning("delete_raw_jsonl: could not delete %s: %s", raw, e)
		return False
	log.info("delete_raw_jsonl: deleted raw JSONL for %s (compressed copy is authoritative)", capture_date)
	return True


def prune_old_bundles(
	capture_dir: Path,
	retention_days: int,
	*,
	require_uploaded: bool = True,
	today: Optional[date] = None,
) -> list[date]:
	"""Delete bundle directories older than ``retention_days``.

	Args:
		capture_dir:      Root directory containing date-named bundle subdirs.
		retention_days:   How many days of bundles to keep. ``0`` disables pruning.
		require_uploaded: If True (default), only prune bundles that have the
		                  ``.uploaded`` sentinel file. Bundles without it are
		                  skipped and a warning is logged — they may be stuck
		                  mid-upload or awaiting manual retry.
		today:            Testing hook — defaults to ``date.today()``.

	Returns:
		List of dates whose bundle directories were actually deleted. Empty
		list if nothing needed pruning.
	"""
	if retention_days <= 0:
		return []
	if today is None:
		today = date.today()
	cutoff = today - timedelta(days=retention_days)

	deleted: list[date] = []
	for child in sorted(capture_dir.iterdir()):
		if not child.is_dir():
			continue
		# Only consider directories whose name is an ISO date — this skips
		# stray files, the active capture dir, etc.
		try:
			bundle_date = date.fromisoformat(child.name)
		except ValueError:
			continue
		if bundle_date >= cutoff:
			continue
		if require_uploaded and not (child / UPLOADED_SENTINEL).exists():
			log.warning(
				"prune_old_bundles: bundle %s is %d days old but has no %s sentinel — "
				"skipping prune (upload failed or is in progress; fix or delete manually)",
				child.name, (today - bundle_date).days, UPLOADED_SENTINEL,
			)
			continue
		try:
			shutil.rmtree(child)
			deleted.append(bundle_date)
			log.info(
				"prune_old_bundles: removed %s (uploaded, older than %d days)",
				child.name, retention_days,
			)
		except OSError as e:
			log.warning("prune_old_bundles: could not delete %s: %s", child, e)
	return deleted
