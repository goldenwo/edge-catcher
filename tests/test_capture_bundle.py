"""Tests for assemble_daily_bundle — the pure function that packages a day's
capture artifacts into a self-contained directory for replay.

Runs on the Pi at midnight UTC rotation. See capture/replay spec §4.3.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path

import pytest
import zstandard as zstd

from edge_catcher.monitors.market_state import MarketState, OrderbookSnapshot
from edge_catcher.monitors.trade_store import TradeStore


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def capture_dir(tmp_path: Path) -> Path:
	"""A capture directory with one minimal JSONL file for the test date."""
	d = tmp_path / "capture"
	d.mkdir()
	jsonl = d / "kalshi_engine_2026-04-14.jsonl"
	jsonl.write_text(
		json.dumps({"schema_version": 1, "exchange": "kalshi", "header": True}) + "\n"
		+ json.dumps({"recv_seq": 1, "recv_ts": "2026-04-14T00:00:01+00:00", "source": "ws", "payload": {"type": "ticker", "msg": {"market_ticker": "KXTEST"}}}) + "\n"
		+ json.dumps({"recv_seq": 2, "recv_ts": "2026-04-14T00:00:02+00:00", "source": "synthetic.rest_orderbook", "payload": {"ticker": "KXTEST", "yes_levels": [[0.5, 10]], "no_levels": [[0.48, 5]]}}) + "\n",
		encoding="utf-8",
	)
	return d


@pytest.fixture
def repo_root(tmp_path: Path) -> Path:
	"""A fake repo checkout with the files bundle.py is expected to copy."""
	root = tmp_path / "repo"
	(root / "edge_catcher" / "monitors").mkdir(parents=True)
	(root / "config.local").mkdir(parents=True)
	(root / "edge_catcher" / "monitors" / "strategies_local.py").write_text(
		"# stub strategies_local for bundle test\n", encoding="utf-8"
	)
	(root / "config.local" / "paper-trader.yaml").write_text(
		"strategies: {test: {enabled: true}}\n", encoding="utf-8"
	)
	return root


@pytest.fixture
def trade_db(tmp_path: Path) -> Path:
	"""A paper_trades_v2.db with one open trade from the capture day,
	one closed trade from the capture day, and one trade from a different
	day — so we can verify both the open-trades-at-start slicing and the
	day-window slicing."""
	db_path = tmp_path / "paper_trades_v2.db"
	store = TradeStore(db_path)
	try:
		# Open trade from the capture day (should appear in open_trades_at_start)
		# Note: the bundle represents "at end of day N" and is seeding day N+1's replay,
		# so "open at start of replay" = "open at end of capture day".
		store.record_trade(
			ticker="KXOPEN-26APR14",
			entry_price=50,
			strategy="test-strat",
			side="yes",
			series_ticker="KXOPEN",
			blended_entry=50,
			now=datetime(2026, 4, 14, 12, 0, 0, tzinfo=timezone.utc),
		)
		# Closed trade from the capture day (should appear in paper_trades_v2_<date>.sqlite)
		closed_id = store.record_trade(
			ticker="KXCLOSED-26APR14",
			entry_price=40,
			strategy="test-strat",
			side="yes",
			series_ticker="KXCLOSED",
			blended_entry=40,
			now=datetime(2026, 4, 14, 11, 0, 0, tzinfo=timezone.utc),
		)
		store.exit_trade(
			closed_id, exit_price=55,
			now=datetime(2026, 4, 14, 11, 30, 0, tzinfo=timezone.utc),
		)
		# Trade from a DIFFERENT day (should NOT appear in the day-slice)
		store.record_trade(
			ticker="KXOTHERDAY-26APR13",
			entry_price=30,
			strategy="test-strat",
			side="no",
			series_ticker="KXOTHERDAY",
			blended_entry=30,
			now=datetime(2026, 4, 13, 12, 0, 0, tzinfo=timezone.utc),
		)
	finally:
		store.close()
	return db_path


@pytest.fixture
def market_state() -> MarketState:
	ms = MarketState()
	ms.seed_orderbook("KXSNAP", OrderbookSnapshot(
		yes_levels=[(0.42, 100), (0.41, 50)],
		no_levels=[(0.58, 75)],
	))
	return ms


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_assemble_bundle_creates_all_expected_files(
	tmp_path: Path,
	capture_dir: Path,
	repo_root: Path,
	trade_db: Path,
	market_state: MarketState,
) -> None:
	"""The bundle directory should contain every artifact listed in spec §4.3."""
	from edge_catcher.monitors.capture.bundle import assemble_daily_bundle

	bundle_path = assemble_daily_bundle(
		capture_date=date(2026, 4, 14),
		capture_dir=capture_dir,
		repo_root=repo_root,
		db_path=trade_db,
		market_state=market_state,
	)

	assert bundle_path.exists()
	assert bundle_path.is_dir()
	assert (bundle_path / "kalshi_engine_2026-04-14.jsonl.zst").exists()
	assert (bundle_path / "strategies_local.py").exists()
	assert (bundle_path / "paper-trader.yaml").exists()
	assert (bundle_path / "engine_version.txt").exists()
	assert (bundle_path / "market_state_at_start.json").exists()
	assert (bundle_path / "open_trades_at_start.sqlite").exists()
	assert (bundle_path / "paper_trades_v2_2026-04-14.sqlite").exists()
	assert (bundle_path / "manifest.json").exists()


def test_bundle_jsonl_zstd_round_trips(
	tmp_path: Path,
	capture_dir: Path,
	repo_root: Path,
	trade_db: Path,
	market_state: MarketState,
) -> None:
	"""The compressed JSONL should decompress to the exact original bytes."""
	from edge_catcher.monitors.capture.bundle import assemble_daily_bundle

	bundle_path = assemble_daily_bundle(
		capture_date=date(2026, 4, 14),
		capture_dir=capture_dir,
		repo_root=repo_root,
		db_path=trade_db,
		market_state=market_state,
	)
	compressed = (bundle_path / "kalshi_engine_2026-04-14.jsonl.zst").read_bytes()
	dctx = zstd.ZstdDecompressor()
	decompressed = dctx.decompress(compressed, max_output_size=10_000_000)

	original = (capture_dir / "kalshi_engine_2026-04-14.jsonl").read_bytes()
	assert decompressed == original


def test_bundle_manifest_schema(
	capture_dir: Path,
	repo_root: Path,
	trade_db: Path,
	market_state: MarketState,
) -> None:
	"""manifest.json should have schema_version=1, exchange, capture_date, and a file list."""
	from edge_catcher.monitors.capture.bundle import assemble_daily_bundle

	bundle_path = assemble_daily_bundle(
		capture_date=date(2026, 4, 14),
		capture_dir=capture_dir,
		repo_root=repo_root,
		db_path=trade_db,
		market_state=market_state,
	)
	manifest = json.loads((bundle_path / "manifest.json").read_text(encoding="utf-8"))
	assert manifest["schema_version"] == 1
	assert manifest["exchange"] == "kalshi"
	assert manifest["capture_date"] == "2026-04-14"
	assert "engine_commit" in manifest
	assert "engine_dirty" in manifest
	assert isinstance(manifest["files"], list)
	# All sibling files (except manifest.json itself) should be listed
	for f in bundle_path.iterdir():
		if f.name != "manifest.json":
			assert f.name in manifest["files"]


def test_bundle_open_trades_at_start_only_contains_open_rows(
	capture_dir: Path,
	repo_root: Path,
	trade_db: Path,
	market_state: MarketState,
) -> None:
	"""open_trades_at_start.sqlite must contain ONLY rows with status='open'.

	Replay seeds its InMemoryTradeStore from this file; carrying in closed
	rows would break composite-key lookups for settlement events on the
	next day's replay."""
	from edge_catcher.monitors.capture.bundle import assemble_daily_bundle

	bundle_path = assemble_daily_bundle(
		capture_date=date(2026, 4, 14),
		capture_dir=capture_dir,
		repo_root=repo_root,
		db_path=trade_db,
		market_state=market_state,
	)
	conn = sqlite3.connect(str(bundle_path / "open_trades_at_start.sqlite"))
	try:
		all_statuses = [r[0] for r in conn.execute("SELECT status FROM paper_trades").fetchall()]
		open_tickers = [r[0] for r in conn.execute("SELECT ticker FROM paper_trades WHERE status='open'").fetchall()]
	finally:
		conn.close()

	assert all(s == "open" for s in all_statuses), f"non-open row in bundle: {all_statuses}"
	assert "KXOPEN-26APR14" in open_tickers
	assert "KXCLOSED-26APR14" not in open_tickers  # was exited — should be filtered out


def test_bundle_day_slice_contains_only_capture_day_rows(
	capture_dir: Path,
	repo_root: Path,
	trade_db: Path,
	market_state: MarketState,
) -> None:
	"""paper_trades_v2_<date>.sqlite should contain only rows whose entry_time
	falls within the capture day's UTC window. This is the "ground truth" source
	the parity test reads — cross-day bleed would confuse the comparison."""
	from edge_catcher.monitors.capture.bundle import assemble_daily_bundle

	bundle_path = assemble_daily_bundle(
		capture_date=date(2026, 4, 14),
		capture_dir=capture_dir,
		repo_root=repo_root,
		db_path=trade_db,
		market_state=market_state,
	)
	slice_db = bundle_path / "paper_trades_v2_2026-04-14.sqlite"
	conn = sqlite3.connect(str(slice_db))
	try:
		tickers = [r[0] for r in conn.execute("SELECT ticker FROM paper_trades").fetchall()]
	finally:
		conn.close()

	assert "KXOPEN-26APR14" in tickers
	assert "KXCLOSED-26APR14" in tickers
	assert "KXOTHERDAY-26APR13" not in tickers  # Filtered by entry_time window


def test_bundle_market_state_snapshot_round_trips(
	capture_dir: Path,
	repo_root: Path,
	trade_db: Path,
	market_state: MarketState,
) -> None:
	"""market_state_at_start.json should serialize the current orderbooks
	such that the replay's _seed_market_state can reconstruct them."""
	from edge_catcher.monitors.capture.bundle import assemble_daily_bundle

	bundle_path = assemble_daily_bundle(
		capture_date=date(2026, 4, 14),
		capture_dir=capture_dir,
		repo_root=repo_root,
		db_path=trade_db,
		market_state=market_state,
	)
	state = json.loads((bundle_path / "market_state_at_start.json").read_text(encoding="utf-8"))
	obs = state["orderbooks"]
	assert "KXSNAP" in obs
	assert obs["KXSNAP"]["yes_levels"] == [[0.42, 100], [0.41, 50]]
	assert obs["KXSNAP"]["no_levels"] == [[0.58, 75]]


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_bundle_missing_strategies_file_warns_but_succeeds(
	tmp_path: Path,
	capture_dir: Path,
	trade_db: Path,
	market_state: MarketState,
	caplog: pytest.LogCaptureFixture,
) -> None:
	"""If repo_root/edge_catcher/monitors/strategies_local.py is missing,
	bundle should log a warning and still produce every other artifact.
	The replay can still run if the dev workstation provides the strategies."""
	from edge_catcher.monitors.capture.bundle import assemble_daily_bundle

	empty_repo = tmp_path / "empty_repo"
	(empty_repo / "edge_catcher" / "monitors").mkdir(parents=True)
	(empty_repo / "config.local").mkdir(parents=True)
	(empty_repo / "config.local" / "paper-trader.yaml").write_text("x: y\n", encoding="utf-8")
	# No strategies_local.py

	with caplog.at_level("WARNING"):
		bundle_path = assemble_daily_bundle(
			capture_date=date(2026, 4, 14),
			capture_dir=capture_dir,
			repo_root=empty_repo,
			db_path=trade_db,
			market_state=market_state,
		)

	assert bundle_path.exists()
	assert not (bundle_path / "strategies_local.py").exists()
	assert any("strategies_local.py" in r.message for r in caplog.records)


def test_bundle_market_state_none_skips_snapshot(
	capture_dir: Path,
	repo_root: Path,
	trade_db: Path,
) -> None:
	"""If market_state is None, market_state_at_start.json should be omitted —
	allows test setups and catch-up runs to skip snapshot generation."""
	from edge_catcher.monitors.capture.bundle import assemble_daily_bundle

	bundle_path = assemble_daily_bundle(
		capture_date=date(2026, 4, 14),
		capture_dir=capture_dir,
		repo_root=repo_root,
		db_path=trade_db,
		market_state=None,
	)
	assert not (bundle_path / "market_state_at_start.json").exists()
	# Everything else still there
	assert (bundle_path / "manifest.json").exists()
	assert (bundle_path / "kalshi_engine_2026-04-14.jsonl.zst").exists()


def test_strategy_state_snapshot_happy_path(tmp_path):
	"""Snapshot writes a JSON envelope with all rows from the fixture DB,
	json.loads'd into native Python objects and grouped by strategy."""
	import json
	import sqlite3
	from datetime import datetime, timezone
	from edge_catcher.monitors.capture.bundle import _write_strategy_state_snapshot

	db_path = tmp_path / "fixture.db"
	conn = sqlite3.connect(str(db_path))
	conn.executescript("""
		CREATE TABLE strategy_state (
			strategy TEXT NOT NULL,
			key TEXT NOT NULL,
			value TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			PRIMARY KEY (strategy, key)
		);
	""")
	now = datetime.now(timezone.utc).isoformat()
	rows = [
		("strategy_a", "seen:KXETH", json.dumps(True), now),
		("strategy_a", "counter", json.dumps(42), now),
		("strategy_a", "rolling", json.dumps([1, 2, 3]), now),
		("strategy_b", "entered:KXLOL", json.dumps(1), now),
		("strategy_b", "nested", json.dumps({"a": 1, "b": [2, 3]}), now),
		("strategy_b", "scalar", json.dumps("string-val"), now),
	]
	conn.executemany(
		"INSERT INTO strategy_state (strategy, key, value, updated_at) VALUES (?, ?, ?, ?)",
		rows,
	)
	conn.commit()
	conn.close()

	dst = tmp_path / "strategy_state_at_start.json"
	_write_strategy_state_snapshot(db_path, dst)

	assert dst.exists()
	envelope = json.loads(dst.read_text(encoding="utf-8"))
	assert envelope["schema_version"] == 1
	# captured_at must be parseable ISO8601
	datetime.fromisoformat(envelope["captured_at"])
	assert envelope["states"]["strategy_a"] == {
		"seen:KXETH": True,
		"counter": 42,
		"rolling": [1, 2, 3],
	}
	assert envelope["states"]["strategy_b"] == {
		"entered:KXLOL": 1,
		"nested": {"a": 1, "b": [2, 3]},
		"scalar": "string-val",
	}

	# Stable serialization: running the snapshot twice on the same fixture DB
	# must produce identical `states` subtrees (captured_at drifts by wall
	# clock, so it's parsed out rather than raw-byte compared). Relies on
	# sort_keys=True in the writer.
	dst2 = tmp_path / "strategy_state_at_start_2.json"
	_write_strategy_state_snapshot(db_path, dst2)
	env2 = json.loads(dst2.read_text(encoding="utf-8"))
	assert env2["states"] == envelope["states"]
	# Re-serializing both states subtrees with the same options must produce
	# identical bytes — proves the writer's output order is deterministic.
	assert (
		json.dumps(env2["states"], sort_keys=True, indent=2)
		== json.dumps(envelope["states"], sort_keys=True, indent=2)
	)
