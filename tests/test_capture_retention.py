"""Tests for the bundle retention helpers.

Covers ``mark_bundle_uploaded``, ``delete_raw_jsonl``, and ``prune_old_bundles``
in ``edge_catcher/monitors/capture/bundle.py``, plus the rotation callback
end-to-end retention flow.

Without retention, the Pi's raw JSONL accumulates at ~1.5 GB/day and fills
the disk in ~3 months. These tests verify that with retention enabled:
  * Raw JSONL is deleted after a compressed bundle is produced
  * The .uploaded sentinel gates pruning so silent upload failures don't
    cause data loss
  * Pruning only touches uploaded bundles older than the retention window
  * Local-only mode (transport=None) never auto-prunes
"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pytest

from edge_catcher.monitors.capture.bundle import (
	UPLOADED_SENTINEL,
	delete_raw_jsonl,
	mark_bundle_uploaded,
	prune_old_bundles,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def capture_dir(tmp_path: Path) -> Path:
	d = tmp_path / "capture"
	d.mkdir()
	return d


def _make_bundle(capture_dir: Path, capture_date: date, uploaded: bool = False) -> Path:
	"""Create a fake bundle dir with the compressed JSONL marker inside."""
	bundle = capture_dir / capture_date.isoformat()
	bundle.mkdir(parents=True, exist_ok=True)
	(bundle / f"kalshi_engine_{capture_date.isoformat()}.jsonl.zst").write_bytes(b"stub")
	(bundle / "manifest.json").write_text("{}", encoding="utf-8")
	if uploaded:
		(bundle / UPLOADED_SENTINEL).touch()
	return bundle


def _make_raw(capture_dir: Path, capture_date: date) -> Path:
	raw = capture_dir / f"kalshi_engine_{capture_date.isoformat()}.jsonl"
	raw.write_text("stub raw jsonl", encoding="utf-8")
	return raw


# ---------------------------------------------------------------------------
# mark_bundle_uploaded
# ---------------------------------------------------------------------------


def test_mark_bundle_uploaded_creates_sentinel(capture_dir: Path) -> None:
	bundle = _make_bundle(capture_dir, date(2026, 4, 14), uploaded=False)
	assert not (bundle / UPLOADED_SENTINEL).exists()
	mark_bundle_uploaded(bundle)
	assert (bundle / UPLOADED_SENTINEL).exists()


def test_mark_bundle_uploaded_is_idempotent(capture_dir: Path) -> None:
	bundle = _make_bundle(capture_dir, date(2026, 4, 14))
	mark_bundle_uploaded(bundle)
	mark_bundle_uploaded(bundle)  # must not raise
	assert (bundle / UPLOADED_SENTINEL).exists()


# ---------------------------------------------------------------------------
# delete_raw_jsonl
# ---------------------------------------------------------------------------


def test_delete_raw_jsonl_happy_path(capture_dir: Path) -> None:
	"""With a compressed copy present, the raw file is deleted."""
	day = date(2026, 4, 14)
	_make_bundle(capture_dir, day)  # creates the compressed copy
	raw = _make_raw(capture_dir, day)

	assert delete_raw_jsonl(capture_dir, day) is True
	assert not raw.exists()


def test_delete_raw_jsonl_refuses_without_compressed_copy(capture_dir: Path) -> None:
	"""If the bundle's compressed copy is missing, the raw MUST NOT be deleted
	— otherwise a crashed rotation callback would lose the day's capture."""
	day = date(2026, 4, 14)
	raw = _make_raw(capture_dir, day)
	# No bundle dir at all → no compressed copy

	assert delete_raw_jsonl(capture_dir, day) is False
	assert raw.exists(), "raw JSONL must survive when compressed copy is missing"


def test_delete_raw_jsonl_missing_raw_is_noop(capture_dir: Path) -> None:
	"""Called for a day where the raw file was already deleted (idempotent retry)."""
	day = date(2026, 4, 14)
	_make_bundle(capture_dir, day)
	# No raw file to delete
	assert delete_raw_jsonl(capture_dir, day) is False


# ---------------------------------------------------------------------------
# prune_old_bundles
# ---------------------------------------------------------------------------


def test_prune_keeps_recent_bundles(capture_dir: Path) -> None:
	"""Bundles within the retention window are kept regardless of upload state."""
	today = date(2026, 4, 14)
	_make_bundle(capture_dir, today - timedelta(days=1), uploaded=True)
	_make_bundle(capture_dir, today - timedelta(days=2), uploaded=True)
	_make_bundle(capture_dir, today - timedelta(days=3), uploaded=False)  # not uploaded but recent

	deleted = prune_old_bundles(capture_dir, retention_days=7, today=today)
	assert deleted == []
	assert (capture_dir / (today - timedelta(days=1)).isoformat()).exists()
	assert (capture_dir / (today - timedelta(days=2)).isoformat()).exists()
	assert (capture_dir / (today - timedelta(days=3)).isoformat()).exists()


def test_prune_deletes_old_uploaded_bundles(capture_dir: Path) -> None:
	today = date(2026, 4, 14)
	_make_bundle(capture_dir, today - timedelta(days=10), uploaded=True)
	_make_bundle(capture_dir, today - timedelta(days=8), uploaded=True)
	_make_bundle(capture_dir, today - timedelta(days=1), uploaded=True)  # fresh, keep

	deleted = prune_old_bundles(capture_dir, retention_days=7, today=today)
	assert sorted(deleted) == [today - timedelta(days=10), today - timedelta(days=8)]
	assert not (capture_dir / (today - timedelta(days=10)).isoformat()).exists()
	assert not (capture_dir / (today - timedelta(days=8)).isoformat()).exists()
	assert (capture_dir / (today - timedelta(days=1)).isoformat()).exists()


def test_prune_skips_old_unuploaded_bundles(
	capture_dir: Path, caplog: pytest.LogCaptureFixture,
) -> None:
	"""An old bundle without the .uploaded sentinel is preserved and logs a warning.
	This is the safety net against silent upload failures."""
	today = date(2026, 4, 14)
	_make_bundle(capture_dir, today - timedelta(days=15), uploaded=False)

	with caplog.at_level("WARNING"):
		deleted = prune_old_bundles(capture_dir, retention_days=7, today=today)

	assert deleted == []
	assert (capture_dir / (today - timedelta(days=15)).isoformat()).exists()
	assert any(
		UPLOADED_SENTINEL in r.message and "skipping prune" in r.message
		for r in caplog.records
	)


def test_prune_with_retention_zero_is_noop(capture_dir: Path) -> None:
	today = date(2026, 4, 14)
	_make_bundle(capture_dir, today - timedelta(days=30), uploaded=True)
	deleted = prune_old_bundles(capture_dir, retention_days=0, today=today)
	assert deleted == []
	assert (capture_dir / (today - timedelta(days=30)).isoformat()).exists()


def test_prune_ignores_non_date_dirs(capture_dir: Path) -> None:
	"""Random subdirectories with non-date names are left alone."""
	today = date(2026, 4, 14)
	(capture_dir / "some_other_dir").mkdir()
	(capture_dir / "not-a-date-at-all").mkdir()
	_make_bundle(capture_dir, today - timedelta(days=15), uploaded=True)

	deleted = prune_old_bundles(capture_dir, retention_days=7, today=today)
	assert deleted == [today - timedelta(days=15)]
	assert (capture_dir / "some_other_dir").exists()
	assert (capture_dir / "not-a-date-at-all").exists()


def test_prune_ignores_files(capture_dir: Path) -> None:
	"""Files in the capture directory (e.g. the active JSONL) are ignored."""
	today = date(2026, 4, 14)
	(capture_dir / "kalshi_engine_2026-04-14.jsonl").write_text("active raw", encoding="utf-8")
	(capture_dir / ".recv_seq").write_text("42", encoding="utf-8")
	(capture_dir / ".writer.lock").write_text("", encoding="utf-8")
	_make_bundle(capture_dir, today - timedelta(days=10), uploaded=True)

	deleted = prune_old_bundles(capture_dir, retention_days=7, today=today)
	assert deleted == [today - timedelta(days=10)]
	assert (capture_dir / "kalshi_engine_2026-04-14.jsonl").exists()
	assert (capture_dir / ".recv_seq").exists()
	assert (capture_dir / ".writer.lock").exists()


def test_prune_require_uploaded_false_deletes_unconditionally(capture_dir: Path) -> None:
	"""When require_uploaded=False, pruning is driven only by age — used
	in test harnesses or aggressive cleanup modes. Production should
	always pass require_uploaded=True."""
	today = date(2026, 4, 14)
	_make_bundle(capture_dir, today - timedelta(days=15), uploaded=False)

	deleted = prune_old_bundles(
		capture_dir,
		retention_days=7,
		require_uploaded=False,
		today=today,
	)
	assert deleted == [today - timedelta(days=15)]


# ---------------------------------------------------------------------------
# End-to-end retention through the rotation callback
# ---------------------------------------------------------------------------


def test_rotation_callback_deletes_raw_and_marks_uploaded(
	tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
	"""End-to-end: the rotation callback with a transport configured should
	delete the raw JSONL and write the .uploaded sentinel on success.

	Uses ``today() - 1`` rather than a hardcoded date so the freshly-uploaded
	bundle stays inside ``local_retention_days=7`` — mirroring the production
	scenario where rotation fires at midnight UTC for the day that just
	ended. A hardcoded past date would let ``prune_old_bundles`` delete the
	bundle directory (sentinel and all) before the test's assertions ran.
	"""
	import time
	from unittest.mock import MagicMock
	pytest.importorskip("websockets", reason="rotation callback tests require the [live] extra")
	import edge_catcher.monitors.engine as engine_mod
	from edge_catcher.monitors.engine import _make_rotation_callback
	from edge_catcher.monitors.market_state import MarketState

	capture_dir = tmp_path / "capture"
	capture_dir.mkdir()
	day = date.today() - timedelta(days=1)

	# Seed a raw JSONL + a stub compressed copy that assemble_daily_bundle "produced"
	_make_raw(capture_dir, day)

	def fake_assemble(*, capture_date, capture_dir, repo_root, db_path, market_state):
		# Create the compressed file where delete_raw_jsonl expects it
		bundle_dir = capture_dir / capture_date.isoformat()
		bundle_dir.mkdir(parents=True, exist_ok=True)
		(bundle_dir / f"kalshi_engine_{capture_date.isoformat()}.jsonl.zst").write_bytes(b"stub")
		return bundle_dir

	monkeypatch.setattr(engine_mod, "assemble_daily_bundle", fake_assemble)

	transport = MagicMock()
	transport.upload_bundle = MagicMock()

	cb = _make_rotation_callback(
		capture_dir=capture_dir,
		repo_root=tmp_path / "repo",
		db_path=tmp_path / "paper.db",
		market_state=MarketState(),
		transport=transport,
		delete_raw_after_bundle=True,
		local_retention_days=7,
	)

	cb(day)

	# Wait for background thread to finish
	deadline = time.monotonic() + 2.0
	raw_path = capture_dir / f"kalshi_engine_{day.isoformat()}.jsonl"
	bundle_path = capture_dir / day.isoformat()
	while time.monotonic() < deadline:
		if transport.upload_bundle.called and not raw_path.exists():
			break
		time.sleep(0.01)

	assert transport.upload_bundle.call_count == 1
	assert not raw_path.exists(), "raw JSONL should be deleted after bundle assembly"
	assert (bundle_path / UPLOADED_SENTINEL).exists(), ".uploaded sentinel should be written"


def test_rotation_callback_keeps_raw_when_assembly_fails(
	tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
	"""If assemble_daily_bundle raises, the raw JSONL must survive so the
	operator can reassemble manually."""
	import time
	pytest.importorskip("websockets", reason="rotation callback tests require the [live] extra")
	import edge_catcher.monitors.engine as engine_mod
	from edge_catcher.monitors.engine import _make_rotation_callback
	from edge_catcher.monitors.market_state import MarketState

	capture_dir = tmp_path / "capture"
	capture_dir.mkdir()
	day = date(2026, 4, 13)
	_make_raw(capture_dir, day)

	def failing_assemble(*args, **kwargs):
		raise RuntimeError("disk full or whatever")

	monkeypatch.setattr(engine_mod, "assemble_daily_bundle", failing_assemble)

	cb = _make_rotation_callback(
		capture_dir=capture_dir,
		repo_root=tmp_path / "repo",
		db_path=tmp_path / "paper.db",
		market_state=MarketState(),
		transport=None,
		delete_raw_after_bundle=True,
		local_retention_days=0,
	)

	cb(day)

	# Give the background thread a tick
	time.sleep(0.1)
	raw_path = capture_dir / f"kalshi_engine_{day.isoformat()}.jsonl"
	assert raw_path.exists(), "raw JSONL must survive an assembly failure"


def test_rotation_callback_no_transport_skips_sentinel_and_prune(
	tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
	"""With transport=None, the callback still assembles + deletes the raw
	JSONL (compressed copy is authoritative) but does NOT write the
	.uploaded sentinel and does NOT prune old bundles. Local-only capture
	must be manually managed."""
	import time
	pytest.importorskip("websockets", reason="rotation callback tests require the [live] extra")
	import edge_catcher.monitors.engine as engine_mod
	from edge_catcher.monitors.engine import _make_rotation_callback
	from edge_catcher.monitors.market_state import MarketState

	capture_dir = tmp_path / "capture"
	capture_dir.mkdir()
	day = date(2026, 4, 13)
	_make_raw(capture_dir, day)

	# Pre-existing old bundle that WOULD be pruned if retention ran
	old_day = day - timedelta(days=30)
	_make_bundle(capture_dir, old_day, uploaded=False)

	def fake_assemble(*, capture_date, capture_dir, repo_root, db_path, market_state):
		bundle_dir = capture_dir / capture_date.isoformat()
		bundle_dir.mkdir(parents=True, exist_ok=True)
		(bundle_dir / f"kalshi_engine_{capture_date.isoformat()}.jsonl.zst").write_bytes(b"stub")
		return bundle_dir

	monkeypatch.setattr(engine_mod, "assemble_daily_bundle", fake_assemble)

	cb = _make_rotation_callback(
		capture_dir=capture_dir,
		repo_root=tmp_path / "repo",
		db_path=tmp_path / "paper.db",
		market_state=MarketState(),
		transport=None,
		delete_raw_after_bundle=True,
		local_retention_days=7,
	)

	cb(day)

	# Wait for background
	deadline = time.monotonic() + 2.0
	raw_path = capture_dir / f"kalshi_engine_{day.isoformat()}.jsonl"
	while time.monotonic() < deadline:
		if not raw_path.exists():
			break
		time.sleep(0.01)

	# Raw should be deleted (compressed copy exists)
	assert not raw_path.exists()
	# No sentinel (no transport → no upload)
	bundle_path = capture_dir / day.isoformat()
	assert not (bundle_path / UPLOADED_SENTINEL).exists()
	# Old bundle preserved (pruning is skipped in local-only mode)
	assert (capture_dir / old_day.isoformat()).exists()


def test_rotation_callback_keeps_raw_when_delete_flag_false(
	tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
	"""delete_raw_after_bundle=False preserves the raw JSONL (debugging mode)."""
	import time
	pytest.importorskip("websockets", reason="rotation callback tests require the [live] extra")
	import edge_catcher.monitors.engine as engine_mod
	from edge_catcher.monitors.engine import _make_rotation_callback
	from edge_catcher.monitors.market_state import MarketState

	capture_dir = tmp_path / "capture"
	capture_dir.mkdir()
	day = date(2026, 4, 13)
	_make_raw(capture_dir, day)

	def fake_assemble(*, capture_date, capture_dir, repo_root, db_path, market_state):
		bundle_dir = capture_dir / capture_date.isoformat()
		bundle_dir.mkdir(parents=True, exist_ok=True)
		(bundle_dir / f"kalshi_engine_{capture_date.isoformat()}.jsonl.zst").write_bytes(b"stub")
		return bundle_dir

	monkeypatch.setattr(engine_mod, "assemble_daily_bundle", fake_assemble)

	cb = _make_rotation_callback(
		capture_dir=capture_dir,
		repo_root=tmp_path / "repo",
		db_path=tmp_path / "paper.db",
		market_state=MarketState(),
		transport=None,
		delete_raw_after_bundle=False,  # preserved for debugging
		local_retention_days=0,
	)

	cb(day)
	time.sleep(0.1)

	raw_path = capture_dir / f"kalshi_engine_{day.isoformat()}.jsonl"
	assert raw_path.exists(), "raw JSONL must be preserved when delete_raw_after_bundle=False"
