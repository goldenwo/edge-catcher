"""Tests for the engine's rotation callback factory.

Covers the closure built by _make_rotation_callback that RawFrameWriter
fires at midnight UTC rollover. Verifies:
  * The market_state is snapshotted via deepcopy on the engine thread.
  * assemble_daily_bundle is called on a background thread with the snapshot.
  * transport.upload_bundle runs after assembly when a transport is provided.
  * A None transport is handled cleanly (local-only bundles).
  * Exceptions in assembly/upload are logged but don't leak.
"""
from __future__ import annotations

import time
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest

pytest.importorskip("websockets", reason="paper-trader engine tests require the [live] extra")

from edge_catcher.monitors.engine import _make_rotation_callback
from edge_catcher.monitors.market_state import MarketState, OrderbookSnapshot


def _wait_for(predicate, timeout: float = 2.0, step: float = 0.01) -> bool:
	"""Spin-wait for ``predicate()`` to return truthy, up to ``timeout`` seconds."""
	deadline = time.monotonic() + timeout
	while time.monotonic() < deadline:
		if predicate():
			return True
		time.sleep(step)
	return False


def test_rotation_callback_deepcopies_market_state_synchronously(
	tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
	"""The callback must deepcopy market_state BEFORE returning from the
	synchronous call — otherwise a background assemble thread could race
	with engine-thread mutations."""
	import edge_catcher.monitors.engine as engine_mod

	# Capture what assemble_daily_bundle sees as its market_state argument.
	seen_market_state: list[MarketState] = []
	bundle_done = [False]

	def fake_assemble(capture_date, capture_dir, repo_root, db_path, market_state):
		seen_market_state.append(market_state)
		bundle_done[0] = True
		return tmp_path / "bundle-stub"

	monkeypatch.setattr(engine_mod, "assemble_daily_bundle", fake_assemble)

	live = MarketState()
	live.seed_orderbook("KXLIVE", OrderbookSnapshot(yes_levels=[(0.5, 10)], no_levels=[(0.5, 10)]))

	cb = _make_rotation_callback(
		capture_dir=tmp_path / "capture",
		repo_root=tmp_path / "repo",
		db_path=tmp_path / "paper.db",
		market_state=live,
		transport=None,
	)

	# Fire the callback — the deepcopy happens synchronously before the
	# background thread is spawned. We mutate `live` AFTER the callback
	# returns to prove the background thread's snapshot is independent.
	cb(date(2026, 4, 14))
	live.seed_orderbook("KXMUTATED", OrderbookSnapshot(yes_levels=[(0.9, 99)], no_levels=[]))

	# Wait for the background assemble to actually run
	assert _wait_for(lambda: bundle_done[0]), "assemble_daily_bundle never ran"
	assert len(seen_market_state) == 1
	snap = seen_market_state[0]
	# The snapshot saw KXLIVE but NOT the post-return KXMUTATED
	assert "KXLIVE" in snap._orderbooks
	assert "KXMUTATED" not in snap._orderbooks


def test_rotation_callback_uploads_via_transport_when_provided(
	tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
	"""When a transport is wired in, the callback should call upload_bundle
	with the bundle path returned by assemble_daily_bundle and a
	'kalshi/<date>' remote key."""
	import edge_catcher.monitors.engine as engine_mod

	stub_bundle_path = tmp_path / "bundle-stub"
	stub_bundle_path.mkdir()

	def fake_assemble(*, capture_date, capture_dir, repo_root, db_path, market_state):
		return stub_bundle_path

	monkeypatch.setattr(engine_mod, "assemble_daily_bundle", fake_assemble)

	transport = MagicMock()
	transport.upload_bundle = MagicMock()

	cb = _make_rotation_callback(
		capture_dir=tmp_path / "capture",
		repo_root=tmp_path / "repo",
		db_path=tmp_path / "paper.db",
		market_state=MarketState(),
		transport=transport,
	)

	cb(date(2026, 4, 14))
	assert _wait_for(lambda: transport.upload_bundle.call_count == 1), "upload_bundle never called"
	args, _kwargs = transport.upload_bundle.call_args
	assert args[0] == stub_bundle_path
	assert args[1] == "kalshi/2026-04-14"


def test_rotation_callback_none_transport_skips_upload(
	tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
	"""When transport is None the callback still assembles the bundle but
	doesn't try to upload — bundles accumulate on local disk only."""
	import edge_catcher.monitors.engine as engine_mod

	assembled = [False]

	def fake_assemble(*, capture_date, capture_dir, repo_root, db_path, market_state):
		assembled[0] = True
		return tmp_path / "bundle-stub"

	monkeypatch.setattr(engine_mod, "assemble_daily_bundle", fake_assemble)

	cb = _make_rotation_callback(
		capture_dir=tmp_path / "capture",
		repo_root=tmp_path / "repo",
		db_path=tmp_path / "paper.db",
		market_state=MarketState(),
		transport=None,
	)

	cb(date(2026, 4, 14))
	assert _wait_for(lambda: assembled[0]), "assemble never ran"
	# No transport — nothing to assert about upload. Just confirm no crash.


def test_rotation_callback_exception_in_background_is_logged(
	tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
	"""An exception during background assemble/upload must be logged but
	not propagate — the engine thread has already returned from the
	synchronous callback by the time the error happens."""
	import edge_catcher.monitors.engine as engine_mod

	def fake_assemble(*args, **kwargs):
		raise RuntimeError("assemble blew up")

	monkeypatch.setattr(engine_mod, "assemble_daily_bundle", fake_assemble)

	cb = _make_rotation_callback(
		capture_dir=tmp_path / "capture",
		repo_root=tmp_path / "repo",
		db_path=tmp_path / "paper.db",
		market_state=MarketState(),
		transport=None,
	)

	with caplog.at_level("ERROR"):
		cb(date(2026, 4, 14))  # sync call must not raise
		# Post-retention split: assembly failures log "background bundle assembly failed",
		# upload failures log "bundle <day> upload failed". Accept either shape.
		assert _wait_for(
			lambda: any(
				"background bundle assembly failed" in r.message or "upload failed" in r.message
				for r in caplog.records
			)
		), "expected background failure log message"
