"""Bundle/replay resting-orders step tests (SPEC §8.3/§10) — the writer/seeder
pair, the manifest schema_version 2 discriminator, and the loud-absence rule.

Deliberately avoids assemble_daily_bundle's full path (git subprocess) so the
file runs clean on the Windows dev box; the end-to-end bundle round-trip is
covered by the existing capture-bundle suite (CI) + the Task 12 parity fixture.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from edge_catcher.engine.capture.bundle import (
	BUNDLE_MANIFEST_SCHEMA_VERSION, _write_resting_orders,
)
from edge_catcher.engine.replay.backtester import ReplayResult, _seed_resting_orders
from edge_catcher.engine.resting import QueueFillModel, RestingOrder, RestingOrderTracker


def _tracker() -> RestingOrderTracker:
	return RestingOrderTracker(QueueFillModel(), mid_provider=lambda t: None)


def _order() -> RestingOrder:
	return RestingOrder(
		client_order_id="cid-1", order_id="paper-cid-1", ticker="KXTEST-1",
		series="KXTEST", strategy="s", side="no", rest_price_cents=15,
		intended_size=10, filled_size=0, placed_ts=1000.0, expires_ts=99999.0,
		market_close_ts=None, cancel_before_close_seconds=None, trade_id=None,
		queue_ahead=3.0, state="resting")


def _mk_prior(tmp_path: Path, *, version: int, write_file: bool,
              snapshot: list | None = None) -> tuple[Path, Path]:
	"""Build day-N (prior) and day-N+1 (current) sibling bundle dirs."""
	prior = tmp_path / "2026-07-15"
	current = tmp_path / "2026-07-16"
	prior.mkdir()
	current.mkdir()
	(prior / "manifest.json").write_text(
		json.dumps({"schema_version": version, "capture_date": "2026-07-15"}),
		encoding="utf-8")
	if write_file:
		_write_resting_orders(prior / "resting_orders.json", snapshot)
	return prior, current


def test_manifest_schema_version_is_2():
	assert BUNDLE_MANIFEST_SCHEMA_VERSION == 2


def test_writer_always_writes_empty_list_for_none(tmp_path):
	dst = tmp_path / "resting_orders.json"
	_write_resting_orders(dst, None)
	assert json.loads(dst.read_text(encoding="utf-8")) == []


def test_writer_round_trips_tracker_snapshot(tmp_path):
	tr = _tracker()
	tr.register(_order())
	dst = tmp_path / "resting_orders.json"
	_write_resting_orders(dst, tr.to_snapshot())
	seeded = _tracker()
	_data = json.loads(dst.read_text(encoding="utf-8"))
	seeded.from_snapshot(_data)
	assert seeded.in_flight_count() == 1
	assert seeded.has_level("s", "KXTEST-1", "no", 15)


def test_seed_from_v2_prior_bundle(tmp_path):
	src = _tracker()
	src.register(_order())
	prior, current = _mk_prior(tmp_path, version=2, write_file=True,
	                           snapshot=src.to_snapshot())
	tr = _tracker()
	_seed_resting_orders(tr, current, prior)
	assert tr.in_flight_count() == 1


def test_seed_quiet_on_pre_maker_v1_bundle(tmp_path):
	prior, current = _mk_prior(tmp_path, version=1, write_file=False)
	tr = _tracker()
	_seed_resting_orders(tr, current, prior)          # no raise, empty seed
	assert tr.in_flight_count() == 0


def test_seed_loud_on_v2_bundle_missing_file(tmp_path):
	prior, current = _mk_prior(tmp_path, version=2, write_file=False)
	tr = _tracker()
	with pytest.raises(ValueError, match="assembly bug"):
		_seed_resting_orders(tr, current, prior)


def test_seed_no_prior_bundle_is_quiet(tmp_path):
	current = tmp_path / "2026-07-16"
	current.mkdir()
	tr = _tracker()
	_seed_resting_orders(tr, current, None)
	assert tr.in_flight_count() == 0


def test_replay_result_resting_ledger_defaults_empty():
	from edge_catcher.engine.market_state import MarketState
	res = ReplayResult(trades=[], final_market_state=MarketState(),
	                   events_processed=0, duration_seconds=0.0)
	assert res.resting_ledger == []
