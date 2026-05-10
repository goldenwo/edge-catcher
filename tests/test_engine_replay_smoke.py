"""Smoke test for engine/replay/backtester — runs synthetic bundle, asserts trades.

This is the only CI test that exercises the full replay path through the new
engine end-to-end. The parity sweep against R2 bundles is gitignored + operator-
invoked (see scripts/check_g_parity.py); this catches regressions in the
engine/replay codepath itself without needing R2 access.

The fixture under tests/fixtures/synthetic_bundle/ is intentionally synthetic —
``SyntheticTickStrategy`` enters on the first observation of ``SYN-TEST-T1`` and
that's it. No real strategy parameters or logic, so this fixture is safe to
track in the public repo (the gitignored real-bundle parity sweep covers
production-strategy verification).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from edge_catcher.engine.replay.backtester import replay_capture

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "synthetic_bundle" / "2026-04-15"
EXPECTED_FIELDS = ("strategy", "ticker", "side", "fill_size", "blended_entry", "status")


@pytest.mark.asyncio
async def test_replay_capture_synthetic_bundle_produces_expected_trades():
	"""End-to-end: bundle -> replay_capture -> InMemoryTradeStore.all_trades().

	Asserts the replay path through the new engine produces exactly the
	canned expectation in expected_trades.json. Any drift in dispatch
	wiring, executor sizing, or trade_store recording surfaces here as
	a row-shape mismatch.
	"""
	expected = json.loads((FIXTURE_DIR / "expected_trades.json").read_text(encoding="utf-8"))

	result = await replay_capture(FIXTURE_DIR)

	assert result.events_processed == 2, (
		f"expected 2 events processed (orderbook_snapshot + ticker), "
		f"got {result.events_processed}"
	)
	assert len(result.trades) == len(expected), (
		f"trade count mismatch: got {len(result.trades)}, expected {len(expected)}"
	)
	for actual_row, expected_row in zip(result.trades, expected):
		projection = {k: actual_row.get(k) for k in EXPECTED_FIELDS}
		assert projection == expected_row, (
			f"trade row diverged from expectation:\n"
			f"  got:      {projection}\n"
			f"  expected: {expected_row}"
		)
