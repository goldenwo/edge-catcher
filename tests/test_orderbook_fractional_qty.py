"""Cross-cutting validation tests for the fractional orderbook quantity feature.

Covers:
  A1 - Synthetic-REST round-trip: fractional levels survive JSON serialization
       without re-truncation (write → serialize → deserialize → ingest →
       assert bit-for-bit).
  A2 - Non-finite quantity absence: inf/nan quantity levels from a WS snapshot
       are ABSENT from the in-memory book (len==1, not just "no crash").
  A3 - Determinism after accumulation: repeated serialize → json.dumps of the
       same accumulated book yields identical bytes with no float64 noise.
"""

import json

from edge_catcher.engine.dispatch import (
	_handle_orderbook_snapshot,
	_handle_synthetic_rest_orderbook,
)
from edge_catcher.engine.market_state import MarketState, OrderbookSnapshot


# ---------------------------------------------------------------------------
# A1 — Synthetic-REST round-trip
# ---------------------------------------------------------------------------


def test_book_snapshot_roundtrips_through_synthetic_rest() -> None:
	"""Fractional levels written to the synthetic-REST payload shape and
	re-ingested via _handle_synthetic_rest_orderbook must be bit-for-bit
	equal to the originals — no re-truncation on the ingest path.

	Real handler signature: _handle_synthetic_rest_orderbook(market_state, payload)
	where payload = {"ticker": "T", "yes_levels": [[price, qty], ...], "no_levels": [...]}
	"""
	# Writer side: fractional levels that were already rounded to _QTY_DP=4.
	# These values have exact float representations so bit-for-bit comparison holds.
	levels: list[tuple[float, float]] = [(0.64, 20.56), (0.24, 7.25)]
	payload: dict = {
		"ticker": "T",
		"yes_levels": [list(t) for t in levels],
		"no_levels": [],
	}
	# Simulate JSONL round-trip (serialize → deserialize).
	wire: dict = json.loads(json.dumps(payload))

	ms = MarketState()
	_handle_synthetic_rest_orderbook(ms, wire)

	ob = ms.get_orderbook("T")
	assert ob is not None, "orderbook must be seeded after _handle_synthetic_rest_orderbook"
	# _handle_synthetic_rest_orderbook preserves input order (no re-sort on the
	# captured REST path — the live engine already sorted at capture time) and
	# seed_orderbook stores the snapshot verbatim, so ordered-list equality holds
	# and is the strongest assertion: it also catches accidental reorder/dedup
	# regressions in the ingest path, not just value re-truncation.
	assert ob.yes_levels == levels, (
		f"levels differ after JSON round-trip: got {ob.yes_levels!r}, expected {levels!r}"
	)
	assert ob.no_levels == []


# ---------------------------------------------------------------------------
# A2 — Non-finite quantity absence
# ---------------------------------------------------------------------------


def test_non_finite_qty_absent_from_book() -> None:
	"""Levels with inf or nan quantities in a WS orderbook_snapshot must be
	ABSENT from the in-memory book — asserting len==1, not just 'no crash'.

	Real handler signature: _handle_orderbook_snapshot(market_state, msg)
	where msg = {"msg": {"market_ticker": "T",
	                      "yes_dollars_fp": [[price_str, qty_str], ...],
	                      "no_dollars_fp": [[price_str, qty_str], ...]}}

	Prices 0.62–0.64 are valid integer cents (62–64¢), so the only reason
	the inf/nan levels are absent is the non-finite guard in _parse_qty.
	"""
	ms = MarketState()
	msg: dict = {
		"msg": {
			"market_ticker": "T",
			"yes_dollars_fp": [
				["0.6400", "20.56"],   # valid — must be present
				["0.6300", "inf"],     # non-finite qty — must be absent
				["0.6200", "nan"],     # non-finite qty — must be absent
			],
			"no_dollars_fp": [],
		},
	}
	_handle_orderbook_snapshot(ms, msg)

	ob = ms.get_orderbook("T")
	assert ob is not None, "orderbook must be seeded after _handle_orderbook_snapshot"

	yes = ob.yes_levels
	# The single valid level must be present.
	assert (0.64, 20.56) in yes, f"valid level missing from book: {yes!r}"
	# inf and nan levels must be ABSENT — len must be exactly 1.
	assert len(yes) == 1, (
		f"inf/nan levels leaked into book; expected 1 level, got {len(yes)}: {yes!r}"
	)
	# Depth should equal the single valid level's quantity.
	assert round(ob.depth) == 21, f"unexpected depth: {ob.depth!r}"


# ---------------------------------------------------------------------------
# A3 — Serialization determinism after accumulation
# ---------------------------------------------------------------------------


def test_serialization_is_deterministic_after_accumulation() -> None:
	"""Accumulating multiple fractional deltas then serializing the result
	to JSON must be deterministic (identical bytes every call) and must
	contain no float64 noise digits (e.g. 27.560000000000002).

	This locks the _QTY_DP=4 rounding in apply_orderbook_delta so that
	float64 accumulation errors can never produce non-round stored values.
	"""
	def build() -> str:
		ms = MarketState()
		ms.seed_orderbook("T", OrderbookSnapshot(yes_levels=[(0.64, 0.65)], no_levels=[]))
		# Accumulate: 0.65 + 10.0 = 10.65, + 19.91 = 30.56, − 3.0 = 27.56 (4dp clean)
		for d in (10.0, 19.91, -3.0):
			ms.apply_orderbook_delta("T", "yes", 0.64, d)
		lv = ms.get_orderbook("T")
		assert lv is not None
		return json.dumps([[p, q] for p, q in lv.yes_levels])

	# NOTE: the equal-bytes assertion relies on apply_orderbook_delta keeping the
	# level list in a deterministic (sorted) order. If this test is ever extended
	# to multiple price points, keep that in mind to avoid order-dependent flakiness.
	first = build()
	assert first == build(), "serialization must be deterministic"
	# No float64 noise: 27.56 must not appear as 27.560000000000002
	assert "0000000" not in first, (
		f"float64 noise detected in serialized book: {first!r}"
	)
	# Sanity: the value we expect is present
	assert "27.56" in first, f"expected accumulated value 27.56 in {first!r}"
