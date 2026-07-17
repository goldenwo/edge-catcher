"""RestingOrderTracker tests (SPEC §5): validity-window lifecycle, backdated
cancels, disposition/end_cause mapping, error isolation, ledger, serialization.

Model-time determinism (SPEC §5.1) is the load-bearing property: the ledger
must be a pure function of the event stream — stepping cadence must never
change outcomes, and a print at/after deadline_ts must never fill.
"""


from edge_catcher.engine.resting import (
	Print, QueueFillModel, RestingOrder, RestingOrderTracker,
)


def _order(coid="cid-1", ticker="KXTEST-1", side="no", price=15, size=10,
           queue_ahead=0.0, expires_ts=2000.0, market_close_ts=None,
           cancel_before_close_seconds=None):
	return RestingOrder(
		client_order_id=coid, order_id=f"paper-{coid}", ticker=ticker,
		series="KXTEST", strategy="s", side=side, rest_price_cents=price,
		intended_size=size, filled_size=0, placed_ts=1000.0,
		expires_ts=expires_ts, market_close_ts=market_close_ts,
		cancel_before_close_seconds=cancel_before_close_seconds,
		trade_id=None, queue_ahead=queue_ahead, state="resting")


def _tracker(mid=None):
	return RestingOrderTracker(QueueFillModel(), mid_provider=lambda t: mid)


def _crossing(ts, size=50.0):
	# Through-level print for a NO bid at 15 (level 85): yes taker at 90.
	return Print(ts=ts, yes_price_cents=90, size=size, taker_side="yes")


def _at_level(ts, size):
	return Print(ts=ts, yes_price_cents=85, size=size, taker_side="yes")


# ---------------------------------------------------------------------------
# Fill path
# ---------------------------------------------------------------------------

def test_full_fill_emits_first_fill_event_and_books_state():
	tr = _tracker(mid=16)
	tr.register(_order())
	events = tr.step(now=1500.0, prints_by_ticker={"KXTEST-1": [_crossing(1500.0)]})
	fills = [e for e in events if e.kind == "fill"]
	assert len(fills) == 1 and fills[0].first_fill and fills[0].size == 10
	assert fills[0].order.state == "filled"
	row = tr.ledger[0]
	assert row.disposition == "filled" and row.end_cause is None
	assert row.fills == [(1500.0, 10)]
	assert row.time_to_first_fill == 500.0


def test_partial_fills_accumulate_across_steps():
	tr = _tracker()
	tr.register(_order(queue_ahead=0.0))
	e1 = tr.step(1400.0, {"KXTEST-1": [_at_level(1400.0, 4.0)]})
	e2 = tr.step(1500.0, {"KXTEST-1": [_at_level(1500.0, 3.0)]})
	assert [x.size for x in e1 + e2 if x.kind == "fill"] == [4, 3]
	assert e1[0].first_fill and not e2[0].first_fill
	o = e1[0].order
	assert o.filled_size == 7 and o.state == "partially_filled"


# ---------------------------------------------------------------------------
# Validity window (SPEC §5.1): the decisive determinism rule
# ---------------------------------------------------------------------------

def test_validity_window_backdated_cancel_beats_late_crossing_print():
	tr = _tracker()
	tr.register(_order(expires_ts=2000.0))
	# Single LATE step carrying a crossing print stamped after the deadline:
	events = tr.step(now=2500.0, prints_by_ticker={"KXTEST-1": [_crossing(2500.0)]})
	assert [e.kind for e in events] == ["cancel"]
	assert events[0].cause == "expired"
	assert events[0].ts == 2000.0            # BACKDATED to deadline, not now
	row = tr.ledger[0]
	assert row.disposition == "expired" and row.fills == []


def test_print_at_exact_deadline_does_not_fill():
	tr = _tracker()
	tr.register(_order(expires_ts=2000.0))
	events = tr.step(2000.0, {"KXTEST-1": [_crossing(2000.0)]})
	assert [e.kind for e in events] == ["cancel"]
	assert tr.ledger[0].disposition == "expired"


def test_cadence_irrelevance_one_late_step_equals_many_small_steps():
	prints = [_at_level(1400.0, 4.0), _at_level(1600.0, 3.0), _crossing(2400.0)]

	def run(step_plan):
		tr = _tracker()
		tr.register(_order(expires_ts=2000.0))
		for now, batch in step_plan:
			tr.step(now, {"KXTEST-1": batch} if batch else {})
		return [(r.disposition, r.end_cause, tuple(r.fills)) for r in tr.ledger]

	one_late = run([(2500.0, prints)])
	fine_grained = run([(1400.0, prints[:1]), (1600.0, prints[1:2]),
	                    (2100.0, []), (2400.0, prints[2:]), (2500.0, [])])
	assert one_late == fine_grained
	# 7 filled of 10, TTL cancelled the rest, late crossing print ignored:
	assert one_late[0][0] == "partial" and one_late[0][1] == "expired"


# ---------------------------------------------------------------------------
# Disposition / end_cause canonical rule (SPEC §5.3)
# ---------------------------------------------------------------------------

def test_zero_fill_ttl_is_expired_close_window_is_cancelled():
	tr = _tracker()
	tr.register(_order(coid="a", expires_ts=2000.0))
	tr.register(_order(coid="b", ticker="KXTEST-2", expires_ts=9000.0,
	                   market_close_ts=3000.0, cancel_before_close_seconds=1500))
	tr.step(2600.0, {})
	by = {r.client_order_id: r for r in tr.ledger}
	assert by["a"].disposition == "expired"
	assert by["b"].disposition == "cancelled"      # close-window bound (1500)
	assert by["b"].end_cause is None               # end_cause populated for partial only


def test_partial_then_ttl_maps_to_partial_with_end_cause():
	tr = _tracker()
	tr.register(_order(expires_ts=2000.0))
	tr.step(1500.0, {"KXTEST-1": [_at_level(1500.0, 4.0)]})
	events = tr.step(2500.0, {})
	assert events[0].kind == "cancel" and events[0].ts == 2000.0
	row = tr.ledger[0]
	assert row.disposition == "partial" and row.end_cause == "expired"
	assert row.fills == [(1500.0, 4)]


def test_strategy_cancel_is_cancelled_not_backdated():
	tr = _tracker()
	tr.register(_order(expires_ts=9999.0))
	tr.cancel("cid-1", cause="cancelled", now=1500.0)
	row = tr.ledger[0]
	assert row.disposition == "cancelled"
	assert tr.in_flight_count() == 0


def test_terminal_states_absorb_duplicate_events():
	tr = _tracker()
	tr.register(_order(expires_ts=2000.0))
	tr.step(2500.0, {})
	tr.step(2600.0, {"KXTEST-1": [_crossing(2600.0)]})   # nothing revives
	tr.cancel("cid-1", cause="cancelled", now=2700.0)     # idempotent no-op
	assert len(tr.ledger) == 1 and tr.ledger[0].disposition == "expired"


# ---------------------------------------------------------------------------
# Guard data source + caps support
# ---------------------------------------------------------------------------

def test_has_level_and_in_flight_count():
	tr = _tracker()
	tr.register(_order())
	assert tr.has_level("s", "KXTEST-1", "no", 15) is True
	assert tr.has_level("s", "KXTEST-1", "no", 16) is False
	assert tr.has_level("other", "KXTEST-1", "no", 15) is False
	assert tr.in_flight_count() == 1
	assert tr.in_flight_count(strategy="s") == 1
	assert tr.in_flight_count(strategy="other") == 0
	tr.step(2500.0, {})    # TTL cancels it
	assert tr.has_level("s", "KXTEST-1", "no", 15) is False
	assert tr.in_flight_count() == 0


# ---------------------------------------------------------------------------
# Mark-out scheduling (SPEC §7.5: pending-sample, first tick at-or-after)
# ---------------------------------------------------------------------------

def test_markouts_sample_at_first_step_at_or_after_offsets():
	mids = {"value": 20}
	tr = RestingOrderTracker(QueueFillModel(), mid_provider=lambda t: mids["value"])
	tr.register(_order(expires_ts=99999.0))
	tr.step(1500.0, {"KXTEST-1": [_crossing(1500.0)]})     # fill at 1500
	row = tr.ledger[0]
	assert row.mark_outs == []                              # nothing sampled yet
	mids["value"] = 25
	tr.step(1530.0, {})                                     # 1500+30
	assert row.mark_outs == [(1500.0, 30, 25)]
	mids["value"] = 30
	tr.step(1900.0, {})                                     # covers +120 and +300
	assert (1500.0, 120, 30) in row.mark_outs and (1500.0, 300, 30) in row.mark_outs


def test_multi_fill_markouts_recorded_per_fill_never_overwritten():
	# SPEC §11: "mark-outs present for every fill" — a second fill must ADD
	# its own records, not overwrite the first's (Tasks 4+5 quality review).
	tr = RestingOrderTracker(QueueFillModel(), mid_provider=lambda t: 42)
	tr.register(_order(expires_ts=99999.0, size=10))
	tr.step(1500.0, {"KXTEST-1": [_at_level(1500.0, 4.0)]})   # fill 4 @1500
	tr.step(1600.0, {"KXTEST-1": [_at_level(1600.0, 3.0)]})   # fill 3 @1600
	tr.step(2000.0, {})                                        # all 6 samples due
	row = tr.ledger[0]
	assert sorted(row.mark_outs) == [
		(1500.0, 30, 42), (1500.0, 120, 42), (1500.0, 300, 42),
		(1600.0, 30, 42), (1600.0, 120, 42), (1600.0, 300, 42),
	]


def test_register_duplicate_client_order_id_raises():
	import pytest
	tr = _tracker()
	tr.register(_order())
	with pytest.raises(ValueError, match="duplicate client_order_id"):
		tr.register(_order())


# ---------------------------------------------------------------------------
# Error isolation (SPEC §5 internals): one bad order never kills the step
# ---------------------------------------------------------------------------

def test_per_order_error_isolation():
	class BoomModel(QueueFillModel):
		def consume(self, order, p):
			if order.client_order_id == "bad":
				raise RuntimeError("boom")
			return super().consume(order, p)

	tr = RestingOrderTracker(BoomModel(), mid_provider=lambda t: None)
	tr.register(_order(coid="bad"))
	tr.register(_order(coid="good"))
	events = tr.step(1500.0, {"KXTEST-1": [_crossing(1500.0)]})
	kinds = {(e.kind, e.order.client_order_id) for e in events}
	assert ("error", "bad") in kinds
	assert ("fill", "good") in kinds
	by = {r.client_order_id: r for r in tr.ledger}
	assert by["bad"].disposition == "errored"
	assert by["good"].disposition == "filled"


# ---------------------------------------------------------------------------
# Serialization round-trip (SPEC §5.5) + stream-end censoring (SPEC §11)
# ---------------------------------------------------------------------------

def test_snapshot_round_trip_identical_continuation():
	def run(snapshot_mid_way):
		tr = _tracker()
		tr.register(_order(expires_ts=3000.0, queue_ahead=5.0))
		tr.step(1400.0, {"KXTEST-1": [_at_level(1400.0, 8.0)]})   # 3 fill (8-5)
		if snapshot_mid_way:
			snap = tr.to_snapshot()
			tr = RestingOrderTracker(QueueFillModel(), mid_provider=lambda t: None)
			tr.from_snapshot(snap)
		tr.step(1600.0, {"KXTEST-1": [_at_level(1600.0, 4.0)]})
		tr.step(3500.0, {})
		return [(r.disposition, r.end_cause, tuple(r.fills)) for r in tr.ledger
		        if r.disposition is not None]

	direct = run(False)
	resumed = run(True)
	# The resumed tracker only carries the in-flight order (ledger rows for
	# already-terminal orders live in the pre-snapshot session), so compare
	# the surviving order's outcome:
	assert direct == resumed


def test_snapshot_serializes_plain_data_only():
	tr = _tracker()
	tr.register(_order())
	snap = tr.to_snapshot()
	import json
	json.dumps(snap)   # must be JSON-serializable for the bundle step


def test_censor_open_marks_still_resting_orders():
	tr = _tracker()
	tr.register(_order(expires_ts=99999.0))
	tr.censor_open(ts=5000.0)
	assert tr.ledger[0].disposition == "censored_stream_end"


# ---------------------------------------------------------------------------
# Hot-path guarantee (SPEC §5 internals): empty tracker = one cheap check
# ---------------------------------------------------------------------------

def test_empty_tracker_step_is_noop():
	tr = _tracker()
	assert tr.step(1500.0, {"KXTEST-1": [_crossing(1500.0)]}) == []
	assert tr.ledger == []
