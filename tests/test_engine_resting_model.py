"""QueueFillModel unit tests (SPEC §7). All prices in cents, yes-terms prints.

The model is a pure per-order print-consumer: it mutates ONLY
``RestingOrder.queue_ahead`` and returns fill counts. Fill bookkeeping
(``filled_size``) belongs to the tracker (Task 5) — several tests below
simulate the tracker by bumping ``filled_size`` manually between consumes.
"""
from edge_catcher.engine.resting import Print, QueueFillModel, RestingOrder


def _order(side="no", price=15, size=10, queue_ahead=20.0, filled=0,
           expires_ts=2000.0, market_close_ts=3000.0,
           cancel_before_close_seconds=None):
	return RestingOrder(
		client_order_id="cid-1", order_id="oid-1", ticker="KXTEST-1",
		series="KXTEST", strategy="s", side=side, rest_price_cents=price,
		intended_size=size, filled_size=filled, placed_ts=1000.0,
		expires_ts=expires_ts, market_close_ts=market_close_ts,
		cancel_before_close_seconds=cancel_before_close_seconds, trade_id=None,
		queue_ahead=queue_ahead, state="resting")


# ---------------------------------------------------------------------------
# Base semantics (SPEC §7.2): at-level FIFO, through-level sweep, side routing
# ---------------------------------------------------------------------------

def test_at_level_print_consumes_queue_then_fills():
	# NO bid at 15 -> counter side yes, level L = 85.
	o = _order(queue_ahead=20.0)
	m = QueueFillModel()
	fills = m.consume(o, Print(ts=1500.0, yes_price_cents=85, size=25.0, taker_side="yes"))
	assert o.queue_ahead == 0.0
	assert fills == 5          # 25 - 20 queue-ahead


def test_at_level_print_smaller_than_queue_fills_nothing():
	o = _order(queue_ahead=20.0)
	m = QueueFillModel()
	assert m.consume(o, Print(1500.0, 85, 12.0, "yes")) == 0
	assert o.queue_ahead == 8.0


def test_through_level_print_fills_remaining_fully():
	o = _order(queue_ahead=999.0)
	m = QueueFillModel()
	assert m.consume(o, Print(1500.0, 90, 1.0, "yes")) == 10   # 90 > 85 = swept


def test_wrong_taker_side_ignored():
	o = _order()
	m = QueueFillModel()
	assert m.consume(o, Print(1500.0, 85, 25.0, "no")) == 0


def test_below_level_ignored():
	o = _order(queue_ahead=0.0)
	m = QueueFillModel()
	assert m.consume(o, Print(1500.0, 84, 25.0, "yes")) == 0


def test_degenerate_prints_never_fill():
	o = _order(queue_ahead=0.0)
	m = QueueFillModel()
	for p in (Print(1500.0, 85, 25.0, None), Print(1500.0, 85, 0.0, "yes"),
	          Print(1500.0, 0, 25.0, "yes"), Print(1500.0, 100, 25.0, "yes")):
		assert m.consume(o, p) == 0
	assert m.degenerate_count == 4


def test_yes_side_symmetry_through_level():
	# YES bid at 40 -> counter side no, level L = 60 in no-terms.
	# NO taker paying THROUGH = no_price > 60 = yes_price < 40.
	o = _order(side="yes", price=40, queue_ahead=0.0)
	m = QueueFillModel()
	assert m.consume(o, Print(1500.0, 39, 3.0, "no")) == 10
	o2 = _order(side="yes", price=40, queue_ahead=0.0)
	assert m.consume(o2, Print(1500.0, 40, 3.0, "no")) == 3   # at-level


def test_yes_side_at_level_queue_consumption():
	# Mirror of the first test on the YES side: YES bid at 40 -> counter
	# side no, level 60 in no-terms = yes_price 40. Queue 20 ahead.
	o = _order(side="yes", price=40, queue_ahead=20.0)
	m = QueueFillModel()
	fills = m.consume(o, Print(1500.0, 40, 25.0, "no"))
	assert o.queue_ahead == 0.0
	assert fills == 5


# ---------------------------------------------------------------------------
# FIFO across sequential prints + partial accumulation (tracker simulated)
# ---------------------------------------------------------------------------

def test_fifo_across_sequential_at_level_prints():
	o = _order(queue_ahead=20.0, size=10)
	m = QueueFillModel()
	# Print 12: all absorbed by queue (20 -> 8), nothing for us.
	assert m.consume(o, Print(1500.0, 85, 12.0, "yes")) == 0
	assert o.queue_ahead == 8.0
	# Print 10: 8 to queue, 2 to us.
	got = m.consume(o, Print(1501.0, 85, 10.0, "yes"))
	assert got == 2
	assert o.queue_ahead == 0.0
	o.filled_size += got  # tracker's job, simulated
	# Print 10: queue empty, all 10 available, but only 8 remaining.
	got = m.consume(o, Print(1502.0, 85, 10.0, "yes"))
	assert got == 8
	o.filled_size += got
	assert o.remaining == 0


def test_fill_capped_at_remaining_on_through_print():
	o = _order(queue_ahead=0.0, size=10, filled=8)
	m = QueueFillModel()
	assert m.consume(o, Print(1500.0, 95, 500.0, "yes")) == 2


def test_consume_never_mutates_filled_size_and_never_negative():
	o = _order(queue_ahead=20.0)
	m = QueueFillModel()
	for p in (Print(1500.0, 85, 25.0, "yes"), Print(1501.0, 90, 5.0, "yes"),
	          Print(1502.0, 84, 5.0, "yes"), Print(1503.0, 85, 1.0, "no")):
		got = m.consume(o, p)
		assert got >= 0
		assert o.filled_size == 0  # the model NEVER books fills itself


# ---------------------------------------------------------------------------
# Fractional print sizes (Kalshi qty can be fractional; contracts are whole)
# ---------------------------------------------------------------------------

def test_fractional_available_floors_to_whole_contracts():
	o = _order(queue_ahead=9.3)
	m = QueueFillModel()
	assert m.consume(o, Print(1500.0, 85, 12.0, "yes")) == 2   # int(2.7)
	o2 = _order(queue_ahead=11.6)
	assert m.consume(o2, Print(1500.0, 85, 12.0, "yes")) == 0  # int(0.4)


# ---------------------------------------------------------------------------
# RestingOrder derived properties (SPEC §5.1 deadline rule)
# ---------------------------------------------------------------------------

def test_deadline_ttl_only_when_close_unknown():
	o = _order(market_close_ts=None, expires_ts=2000.0)
	assert o.deadline_ts == 2000.0


def test_deadline_three_term_min_with_close_window():
	# expires 5000, close 3000, window 1500 -> close-window term 1500 wins.
	o = _order(expires_ts=5000.0, market_close_ts=3000.0,
	           cancel_before_close_seconds=1500)
	assert o.deadline_ts == 1500.0


def test_deadline_close_without_window_is_min_expires_close():
	o = _order(expires_ts=5000.0, market_close_ts=3000.0)
	assert o.deadline_ts == 3000.0
	o2 = _order(expires_ts=2000.0, market_close_ts=3000.0)
	assert o2.deadline_ts == 2000.0


def test_remaining_property():
	assert _order(size=10, filled=0).remaining == 10
	assert _order(size=10, filled=7).remaining == 3
	assert _order(size=10, filled=10).remaining == 0


# ---------------------------------------------------------------------------
# Determinism (SPEC §7.6): pure function of order params + print stream
# ---------------------------------------------------------------------------

def test_determinism_same_stream_twice_identical():
	prints = [Print(1500.0 + i, 85, 7.5, "yes") for i in range(6)]

	def run():
		o = _order(queue_ahead=20.0, size=10)
		m = QueueFillModel()
		total = []
		for p in prints:
			got = m.consume(o, p)
			o.filled_size += got
			total.append(got)
		return total, o.queue_ahead, o.filled_size

	assert run() == run()
