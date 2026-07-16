"""Maker helper tests (SPEC §6, §4.4). Book fixtures use OrderbookSnapshot directly.

NOTE on fixture prices: ``OrderbookSnapshot.yes_levels``/``no_levels`` store
price in DOLLARS (float), not cents — confirmed against the class docstring
and every existing fixture in tests/test_engine.py, tests/test_market_state.py,
tests/test_honest_paper_executor.py (e.g. ``(0.48, 20)`` for a 48c level).
``implied_asks`` converts via ``100 - round(p * 100)``. The cent-scale
literals in the original test draft (e.g. ``80`` for 80c) would produce
nonsensical implied asks under that formula, so fixture price components
here are dollars (``0.80``) — the comments' arithmetic is otherwise
unchanged from the draft. This is a fixture-only adaptation; would_cross's
semantics (boundary-inclusive crossing check) are untouched.
"""
import pytest

from edge_catcher.engine.execution import (
	build_maker_entry_order, resting_cap, validate_maker_signal, would_cross,
)
from edge_catcher.engine.market_state import OrderbookSnapshot
from edge_catcher.engine.strategy_base import Signal


def _maker_sig(**over):
	base = dict(action="enter", ticker="KXTEST-1", side="no", series="KXTEST",
	            strategy="s", reason="r", entry_price_cents=15,
	            exec_style="maker", rest_ttl_seconds=300,
	            stop_loss_distance_cents=5)
	base.update(over)
	return Signal(**base)


def test_would_cross_no_side_at_touch_equality_rejects():
	book = OrderbookSnapshot(yes_levels=[[0.80, 10]], no_levels=[[0.15, 5]])
	# implied NO ask = 100-80 = 20. q=20 == ask -> crosses (>= boundary).
	assert would_cross(book, "no", 20) is True
	assert would_cross(book, "no", 19) is False

def test_would_cross_yes_side_symmetric():
	book = OrderbookSnapshot(yes_levels=[[0.40, 10]], no_levels=[[0.55, 5]])
	# implied YES ask = 100-55 = 45.
	assert would_cross(book, "yes", 45) is True
	assert would_cross(book, "yes", 44) is False

def test_would_cross_empty_ladder_allows():
	book = OrderbookSnapshot(yes_levels=[], no_levels=[])
	assert would_cross(book, "no", 99) is False

def test_would_cross_multi_level_ladder_uses_cheapest_implied_ask():
	# yes bids 80c and 85c -> implied NO asks 20 and 15; BEST (cheapest) = 15
	# from the HIGHEST yes bid. would_cross must key off 15, not 20.
	book = OrderbookSnapshot(yes_levels=[[0.80, 10], [0.85, 5]], no_levels=[])
	assert would_cross(book, "no", 15) is True
	assert would_cross(book, "no", 14) is False

def test_would_cross_locked_book_at_own_touch_crosses():
	# Locked market: best NO bid (20c) == implied NO ask (100-80=20).
	# Joining our own side's touch at 20 IS at the implied ask -> crosses.
	book = OrderbookSnapshot(yes_levels=[[0.80, 10]], no_levels=[[0.20, 5]])
	assert would_cross(book, "no", 20) is True
	assert would_cross(book, "no", 19) is False

def test_validate_rejects_malformed_side():
	# implied_asks treats any non-"yes" string as "no" — a malformed side
	# must be caught HERE, before would_cross, or the no-cross guard is
	# silently evaluated against the wrong ladder (quality review, Task 2).
	for bad in ("Yes", "YES", "no ", "", "both"):
		assert validate_maker_signal(_maker_sig(side=bad)) == "invalid_maker_signal:side"

def test_validate_rejects_missing_ttl():
	assert validate_maker_signal(_maker_sig(rest_ttl_seconds=None)) == "invalid_maker_signal:no_ttl"

def test_validate_rejects_nonpositive_ttl():
	assert validate_maker_signal(_maker_sig(rest_ttl_seconds=0)) == "invalid_maker_signal:no_ttl"

def test_validate_rejects_out_of_band_price():
	assert validate_maker_signal(_maker_sig(entry_price_cents=0)) == "invalid_maker_signal:price_band"
	assert validate_maker_signal(_maker_sig(entry_price_cents=100)) == "invalid_maker_signal:price_band"
	assert validate_maker_signal(_maker_sig(entry_price_cents=None)) == "invalid_maker_signal:price_band"

def test_validate_ok():
	assert validate_maker_signal(_maker_sig()) is None

def test_resting_cap_absent_is_zero():
	assert resting_cap({}) == 0
	assert resting_cap({"execution": {}}) == 0

def test_resting_cap_reads_value():
	assert resting_cap({"execution": {"max_resting_per_strategy": 2}}) == 2
	assert resting_cap({"execution": {"max_resting_per_strategy": 0}}) == 0

@pytest.mark.parametrize("bad", ["2", True, -1, 2.5])
def test_resting_cap_present_invalid_raises(bad):
	with pytest.raises((TypeError, ValueError)):
		resting_cap({"execution": {"max_resting_per_strategy": bad}})

def test_builder_sets_gtc_and_unwalked_price():
	from datetime import datetime, timezone
	book = OrderbookSnapshot(yes_levels=[[0.80, 10]], no_levels=[[0.14, 5]])
	req = build_maker_entry_order(_maker_sig(), 3, book, datetime.now(timezone.utc))
	assert req.time_in_force == "gtc"
	assert req.limit_price_cents == 15   # NOT walked by slippage
	assert req.action == "buy" and req.size_contracts == 3

def test_builder_rejects_crossing_price():
	from datetime import datetime, timezone
	book = OrderbookSnapshot(yes_levels=[[0.86, 10]], no_levels=[[0.10, 5]])
	# implied NO ask = 14; rest at 15 would cross
	with pytest.raises(ValueError, match="would_cross"):
		build_maker_entry_order(_maker_sig(entry_price_cents=15), 3, book,
		                        datetime.now(timezone.utc))

def test_builder_rejects_nonpositive_size():
	from datetime import datetime, timezone
	book = OrderbookSnapshot(yes_levels=[], no_levels=[])
	with pytest.raises(ValueError):
		build_maker_entry_order(_maker_sig(), 0, book, datetime.now(timezone.utc))

def test_builder_rejects_invalid_signal():
	from datetime import datetime, timezone
	book = OrderbookSnapshot(yes_levels=[], no_levels=[])
	with pytest.raises(ValueError, match="invalid_maker_signal"):
		build_maker_entry_order(_maker_sig(rest_ttl_seconds=None), 3, book,
		                        datetime.now(timezone.utc))


# --- additional cases beyond the base draft ---

def test_would_cross_no_side_just_inside_boundary_is_safe():
	# rest one cent BELOW the implied ask -> joins, does not cross.
	book = OrderbookSnapshot(yes_levels=[[0.80, 10]], no_levels=[])
	assert would_cross(book, "no", 19) is False
	assert would_cross(book, "no", 20) is True

def test_resting_cap_missing_key_in_present_section_is_zero():
	assert resting_cap({"execution": {"unrelated_key": 5}}) == 0

def test_builder_preserves_ticker_series_strategy():
	from datetime import datetime, timezone
	book = OrderbookSnapshot(yes_levels=[], no_levels=[])
	req = build_maker_entry_order(_maker_sig(ticker="KXTEST-2", series="KXTEST"), 7, book,
	                              datetime.now(timezone.utc))
	assert req.ticker == "KXTEST-2"
	assert req.series == "KXTEST"
	assert req.strategy == "s"
	assert req.client_order_id  # non-empty, generated
