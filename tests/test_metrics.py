"""Tests for the paper trader operational metrics counter."""

import pytest

from edge_catcher.monitors.metrics import Metrics


def test_metrics_starts_at_zero():
	m = Metrics()
	snap = m.snapshot()
	assert snap["entries_attempted"] == 0
	assert snap["entries_filled"] == 0
	assert snap["entries_skipped_stale"] == 0
	assert snap["entries_skipped_unsupported"] == 0
	assert snap["entries_skipped_other"] == 0
	assert snap["trades_settled_won"] == 0
	assert snap["trades_settled_lost"] == 0


def test_counter_increment():
	m = Metrics()
	m.inc("entries_attempted")
	m.inc("entries_attempted")
	m.inc("entries_filled")
	assert m.snapshot()["entries_attempted"] == 2
	assert m.snapshot()["entries_filled"] == 1


def test_reset_zeroes_counters_only():
	m = Metrics()
	m.inc("entries_attempted")
	m.inc("entries_attempted")
	m.set_gauge("entries_skipped_unsupported", 7)
	snap = m.reset_and_snapshot()
	assert snap["entries_attempted"] == 2
	assert snap["entries_skipped_unsupported"] == 7
	# After reset: counters zeroed, gauge retained
	next_snap = m.snapshot()
	assert next_snap["entries_attempted"] == 0
	assert next_snap["entries_skipped_unsupported"] == 7


def test_gauge_persists_across_many_resets():
	"""Regression test: the unsupported-skip gauge must not decay to zero."""
	m = Metrics()
	m.set_gauge("entries_skipped_unsupported", 3)
	for _ in range(5):
		m.reset_and_snapshot()
	assert m.snapshot()["entries_skipped_unsupported"] == 3


def test_inc_rejects_gauge_key():
	"""Guardrail: inc() on a gauge key raises so callers don't silently lose data."""
	m = Metrics()
	with pytest.raises(KeyError):
		m.inc("entries_skipped_unsupported")
