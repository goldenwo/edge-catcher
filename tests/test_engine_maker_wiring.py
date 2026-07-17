"""Engine-side maker wiring tests (SPEC §4.4 boot visibility, §5 internals,
§7.5 mid provider). The timer loop's LEDGER-irrelevance is pinned by the
dispatch/tracker suites (validity window + backdating); here we pin the boot
contract: fail-fast validation, tracker stashed, one explicit log line.
"""
from __future__ import annotations

import logging

import pytest

from edge_catcher.engine.engine import _boot_maker_wiring, _make_mid_provider
from edge_catcher.engine.resting import RestingOrderTracker


class _StubMarketState:
	def __init__(self, bid=None, ask=None):
		self._bid, self._ask = bid, ask

	def get_yes_bid(self, ticker):
		return self._bid

	def get_yes_ask(self, ticker):
		return self._ask


def test_mid_provider_returns_mid_when_both_sides_quoted():
	mid = _make_mid_provider(_StubMarketState(bid=14, ask=17))
	assert mid("KXTEST-1") == 16          # round(15.5) banker's -> 16


def test_mid_provider_returns_none_when_one_sided():
	assert _make_mid_provider(_StubMarketState(bid=14))("KXTEST-1") is None
	assert _make_mid_provider(_StubMarketState(ask=17))("KXTEST-1") is None


def test_boot_enabled_logs_cap_and_stashes_tracker(caplog):
	config = {"execution": {"max_resting_per_strategy": 2}}
	with caplog.at_level(logging.INFO, logger="edge_catcher.engine.engine"):
		cap = _boot_maker_wiring(config, _StubMarketState())
	assert cap == 2
	assert isinstance(config["_tracker"], RestingOrderTracker)
	assert any("maker execution ENABLED (max_resting_per_strategy=2)" in r.message
	           for r in caplog.records)


def test_boot_disabled_logs_disabled(caplog):
	config = {}
	with caplog.at_level(logging.INFO, logger="edge_catcher.engine.engine"):
		cap = _boot_maker_wiring(config, _StubMarketState())
	assert cap == 0
	assert any("maker execution DISABLED" in r.message for r in caplog.records)
	# Tracker is stashed even when disabled — dispatch's cap guard is the
	# enable switch; uniform wiring keeps the hot path a single check.
	assert isinstance(config["_tracker"], RestingOrderTracker)


def test_boot_present_but_invalid_raises():
	with pytest.raises(TypeError):
		_boot_maker_wiring({"execution": {"max_resting_per_strategy": "2"}},
		                   _StubMarketState())
