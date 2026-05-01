"""Tests for the Polymarket adapter — public Gamma + CLOB endpoints.

Network calls are mocked via requests-style monkeypatch on the adapter's
session.get; no actual HTTP traffic in unit tests.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

from edge_catcher.adapters.polymarket.adapter import (
	PolymarketAdapter,
	_parse_iso,
	_parse_iso_strict,
	_safe_float,
	_safe_int,
)


@pytest.fixture
def config_yaml(tmp_path: Path) -> Path:
	"""Minimal Polymarket config — uses default URLs, single test category."""
	cfg = {
		"adapters": {
			"polymarket": {
				"enabled": True,
				"gamma_base": "https://gamma-test.local",
				"clob_base": "https://clob-test.local",
				"rate_limit_seconds": 0.0,  # no sleep in tests
				"series": ["politics"],
				"statuses": ["closed"],
				"min_available_ram_pct": 0,  # never trigger memory pause
				"pagination": {"default_limit": 50},
			},
		},
	}
	p = tmp_path / "markets-polymarket.yaml"
	p.write_text(yaml.safe_dump(cfg), encoding="utf-8")
	return p


@pytest.fixture
def adapter(config_yaml: Path) -> PolymarketAdapter:
	return PolymarketAdapter(config_path=config_yaml)


# ---------------------------------------------------------------------------
# Module helpers
# ---------------------------------------------------------------------------


class TestSafeCoercions:
	def test_safe_float_handles_string(self) -> None:
		assert _safe_float("0.5") == 0.5

	def test_safe_float_handles_none_and_empty(self) -> None:
		assert _safe_float(None) is None
		assert _safe_float("") is None

	def test_safe_float_handles_garbage(self) -> None:
		assert _safe_float("not a number") is None

	def test_safe_int_handles_float_string(self) -> None:
		assert _safe_int("100.0") == 100

	def test_safe_int_handles_none(self) -> None:
		assert _safe_int(None) is None


class TestParseIso:
	def test_parses_z_suffix(self) -> None:
		dt = _parse_iso("2026-04-30T12:00:00Z")
		assert dt is not None
		assert dt == datetime(2026, 4, 30, 12, 0, 0, tzinfo=timezone.utc)

	def test_parses_offset_suffix(self) -> None:
		dt = _parse_iso("2026-04-30T12:00:00+00:00")
		assert dt is not None
		assert dt.tzinfo is not None

	def test_attaches_utc_when_naive(self) -> None:
		dt = _parse_iso("2026-04-30T12:00:00")
		assert dt is not None
		assert dt.tzinfo == timezone.utc

	def test_returns_none_on_garbage(self) -> None:
		assert _parse_iso("not a date") is None
		assert _parse_iso(None) is None

	def test_strict_raises_on_missing(self) -> None:
		with pytest.raises(ValueError):
			_parse_iso_strict(None)


# ---------------------------------------------------------------------------
# Adapter construction + config loading
# ---------------------------------------------------------------------------


class TestAdapterConstruction:
	def test_loads_config_from_yaml(self, adapter: PolymarketAdapter) -> None:
		assert adapter.gamma_base == "https://gamma-test.local"
		assert adapter.clob_base == "https://clob-test.local"
		assert adapter.series == ["politics"]
		assert adapter.statuses == ["closed"]
		assert adapter.pagination_limit == 50

	def test_uses_default_urls_when_unspecified(self, tmp_path: Path) -> None:
		cfg = {"adapters": {"polymarket": {"enabled": True}}}
		p = tmp_path / "minimal.yaml"
		p.write_text(yaml.safe_dump(cfg), encoding="utf-8")
		ad = PolymarketAdapter(config_path=p)
		assert ad.gamma_base == PolymarketAdapter.GAMMA_BASE
		assert ad.clob_base == PolymarketAdapter.CLOB_BASE


# ---------------------------------------------------------------------------
# validate_response + _validate_list
# ---------------------------------------------------------------------------


class TestValidation:
	def test_validate_response_accepts_required_fields(self, adapter: PolymarketAdapter) -> None:
		assert adapter.validate_response({"id": "x", "conditionId": "y"}, "gamma_market_detail") is True

	def test_validate_response_raises_on_missing(self, adapter: PolymarketAdapter) -> None:
		with pytest.raises(ValueError):
			adapter.validate_response({"id": "x"}, "gamma_market_detail")

	def test_validate_response_unknown_schema(self, adapter: PolymarketAdapter) -> None:
		with pytest.raises(ValueError):
			adapter.validate_response({}, "nonexistent_schema")

	def test_validate_list_per_item_required(self, adapter: PolymarketAdapter) -> None:
		# Missing `closed` field on second item.
		items = [
			{"id": 1, "conditionId": "a", "active": True, "closed": False},
			{"id": 2, "conditionId": "b", "active": True},
		]
		with pytest.raises(ValueError):
			adapter._validate_list(items, "gamma_markets_list")


# ---------------------------------------------------------------------------
# collect_markets — series/category filtering + pagination
# ---------------------------------------------------------------------------


class TestCollectMarkets:
	def test_filters_by_series_via_category(self, adapter: PolymarketAdapter, monkeypatch) -> None:
		"""Markets whose `category` doesn't match the configured series filter
		should be excluded."""
		batch = [
			{"id": 1, "conditionId": "c1", "active": False, "closed": True,
				"category": "politics", "question": "Will X happen?"},
			{"id": 2, "conditionId": "c2", "active": False, "closed": True,
				"category": "sports", "question": "Will Y win?"},  # filtered out
		]
		# One page, then empty → loop exits.
		monkeypatch.setattr(adapter, "_get", MagicMock(return_value=batch))

		out = adapter.collect_markets()
		assert len(out) == 1
		assert out[0].ticker == "c1"
		assert out[0].series_ticker == "politics"

	def test_no_filter_when_series_empty(self, adapter: PolymarketAdapter, monkeypatch) -> None:
		"""series_filter=[] means accept all markets regardless of category."""
		adapter.series = []
		batch = [
			{"id": 1, "conditionId": "c1", "active": False, "closed": True,
				"category": "politics", "question": "Q1"},
			{"id": 2, "conditionId": "c2", "active": False, "closed": True,
				"category": "sports", "question": "Q2"},
		]
		monkeypatch.setattr(adapter, "_get", MagicMock(return_value=batch))

		out = adapter.collect_markets()
		assert len(out) == 2

	def test_paginates_until_partial_page(self, adapter: PolymarketAdapter, monkeypatch) -> None:
		"""Loop should request additional pages while the response is full,
		then stop on a short page."""
		adapter.pagination_limit = 2  # tiny page for fast test
		full = [
			{"id": 1, "conditionId": "c1", "active": False, "closed": True, "category": "politics"},
			{"id": 2, "conditionId": "c2", "active": False, "closed": True, "category": "politics"},
		]
		short = [
			{"id": 3, "conditionId": "c3", "active": False, "closed": True, "category": "politics"},
		]
		mock_get = MagicMock(side_effect=[full, short])
		monkeypatch.setattr(adapter, "_get", mock_get)

		out = adapter.collect_markets()
		assert len(out) == 3
		assert mock_get.call_count == 2


# ---------------------------------------------------------------------------
# collect_trades — taker_side mapping + since filter
# ---------------------------------------------------------------------------


class TestCollectTrades:
	def test_maps_buy_yes_to_taker_yes(self, adapter: PolymarketAdapter, monkeypatch) -> None:
		"""BUY of Yes outcome → taker now holds Yes."""
		monkeypatch.setattr(adapter, "_get", MagicMock(return_value=[
			{"id": "t1", "side": "BUY", "outcome": "Yes", "size": 10, "price": 0.65,
				"timestamp": "2026-04-30T12:00:00Z"},
		]))
		trades = adapter.collect_trades("c1")
		assert len(trades) == 1
		assert trades[0].taker_side == "yes"
		assert trades[0].yes_price == 65
		assert trades[0].no_price == 35

	def test_maps_sell_yes_to_taker_no(self, adapter: PolymarketAdapter, monkeypatch) -> None:
		"""SELL of Yes outcome → taker now holds No."""
		monkeypatch.setattr(adapter, "_get", MagicMock(return_value=[
			{"id": "t1", "side": "SELL", "outcome": "Yes", "size": 5, "price": 0.30,
				"timestamp": "2026-04-30T12:00:00Z"},
		]))
		trades = adapter.collect_trades("c1")
		assert trades[0].taker_side == "no"

	def test_since_filter_excludes_old_trades(self, adapter: PolymarketAdapter, monkeypatch) -> None:
		monkeypatch.setattr(adapter, "_get", MagicMock(return_value=[
			{"id": "t1", "side": "BUY", "outcome": "Yes", "size": 1, "price": 0.5,
				"timestamp": "2026-01-01T00:00:00Z"},
			{"id": "t2", "side": "BUY", "outcome": "Yes", "size": 1, "price": 0.5,
				"timestamp": "2026-04-01T00:00:00Z"},
		]))
		trades = adapter.collect_trades("c1", since="2026-03-01T00:00:00")
		assert len(trades) == 1
		assert trades[0].trade_id == "t2"


# ---------------------------------------------------------------------------
# Registry contract — adapter is discoverable + correctly wired
# ---------------------------------------------------------------------------


class TestRegistryWireup:
	def test_polymarket_adapters_registered_in_central_registry(self) -> None:
		from api.adapter_registry import ADAPTERS

		poly = [a for a in ADAPTERS if a.exchange == "polymarket"]
		assert len(poly) >= 1
		assert any(a.id == "polymarket_default" for a in poly)

	def test_dispatchers_registered(self) -> None:
		from api.dispatchers import DOWNLOAD_DISPATCHERS, DATA_CHECK_DISPATCHERS

		assert "polymarket" in DOWNLOAD_DISPATCHERS
		assert "polymarket" in DATA_CHECK_DISPATCHERS
