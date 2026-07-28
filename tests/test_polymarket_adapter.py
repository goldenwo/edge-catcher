"""Tests for the Polymarket adapter — public Gamma + data-api endpoints.

Network calls are mocked (monkeypatched `_get`); no actual HTTP traffic in
unit tests. Trade fixtures mirror the REAL data-api /trades row shape
verified live 2026-07-27 (project rule: mocks must match real API shape):
epoch-int timestamps in unix SECONDS, no `id` field, `outcome` as a display
label (team names on sports markets, "Yes"/"No" on binaries).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
import requests
import yaml

from edge_catcher.adapters.polymarket.adapter import (
	PolymarketAdapter,
	_parse_epoch,
	_parse_iso,
	_parse_iso_strict,
	_safe_float,
	_safe_int,
	_synth_trade_id,
	_usd_to_cents,
)

_ADAPTER_LOGGER = "edge_catcher.adapters.polymarket.adapter"

# Verified epoch fixtures (unix seconds → UTC):
#   1785168078 → 2026-07-27T16:01:18+00:00   (real captured trade timestamp)
#   1782864000 → 2026-07-01T00:00:00+00:00   (used as the `since` bound)
#   1783000000 → 2026-07-02T13:46:40+00:00   (newer than since)
#   1780000000 → 2026-05-28T20:26:40+00:00   (older than since)
_TS_REAL = 1785168078
_TS_SINCE = 1782864000
_TS_MID = 1783000000
_TS_OLD = 1780000000
_SINCE_ISO = "2026-07-01T00:00:00+00:00"


def _write_cfg(tmp_path: Path, **poly_overrides: Any) -> Path:
	"""Write a minimal Polymarket config YAML, with per-test overrides."""
	poly: dict[str, Any] = {
		"enabled": True,
		"gamma_base": "https://gamma-test.local",
		"clob_base": "https://clob-test.local",
		"data_base": "https://data-test.local",
		"rate_limit_seconds": 0.0,  # no sleep in tests
		"series": ["politics"],
		"statuses": ["closed"],
		"min_available_ram_pct": 0,  # never trigger memory pause
		"pagination": {"default_limit": 50},
	}
	poly.update(poly_overrides)
	cfg = {"adapters": {"polymarket": poly}}
	p = tmp_path / "markets-polymarket.yaml"
	p.write_text(yaml.safe_dump(cfg), encoding="utf-8")
	return p


@pytest.fixture
def config_yaml(tmp_path: Path) -> Path:
	"""Minimal Polymarket config — test URLs, single test category."""
	return _write_cfg(tmp_path)


@pytest.fixture
def adapter(config_yaml: Path) -> PolymarketAdapter:
	return PolymarketAdapter(config_path=config_yaml)


def _real_trade_row(**overrides: Any) -> dict[str, Any]:
	"""A data-api GET /trades row — REAL field set and types (verified live
	2026-07-27). NB: no `id` field exists on this endpoint; `timestamp` is
	an epoch int in unix SECONDS; `size`/`price` are floats."""
	row: dict[str, Any] = {
		"proxyWallet": "0x50fe8f2ecb0d1fc1ae0a2c5d0f7d29f5a1c3f1aa",
		"side": "SELL",
		"asset": "9729092554133288038542962040798144763943846251636946171115606767",
		"conditionId": "0xde9e46cb2e766a597ec8d79357e206c5a3d495c1c14b0b8e6a6c954172e15a92",
		"size": 35.71,
		"price": 0.999,
		"timestamp": _TS_REAL,
		"title": "Example market title",
		"slug": "example-market-title",
		"eventSlug": "example-event",
		"outcome": "Yes",
		"outcomeIndex": 0,
		"name": "polytrader",
		"pseudonym": "Adamant-Cattle",
		"transactionHash": "0xa7df19b8f5984e17c8f4b9f5e3f13a4a2a1de0dbf0a1b2c3d4e5f60718293a4b",
	}
	row.update(overrides)
	return row


def _gamma_market(**overrides: Any) -> dict[str, Any]:
	"""A Gamma /markets row — list fields are JSON-in-STRING per the live API."""
	raw: dict[str, Any] = {
		"id": 1,
		"conditionId": "0xc1",
		"active": False,
		"closed": True,
		"category": "politics",
		"question": "Will X happen?",
		"outcomes": '["Yes", "No"]',
		"outcomePrices": '["1", "0"]',
		"clobTokenIds": '["111", "222"]',
		"bestBid": 0.35,
		"bestAsk": 0.37,
		"lastTradePrice": 0.36,
		"volumeNum": 123.45,
		"endDate": "2026-07-27T12:00:00Z",
	}
	raw.update(overrides)
	return raw


def _http_error(status: int) -> requests.exceptions.HTTPError:
	resp = MagicMock()
	resp.status_code = status
	return requests.exceptions.HTTPError(response=resp)


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


class TestUsdToCents:
	def test_converts_usd_floats_to_integer_cents(self) -> None:
		assert _usd_to_cents(0.35) == 35
		assert _usd_to_cents(0.999) == 100  # rounds, stays int
		assert _usd_to_cents(1.0) == 100
		assert _usd_to_cents(0.0) == 0

	def test_tolerates_numeric_strings(self) -> None:
		assert _usd_to_cents("0.5") == 50

	def test_none_and_garbage_map_to_none(self) -> None:
		assert _usd_to_cents(None) is None
		assert _usd_to_cents("") is None
		assert _usd_to_cents("nope") is None


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


class TestParseEpoch:
	def test_parses_unix_seconds_to_utc(self) -> None:
		dt = _parse_epoch(_TS_REAL)
		assert dt == datetime(2026, 7, 27, 16, 1, 18, tzinfo=timezone.utc)
		assert dt.tzinfo == timezone.utc

	def test_tolerates_numeric_string(self) -> None:
		assert _parse_epoch(str(_TS_SINCE)) == datetime(2026, 7, 1, tzinfo=timezone.utc)

	def test_raises_on_missing_or_garbage(self) -> None:
		for bad in (None, "", "not-a-number", True):
			with pytest.raises(ValueError):
				_parse_epoch(bad)


class TestSynthTradeId:
	"""data-api rows have no `id`; we synthesize one deterministically.
	transactionHash alone is NOT unique — one tx can carry multiple fills."""

	def test_deterministic_across_calls(self) -> None:
		row = _real_trade_row()
		assert _synth_trade_id(row) == _synth_trade_id(dict(row))

	def test_distinct_fills_in_same_tx_get_distinct_ids(self) -> None:
		base = _real_trade_row()
		other_size = _real_trade_row(size=12.5)
		other_asset = _real_trade_row(asset="123456789")
		other_side = _real_trade_row(side="BUY")
		ids = {
			_synth_trade_id(base),
			_synth_trade_id(other_size),
			_synth_trade_id(other_asset),
			_synth_trade_id(other_side),
		}
		assert len(ids) == 4

	def test_id_is_nonempty_string(self) -> None:
		tid = _synth_trade_id(_real_trade_row())
		assert isinstance(tid, str)
		assert tid


# ---------------------------------------------------------------------------
# Adapter construction + config loading
# ---------------------------------------------------------------------------


class TestAdapterConstruction:
	def test_loads_config_from_yaml(self, adapter: PolymarketAdapter) -> None:
		assert adapter.gamma_base == "https://gamma-test.local"
		assert adapter.clob_base == "https://clob-test.local"
		assert adapter.data_base == "https://data-test.local"
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
		assert ad.data_base == PolymarketAdapter.DATA_BASE
		assert ad.data_base == "https://data-api.polymarket.com"

	def test_diagnostic_counters_start_at_zero(self, adapter: PolymarketAdapter) -> None:
		assert adapter.trade_404_count == 0
		assert adapter.trade_empty_count == 0
		assert adapter.non_binary_skipped_count == 0

	def test_max_trade_pages_configurable(self, tmp_path: Path) -> None:
		p = _write_cfg(tmp_path, pagination={"default_limit": 10, "max_trade_pages": 7})
		ad = PolymarketAdapter(config_path=p)
		assert ad.max_trade_pages == 7

	def test_gamma_sort_and_window_pass_through(self, tmp_path: Path) -> None:
		p = _write_cfg(
			tmp_path,
			order="volumeNum",
			ascending=False,
			end_date_min="2026-07-27T00:00:00Z",
			end_date_max="2026-07-27T23:59:59Z",
		)
		ad = PolymarketAdapter(config_path=p)
		assert ad.order == "volumeNum"
		assert ad.ascending is False
		assert ad.end_date_min == "2026-07-27T00:00:00Z"
		assert ad.end_date_max == "2026-07-27T23:59:59Z"


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

	def test_real_trade_row_passes_schema(self, adapter: PolymarketAdapter) -> None:
		"""The REAL data-api row (no `id` field!) must validate — the old
		schema required `id`/`outcome` and would have rejected every row."""
		adapter._validate_list([_real_trade_row()], "clob_trades_list")

	@pytest.mark.parametrize(
		"missing",
		["side", "size", "price", "timestamp", "conditionId", "outcomeIndex", "transactionHash"],
	)
	def test_trade_row_missing_required_field_raises(
		self, adapter: PolymarketAdapter, missing: str
	) -> None:
		row = _real_trade_row()
		del row[missing]
		with pytest.raises(ValueError):
			adapter._validate_list([row], "clob_trades_list")


# ---------------------------------------------------------------------------
# collect_markets — series/category filtering + pagination + Gamma params
# ---------------------------------------------------------------------------


class TestCollectMarkets:
	def test_filters_by_series_via_category(self, adapter: PolymarketAdapter, monkeypatch) -> None:
		"""Markets whose `category` doesn't match the configured series filter
		should be excluded."""
		batch = [
			_gamma_market(id=1, conditionId="c1", category="politics"),
			_gamma_market(id=2, conditionId="c2", category="sports"),  # filtered out
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
			_gamma_market(id=1, conditionId="c1", category="politics"),
			_gamma_market(id=2, conditionId="c2", category="sports"),
		]
		monkeypatch.setattr(adapter, "_get", MagicMock(return_value=batch))

		out = adapter.collect_markets()
		assert len(out) == 2

	def test_paginates_until_partial_page(self, adapter: PolymarketAdapter, monkeypatch) -> None:
		"""Loop should request additional pages while the response is full,
		then stop on a short page."""
		adapter.pagination_limit = 2  # tiny page for fast test
		full = [
			_gamma_market(id=1, conditionId="c1"),
			_gamma_market(id=2, conditionId="c2"),
		]
		short = [_gamma_market(id=3, conditionId="c3")]
		mock_get = MagicMock(side_effect=[full, short])
		monkeypatch.setattr(adapter, "_get", mock_get)

		out = adapter.collect_markets()
		assert len(out) == 3
		assert mock_get.call_count == 2

	def test_422_at_offset_treated_as_end_of_pagination(
		self, adapter: PolymarketAdapter, monkeypatch
	) -> None:
		"""Gamma rejects offsets beyond a hard ceiling with 422 — must end the
		sweep, not crash it (existing guard, kept through the repair)."""
		adapter.pagination_limit = 1
		mock_get = MagicMock(side_effect=[[_gamma_market()], _http_error(422)])
		monkeypatch.setattr(adapter, "_get", mock_get)

		out = adapter.collect_markets()
		assert len(out) == 1
		assert mock_get.call_count == 2

	def test_market_prices_stored_as_integer_cents(
		self, adapter: PolymarketAdapter, monkeypatch
	) -> None:
		"""Gamma bestBid/bestAsk/lastTradePrice are 0–1 USD floats — must be
		converted to cents for parity with Trade and the Kalshi adapter."""
		batch = [_gamma_market(bestBid=0.35, bestAsk=0.37, lastTradePrice=0.36)]
		monkeypatch.setattr(adapter, "_get", MagicMock(return_value=batch))

		m = adapter.collect_markets()[0]
		assert m.yes_bid == 35
		assert m.yes_ask == 37
		assert m.last_price == 36

	def test_passes_order_ascending_and_end_date_window(
		self, tmp_path: Path, monkeypatch
	) -> None:
		"""Configured Gamma sort + endDate window params must reach the API
		call (all bind server-side, verified live 2026-07-27)."""
		p = _write_cfg(
			tmp_path,
			order="volumeNum",
			ascending=False,
			end_date_min="2026-07-27T00:00:00Z",
			end_date_max="2026-07-27T23:59:59Z",
		)
		ad = PolymarketAdapter(config_path=p)
		mock_get = MagicMock(return_value=[])
		monkeypatch.setattr(ad, "_get", mock_get)

		ad.collect_markets()
		params = mock_get.call_args.kwargs["params"]
		assert params["order"] == "volumeNum"
		assert params["ascending"] == "false"
		assert params["end_date_min"] == "2026-07-27T00:00:00Z"
		assert params["end_date_max"] == "2026-07-27T23:59:59Z"

	def test_omits_sort_and_window_params_when_unconfigured(
		self, adapter: PolymarketAdapter, monkeypatch
	) -> None:
		mock_get = MagicMock(return_value=[])
		monkeypatch.setattr(adapter, "_get", mock_get)

		adapter.collect_markets()
		params = mock_get.call_args.kwargs["params"]
		for key in ("order", "ascending", "end_date_min", "end_date_max"):
			assert key not in params

	def test_flags_non_binary_markets(self, adapter: PolymarketAdapter, monkeypatch) -> None:
		"""negRisk markets and >2-outcome markets are still returned as Market
		rows but flagged so collect_trades skips them."""
		batch = [
			_gamma_market(id=1, conditionId="0xbin"),
			_gamma_market(id=2, conditionId="0xneg", negRisk=True),
			_gamma_market(id=3, conditionId="0xmulti", outcomes='["A", "B", "C"]'),
		]
		monkeypatch.setattr(adapter, "_get", MagicMock(return_value=batch))

		out = adapter.collect_markets()
		assert len(out) == 3  # metadata still stored for all
		assert adapter._non_binary_tickers == {"0xneg", "0xmulti"}


# ---------------------------------------------------------------------------
# collect_trades — data-api paging, taker_side mapping, failure visibility
# ---------------------------------------------------------------------------


class TestCollectTrades:
	def test_calls_data_api_trades_endpoint(self, adapter: PolymarketAdapter, monkeypatch) -> None:
		"""Trades must come from {data_base}/trades?market=... — the old CLOB
		/markets/{id}/trades endpoint is gone (404s for every market)."""
		mock_get = MagicMock(return_value=[_real_trade_row()])
		monkeypatch.setattr(adapter, "_get", mock_get)

		adapter.collect_trades("0xc1")
		url = mock_get.call_args.args[0]
		params = mock_get.call_args.kwargs["params"]
		assert url == "https://data-test.local/trades"
		assert params["market"] == "0xc1"
		assert params["limit"] == adapter.pagination_limit
		assert params["offset"] == 0

	def test_buy_of_leg0_maps_to_taker_yes(self, adapter: PolymarketAdapter, monkeypatch) -> None:
		"""outcomeIndex 0 = yes-leg token; BUY → taker holds yes."""
		monkeypatch.setattr(adapter, "_get", MagicMock(return_value=[
			_real_trade_row(side="BUY", outcomeIndex=0, price=0.65, size=10.0),
		]))
		trades = adapter.collect_trades("0xc1")
		assert len(trades) == 1
		assert trades[0].taker_side == "yes"
		assert trades[0].yes_price == 65
		assert trades[0].no_price == 35
		assert trades[0].count == 10

	def test_sell_of_leg0_with_team_name_outcome_maps_to_taker_no(
		self, adapter: PolymarketAdapter, monkeypatch
	) -> None:
		"""Sports markets label outcomes with team names ('T1'), not Yes/No.
		Side mapping must key off outcomeIndex — never fall into 'unknown'."""
		monkeypatch.setattr(adapter, "_get", MagicMock(return_value=[
			_real_trade_row(side="SELL", outcome="T1", outcomeIndex=0, price=0.999),
		]))
		trades = adapter.collect_trades("0xc1")
		assert len(trades) == 1
		assert trades[0].taker_side == "no"
		assert trades[0].yes_price == 100  # 0.999 → 100¢ rounded
		assert trades[0].no_price == 0

	def test_buy_of_leg1_maps_to_taker_no_with_no_leg_price(
		self, adapter: PolymarketAdapter, monkeypatch
	) -> None:
		"""outcomeIndex 1 = no-leg token; its `price` is the NO price."""
		monkeypatch.setattr(adapter, "_get", MagicMock(return_value=[
			_real_trade_row(side="BUY", outcome="T2", outcomeIndex=1, price=0.25),
		]))
		trades = adapter.collect_trades("0xc1")
		assert trades[0].taker_side == "no"
		assert trades[0].no_price == 25
		assert trades[0].yes_price == 75

	def test_sell_of_leg1_maps_to_taker_yes(self, adapter: PolymarketAdapter, monkeypatch) -> None:
		monkeypatch.setattr(adapter, "_get", MagicMock(return_value=[
			_real_trade_row(side="SELL", outcome="No", outcomeIndex=1, price=0.25),
		]))
		trades = adapter.collect_trades("0xc1")
		assert trades[0].taker_side == "yes"

	def test_epoch_timestamp_parsed_to_utc(self, adapter: PolymarketAdapter, monkeypatch) -> None:
		"""`timestamp` is an epoch int (unix seconds) — NOT an ISO string."""
		monkeypatch.setattr(adapter, "_get", MagicMock(return_value=[_real_trade_row()]))
		trades = adapter.collect_trades("0xc1")
		assert trades[0].created_time == datetime(2026, 7, 27, 16, 1, 18, tzinfo=timezone.utc)
		assert trades[0].created_time.tzinfo == timezone.utc

	def test_trade_ids_synthesized_and_distinct_per_fill(
		self, adapter: PolymarketAdapter, monkeypatch
	) -> None:
		"""Two fills in the same transaction (same tx hash, different size)
		must get distinct, non-empty, deterministic trade ids."""
		rows = [
			_real_trade_row(size=35.71),
			_real_trade_row(size=4.29),
		]
		monkeypatch.setattr(adapter, "_get", MagicMock(return_value=rows))
		trades = adapter.collect_trades("0xc1")
		assert len(trades) == 2
		assert trades[0].trade_id and trades[1].trade_id
		assert trades[0].trade_id != trades[1].trade_id
		assert trades[0].trade_id == _synth_trade_id(rows[0])

	def test_since_early_stops_pagination(self, adapter: PolymarketAdapter, monkeypatch) -> None:
		"""Rows are NEWEST-first, so `since` is an early-stop: once a page
		crosses the bound, no further (older) pages may be requested."""
		adapter.pagination_limit = 2
		page1 = [
			_real_trade_row(timestamp=_TS_REAL, size=1.0),
			_real_trade_row(timestamp=_TS_MID, size=2.0),
		]
		page2 = [
			_real_trade_row(timestamp=_TS_SINCE, size=3.0),  # == since → kept
			_real_trade_row(timestamp=_TS_OLD, size=4.0),    # < since → stop here
		]
		page3_never_fetched = [_real_trade_row(timestamp=_TS_OLD - 1000, size=5.0)]
		mock_get = MagicMock(side_effect=[page1, page2, page3_never_fetched])
		monkeypatch.setattr(adapter, "_get", mock_get)

		trades = adapter.collect_trades("0xc1", since=_SINCE_ISO)
		assert mock_get.call_count == 2  # page 3 never requested
		assert len(trades) == 3
		assert all(t.created_time >= datetime(2026, 7, 1, tzinfo=timezone.utc) for t in trades)

	def test_since_bad_string_ignored_with_warning(
		self, adapter: PolymarketAdapter, monkeypatch, caplog
	) -> None:
		monkeypatch.setattr(adapter, "_get", MagicMock(return_value=[_real_trade_row()]))
		with caplog.at_level(logging.WARNING, logger=_ADAPTER_LOGGER):
			trades = adapter.collect_trades("0xc1", since="not-a-date")
		assert len(trades) == 1
		assert "bad since" in caplog.text

	def test_404_is_counted_and_logged(
		self, adapter: PolymarketAdapter, monkeypatch, caplog
	) -> None:
		"""A silent 404→[] hid the dead CLOB endpoint for 12 weeks. 404s must
		now increment a counter and log a WARNING naming the market."""
		monkeypatch.setattr(adapter, "_get", MagicMock(side_effect=_http_error(404)))
		with caplog.at_level(logging.WARNING, logger=_ADAPTER_LOGGER):
			trades = adapter.collect_trades("0xdeadbeef")
		assert trades == []
		assert adapter.trade_404_count == 1
		assert "0xdeadbeef" in caplog.text

	def test_non_404_http_errors_still_raise(self, adapter: PolymarketAdapter, monkeypatch) -> None:
		monkeypatch.setattr(adapter, "_get", MagicMock(side_effect=_http_error(500)))
		with pytest.raises(requests.exceptions.HTTPError):
			adapter.collect_trades("0xc1")

	def test_empty_first_page_is_counted_and_logged(
		self, adapter: PolymarketAdapter, monkeypatch, caplog
	) -> None:
		monkeypatch.setattr(adapter, "_get", MagicMock(return_value=[]))
		with caplog.at_level(logging.WARNING, logger=_ADAPTER_LOGGER):
			trades = adapter.collect_trades("0xempty")
		assert trades == []
		assert adapter.trade_empty_count == 1
		assert "0xempty" in caplog.text

	def test_empty_page_after_full_pages_is_natural_end(
		self, adapter: PolymarketAdapter, monkeypatch
	) -> None:
		"""total % limit == 0 → the API serves a trailing empty page; that is
		end-of-data, not a zero-trades market — must not count as empty."""
		adapter.pagination_limit = 1
		mock_get = MagicMock(side_effect=[[_real_trade_row()], []])
		monkeypatch.setattr(adapter, "_get", mock_get)

		trades = adapter.collect_trades("0xc1")
		assert len(trades) == 1
		assert adapter.trade_empty_count == 0

	def test_page_cap_truncates_defensively(
		self, adapter: PolymarketAdapter, monkeypatch, caplog
	) -> None:
		adapter.pagination_limit = 1
		adapter.max_trade_pages = 2
		mock_get = MagicMock(return_value=[_real_trade_row()])  # endless full pages
		monkeypatch.setattr(adapter, "_get", mock_get)

		with caplog.at_level(logging.WARNING, logger=_ADAPTER_LOGGER):
			trades = adapter.collect_trades("0xc1")
		assert mock_get.call_count == 2
		assert len(trades) == 2
		assert "page cap" in caplog.text

	def test_non_binary_market_skipped_without_api_call(
		self, adapter: PolymarketAdapter, monkeypatch, caplog
	) -> None:
		"""Markets flagged non-binary during collect_markets must skip trades
		collection entirely (never store taker_side 'unknown' rows)."""
		mock_get = MagicMock()
		monkeypatch.setattr(adapter, "_get", mock_get)
		adapter._non_binary_tickers.add("0xneg")

		with caplog.at_level(logging.INFO, logger=_ADAPTER_LOGGER):
			trades = adapter.collect_trades("0xneg")
		assert trades == []
		assert mock_get.call_count == 0
		assert adapter.non_binary_skipped_count == 1
		assert "0xneg" in caplog.text

	def test_rows_with_out_of_range_outcome_index_dropped(
		self, adapter: PolymarketAdapter, monkeypatch, caplog
	) -> None:
		"""Belt-and-braces: a >2-outcome row reaching the mapper (e.g. after a
		--skip-market-scan restart) must be dropped, never stored 'unknown'."""
		rows = [
			_real_trade_row(side="BUY", outcomeIndex=0, size=1.0),
			_real_trade_row(side="BUY", outcome="C", outcomeIndex=2, size=2.0),
		]
		monkeypatch.setattr(adapter, "_get", MagicMock(return_value=rows))
		with caplog.at_level(logging.WARNING, logger=_ADAPTER_LOGGER):
			trades = adapter.collect_trades("0xc1")
		assert len(trades) == 1
		assert trades[0].taker_side == "yes"
		assert all(t.taker_side in ("yes", "no") for t in trades)
		assert "outcomeIndex" in caplog.text


# ---------------------------------------------------------------------------
# CLI wiring — `since` must reach collect_trades (API-service path already does)
# ---------------------------------------------------------------------------


class TestCliSincePassThrough:
	def _run(self, config_yaml: Path, tmp_path: Path, monkeypatch, args) -> dict[str, Any]:
		from edge_catcher.cli.download import _run_polymarket_download
		from edge_catcher.storage.models import Market

		captured: dict[str, Any] = {}
		market = Market(
			ticker="0xc1", event_ticker="1", series_ticker="politics",
			title="Q", status="closed", result=None,
			yes_bid=None, yes_ask=None, last_price=None,
			open_interest=None, volume=10,
			expiration_time=None, close_time=None, created_time=None,
			settled_time=None, open_time=None, notional_value=None,
			floor_strike=None, cap_strike=None,
		)

		def fake_collect_markets(self, series_tickers=None):  # noqa: ANN001
			return [market]

		def fake_collect_trades(self, ticker, since=None):  # noqa: ANN001
			captured["ticker"] = ticker
			captured["since"] = since
			return []

		monkeypatch.setattr(PolymarketAdapter, "collect_markets", fake_collect_markets)
		monkeypatch.setattr(PolymarketAdapter, "collect_trades", fake_collect_trades)
		_run_polymarket_download(args, config_yaml, tmp_path / "poly-test.db")
		return captured

	def test_since_passed_through_to_collect_trades(
		self, config_yaml: Path, tmp_path: Path, monkeypatch
	) -> None:
		args = SimpleNamespace(
			markets=str(config_yaml), db_path=None, dry_run=False,
			skip_market_scan=False, max_trade_markets=None,
			since="2026-07-01T00:00:00",
		)
		captured = self._run(config_yaml, tmp_path, monkeypatch, args)
		assert captured["since"] == "2026-07-01T00:00:00"
		assert captured["ticker"] == "0xc1"

	def test_missing_since_attr_defaults_to_none(
		self, config_yaml: Path, tmp_path: Path, monkeypatch
	) -> None:
		args = SimpleNamespace(
			markets=str(config_yaml), db_path=None, dry_run=False,
			skip_market_scan=False, max_trade_markets=None,
		)
		captured = self._run(config_yaml, tmp_path, monkeypatch, args)
		assert captured["since"] is None


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
