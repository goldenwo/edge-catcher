"""Tests for the REST recovery module."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest


class TestFetchActiveTickersForSeries:
	def test_returns_active_tickers(self):
		"""Should return all market tickers for an active series."""
		from edge_catcher.monitors.recovery import fetch_active_tickers_for_series

		events_response = MagicMock(
			status_code=200,
			json=lambda: {"events": [{"event_ticker": "EVT1"}, {"event_ticker": "EVT2"}]},
		)
		markets_evt1_response = MagicMock(
			status_code=200,
			json=lambda: {
				"markets": [
					{"ticker": "EVT1-T10"},
					{"ticker": "EVT1-T20"},
				],
				"cursor": "",
			},
		)
		markets_evt2_response = MagicMock(
			status_code=200,
			json=lambda: {
				"markets": [{"ticker": "EVT2-T30"}],
				"cursor": "",
			},
		)

		mock_client = AsyncMock()
		mock_client.get = AsyncMock(side_effect=[
			events_response,
			markets_evt1_response,
			markets_evt2_response,
		])

		tickers, reliable = asyncio.run(fetch_active_tickers_for_series(mock_client, "SERIES_A"))
		assert sorted(tickers) == ["EVT1-T10", "EVT1-T20", "EVT2-T30"]
		assert reliable is True

	def test_handles_api_error_returns_empty(self):
		"""Should return ([], False) gracefully when the API call fails."""
		from edge_catcher.monitors.recovery import fetch_active_tickers_for_series

		mock_client = AsyncMock()
		mock_client.get = AsyncMock(side_effect=Exception("network error"))

		tickers, reliable = asyncio.run(fetch_active_tickers_for_series(mock_client, "SERIES_A"))
		assert tickers == []
		assert reliable is False

	def test_handles_non_200_status_returns_empty(self):
		"""Should return ([], False) when events endpoint returns non-200."""
		from edge_catcher.monitors.recovery import fetch_active_tickers_for_series

		mock_client = AsyncMock()
		mock_client.get = AsyncMock(return_value=MagicMock(
			status_code=500,
			json=lambda: {},
		))

		tickers, reliable = asyncio.run(fetch_active_tickers_for_series(mock_client, "SERIES_A"))
		assert tickers == []
		assert reliable is False

	def test_partial_market_error_returns_unreliable(self):
		"""When events succeeds but one market call fails, reliable should be False."""
		from edge_catcher.monitors.recovery import fetch_active_tickers_for_series

		events_response = MagicMock(
			status_code=200,
			json=lambda: {"events": [{"event_ticker": "EVT1"}, {"event_ticker": "EVT2"}]},
		)
		markets_evt1_ok = MagicMock(
			status_code=200,
			json=lambda: {"markets": [{"ticker": "EVT1-T10"}], "cursor": ""},
		)
		markets_evt2_fail = MagicMock(
			status_code=429,
			json=lambda: {},
		)

		mock_client = AsyncMock()
		mock_client.get = AsyncMock(side_effect=[
			events_response, markets_evt1_ok, markets_evt2_fail,
		])

		tickers, reliable = asyncio.run(
			fetch_active_tickers_for_series(mock_client, "SERIES_A")
		)
		assert tickers == ["EVT1-T10"]
		assert reliable is False

	def test_paginates_markets_via_cursor(self):
		"""Should follow cursor to fetch additional pages of markets."""
		from edge_catcher.monitors.recovery import fetch_active_tickers_for_series

		events_response = MagicMock(
			status_code=200,
			json=lambda: {"events": [{"event_ticker": "EVT1"}]},
		)
		page1 = MagicMock(
			status_code=200,
			json=lambda: {
				"markets": [{"ticker": "EVT1-T10"}],
				"cursor": "page2token",
			},
		)
		page2 = MagicMock(
			status_code=200,
			json=lambda: {
				"markets": [{"ticker": "EVT1-T20"}],
				"cursor": "",
			},
		)

		mock_client = AsyncMock()
		mock_client.get = AsyncMock(side_effect=[events_response, page1, page2])

		tickers, reliable = asyncio.run(fetch_active_tickers_for_series(mock_client, "SERIES_A"))
		assert sorted(tickers) == ["EVT1-T10", "EVT1-T20"]
		assert reliable is True


class TestFetchOrderbookSnapshot:
	def test_returns_snapshot_with_correct_levels(self):
		"""Should parse yes/no levels from orderbook_fp response."""
		from edge_catcher.monitors.recovery import fetch_orderbook_snapshot

		mock_client = AsyncMock()
		mock_client.get = AsyncMock(return_value=MagicMock(
			status_code=200,
			json=lambda: {
				"orderbook_fp": {
					"yes_dollars": [[0.55, 10], [0.60, 5]],
					"no_dollars": [[0.45, 8]],
				}
			},
		))

		result = asyncio.run(fetch_orderbook_snapshot(mock_client, "EVT1-T10"))
		assert result is not None
		assert result.yes_levels == [(0.55, 10), (0.60, 5)]
		assert result.no_levels == [(0.45, 8)]

	def test_returns_none_on_api_error(self):
		"""Should return None when the API call raises."""
		from edge_catcher.monitors.recovery import fetch_orderbook_snapshot

		mock_client = AsyncMock()
		mock_client.get = AsyncMock(side_effect=Exception("timeout"))

		result = asyncio.run(fetch_orderbook_snapshot(mock_client, "EVT1-T10"))
		assert result is None

	def test_returns_none_on_non_200_status(self):
		"""Should return None for non-200 responses (except 429 which retries)."""
		from edge_catcher.monitors.recovery import fetch_orderbook_snapshot

		mock_client = AsyncMock()
		mock_client.get = AsyncMock(return_value=MagicMock(
			status_code=404,
			json=lambda: {},
		))

		result = asyncio.run(fetch_orderbook_snapshot(mock_client, "EVT1-T10"))
		assert result is None

	def test_retries_on_429(self):
		"""Should retry once on 429 and succeed on second attempt."""
		from edge_catcher.monitors.recovery import fetch_orderbook_snapshot

		rate_limit = MagicMock(
			status_code=429,
			json=lambda: {},
		)
		success = MagicMock(
			status_code=200,
			json=lambda: {
				"orderbook_fp": {
					"yes_dollars": [[0.50, 3]],
					"no_dollars": [[0.50, 2]],
				}
			},
		)
		mock_client = AsyncMock()
		mock_client.get = AsyncMock(side_effect=[rate_limit, success])

		result = asyncio.run(fetch_orderbook_snapshot(mock_client, "EVT1-T10"))
		assert result is not None
		assert result.yes_levels == [(0.50, 3)]


class TestCheckMarketResult:
	def test_returns_result_string(self):
		"""Should return the result field from the market response."""
		from edge_catcher.monitors.recovery import check_market_result

		mock_client = AsyncMock()
		mock_client.get = AsyncMock(return_value=MagicMock(
			status_code=200,
			json=lambda: {"market": {"result": "yes"}},
		))

		result = asyncio.run(check_market_result(mock_client, "EVT1-T10"))
		assert result == "yes"

	def test_returns_none_when_no_result(self):
		"""Should return None if the market has no result field."""
		from edge_catcher.monitors.recovery import check_market_result

		mock_client = AsyncMock()
		mock_client.get = AsyncMock(return_value=MagicMock(
			status_code=200,
			json=lambda: {"market": {}},
		))

		result = asyncio.run(check_market_result(mock_client, "EVT1-T10"))
		assert result is None

	def test_handles_api_error_returns_none(self):
		"""Should return None on exception."""
		from edge_catcher.monitors.recovery import check_market_result

		mock_client = AsyncMock()
		mock_client.get = AsyncMock(side_effect=Exception("network error"))

		result = asyncio.run(check_market_result(mock_client, "EVT1-T10"))
		assert result is None

	def test_retries_on_429_up_to_3_times(self):
		"""Should retry up to 3 times on 429, returning result on eventual success."""
		from edge_catcher.monitors.recovery import check_market_result

		rate_limit = MagicMock(status_code=429, json=lambda: {})
		success = MagicMock(
			status_code=200,
			json=lambda: {"market": {"result": "no"}},
		)
		mock_client = AsyncMock()
		mock_client.get = AsyncMock(side_effect=[rate_limit, rate_limit, success])

		result = asyncio.run(check_market_result(mock_client, "EVT1-T10"))
		assert result == "no"

	def test_returns_none_after_3_429s(self):
		"""Should return None if all 3 retry attempts get 429."""
		from edge_catcher.monitors.recovery import check_market_result

		rate_limit = MagicMock(status_code=429, json=lambda: {})
		mock_client = AsyncMock()
		mock_client.get = AsyncMock(side_effect=[rate_limit, rate_limit, rate_limit])

		result = asyncio.run(check_market_result(mock_client, "EVT1-T10"))
		assert result is None


class TestFetchMarketMeta:
	def test_returns_expected_fields(self):
		"""Should extract expiration_time, status, result, event_ticker."""
		from edge_catcher.monitors.recovery import fetch_market_meta

		mock_client = AsyncMock()
		mock_client.get = AsyncMock(return_value=MagicMock(
			status_code=200,
			json=lambda: {
				"market": {
					"expiration_time": "2026-04-10T12:00:00Z",
					"status": "open",
					"result": None,
					"event_ticker": "EVT1",
					"other_field": "ignored",
				}
			},
		))

		result = asyncio.run(fetch_market_meta(mock_client, "EVT1-T10"))
		assert result["expiration_time"] == "2026-04-10T12:00:00Z"
		assert result["status"] == "open"
		assert result["result"] is None
		assert result["event_ticker"] == "EVT1"
		assert "other_field" not in result

	def test_returns_empty_dict_on_error(self):
		"""Should return {} on any error."""
		from edge_catcher.monitors.recovery import fetch_market_meta

		mock_client = AsyncMock()
		mock_client.get = AsyncMock(side_effect=Exception("timeout"))

		result = asyncio.run(fetch_market_meta(mock_client, "EVT1-T10"))
		assert result == {}


class TestRunRecovery:
	def test_registers_tickers_and_seeds_orderbooks(self):
		"""run_recovery should register tickers and seed their orderbooks."""
		from edge_catcher.monitors.recovery import run_recovery
		from edge_catcher.monitors.market_state import MarketState

		market_state = MarketState()
		active_series = ["SERIES_A"]

		events_response = MagicMock(
			status_code=200,
			json=lambda: {"events": [{"event_ticker": "EVT1"}]},
		)
		markets_response = MagicMock(
			status_code=200,
			json=lambda: {"markets": [{"ticker": "EVT1-T10"}], "cursor": ""},
		)
		meta_response = MagicMock(
			status_code=200,
			json=lambda: {
				"market": {
					"expiration_time": "2026-04-10T12:00:00Z",
					"status": "open",
					"result": None,
					"event_ticker": "EVT1",
				}
			},
		)
		orderbook_response = MagicMock(
			status_code=200,
			json=lambda: {
				"orderbook_fp": {
					"yes_dollars": [[0.55, 5]],
					"no_dollars": [[0.45, 5]],
				}
			},
		)

		mock_client = AsyncMock()
		mock_client.get = AsyncMock(side_effect=[
			events_response,
			markets_response,
			meta_response,
			orderbook_response,
		])

		asyncio.run(run_recovery(mock_client, market_state, active_series))

		# Ticker should be registered
		assert market_state.get_price_history("EVT1-T10") is not None
		# Orderbook should be seeded
		ob = market_state.get_orderbook("EVT1-T10")
		assert ob is not None
		assert ob.yes_levels == [(0.55, 5)]
