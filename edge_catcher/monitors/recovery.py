"""REST recovery module for the paper trading framework.

Fetches active tickers, orderbook snapshots, and market metadata from
Kalshi's REST API.  Used on startup and after WebSocket reconnects.
"""

import asyncio
import logging
from typing import Optional

from edge_catcher.monitors.auth import KALSHI_REST_BASE
from edge_catcher.monitors.market_state import (
	MarketState,
	OrderbookSnapshot,
	_is_tradeable_cents,
)

log = logging.getLogger(__name__)


async def fetch_active_tickers_for_series(
	client,
	series_ticker: str,
) -> tuple[list[str], bool]:
	"""Fetch all active market tickers for a given series.

	Calls GET /events?series_ticker=...&status=open, then for each event
	calls GET /markets?event_ticker=..., paginating via cursor.

	Args:
		client:        httpx.AsyncClient (or compatible mock).
		series_ticker: The series to query, e.g. "KXBTC15M".

	Returns:
		(tickers, reliable) — *reliable* is False when the result may be
		incomplete due to rate-limiting or errors (caller should not purge
		existing tickers in that case).
	"""
	try:
		resp = await client.get(
			f"{KALSHI_REST_BASE}/events",
			params={"series_ticker": series_ticker, "status": "open"},
		)
		if resp.status_code != 200:
			log.warning(
				"fetch_active_tickers_for_series: events returned %s for %s",
				resp.status_code,
				series_ticker,
			)
			return [], False

		events = resp.json().get("events", [])
		tickers: list[str] = []
		hit_error = False

		for event in events:
			event_ticker = event.get("event_ticker", "")
			cursor: str = ""
			while True:
				params: dict = {"event_ticker": event_ticker}
				if cursor:
					params["cursor"] = cursor

				mresp = await client.get(
					f"{KALSHI_REST_BASE}/markets",
					params=params,
				)
				if mresp.status_code != 200:
					log.warning(
						"fetch_active_tickers_for_series: markets returned %s for %s",
						mresp.status_code,
						event_ticker,
					)
					hit_error = True
					break

				data = mresp.json()
				for market in data.get("markets", []):
					ticker = market.get("ticker")
					if ticker:
						tickers.append(ticker)

				cursor = data.get("cursor", "")
				if not cursor:
					break

		return tickers, not hit_error

	except Exception:
		log.exception("fetch_active_tickers_for_series failed for %s", series_ticker)
		return [], False


async def fetch_orderbook_snapshot(
	client,
	ticker: str,
) -> Optional[OrderbookSnapshot]:
	"""Fetch a fresh orderbook snapshot for a single market ticker.

	Parses ``orderbook_fp.yes_dollars`` and ``no_dollars`` as
	``list[tuple[float, int]]``.  Retries once on HTTP 429.

	Args:
		client: httpx.AsyncClient (or compatible mock).
		ticker: Market ticker string.

	Returns:
		OrderbookSnapshot, or None on error.
	"""
	for attempt in range(2):
		try:
			resp = await client.get(f"{KALSHI_REST_BASE}/markets/{ticker}/orderbook")

			if resp.status_code == 429:
				if attempt == 0:
					log.warning("fetch_orderbook_snapshot: 429 for %s, retrying", ticker)
					await asyncio.sleep(1)
					continue
				log.warning("fetch_orderbook_snapshot: 429 again for %s, giving up", ticker)
				return None

			if resp.status_code != 200:
				log.warning(
					"fetch_orderbook_snapshot: status %s for %s",
					resp.status_code,
					ticker,
				)
				return None

			data = resp.json()
			ob_fp = data.get("orderbook_fp", {})
			# Kalshi REST returns prices and quantities as strings (e.g. ["0.1300", "685.00"])
			# Kalshi markets trade only at integer cents (1¢–99¢); sub-cent
			# ghost levels (0.1¢, 0.7¢, 0.9¢, …) have been observed in the
			# REST /orderbook response for 15m crypto series but are never
			# tradeable. Drop them at ingest so downstream code (walk_book,
			# stale-fallback detection) doesn't see a "best price" of 0c.
			# Symmetric upper bound guards against a hypothetical >=100c
			# level. We require the price to be an integer number of cents
			# (tolerance 1e-3) — naive round() alone lets 0.7¢ and 0.9¢
			# through because 0.007*100 → 0.70000000000001 rounds to 1.
			yes_levels = [
				(float(p), int(float(q)))
				for p, q in ob_fp.get("yes_dollars", [])
				if _is_tradeable_cents(float(p))
			]
			no_levels = [
				(float(p), int(float(q)))
				for p, q in ob_fp.get("no_dollars", [])
				if _is_tradeable_cents(float(p))
			]

			return OrderbookSnapshot(
				yes_levels=yes_levels,  # type: ignore[arg-type]
				no_levels=no_levels,  # type: ignore[arg-type]
			)

		except Exception:
			log.exception("fetch_orderbook_snapshot failed for %s", ticker)
			return None

	return None  # unreachable, but satisfies type checker


async def fetch_market_meta(client, ticker: str) -> dict:
	"""Fetch metadata for a single market ticker.

	Extracts: ``expiration_time``, ``status``, ``result``, ``event_ticker``.

	Args:
		client: httpx.AsyncClient (or compatible mock).
		ticker: Market ticker string.

	Returns:
		Dict with extracted fields, or {} on error.
	"""
	try:
		resp = await client.get(f"{KALSHI_REST_BASE}/markets/{ticker}")
		if resp.status_code != 200:
			log.warning("fetch_market_meta: status %s for %s", resp.status_code, ticker)
			return {}

		market = resp.json().get("market", {})
		return {
			"expiration_time": market.get("expiration_time"),
			"status": market.get("status"),
			"result": market.get("result"),
			"event_ticker": market.get("event_ticker"),
		}

	except Exception:
		log.exception("fetch_market_meta failed for %s", ticker)
		return {}


async def check_market_result(client, ticker: str) -> Optional[str]:
	"""Check the settled result of a market.

	Retries up to 3 times on HTTP 429.

	Args:
		client: httpx.AsyncClient (or compatible mock).
		ticker: Market ticker string.

	Returns:
		'yes', 'no', or None (unsettled / error).
	"""
	for attempt in range(3):
		try:
			resp = await client.get(f"{KALSHI_REST_BASE}/markets/{ticker}")

			if resp.status_code == 429:
				if attempt < 2:
					log.warning(
						"check_market_result: 429 for %s (attempt %d), retrying",
						ticker,
						attempt + 1,
					)
					await asyncio.sleep(1)
					continue
				log.warning("check_market_result: 429 exhausted for %s", ticker)
				return None

			if resp.status_code != 200:
				log.warning(
					"check_market_result: status %s for %s", resp.status_code, ticker
				)
				return None

			market = resp.json().get("market", {})
			return market.get("result")  # may be None if not settled

		except Exception:
			log.exception("check_market_result failed for %s", ticker)
			return None

	return None


async def run_recovery(
	client,
	market_state: MarketState,
	active_series: list[str],
) -> None:
	"""Run a full REST recovery sweep.

	For each series ticker: fetches active market tickers, fetches metadata
	for each ticker and registers it in market_state, then seeds the
	orderbook with a fresh snapshot.

	Args:
		client:        httpx.AsyncClient (or compatible mock).
		market_state:  MarketState instance to populate.
		active_series: List of series tickers to sweep.
	"""
	total = 0

	for i, series in enumerate(active_series):
		if i > 0:
			# Small delay between series to avoid Kalshi API rate limits (429)
			await asyncio.sleep(1.0)
		tickers, _reliable = await fetch_active_tickers_for_series(client, series)
		log.info("run_recovery: series %s → %d tickers", series, len(tickers))
		total += len(tickers)

		for ticker in tickers:
			meta = await fetch_market_meta(client, ticker)
			market_state.register_ticker(ticker, meta=meta)

			snapshot = await fetch_orderbook_snapshot(client, ticker)
			if snapshot is not None:
				market_state.seed_orderbook(ticker, snapshot)

	log.info("run_recovery: complete — %d total tickers across %d series", total, len(active_series))
