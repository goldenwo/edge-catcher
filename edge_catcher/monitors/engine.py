"""Paper trading engine — WS loop, strategy router, signal processing pipeline."""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Any

import httpx
import websockets

from edge_catcher.monitors.auth import KALSHI_WS_URL, WS_PATH, make_auth_headers
from edge_catcher.monitors.discovery import (
	discover_strategies,
	get_enabled_strategies,
	load_config,
)
from edge_catcher.monitors.sizing import resolve_fill
from edge_catcher.monitors.market_state import (
	MarketState,
	OrderbookSnapshot,
	TickContext,
	derive_event_ticker,
)
from edge_catcher.monitors.notifications import notify
from edge_catcher.monitors.recovery import (
	check_market_result,
	fetch_active_tickers_for_series,
	fetch_orderbook_snapshot,
	run_recovery,
)
from edge_catcher.monitors.strategy_base import PaperStrategy, Signal
from edge_catcher.monitors.trade_store import TradeStore

log = logging.getLogger(__name__)


def _pnl_label(pnl: int | None) -> tuple[str, str]:
	"""Return (outcome, pnl_str) for a trade's PnL value.

	outcome: 'WIN', 'LOSS', or 'SCRATCH'
	pnl_str: '+7¢', '-3¢', '0¢', or '?' if None
	"""
	if pnl is None:
		return "?", "?"
	if pnl > 0:
		return "WIN", f"{pnl:+d}¢"
	if pnl < 0:
		return "LOSS", f"{pnl:+d}¢"
	return "SCRATCH", "0¢"


# ---------------------------------------------------------------------------
# Part 1: Synchronous signal pipeline (testable)
# ---------------------------------------------------------------------------

def process_tick(
	ctx: TickContext,
	strategies: list[PaperStrategy],
	store: TradeStore,
	config: dict,
) -> None:
	"""Run every enabled strategy against the current tick context.

	For each strategy:
	  - Call on_tick → get signals
	  - Process enter/exit signals with exception isolation per-signal

	This is the synchronous, testable core of the engine.
	"""
	for strategy in strategies:
		try:
			signals = strategy.on_tick(ctx)
		except Exception:
			log.exception("Strategy %s raised on on_tick for %s", strategy.name, ctx.ticker)
			continue

		for signal in signals:
			try:
				_handle_signal(signal, ctx, store, config)
			except Exception:
				log.exception(
					"Error handling %s signal from %s for %s",
					signal.action, signal.strategy, signal.ticker,
				)


def _handle_signal(
	signal: Signal,
	ctx: TickContext,
	store: TradeStore,
	config: dict,
) -> None:
	"""Dispatch a single signal — enter or exit."""
	if signal.action == "enter":
		_handle_enter(signal, ctx, store, config)
	elif signal.action == "exit":
		_handle_exit(signal, ctx, store)
	else:
		log.warning("Unknown signal action '%s' from %s", signal.action, signal.strategy)


def _handle_enter(
	signal: Signal,
	ctx: TickContext,
	store: TradeStore,
	config: dict,
) -> None:
	"""Process an entry signal: resolve sizing, walk orderbook, record trade."""
	# Raw tick price for the side: yes pays yes_ask, no pays no_ask
	entry_price = ctx.yes_ask if signal.side == "yes" else ctx.no_ask

	fill = resolve_fill(config, entry_price, signal.side, ctx.orderbook)

	if fill is None:
		log.info(
			"No fill for %s %s %s (entry=%dc) — skipping",
			signal.strategy, signal.side, signal.ticker, entry_price,
		)
		return

	side_levels = (
		ctx.orderbook.yes_levels if signal.side == "yes"
		else ctx.orderbook.no_levels
	)

	trade_id = store.record_trade(
		ticker=signal.ticker,
		entry_price=entry_price,
		strategy=signal.strategy,
		side=signal.side,
		series_ticker=signal.series,
		intended_size=fill.intended_size,
		fill_size=fill.fill_size,
		blended_entry=fill.blended_price_cents,
		book_depth=ctx.orderbook.depth,
		fill_pct=fill.fill_pct,
		slippage_cents=fill.slippage_cents,
		book_snapshot=json.dumps(side_levels),
	)

	msg = (
		f"ENTER {signal.strategy} {signal.side} {signal.ticker} "
		f"@ {entry_price}c (blended {fill.blended_price_cents}c, "
		f"fill {fill.fill_size}/{fill.intended_size}, slip {fill.slippage_cents}c) "
		f"— {signal.reason} [id={trade_id}]"
	)
	log.info(msg)
	notify(msg)


def _handle_exit(
	signal: Signal,
	ctx: TickContext,
	store: TradeStore,
) -> None:
	"""Process an exit signal: compute exit price, close trade."""
	if signal.trade_id is None:
		log.warning(
			"Exit signal from %s for %s has no trade_id — skipping",
			signal.strategy, signal.ticker,
		)
		return

	# Selling hits the bid, not the ask
	exit_price = ctx.yes_bid if signal.side == "yes" else ctx.no_bid

	store.exit_trade(signal.trade_id, exit_price)

	# Read back PnL from DB (includes fee deduction)
	exited = store.get_trade_by_id(signal.trade_id)
	pnl = exited.get("pnl_cents") if exited else None
	_, pnl_str = _pnl_label(pnl)
	pnl_part = f" PnL={pnl_str}" if pnl is not None else ""

	msg = (
		f"EXIT {signal.strategy} {signal.side} {signal.ticker} "
		f"@ {exit_price}c{pnl_part} — {signal.reason} [id={signal.trade_id}]"
	)
	log.info(msg)
	notify(msg)


# ---------------------------------------------------------------------------
# Part 2: Async engine and background tasks
# ---------------------------------------------------------------------------

async def _settlement_poller(
	store: TradeStore,
	client: httpx.AsyncClient,
	strategies: list[PaperStrategy],
	pending_states: dict[str, dict],
	interval: int = 60,
) -> None:
	"""Periodically check open trades for settlement."""
	strat_by_name = {s.name: s for s in strategies}
	while True:
		await asyncio.sleep(interval)
		try:
			open_trades = store.get_open_trades()
			for trade in open_trades:
				result = await check_market_result(client, trade["ticker"])
				if result is not None:
					store.settle_trade(trade["id"], result)
					# Read back PnL from DB (settle_trade computes it including fees)
					settled = store.get_trade_by_id(trade["id"])
					pnl = settled.get("pnl_cents") if settled else None
					outcome, pnl_str = _pnl_label(pnl)
					msg = (
						f"SETTLED {trade['strategy']} {trade.get('side', '?')} {trade['ticker']} "
						f"→ {outcome} {pnl_str} [id={trade['id']}]"
					)
					log.info(msg)
					notify(msg)
					# Call on_settle on matching strategy
					strat = strat_by_name.get(trade["strategy"])
					if strat is not None:
						state = pending_states.get(strat.name, {})
						try:
							strat.on_settle(trade, state)
						except Exception:
							log.exception("on_settle failed for %s", strat.name)
						# Flush state immediately after settlement
						store.save_state(strat.name, state)
		except Exception:
			log.exception("Settlement poller error")


async def _summary_logger(
	store: TradeStore,
	interval: int = 300,
) -> None:
	"""Periodically log open trade count."""
	while True:
		await asyncio.sleep(interval)
		try:
			count = len(store.get_open_trades())
			log.info("Open trades: %d", count)
		except Exception:
			log.exception("Summary logger error")


async def _state_flusher(
	store: TradeStore,
	strategies: list[PaperStrategy],
	pending_states: dict[str, dict],
	dirty: set[str],
	interval: int = 5,
) -> None:
	"""Periodically flush dirty strategy state to SQLite.

	Only writes strategies that have been marked dirty since last flush.
	Reduces SD card writes on Pi.
	"""
	while True:
		await asyncio.sleep(interval)
		try:
			to_flush = dirty.copy()
			dirty.clear()
			for name in to_flush:
				state = pending_states.get(name)
				if state is not None:
					store.save_state(name, state)
		except Exception:
			log.exception("State flusher error")


async def _ticker_refresh(
	client: httpx.AsyncClient,
	market_state: MarketState,
	active_series: list[str],
	ws_ref: list,
	config: dict | None = None,
	interval: int = 300,
) -> None:
	"""Periodically re-fetch tickers and subscribe new ones on WS."""
	while True:
		await asyncio.sleep(interval)
		try:
			new_tickers: list[str] = []
			for series in active_series:
				tickers = await fetch_active_tickers_for_series(client, series)
				fresh_set = set(tickers)

				# Register new tickers
				for ticker in tickers:
					if market_state.get_price_history(ticker) is None:
						market_state.register_ticker(ticker)
						snapshot = await fetch_orderbook_snapshot(client, ticker)
						if snapshot is not None:
							market_state.seed_orderbook(ticker, snapshot)
						new_tickers.append(ticker)

				# Purge stale tickers for this series
				for existing in market_state.all_tickers():
					if existing.startswith(series) and existing not in fresh_set:
						market_state.unregister_ticker(existing)

			if new_tickers and ws_ref and ws_ref[0] is not None:
				try:
					ws_channels = (config or {}).get("ws", {}).get("channels", ["ticker", "orderbook_delta"])
					sub_msg = {
						"id": 2,
						"cmd": "subscribe",
						"params": {
							"channels": ws_channels,
							"market_tickers": new_tickers,
						},
					}
					await ws_ref[0].send(json.dumps(sub_msg))
					log.info("Subscribed %d new tickers", len(new_tickers))
				except Exception:
					log.exception("Failed to subscribe new tickers on WS")
		except Exception:
			log.exception("Ticker refresh error")


def _collect_active_series(config: dict) -> list[str]:
	"""Collect all unique series from enabled strategies in config."""
	series: set[str] = set()
	for _name, scfg in config.get("strategies", {}).items():
		if scfg.get("enabled", False):
			for s in scfg.get("series", []):
				series.add(s)
	return sorted(series)


def _series_for_strategy(config: dict, strategy_name: str) -> set[str]:
	"""Get the configured series for a strategy."""
	scfg = config.get("strategies", {}).get(strategy_name, {})
	return set(scfg.get("series", []))


async def run_engine(config_path: Path) -> None:
	"""Main engine loop — connect WS, dispatch ticks, manage background tasks.

	Args:
		config_path: Path to the YAML config file.
	"""
	# 1. Load config, init TradeStore, init MarketState
	config = load_config(config_path)
	db_path = Path(config.get("db_path", "data/paper_trades.db"))
	ws_cfg = config.get("ws", {})
	recovery_cfg = config.get("recovery", {})
	reconnect_delay = ws_cfg.get("reconnect_delay", 30)
	ping_interval = ws_cfg.get("ping_interval", 20)
	price_history_limit = ws_cfg.get("price_history_limit", 100)
	state_flush_interval = recovery_cfg.get("state_flush_interval", 5)
	store = TradeStore(db_path)
	market_state = MarketState(limit=price_history_limit)

	# 2. Discover and filter strategies
	all_strategies = discover_strategies()
	strategies = get_enabled_strategies(config, all_strategies)
	if not strategies:
		log.error("No enabled strategies found — exiting")
		store.close()
		return

	log.info("Enabled strategies: %s", [s.name for s in strategies])

	# 3. Load persisted states, determine active series
	all_states = store.load_all_states()
	pending_states: dict[str, dict] = {}
	for strat in strategies:
		pending_states[strat.name] = all_states.get(strat.name, {})

	active_series = _collect_active_series(config)
	log.info("Active series: %s", active_series)

	# 4. Run recovery
	async with httpx.AsyncClient(timeout=30.0) as client:
		await run_recovery(client, market_state, active_series)

		# 5. Call on_startup for each strategy
		all_open = store.get_open_trades()
		for strat in strategies:
			strat_open = [t for t in all_open if t["strategy"] == strat.name]
			try:
				strat.on_startup({
					"open_positions": strat_open,
					"active_tickers": market_state.all_tickers(),
					"state": pending_states[strat.name],
				})
			except Exception:
				log.exception("on_startup failed for %s", strat.name)

		# 6. Start background tasks
		ws_ref: list[Any] = [None]
		dirty_strategies: set[str] = set()
		tasks = [
			asyncio.create_task(
				_settlement_poller(store, client, strategies, pending_states),
				name="settlement_poller",
			),
			asyncio.create_task(
				_summary_logger(store),
				name="summary_logger",
			),
			asyncio.create_task(
				_state_flusher(store, strategies, pending_states, dirty_strategies, interval=state_flush_interval),
				name="state_flusher",
			),
			asyncio.create_task(
				_ticker_refresh(client, market_state, active_series, ws_ref, config=config),
				name="ticker_refresh",
			),
		]

		# Build strategy lookup by series
		strat_by_series: dict[str, list[PaperStrategy]] = {}
		for strat in strategies:
			strat_series = _series_for_strategy(config, strat.name)
			for s in strat_series:
				strat_by_series.setdefault(s, []).append(strat)

		try:
			# 7. WS loop with reconnect
			while True:
				try:
					await _ws_loop(
						config, market_state, store, strategies,
						strat_by_series, pending_states, active_series,
						client, ws_ref, dirty_strategies,
					)
				except (
					websockets.ConnectionClosed,
					websockets.InvalidStatusCode,
					ConnectionError,
					OSError,
				) as exc:
					log.warning("WS disconnected: %s — reconnecting in %ds", exc, reconnect_delay)
					await asyncio.sleep(reconnect_delay)
					market_state.clear()
					await run_recovery(client, market_state, active_series)
				except Exception:
					log.exception("Unexpected WS error — reconnecting in %ds", reconnect_delay)
					await asyncio.sleep(reconnect_delay)
					market_state.clear()
					await run_recovery(client, market_state, active_series)

		finally:
			# 8. Graceful shutdown
			log.info("Shutting down engine")
			for strat in strategies:
				state = pending_states.get(strat.name)
				if state is not None:
					store.save_state(strat.name, state)
			for task in tasks:
				task.cancel()
			await asyncio.gather(*tasks, return_exceptions=True)
			store.close()


async def _ws_loop(
	config: dict,
	market_state: MarketState,
	store: TradeStore,
	strategies: list[PaperStrategy],
	strat_by_series: dict[str, list[PaperStrategy]],
	pending_states: dict[str, dict],
	active_series: list[str],
	client: httpx.AsyncClient,
	ws_ref: list,
	dirty: set[str],
) -> None:
	"""Single WS connection lifecycle — connect, subscribe, process messages."""
	headers = make_auth_headers()

	# Use tickers already registered in market_state (seeded by recovery)
	all_tickers = market_state.all_tickers()

	async with websockets.connect(
		KALSHI_WS_URL,
		additional_headers=headers,
		ping_interval=config.get("ws", {}).get("ping_interval", 20),
		ping_timeout=10,
	) as ws:
		ws_ref[0] = ws

		# Subscribe
		ws_channels = config.get("ws", {}).get("channels", ["ticker", "orderbook_delta"])
		if all_tickers:
			sub_msg = {
				"id": 1,
				"cmd": "subscribe",
				"params": {
					"channels": ws_channels,
					"market_tickers": all_tickers,
				},
			}
			await ws.send(json.dumps(sub_msg))
			log.info("Subscribed to %d tickers (channels: %s)", len(all_tickers), ws_channels)

		# Process messages
		async for raw in ws:
			try:
				msg = json.loads(raw)
			except json.JSONDecodeError:
				log.warning("Non-JSON WS message: %s", raw[:200])
				continue

			msg_type = msg.get("type")

			if msg_type == "orderbook_delta":
				try:
					_handle_orderbook_delta(market_state, msg)
				except Exception:
					log.exception("Error handling orderbook_delta")

			elif msg_type == "ticker":
				try:
					_handle_ticker_msg(
						msg, config, market_state, store,
						strategies, strat_by_series, pending_states, dirty,
					)
				except Exception:
					log.exception("Error handling ticker msg")

			elif msg_type == "trade":
				try:
					_handle_trade_msg(
						msg, config, market_state, store,
						strategies, strat_by_series, pending_states, dirty,
					)
				except Exception:
					log.exception("Error handling trade msg")


def _handle_trade_msg(
	msg: dict,
	config: dict,
	market_state: MarketState,
	store: TradeStore,
	strategies: list[PaperStrategy],
	strat_by_series: dict[str, list[PaperStrategy]],
	pending_states: dict[str, dict],
	dirty: set[str],
) -> None:
	"""Handle a trade WS message — routes to flow-sensitive strategies."""
	data = msg.get("msg", {})
	ticker = data.get("market_ticker", "")
	if not ticker:
		return

	# Skip if ticker not registered (trade can arrive before recovery seeds it)
	if market_state.get_price_history(ticker) is None:
		return

	yes_price_raw = data.get("yes_price")
	if yes_price_raw is None:
		return

	try:
		yes_ask_cents = int(round(float(yes_price_raw) * 100))
	except (TypeError, ValueError):
		return
	if not (1 <= yes_ask_cents <= 99):
		return

	no_price_raw = data.get("no_price")
	try:
		no_ask_cents = int(round(float(no_price_raw) * 100)) if no_price_raw is not None else (100 - yes_ask_cents)
	except (TypeError, ValueError):
		no_ask_cents = 100 - yes_ask_cents

	taker_side = data.get("taker_side")
	count_raw = data.get("count")
	try:
		trade_count = int(float(count_raw)) if count_raw is not None else None
	except (TypeError, ValueError):
		trade_count = None

	yes_bid_cents = 100 - no_ask_cents
	no_bid_cents = 100 - yes_ask_cents

	# Update market state so price history reflects trades
	is_first = market_state.update_price(ticker, yes_ask_cents)
	event_ticker = derive_event_ticker(ticker)
	orderbook = market_state.get_orderbook(ticker) or OrderbookSnapshot([], [])
	history = list(market_state.get_price_history(ticker) or [])

	matched_series = [s for s in strat_by_series if ticker.startswith(s)]

	for series in matched_series:
		for strat in strat_by_series.get(series, []):
			open_positions = store.get_open_trades_for(strat.name, ticker)
			ctx = TickContext(
				ticker=ticker,
				event_ticker=event_ticker,
				yes_bid=yes_bid_cents,
				yes_ask=yes_ask_cents,
				no_bid=no_bid_cents,
				no_ask=no_ask_cents,
				orderbook=orderbook,
				price_history=history,
				open_positions=open_positions,
				persisted_state=pending_states.get(strat.name, {}),
				market_metadata=market_state.get_metadata(ticker),
				series=series,
				is_first_observation=is_first,
				taker_side=taker_side,
				trade_count=trade_count,
			)
			process_tick(ctx, [strat], store, config)
			dirty.add(strat.name)


def _handle_orderbook_delta(market_state: MarketState, msg: dict) -> None:
	"""Apply an orderbook delta message to market state."""
	ticker = msg.get("msg", {}).get("market_ticker", "")
	if not ticker:
		return

	data = msg.get("msg", {})
	for side in ("yes", "no"):
		for price_str, delta in data.get(side, []):
			try:
				market_state.apply_orderbook_delta(
					ticker, side, float(price_str), int(delta),
				)
			except Exception:
				log.exception("Error applying orderbook delta for %s", ticker)


def _handle_ticker_msg(
	msg: dict,
	config: dict,
	market_state: MarketState,
	store: TradeStore,
	strategies: list[PaperStrategy],
	strat_by_series: dict[str, list[PaperStrategy]],
	pending_states: dict[str, dict],
	dirty: set[str],
) -> None:
	"""Handle a ticker (price update) WS message."""
	data = msg.get("msg", {})
	ticker = data.get("market_ticker", "")
	if not ticker:
		return

	# Kalshi WS sends prices as 'yes_ask_dollars' (string) or 'yes_ask' (legacy float)
	yes_ask_raw = data.get("yes_ask_dollars") or data.get("yes_ask")
	if yes_ask_raw is None:
		return

	# Validate price range
	try:
		yes_ask_cents = int(round(float(yes_ask_raw) * 100))
	except (TypeError, ValueError):
		return
	if not (1 <= yes_ask_cents <= 99):
		return

	# Read yes_bid separately (may differ from yes_ask)
	yes_bid_raw = data.get("yes_bid_dollars") or data.get("yes_bid")
	try:
		yes_bid_cents = int(round(float(yes_bid_raw) * 100)) if yes_bid_raw is not None else yes_ask_cents
	except (TypeError, ValueError):
		yes_bid_cents = yes_ask_cents

	# Update market state
	is_first = market_state.update_price(ticker, yes_ask_cents)
	event_ticker = derive_event_ticker(ticker)
	orderbook = market_state.get_orderbook(ticker) or OrderbookSnapshot([], [])
	history = list(market_state.get_price_history(ticker) or [])

	# Determine which series this ticker belongs to
	# Convention: series ticker is the prefix before the date segment
	# We match against configured series
	matched_series: list[str] = []
	for series in strat_by_series:
		if ticker.startswith(series):
			matched_series.append(series)

	for series in matched_series:
		series_strategies = strat_by_series.get(series, [])
		if not series_strategies:
			continue

		# Build TickContext for this series + strategy set
		for strat in series_strategies:
			open_positions = store.get_open_trades_for(strat.name, ticker)
			ctx = TickContext(
				ticker=ticker,
				event_ticker=event_ticker,
				yes_bid=yes_bid_cents,
				yes_ask=yes_ask_cents,
				no_bid=100 - yes_ask_cents,
				no_ask=100 - yes_bid_cents,
				orderbook=orderbook,
				price_history=history,
				open_positions=open_positions,
				persisted_state=pending_states.get(strat.name, {}),
				market_metadata=market_state.get_metadata(ticker),
				series=series,
				is_first_observation=is_first,
			)
			process_tick(ctx, [strat], store, config)
			dirty.add(strat.name)
