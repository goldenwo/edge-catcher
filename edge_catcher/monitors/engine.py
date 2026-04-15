"""Paper trading engine — WS loop, strategy router, signal processing pipeline."""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import threading
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional

import httpx
import websockets

from edge_catcher.monitors.auth import KALSHI_WS_URL, WS_PATH, make_auth_headers
from edge_catcher.monitors.capture.bundle import assemble_daily_bundle
from edge_catcher.monitors.capture.transport import (
	CaptureTransport,
	LocalTransport,
	R2Transport,
)
from edge_catcher.monitors.capture.writer import RawFrameWriter
from edge_catcher.monitors.discovery import (
	discover_strategies,
	get_enabled_strategies,
	load_config,
)
from edge_catcher.monitors.dispatch import (
	_format_close_message,
	_pnl_label,
	dispatch_message,
)
from edge_catcher.monitors.metrics import Metrics
from edge_catcher.monitors.market_state import MarketState
from edge_catcher.monitors.notifications import notify
from edge_catcher.monitors.recovery import (
	check_market_result,
	fetch_active_tickers_for_series,
	fetch_orderbook_snapshot,
	run_recovery,
)
from edge_catcher.monitors.strategy_base import PaperStrategy
from edge_catcher.monitors.trade_store import TradeStore

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Async engine and background tasks
#
# The synchronous signal pipeline (process_tick, _handle_signal, _handle_enter,
# _handle_exit) and the WS/trade handlers have moved to dispatch.py. This file
# now contains only the async lifecycle: WS loop, settlement poller, ticker
# refresh, and run_engine bootstrap.
# ---------------------------------------------------------------------------

async def _settlement_poller(
	store: TradeStore,
	client: httpx.AsyncClient,
	strategies: list[PaperStrategy],
	pending_states: dict[str, dict],
	metrics: Metrics | None = None,
	interval: int = 60,
	capture_writer: RawFrameWriter | None = None,
) -> None:
	"""Periodically check open trades for settlement.

	If `capture_writer` is provided, tees each settlement resolution to the
	capture pipeline as a `synthetic.settlement` event (tee point 4 of 4).
	The tee uses the SAME `now` as the store.settle_trade call so replay
	produces identical exit_time values to live.
	"""
	strat_by_name = {s.name: s for s in strategies}
	if metrics is None:
		metrics = Metrics()
	while True:
		await asyncio.sleep(interval)
		try:
			open_trades = store.get_open_trades()
			for trade in open_trades:
				result = await check_market_result(client, trade["ticker"])
				if result is not None:
					# Capture the clock ONCE per settled trade. The capture payload
					# and the store call share this `now` so that replay produces
					# byte-identical exit_time values.
					now = datetime.now(timezone.utc)
					if capture_writer is not None:
						# Tee point 4/4 — see capture/replay spec §6.1
						# `result` is 'yes' or 'no' (raw market outcome). The store
						# translates to 'won'/'lost' internally based on trade['side'].
						capture_writer.write_synthetic("settlement", {
							"strategy": trade["strategy"],
							"ticker": trade["ticker"],
							"side": trade.get("side"),
							"entry_time": trade.get("entry_time"),
							"result": result,
						})
					store.settle_trade(trade["id"], result, now=now)
					# Read back PnL from DB (settle_trade computes it including fees)
					settled = store.get_trade_by_id(trade["id"])
					# Branch settlement counters on DB 'status' (won/lost only),
					# NOT on _pnl_label's three-way outcome (which includes SCRATCH).
					status = settled.get("status") if settled else None
					if status == "won":
						metrics.inc("trades_settled_won")
					elif status == "lost":
						metrics.inc("trades_settled_lost")
					if settled is None:
						log.warning("SETTLE: trade id=%d not found post-settle_trade", trade["id"])
						continue
					pnl = settled.get("pnl_cents") or 0
					outcome, _ = _pnl_label(pnl)
					strat_obj = strat_by_name.get(trade["strategy"])
					bullet = strat_obj.emoji if strat_obj else "🔵"
					series = trade.get("series_ticker", "?")
					blended = settled.get("blended_entry") or 0
					effective_entry = blended if blended else (settled.get("entry_price") or 0)
					fill_size = settled.get("fill_size") or 0
					entry_fee = settled.get("entry_fee_cents") or 0
					# settle_trade subtracts only entry_fee (P*(1-P)=0 at 0/100)
					settlement_exit_price = settled.get("exit_price") or 0
					log_line, notify_line = _format_close_message(
						event="SETTLED",
						outcome=outcome,
						strategy=trade["strategy"],
						series=series,
						ticker=trade["ticker"],
						side=trade.get("side", "?"),
						fill_size=fill_size,
						effective_entry=effective_entry,
						exit_price=settlement_exit_price,
						pnl_cents=pnl,
						fee_cents=entry_fee,
						settled_result=result,
						trade_id=trade["id"],
						bullet=bullet,
					)
					log.info(log_line)
					notify(notify_line)
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
	metrics: Metrics | None = None,
	interval: int = 300,
) -> None:
	"""Periodically log open trade count and per-interval metrics snapshot.

	The unsupported-skip value is a persistent gauge (set at startup), so it
	stays non-zero across resets. Counters reset after each snapshot so the
	next interval reflects fresh activity.
	"""
	if metrics is None:
		metrics = Metrics()
	while True:
		await asyncio.sleep(interval)
		try:
			count = len(store.get_open_trades())
			snap = metrics.reset_and_snapshot()
			log.info(
				"Summary interval=%ds open=%d attempted=%d filled=%d "
				"stale_skipped=%d other_skipped=%d settled_won=%d "
				"settled_lost=%d unsupported=%d",
				interval,
				count,
				snap["entries_attempted"],
				snap["entries_filled"],
				snap["entries_skipped_stale"],
				snap["entries_skipped_other"],
				snap["trades_settled_won"],
				snap["trades_settled_lost"],
				snap["entries_skipped_unsupported"],
			)
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
	capture_writer: RawFrameWriter | None = None,
) -> None:
	"""Periodically re-fetch tickers and subscribe new ones on WS.

	When a new ticker is discovered, tees the initial orderbook to the
	capture pipeline as a `synthetic.ticker_discovered` event (tee point 3 of 4).
	"""
	while True:
		await asyncio.sleep(interval)
		try:
			new_tickers: list[str] = []
			for i, series in enumerate(active_series):
				if i > 0:
					await asyncio.sleep(1.0)
				tickers, reliable = await fetch_active_tickers_for_series(client, series)
				fresh_set = set(tickers)

				# Register new tickers
				for ticker in tickers:
					if market_state.get_price_history(ticker) is None:
						market_state.register_ticker(ticker)
						snapshot = await fetch_orderbook_snapshot(client, ticker)
						if snapshot is not None:
							market_state.seed_orderbook(ticker, snapshot)
							if capture_writer is not None:
								# Tee point 3/4 — see capture/replay spec §6.1
								capture_writer.write_synthetic("ticker_discovered", {
									"ticker": ticker,
									"yes_levels": snapshot.yes_levels,
									"no_levels": snapshot.no_levels,
								})
						new_tickers.append(ticker)

				# Purge stale tickers only when the API response was complete
				if reliable:
					for existing in market_state.all_tickers():
						if existing.startswith(series) and existing not in fresh_set:
							market_state.unregister_ticker(existing)
				else:
					log.warning("Skipping ticker purge for %s: API response unreliable (got %d partial tickers)", series, len(tickers))

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


def _make_rotation_callback(
	capture_dir: Path,
	repo_root: Path,
	db_path: Path,
	market_state: MarketState,
	transport: Optional[CaptureTransport],
):
	"""Build the rotation_callback closure that RawFrameWriter fires on
	midnight UTC rollover.

	The callback runs SYNCHRONOUSLY on the engine thread. Its first action
	is a ``copy.deepcopy(market_state)`` to snapshot live state safely —
	the copy is safe only when taken on the engine thread because we have
	no lock over MarketState's internal dicts. After snapshotting, the
	callback spawns a daemon thread for the slow bundle assembly + upload
	work so the engine loop is only blocked by the deepcopy itself
	(typically ~ms even with hundreds of orderbooks).
	"""
	def on_rotation(old_day: date) -> None:
		# 1. Synchronous snapshot on the engine thread (fast, safe).
		snapshot = copy.deepcopy(market_state)

		# 2. Background thread for assemble + upload (slow, can take minutes).
		def _assemble_and_upload() -> None:
			try:
				bundle_path = assemble_daily_bundle(
					capture_date=old_day,
					capture_dir=capture_dir,
					repo_root=repo_root,
					db_path=db_path,
					market_state=snapshot,
				)
				if transport is not None:
					remote_key = f"kalshi/{old_day.isoformat()}"
					transport.upload_bundle(bundle_path, remote_key)
					log.info("uploaded bundle %s to transport (%s)", old_day, remote_key)
				else:
					log.info("bundle %s assembled; no transport configured, skipping upload", old_day)
			except Exception:
				log.exception("background bundle assembly/upload failed for %s", old_day)

		thread = threading.Thread(
			target=_assemble_and_upload,
			name=f"bundle-assemble-{old_day}",
			daemon=True,
		)
		thread.start()

	return on_rotation


def _build_capture_transport(capture_cfg: dict) -> Optional[CaptureTransport]:
	"""Construct a CaptureTransport based on config.

	Config shape:
		capture:
		  transport: none | local | r2           # default 'none'
		  transport_local_root: <path>           # used when transport=local
		  # R2 reads CAPTURE_TRANSPORT_* env vars

	Returns None when transport is 'none' or when R2 config is missing —
	the bundle assembler still runs and bundles accumulate on local disk,
	just without uploading.
	"""
	kind = (capture_cfg.get("transport") or "none").lower()
	if kind == "none":
		return None
	if kind == "local":
		root = Path(capture_cfg.get("transport_local_root", "data/capture_bundles"))
		log.info("capture transport: local → %s", root)
		return LocalTransport(root=root)
	if kind == "r2":
		try:
			transport = R2Transport()
			log.info("capture transport: R2 (bucket=%s)", transport.bucket)
			return transport
		except KeyError as e:
			log.warning(
				"capture transport R2 requested but env var missing: %s — "
				"continuing with local-only bundles",
				e,
			)
			return None
	log.warning("capture transport: unknown kind %r — continuing without upload", kind)
	return None


async def run_engine(config_path: Path) -> None:
	"""Main engine loop — connect WS, dispatch ticks, manage background tasks.

	Args:
		config_path: Path to the YAML config file.
	"""
	# 1. Load config, init TradeStore, init MarketState
	config = load_config(config_path)
	# Operational metrics counter — stashed in config so tick-path functions
	# (_handle_enter) that already receive config can read it without adding
	# a new parameter to every handler. The underscore signals "internal".
	metrics = Metrics()
	config["_metrics"] = metrics
	db_path = Path(config.get("db_path", "data/paper_trades.db"))
	ws_cfg = config.get("ws", {})
	recovery_cfg = config.get("recovery", {})
	reconnect_delay = ws_cfg.get("reconnect_delay", 30)
	ping_interval = ws_cfg.get("ping_interval", 20)
	price_history_limit = ws_cfg.get("price_history_limit", 100)
	state_flush_interval = recovery_cfg.get("state_flush_interval", 5)
	store = TradeStore(db_path)
	market_state = MarketState(limit=price_history_limit)

	# Capture pipeline (default disabled — the `capture:` block in config
	# opts in per-deploy). The writer is best-effort; if capture is disabled
	# the writer is a no-op instance that doesn't touch disk.
	capture_cfg = config.get("capture", {}) or {}
	capture_enabled = capture_cfg.get("enabled", False)
	capture_output_dir = Path(capture_cfg.get("output_dir", "data/orderbook_capture"))

	# Build the rotation callback only when capture is enabled. The callback
	# closes over market_state (by reference — deepcopies on call to stay
	# consistent on the engine thread) and db_path, plus an optional transport.
	# repo_root is derived from this file's location so it works on dev
	# and on the Pi regardless of cwd.
	rotation_callback = None
	if capture_enabled:
		transport = _build_capture_transport(capture_cfg)
		repo_root = Path(__file__).resolve().parent.parent.parent
		rotation_callback = _make_rotation_callback(
			capture_dir=capture_output_dir,
			repo_root=repo_root,
			db_path=db_path,
			market_state=market_state,
			transport=transport,
		)

	capture_writer = RawFrameWriter(
		output_dir=capture_output_dir,
		enabled=capture_enabled,
		min_free_gb=capture_cfg.get("min_free_gb", 10),
		rotation_callback=rotation_callback,
	)
	if capture_writer.enabled:
		log.info("orderbook capture enabled → %s", capture_output_dir)

	# 2. Discover and filter strategies
	all_strategies = discover_strategies()
	strategies, rejected_pairs = get_enabled_strategies(config, all_strategies)
	if not strategies:
		log.error("No enabled strategies found — exiting")
		store.close()
		return

	log.info("Enabled strategies: %s", [s.name for s in strategies])
	if rejected_pairs:
		log.warning(
			"Startup: %d (strategy, series) pair(s) flagged unsupported under "
			"non-strict validation: %s",
			len(rejected_pairs),
			rejected_pairs,
		)
	metrics.set_gauge("entries_skipped_unsupported", len(rejected_pairs))

	# 3. Load persisted states, determine active series
	all_states = store.load_all_states()
	pending_states: dict[str, dict] = {}
	for strat in strategies:
		pending_states[strat.name] = all_states.get(strat.name, {})

	active_series = _collect_active_series(config)
	log.info("Active series: %s", active_series)

	# 4. Run recovery
	async with httpx.AsyncClient(timeout=30.0) as client:
		await run_recovery(client, market_state, active_series, capture_writer=capture_writer)

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
				_settlement_poller(
					store, client, strategies, pending_states,
					metrics=metrics, capture_writer=capture_writer,
				),
				name="settlement_poller",
			),
			asyncio.create_task(
				_summary_logger(store, metrics=metrics),
				name="summary_logger",
			),
			asyncio.create_task(
				_state_flusher(store, strategies, pending_states, dirty_strategies, interval=state_flush_interval),
				name="state_flusher",
			),
			asyncio.create_task(
				_ticker_refresh(
					client, market_state, active_series, ws_ref,
					config=config, capture_writer=capture_writer,
				),
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
						capture_writer=capture_writer,
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
					await run_recovery(client, market_state, active_series, capture_writer=capture_writer)
				except Exception:
					log.exception("Unexpected WS error — reconnecting in %ds", reconnect_delay)
					await asyncio.sleep(reconnect_delay)
					market_state.clear()
					await run_recovery(client, market_state, active_series, capture_writer=capture_writer)

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
			capture_writer.close()


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
	capture_writer: RawFrameWriter | None = None,
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

			# Tee point 1/4 — capture BEFORE dispatch so a dispatch failure
			# can't lose the message from the capture log. The writer never
			# raises into this loop (verified by test_write_ws_never_raises_*).
			if capture_writer is not None:
				capture_writer.write_ws(msg)

			# Capture the wall clock ONCE per message so any store rows written
			# from this message share an identical timestamp (required for
			# capture/replay parity — see spec §4.7).
			now = datetime.now(timezone.utc)

			try:
				dispatch_message(
					{"source": "ws", "payload": msg},
					config, market_state, store,
					strategies, strat_by_series, pending_states, dirty,
					now=now,
				)
			except Exception:
				log.exception("Error dispatching WS message (type=%s)", msg.get("type"))


