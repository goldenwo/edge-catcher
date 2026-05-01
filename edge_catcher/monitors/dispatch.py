"""Pure dispatch + handlers for engine input events.

This module is called by both the live engine (engine.py message loop) and —
once Phase 6 lands — the replay backtester (replay/backtester.py). Both pass
the same event shape through the same handlers, which is what makes byte-equal
parity possible between the two paths.

No globals. No I/O beyond what the handlers already did while inside engine.py.
No async. Pure routing + state mutation + store writes.

Invariants (see capture/replay spec §4.7):
  * `now: datetime` is threaded from the caller (WS loop, settlement poller,
    or replay dispatcher) all the way to `store.record_trade` /
    `store.exit_trade` / `store.settle_trade`. NO handler reads
    `datetime.now()` internally.
  * Handlers are relocated from engine.py verbatim; their logic is unchanged.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from edge_catcher.monitors.market_state import (
	MarketState,
	OrderbookSnapshot,
	TickContext,
	_is_tradeable_cents,
	derive_event_ticker,
)
from edge_catcher.monitors.metrics import Metrics
from edge_catcher.monitors.notifications import notify
from edge_catcher.monitors.sizing import FillSkip, resolve_fill
from edge_catcher.monitors.strategy_base import PaperStrategy, Signal
from edge_catcher.monitors.trade_store import TradeStoreProtocol

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Formatting helpers (relocated from engine.py verbatim)
# ---------------------------------------------------------------------------

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


def _format_enter_message(
	*,
	strategy: str,
	series: str,
	ticker: str,
	side: str,
	fill_size: int,
	entry_price: int,
	trade_id: int,
	bullet: str,
) -> tuple[str, str]:
	"""Format an ENTER event for log + Discord."""
	side_label = "YES" if side == "yes" else "NO"
	tag = f"{strategy} | {series}"
	cost = fill_size * entry_price
	log_line = (
		f"ENTER {strategy} {side} {ticker} {fill_size}x@{entry_price}c "
		f"cost={cost}c [id={trade_id}]"
	)
	notify_line = (
		f"{bullet} **[{tag}] PAPER BUY {side_label}** — "
		f"`{ticker}` {fill_size} @ {entry_price}¢ ({cost}¢ cost)"
	)
	return log_line, notify_line


def _format_close_message(
	*,
	event: str,  # "EXIT" or "SETTLED"
	outcome: str,  # "WIN", "LOSS", "SCRATCH"
	strategy: str,
	series: str,
	ticker: str,
	side: str,
	fill_size: int,
	effective_entry: int,
	exit_price: int,
	pnl_cents: int,
	fee_cents: int,
	settled_result: str | None,  # "yes"/"no" for SETTLED, None for EXIT
	trade_id: int,
	bullet: str,
) -> tuple[str, str]:
	"""Format an EXIT (TP/SL) or SETTLED event for log + Discord."""
	side_label = "YES" if side == "yes" else "NO"
	tag = f"{strategy} | {series}"
	outcome_emoji = "🏆" if outcome == "WIN" else ("💀" if outcome == "LOSS" else "🧣")
	pnl_str = f"{pnl_cents:+d}"
	fee_log = f" fee=-{fee_cents}c" if fee_cents else ""
	fee_notify = f" (−{fee_cents}¢ fee)" if fee_cents else ""

	if event == "SETTLED" and settled_result:
		result_tag = f"result={settled_result}"
		action_tag = f"{outcome} (settled {settled_result.upper()})"
	else:
		result_tag = "(exit)"
		action_tag = f"{outcome} (exit)"

	log_line = (
		f"{event} {strategy} {side} {ticker} {fill_size}x "
		f"{effective_entry}c->{exit_price}c {result_tag} "
		f"{outcome} pnl={pnl_str}c{fee_log} [id={trade_id}]"
	)
	notify_line = (
		f"{outcome_emoji}{bullet} **[{tag}] {action_tag}** — "
		f"`{ticker}` {fill_size} {side_label} "
		f"{effective_entry}¢ → {exit_price}¢, "
		f"{pnl_str}¢ pnl{fee_notify}"
	)
	return log_line, notify_line


# ---------------------------------------------------------------------------
# Signal pipeline (relocated from engine.py verbatim)
# ---------------------------------------------------------------------------

def process_tick(
	ctx: TickContext,
	strategies: list[PaperStrategy],
	store: TradeStoreProtocol,
	config: dict,
	*,
	now: datetime,
) -> None:
	"""Run every enabled strategy against the current tick context.

	For each strategy:
	  - Call on_tick → get signals
	  - Process enter/exit signals with exception isolation per-signal

	This is the synchronous, testable core of the engine.

	`now` is the wall-clock timestamp for this tick, captured once in the WS
	message loop (or equivalent replay source) and threaded down so that every
	trade row written during this call has an identical entry_time/exit_time.
	Required for byte-equal parity between live and replay backtester paths.
	"""
	for strategy in strategies:
		try:
			signals = strategy.on_tick(ctx)
		except Exception:
			log.exception("Strategy %s raised on on_tick for %s", strategy.name, ctx.ticker)
			continue

		for signal in signals:
			try:
				_handle_signal(signal, ctx, store, config, strategy.emoji, now=now)
			except Exception:
				log.exception(
					"Error handling %s signal from %s for %s",
					signal.action, signal.strategy, signal.ticker,
				)


def _handle_signal(
	signal: Signal,
	ctx: TickContext,
	store: TradeStoreProtocol,
	config: dict,
	bullet: str = "🔵",
	*,
	now: datetime,
) -> None:
	"""Dispatch a single signal — enter or exit."""
	if signal.action == "enter":
		_handle_enter(signal, ctx, store, config, bullet, now=now)
	elif signal.action == "exit":
		_handle_exit(signal, ctx, store, bullet, now=now)
	else:
		log.warning("Unknown signal action '%s' from %s", signal.action, signal.strategy)


def _handle_enter(
	signal: Signal,
	ctx: TickContext,
	store: TradeStoreProtocol,
	config: dict,
	bullet: str = "🔵",
	*,
	now: datetime,
) -> None:
	"""Process an entry signal: resolve sizing, walk orderbook, record trade."""
	# Raw tick price for the side: yes pays yes_ask, no pays no_ask
	entry_price = ctx.yes_ask if signal.side == "yes" else ctx.no_ask

	# Reject degenerate prices on binary contracts (0c or 100c have zero upside).
	# Placed BEFORE the attempt counter so the invariant
	# attempted == filled + skipped_stale + skipped_other holds.
	if not (1 <= entry_price <= 99):
		log.debug("Skip: entry_price %dc out of range for %s %s", entry_price, signal.side, signal.ticker)
		return

	metrics = config.get("_metrics")
	if metrics is None:
		metrics = Metrics()
	metrics.inc("entries_attempted")

	fill = resolve_fill(config, entry_price, signal.side, ctx.orderbook)

	if isinstance(fill, FillSkip):
		if fill.reason == "stale_book":
			metrics.inc("entries_skipped_stale")
		else:
			metrics.inc("entries_skipped_other")
		log.info(
			"No fill for %s %s %s (entry=%dc) — skipping (reason=%s)",
			signal.strategy, signal.side, signal.ticker, entry_price, fill.reason,
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
		now=now,
	)
	metrics.inc("entries_filled")

	display_price = fill.blended_price_cents if fill.blended_price_cents else entry_price
	log_line, notify_line = _format_enter_message(
		strategy=signal.strategy,
		series=signal.series,
		ticker=signal.ticker,
		side=signal.side,
		fill_size=fill.fill_size,
		entry_price=display_price,
		trade_id=trade_id,
		bullet=bullet,
	)
	log.info(log_line)
	notify(notify_line)


def _handle_exit(
	signal: Signal,
	ctx: TickContext,
	store: TradeStoreProtocol,
	bullet: str = "🔵",
	*,
	now: datetime,
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

	store.exit_trade(signal.trade_id, exit_price, now=now)

	# Read back PnL + fill fields from DB (includes fee deduction)
	exited = store.get_trade_by_id(signal.trade_id)
	if exited is None:
		log.warning("EXIT: trade id=%d not found post-exit_trade", signal.trade_id)
		return
	pnl = exited.get("pnl_cents") or 0
	outcome, _ = _pnl_label(pnl)
	blended = exited.get("blended_entry") or 0
	effective_entry = blended if blended else (exited.get("entry_price") or 0)
	fill_size = exited.get("fill_size") or 0
	entry_fee = exited.get("entry_fee_cents") or 0
	# Exit fee isn't stored separately — back-derive from the pnl formula
	# pnl = fill_size * (exit_price - effective_entry) - entry_fee - exit_fee
	#   → exit_fee = fill_size * (exit_price - effective_entry) - entry_fee - pnl
	gross = fill_size * (exit_price - effective_entry)
	exit_fee = gross - entry_fee - pnl
	total_fee = entry_fee + (exit_fee if exit_fee > 0 else 0)

	log_line, notify_line = _format_close_message(
		event="EXIT",
		outcome=outcome,
		strategy=signal.strategy,
		series=signal.series,
		ticker=signal.ticker,
		side=signal.side,
		fill_size=fill_size,
		effective_entry=effective_entry,
		exit_price=exit_price,
		pnl_cents=pnl,
		fee_cents=total_fee,
		settled_result=None,
		trade_id=signal.trade_id,
		bullet=bullet,
	)
	log.info(log_line)
	notify(notify_line)


# ---------------------------------------------------------------------------
# WS handlers (relocated from engine.py verbatim)
# ---------------------------------------------------------------------------

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


def _handle_orderbook_snapshot(market_state: MarketState, msg: dict) -> None:
	"""Install a full orderbook snapshot from a Kalshi WS message.

	Kalshi emits ``orderbook_snapshot`` as the initial response when a
	client subscribes to the ``orderbook_delta`` channel, and again for
	full-book refreshes mid-session on quiet markets. Without this handler
	the engine drops the message and relies on REST recovery to re-seed on
	reconnect, which leaves quiet markets stale mid-session.

	Accepts both the legacy ``yes``/``no`` shape (matching the in-prod
	delta handler) and the ``yes_dollars_fp``/``no_dollars_fp`` shape from
	Kalshi's current public schema. Sub-cent ghost levels are filtered —
	same invariant as REST snapshot ingest and delta application.
	"""
	data = msg.get("msg", {})
	ticker = data.get("market_ticker", "")
	if not ticker:
		return

	def _parse_side(raw_levels: Any) -> list[tuple[float, int]]:
		parsed: list[tuple[float, int]] = []
		for entry in raw_levels or []:
			try:
				price = float(entry[0])
				qty = int(float(entry[1]))
			except (TypeError, ValueError, IndexError):
				continue
			if qty <= 0:
				continue
			if not _is_tradeable_cents(price):
				continue
			parsed.append((price, qty))
		parsed.sort(key=lambda lvl: lvl[0])
		return parsed

	yes_raw = data.get("yes") if data.get("yes") is not None else data.get("yes_dollars_fp")
	no_raw = data.get("no") if data.get("no") is not None else data.get("no_dollars_fp")

	snapshot = OrderbookSnapshot(
		yes_levels=_parse_side(yes_raw),
		no_levels=_parse_side(no_raw),
	)
	market_state.seed_orderbook(ticker, snapshot)


def _handle_ticker_msg(
	msg: dict,
	config: dict,
	market_state: MarketState,
	store: TradeStoreProtocol,
	strategies: list[PaperStrategy],
	strat_by_series: dict[str, list[PaperStrategy]],
	pending_states: dict[str, dict],
	dirty: set[str],
	*,
	now: datetime,
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
			process_tick(ctx, [strat], store, config, now=now)
			dirty.add(strat.name)


def _handle_trade_msg(
	msg: dict,
	config: dict,
	market_state: MarketState,
	store: TradeStoreProtocol,
	strategies: list[PaperStrategy],
	strat_by_series: dict[str, list[PaperStrategy]],
	pending_states: dict[str, dict],
	dirty: set[str],
	*,
	now: datetime,
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
		trade_price_cents = int(round(float(yes_price_raw) * 100))
	except (TypeError, ValueError):
		return
	if not (1 <= trade_price_cents <= 99):
		return

	taker_side = data.get("taker_side")
	count_raw = data.get("count")
	try:
		trade_count = int(float(count_raw)) if count_raw is not None else None
	except (TypeError, ValueError):
		trade_count = None

	# Record the trade price in history (legitimate event data) before the
	# orderbook guard — price_history should still accumulate even if the
	# book isn't populated yet.
	is_first = market_state.update_price(ticker, trade_price_cents)

	# Bid/ask come from the orderbook, NOT the trade price. A trade can execute
	# off-book (late limit orders, aggressive fills); treating yes_price as the
	# current ask lets strategies enter phantom trades at a price no resting
	# order would fill. If the orderbook isn't populated, skip strategies rather
	# than fire blind.
	yes_ask_cents = market_state.get_yes_ask(ticker)
	yes_bid_cents = market_state.get_yes_bid(ticker)
	if yes_ask_cents is None or yes_bid_cents is None:
		return
	no_ask_cents = 100 - yes_bid_cents
	no_bid_cents = 100 - yes_ask_cents
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
			process_tick(ctx, [strat], store, config, now=now)
			dirty.add(strat.name)


# ---------------------------------------------------------------------------
# dispatch_message router
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Synthetic event handlers
#
# Synthetic events are captured-side-only: they represent state transitions
# that don't arrive via WS but still affect MarketState or TradeStore. The
# capture writer emits these events at the same moment the engine applies
# them live, so the replay backtester can re-apply them in the same order as
# the interleaved WS events.
# ---------------------------------------------------------------------------

def _handle_synthetic_rest_orderbook(market_state: MarketState, payload: dict) -> None:
	"""Apply a captured REST orderbook response.

	Replay equivalent of run_recovery's fetch_orderbook_snapshot + seed_orderbook
	pair at the moment the live engine made the REST call. The payload carries
	the post-parse `yes_levels` / `no_levels` so the replay doesn't re-parse
	the raw Kalshi response.
	"""
	from edge_catcher.monitors.market_state import OrderbookSnapshot
	ticker = payload.get("ticker")
	if not ticker:
		return
	yes_levels = [(float(p), int(q)) for p, q in payload.get("yes_levels", [])]
	no_levels = [(float(p), int(q)) for p, q in payload.get("no_levels", [])]
	snapshot = OrderbookSnapshot(yes_levels=yes_levels, no_levels=no_levels)
	market_state.seed_orderbook(ticker, snapshot)
	meta = payload.get("market_metadata")
	if meta is not None:
		market_state.register_ticker(ticker, meta=meta)


def _handle_synthetic_ticker_discovered(market_state: MarketState, payload: dict) -> None:
	"""Functionally identical to _handle_synthetic_rest_orderbook but tagged
	separately for telemetry — fires from _ticker_refresh when a new ticker
	is discovered mid-day (vs run_recovery re-seeding after a reconnect)."""
	_handle_synthetic_rest_orderbook(market_state, payload)


def _handle_synthetic_settlement(store: TradeStoreProtocol, payload: dict, now: datetime) -> None:
	"""Apply a captured settlement decision from _settlement_poller.

	Resolves the open trade by composite key (strategy, ticker, side, entry_time).
	Per spec §4.1, this key is unique across open trades, enforced by
	DuplicateOpenTradeError in record_trade.

	`payload["result"]` is the raw market outcome 'yes' or 'no' — NOT 'won'/'lost'.
	The store translates to won/lost internally based on the trade's side.
	"""
	strategy = payload.get("strategy")
	ticker = payload.get("ticker")
	side = payload.get("side")
	entry_time = payload.get("entry_time")
	result = payload.get("result")
	# Per-key isinstance check (vs `all()`) so mypy narrows each name from
	# `Any | None` to `str` for the downstream calls.
	if not (
		isinstance(strategy, str)
		and isinstance(ticker, str)
		and isinstance(side, str)
		and isinstance(entry_time, str)
		and isinstance(result, str)
	):
		log.warning("synthetic.settlement: incomplete payload, skipping: %r", payload)
		return

	open_trades = store.get_open_trades_for(strategy, ticker)  # parameter is `strategy`, not `strat_name`
	matches = [
		t for t in open_trades
		if t.get("side") == side and t.get("entry_time") == entry_time
	]
	if not matches:
		log.warning(
			"synthetic.settlement: no open trade matches key "
			"(strategy=%s ticker=%s side=%s entry_time=%s) — skipping",
			strategy, ticker, side, entry_time,
		)
		return
	if len(matches) > 1:
		# Shouldn't happen — DuplicateOpenTradeError in record_trade prevents this.
		raise RuntimeError(
			f"synthetic.settlement: composite key matched {len(matches)} open trades "
			f"for {strategy}/{ticker} — DuplicateOpenTradeError invariant violated"
		)
	store.settle_trade(matches[0]["id"], result, now=now)


# ---------------------------------------------------------------------------
# dispatch_message router
# ---------------------------------------------------------------------------

def dispatch_message(
	event: dict,
	config: dict,
	market_state: MarketState,
	store: TradeStoreProtocol,
	strategies: list[PaperStrategy],
	strat_by_series: dict[str, list[PaperStrategy]],
	pending_states: dict[str, dict],
	dirty: set[str],
	*,
	now: datetime,
) -> None:
	"""Route one parsed event to its handler.

	`event` is either the on-disk JSONL shape (``{recv_seq, recv_ts, source, payload}``)
	or the legacy WS-only shape (just the parsed WS message dict). The router
	accepts both so the live engine can construct events from raw WS messages
	without going through a capture writer first, and the replay backtester
	can feed the on-disk shape directly.
	"""
	# Accept both the wrapped (capture) shape and the raw WS shape.
	source = event.get("source")
	if source is None and "type" in event:
		# Raw WS message — wrap it implicitly
		msg = event
		source = "ws"
	elif source == "ws":
		msg = event.get("payload", event)

	if source == "ws":
		msg_type = msg.get("type")
		if msg_type == "orderbook_snapshot":
			_handle_orderbook_snapshot(market_state, msg)
		elif msg_type == "orderbook_delta":
			_handle_orderbook_delta(market_state, msg)
		elif msg_type == "ticker":
			_handle_ticker_msg(
				msg, config, market_state, store, strategies,
				strat_by_series, pending_states, dirty, now=now,
			)
		elif msg_type == "trade":
			_handle_trade_msg(
				msg, config, market_state, store, strategies,
				strat_by_series, pending_states, dirty, now=now,
			)
		else:
			log.debug("dispatch_message: unknown msg_type %r", msg_type)
	elif source == "synthetic.rest_orderbook":
		_handle_synthetic_rest_orderbook(market_state, event.get("payload", {}))
	elif source == "synthetic.ticker_discovered":
		_handle_synthetic_ticker_discovered(market_state, event.get("payload", {}))
	elif source == "synthetic.settlement":
		_handle_synthetic_settlement(store, event.get("payload", {}), now)
	else:
		log.warning("dispatch_message: unknown source %r", source)
