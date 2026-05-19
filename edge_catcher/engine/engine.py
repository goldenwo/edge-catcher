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

from edge_catcher.adapters.kalshi.auth import (
	KALSHI_LIVE_KEY_ID_ENV,
	KALSHI_LIVE_PRIVATE_KEY_ENV,
	KALSHI_WS_URL,
	make_auth_headers,
)
from edge_catcher.engine.capture.bundle import (
	assemble_daily_bundle,
	delete_raw_jsonl,
	mark_bundle_uploaded,
	prune_old_bundles,
)
from edge_catcher.engine.capture.transport import (
	CaptureTransport,
	LocalTransport,
	R2Transport,
)
from edge_catcher.engine.capture.writer import RawFrameWriter
from edge_catcher.engine.discovery import (
	discover_strategies,
	get_enabled_strategies,
	load_config,
)
from edge_catcher.engine.dispatch import (
	_OPERATOR_KILL,
	_format_close_message,
	_pnl_label,
	dispatch_message,
	drain_inflight_sections,
)
from edge_catcher.engine.executor import Executor
from edge_catcher.engine.executors.paper import PaperExecutor
from edge_catcher.engine.metrics import Metrics
from edge_catcher.engine.market_state import MarketState
from edge_catcher.engine.notifications import configure_notify, notify
from edge_catcher.engine.recovery import (
	check_market_result,
	fetch_active_tickers_for_series,
	fetch_orderbook_snapshot,
	run_recovery,
)
from edge_catcher.engine.strategy_base import Strategy
from edge_catcher.engine.trade_store import TradeStore

# KillSwitchTripFailed must propagate out of run_engine when raised — it's the
# C-spec L214 ghost-reject defense. process_tick re-raises it past _handle_signal's
# broad-except; this module's _ws_loop and the outer reconnect block must ALSO
# re-raise so the engine actually STOPS instead of continuing to the next tick
# (which would re-evaluate the gate against unchanged DB state and let the
# previously-blocked trade through with real money).
#
# Imported at runtime via try/except so engine.py still imports when risk.py is
# absent (paper-only deployments / tests with no live risk module).
try:
	from edge_catcher.engine.risk import KillSwitchTripFailed  # noqa: PLC0415
except ImportError:
	class KillSwitchTripFailed(Exception):  # type: ignore[no-redef]
		pass

# RecordPendingFailed (B / PR 5) is the same ghost-reject defense for the
# live-trades persistence layer: a failed record_pending/record_open INSERT
# strands a funds-at-risk Kalshi-side order with no local row. It must
# propagate out of run_engine for the same reason KillSwitchTripFailed does —
# continuing past it would re-evaluate the gate against unchanged DB state.
# Same runtime try/except so engine.py still imports when live.state is absent.
try:
	from edge_catcher.live.state import RecordPendingFailed  # noqa: PLC0415
except ImportError:
	class RecordPendingFailed(Exception):  # type: ignore[no-redef]
		pass

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# §6 boot step 3 — live risk-event callback slot (G3 fills the routing).
#
# spec §6 (NORMATIVE boot order): step (3) constructs the risk module AND
# registers `_handle_risk_event` into the Gate's callback list BEFORE any
# gate evaluation (reconcile in step 4, trading in step 5), so a boot-time
# trip (e.g. the startup balance read already ≤ absolute_panic_floor) still
# reaches the live risk channel. E3 only LEAVES/REGISTERS this slot per §6;
# the CR-1 notification ROUTING (which Discord channel, the unified
# notifications/ layer) is G3's deliverable, NOT E3's — so this is a typed,
# documented no-op placeholder G3 replaces with the real send(). Signature
# matches Gate._event_callbacks' contract (risk.py:923-927): called with a
# single RiskEvent, sync, exceptions are swallowed by the Gate.
# ---------------------------------------------------------------------------

def _handle_risk_event(event: Any) -> None:
	"""§6-step-3 risk-event callback SLOT (placeholder — G3 wires routing).

	Registered into the live ``Gate``'s callback list at boot so the wiring
	point exists before any gate evaluation. E3 deliberately does NOT route
	notifications here (CR-1 / the unified ``notifications/`` convergence is
	G3's chartered scope); it logs at WARNING so a boot-time / runtime trip is
	never silently lost in the window before G3 lands. G3 replaces the body
	with the real dedicated-live-risk-channel ``send()``."""
	log.warning(
		"RiskEvent (kind=%s reason=%s severity=%s) — E3 risk-event slot is a "
		"placeholder; CR-1 channel routing lands in G3",
		getattr(event, "kind", "?"),
		getattr(event, "reason", "?"),
		getattr(event, "severity", "?"),
	)


# ---------------------------------------------------------------------------
# §2 fail-closed mode-coherence invariant (NORMATIVE — spec §2 / §6)
#
# Wrong-mode is structurally unstartable. Before ANY WS connect, REST call,
# DB open, or order placement, run_engine asserts the declared execution
# mode coheres with EVERY independent live/paper signal. Any disagreement
# aborts with a precise RuntimeError BEFORE the first side effect.
#
# §6 boot ordering: this is step (2) — called FIRST right after config load,
# BEFORE the store/executor is constructed and BEFORE any network. It is
# read-only: it resolves predicates (creds/channels/caps) without performing
# I/O. A coherence check that itself touched the network, or that could be
# bypassed, would defeat its own purpose.
#
# §9 G-parity: for paper mode only checks (1) executor key ∈ {live, paper}
# and (2) paper ⟺ paper_trades*.db run; checks 3/4/5 are live-only and are
# genuinely skipped. The paper path is byte-unchanged.
# ---------------------------------------------------------------------------

# Default unified-notifications config path — mirrors the reporting CLI's
# _DEFAULT_NOTIFY_CONFIG (edge_catcher/reporting/__main__.py) so live engine
# and live P&L cron resolve the SAME channels file by default.
_DEFAULT_NOTIFY_CONFIG = "config.local/notifications.yaml"

# Live trade-scope signing env-var names. A's design (adapters/kalshi/auth.py
# docstring): the live trader passes trade-scope key var names so a leaked
# read-only key cannot place orders. Overridable via the notifications/creds
# config block; these defaults are the CANONICAL auth constants — the SAME
# objects the live signer (live/client.py `_request`) uses — so the §2
# coherence gate (which exists to catch signer/config drift) and the signer
# cannot drift apart (single source; spec Obl-3 / Minor#1).
_DEFAULT_LIVE_KEY_ID_ENV = KALSHI_LIVE_KEY_ID_ENV
_DEFAULT_LIVE_PRIVATE_KEY_ENV = KALSHI_LIVE_PRIVATE_KEY_ENV


def _coherence_fail(check: str, detail: str) -> RuntimeError:
	"""Log a precise error naming WHICH coherence check failed and return
	the RuntimeError to raise. Centralized so every abort path has an
	identical, operator-actionable, grep-able shape ("coherence" + the
	failed dimension + why)."""
	msg = f"mode-coherence FAILED [{check}]: {detail}"
	log.error("BOOT ABORT — %s. Wrong-mode is structurally unstartable "
	          "(spec §2/§6); no network/order was performed.", msg)
	return RuntimeError(msg)


def _assert_mode_coherence(config: dict) -> None:
	"""§2 fail-closed coherence invariant — the funds-safety boot gate.

	Read-only. No network, no DB open, no order. Raises ``RuntimeError``
	(message contains "coherence" + the failed check) on ANY disagreement
	between the declared mode and the resolved db/creds/channel/caps.

	Live mode asserts all five §2 dimensions; paper mode asserts only the
	two that apply (executor key + db path) and skips the live-only ones
	with ZERO behavior change (§9 G-parity).
	"""
	# --- Check 1: the `executor:` key is the mode of record (§2.1). ---
	mode = config.get("executor")
	if mode not in ("live", "paper"):
		raise _coherence_fail(
			"executor",
			f"`executor:` must be 'live' or 'paper' (the mode of record, "
			f"spec §2.1); got {mode!r}",
		)

	# --- Check 2: resolved DB path ⟺ mode (§2.2). Applies to BOTH modes
	# symmetrically — a paper run must never touch the real-money DB and a
	# live run must never write into the paper DB. Substring match on the
	# resolved path (live_trades / paper_trades), matching the codebase
	# convention (live/store.py, live/cli.py default data/live_trades.db;
	# engine default data/paper_trades.db). ---
	db_path = str(config.get("db_path", "data/paper_trades.db"))
	db_name = Path(db_path).name.lower()
	if mode == "live":
		if "live_trades" not in db_name:
			raise _coherence_fail(
				"db",
				f"executor=live but db_path {db_path!r} is not a "
				f"live_trades*.db (a live run must write the real-money DB)",
			)
		if "paper_trades" in db_name:
			raise _coherence_fail(
				"db",
				f"executor=live but db_path {db_path!r} looks like a paper "
				f"DB (real-money rows must not land in the paper DB)",
			)
	else:  # paper
		if "paper_trades" not in db_name:
			raise _coherence_fail(
				"db",
				f"executor=paper but db_path {db_path!r} is not a "
				f"paper_trades*.db",
			)
		if "live_trades" in db_name:
			raise _coherence_fail(
				"db",
				f"executor=paper but db_path {db_path!r} looks like the "
				f"live real-money DB (paper must never touch it)",
			)

	# Checks 3/4/5 are LIVE-ONLY. For paper they are skipped entirely so
	# the paper path is byte-unchanged (§9). Return now for paper.
	if mode == "paper":
		return

	notif_cfg = config.get("notifications", {}) or {}

	# --- Check 3: creds resolvable via A's auth resolver (§2.3, live
	# only). make_auth_headers reads the trade-scope env vars and signs a
	# local string with RSA-PSS — NO network. A missing/invalid key raises
	# KeyError/ValueError; we translate to the coherence RuntimeError. ---
	key_id_env = notif_cfg.get("live_key_id_env", _DEFAULT_LIVE_KEY_ID_ENV)
	private_key_env = notif_cfg.get(
		"live_private_key_env", _DEFAULT_LIVE_PRIVATE_KEY_ENV
	)
	try:
		make_auth_headers(
			key_id_env=key_id_env, private_key_env=private_key_env
		)
	except KeyError as exc:
		raise _coherence_fail(
			"creds",
			f"executor=live but Kalshi trade-scope credentials are "
			f"unresolvable: env var {exc} is not set (checked "
			f"{key_id_env!r}/{private_key_env!r})",
		) from exc
	except ValueError as exc:
		raise _coherence_fail(
			"creds",
			f"executor=live but the resolved Kalshi private key is "
			f"invalid: {exc}",
		) from exc

	# --- Check 4: the live Discord channel(s) resolvable from the unified
	# notifications config (§2.4, live only). load_channels parses the
	# YAML and constructs the adapter objects — NO network (delivery is
	# lazy). E2 only CHECKS resolvability; it does NOT migrate engine
	# notifications onto the unified layer (that is G). ---
	notify_path = Path(
		notif_cfg.get("config_path", _DEFAULT_NOTIFY_CONFIG)
	)
	live_channel = notif_cfg.get("live_channel")
	if not live_channel:
		raise _coherence_fail(
			"channel",
			"executor=live but no `notifications.live_channel` is "
			"configured (live alerts/risk events would go nowhere)",
		)
	# Local import: keep engine.py importable on paper-only deployments
	# that may not have the notifications extra wired, mirroring the
	# risk.py / live.state runtime-import pattern at module top.
	from edge_catcher.notifications import (  # noqa: PLC0415
		NotificationConfigError,
		load_channels,
	)
	try:
		channels = load_channels(notify_path)
	except NotificationConfigError as exc:
		raise _coherence_fail(
			"channel",
			f"executor=live but the unified notifications config "
			f"{str(notify_path)!r} is unresolvable: {exc}",
		) from exc
	if live_channel not in channels:
		raise _coherence_fail(
			"channel",
			f"executor=live but the configured live channel "
			f"{live_channel!r} is not defined in {str(notify_path)!r} "
			f"(available: {sorted(channels)})",
		)

	# --- Check 5: Phase-1 caps present in the `risk:` block (§2.5, live
	# only). Reuse RiskConfig.from_dict — the SAME authoritative parser
	# build_risk_module uses (risk.py) — so there is no drift-prone
	# duplicated key list; it raises KeyError on a missing cap and
	# ValueError on an out-of-range one. Construction is pure (no I/O). ---
	risk_block = config.get("risk")
	if not isinstance(risk_block, dict) or not risk_block:
		raise _coherence_fail(
			"caps",
			"executor=live but the `risk:` block is missing/empty — the "
			"Phase-1 caps are mandatory for live (spec §2.5/§8)",
		)
	# Local import for the same paper-only-deployment resilience reason as
	# the KillSwitchTripFailed/RecordPendingFailed runtime imports.
	from edge_catcher.engine.risk import RiskConfig  # noqa: PLC0415
	try:
		RiskConfig.from_dict(risk_block)
	except KeyError as exc:
		raise _coherence_fail(
			"caps",
			f"executor=live but a required Phase-1 risk cap is absent "
			f"from the `risk:` block: missing key {exc} "
			f"(spec §2.5/§8; canonical set = RiskConfig.from_dict)",
		) from exc
	except (ValueError, TypeError) as exc:
		raise _coherence_fail(
			"caps",
			f"executor=live but a Phase-1 risk cap is invalid: {exc}",
		) from exc


# ---------------------------------------------------------------------------
# §6 boot step 3 — unified notifications channel resolution (Path B).
#
# Resolves the mode's notification channel(s) ONCE at boot from the SAME
# unified `notifications:` config the §2.4 coherence check already parses
# (`config_path` + `live_channel`/`paper_channel`), then hands them to the
# engine notify helper via `configure_notify`. `notify` is NEVER re-resolved
# per call (mirrors the §1 keystone: wired at boot, not per-call). The
# env-var facade is retired — there is no second path; delivery is the
# unified `send()` (sync, never-raises, so a notify cannot perturb the
# trade path — §6/§9).
#
# §9 G-parity: paper resolves `notifications.paper_channel` (optional — the
# paper analog of the retired `DISCORD_*WEBHOOK*` env var; absent ⇒ empty
# list ⇒ notify is a silent no-op, byte-equivalent to the pre-G facade's
# no-webhook no-op). The paper trade-row path is byte-unchanged: notify is
# a side-effect, not trade state, and an unresolvable/absent paper channel
# never aborts boot (live channel resolvability is already enforced by the
# §2.4 coherence gate; this resolution is delivery-only).
# ---------------------------------------------------------------------------

def _resolve_notify_channels(config: dict) -> list:
	"""§6 Path B — resolve the mode's notify channel(s) for the boot helper.

	Returns the list of unified-layer ``Channel`` objects ``notify`` will
	deliver to (``[]`` if none configured — a silent-no-op, the paper analog
	of the retired facade's no-webhook behaviour). Reuses the SAME
	``notifications:`` keys the §2.4 coherence gate parses so the engine and
	the live P&L cron resolve identical channels. Best-effort: a malformed/
	absent config logs a WARNING and yields ``[]`` (delivery-only — the
	live-channel resolvability hard-gate is the §2.4 coherence check, not
	here; a notify failure must never abort the engine — §6/§9).
	"""
	notif_cfg = config.get("notifications", {}) or {}
	mode = config.get("executor")
	channel_name = (
		notif_cfg.get("live_channel")
		if mode == "live"
		else notif_cfg.get("paper_channel")
	)
	if not channel_name:
		# Paper with no `paper_channel` (the common case — the paper analog
		# of "no webhook env var set"): notify is a silent no-op. Live
		# without `live_channel` is already a hard coherence abort upstream;
		# reaching here in live means the gate passed, so this is defensive.
		return []
	notify_path = Path(notif_cfg.get("config_path", _DEFAULT_NOTIFY_CONFIG))
	# Local import: keep engine.py importable on paper-only deployments that
	# may not have the notifications extra wired (same pattern as the §2.4
	# gate / the risk.py / live.state runtime-import convention).
	from edge_catcher.notifications import (  # noqa: PLC0415
		NotificationConfigError,
		load_channels,
	)
	try:
		channels = load_channels(notify_path)
	except NotificationConfigError as exc:
		log.warning(
			"notify channel resolution: unified config %r unresolvable "
			"(%s) — engine notifications disabled this run (delivery-only; "
			"trade path unaffected)",
			str(notify_path), exc,
		)
		return []
	channel = channels.get(channel_name)
	if channel is None:
		log.warning(
			"notify channel resolution: configured channel %r not defined "
			"in %r (available: %s) — engine notifications disabled this run",
			channel_name, str(notify_path), sorted(channels),
		)
		return []
	return [channel]


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
	strategies: list[Strategy],
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
					# Capture the clock ONCE per settled trade. The capture payload,
					# its recv_ts, and the store call ALL share this `now` so that
					# replay produces byte-identical exit_time values.
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
						}, recv_ts=now)
					# SC-D3 (settlement leg — see dispatch._handle_exit's SC-D3
					# note for the shared §1-keystone / R1-deferral rationale,
					# not restated here): live `store.settle_trade` is C5's
					# settlement CAS to B `record_close`
					# (exit_reason='settlement', SUPERSEDES an in-flight
					# `exit_pending`, consumes the entry-fee-remainder) and
					# races SAFELY with B's E3-wired async on_settlement_event;
					# paper `store.settle_trade` is byte-unchanged. The §3
					# "place exit via executor" obligation is the strategy/
					# TP-SL exit (E3's deliverable per the dispatch._handle_exit
					# SC-D3 note); settlement has NO executor leg — it is purely
					# this store-shaped resolution.
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
	strategies: list[Strategy],
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
							# Capture the clock ONCE so both seed_orderbook and
							# the capture tee share an identical recv_ts.
							tick_now = datetime.now(timezone.utc)
							market_state.seed_orderbook(ticker, snapshot)
							if capture_writer is not None:
								# Tee point 3/4 — see capture/replay spec §6.1
								capture_writer.write_synthetic("ticker_discovered", {
									"ticker": ticker,
									"yes_levels": snapshot.yes_levels,
									"no_levels": snapshot.no_levels,
								}, recv_ts=tick_now)
						new_tickers.append(ticker)

				# Purge stale tickers only when the API response was complete
				if reliable:
					for existing in market_state.all_tickers():
						if existing.startswith(series) and existing not in fresh_set:
							market_state.unregister_ticker(existing)
				else:
					log.warning(
						"Skipping ticker purge for %s: API response unreliable (got %d partial tickers)",
						series, len(tickers),
					)

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
	"""Collect all unique series the engine should subscribe to.

	Sources, in order:
	  1. Series declared by every enabled strategy under ``strategies.<name>.series``.
	  2. Capture-only series under ``capture.extra_series`` — observed but never
	     dispatched to any strategy. Lets the operator record markets for future
	     research without firing any live trades on them. Only included when
	     ``capture.enabled`` is true (no point subscribing to extra tickers if
	     capture is off).

	Tickers in capture-only series get full WS subscription, REST recovery
	snapshots, and ticker_refresh updates — so they're recorded with the same
	fidelity as strategy tickers — but they have no entry in ``strat_by_series``
	so dispatch silently drops them at the strategy-routing step. The capture
	tee fires BEFORE dispatch, so observation is unaffected.
	"""
	series: set[str] = set()
	for _name, scfg in config.get("strategies", {}).items():
		if scfg.get("enabled", False):
			for s in scfg.get("series", []) or []:
				series.add(s)

	capture_cfg = config.get("capture", {}) or {}
	if capture_cfg.get("enabled", False):
		for s in capture_cfg.get("extra_series", []) or []:
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
	*,
	delete_raw_after_bundle: bool = True,
	local_retention_days: int = 7,
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

	Retention:
	  * ``delete_raw_after_bundle``: after ``assemble_daily_bundle`` succeeds,
	    delete the raw ``kalshi_engine_<date>.jsonl`` (the compressed copy
	    in the bundle dir is authoritative). Default True — disable only
	    if you want the raw file kept for debugging.
	  * ``local_retention_days``: prune local bundle dirs older than N days,
	    but ONLY if they've been successfully uploaded (``.uploaded``
	    sentinel present). Set to 0 to disable pruning entirely. Default 7.
	    When ``transport`` is None, uploads never happen → sentinels never
	    get written → pruning silently skips every bundle. That's intentional:
	    local-only capture must be manually managed by the operator.
	"""
	def on_rotation(old_day: date) -> None:
		# 1. Synchronous snapshot on the engine thread (fast, safe).
		snapshot = copy.deepcopy(market_state)

		# 2. Background thread for assemble + upload + retention (slow).
		def _assemble_upload_prune() -> None:
			bundle_assembled = False
			try:
				bundle_path = assemble_daily_bundle(
					capture_date=old_day,
					capture_dir=capture_dir,
					repo_root=repo_root,
					db_path=db_path,
					market_state=snapshot,
				)
				bundle_assembled = True

				if transport is not None:
					remote_key = f"kalshi/{old_day.isoformat()}"
					try:
						transport.upload_bundle(bundle_path, remote_key)
						mark_bundle_uploaded(bundle_path)
						log.info("uploaded bundle %s to transport (%s)", old_day, remote_key)
					except Exception:
						log.exception(
							"bundle %s upload failed; bundle stays local for retry",
							old_day,
						)
				else:
					log.info(
						"bundle %s assembled; no transport configured, skipping upload",
						old_day,
					)
			except Exception:
				log.exception("background bundle assembly failed for %s", old_day)

			# 3. Retention (only runs when assembly succeeded — we MUST have
			# a verified compressed copy before deleting the raw). Wrapped
			# in its own try so a retention failure doesn't leak.
			if bundle_assembled and delete_raw_after_bundle:
				try:
					delete_raw_jsonl(capture_dir, old_day)
				except Exception:
					log.exception("delete_raw_jsonl failed for %s", old_day)

			# 4. Prune old bundles (only uploaded ones, and only when a
			# transport is configured — otherwise pruning would have nothing
			# to prune anyway since sentinels never get written).
			if transport is not None and local_retention_days > 0:
				try:
					prune_old_bundles(capture_dir, local_retention_days)
				except Exception:
					log.exception("prune_old_bundles failed")

		thread = threading.Thread(
			target=_assemble_upload_prune,
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


class _LiveRuntime:
	"""Live-only composition products that §6 boot steps 4/5 still need.

	Carried out of :func:`_compose_live` (boot step 3) so ``run_engine``'s
	async-client block can run ``startup_reconcile`` (step 4) and start B's
	reconciler/phantom-pending poller background task (step 5) with the SAME
	wired ``Gate`` (its pre-refreshed bankroll cache) + ``KalshiOrderClient``
	+ the live ``sqlite3.Connection`` (owned by ``SQLiteTradeStore``). Plain
	attribute holder (no dataclass decorator needed — internal, never
	serialized)."""

	def __init__(self, gate: Any, kalshi_client: Any, db_conn: Any) -> None:
		self.gate = gate
		self.kalshi_client = kalshi_client
		self.db_conn = db_conn


async def _compose_live(
	config: dict,
	config_path: Path,
	db_path: Path,
	market_state: MarketState,
	injected_executor: Executor | None,
):
	"""§6 boot step 3 — construct the LIVE composition.

	Returns ``(store, executor, live_runtime)``:

	* ``SQLiteTradeStore`` over ``live_trades.db`` (owns the single live
	  ``sqlite3.Connection`` for its lifetime — §5; ``run_engine`` closes it
	  exactly once on shutdown).
	* ``LiveExecutor`` wrapping a ``KalshiOrderClient`` built from A's
	  ``live/config.py`` (the trade-scope signing key — auth.py's canonical
	  ``KALSHI_LIVE_*`` constants, single-sourced with the §2 gate). An
	  explicitly-injected executor (tests) is honoured verbatim instead.
	* ``_LiveRuntime`` carrying the wired ``Gate`` (``await
	  build_risk_module`` — pre-refreshes the bankroll cache so the first
	  ``gate_entry`` sees real cash) + the client + the conn, for steps 4/5.

	``validate_exec_cfg(config["execution"])`` is also CALLED here at T0
	(§2.5/§6) purely for its fail-fast side-effect — a malformed ``execution:``
	block aborts at boot, not at first exit. Its typed result is intentionally
	NOT returned/stashed: there is no consumer (the exit path builds its
	``OrderRequest`` directly — see ``dispatch._handle_exit``), so binding it
	would be YAGNI dead state (reviewer-prescribed).

	The §6-step-3 ``_handle_risk_event`` slot is registered into the Gate's
	callback list HERE (before any gate evaluation — reconcile in step 4,
	trading in step 5) so a boot-time trip still reaches the (G3-routed)
	risk channel. Lazy imports of ``edge_catcher.live.*`` /
	``engine.risk`` / ``engine.execution`` mirror the established
	paper-only-deployment resilience pattern (the
	``_assert_mode_coherence`` local imports / the module-top
	``KillSwitchTripFailed`` / ``RecordPendingFailed`` try-imports) so
	``engine.py`` still imports on a paper-only box.
	"""
	# Lazy imports — keep engine.py importable on paper-only deployments.
	from edge_catcher.engine.execution import validate_exec_cfg  # noqa: PLC0415
	from edge_catcher.engine.executors.live import LiveExecutor  # noqa: PLC0415
	from edge_catcher.engine.risk import build_risk_module  # noqa: PLC0415
	from edge_catcher.live.audit import AuditLogger  # noqa: PLC0415
	from edge_catcher.live.client import KalshiOrderClient  # noqa: PLC0415
	from edge_catcher.live.config import load_config as load_live_config  # noqa: PLC0415
	from edge_catcher.live.store import SQLiteTradeStore  # noqa: PLC0415

	# Live store owns the single live_trades.db connection (WAL + 0003 +
	# busy_timeout applied inside connect_live_trades_db — §5). The caller
	# (run_engine) closes it exactly once on shutdown (drain order §4.3).
	store = SQLiteTradeStore(db_path)
	db_conn = store._conn

	# A's live config (trade-scope creds via auth.py's canonical constants —
	# single-sourced with the §2 gate; kalshi_rest_base / http_timeout /
	# audit_log_path). load_config returns defaults if the file is absent.
	live_cfg = load_live_config(config_path)
	audit = AuditLogger(live_cfg.audit_log_path)
	kalshi_client = KalshiOrderClient(live_cfg, audit)

	# LiveExecutor wraps the one client for the process lifetime. An
	# explicitly-injected executor (tests) wins — but the live store + B's
	# async tasks still follow the config mode so the seam stays coherent.
	executor: Executor = (
		injected_executor
		if injected_executor is not None
		else LiveExecutor(client=kalshi_client)
	)

	# §2.5/§6: validate execution: at T0 — a malformed block fails at boot,
	# not at first exit. Result intentionally not bound/returned (no consumer;
	# the exit path builds its OrderRequest directly — see dispatch._handle_exit).
	validate_exec_cfg(config.get("execution", {}))

	# §6 step 3 — wire the risk module (pre-refreshes the bankroll cache so
	# the first gate_entry sees real cash; a Kalshi-unreachable boot leaves
	# the cache at 0 ⇒ KILL_AUTO_PANIC on first signal, the correct
	# fail-closed behaviour). build_risk_module reads config["risk"].
	gate = await build_risk_module(config, db_conn, kalshi_client)
	# Register the §6-step-3 risk-event slot BEFORE any gate evaluation
	# (reconcile/trading in steps 4/5) so a boot-time trip reaches it. G3
	# replaces _handle_risk_event's body with the real CR-1 channel send.
	gate._event_callbacks.append(_handle_risk_event)

	return store, executor, _LiveRuntime(gate, kalshi_client, db_conn)


async def run_engine(
	config_path: Path,
	executor: Executor | None = None,
) -> None:
	"""Main engine loop — connect WS, dispatch ticks, manage background tasks.

	Args:
		config_path: Path to the YAML config file.
		executor: Pluggable execution endpoint. Defaults to ``PaperExecutor``
			constructed against ``MarketState`` + ``config``. Sub-project D
			provides ``LiveExecutor`` for live trading.
	"""
	# 1. Load config, init TradeStore, init MarketState
	config = load_config(config_path)

	# 2. §2 fail-closed mode-coherence invariant (NORMATIVE — spec §2/§6
	# boot step 2). Called FIRST, immediately after config load and BEFORE
	# the store/executor is constructed or ANY network/WS/order. A
	# wrong-mode start (executor:live with a mismatched db/creds/channel/
	# caps, or executor:paper pointed at the real-money DB) raises a
	# precise RuntimeError here — structurally unstartable, no side effect
	# performed. For paper this passes cleanly with zero behavior change
	# (§9 G-parity): only the executor-key + paper-DB checks apply.
	_assert_mode_coherence(config)

	# Operational metrics counter — stashed in config so tick-path functions
	# (_handle_enter) that already receive config can read it without adding
	# a new parameter to every handler. The underscore signals "internal".
	metrics = Metrics()
	config["_metrics"] = metrics
	db_path = Path(config.get("db_path", "data/paper_trades.db"))
	ws_cfg = config.get("ws", {})
	recovery_cfg = config.get("recovery", {})
	reconnect_delay = ws_cfg.get("reconnect_delay", 30)
	price_history_limit = ws_cfg.get("price_history_limit", 100)
	state_flush_interval = recovery_cfg.get("state_flush_interval", 5)
	market_state = MarketState(limit=price_history_limit)

	# -------------------------------------------------------------------
	# §1/§3/§6 MODE-DRIVEN COMPOSITION ROOT (the keystone).
	#
	# Mode is decided ONCE, here, after the §2 coherence gate (boot step 2)
	# and per the §6 NORMATIVE boot order. The live-vs-paper difference is
	# WHICH components are wired at this single branch — NEVER a per-call
	# conditional downstream (dispatch / the store-Protocol calls stay
	# mode-agnostic; the executor + which store + whether B's async tasks
	# run is the entire difference — §1). An explicitly-injected `executor`
	# (tests) overrides the mode-driven construction but the store/B-tasks
	# still follow the config mode so the seam stays coherent.
	#
	#   executor: live  ⇒ LiveExecutor(KalshiOrderClient from A's live
	#       config) + SQLiteTradeStore + await build_risk_module + register
	#       the §6 _handle_risk_event slot + validate_exec_cfg(execution:) +
	#       (startup_reconcile + B's reconciler/poller task started in the
	#       async-client block below — §6 steps 4/5).
	#   executor: paper ⇒ PaperExecutor + paper TradeStore + NONE of B's
	#       tasks (today's behaviour — byte-exact, §9 G-parity).
	#
	# `_assert_mode_coherence` already validated `config["executor"]` ∈
	# {live, paper}; this is the SINGLE branch on it.
	mode = config.get("executor")
	live_runtime: _LiveRuntime | None = None
	if mode == "live":
		store, executor, live_runtime = await _compose_live(
			config, config_path, db_path, market_state, executor,
		)
	else:  # paper (coherence-gated to exactly {live, paper})
		store = TradeStore(db_path)
		# Construct the default PaperExecutor if no executor was injected.
		# PaperExecutor takes (market_state, config) — fees compute inside
		# trade_store.record_trade, so no fee_model parameter is required.
		if executor is None:
			executor = PaperExecutor(market_state=market_state, config=config)

	# Risk gate (Sub-project C) — Gate / BankrollCache / KillSwitch / etc.
	# all live in engine/risk.py and SHIP in this PR (PR 3). However the
	# actual construction + wiring requires KalshiBalanceSource (live HTTP
	# client), the live_trades.db connection, and a periodic-refresh task —
	# none of which dispatch.py has access to. E's PR owns the full
	# bootstrap: instantiating KalshiBalanceSource, calling
	# BankrollCache.refresh() at T0, threading RiskContext to dispatch,
	# and starting the periodic-refresh background task.
	#
	# PR 3 ships only the building blocks. No risk-related wiring happens
	# at engine startup yet. If config has executor_kind=live before E
	# ships, the engine starts paper-style (gate not consulted) — see the
	# warning in dispatch._handle_signal when a Gate is constructed
	# without dispatch wiring.

	# §6 Path B — install the boot-resolved notify channel(s) ONCE here
	# (after the §2 coherence gate + the mode-composition branch; the live
	# channel's resolvability was already hard-gated by §2.4). `notify` then
	# delegates to the unified `send()` with these channels and is never
	# re-resolved per call. Delivery-only: an absent/unresolvable channel
	# yields a silent-no-op notify and never aborts boot (§6/§9 — notify is
	# a side-effect, not trade state; the paper trade-row path is unchanged).
	configure_notify(_resolve_notify_channels(config))

	# Cutover-verification beacon. Pi cutover step 5 greps journalctl for this
	# exact substring to prove the engine/ package is loaded (NOT monitors/).
	# Generic "Engine starting" is shared with the OLD engine; this line is unique.
	log.info("engine[G]: paper executor wired, package=edge_catcher.engine")

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
			delete_raw_after_bundle=bool(capture_cfg.get("delete_raw_after_bundle", True)),
			local_retention_days=int(capture_cfg.get("local_retention_days", 7)),
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

		# §6 boot step 4 — startup_reconcile (LIVE only). Pulls the
		# authoritative Kalshi state at T0 (positions + recent orders) and
		# resolves every divergence via B's 6-case matrix BEFORE the WS
		# subscribes / any new order — so a severed prior run's pending /
		# orphan position is reconciled by client_order_id first. Runs AFTER
		# the risk module is wired (step 3) so a boot-time balance≤panic trip
		# still reaches the §6 risk-event slot. Paper has no analog (no
		# Kalshi-truth to reconcile) — this whole block is live-only,
		# byte-exact-invisible to paper (§9 G-parity).
		if live_runtime is not None:
			from edge_catcher.live.reconciliation import (  # noqa: PLC0415
				startup_reconcile,
			)
			try:
				await startup_reconcile(
					live_runtime.kalshi_client,
					live_runtime.db_conn,
					live_runtime.gate._bankroll,
				)
			except Exception:
				# startup_reconcile's own contract: the cash-seed step is
				# FATAL (a live engine that cannot read its balance must not
				# proceed). Re-raise so the engine aborts BEFORE the WS loop
				# rather than trading blind — consistent with the §2/§6
				# fail-closed posture (no order has been placed yet).
				log.exception(
					"startup_reconcile FAILED — aborting live boot before the "
					"WS loop (no order placed; fail-closed §2/§6)"
				)
				raise

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

		# §6 boot step 5 — start B's async lifecycle (LIVE only). B's
		# phantom-pending poller continuously reconciles pending /
		# exit_pending rows against Kalshi truth (one list_orders() per
		# cycle, matched locally by client_order_id) so a row whose WS
		# fill/reject event was missed still resolves (TTL → rejected_post_hoc
		# / exit_pending → open). The settlement leg is already covered by the
		# shared _settlement_poller above (mode-agnostic store.settle_trade →
		# C5's settlement CAS for live); the account-scope WS event loop that
		# pumps on_fill_event/on_order_status_event is F's daemon scope (not
		# E3) — the poller is the E3-scope reconciliation backstop that makes
		# the live lifecycle correct without it. CancelledError-safe by B's
		# own contract (reconciliation.py:874). Paper starts NONE of this
		# (byte-exact today — §1/§9 G-parity).
		if live_runtime is not None:
			from edge_catcher.live.reconciliation import (  # noqa: PLC0415
				poll_pending_rows_loop,
			)
			tasks.append(
				asyncio.create_task(
					poll_pending_rows_loop(
						live_runtime.kalshi_client, live_runtime.db_conn,
					),
					name="live_reconciler_poll_pending",
				)
			)

		# Build strategy lookup by series
		strat_by_series: dict[str, list[Strategy]] = {}
		for strat in strategies:
			strat_series = _series_for_strategy(config, strat.name)
			for s in strat_series:
				strat_by_series.setdefault(s, []).append(strat)

		# §4.3 drain discriminator. The ``while True`` WS loop NEVER falls
		# through normally — it exits only via an exception: a SIGTERM/parent
		# ``CancelledError`` (the §4.3 graceful-shutdown path), or the two
		# fatal ghost-reject re-raises (KillSwitch/RecordPending — a crash-stop,
		# NOT a clean operator shutdown), or a reconnect-and-continue. So the
		# ``finally`` is reached only on a stop. This flag distinguishes the
		# CLEAN SIGTERM/cancel drain (steps 1 & 7 — operator-kill + the final
		# "shutting down" alert — fire) from a fatal crash-stop and, critically,
		# keeps the §9 G-parity contract: paper replay/backtest/CI never
		# SIGTERM, so the non-signal path sets NEITHER the operator-kill flag
		# NOR emits a new alert — byte-identical to F1's drain.
		_shutdown_via_cancel = False
		try:
			# 7. WS loop with reconnect
			while True:
				try:
					await _ws_loop(
						config, market_state, store, strategies,
						strat_by_series, pending_states, active_series,
						client, ws_ref, dirty_strategies,
						executor,
						capture_writer=capture_writer,
					)
				except asyncio.CancelledError:
					# Cooperative cancellation (SIGTERM, parent task cancel). Re-raise
					# BEFORE the OSError tuple below — some websocket implementations
					# wrap shutdown-time errors as OSError, which would route us into
					# the reconnect_delay sleep + run_recovery() call before honouring
					# the cancel. Propagate immediately so shutdown is prompt.
					# Record that THIS is the clean SIGTERM/cancel drain so the
					# §4.3 finally runs steps 1 (operator-kill) & 7 (final alert).
					_shutdown_via_cancel = True
					raise
				except (
					websockets.ConnectionClosed,
					# websockets ≥12 renamed InvalidStatusCode → InvalidStatus.
					# pyproject pin is `websockets>=12.0` so InvalidStatus is
					# always present; the getattr fallback keeps this resilient
					# if the floor ever gets loosened backwards (re-collapses
					# to InvalidStatus on ≥12, where the legacy name is gone).
					websockets.InvalidStatus,
					getattr(websockets, "InvalidStatusCode", websockets.InvalidStatus),
					ConnectionError,
					OSError,
				) as exc:
					log.warning("WS disconnected: %s — reconnecting in %ds", exc, reconnect_delay)
					await asyncio.sleep(reconnect_delay)
					market_state.clear()
					await run_recovery(client, market_state, active_series, capture_writer=capture_writer)
				except KillSwitchTripFailed:
					# C-spec L214 ghost-reject defense — must STOP, not reconnect.
					# Reconnecting would re-enter the WS loop, accept the next tick,
					# query the gate, find no kill row (the INSERT that triggered
					# this exception failed), and allow the previously-rejected
					# trade through. The fail-loud behavior is intentional: the
					# operator sees the crash, investigates DB health, and either
					# restarts the engine after the DB is fixed or manually trips
					# the kill via the CLI.
					#
					# §4.3 tripped-kill ≠ process exit (the OPPOSITE case): a
					# SUCCESSFUL C auto-kill trip does NOT reach here — the gate
					# returns ``Reject`` with NO exception (risk.py gate_entry,
					# the §4.3 NORMATIVE block), so this ``while True`` simply
					# continues awaiting ``_ws_loop`` and the engine keeps
					# running with the gate in KILL state (new entries rejected,
					# exits still allowed). Only this FAILED-write case (and a
					# crash / SIGTERM-drain) stops the process. That asymmetry is
					# what makes the live unit's ``Restart=always`` safe — a
					# tripped auto-kill never exits, so systemd can never restart
					# past it and clear operator intent.
					raise
				except RecordPendingFailed:
					# B / PR 5 ghost-reject defense — same fail-loud contract as
					# KillSwitchTripFailed. record_pending/record_open INSERT
					# failed: a funds-at-risk Kalshi order is stranded with no
					# local row. Reconnecting would re-enter the WS loop and let
					# the next tick re-evaluate the gate against unchanged DB
					# state. STOP so the operator investigates DB health and
					# reconciles the stranded order via the Kalshi UI before
					# restarting. Placed before the broad except for the same
					# reason as the KillSwitchTripFailed clause above.
					raise
				except Exception:
					log.exception("Unexpected WS error — reconnecting in %ds", reconnect_delay)
					await asyncio.sleep(reconnect_delay)
					market_state.clear()
					await run_recovery(client, market_state, active_series, capture_writer=capture_writer)

		finally:
			# ===============================================================
			# 8. Graceful shutdown — the §4.3 NORMATIVE 7-step money-safe
			#    drain ORDER (sub-project E / spec §4.2 L2 + §4.3). The order
			#    is LOAD-BEARING:
			#
			#  (1) set the operator-kill flag FIRST — the risk gate then
			#      rejects every NEW entry via KILL_OPERATOR, so no new
			#      place→persist section can enter the in-flight registry
			#      DURING the drain (signal/cancel path only — §9 G-parity);
			#  (2) stop WS/dispatch intake — the WS loop has already exited
			#      (we are in its ``finally``) and ``ws_ref`` is dropped, so
			#      no further tick reaches dispatch;
			#  (3) AWAIT the §4.2-L2 in-flight place→persist registry to
			#      completion — the DRAIN owns this await (NOT a naive
			#      ``await shield`` in dispatch). A SIGTERM that landed
			#      EXACTLY between ``executor.place()`` returning ``filled``
			#      and the ``record_trade`` persist is made safe HERE: the
			#      shielded section is still running; we wait for its persist
			#      to become durable BEFORE closing the DB;
			#  (4) cancel B's loops (CancelledError-safe by B's contract);
			#  (5) ``gather(*tasks, return_exceptions=True)``;
			#  (6) ``store.close()`` EXACTLY once (the SQLiteTradeStore
			#      ``_closed`` idempotent guard) — STRICTLY AFTER step (3):
			#      never close the live DB connection while a shielded persist
			#      is mid-write (FUNDS-AT-RISK);
			#  (7) final "shutting down" alert to the live ops channel — LAST
			#      (signal/cancel path only; the existing ``notify`` path —
			#      G's CR-1 channel convergence is out of F2 scope).
			#
			# Steps (2)/(4)/(5)/(6) are the pre-existing F1 drain effects in
			# the SAME relative order, with step (3) inserted before close and
			# steps (1)/(7) added signal-only. For paper (no live runtime, an
			# always-empty in-flight registry, ``_shutdown_via_cancel`` False
			# on the non-signal path) this is byte-identical to F1's drain:
			# step (1)/(7) are skipped, step (3) is a no-op
			# (``drain_inflight_sections`` returns immediately on an empty
			# set), and (2)/(4)/(5)/(6) are exactly F1's
			# save_state→cancel→gather→close→capture-close sequence.
			# ===============================================================
			log.info("Shutting down engine")

			# (1) operator-kill FIRST (signal/cancel drain only — paper
			# byte-exact: the non-signal path must NOT set it).
			if _shutdown_via_cancel:
				_OPERATOR_KILL.activate()
				log.info(
					"shutdown drain: operator-kill set — gate now rejects new "
					"entries via KILL_OPERATOR for the duration of the drain "
					"(§4.3 step 1)"
				)

			# (2) stop WS/dispatch intake. The WS loop already exited into
			# this finally; drop the socket ref so nothing re-enters dispatch.
			ws_ref[0] = None

			# (2 cont.) flush per-strategy state — part of "stop intake": no new tick will mutate it; persist BEFORE the in-flight drain (do NOT move past (3) drain_inflight_sections / (6) store.close()).
			for strat in strategies:
				state = pending_states.get(strat.name)
				if state is not None:
					store.save_state(strat.name, state)

			# (3) await the §4.2-L2 in-flight place→persist registry to
			# completion — STRICTLY BEFORE store.close() (step 6). The DRAIN
			# owns this await. No-op on an empty registry (paper always; live
			# steady-state when no entry is mid-flight at SIGTERM).
			await drain_inflight_sections()

			# (4) cancel B's loops + base tasks (CancelledError-safe).
			for task in tasks:
				task.cancel()
			# (5) gather.
			await asyncio.gather(*tasks, return_exceptions=True)
			# (6) store.close() exactly once — STRICTLY AFTER step (3).
			store.close()
			capture_writer.close()

			# (7) final "shutting down" alert — LAST, signal/cancel only
			# (paper byte-exact: the non-signal path emits no new notify).
			if _shutdown_via_cancel:
				notify(
					"🛑 **edge-catcher engine shutting down** — SIGTERM "
					"drain complete (in-flight place→persist sections drained, "
					"trade store closed)"
				)


async def _ws_loop(
	config: dict,
	market_state: MarketState,
	store: TradeStore,
	strategies: list[Strategy],
	strat_by_series: dict[str, list[Strategy]],
	pending_states: dict[str, dict],
	active_series: list[str],
	client: httpx.AsyncClient,
	ws_ref: list,
	dirty: set[str],
	executor: Executor,
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

			# Capture the wall clock ONCE per message. The SAME `now` is passed
			# to BOTH the capture writer's recv_ts AND the dispatch now
			# parameter so replay reads back the exact same timestamp the
			# live engine used for record_trade / exit_trade. Without this,
			# the writer's internal clock read and the dispatch's clock read
			# are microseconds apart and every trade row diverges on
			# entry_time / exit_time. See spec §4.7.
			now = datetime.now(timezone.utc)

			# Tee point 1/4 — capture BEFORE dispatch so a dispatch failure
			# can't lose the message from the capture log. The writer never
			# raises into this loop (verified by test_write_ws_never_raises_*).
			if capture_writer is not None:
				capture_writer.write_ws(msg, recv_ts=now)

			try:
				await dispatch_message(
					{"source": "ws", "payload": msg},
					config, market_state, store,
					strategies, strat_by_series, pending_states, dirty,
					executor,
					now=now,
				)
			except asyncio.CancelledError:
				# Cooperative cancellation must propagate so the outer reconnect
				# block (and ultimately run_engine's finally:) honour shutdown
				# promptly. Explicit handler mirrors the LiveExecutor.place
				# pattern; while ``except Exception`` below would NOT catch
				# CancelledError (BaseException subclass in Py3.8+), this clause
				# pins the behaviour against a future refactor that broadens the
				# catch to ``except BaseException``.
				raise
			except KillSwitchTripFailed:
				# C-spec L214 ghost-reject defense — full chain. process_tick
				# already re-raised past _handle_signal's broad except; we must
				# NOT swallow here either, otherwise the engine continues to the
				# next message, the gate re-evaluates with no kill row persisted,
				# and the previously-blocked trade goes through with real money.
				# Propagate to the outer reconnect block (which also re-raises
				# it) so run_engine terminates.
				raise
			except RecordPendingFailed:
				# B / PR 5 ghost-reject defense — full chain, mirrors
				# KillSwitchTripFailed. process_tick already re-raised past
				# _handle_signal's broad except; swallowing here would let the
				# engine continue to the next WS message with a funds-at-risk
				# Kalshi order stranded and no local row for B's reconciler.
				# Propagate to the outer reconnect block (which also re-raises
				# it) so run_engine terminates.
				raise
			except Exception:
				log.exception("Error dispatching WS message (type=%s)", msg.get("type"))


