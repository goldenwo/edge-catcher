"""Pure dispatch + handlers for engine input events.

This module is called by both the live engine (engine.py message loop) and —
once Phase 6 lands — the replay backtester (replay/backtester.py). Both pass
the same event shape through the same handlers, which is what makes byte-equal
parity possible between the two paths.

No globals. No I/O beyond what the handlers already did while inside engine.py.
Async on the call graph that reaches `executor.place` (`process_tick`,
`dispatch_message`, ticker/trade handlers, signal pipeline are coroutines);
other handlers (orderbook, synthetic) remain sync. Pure routing + state
mutation + store writes.

Invariants (see capture/replay spec §4.7):
  * `now: datetime` is threaded from the caller (WS loop, settlement poller,
    or replay dispatcher) all the way to `store.record_trade` /
    `store.exit_trade` / `store.settle_trade`. NO handler reads
    `datetime.now()` internally.
  * Handlers are relocated from engine.py verbatim; their logic is unchanged.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal, cast

from edge_catcher.engine.execution import _make_client_order_id, build_entry_order, entry_spread_too_wide
from edge_catcher.engine.executor import Executor, OrderRequest, OrderResult
from edge_catcher.engine.market_state import (
	MarketState,
	OrderbookSnapshot,
	TickContext,
	_is_tradeable_cents,
	derive_event_ticker,
)
from edge_catcher.engine.metrics import Metrics, _GATE_REJECT_COUNTER
from edge_catcher.engine.notifications import notify
from edge_catcher.engine.strategy_base import Signal, Strategy
from edge_catcher.engine.trade_store import TradeStoreProtocol

if TYPE_CHECKING:
	# Gate lives in engine/risk.py (Agent A's scope, PR 3/6).
	# Import only for type-checking so dispatch doesn't fail to import when
	# risk.py is absent (e.g. paper-trader, replay, tests that run without it).
	from edge_catcher.engine.risk import Gate, RiskContext
	# RiskContextProvider builds one RiskContext per signal (live only; C3).
	# TYPE_CHECKING-only import: same rationale as Gate — paper/replay paths
	# never instantiate this and must not fail when risk_context_provider.py
	# is absent. The param is annotated as the class name; mypy resolves it
	# from this block; at runtime the annotation is a string (from __future__
	# import annotations at the top of the file).
	from edge_catcher.engine.risk_context_provider import RiskContextProvider


# Hard ceiling on a single ``await executor.place(req)`` call. Set to
# 2 × live/config.py:http_timeout_seconds (default 30s) so KalshiOrderClient
# has room for one full HTTP-timeout retry cycle while the engine still bails
# out before an infinite-retry pathology can block the WS message loop.
_ENTRY_PLACEMENT_TIMEOUT_SECONDS = 60

# KillSwitchTripFailed is imported at runtime (not under TYPE_CHECKING) because
# process_tick's except clause needs the actual class. Use a try/except so
# dispatch.py still imports cleanly when risk.py is absent (paper/replay).
try:
	from edge_catcher.engine.risk import KillSwitchTripFailed  # noqa: PLC0415
except ImportError:
	# Sentinel: a class no exception will ever be (so the except clause is a no-op).
	class KillSwitchTripFailed(Exception):  # type: ignore[no-redef]
		pass

# RecordPendingFailed mirrors KillSwitchTripFailed (B / PR 5 ghost-reject
# defense): a failed record_pending/record_open INSERT means a funds-at-risk
# Kalshi order is stranded with no local row. process_tick must re-raise it
# past the broad per-signal except so the engine STOPS rather than re-entering
# the gate against unchanged DB state. Same runtime-import + sentinel pattern
# so dispatch.py still imports when live.state is absent (paper/replay).
try:
	from edge_catcher.live.state import RecordPendingFailed  # noqa: PLC0415
except ImportError:
	class RecordPendingFailed(Exception):  # type: ignore[no-redef]
		pass

# Allow / Reject / GateDecision are needed at runtime by _inc_gate_metric
# (dispatch-side counter translation, spec §4.2). Same try/except pattern so
# dispatch still imports cleanly on the paper/replay path where risk.py is
# absent (though _inc_gate_metric is only called from the live gate path).
try:
	from edge_catcher.engine.risk import Allow, GateDecision, Reject  # noqa: PLC0415
except ImportError:
	Allow = None  # type: ignore[assignment,misc]
	Reject = None  # type: ignore[assignment,misc]
	GateDecision = None  # type: ignore[assignment,misc]

log = logging.getLogger(__name__)


# ===========================================================================
# Sub-project E / §4.2 L2 + §4.3 — money-safe SIGTERM drain primitives.
#
# Two engine-scoped objects the §4.3 graceful-shutdown drain (in
# ``run_engine``'s ``finally:``) depends on:
#
#  1. ``_INFLIGHT_SECTIONS`` — the in-flight-section registry. ``_handle_enter``
#     wraps EXACTLY the place→persist critical region (one IOC round-trip + one
#     local sync write) in an ``asyncio.shield`` whose underlying task is
#     registered here on entry and removed on completion (INCLUDING on
#     exception). The §4.3 drain step (3) explicitly AWAITS every registered
#     section to completion *before* ``store.close()`` — a SIGTERM landing
#     EXACTLY between ``executor.place()`` returning ``filled`` and the local
#     ``record_trade`` persist therefore cannot orphan a real-money position
#     (the shielded task finishes the persist; the drain waits for it). Naive
#     ``await asyncio.shield(x)`` is FORBIDDEN by the spec (it does NOT
#     guarantee completion if the awaiting task is cancelled) — the DRAIN owns
#     the await, via :func:`drain_inflight_sections`.
#
#  2. ``_OPERATOR_KILL`` — the engine-scoped operator-kill flag. §4.3 drain
#     step (1) sets it FIRST (before awaiting the in-flight registry) so the
#     risk gate rejects any NEW entry via ``KILL_OPERATOR`` and no new section
#     enters the registry mid-drain. This is the concrete realization of the
#     B/C→E ``RiskContext.operator_kill_active`` contract (risk.py:683): the
#     value the per-call ``RiskContext`` reads is sourced from this flag. (The
#     dispatch-side ``gate_entry`` call is deferred past E by design — see
#     ``_handle_signal``'s docstring — so the load-bearing F2 guarantee that
#     actually prevents new registry entries during the drain is step (1) here
#     + step (2) stop-intake; the flag is wired so the gate is correct the
#     moment that deferred call lands, with zero further change.)
#
# §9 G-parity: PAPER replay/backtest/CI never SIGTERM and ``PaperExecutor.place``
# is synchronous, so the shield over an already-resolving sync path adds no
# observable behavior (the section registers and immediately deregisters within
# the one ``_handle_enter`` call with no suspension the paper path exercises),
# and the operator-kill flag is only ever set by the signal-driven drain. The
# non-signal path is byte-identical.
# ===========================================================================

# The currently-shielded place→persist section tasks. A set keyed by task
# identity; ``_handle_enter`` add()s its shielded task on entry and discard()s
# it on completion (incl. exception). The §4.3 drain awaits these.
_INFLIGHT_SECTIONS: set[asyncio.Task[Any]] = set()


class _OperatorKill:
	"""Engine-scoped operator-kill flag (the §4.3 step-1 / B-C→E
	``RiskContext.operator_kill_active`` source of truth).

	One process-lifetime instance (``_OPERATOR_KILL``). ``activate()`` is
	called by the §4.3 drain as its FIRST step; once active it stays active
	(tripped-kill ≠ process exit is F3's scope — F2 only SETS it). The
	per-gate-call ``RiskContext`` reads :attr:`active` so a NEW entry during
	the drain is rejected with ``KILL_OPERATOR``.
	"""

	__slots__ = ("active",)

	def __init__(self) -> None:
		self.active: bool = False

	def activate(self) -> None:
		"""Idempotently mark operator-kill active (§4.3 drain step 1)."""
		self.active = True


# Process-lifetime singleton. Module-scoped (NOT engine-instance-scoped):
# there is exactly one engine per process (one ``asyncio.run`` root), and the
# ``RiskContext`` construction reads it without threading a new param through
# every handler — the same module-global, wired-at-boot convention as
# ``_INFLIGHT_SECTIONS`` (no per-call parameter threading required).
_OPERATOR_KILL = _OperatorKill()


async def drain_inflight_sections() -> None:
	"""Await EVERY registered in-flight place→persist section to completion.

	The §4.3 drain step (3): the DRAIN owns the await (NOT a naive
	``await asyncio.shield(...)`` inside ``_handle_enter`` — that does not
	guarantee completion when the awaiting task is cancelled). Each section is
	an ``asyncio.shield``-protected task that finishes its one IOC round-trip +
	one local write regardless of the cancel; this gathers them so the durable
	persist has happened BEFORE ``store.close()`` (step 6). ``return_exceptions``
	keeps a single section's failure from masking the rest of the drain (the
	failed section's own row-state is the reconciler's concern, not the drain's).
	Snapshot the set first — a section deregisters itself as it completes, which
	would mutate the set during iteration.
	"""
	sections = list(_INFLIGHT_SECTIONS)
	if not sections:
		return
	log.info(
		"shutdown drain: awaiting %d in-flight place→persist section(s) to "
		"completion before store.close() (§4.3 step 3)",
		len(sections),
	)
	await asyncio.gather(*sections, return_exceptions=True)


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


def _inc_gate_metric(metrics: Metrics, decision: "GateDecision") -> None:
	"""Increment the per-decision risk-gate counter (dispatch-side; the gate
	holds no Metrics handle — spec §4.2). Entry-gate only."""
	if isinstance(decision, Allow):
		metrics.inc("risk_gate_allowed")
	else:  # Reject
		metrics.inc(_GATE_REJECT_COUNTER[decision.reason])


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
	mode_label: str = "PAPER",
) -> tuple[str, str]:
	"""Format an ENTER event for log + Discord.

	``mode_label`` is the operator-facing mode of record ("PAPER"/"LIVE"),
	a pure presentation token resolved by the caller from ``config`` (the
	G1-blessed mode-of-record lookup — NOT a trade-path mode branch). It
	defaults to ``"PAPER"`` so a caller that omits it can never accidentally
	emit a LIVE alert (money-safe fail-safe); the production dispatch call
	site always passes it explicitly.
	"""
	side_label = "YES" if side == "yes" else "NO"
	tag = f"{strategy} | {series}"
	cost = fill_size * entry_price
	log_line = (
		f"ENTER {strategy} {side} {ticker} {fill_size}x@{entry_price}c "
		f"cost={cost}c [id={trade_id}]"
	)
	notify_line = (
		f"{bullet} **[{tag}] {mode_label} BUY {side_label}** — "
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

async def process_tick(
	ctx: TickContext,
	strategies: list[Strategy],
	store: TradeStoreProtocol,
	config: dict,
	executor: Executor,
	*,
	now: datetime,
	risk: Gate | None = None,
	risk_ctx_provider: RiskContextProvider | None = None,
) -> None:
	"""Run every enabled strategy against the current tick context.

	For each strategy:
	  - Call on_tick → get signals
	  - Process enter/exit signals with exception isolation per-signal

	This is the async, testable core of the engine. Strategy.on_tick remains
	sync (signal generation is pure logic) — only signal handling is async
	because `_handle_enter` awaits the executor's network call.

	`now` is the wall-clock timestamp for this tick, captured once in the WS
	message loop (or equivalent replay source) and threaded down so that every
	trade row written during this call has an identical entry_time/exit_time.
	Required for byte-equal parity between live and replay backtester paths.

	`executor` is the engine's pluggable execution endpoint — `PaperExecutor` for
	paper-trader, `LiveExecutor` (added by sub-project D) for live. Both implement
	the `Executor` Protocol with async `place(req) -> OrderResult`.

	`risk` is the Gate instance for live trading (Sub-project C), or None for
	paper-trader and replay paths. When None, the gate is a no-op: every signal
	proceeds to executor.place without a gate check. Construction of Gate is
	gated on `executor_kind == "live"` in engine.py (E's wiring point).

	`risk_ctx_provider` builds one `RiskContext` per signal (live only; C3).
	Paired with `risk`: both set in live, both None in paper/replay.
	"""
	for strategy in strategies:
		try:
			signals = strategy.on_tick(ctx)
		except Exception:
			log.exception("Strategy %s raised on on_tick for %s", strategy.name, ctx.ticker)
			continue

		for signal in signals:
			try:
				await _handle_signal(
					signal, ctx, store, config, executor, strategy.emoji,
					now=now, risk=risk, risk_ctx_provider=risk_ctx_provider,
					protective_stop_cents=getattr(strategy, "default_params", {}).get("stop_loss"),
				)
			except KillSwitchTripFailed:
				# C-spec L214 ghost-reject defense: kill-switch INSERT failure
				# means the kill is NOT persisted (funds-at-risk). The engine
				# loop catches this above and STOPS rather than letting the
				# next tick re-enter ungated. This re-raise is the structural
				# enforcement of E's gate-wiring contract — must NOT be
				# swallowed by the broad except below.
				raise
			except RecordPendingFailed:
				# B / PR 5 ghost-reject defense: record_pending/record_open
				# INSERT failed, so a funds-at-risk Kalshi-side order is now
				# stranded with NO local row for B's reconciler to find.
				# Swallowing here would let the next tick re-enter the gate
				# against unchanged DB state. Must propagate (mirrors
				# KillSwitchTripFailed) so the WS loop + outer reconnect block
				# terminate run_engine instead of continuing past a failed
				# persistence. Placed before the broad except below for the
				# same reason.
				raise
			except Exception:
				log.exception(
					"Error handling %s signal from %s for %s",
					signal.action, signal.strategy, signal.ticker,
				)


def _consult_entry_gate(
	risk: Gate,
	signal: Signal,
	rctx: "RiskContext",
	metrics: Metrics,
) -> tuple[int | None, bool]:
	"""Call gate_entry and translate the GateDecision into dispatch primitives.

	Extracted from ``_handle_signal`` so the ``isinstance`` check lives here, not
	inside ``_handle_signal`` (an AST-level structural guard in
	``test_live_exit_settlement_routing.py`` forbids ``isinstance`` in
	``_handle_signal`` because it serves as a proxy for mode-discriminator
	branches — any ``isinstance`` in that function would trip the assertion).

	Returns:
		``(allowed_size, rejected)`` where ``allowed_size`` is the contract count
		from ``Allow.size_contracts`` (or ``None`` on paper path — never reached
		from this function) and ``rejected`` is ``True`` when ``gate_entry``
		returned a ``Reject`` (caller must return immediately without placing).

	May raise ``KillSwitchTripFailed`` — callers must NOT catch it.
	"""
	decision = risk.gate_entry(signal, rctx)  # may raise KillSwitchTripFailed — do NOT catch
	_inc_gate_metric(metrics, decision)
	if isinstance(decision, Reject):
		log.info(
			"Gate REJECT enter %s %s: %s (%s)",
			signal.strategy, signal.ticker, decision.reason, decision.detail,
		)
		return None, True
	return decision.size_contracts, False


def _consult_exit_gate(
	risk: Gate,
	signal: Signal,
	rctx: "RiskContext",
) -> bool:
	"""Consult gate_exit (operator-kill full-stop, spec §6). Returns True when the
	exit is BLOCKED (operator kill active) and the caller must return without exiting.

	Extracted from ``_handle_signal`` so the ``isinstance(Reject)`` check lives here,
	not inside ``_handle_signal`` (an AST-level structural guard in
	``test_live_exit_settlement_routing.py`` forbids ``isinstance`` in
	``_handle_signal`` because it serves as a proxy for mode-discriminator branches —
	any ``isinstance`` in that function would trip the assertion).

	gate_exit is pure and never raises; ``Allow.size_contracts`` is a proxy size and
	is intentionally discarded — the real exit size comes from the trade row inside
	``_handle_exit``.
	"""
	decision = risk.gate_exit(signal, rctx)
	if isinstance(decision, Reject):  # KILL_OPERATOR only (spec §6)
		log.info(
			"Gate REJECT exit %s %s: %s (%s)",
			signal.strategy, signal.ticker, decision.reason, decision.detail,
		)
		return True
	return False


def _enrich_live_entry_signal(signal: Signal, ctx: TickContext, protective_stop_cents: int | None = None) -> None:
	"""Populate the execution fields the LIVE entry path needs but strategies
	don't emit.

	Strategies stay framework-agnostic: they pick side/ticker and a reason; the
	engine owns execution mechanics. The PAPER path already derives the entry
	price from the tick inside ``_handle_enter`` (``entry_price = ctx.yes_ask``
	for a yes-side entry). The LIVE path's gate (``gate_entry``, which Rejects
	INVALID_SIGNAL when ``entry<=0 or sl<=0``) AND order builder
	(``build_entry_order``, which raises on a missing field) both REQUIRE
	``entry_price_cents`` + ``stop_loss_distance_cents`` on the signal — so
	derive them here, ONCE, before the gate, from the SAME tick price paper
	books at, so live and paper enter at the same limit.

	Only fills fields the strategy left ``None`` — a strategy that DOES populate
	them (a future limit-below-ask entry, or an explicit stop) keeps its values.

	``stop_loss_distance_cents`` defaults to the entry cost: on a binary contract
	with no hard stop the worst case is losing the full entry, so the cost basis
	IS the per-contract risk. That makes the fixed-fraction sizing arm
	(``equity * sizing_pct / sl_cents``, risk.py) reduce to
	``equity * sizing_pct / entry`` — pure-fractional capital allocation, the
	Phase-1 sizing intent. Caller MUST gate this on ``risk is not None`` so the
	paper/replay path never enriches (G-parity: paper signals stay byte-exact).
	"""
	if signal.entry_price_cents is None:
		signal.entry_price_cents = ctx.yes_ask if signal.side == "yes" else ctx.no_ask
	if signal.stop_loss_distance_cents is None:
		signal.stop_loss_distance_cents = signal.entry_price_cents
	if signal.protective_stop_cents is None:
		signal.protective_stop_cents = protective_stop_cents


async def _handle_signal(
	signal: Signal,
	ctx: TickContext,
	store: TradeStoreProtocol,
	config: dict,
	executor: Executor,
	bullet: str = "🔵",
	*,
	now: datetime,
	risk: Gate | None = None,
	risk_ctx_provider: RiskContextProvider | None = None,
	protective_stop_cents: int | None = None,
) -> None:
	"""Dispatch a single signal — enter or exit.

	`_handle_enter` and `_handle_exit` are both async — each awaits the
	executor's `place()` network call (the §1 executor seam).

	Gate consultation (Sub-project C, wired by C3):
	  Entry signals are gated BEFORE building/placing the order. `risk` is the
	  Gate instance constructed by E when `executor_kind == "live"`; for paper-
	  trader and replay paths, `risk` is None and the gate is a no-op.

	  Live path: `_consult_entry_gate` calls `gate_entry(signal, rctx)`. On
	  Allow, `_handle_enter` is called with `allowed_size=decision.size_contracts`,
	  which builds the sized order via `build_entry_order` internally. On Reject,
	  log at INFO and return — no order placed, no notify (spec §4.1: routine
	  rejects are silent; audit/Discord routing is E's RiskEvent contract, CR-1).

	  Paper/replay (`risk is None`): `_handle_enter` is called with
	  `allowed_size=None` — the ungated paper path; `PaperExecutor` sizes
	  internally. Byte-identical to pre-C3 behaviour.

	  Exit signals bypass the entry gate — exits are always allowed even when
	  auto-kills are active (kills cap new exposure; they don't trap existing
	  exposure).  However, exits ARE subject to the operator-kill full-stop
	  (spec §6), wired via ``_consult_exit_gate``: the operator kill halts
	  BOTH new entries and exits, whereas auto-tripped caps (drawdown/daily/
	  panic) only ever block entries.
	"""
	# LIVE only: build one RiskContext per signal (spec §3), reused by whichever
	# gate the action routes to. Paper/replay never builds one (risk is None) —
	# G-parity: the paper path is BYTE-IDENTICAL; no RiskContext construction,
	# no gate call, no conditional divergence on this path.
	rctx = None
	if risk is not None:
		# risk and risk_ctx_provider are wired together at composition (G1) —
		# both set in live, both None in paper/replay. Assert the pairing so a
		# mis-wire fails loudly rather than NoneType.build at the first signal.
		assert risk_ctx_provider is not None
		rctx = risk_ctx_provider.build(signal, now)

	if signal.action == "enter":
		# Entry gate — live path only (spec §2.1). Paper/replay (risk is None)
		# short-circuits to allowed_size=None and calls _handle_enter ungated,
		# preserving the byte-exact paper path.
		#
		# On Allow: proceed with allowed_size = decision.size_contracts.
		# On Reject: log at INFO and return — NO notify() (spec §4.1: routine
		#   rejects are silent; audit/Discord routing is E's RiskEvent contract).
		# KillSwitchTripFailed: do NOT catch — let it propagate so the engine
		#   STOPS rather than re-entering ungated (C-spec L214 ghost-reject
		#   defense). process_tick already re-raises it past the broad except.
		allowed_size: int | None = None  # paper sentinel — None = ungated paper path
		if risk is not None:
			# LIVE only: strategies emit framework-agnostic enter signals (no
			# execution price/stop). Derive both from the tick BEFORE the gate
			# AND build_entry_order consume them. Paper derives the price itself
			# in _handle_enter and runs no gate, so this is live-only and the
			# paper path stays byte-exact (G-parity).
			_enrich_live_entry_signal(signal, ctx, protective_stop_cents)
			# _consult_entry_gate holds the isinstance(Reject) check so this
			# function stays free of it (structural AST guard in test suite).
			# KillSwitchTripFailed propagates untouched — do NOT catch here.
			assert rctx is not None  # invariant: set above whenever risk is not None
			allowed_size, rejected = _consult_entry_gate(
				risk, signal, rctx, config.get("_metrics") or Metrics(),
			)
			if rejected:
				return
		await _handle_enter(signal, ctx, store, config, executor, bullet, now=now, allowed_size=allowed_size)
	elif signal.action == "exit":
		# SC-D3 (E3): _handle_exit is now async (it awaits executor.place for
		# the exit order — the §1 seam: PaperExecutor resolves synchronously /
		# LiveExecutor places a real IOC + B's async path owns the close) and
		# receives executor/config UNCONDITIONALLY (no mode branch — the
		# executor absorbs the live-vs-paper difference; paper close stays
		# byte-EXACT via the unconditional store.exit_trade).
		#
		# D1 (spec §6): gate_exit is now wired here for live mode. ONLY the
		# operator kill (KILL_SWITCH env or SIGTERM-driven _OperatorKill) blocks
		# exits — it is a true full-stop that halts BOTH new entries AND exits.
		# Auto-tripped caps (drawdown/daily/panic) do NOT block exits because
		# exits REDUCE risk; trapping existing exposure would be worse. The
		# isinstance(Reject) check lives in _consult_exit_gate (not here) to
		# keep _handle_signal free of isinstance (AST structural guard).
		# Paper/replay (risk is None): unconditional _handle_exit — G-parity.
		if risk is not None:
			assert rctx is not None  # invariant: set above whenever risk is not None
			if _consult_exit_gate(risk, signal, rctx):
				return  # operator-kill full-stop (spec §6): exit blocked
		closed = await _handle_exit(
			signal, ctx, store, bullet, now=now,
			executor=executor, config=config,
		)
		# Ratchet the closed-equity peak ONLY on a CONFIRMED persisted close
		# (spec §3.3b). Build a FRESH post-close context so open_positions
		# excludes the just-closed trade (no over-count). LIVE only (risk is
		# None in paper/replay → no-op, byte-exact G-parity). _handle_exit
		# returns False on partial/no-fill/timeout (row left open) — no
		# ratchet on a non-close (the B2 guard).
		if closed and risk is not None:
			assert risk_ctx_provider is not None
			risk.record_trade_close(risk_ctx_provider.build(None, now))
	else:
		log.warning("Unknown signal action '%s' from %s", signal.action, signal.strategy)


async def _handle_enter(
	signal: Signal,
	ctx: TickContext,
	store: TradeStoreProtocol,
	config: dict,
	executor: Executor,
	bullet: str = "🔵",
	*,
	now: datetime,
	allowed_size: int | None = None,
) -> None:
	"""Process an entry signal: build OrderRequest, call executor, route by status."""
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

	# Live-only spread gate (spec 2026-05-25-live-spread-entry-gate v2). strat-34
	# is an IOC taker: buys ask, marks bid, so a fill starts -(spread) underwater;
	# spread >= the strategy's stop stops it out on entry (the proven cause of the
	# 2026-05-25 cutover loss). `protective_stop_cents` carries the strategy's real
	# stop (NOT stop_loss_distance_cents, which is the sizing basis). LIVE-ONLY: the
	# `allowed_size is not None` guard keeps paper/replay byte-exact. Skip is before
	# `entries_attempted` (a skip is not an attempt), mirroring the degenerate guard.
	if allowed_size is not None and signal.protective_stop_cents is not None:
		exec_cfg = config["_exec_cfg"]
		spread = ctx.yes_ask - ctx.yes_bid
		if entry_spread_too_wide(spread, signal.protective_stop_cents,
		                         exec_cfg.entry_spread_stop_buffer_cents):
			metrics.inc("entries_skipped_wide_spread")
			log.info(
				"Skip: wide spread %dc >= stop %dc - buffer %dc for %s %s",
				spread, signal.protective_stop_cents,
				exec_cfg.entry_spread_stop_buffer_cents, signal.side, signal.ticker,
			)
			return

	metrics.inc("entries_attempted")

	# Build typed request — EXACTLY ONCE (spec §2.2 single-build invariant).
	# Both consumers below (record_intent, executor.place via _place_and_persist)
	# share the ONE req object so client_order_id is identical across both calls.
	#
	# LIVE path (allowed_size is not None): build_entry_order applies taker-with-
	# cap slippage and generates a fresh uuid4-suffixed client_order_id internally.
	# Calling it twice would produce two DIFFERENT ids — DO NOT call it again.
	#
	# PAPER/replay path (allowed_size is None): byte-exact unchanged construction.
	# PaperExecutor's resolve_fill computes the actual size_contracts from
	# config["sizing"]["risk_per_trade_cents"] / entry_price; dispatch defers
	# sizing to the executor's internal pipeline on this path.
	#
	# Signal.side is typed as plain `str` for strategy-author ergonomics
	# (strategies build sides from data); OrderRequest.side narrows to
	# Literal["yes", "no"]. Cast at the boundary — pre-G dispatch did no
	# runtime validation here, so neither do we (byte-exact preservation).
	if allowed_size is not None:  # LIVE — sized pre-executor build (spec §2.2)
		exec_cfg = config["_exec_cfg"]
		req = build_entry_order(signal, allowed_size, exec_cfg, now)
	else:  # PAPER/replay — byte-exact unchanged; executor sizes internally
		req = OrderRequest(
			ticker=signal.ticker,
			series=signal.series,
			side=cast(Literal["yes", "no"], signal.side),
			size_contracts=0,
			limit_price_cents=entry_price,
			strategy=signal.strategy,
			client_order_id=_make_client_order_id(signal.strategy, signal.ticker, now),
		)

	# Dual-slippage book-best reference (spec §4.2): top-of-book implied
	# ASK in cents for the side being bought (100 − best opposite-side
	# bid — yes_levels/no_levels are resting BIDS), persisted on the live
	# pending row so transition_pending_to_open can compute
	# market_impact_cents at fill.  Empty opposite side / missing
	# orderbook → None per §4.3 ("not measurable", never 0).  The
	# isinstance guard handles test ad-hoc ctx classes that omit
	# `orderbook` or supply a MagicMock — production TickContext.orderbook
	# is always a real OrderbookSnapshot.  Reporting-only path;
	# record_intent below stays fail-loud (§3.1 RecordPendingFailed).
	_orderbook = getattr(ctx, "orderbook", None)
	if isinstance(_orderbook, OrderbookSnapshot):
		entry_best_price_cents: int | None = (
			_orderbook.best_yes_ask if signal.side == "yes"
			else _orderbook.best_no_ask
		)
	else:
		entry_best_price_cents = None

	# Pre-place durability hook (sub-project E / L1; spec §3 keystone + §3.1).
	# Called UNCONDITIONALLY — no mode branch. The TradeStoreProtocol absorbs
	# the paper/live difference: paper + InMemory record_intent is a strict
	# no-op (return None — byte-exact-invisible to the parity sweep, §9), the
	# live store durably INSERTs a `pending` row keyed by client_order_id
	# BEFORE any order is sent. That makes a severed place→persist recoverable
	# by B's reconciler via client_order_id — "no untracked real-money
	# position" holds even if async code downstream is imperfect.
	#
	# 🚨 §3.1 FATAL: a live record_intent INSERT failure raises
	# RecordPendingFailed. There is intentionally NO try/except here — it
	# propagates UNCAUGHT so the entry ABORTS BEFORE `await executor.place`
	# (nothing was sent ⇒ nothing at risk ⇒ a hard engine stop strands
	# nothing — STRONGER than the post-place ghost-reject). It reaches
	# process_tick's `except RecordPendingFailed: raise` (mirroring
	# KillSwitchTripFailed) which halts the engine rather than re-entering
	# the gate against unchanged DB state. intended_size reflects
	# req.size_contracts, which is the real sized count on the LIVE path and
	# 0 on the PAPER/replay path (where the executor sizes internally) —
	# same convention as the engine-timeout pending row below;
	# B's reconciler resolves the true filled size from Kalshi by client_order_id.
	# entry_price_cents is the ORIGINAL Signal intent (NOT D's slippage-
	# adjusted limit), matching the post-place record_pending contract.
	# `now` is the threaded tick clock (module invariant L14-L18: handlers
	# never read datetime.now()) so replay produces a byte-identical
	# placed_at_utc to the original live execution.
	# entry_best_price_cents + entry_limit_price_cents are dual-slippage
	# references (spec §4.2) persisted on the live pending row; paper/in-
	# memory ignore. entry_limit_price_cents = req.limit_price_cents — the
	# executor's actually-offered limit after taker-cap slippage, NOT
	# signal.entry_price_cents (which is the Signal's original intent).
	# Pairing matters: market_impact uses entry_best (vs top-of-book);
	# limit_slippage uses entry_limit (vs what we offered).
	store.record_intent(
		ticker=signal.ticker,
		series=signal.series,
		strategy=signal.strategy,
		side=signal.side,
		intended_size=req.size_contracts,
		entry_price_cents=signal.entry_price_cents,
		stop_loss_distance_cents=signal.stop_loss_distance_cents,
		client_order_id=req.client_order_id,
		placed_at_utc=now.isoformat(),
		entry_best_price_cents=entry_best_price_cents,
		entry_limit_price_cents=req.limit_price_cents,
	)

	# Hard cap on the executor call. ``LiveExecutor.place`` is supposed to
	# never raise (every error path returns a defined OrderResult), but a
	# bug in ``KalshiOrderClient``'s retry loop (infinite retry on a
	# particular error code, or a sub-timeout exceeding the engine tick
	# budget) would block this coroutine forever — and the entire WS message
	# loop blocks behind it because dispatch_message is awaited from _ws_loop's
	# ``async for raw in ws``. The 60s ceiling = 2 × http_timeout_seconds
	# (live/config.py default), leaving room for one retry cycle while
	# capping infinite-retry pathology. On timeout: synthesize a pending+None
	# OrderResult (Kalshi may still have received the POST; we don't know
	# the truth, so we don't lie) and let B's reconciler resolve it.
	#
	# ┌─ §4.2 L2 — the place→persist in-flight SHIELD (money-safety, maximal) ─┐
	# The single worst-case for live funds: a SIGTERM landing EXACTLY between
	# ``place()`` returning ``filled`` and the ``record_trade`` persist would
	# orphan a real-money Kalshi position (C1 ``pending`` row never CAS'd to
	# ``open``). L1's pre-place ``record_intent`` (above) already makes that
	# RECOVERABLE by B's reconciler; L2 is the common-case optimization that
	# AVOIDS the orphan window entirely: wrap EXACTLY the place→persist critical
	# region (one IOC round-trip already bounded by ``_ENTRY_PLACEMENT_TIMEOUT_
	# SECONDS`` + one local sync write — and NOTHING more) in an
	# ``asyncio.shield``-protected task registered in ``_INFLIGHT_SECTIONS``.
	# The §4.3 drain (run_engine's finally, step 3) explicitly AWAITS that
	# registry to completion BEFORE ``store.close()`` — so even though THIS
	# awaiting frame may be cancelled by the SIGTERM, the shielded task still
	# finishes the persist and the drain waits for it. The DRAIN owns the await
	# (``drain_inflight_sections``); a naive ``await asyncio.shield(x)`` HERE is
	# forbidden by the spec (it does NOT guarantee completion when this frame is
	# cancelled). §9 G-parity: ``PaperExecutor.place`` resolves synchronously,
	# so the shielded task runs to completion before the very next loop tick —
	# the section registers and immediately deregisters within this one
	# ``_handle_enter`` call with no suspension the paper path observes; the
	# shield is a no-op-equivalent and paper behaviour is byte-identical.

	async def _place_and_persist() -> tuple[OrderResult, int | None]:
		"""The §4.2-L2 critical region: ONE IOC round-trip + (if filled) the
		ONE immediate local persist. Bounded above by the existing
		``_ENTRY_PLACEMENT_TIMEOUT_SECONDS`` cap on ``place()``. Returns
		``(result, trade_id_or_None)`` so the (non-money) post-persist
		notify/lost-CAS observability stays OUTSIDE the shield, keeping the
		shielded span EXACTLY place+persist (spec: do not shield more)."""
		try:
			_result = await asyncio.wait_for(
				executor.place(req),
				timeout=_ENTRY_PLACEMENT_TIMEOUT_SECONDS,
			)
		except asyncio.TimeoutError:
			# NOTE: req.size_contracts is the real sized count on the LIVE path
			# and 0 on the PAPER/replay path (where the executor sizes
			# internally), so the synthesized pending row carries
			# intended_size=req.size_contracts accordingly. B's reconciler MUST
			# treat an engine_timeout pending row as "unknown — resolve the true
			# filled size from Kalshi by client_order_id", same as the
			# NetworkError-pending path. Flagged by the PR #38 pass-3 review
			# (G2); surfaced loudly so the reconciler never silently drops it.
			log.warning(
				"executor.place exceeded %ds for %s %s (client_order_id=%s, "
				"intended_size=%d) — synthesizing pending+None for B's reconciler "
				"to resolve via client_order_id",
				_ENTRY_PLACEMENT_TIMEOUT_SECONDS, signal.strategy, signal.ticker,
				req.client_order_id, req.size_contracts,
			)
			_result = OrderResult(
				status="pending",
				intended_size=req.size_contracts,
				filled_size=0,
				blended_entry_cents=0,
				fill_pct=0.0,
				slippage_cents=0,
				rejection_reason=f"engine_timeout:{_ENTRY_PLACEMENT_TIMEOUT_SECONDS}s",
				order_id=None,
			)

		_trade_id: int | None = None
		if _result.status == "filled":
			# Field-by-field match to the pre-G record_trade call shape — byte-exact
			# preservation is the parity-sweep success criterion. Called
			# UNCONDITIONALLY — NO mode branch (spec §1 keystone). The two identity
			# keys are ADDITIVE keyword-only args: paper + InMemory record_trade
			# accept-and-IGNORE them (byte-exact-invisible to the parity sweep,
			# §9 / C2); the live store CONSUMES them to CAS-transition the C1
			# pending row pending→open located by client_order_id, recording the
			# Kalshi order id (spec §3 `:400 filled` row / §4.2). kalshi_order_id
			# is result.order_id — the same field the pending arm threads at the
			# record_pending call below (executor.py `OrderResult.order_id`).
			_trade_id = store.record_trade(
				ticker=signal.ticker,
				entry_price=entry_price,
				strategy=signal.strategy,
				side=signal.side,
				series_ticker=signal.series,
				intended_size=_result.intended_size,
				fill_size=_result.filled_size,
				blended_entry=_result.blended_entry_cents,
				book_depth=_result.book_depth,
				fill_pct=_result.fill_pct,
				slippage_cents=_result.slippage_cents,
				book_snapshot=_result.book_snapshot,
				now=now,
				client_order_id=req.client_order_id,
				kalshi_order_id=_result.order_id,
				# Dual-slippage diagnostics (spec §4.2 / §9). Paper persists
				# both columns onto paper_trades (commit 023a9b5 + 27a7695);
				# live's record_trade is the CAS to transition_pending_to_open
				# which IGNORES both (live computes its own pair from the refs
				# persisted on the pending row — Step 10).
				market_impact_cents=_result.market_impact_cents,
				limit_slippage_cents=_result.limit_slippage_cents,
			)
		return _result, _trade_id

	# Register the shielded place→persist task in the in-flight registry the
	# §4.3 drain awaits, then await it via ``asyncio.shield``. If a SIGTERM
	# cancels THIS frame mid-region, the shield keeps ``_section`` running and
	# the drain (``drain_inflight_sections``) awaits it to completion before
	# close — the persist is NEVER skipped.
	#
	# Deregistration is bound to the SECTION's OWN completion via
	# ``add_done_callback`` — NOT a ``try/finally`` around this ``await``. That
	# distinction is load-bearing: when THIS frame is cancelled the section is
	# still running; a ``finally: discard`` here would yank it out of the
	# registry the drain awaits and reopen the orphan window. The done-callback
	# fires only when ``_section`` ITSELF finishes (normally OR with an
	# exception, INCLUDING if the section task itself is cancelled), so the
	# registry holds the section exactly until its persist is durable and never
	# leaks a completed task.
	_section: asyncio.Task[tuple[OrderResult, int | None]] = asyncio.ensure_future(
		_place_and_persist()
	)
	_INFLIGHT_SECTIONS.add(_section)
	_section.add_done_callback(_INFLIGHT_SECTIONS.discard)
	result, trade_id = await asyncio.shield(_section)

	if result.status == "filled":
		metrics.inc("entries_filled")

		# Type-narrow the documented invariant: a `filled` result ALWAYS carries
		# a persisted trade_id — `_place_and_persist` only returns a non-None
		# trade_id on the filled path (paper/InMemory record_trade INSERTs and
		# returns the rowid; live transition_pending_to_open CAS returns the
		# located row_id even on a lost race). NEVER fires for paper/replay, so
		# byte-exact (K2). Codifies the invariant for mypy + as a money-path
		# tripwire (durable_status below MAY still be None — get_trade_by_id can
		# find no row — and that is handled gracefully; trade_id itself is not).
		assert trade_id is not None, "filled result must carry a persisted trade_id"

		# Notify/log the ACTUAL DURABLE PERSISTED status, not the optimistic
		# IOC `filled` result — and do it MODE-AGNOSTICALLY (the §1 keystone:
		# branch on persisted truth, never on paper-vs-live / isinstance).
		#
		# §4.2 lost-CAS race: B's reconciler can transition the C1 row
		# pending→rejected_post_hoc (Kalshi-truth: TTL elapsed, list_orders
		# found no order) BEFORE this filled branch runs. The live
		# record_trade→transition_pending_to_open is a CAS on
		# `WHERE status='pending'`, so it correctly NO-OPs (B's _cas_update
		# never clobbers a non-pending/terminal row — the durable money state
		# is authoritative & untouched, exactly one row) but record_trade still
		# returns the located row_id. Firing the celebratory "filled" notify
		# here would be a FALSE operator alert for a row the durable record
		# holds as rejected. This is NOT a fund-loss (B's Kalshi-truth
		# reconciler owns the authoritative lifecycle — §4.2 is LOCKED; D2
		# does NOT change the money logic) — it is an operator-TRUST defect, so
		# this is NOT fatal (§3.1): NEVER raise, just notify/log distinctly.
		#
		# get_trade_by_id is mode-agnostic: paper + InMemory ALWAYS yield an
		# 'open' row for a just-record_trade'd id (paper INSERTs literal
		# 'open'), so the celebratory branch fires byte-identically for
		# paper/replay (mandatory K2 11/11 byte-exact). A non-'open' durable
		# status is only ever reachable in LIVE on a genuine lost-CAS race.
		durable = store.get_trade_by_id(trade_id)
		durable_status = durable.get("status") if durable is not None else None
		# Only a CONFIRMED non-'open' terminal status suppresses the
		# celebratory alert. 'open' (the normal case — CAS landed; paper
		# always) and the can't-happen-here None (record_trade located the row
		# by this id ⇒ get_trade_by_id finds it) preserve the pre-D2 behavior
		# byte-exactly.
		if durable_status is None or durable_status == "open":
			display_price = result.blended_entry_cents if result.blended_entry_cents else entry_price
			# Presentation-only mode-of-record lookup (the G1-blessed pattern
			# — reading config["executor"] for a display label is NOT a §1
			# keystone trade-path branch). config is already threaded here.
			mode_label = "LIVE" if config.get("executor") == "live" else "PAPER"
			log_line, notify_line = _format_enter_message(
				strategy=signal.strategy,
				series=signal.series,
				ticker=signal.ticker,
				side=signal.side,
				fill_size=result.filled_size,
				entry_price=display_price,
				trade_id=trade_id,
				bullet=bullet,
				mode_label=mode_label,
			)
			log.info(log_line)
			notify(notify_line)
		else:
			# Live lost-CAS race: B's Kalshi-truth reconciler already resolved
			# the C1 row to a terminal status. DO NOT fire the celebratory
			# "filled" notify; emit a DISTINCT non-celebratory record instead.
			# Uniform with the C3/C4/C5 lost-CAS observability taxonomy in
			# live/store.py (coid + actual status + §-cite + best-effort/not
			# fatal rationale). NEVER raise — the money state is already
			# authoritative & untouched (§4.2); this only corrects the alert.
			metrics.inc("entries_filled_lost_cas")
			lost_cas_msg = (
				"dispatch._handle_enter: IOC returned 'filled' but the durable "
				"row for client_order_id=%r is %r (kalshi_order_id=%r) — B's "
				"reconciler / Kalshi-truth is authoritative (spec §4.2); NOT "
				"recording as a live fill, NOT firing the 'filled' alert. "
				"§3.1 best-effort, not fatal (the durable money state is "
				"authoritative & untouched — exactly one row, owned by B)."
			)
			log.error(
				lost_cas_msg,
				req.client_order_id,
				durable_status,
				result.order_id,
			)
			notify(
				f"⚠️ **[{signal.strategy} | {signal.series}] LOST-CAS / "
				f"NOT FILLED** — `{signal.ticker}` IOC returned filled but the "
				f"durable order (`{req.client_order_id}`, kalshi=`{result.order_id}`) "
				f"is `{durable_status}` (B reconciler / Kalshi-truth authoritative, "
				f"§4.2); not recorded as a live fill"
			)
	elif result.status == "rejected":
		if result.rejection_reason == "stale_book":
			metrics.inc("entries_skipped_stale")
		else:
			metrics.inc("entries_skipped_other")
		log.info(
			"No fill for %s %s %s (entry=%dc) — skipping (reason=%s)",
			signal.strategy, signal.side, signal.ticker, entry_price, result.rejection_reason,
		)
		# Persist a rejected row for operator triage + F's UI surface. Paper +
		# replay no-op; B's PR 5 implements the real SQLite write. ``stale_book``
		# is the only paper-side rejection_reason (its existing log+metric is
		# the source of truth for paper); LIVE rejections (kalshi_4xx,
		# absolute_max_exceeded, ioc_zero_fill, invalid_intended_size) need
		# durable audit beyond the rotating process log.
		if result.rejection_reason != "stale_book":
			store.record_rejected(
				ticker=signal.ticker,
				series=signal.series,
				strategy=signal.strategy,
				side=signal.side,
				intended_size=result.intended_size,
				entry_price_cents=signal.entry_price_cents,
				stop_loss_distance_cents=signal.stop_loss_distance_cents,
				client_order_id=req.client_order_id,
				placed_at_utc=now.isoformat(),
				rejection_reason=result.rejection_reason or "unknown",
			)
	elif result.status == "pending":
		# D's NetworkError or malformed-fills path. Paper never returns pending;
		# only live execution (D) produces this status.
		#
		# Funds-at-risk semantic: writing the pending row is REQUIRED for B's
		# reconciliation. If Kalshi accepted the order but A's HTTP layer lost
		# the response (NetworkError), B reconciles via client_order_id; if
		# Kalshi returned filled_count>0 with a malformed fills array, B
		# reconciles via kalshi_order_id. Skipping this branch — even on
		# NetworkError where order_id=None — strands the order on Kalshi with
		# no local record. Per D spec L657-L685; locked cross-PR contract
		# with B (see B spec L479-L502).
		#
		# Notification routing for pending rows is E's responsibility (CR-1).
		# Dispatch only writes the row; E reads OrderResult.status and routes.
		# entry_price_cents is the ORIGINAL Signal intent — NOT D's slippage-
		# adjusted limit_price_cents. Per D spec L679 + B's reconciliation
		# contract: B computes PnL on settlement against the strategy's
		# intended entry, not the walked-up limit. Pinned by
		# tests/test_engine_dispatch_pending_branch.py.
		store.record_pending(
			ticker=signal.ticker,
			series=signal.series,
			strategy=signal.strategy,
			side=signal.side,
			intended_size=result.intended_size,
			entry_price_cents=signal.entry_price_cents,
			stop_loss_distance_cents=signal.stop_loss_distance_cents,
			client_order_id=req.client_order_id,
			# kalshi_order_id: None on NetworkError; preserved on malformed-fills
			kalshi_order_id=result.order_id,
			# Use the threaded `now` (not datetime.now()) — see module
			# invariant at L14-L18: handlers must NOT read the wall clock
			# internally. During replay, `now` is sourced from the captured
			# bundle's recv_ts so replay produces a byte-identical
			# placed_at_utc to the original live execution.
			placed_at_utc=now.isoformat(),
			rejection_reason=result.rejection_reason,
		)
		metrics.inc("entries_pending")
	else:
		# Defensive exhaustiveness arm — the OrderResult.status Literal at
		# executor.py:65 enumerates {"filled","pending","rejected"} so static
		# type checking would catch a missing branch, but a new variant added
		# to the Literal without a matching dispatch branch (PR 5 / PR 6 risk)
		# would otherwise silently fall through with no audit row + no notify.
		# Loud log + metric surfaces the dispatch-side miss before live money
		# is affected.
		log.error(
			"dispatch._handle_enter: unhandled OrderResult.status=%r for %s %s — "
			"either a new status literal was added without a matching branch, "
			"or an executor returned an out-of-spec status. Funds-at-risk.",
			result.status, signal.strategy, signal.ticker,
		)
		metrics.inc("entries_unhandled_status")


async def _handle_exit(
	signal: Signal,
	ctx: TickContext,
	store: TradeStoreProtocol,
	bullet: str = "🔵",
	*,
	now: datetime,
	executor: Executor,
	config: dict,
) -> bool:
	"""Process an exit signal: place the exit via the executor, then close.

	SC-D3 (spec §10 / §3 `:534/:537` / §1 keystone — E3's deliverable, the
	controller-adjudicated R1 deferral from D3): the exit Signal is placed via
	``executor.place(exit_req)`` UNCONDITIONALLY (no mode branch — the executor
	IS the live-vs-paper seam, never a per-call ``isinstance``/mode test).

	* PAPER: ``PaperExecutor.place`` resolves the sell synchronously as a
	  deterministic exit-ACK (its fill fields are NOT consumed here); the
	  AUTHORITATIVE paper close remains the SAME synchronous
	  ``store.exit_trade(trade_id, ctx_bid)`` call dispatch has always made —
	  byte-EXACT vs pre-E3 (mandatory K2 11/11 G-parity; the paper store does
	  the won/lost/scratch + pnl arithmetic exactly as before).
	* LIVE: ``LiveExecutor.place`` places a real IOC sell on Kalshi; the
	  AUTHORITATIVE close is owned by B's async ``on_fill_event`` / reconciler
	  (started by E3's composition root in live mode). The unconditional
	  ``store.exit_trade`` below is then C5's IDEMPOTENT, NON-authoritative
	  backstop: live ``store.exit_trade`` → ``live.state.record_close`` CAS
	  (``exit_reason='ws_exit_fill'``) whose precondition
	  ``status IN ('open','exit_pending')`` makes it race SAFELY with B's
	  async path — whichever lands the CAS first wins, the other is a logged
	  no-op that NEVER raises (the §4.2-adjudicated C5/D2 benign-lost-CAS
	  property; B/Kalshi-truth is the authority + reconciler is the L3
	  backstop). The store/Protocol absorbs the live-vs-paper difference; this
	  function is mode-AGNOSTIC (§1).

	**F-PENDING REALITY (engine.py ~1566):** B's account-scope fill WS pump
	(``on_fill_event`` / ``on_order_status_event``) is sub-project F and is
	UNWIRED today. Until F ships, the LIVE close is owned by the sync
	``store.exit_trade`` here + the ``_settlement_poller`` + the
	pending/exit_pending reconciler — NOT ``on_fill_event``. The "races B's
	async path / B is the authority" framing above is the F-shipped END-STATE.
	"""
	if signal.trade_id is None:
		log.warning(
			"Exit signal from %s for %s has no trade_id — skipping",
			signal.strategy, signal.ticker,
		)
		return False

	# Selling hits the bid, not the ask
	exit_price = ctx.yes_bid if signal.side == "yes" else ctx.no_bid

	# Resolve the open position's size BEFORE the close (the close transitions
	# the row out of 'open'; reading after would see fill_size on a closed
	# row / miss it). get_trade_by_id is the mode-agnostic by-id read every
	# store implements (paper TradeStore / replay InMemory / live
	# SQLiteTradeStore) — NOT a mode branch. A missing/closed row ⇒ no
	# position to place an exit for; fall through to the (idempotent)
	# store.exit_trade which handles row-not-found / already-closed safely.
	pos_row = store.get_trade_by_id(signal.trade_id)
	exit_size = int((pos_row or {}).get("fill_size") or 0)

	# Captured from the exit place() so the close below is booked ONLY on a
	# venue-confirmed fill. None ⇒ no place attempted (no position) or timeout.
	exit_result: OrderResult | None = None
	if exit_size > 0:
		# Build the exit OrderRequest for the open position (action="sell";
		# limit = the bid we sell into, the same price the paper close books
		# at — so paper's executor-ACK and its store.exit_trade agree, and the
		# live IOC sells at the strategy's exit price). client_order_id is a
		# FRESH idempotency key (an EXIT order's coid intentionally matches NO
		# pending row — B's on_fill_event keys exit fills by ticker+side, not
		# coid; see ws_handlers._find_active_parent_for_exit). Constructed
		# directly (NOT execution.build_exit_order, which couples to ExecCfg +
		# strategy-populated target_price/exit_kind fields a bare TP/SL exit
		# Signal need not carry; the exit price here is the live book bid,
		# already the correct taker price). `config` is threaded for parity
		# with the entry path / future exit-policy use; the exit limit is the
		# bid (no slippage walk — selling into the resting bid is immediate).
		exit_req = OrderRequest(
			ticker=signal.ticker,
			series=signal.series,
			side=cast(Literal["yes", "no"], signal.side),
			size_contracts=exit_size,
			limit_price_cents=exit_price,
			strategy=signal.strategy,
			client_order_id=_make_client_order_id(
				signal.strategy, signal.ticker, now
			),
			action="sell",
		)
		# Place the exit UNCONDITIONALLY through the executor (the §1 seam).
		# PaperExecutor → synchronous deterministic ACK (not consumed here —
		# the paper close is store.exit_trade below, byte-exact). LiveExecutor
		# → real IOC sell; the live close is the sync store.exit_trade below
		# (on_fill_event is F-scope/unwired — see docstring). Hard-capped
		# exactly like the entry place() so a
		# pathological client retry-loop cannot wedge the WS message loop.
		try:
			exit_result = await asyncio.wait_for(
				executor.place(exit_req),
				timeout=_ENTRY_PLACEMENT_TIMEOUT_SECONDS,
			)
		except asyncio.TimeoutError:
			# The exit POST may still have reached Kalshi (live) — we don't
			# know, so we don't lie: leave exit_result None so the close below
			# is SKIPPED and the row stays OPEN. dispatch does NOT set the row
			# exit_pending here, so the reconciler (which scans pending/
			# exit_pending) does not own this — the settlement poller books the
			# true outcome on the open row (exit_reason='settlement') at expiry.
			# Paper's PaperExecutor.place cannot time out (pure CPU) so this is
			# a live-only safety net, not a paper-visible path (G-parity safe).
			exit_result = None
			log.warning(
				"exit executor.place exceeded %ds for %s %s (coid=%s) — close "
				"SKIPPED (no confirmed fill); row left open, settlement poller "
				"owns recovery",
				_ENTRY_PLACEMENT_TIMEOUT_SECONDS, signal.strategy,
				signal.ticker, exit_req.client_order_id,
			)

	# Book the close ONLY on a venue-confirmed FULL fill (the §1 seam: the
	# executor reports what actually traded). PaperExecutor returns a full
	# fill at the bid → byte-EXACT close (mandatory G-parity). LiveExecutor
	# returns the REAL outcome: a FULL fill (filled_size >= the position), a
	# PARTIAL fill, a 0-fill 'rejected', or an unknown 'pending' (the IOC sell
	# found no / not enough resting bid at the limit on a thin book — common).
	#
	# Only a FULL fill is booked here. The Protocol's exit_trade is
	# structurally a FULL close (no closed_size / kalshi_exit_order_id), so it
	# CANNOT express a partial — and booking a full close of a partially-sold
	# position fabricates a sale of the unsold remainder (and, once the row is
	# terminal, B's settlement CAS no-ops, so that remainder's true outcome is
	# lost). A PARTIAL also cannot be expressed by the Protocol exit_trade, so
	# dispatch STEPS ASIDE and leaves the row OPEN. TODAY the settlement poller
	# (the E3 backstop) settles the full remaining fill_size at the binary
	# outcome (exit_reason='settlement') — the M already-sold contracts are then
	# re-settled at binary, not their real IOC price (a P&L mislabel on the sold
	# sliver, NOT a dropped/fabricated position; strictly better than a phantom
	# full close). The PRECISE M/(N-M) split (record_partial_exit via B's
	# on_fill_event) needs the account-scope fill WS pump — sub-project F;
	# on_fill_event is UNWIRED in the live engine today (engine.py ~1566). A
	# non-fill / timeout is the same: row left OPEN → settlement poller. Booking
	# on a non-fill was the 2026-05-26 phantom-exit bug (live db -$8.53 of
	# phantom closes vs Kalshi settlements -$3.53; wins logged as stops).
	confirmed_full_fill = (
		exit_result is not None
		and exit_result.status == "filled"
		and exit_result.filled_size >= exit_size
	)
	if not confirmed_full_fill:
		if (
			exit_result is not None
			and exit_result.status == "filled"
			and 0 < exit_result.filled_size < exit_size
		):
			# PARTIAL fill — full close WITHHELD (Protocol exit_trade is
			# full-close-only). Row left OPEN → today the settlement poller
			# settles the full remainder at binary; precise split awaits F.
			log.info(
				"EXIT partial fill %d of %d for %s %s (trade_id=%s) — full close "
				"WITHHELD; row left open, settlement poller settles the remainder "
				"at binary (precise split awaits sub-project F)",
				exit_result.filled_size, exit_size, signal.strategy,
				signal.ticker, signal.trade_id,
			)
		else:
			log.info(
				"EXIT not booked for %s %s (trade_id=%s): executor reported no "
				"confirmed fill (status=%s) — row left open for settlement",
				signal.strategy, signal.ticker, signal.trade_id,
				exit_result.status if exit_result is not None else "timeout/none",
			)
		return False

	# Mode-agnostic FULL close (§1). PAPER: authoritative close (byte-EXACT —
	# paper TradeStore.exit_trade does won/lost/scratch + pnl + fee
	# synchronously, idempotent on WHERE status='open'). LIVE: C5's idempotent
	# record_close CAS backstop racing B's async path (whichever lands first
	# wins, the other no-ops, NEVER raises — the §4.2-sound benign-lost-CAS
	# property). dispatch does NOT branch on mode — the store/Protocol absorbs it.
	store.exit_trade(signal.trade_id, exit_price, now=now)

	# Read back PnL + fill fields from DB (includes fee deduction)
	exited = store.get_trade_by_id(signal.trade_id)
	if exited is None:
		log.warning("EXIT: trade id=%d not found post-exit_trade", signal.trade_id)
		return True  # store.exit_trade above already persisted the close; only readback failed
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
	# Reached only via the confirmed_full_fill path (store.exit_trade above
	# persisted the close). Signal a confirmed close so _handle_signal ratchets
	# the drawdown peak (spec §3.3b).
	return True


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


async def _handle_ticker_msg(
	msg: dict,
	config: dict,
	market_state: MarketState,
	store: TradeStoreProtocol,
	strategies: list[Strategy],
	strat_by_series: dict[str, list[Strategy]],
	pending_states: dict[str, dict],
	dirty: set[str],
	executor: Executor,
	*,
	now: datetime,
	risk: Gate | None = None,
	risk_ctx_provider: RiskContextProvider | None = None,
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
			await process_tick(
				ctx, [strat], store, config, executor,
				now=now, risk=risk, risk_ctx_provider=risk_ctx_provider,
			)
			dirty.add(strat.name)


async def _handle_trade_msg(
	msg: dict,
	config: dict,
	market_state: MarketState,
	store: TradeStoreProtocol,
	strategies: list[Strategy],
	strat_by_series: dict[str, list[Strategy]],
	pending_states: dict[str, dict],
	dirty: set[str],
	executor: Executor,
	*,
	now: datetime,
	risk: Gate | None = None,
	risk_ctx_provider: RiskContextProvider | None = None,
) -> None:
	"""Handle a trade WS message — routes to flow-sensitive strategies."""
	data = msg.get("msg", {})
	ticker = data.get("market_ticker", "")
	if not ticker:
		return

	# Skip if ticker not registered (trade can arrive before recovery seeds it)
	if market_state.get_price_history(ticker) is None:
		return

	# Kalshi WS sends the executed trade price as 'yes_price_dollars' (dollar
	# string, e.g. "0.7000") or 'yes_price' (legacy float). Mirror the v2/legacy
	# fallback in _handle_ticker_msg so current v2 trade frames aren't dropped.
	yes_price_raw = data.get("yes_price_dollars") or data.get("yes_price")
	if yes_price_raw is None:
		return

	try:
		# OverflowError guards a crafted/garbage "1e999"-style value: float() yields
		# inf and int(round(inf)) raises OverflowError, which is NOT a subclass of
		# (TypeError, ValueError). WS fields are externally controlled; catching here
		# skips the bad frame cleanly instead of letting it fall to dispatch's broad
		# handler (per-frame log.exception noise + dropping a recoverable trade).
		trade_price_cents = int(round(float(yes_price_raw) * 100))
	except (TypeError, ValueError, OverflowError):
		return
	if not (1 <= trade_price_cents <= 99):
		return

	taker_side = data.get("taker_side")
	# 'count_fp' is the v2 fixed-point count string (e.g. "1.39"); 'count' is the
	# legacy int. The int(float(...)) parse below handles both shapes.
	count_raw = data.get("count_fp") or data.get("count")
	try:
		trade_count = int(float(count_raw)) if count_raw is not None else None
	except (TypeError, ValueError, OverflowError):  # inf from "1e999"-style count
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
			await process_tick(
				ctx, [strat], store, config, executor,
				now=now, risk=risk, risk_ctx_provider=risk_ctx_provider,
			)
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

async def dispatch_message(
	event: dict,
	config: dict,
	market_state: MarketState,
	store: TradeStoreProtocol,
	strategies: list[Strategy],
	strat_by_series: dict[str, list[Strategy]],
	pending_states: dict[str, dict],
	dirty: set[str],
	executor: Executor,
	*,
	now: datetime,
	risk: Gate | None = None,
	risk_ctx_provider: RiskContextProvider | None = None,
) -> None:
	"""Route one parsed event to its handler.

	`event` is either the on-disk JSONL shape (``{recv_seq, recv_ts, source, payload}``)
	or the legacy WS-only shape (just the parsed WS message dict). The router
	accepts both so the live engine can construct events from raw WS messages
	without going through a capture writer first, and the replay backtester
	can feed the on-disk shape directly.

	Async because ticker/trade handlers fan out to `process_tick` →
	`_handle_enter` → `await executor.place(...)`. Synthetic and orderbook
	handlers stay sync (no I/O) — calling them from this async router is fine.

	`risk` is the Gate instance (Sub-project C) for the live path, or None for
	paper-trader and replay.  When None, all signals proceed to executor.place
	without any gate check — replay of a historical bundle produces zero gate
	calls.  Callers construct Gate only when `executor_kind == "live"`.
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
			await _handle_ticker_msg(
				msg, config, market_state, store, strategies,
				strat_by_series, pending_states, dirty, executor,
				now=now, risk=risk, risk_ctx_provider=risk_ctx_provider,
			)
		elif msg_type == "trade":
			await _handle_trade_msg(
				msg, config, market_state, store, strategies,
				strat_by_series, pending_states, dirty, executor,
				now=now, risk=risk, risk_ctx_provider=risk_ctx_provider,
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
