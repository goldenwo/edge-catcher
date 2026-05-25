"""Paper trading engine — WS loop, strategy router, signal processing pipeline."""

from __future__ import annotations

import asyncio
import copy
import functools
import json
import logging
import threading
from datetime import date, datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Optional, cast

if TYPE_CHECKING:
	from edge_catcher.engine.risk import BankrollCache, Gate
	from edge_catcher.engine.risk_context_provider import RiskContextProvider
	from edge_catcher.live.reconciliation import StartupReconcileReport
	from edge_catcher.notifications import Notification

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
from edge_catcher.engine.trade_store import TradeStore, TradeStoreProtocol

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
# §6 boot step 3 — live risk-event routing to a DEDICATED risk channel (G3).
#
# spec §6 (NORMATIVE boot order): step (3) constructs the risk module AND
# registers `_handle_risk_event` into the Gate's callback list + binds the
# dedicated live risk channel BEFORE any gate evaluation (reconcile in step 4,
# trading in step 5), so a boot-time trip (e.g. the startup balance read
# already ≤ absolute_panic_floor, tripping KILL_AUTO_PANIC on the first
# gate_entry inside startup_reconcile) still reaches the operator's dedicated
# risk channel.
#
# A `RiskEvent` is a kill-switch / auto-panic trip — the ONLY signal a human
# operator gets that real-money trading has HALTED. It MUST reach the
# operator; a risk alert that silently "goes nowhere" is a serious
# operator-safety defect. The channel's resolvability is therefore ALSO a
# fail-closed §2.4 coherence check (`_assert_mode_coherence` Check-4,
# additive to the general `live_channel` branch — see there).
#
# G1 (`engine/notifications.py`) converged the GENERAL notify onto the
# unified layer with a boot-resolved module binding + `configure_notify`.
# G3 mirrors that shape for the DEDICATED risk channel, with one crucial
# difference: `_handle_risk_event` is SYNC (the Gate-callback contract —
# risk.py ~:944 calls `cb(event)` directly; spec §6: "`Gate.trip()` and
# `send()` are both sync"), so it calls the unified `send()` DIRECTLY /
# synchronously — it does NOT schedule an async task the way G1's `notify`
# does (a kill-switch alert must not depend on a running event loop and
# must not be droppable by a bounded queue). Signature matches
# Gate._event_callbacks' contract (risk.py ~:944-948): called with a single
# RiskEvent, sync, exceptions swallowed by the Gate (so this never raises).
#
# §9 G-parity: paper has NO risk-event surface — this whole slot is
# live-only. `_handle_risk_event` is appended to the Gate ONLY inside
# `_compose_live` (step-3); paper composition never registers it and never
# resolves a risk channel, so the paper trade-row path is byte-unchanged.
# ---------------------------------------------------------------------------

# Boot-resolved DEDICATED risk-channel binding (§6 step-3). Empty until
# `_configure_risk_channel` is called once inside the live `_compose_live`
# (the paper analog is "never bound" — paper has no risk surface). NEVER
# re-resolved per trip (mirrors the §1 keystone: wired at boot, not per-call).
_risk_channels: list = []

# F1 (§5.1 drain-then-crash supervisor) — holds the BaseException a bankroll
# refresh raised (a KillSwitchTripFailed: the auto-panic trip's kill-WRITE
# itself failed). asyncio ISOLATES a task's exception, so without this the
# refresh task would die silently and the engine would keep trading ungated
# against an untrusted balance (the C-spec L214 ghost-reject hazard). The
# done-callback stashes it here and cancels the root task; run_engine's finally
# turns that cancel into a fail-loud crash AFTER the money-safe drain ran.
# module-scoped (one engine per process — reset in test fixtures).
_REFRESH_FATAL: BaseException | None = None


def _refresh_done_cb(
	task: "asyncio.Task[Any]", *, root_task: "asyncio.Task[Any]"
) -> None:
	"""Done-callback on the §5.1 bankroll-refresh task (F1).

	asyncio swallows a task's exception unless something retrieves it. This
	callback retrieves it: a CLEAN drain cancel (``task.cancelled()``) is NOT
	fatal and is ignored; ANY other exception (the loop's
	``KillSwitchTripFailed`` — kill-WRITE failed) is stashed in the module
	holder and the engine root task is cancelled. The cancel propagates the
	usual CancelledError into ``_ws_loop`` so ``run_engine``'s finally drains
	the in-flight place→persist sections (money-safe) and THEN re-raises the
	stashed exception fail-loud."""
	global _REFRESH_FATAL
	if task.cancelled():
		return  # clean drain cancel — not fatal
	exc = task.exception()
	if exc is not None:
		_REFRESH_FATAL = exc
		root_task.cancel()  # interrupt the engine; the finally turns this into a fail-loud crash


def _configure_risk_channel(channels: list) -> None:
	"""Install the boot-resolved DEDICATED risk channel(s) (§6 step-3).

	Called ONCE from the live ``_compose_live`` (boot step-3) BEFORE the
	``_handle_risk_event`` callback is registered into the Gate and BEFORE
	any gate evaluation (reconcile in step-4, trading in step-5), so a
	boot-time trip still reaches the operator. Subsequent ``_handle_risk_
	event`` calls deliver to these channels via the unified ``send()`` —
	there is no per-trip re-resolution.

	An empty list means "no dedicated risk channel bound" — ``_handle_risk_
	event`` then logs at ERROR (the trip is still recorded in the journal /
	the kill row; this binding is the operator ALERT, not the trip record).
	In live mode the §2.4 coherence gate guarantees a resolvable risk
	channel, so an empty binding in live is unreachable defence; paper never
	binds it (live-only surface — §9 G-parity)."""
	global _risk_channels
	_risk_channels = list(channels)


def _resolve_risk_channel(config: dict) -> list:
	"""§6 step-3 — resolve the DEDICATED live risk channel for the boot bind.

	Reuses the SAME unified ``notifications.yaml`` / ``load_channels``
	mechanism the §2.4 coherence gate parses (spec §6: "channels resolved
	per §2.4's invariant clause") — there is no second bespoke channel
	loader. Reads ``notifications.config_path`` (default
	``_DEFAULT_NOTIFY_CONFIG``) + ``notifications.live_risk_channel``.

	Returns the resolved ``[Channel]`` (a one-element list). In live mode
	the §2.4 coherence gate has ALREADY hard-verified this channel is
	configured AND resolvable BEFORE this runs (boot step-2, before
	step-3), so reaching here means it resolves; a defensive empty-list
	fallback on the (coherence-unreachable) missing/unresolvable case keeps
	this delivery-only resolution from ever aborting boot itself."""
	notif_cfg = config.get("notifications", {}) or {}
	channel_name = notif_cfg.get("live_risk_channel")
	if not channel_name:
		# Unreachable in live (the §2.4 gate aborts boot first); defensive.
		log.error(
			"risk-channel resolution: no `notifications.live_risk_channel` "
			"configured — kill-switch alerts have NO dedicated channel this "
			"run (the §2.4 coherence gate should have aborted boot earlier)"
		)
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
		# Unreachable in live (the §2.4 gate parses the SAME file first).
		log.error(
			"risk-channel resolution: unified config %r unresolvable (%s) — "
			"kill-switch alerts have NO dedicated channel this run",
			str(notify_path), exc,
		)
		return []
	channel = channels.get(channel_name)
	if channel is None:
		log.error(
			"risk-channel resolution: configured risk channel %r not defined "
			"in %r (available: %s) — kill-switch alerts have NO dedicated "
			"channel this run",
			channel_name, str(notify_path), sorted(channels),
		)
		return []
	return [channel]


def _handle_risk_event(event: Any) -> None:
	"""§6-step-3 risk-event callback — route a kill trip to the operator.

	Registered into the live ``Gate``'s callback list at boot step-3 (inside
	``_compose_live``, BEFORE any gate evaluation) so a boot-time trip still
	reaches the operator. A ``RiskEvent`` is a kill-switch / auto-panic trip
	— it MUST reach the operator.

	SYNC by contract: the Gate invokes ``cb(event)`` directly in ``trip()``
	(risk.py ~:944; spec §6: "``Gate.trip()`` and ``send()`` are both
	sync"), so this builds a :class:`Notification` from the event and hands
	it to the unified ``send()`` SYNCHRONOUSLY — it does NOT schedule an
	async task (a kill-switch alert must not depend on a running event loop
	nor be droppable by a bounded queue, unlike G1's general ``notify``).

	``send()`` is sync and never raises (per-channel ``DeliveryResult``);
	the Gate additionally swallows callback exceptions — so this can never
	perturb the trip path. With no dedicated channel bound it logs at ERROR
	(unreachable in live: the §2.4 coherence gate guarantees a resolvable
	risk channel; this is the paper/unconfigured defensive analog)."""
	kind = getattr(event, "kind", "?")
	reason = getattr(event, "reason", "?")
	detail = getattr(event, "detail", "")
	severity = getattr(event, "severity", "error")
	occurred_at = getattr(event, "occurred_at", "?")

	if not _risk_channels:
		# No dedicated channel bound. Unreachable in live (§2.4 gate aborts
		# boot first); still log LOUD so the trip is never silently lost.
		log.error(
			"RISK EVENT (kind=%s reason=%s severity=%s detail=%s) but NO "
			"dedicated risk channel is bound — alert NOT delivered (the §2.4 "
			"coherence gate should have made this unreachable in live)",
			kind, reason, severity, detail,
		)
		return

	# RiskEvent.severity ∈ {"info","warn","error"} maps 1:1 onto
	# Notification.severity (same Literal); default to "error" defensively
	# (a kill trip is operationally an error-class event).
	# `severity` is Any (getattr on a duck-typed event) and `x in (...)` does
	# not narrow Any to the target Literal for mypy — cast after the runtime
	# membership guard (which guarantees one of the three, else "error").
	note_severity = cast(
		Literal["info", "warn", "error"],
		severity if severity in ("info", "warn", "error") else "error",
	)
	title = f"edge-catcher RISK: {reason} ({kind})"
	body = (
		f"Live trading risk event — {kind}.\n"
		f"reason={reason}\n"
		f"severity={severity}\n"
		f"detail={detail}\n"
		f"occurred_at={occurred_at}"
	)
	# Local import: keep engine.py importable on paper-only deployments
	# (same runtime-import convention as the §2.4 gate / _resolve_risk_channel).
	from edge_catcher.notifications import (  # noqa: PLC0415
		Notification,
		send,
	)
	# SYNC send — never raises (per-channel DeliveryResult). The Gate also
	# swallows callback exceptions, so this cannot perturb the trip path.
	send(
		Notification(title=title, body=body, severity=note_severity),
		_risk_channels,
	)
	log.warning(
		"RISK EVENT routed to dedicated channel (kind=%s reason=%s "
		"severity=%s)", kind, reason, severity,
	)


def _reconcile_alert_notification(
	report: StartupReconcileReport,
) -> Notification | None:
	"""Build an operator notification from a ``startup_reconcile`` report, or
	``None`` when the pass found nothing worth surfacing.

	Fires on operator-attention alerts (orphan recoveries + lost_truth) OR
	settled-recovered rows (real money that settled while the daemon was down,
	now handed to the settlement poller). A fully clean reconcile returns
	``None`` — no Discord noise on every boot. Severity is the WORST outcome:

	* ``lost_truth > 0``                  → ``"error"`` (we believe we hold a
	  position Kalshi has no record of — manual investigation, real money).
	* else ``orphan_positions_recovered`` → ``"warn"`` (Kalshi held a position
	  we had no row for; auto-recovered, operator confirms which strategy).
	* else (settled-recovered only)       → ``"info"`` (benign hand-off).
	"""
	has_alerts = report.alerts > 0
	if not has_alerts and report.settled_recovered <= 0:
		return None
	severity: Literal["info", "warn", "error"]
	if report.lost_truth > 0:
		severity = "error"
	elif report.orphan_positions_recovered > 0:
		severity = "warn"
	else:
		severity = "info"
	# Local import keeps engine.py importable on paper-only deployments that
	# lack the notifications extra (same convention as _handle_risk_event).
	from edge_catcher.notifications import Notification  # noqa: PLC0415

	suffix = " — manual investigation" if has_alerts else " (settled-recovered)"
	title = f"edge-catcher reconcile: {report.alerts} alert(s){suffix}"
	body = (
		"Startup reconcile completed with operator-relevant outcomes.\n"
		f"orphan_positions_recovered={report.orphan_positions_recovered}\n"
		f"lost_truth={report.lost_truth}\n"
		f"settled_recovered={report.settled_recovered}\n"
		f"pending_resolved={report.pending_resolved}\n"
		f"pending_post_hoc_rejected={report.pending_post_hoc_rejected}\n"
		f"alerts={report.alerts}"
	)
	return Notification(title=title, body=body, severity=severity)


def _emit_reconcile_report(
	report: StartupReconcileReport,
	channels: list,
) -> None:
	"""Fan a ``startup_reconcile`` report out to the operator's risk channel(s).

	Glue around :func:`_reconcile_alert_notification`: build the notification
	(``None`` → nothing to say, no send) and deliver it via the unified
	``send`` (sync, matching ``_handle_risk_event``; safe at boot — the WS is
	not subscribed yet, so a brief sync HTTP post blocks nothing). A delivery
	failure is logged and SWALLOWED: the reconcile already succeeded, so a
	notification error must never crash a live engine (``send`` is documented
	never-raises; the guard is defence in depth)."""
	note = _reconcile_alert_notification(report)
	if note is None:
		return
	from edge_catcher.notifications import send  # noqa: PLC0415

	try:
		send(note, channels)
	except Exception:
		log.exception(
			"reconcile-report notification send failed (non-fatal — reconcile "
			"already succeeded; not crashing the live engine)"
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

	# --- Check 4b: the DEDICATED risk channel resolvable (§2.4 / §6 G3,
	# live only). ADDITIVE to the general `live_channel` check above (which
	# is untouched). spec §6/§2.4 require "the live channels resolvable"
	# (plural): a RiskEvent is a kill-switch / auto-panic trip — the ONLY
	# signal the operator gets that real-money trading HALTED — so it routes
	# to a DEDICATED channel (`_handle_risk_event` → §6 step-3). If that
	# channel is missing/unresolvable a tripped kill-switch alert would
	# silently go nowhere, defeating G3's entire purpose — that is exactly
	# the "would go nowhere" funds-safety failure this gate exists to make
	# impossible, so it is fail-closed (boot abort). Reuses the SAME
	# already-loaded `channels` (no second load_channels — single source). ---
	live_risk_channel = notif_cfg.get("live_risk_channel")
	if not live_risk_channel:
		raise _coherence_fail(
			"risk_channel",
			"executor=live but no `notifications.live_risk_channel` is "
			"configured — a kill-switch / auto-panic trip (the operator's "
			"ONLY signal that real-money trading HALTED) would go nowhere "
			"(spec §2.4/§6 G3: the dedicated risk channel is mandatory)",
		)
	if live_risk_channel not in channels:
		raise _coherence_fail(
			"risk_channel",
			f"executor=live but the configured risk channel "
			f"{live_risk_channel!r} is not defined in {str(notify_path)!r} "
			f"(available: {sorted(channels)}) — a kill-switch trip alert "
			f"would go nowhere (spec §2.4/§6 G3)",
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


async def bankroll_refresh_loop(
	bankroll: BankrollCache,
	*,
	interval: float,
	warn_after: int,
) -> None:
	"""Periodic bankroll refresh (spec §5.1).

	Awaits ``bankroll.refresh()`` every ``interval`` seconds (caller passes
	``bankroll_ttl_seconds / 2``).  When ``bankroll._consecutive_failures``
	reaches ``warn_after`` (= ``bankroll_failures_until_kill - 1``, the refresh
	BEFORE the kill) a ONE-TIME WARNING is sent to the dedicated risk channel;
	the latch resets on the next successful refresh so a fresh failure streak
	would warn again.  ``warn_after < 1`` disables the pre-kill warning — at the
	``bankroll_failures_until_kill == 1`` floor the kill trips on the first
	failure, so there is no earlier cycle to warn on (a coincident warning would
	misdescribe the manual-clear-only KILL_AUTO_PANIC as a transient gate).

	**LIVE-ONLY** — started only inside the live task block (Task G1 wires the
	``create_task`` call).  Paper / replay paths never call this function;
	G-parity is unaffected.

	Propagation contract:
	  - ``CancelledError`` propagates — clean drain on engine shutdown.
	  - ``KillSwitchTripFailed`` propagates — F1's done-callback surfaces it
	    as a fail-loud crash.  NOT caught here.
	"""
	warned = False
	while True:
		await asyncio.sleep(interval)
		await bankroll.refresh()  # KillSwitchTripFailed propagates — F1 surfaces it
		failures = bankroll._consecutive_failures
		if failures == 0:
			warned = False
		elif warn_after >= 1 and failures >= warn_after and not warned:
			warned = True
			from edge_catcher.notifications import Notification, send  # noqa: PLC0415
			send(
				Notification(
					title="edge-catcher RISK: bankroll refresh failing",
					body=(
						f"Bankroll refresh failing ({failures} consecutive) — "
						f"entries gated STALE_BANKROLL until it recovers."
					),
					severity="warn",
				),
				_risk_channels,
			)
			log.warning(
				"Bankroll refresh sustained failure: %d consecutive — "
				"WARNING sent to risk channel (warn_after=%d)",
				failures, warn_after,
			)


async def _settlement_poller(
	store: TradeStoreProtocol,
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
	store: TradeStoreProtocol,
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
	store: TradeStoreProtocol,
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
) -> tuple[TradeStoreProtocol, Executor, _LiveRuntime]:
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
	(§2.5/§6) for its fail-fast side-effect — a malformed ``execution:`` block
	aborts at boot, not at first exit — AND its typed ``ExecCfg`` result is
	stashed in ``config["_exec_cfg"]`` (LIVE-ONLY, since this is the live
	composition branch) so dispatch's live entry path (``_handle_enter`` with
	``allowed_size is not None``) can build the sized ``OrderRequest`` via
	``build_entry_order``. Paper never reaches here, so paper's config carries
	no ``_exec_cfg`` and ``_handle_enter`` stays on the byte-exact
	``allowed_size=None`` path (§9 G-parity).

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
	# not at first exit. The typed result is stashed in config["_exec_cfg"]
	# (LIVE-ONLY — this is the live composition branch) so dispatch's live
	# entry path (_handle_enter, allowed_size is not None) can build the sized
	# OrderRequest via build_entry_order without re-parsing the YAML or
	# growing a new per-handler parameter. Paper never reaches here, so its
	# config carries NO _exec_cfg ⇒ _handle_enter stays on the byte-exact
	# allowed_size=None paper path (§9 G-parity). _exec_cfg is consumed ONLY
	# by _handle_enter's live sizing branch; the exit path builds its
	# OrderRequest directly WITHOUT _exec_cfg (see dispatch._handle_exit).
	exec_cfg = validate_exec_cfg(config.get("execution", {}))
	config["_exec_cfg"] = exec_cfg

	# §6 step 3 — wire the risk module (pre-refreshes the bankroll cache so
	# the first gate_entry sees real cash; a Kalshi-unreachable boot leaves
	# the cache at 0 ⇒ KILL_AUTO_PANIC on first signal, the correct
	# fail-closed behaviour). build_risk_module reads config["risk"].
	gate = await build_risk_module(config, db_conn, kalshi_client)
	# G3 (§6 step-3): bind the DEDICATED risk channel BEFORE the callback is
	# registered, so the instant `_handle_risk_event` could fire it already
	# has a channel. `build_risk_module` does NOT evaluate/trip during
	# construction — it pre-refreshes the bankroll cache while the Gate's
	# `_emit_trip_fn` is still None (risk.py docstring), so the FIRST trip
	# can only occur in step-4 reconcile / step-5 trading, strictly AFTER
	# this. The §2.4 coherence gate (boot step-2, already passed) hard-
	# verified this channel is configured + resolvable, so this resolves.
	_configure_risk_channel(_resolve_risk_channel(config))
	# Register the §6-step-3 risk-event slot BEFORE any gate evaluation
	# (reconcile/trading in steps 4/5) so a boot-time trip reaches it AND
	# (G3) routes to the now-bound dedicated risk channel via send().
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
	# Reset the fatal holder — robust to in-process engine reuse; production
	# is one-engine-per-process so this is a no-op there, but keeps in-process
	# reuse (e.g. integration tests) safe without relying on the test fixture.
	global _REFRESH_FATAL
	_REFRESH_FATAL = None

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

	# Risk gate (Sub-project C/E) — Gate / BankrollCache / KillSwitch / etc.
	# all live in engine/risk.py.  For live mode (executor_kind == "live"),
	# the full risk stack is composed here at engine startup: a
	# KalshiBalanceSource is constructed, BankrollCache.refresh() is awaited
	# at T0, a periodic-refresh background task is started, and both the
	# Gate instance and a RiskContextProvider are threaded through to
	# dispatch_message (via _ws_loop) so every live signal is gated before
	# an order is placed.  Paper/replay paths receive risk=None and are
	# byte-identical to pre-gate behaviour.

	# §6 Path B — install the boot-resolved notify channel(s) ONCE here
	# (after the §2 coherence gate + the mode-composition branch; the live
	# channel's resolvability was already hard-gated by §2.4). `notify` then
	# delegates to the unified `send()` with these channels and is never
	# re-resolved per call. Delivery-only: an absent/unresolvable channel
	# yields a silent-no-op notify and never aborts boot (§6/§9 — notify is
	# a side-effect, not trade state; the paper trade-row path is unchanged).
	configure_notify(_resolve_notify_channels(config))

	# Cutover-verification beacon. Pi cutover step 5 greps journalctl for this
	# line to prove the engine/ package is loaded (NOT monitors/). The executor
	# word is mode-driven (`mode` is gate-validated to "live"/"paper") so a
	# real-money boot never mislabels itself "paper"; the stable
	# `package=edge_catcher.engine` token is the package discriminator.
	# Generic "Engine starting" is shared with the OLD engine; this line is unique.
	log.info("engine[G]: %s executor wired, package=edge_catcher.engine", mode)

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
				reconcile_report = await startup_reconcile(
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
			# Reconcile succeeded — fan its operator-attention outcomes (orphan
			# recoveries / lost_truth, plus settled-recovered as benign context)
			# out to the DEDICATED risk channel via the same unified `send` the
			# kill-trip path uses. The lost_truth/orphan WARNINGs also hit the
			# logs via 4.A; this surfaces them to Discord so the operator sees a
			# reconcile anomaly during the run without tailing the journal.
			# Post-reconcile and NEVER fatal: nothing-to-say → no send, and a
			# delivery failure is swallowed (the engine has already reconciled).
			_emit_reconcile_report(reconcile_report, _risk_channels)

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
		# G1: the risk gate + its per-signal context provider threaded into the
		# WS loop (LIVE only). Paper/replay leave both None ⇒ the dispatch gate
		# short-circuits and the WS path stays byte-identical (§9 G-parity).
		risk: "Gate | None" = None
		risk_ctx_provider: "RiskContextProvider | None" = None
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

			# G1 — close the no-op-gate gap. The gate (build_risk_module) + a
			# RiskContextProvider over the SAME live db_conn B's writers use and
			# the engine's single MarketState (mutated in place by the WS loop)
			# are threaded into `_ws_loop` → `dispatch_message` → `process_tick`
			# → `_handle_signal` so EVERY live entry is gated on the real WS
			# feed. Without this the live engine sized every entry 0 and
			# LiveExecutor rejected it (a silent no-op — zero real orders).
			from edge_catcher.engine.risk_context_provider import (  # noqa: PLC0415
				RiskContextProvider,
			)
			risk = live_runtime.gate
			risk_ctx_provider = RiskContextProvider(
				conn=live_runtime.db_conn,
				operator_kill=_OPERATOR_KILL,
				market_state=market_state,
			)

			# §5.1 bankroll refresh (LIVE only). Keeps the gate's bankroll cache
			# fresh on a period of bankroll_ttl_seconds/2 so STALE_BANKROLL never
			# trips in steady state. The fatal supervisor (done-callback +
			# drain-then-crash on a refresh KillSwitchTripFailed) is wired in F1;
			# here we only START the task so it runs.
			risk_cfg = config.get("risk", {})
			ttl = float(risk_cfg.get("bankroll_ttl_seconds", 300))
			failures_until_kill = int(
				risk_cfg.get("bankroll_failures_until_kill", 2)
			)
			# One-time WARNING fires the refresh BEFORE the kill. At the
			# failures_until_kill == 1 floor this is 0 — the loop's
			# `warn_after >= 1` guard then disables the (impossible) pre-kill
			# warning, since the kill trips on the very first failure.
			warn_after = failures_until_kill - 1
			refresh_task = asyncio.create_task(
				bankroll_refresh_loop(
					live_runtime.gate._bankroll,
					interval=ttl / 2,
					warn_after=warn_after,
				),
				name="bankroll_refresh",
			)
			tasks.append(refresh_task)
			# F1 — drain-then-crash supervisor. asyncio isolates a task's
			# exception; without this done-callback a refresh KillSwitchTripFailed
			# (the auto-panic trip's kill-WRITE failed) would be silently lost and
			# the engine would keep trading ungated against an untrusted balance.
			# The callback stashes the exception + cancels THIS run_engine root
			# task; the finally's fatal guard re-raises it fail-loud after the
			# money-safe drain. `asyncio.current_task()` here = the run_engine root
			# task the cli `await`s — cancelling it triggers run_engine's finally.
			# It is never None inside a running coroutine (defensive narrow for
			# mypy + a fail-loud guard if that invariant were ever broken).
			_root_task = asyncio.current_task()
			if _root_task is None:  # pragma: no cover - unreachable inside run_engine
				raise RuntimeError(
					"F1: asyncio.current_task() is None inside run_engine — cannot "
					"wire the bankroll-refresh fatal supervisor"
				)
			refresh_task.add_done_callback(
				functools.partial(_refresh_done_cb, root_task=_root_task)
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
						risk=risk, risk_ctx_provider=risk_ctx_provider,
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

			# (2 cont.) flush per-strategy state — part of "stop intake": no new
			# tick will mutate it; persist BEFORE the in-flight drain (do NOT move
			# past (3) drain_inflight_sections / (6) store.close()).
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

			# F1 fatal guard — AFTER the money-safe drain (steps 1-6 ran), BEFORE
			# the clean SIGTERM alert. A bankroll-refresh KillSwitchTripFailed
			# means the auto-panic trip's kill-WRITE itself failed, so NO RiskEvent
			# fired (the §6 risk-event slot is reached only on a SUCCESSFUL trip) —
			# this is the operator's ONLY signal. Emit it best-effort, then re-raise
			# the stashed exception so the process STOPS fail-loud (NOT a clean exit
			# the cli swallows as 0) and the clean "drain complete" alert is SKIPPED.
			if _REFRESH_FATAL is not None:
				try:
					from edge_catcher.notifications import (  # noqa: PLC0415
						Notification,
						send,
					)
					send(
						Notification(
							title="edge-catcher RISK: FATAL — bankroll refresh kill-write failed",
							body=(
								f"Engine STOPPING fail-loud: bankroll-refresh trip's "
								f"kill-write failed. "
								f"{type(_REFRESH_FATAL).__name__}: {_REFRESH_FATAL}"
							),
							severity="error",
						),
						_risk_channels,
					)
				except Exception:
					log.exception("FATAL alert send failed (proceeding to raise)")
				raise _REFRESH_FATAL  # money-safe fail-loud — skips the clean alert below

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
	store: TradeStoreProtocol,
	strategies: list[Strategy],
	strat_by_series: dict[str, list[Strategy]],
	pending_states: dict[str, dict],
	active_series: list[str],
	client: httpx.AsyncClient,
	ws_ref: list,
	dirty: set[str],
	executor: Executor,
	capture_writer: RawFrameWriter | None = None,
	*,
	risk: "Gate | None" = None,
	risk_ctx_provider: "RiskContextProvider | None" = None,
) -> None:
	"""Single WS connection lifecycle — connect, subscribe, process messages.

	``risk`` + ``risk_ctx_provider`` are the live risk gate + its per-signal
	context provider, threaded down to ``dispatch_message`` so the gate is
	consulted on the REAL WS feed (G1 — closes the no-op-gate gap where the
	live WS path bypassed the gate entirely). Both default ``None`` and are
	passed ``None`` by paper/replay ⇒ the dispatch gate short-circuits and the
	paper path stays byte-identical (§9 G-parity).
	"""
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
					now=now, risk=risk, risk_ctx_provider=risk_ctx_provider,
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


