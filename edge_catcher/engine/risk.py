"""Risk gates + bankroll cache + kill switch + peak tracker.

Single pure-logic module that gates every live entry/exit signal before it
reaches the executor.  Paper-trader and replay paths do NOT instantiate this
module (guard: ``executor_kind == "live"`` in E's dispatch wiring).

Public surface
--------------
- ``Gate`` — the gate itself: ``gate_entry(sig, ctx) -> GateDecision``,
  ``gate_exit(sig, ctx) -> GateDecision``
- ``BankrollCache`` — async-refreshed cash cache; sync ``cash_cents()`` read
- ``KillSwitch`` — DB-persisted auto-kill log with auto-clear at midnight
- ``PeakTracker`` — closed-equity peak, persisted in ``risk_state`` table
- ``BalanceSource`` — Protocol for venue-agnostic cash queries
- ``KalshiBalanceSource`` — Phase 1 impl wrapping ``KalshiOrderClient``
- ``RiskContext`` — frozen value object built per-gate-call by E's dispatch
- ``RiskEvent`` — structured event emitted on every kill trip for E to route
- ``build_risk_module`` — factory: wires all components from config + deps

Design invariants
-----------------
1. ``Gate.gate_entry`` / ``gate_exit`` are sync.  Network I/O lives in
   ``BankrollCache.refresh()`` (async) — E schedules it; the gate reads the
   cached value.
2. ``_emit_trip`` raises on DB INSERT failure (C-spec L214).  A silent failure
   would mean a ghost reject without DB persistence; on the next tick the gate
   re-evaluates against unchanged DB state and double-trips.  Raising is
   correct; E treats it as a fatal error and stops the engine.
3. ``engine/risk.py`` MUST NOT import from any venue-specific adapter module
   (CR-6).  ``KalshiBalanceSource`` is the ONLY Kalshi-aware class and it is
   hidden behind the ``BalanceSource`` Protocol.
4. Frozen dataclasses + slots for all value objects — deterministic equality,
   no accidental mutation.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal, Protocol

from edge_catcher.engine.executor import OpenPosition
from edge_catcher.engine.market_state import MarketState
from edge_catcher.engine.strategy_base import Signal
from edge_catcher.live.errors import KalshiAPIError, NetworkError

# KalshiOrderClient imported ONLY inside KalshiBalanceSource — the one place
# this module is permitted to reference a venue-specific type (CR-6).
from edge_catcher.live.client import KalshiOrderClient

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Gate decision types
# ---------------------------------------------------------------------------

GateRejectReason = Literal[
	"KILL_OPERATOR",        # KILL_SWITCH=1 env var or SIGTERM in flight
	"KILL_AUTO_PANIC",      # equity ≤ absolute_panic_floor_cents
	"KILL_AUTO_DRAWDOWN",   # equity ≤ peak * (1 - drawdown_pct)
	"KILL_AUTO_DAILY",      # daily_pnl ≤ -equity * daily_loss_pct (today UTC)
	"INVALID_SIGNAL",       # signal has unusable entry_price or stop_loss
	"MAX_OPEN",             # open_count ≥ max_open
	"BELOW_MIN_FILL",       # size < min_fill_contracts
	"STALE_BANKROLL",       # bankroll cache too stale to trust for sizing
]


class KillSwitchTripFailed(Exception):
	"""Raised when KillSwitch.trip's INSERT fails — engine MUST stop.

	Per C-spec L214: silent INSERT failure = ghost reject = funds-at-risk.
	dispatch.process_tick catches Exception broadly but MUST re-raise this
	class so the engine loop terminates rather than silently swallowing a
	failed kill-switch persistence.
	"""


class KillSwitchClearError(Exception):
	"""Raised when KillSwitch.clear targets a non-existent kill_id.

	The CLI's kill-clear command surfaces this as a clear operator error
	rather than a silent no-op (which would mislead the operator into
	thinking the gate was cleared when it wasn't).
	"""


@dataclass(frozen=True, slots=True)
class SizingBreakdown:
	"""Which arm of the min() bounded the final size."""
	fixed_fraction_contracts: int
	quarter_kelly_contracts: int   # 2**31 sentinel when strategy has no edge config
	absolute_max_contracts: int
	bound_by: Literal["fixed_fraction", "quarter_kelly", "absolute_max"]


@dataclass(frozen=True, slots=True)
class Allow:
	"""Gate allows the trade — proceed to build + place."""
	size_contracts: int
	sizing_breakdown: SizingBreakdown


@dataclass(frozen=True, slots=True)
class Reject:
	"""Gate rejects the trade — audit + notify, do NOT place."""
	reason: GateRejectReason
	detail: str   # human-readable, for audit + Discord notify


GateDecision = Allow | Reject


# ---------------------------------------------------------------------------
# RiskEvent — emitted by Gate on kill trips; E routes to Discord + audit
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class RiskEvent:
	"""Structured event emitted on every kill trip or kill-clear.

	E's ``_handle_risk_event`` reads this and dispatches to the right Discord
	channel + writes the audit log.  C never calls notify() directly.
	"""
	kind: Literal["trip", "auto_clear", "manual_clear"]
	reason: GateRejectReason
	detail: str
	severity: Literal["info", "warn", "error"]
	occurred_at: datetime


# ---------------------------------------------------------------------------
# Kill-switch row value object
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class KillRow:
	"""A single row from the kill_switch table."""
	id: int
	reason: GateRejectReason
	detail: str
	tripped_at: str   # ISO-8601 UTC, microsecond resolution
	cleared_at: str | None
	cleared_by: str | None


# ---------------------------------------------------------------------------
# RiskConfig — parsed from live-trader.yaml's ``risk:`` block
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class RiskConfig:
	"""Phase config bundle.  All six scalars define the phase; promotion is
	YAML-only (no code change between Phase 1 and Phase 2).

	C-spec L527-L548 defines the canonical set of keys; do not add keys here
	without updating the spec.
	"""
	sizing_pct: float               # fixed-fraction arm (0.005 = 0.5% of equity)
	daily_loss_pct: float           # 0.02 = 2% of equity
	drawdown_pct: float             # 0.05 = 5% from closed-equity peak
	max_open: int                   # max concurrent open positions
	min_fill_contracts: int         # below this → BELOW_MIN_FILL
	absolute_panic_floor_cents: int # $30 = 3000 — equity floor (static)
	absolute_max_cents: int         # $50 = 5000 — per-order dollar cap (static)
	kelly_shrinkage: float          # 0.5 = multiplicative on win_prob (inert Phase 1)
	bankroll_ttl_seconds: float     # 300 = 5 min
	bankroll_failures_until_kill: int  # 2 consecutive failures → KILL_AUTO_PANIC

	@classmethod
	def from_dict(cls, d: dict[str, Any]) -> "RiskConfig":
		"""Parse the ``risk:`` block from live-trader.yaml into a RiskConfig.

		All ten keys are required (C-spec L527-L548).  Raises ``KeyError`` if
		any key is absent, ``ValueError`` if any value is out of range.
		"""
		cfg = cls(
			sizing_pct=float(d["sizing_pct"]),
			daily_loss_pct=float(d["daily_loss_pct"]),
			drawdown_pct=float(d["drawdown_pct"]),
			max_open=int(d["max_open"]),
			min_fill_contracts=int(d["min_fill_contracts"]),
			absolute_panic_floor_cents=int(d["absolute_panic_floor_cents"]),
			absolute_max_cents=int(d["absolute_max_cents"]),
			kelly_shrinkage=float(d["kelly_shrinkage"]),
			bankroll_ttl_seconds=float(d["bankroll_ttl_seconds"]),
			bankroll_failures_until_kill=int(d["bankroll_failures_until_kill"]),
		)
		# Range guards — every config knob has a defined valid range. Each
		# guard prevents a specific live-money failure mode noted inline.
		if not (0 < cfg.sizing_pct < 1):
			# 0 = no position sizing; 1+ = full-bankroll sizing per trade.
			raise ValueError(f"sizing_pct must be in (0, 1), got {cfg.sizing_pct}")
		if not (0 < cfg.daily_loss_pct < 1):
			# 0 = trips on any loss (engine never trades); 1+ = never trips.
			raise ValueError(f"daily_loss_pct must be in (0, 1), got {cfg.daily_loss_pct}")
		if not (0 < cfg.drawdown_pct < 1):
			# 0 USED to mean "no drawdown gate" when PeakTracker was inert (peak
			# stayed 0 → threshold 0 → never tripped). The gate is now WIRED
			# (peak seeded + ratcheted), so dd=0 → threshold == peak → trips
			# KILL_AUTO_DRAWDOWN on ANY non-gain (equity <= peak), silently
			# halting trading; negative dd → threshold > peak → trips even on
			# gains. Reject both — a genuinely disabled gate needs a separate
			# switch, not a footgun value. 1+ = liquidation cap collapses to 0.
			raise ValueError(f"drawdown_pct must be in (0, 1), got {cfg.drawdown_pct}")
		if cfg.max_open < 1:
			# 0 max_open would block every entry; surface as config error
			# instead of letting the engine boot and silently no-op.
			raise ValueError(f"max_open must be >= 1, got {cfg.max_open}")
		if cfg.min_fill_contracts < 0:
			# 0 is a valid "no minimum" setting (the sizing arms never produce
			# negative size, so 0 effectively disables the BELOW_MIN_FILL gate
			# — handy for testing pure sizing-arm behaviour). Negative is
			# nonsensical.
			raise ValueError(
				f"min_fill_contracts must be >= 0, got {cfg.min_fill_contracts}"
			)
		if cfg.absolute_panic_floor_cents < 0:
			# Negative equity floor would trip immediately on first refresh.
			raise ValueError(
				f"absolute_panic_floor_cents must be >= 0, got {cfg.absolute_panic_floor_cents}"
			)
		if cfg.absolute_max_cents <= 0:
			# 0 per-order dollar cap blocks every entry; negative is nonsensical.
			raise ValueError(
				f"absolute_max_cents must be > 0, got {cfg.absolute_max_cents}"
			)
		if not (0.0 <= cfg.kelly_shrinkage <= 1.0):
			# Shrinkage > 1 would over-bet Kelly (math undefined); negative is
			# nonsensical. Phase 1 default is 0.5 (¼-Kelly when combined with
			# the 0.25 prefactor in _compute_kelly_arm).
			raise ValueError(
				f"kelly_shrinkage must be in [0, 1], got {cfg.kelly_shrinkage}"
			)
		if cfg.bankroll_ttl_seconds <= 0:
			# Zero TTL = is_stale() always True = perpetual refresh; negative
			# is nonsensical.
			raise ValueError(
				f"bankroll_ttl_seconds must be > 0, got {cfg.bankroll_ttl_seconds}"
			)
		if cfg.bankroll_failures_until_kill < 1:
			# 0 would trip on the very first failure (no resilience); negative
			# is nonsensical.
			raise ValueError(
				f"bankroll_failures_until_kill must be >= 1, "
				f"got {cfg.bankroll_failures_until_kill}"
			)
		return cfg


# ---------------------------------------------------------------------------
# BalanceSource Protocol + KalshiBalanceSource (Phase 1 impl)
# ---------------------------------------------------------------------------

class BalanceSource(Protocol):
	"""Venue-agnostic source for available cash.

	Phase 1 impl: ``KalshiBalanceSource(KalshiOrderClient)``.
	Future: ``PolymarketBalanceSource(PolymarketOrderClient)``, etc.

	``balance_cents()`` MUST return venue-side cash in cents (USD equivalent
	for non-USD venues; FX conversion is the adapter's responsibility, not C's).
	Async because ``KalshiOrderClient`` is async-native (see master memo CR-7).
	"""
	async def balance_cents(self) -> int: ...


@dataclass
class KalshiBalanceSource:
	"""Phase 1 ``BalanceSource`` impl.  Lives in ``engine/risk.py`` for Phase 1
	(single venue).  When a second venue lands, move to
	``edge_catcher/adapters/kalshi/risk_balance.py``.

	This is the ONLY class in ``engine/risk.py`` that references a
	venue-specific import (``KalshiOrderClient`` from ``live.client``).
	All other components in this module are venue-agnostic.
	"""
	_client: KalshiOrderClient

	async def balance_cents(self) -> int:
		"""Return available Kalshi cash in cents."""
		result = await self._client.balance()
		return result.balance_cents


# ---------------------------------------------------------------------------
# BankrollCache — async-refreshed; sync read for Gate
# ---------------------------------------------------------------------------

@dataclass
class BankrollCache:
	"""Cache layer between the venue's balance REST call and the sync Gate.

	E owns refresh orchestration:
	  - At startup (T0 engine reconcile)
	  - After every confirmed fill (``on_fill``)
	  - After every settlement (``on_settlement``)
	  - Periodic safety net at TTL/2

	``cash_cents()`` is sync — Gate calls it; E ensures the cache is fresh.

	Failure policy: consecutive refresh failures exceeding
	``bankroll_failures_until_kill`` trips ``KILL_AUTO_PANIC`` via the
	injected ``_emit_trip`` callable (set by Gate at construction).
	"""
	_source: BalanceSource
	_cfg: RiskConfig
	_cash_cents: int = field(default=0, init=False)
	_last_refresh_ts: float = field(default=0.0, init=False)
	_consecutive_failures: int = field(default=0, init=False)
	# Injected by Gate after construction so BankrollCache can trip the kill
	# switch without holding a direct reference to KillSwitch.
	_emit_trip_fn: Any = field(default=None, init=False)
	# Latches True after the first KILL_AUTO_PANIC trip of a failure streak;
	# reset on the next successful refresh. Without it a sustained outage would
	# re-trip every refresh interval — each trip stamps a fresh tripped_at, so
	# the UNIQUE(reason, tripped_at) guard does not dedup them and kill_switch
	# rows + risk-channel alerts would accumulate for the whole outage.
	_panic_tripped: bool = field(default=False, init=False)

	def cash_cents(self) -> int:
		"""Sync read — returns the last cached balance.

		Gate calls this.  E is responsible for keeping the cache fresh via
		``async refresh()``.  Staleness is exposed via ``is_stale()`` —
		E's periodic task checks this at TTL/2.
		"""
		return self._cash_cents

	def is_stale(self) -> bool:
		"""Return True if the cache is older than bankroll_ttl_seconds.

		A never-refreshed cache (``_last_refresh_ts == 0.0``) is always
		considered stale. ``time.monotonic()``'s reference point is platform-
		dependent and undefined — on a freshly-booted CI runner it returns
		small values (single-digit seconds), so the naive ``time.monotonic() -
		0 > ttl`` check would falsely report a never-refreshed cache as fresh
		until the process has been running for at least ``ttl`` seconds. The
		zero-check makes the "never refreshed = stale" invariant portable.
		"""
		if self._last_refresh_ts == 0.0:
			return True
		return (time.monotonic() - self._last_refresh_ts) > self._cfg.bankroll_ttl_seconds

	async def refresh(self) -> None:
		"""Fetch the current balance from the venue.  E awaits this.

		On network failure: increments failure counter; logs a WARNING.
		After ``bankroll_failures_until_kill`` consecutive failures: trips
		KILL_AUTO_PANIC (the gate will then reject all entries until an
		operator acks the kill-clear).

		Raises:
			KillSwitchTripFailed: when the auto-panic trip's DB INSERT fails
				(via ``_emit_trip_fn`` → ``KillSwitch.trip``). This is the
				C-spec L214 ghost-reject defense — a silent kill-INSERT failure
				would let the next tick re-enter the gate against unchanged
				DB state. Callers (E's periodic refresh task, on_fill,
				on_settlement) must NOT swallow this exception; the engine's
				WS loop and outer reconnect block both re-raise it so the
				process stops rather than continuing past a failed trip.

		All other exceptions (network errors, venue API errors, programming
		errors during balance fetch) are caught and logged; ``refresh`` does
		not raise on those paths.
		"""
		try:
			self._cash_cents = await self._source.balance_cents()
			self._last_refresh_ts = time.monotonic()
			self._consecutive_failures = 0
			self._panic_tripped = False  # recovered — re-arm the trip latch
			log.debug("Bankroll cache refreshed: %d cents", self._cash_cents)
		except (NetworkError, KalshiAPIError, Exception) as exc:
			self._consecutive_failures += 1
			log.warning(
				"Bankroll cache refresh failed: %s (consecutive_failures=%d)",
				exc, self._consecutive_failures,
			)
			if (
				self._consecutive_failures >= self._cfg.bankroll_failures_until_kill
				and self._emit_trip_fn is not None
				and not self._panic_tripped
			):
				self._panic_tripped = True  # latch — trip once per failure streak
				detail = (
					f"bankroll cache stale: {self._consecutive_failures} "
					f"consecutive refresh failures"
				)
				self._emit_trip_fn(
					"KILL_AUTO_PANIC",
					detail=detail,
					now=datetime.now(timezone.utc),
				)

	async def on_fill(self) -> None:
		"""Called by E after every confirmed fill — cash changed, refresh now."""
		await self.refresh()

	async def on_settlement(self) -> None:
		"""Called by E after every settlement — cash changed, refresh now."""
		await self.refresh()


# ---------------------------------------------------------------------------
# KillSwitch — DB-persisted auto-kill log
# ---------------------------------------------------------------------------

class KillSwitch:
	"""Persisted auto-kill log in ``live_trades.db:kill_switch``.

	Auto-trips (panic, drawdown, daily) write a row synchronously.  If the
	INSERT fails, ``trip()`` raises — the caller (Gate._emit_trip) propagates
	the error as fatal (C-spec L214).

	Operator kills (``KILL_SWITCH=1`` env var or SIGTERM) are NOT persisted
	here — they live in ``RiskContext.operator_kill_active`` (set by E).

	Auto-clear behaviour for daily cap: if a ``KILL_AUTO_DAILY`` row exists
	from a UTC date earlier than today, ``active_auto_kill()`` marks it
	auto-cleared in-place and returns None — no separate cron job needed.
	"""

	def __init__(self, conn: sqlite3.Connection) -> None:
		self._conn = conn

	def active_auto_kill(self, now: datetime) -> KillRow | None:
		"""Return the active auto-kill row, or None if none is active.

		Side effect: if a ``KILL_AUTO_DAILY`` row from a previous UTC day
		is found, it is auto-cleared in the same call (``cleared_by =
		'auto_midnight'``).

		Args:
			now: Current UTC datetime (engine clock, not wall clock).
		"""
		rows = self._conn.execute(
			"SELECT id, reason, detail, tripped_at, cleared_at, cleared_by "
			"FROM kill_switch WHERE cleared_at IS NULL "
			"ORDER BY id ASC"
		).fetchall()

		for row in rows:
			kill_id, reason, detail, tripped_at_str, cleared_at, cleared_by = row
			kill_row = KillRow(
				id=kill_id,
				reason=reason,
				detail=detail,
				tripped_at=tripped_at_str,
				cleared_at=cleared_at,
				cleared_by=cleared_by,
			)

			if reason == "KILL_AUTO_DAILY":
				# Auto-clear if the trip was on a previous UTC day
				tripped_date = datetime.fromisoformat(tripped_at_str).date()
				today = now.date()
				if tripped_date < today:
					log.info(
						"Auto-clearing daily kill (id=%d) from %s — UTC day boundary",
						kill_id, tripped_date,
					)
					self._auto_clear(kill_id, now)
					continue  # don't return this row; check next

			return kill_row   # first active row found

		return None

	def trip(
		self,
		reason: GateRejectReason,
		detail: str,
		now: datetime,
	) -> None:
		"""Insert a new kill row synchronously.

		Raises ``KillSwitchTripFailed`` (chained from the underlying
		``sqlite3.Error``) on any DB failure — the engine MUST stop
		(C-spec L214 ghost-reject defense). dispatch.process_tick re-raises
		this class specifically so the broad signal-level except cannot
		swallow it.
		"""
		tripped_at = now.isoformat()
		log.error(
			"Kill switch TRIPPED: reason=%s detail=%s tripped_at=%s",
			reason, detail, tripped_at,
		)
		try:
			self._conn.execute(
				"INSERT INTO kill_switch (reason, detail, tripped_at) VALUES (?, ?, ?)",
				(reason, detail, tripped_at),
			)
			self._conn.commit()
		except sqlite3.Error as exc:
			log.error(
				"Kill switch INSERT FAILED: %s — engine MUST stop (ghost-reject defense)",
				exc,
			)
			raise KillSwitchTripFailed(
				f"kill_switch INSERT failed for reason={reason!r}: {exc}"
			) from exc

	def clear(self, kill_id: int, cleared_by: str, now: datetime) -> None:
		"""Manual clear — operator runs CLI kill-clear.

		Args:
			kill_id: The ``id`` of the kill row to clear.
			cleared_by: Audit string, e.g. ``'human:investigated, resuming'``.
			now: Current UTC datetime.

		Raises:
			KillSwitchClearError: If no kill_switch row matches ``kill_id``.
				CLI surfaces this as an operator error rather than silently
				succeeding (which would mislead the operator into thinking
				the gate was cleared when it wasn't).
		"""
		cursor = self._conn.execute(
			"UPDATE kill_switch SET cleared_at=?, cleared_by=? WHERE id=?",
			(now.isoformat(), cleared_by, kill_id),
		)
		self._conn.commit()
		if cursor.rowcount == 0:
			raise KillSwitchClearError(
				f"No kill_switch row with id={kill_id} — nothing cleared"
			)
		log.info("Kill switch CLEARED: id=%d cleared_by=%r", kill_id, cleared_by)

	def _auto_clear(self, kill_id: int, now: datetime) -> None:
		"""Internal: clear a daily-cap kill at UTC midnight boundary."""
		self._conn.execute(
			"UPDATE kill_switch SET cleared_at=?, cleared_by='auto_midnight' WHERE id=?",
			(now.isoformat(), kill_id),
		)
		self._conn.commit()


# ---------------------------------------------------------------------------
# PeakTracker — closed-equity peak, persisted in risk_state table
# ---------------------------------------------------------------------------

_PEAK_KEY = "closed_equity_peak"


class PeakTracker:
	"""Tracks the all-time high of closed-equity, persisted in ``risk_state``.

	Peak is seeded at engine startup (``initialize_if_unset``) and updated
	only on trade close (``on_trade_close``), never on intraday MTM swings.

	Persistence means a process restart does NOT reset the peak — the
	drawdown gate remains calibrated against the historical high-water mark.
	"""

	def __init__(self, conn: sqlite3.Connection) -> None:
		self._conn = conn
		self._cached_peak_cents: int = self._load()

	def _load(self) -> int:
		"""Load the peak from DB on construction.  Returns 0 if no row yet."""
		row = self._conn.execute(
			"SELECT value FROM risk_state WHERE key=?", (_PEAK_KEY,)
		).fetchone()
		if row is None:
			return 0
		data = json.loads(row[0])
		return int(data["cents"])

	def peak_cents(self) -> int:
		"""Return the current closed-equity peak in cents (cached)."""
		return self._cached_peak_cents

	def initialize_if_unset(self, current_equity_cents: int, now: datetime) -> None:
		"""First-ever startup seed — atomic INSERT OR IGNORE.

		If a row already exists this is a no-op (idempotent across restarts).
		Without this seed, the drawdown gate would trip immediately on a cold
		DB because peak=0 and every equity value ≥ peak*(1-0.05) is false
		only when equity ≤ 0.

		Args:
			current_equity_cents: The equity at engine startup (cash only at
				boot, no open positions yet).
			now: Current UTC datetime.
		"""
		value_json = json.dumps({"cents": current_equity_cents})
		self._conn.execute(
			"INSERT OR IGNORE INTO risk_state (key, value, updated_at) VALUES (?, ?, ?)",
			(_PEAK_KEY, value_json, now.isoformat()),
		)
		self._conn.commit()
		# Reload so the cached value reflects reality (whether we inserted or not)
		self._cached_peak_cents = self._load()
		log.info("PeakTracker initialized: peak=%dc", self._cached_peak_cents)

	def on_trade_close(self, equity_cents_at_close: int, now: datetime) -> None:
		"""Update peak if equity_cents_at_close exceeds the current peak.

		Called by E (or B) from the trade-close handler.  Peak never
		decreases without an explicit admin reset (out of Phase 1 scope).
		"""
		if equity_cents_at_close > self._cached_peak_cents:
			self._cached_peak_cents = equity_cents_at_close
			self._persist(now)
			log.info("PeakTracker updated: new_peak=%dc", self._cached_peak_cents)

	def reseed_if_zero(self, equity_cents: int, now: datetime) -> None:
		"""Establish the peak from ``equity_cents`` ONLY if it is currently 0.

		Recovers from a failed boot seed: if ``build_risk_module``'s startup
		bankroll refresh failed (Kalshi unreachable), ``initialize_if_unset``
		persisted peak=0; once cash later becomes available, ``gate_entry`` calls
		this to lift the peak so the drawdown gate goes live. No-op once a real
		(>0) peak exists, so it NEVER lowers a ratcheted peak (review Finding 1).
		"""
		if self._cached_peak_cents == 0 and equity_cents > 0:
			self._cached_peak_cents = equity_cents
			self._persist(now)
			log.info("PeakTracker re-seeded from 0: peak=%dc", self._cached_peak_cents)

	def _persist(self, now: datetime) -> None:
		value_json = json.dumps({"cents": self._cached_peak_cents})
		self._conn.execute(
			"INSERT OR REPLACE INTO risk_state (key, value, updated_at) VALUES (?, ?, ?)",
			(_PEAK_KEY, value_json, now.isoformat()),
		)
		self._conn.commit()


# ---------------------------------------------------------------------------
# RiskContext — frozen value object built per-gate-call by E's dispatch
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class RiskContext:
	"""All inputs the gate needs for a single evaluation.

	Built fresh per gate call by E's dispatch path.  Frozen + slots ensures
	the gate cannot mutate inputs and makes unit tests trivial (build context
	with synthetic values, assert decision).

	``open_count`` counts open+pending+exit_pending rows (all MAX_OPEN slots);
	``open_positions`` is status='open' only (for equity MTM) — they
	intentionally differ (spec §3).  The caller (RiskContextProvider) supplies
	``open_count`` via ``read_open_count`` so in-flight ``pending`` entries
	correctly hold their MAX_OPEN slot.
	"""
	now_utc: datetime
	market_state: MarketState
	open_positions: list[OpenPosition]
	open_count: int
	daily_pnl_cents: int
	operator_kill_active: bool


# ---------------------------------------------------------------------------
# Internal sizing result
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class _SizingResult:
	size: int
	breakdown: SizingBreakdown


# ---------------------------------------------------------------------------
# Gate — the core risk gate
# ---------------------------------------------------------------------------

class Gate:
	"""Risk gate consulted before every ``Executor.place`` in the live path.

	``gate_entry`` and ``gate_exit`` are sync.  All network I/O lives in
	``BankrollCache.refresh()`` (async) — E schedules it.

	Construction via ``build_risk_module`` factory (below).
	"""

	def __init__(
		self,
		cfg: RiskConfig,
		bankroll: BankrollCache,
		kill_switch: KillSwitch,
		peak_tracker: PeakTracker,
		event_callbacks: list[Any] | None = None,
	) -> None:
		self._cfg = cfg
		self._bankroll = bankroll
		self._kill_switch = kill_switch
		self._peak_tracker = peak_tracker
		# Callbacks invoked with (RiskEvent,) after every trip/clear.
		# E registers its _handle_risk_event here; tests may register spies.
		self._event_callbacks: list[Any] = event_callbacks or []
		# Wire the bankroll cache's trip function back to us
		self._bankroll._emit_trip_fn = self._emit_trip

	def gate_entry(self, sig: Signal, ctx: RiskContext) -> GateDecision:
		"""Evaluate an entry signal against all risk checks.

		Ordered gate — first match wins:
		  1. Operator kill (env/SIGTERM) — not persisted
		  2. Persisted auto-kill (steady-state path)
		  3-5. First-trip branches (panic, drawdown, daily) — persisted on trip
		  6. Max open positions
		  7. Sizing
		  8. Min-fill threshold

		Returns:
			``Allow(size_contracts, sizing_breakdown)`` or
			``Reject(reason, detail)``.
		"""
		# 1. Operator kill — env var or SIGTERM in flight (not persisted)
		if ctx.operator_kill_active:
			return Reject(
				"KILL_OPERATOR",
				detail="KILL_SWITCH env var or SIGTERM in flight",
			)

		# 2. Persisted auto-kill — steady-state path after a previous trip
		auto_kill = self._kill_switch.active_auto_kill(now=ctx.now_utc)
		if auto_kill is not None:
			return Reject(
				auto_kill.reason,
				detail=auto_kill.detail,
			)

		# Signal validity — guards _compute_size from div-by-zero
		entry = sig.entry_price_cents or 0
		sl = sig.stop_loss_distance_cents or 0
		if entry <= 0 or sl <= 0:
			return Reject(
				"INVALID_SIGNAL",
				detail=f"entry={entry}c sl={sl}c",
			)

		# Staleness backstop (spec §5.3): never make an equity-based decision
		# against an untrusted balance. SOFT, transient — NOT a persisted kill;
		# auto-recovers on the next successful refresh. Faster tripwire than the
		# refresh-failure -> KILL_AUTO_PANIC path.
		if self._bankroll.is_stale():
			return Reject(
				"STALE_BANKROLL",
				detail="bankroll cache older than TTL — entry gated until refresh",
			)

		# Equity — recomputed fresh each gate call
		equity_cents = self._compute_equity(ctx)

		# §4.3 NORMATIVE — tripped-kill ≠ process exit. The three first-trip
		# branches below call ``self._emit_trip(...)`` then ``return Reject``.
		# On a SUCCESSFUL trip ``_emit_trip`` returns normally (the kill row is
		# persisted) — NO exception propagates, so ``process_tick`` /
		# ``_handle_signal`` / the WS loop / the reconnect block all CONTINUE
		# and the engine keeps RUNNING. From here on the steady-state path
		# (``active_auto_kill`` at branch 2) returns ``Reject`` so every NEW
		# entry is blocked, while ``gate_exit`` still allows exits (only an
		# operator kill blocks exits). The process stops ONLY on a crash or
		# ``systemctl stop`` (SIGTERM → F1 bridge → the §4.3 F2 drain). This is
		# what makes the live unit's ``Restart=always`` safe: a tripped
		# auto-kill never reaches systemd, so a restart can never clear the
		# KILL state and let previously-blocked trades flow.
		#
		# DISTINCT — ``KillSwitchTripFailed``: if ``_emit_trip``'s DB INSERT
		# FAILS it raises ``KillSwitchTripFailed`` (NOT caught here) which
		# propagates out of ``run_engine`` and STOPS the process (C-spec L214
		# ghost-reject defense — a ghost reject with no persisted row would
		# double-trip next tick). A FAILED kill-WRITE must halt; a SUCCESSFUL
		# kill must NOT. These are deliberately opposite and must stay so.

		# 3. Absolute panic floor — first-trip branch
		if equity_cents <= self._cfg.absolute_panic_floor_cents:
			detail = (
				f"equity={equity_cents}c "
				f"floor={self._cfg.absolute_panic_floor_cents}c"
			)
			self._emit_trip("KILL_AUTO_PANIC", detail=detail, now=ctx.now_utc)
			return Reject("KILL_AUTO_PANIC", detail=detail)

		# 4. Drawdown from closed-equity peak — first-trip branch
		peak_cents = self._peak_tracker.peak_cents()
		if peak_cents == 0:
			# Lazy re-seed (review Finding 1): a boot bankroll-refresh failure
			# can leave peak=0; once a later refresh recovers cash (we are past
			# STALE_BANKROLL + the panic floor here, so cash is real + funded)
			# establish the peak from cash NOW so the drawdown gate is live from
			# THIS entry — otherwise it stays 0 (threshold 0, never trips) until
			# the first confirmed close. Fail-soft: a persist error is swallowed
			# (peak is monitoring-only; the next entry retries). Cash-based to
			# mirror the boot seed (closed-equity semantics, excludes MTM).
			try:
				self._peak_tracker.reseed_if_zero(
					self._bankroll.cash_cents(), ctx.now_utc
				)
			except sqlite3.Error:
				log.warning(
					"lazy peak re-seed failed (ignored — monitoring-only, "
					"retries next entry)", exc_info=True,
				)
			peak_cents = self._peak_tracker.peak_cents()
		drawdown_threshold_cents = int(peak_cents * (1.0 - self._cfg.drawdown_pct))
		if equity_cents <= drawdown_threshold_cents:
			detail = (
				f"equity={equity_cents}c "
				f"peak={peak_cents}c "
				f"threshold={drawdown_threshold_cents}c"
			)
			self._emit_trip("KILL_AUTO_DRAWDOWN", detail=detail, now=ctx.now_utc)
			return Reject("KILL_AUTO_DRAWDOWN", detail=detail)

		# 5. Daily loss cap (UTC day boundary, auto-clear at midnight) — first-trip
		daily_cap_cents = -int(equity_cents * self._cfg.daily_loss_pct)
		if ctx.daily_pnl_cents <= daily_cap_cents:
			detail = (
				f"daily_pnl={ctx.daily_pnl_cents}c "
				f"cap={daily_cap_cents}c"
			)
			self._emit_trip("KILL_AUTO_DAILY", detail=detail, now=ctx.now_utc)
			return Reject("KILL_AUTO_DAILY", detail=detail)

		# 6. Max open positions
		if ctx.open_count >= self._cfg.max_open:
			return Reject("MAX_OPEN", detail=f"open_count={ctx.open_count}")

		# 7. Sizing
		sizing = self._compute_size(sig, equity_cents)

		# 8. Min-fill threshold — also reject size<=0 so Allow never carries a
		# non-placeable size (size=0 raises ValueError in build_entry_order).
		# This fires regardless of min_fill_contracts (even 0 for testing),
		# because a 0-contract order is not a valid Kalshi placement.
		if sizing.size <= 0 or sizing.size < self._cfg.min_fill_contracts:
			return Reject("BELOW_MIN_FILL", detail=f"size={sizing.size}")

		log.info(
			"Gate ALLOW: %s size=%d bound_by=%s equity=%dc",
			sig.ticker, sizing.size, sizing.breakdown.bound_by, equity_cents,
		)
		return Allow(size_contracts=sizing.size, sizing_breakdown=sizing.breakdown)

	def gate_exit(self, sig: Signal, ctx: RiskContext) -> GateDecision:
		"""Evaluate an exit signal.

		Only the operator kill blocks exits.  Auto-tripped caps allow exits
		because exits reduce risk; trapping existing exposure would be worse.

		Returns:
			``Allow`` with the existing position size, or
			``Reject("KILL_OPERATOR", ...)`` if operator kill is active.
		"""
		if ctx.operator_kill_active:
			return Reject(
				"KILL_OPERATOR",
				detail="exit blocked by operator kill",
			)
		# Position size for exit is determined by D's build_exit_order from
		# the position row; we return the open_count as a proxy here.
		# E/D read the actual position size from the trade store, not from C.
		position_size = sum(p.fill_size for p in ctx.open_positions
		                    if p.ticker == sig.ticker)
		breakdown = SizingBreakdown(
			fixed_fraction_contracts=position_size,
			quarter_kelly_contracts=position_size,
			absolute_max_contracts=position_size,
			bound_by="fixed_fraction",
		)
		return Allow(size_contracts=position_size, sizing_breakdown=breakdown)

	def record_trade_close(self, ctx: RiskContext) -> None:
		"""Ratchet the closed-equity peak at a CONFIRMED trade close (spec §3.2).

		Samples the SAME conservative-MTM equity the drawdown gate compares
		against (``_compute_equity``) and offers it to the peak (ratchets up
		only). Caller MUST invoke this only after a close has persisted
		(settlement, or a confirmed-full-fill exit) — never on a partial/no-fill,
		which would inflate the peak on a non-close and cause a premature halt.

		Fail-soft: a peak-persist DB error logs and continues — the peak is a
		monitoring value, NOT a money gate. This is DELIBERATELY OPPOSITE
		``_emit_trip`` (a failed kill-write is fatal, C-spec L214). A stale-low
		peak self-heals on the next successful close.
		"""
		try:
			equity = self._compute_equity(ctx)
			self._peak_tracker.on_trade_close(equity, ctx.now_utc)
		except sqlite3.Error:
			log.warning(
				"record_trade_close: peak persist failed (ignored — peak is "
				"monitoring-only, self-heals next close)", exc_info=True,
			)

	# ------------------------------------------------------------------
	# Internal helpers
	# ------------------------------------------------------------------

	def _compute_equity(self, ctx: RiskContext) -> int:
		"""Cash + conservative MTM of open positions (long@bid, short@ask)."""
		cash = self._bankroll.cash_cents()
		mtm = sum(
			self._mark_position_cents(pos, ctx.market_state)
			for pos in ctx.open_positions
		)
		return cash + mtm

	def _mark_position_cents(self, pos: OpenPosition, ms: MarketState) -> int:
		"""Conservative-side mark of one open position.

		Long marks at bid (yes_levels[0]), short marks at ask (no_levels[0]).
		Falls back to cost basis when the book is empty or missing.

		Prices enter as ``(price_dollars: float, size: int)`` tuples per
		``OrderbookSnapshot``'s shape.  We convert to cents at every read
		boundary via ``round(level[0] * 100)`` — no float-cent values flow
		inside C's logic.
		"""
		book = ms.get_orderbook(pos.ticker)
		if book is None:
			return pos.fill_size * pos.blended_entry_cents

		# Long=yes buys at ask (yes_ask = yes_levels best offer price)
		# Conservative mark for a long position is the current bid (what we
		# could sell at) = best level on the YES side.
		# Short=no: conservative mark is the best level on the NO side.
		levels = book.yes_levels if pos.side == "yes" else book.no_levels
		if not levels:
			return pos.fill_size * pos.blended_entry_cents

		best_cents = round(levels[0][0] * 100)
		if best_cents <= 0:
			return pos.fill_size * pos.blended_entry_cents

		return pos.fill_size * best_cents

	def _compute_size(self, sig: Signal, equity_cents: int) -> _SizingResult:
		"""Min of three sizing arms (C-spec §Sizing function)."""
		sl_cents = max(1, sig.stop_loss_distance_cents or 1)
		entry_cents = max(1, sig.entry_price_cents or 1)

		# Arm 1: fixed-fraction of equity
		fixed_fraction = int(equity_cents * self._cfg.sizing_pct / sl_cents)

		# Arm 2: ¼-Kelly (returns 2**31 sentinel when strategy has no edge config)
		kelly_arm = self._compute_kelly_arm(sig, equity_cents, sl_cents)

		# Arm 3: absolute-max dollar guard (bug protection)
		absolute_max = self._cfg.absolute_max_cents // entry_cents

		# Pick the tightest binding arm
		arms = [
			("fixed_fraction", fixed_fraction),
			("quarter_kelly", kelly_arm),
			("absolute_max", absolute_max),
		]
		bound_name, bound_size = min(arms, key=lambda kv: kv[1])

		return _SizingResult(
			size=bound_size,
			breakdown=SizingBreakdown(
				fixed_fraction_contracts=fixed_fraction,
				quarter_kelly_contracts=kelly_arm,
				absolute_max_contracts=absolute_max,
				bound_by=bound_name,  # type: ignore[arg-type]
			),
		)

	def _compute_kelly_arm(
		self,
		sig: Signal,
		equity_cents: int,
		sl_cents: int,
	) -> int:
		"""Return the ¼-Kelly contract count, or 2**31 (≈ +inf) if inert.

		Phase 1: ``Signal`` has no ``edge_config`` — returns the sentinel so
		the fixed-fraction arm always binds.  Phase 2+: strategies populate
		``edge_config`` and this arm starts contributing.
		"""
		# Signal has no edge_config in Phase 1
		edge = getattr(sig, "edge_config", None)
		if edge is None:
			return 2**31

		win_prob = getattr(edge, "win_prob", 0.0)
		payout_ratio = getattr(edge, "payout_ratio", 1.0)
		kelly_shrinkage = self._cfg.kelly_shrinkage

		if not (0 < win_prob < 1):
			return 2**31

		p = win_prob * kelly_shrinkage   # shrunk win probability
		b = payout_ratio
		# Raw Kelly fraction
		f = max(0.0, (b * p - (1 - p)) / b)
		quarter_kelly = 0.25 * f * equity_cents / sl_cents
		return int(quarter_kelly)

	def _emit_trip(
		self,
		reason: GateRejectReason,
		detail: str,
		now: datetime,
	) -> None:
		"""Write the kill row to DB synchronously and emit a RiskEvent.

		CRITICAL: If the DB INSERT fails, this raises — the gate propagates
		the error as fatal.  A "ghost" reject without DB persistence means
		the next tick re-evaluates against unchanged state and double-trips
		(C-spec L214).

		Severity mapping:
		  KILL_AUTO_PANIC / KILL_AUTO_DRAWDOWN → error
		  KILL_AUTO_DAILY → warn
		"""
		# KillSwitch.trip() raises on INSERT failure — correct, intentional
		self._kill_switch.trip(reason=reason, detail=detail, now=now)

		severity: Literal["info", "warn", "error"]
		if reason == "KILL_AUTO_DAILY":
			severity = "warn"
		else:
			severity = "error"

		event = RiskEvent(
			kind="trip",
			reason=reason,
			detail=detail,
			severity=severity,
			occurred_at=now,
		)
		for cb in self._event_callbacks:
			try:
				cb(event)
			except Exception:
				log.exception("RiskEvent callback raised (ignored): %s", cb)


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

async def build_risk_module(
	config: dict[str, Any],
	db_conn: sqlite3.Connection,
	kalshi_client: KalshiOrderClient,
) -> Gate:
	"""Wire all risk components from config + injected deps.

	Async because we pre-refresh ``BankrollCache`` before returning so the
	first ``Gate.gate_entry`` call sees real cash rather than the 0-default.
	Without the pre-refresh, equity = cash + mtm = 0 + 0 ≤ absolute_panic_
	floor_cents (3000) on the very first signal, tripping KILL_AUTO_PANIC
	on every clean startup. If the pre-refresh FAILS (the rare "Kalshi
	unreachable at boot" case), ``_last_refresh_ts`` stays at its 0.0 default,
	so ``is_stale()`` is True and the first ``gate_entry`` returns the soft,
	non-persisted ``STALE_BANKROLL`` reject — entries are gated before the
	equity/panic branch is reached, no order placed. A persisted
	KILL_AUTO_PANIC still escalates later via the periodic refresh-failure
	path once ``bankroll_failures_until_kill`` consecutive failures accrue.

	Args:
		config: The full live-trader.yaml parsed dict.  Must contain a
			``risk:`` block matching ``RiskConfig.from_dict``.
		db_conn: Open SQLite connection to ``live_trades.db``.  Caller
			owns the connection lifecycle.
		kalshi_client: Async Kalshi REST client (from ``live.client``).

	Returns:
		A fully wired ``Gate`` ready for ``gate_entry`` / ``gate_exit`` calls,
		with the bankroll cache pre-populated when Kalshi is reachable.
	"""
	risk_block = config.get("risk", config)
	cfg = RiskConfig.from_dict(risk_block)

	balance_source: BalanceSource = KalshiBalanceSource(_client=kalshi_client)
	bankroll = BankrollCache(_source=balance_source, _cfg=cfg)
	kill_switch = KillSwitch(conn=db_conn)
	peak_tracker = PeakTracker(conn=db_conn)

	# Pre-refresh BEFORE Gate construction — at this point ``_emit_trip_fn``
	# is still None, so a refresh failure cannot fire a phantom kill trip.
	# refresh() never raises on its own (network/API errors are caught and
	# logged); only the post-Gate refresh path can trip via _emit_trip_fn.
	await bankroll.refresh()

	# Seed the closed-equity peak from the cash just refreshed above (spec §3.1).
	# At boot the live account is flat (CR-3) so equity == cash. INSERT OR
	# IGNORE: seeds a fresh DB, no-op when a peak row already exists (restart-
	# safe). If the pre-refresh failed (Kalshi unreachable at boot) cash is 0
	# and this seeds peak=0; gate_entry's lazy reseed_if_zero then lifts the
	# peak from cash on the first entry after a refresh recovers — STALE_BANKROLL
	# only gates entries UNTIL that recovery, so the lazy reseed (NOT "self-
	# healing on first close") is what closes the gap (review Finding 1, spec §8).
	peak_tracker.initialize_if_unset(bankroll.cash_cents(), datetime.now(timezone.utc))

	return Gate(
		cfg=cfg,
		bankroll=bankroll,
		kill_switch=kill_switch,
		peak_tracker=peak_tracker,
	)
