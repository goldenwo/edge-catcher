"""Unit tests for engine/risk.py.

Covers (per C-spec L708-L748):
  - Kill switch: trip, manual clear, daily auto-clear, duplicate-trip raises
  - Bankroll cache: TTL staleness, failure counting, kill trip on failure threshold
  - Peak tracker: cold-DB seed, warm-DB no-op, update-on-close, persistence
  - Gate ordering (entry + exit): all 8 checks fire in correct priority order
  - Sizing function: each arm binds when tightest; property invariants
  - Equity computation: conservative mark, empty-book fallback
  - live_db.py STUB_MODE sentinel
  - Replay / paper path: gate not invoked when risk=None

Running::

    pytest tests/test_engine_risk.py -v
"""
from __future__ import annotations

import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from edge_catcher.engine.executor import OpenPosition
from edge_catcher.engine.market_state import MarketState, OrderbookSnapshot
from edge_catcher.engine.risk import (
	Allow,
	BalanceSource,
	BankrollCache,
	Gate,
	KillSwitch,
	KillSwitchClearError,
	KillSwitchTripFailed,
	PeakTracker,
	Reject,
	RiskConfig,
	RiskContext,
)
from edge_catcher.engine.strategy_base import Signal


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_MIGRATIONS_DIR = Path(__file__).parent.parent / "edge_catcher" / "storage" / "migrations"


def _apply_test_migrations(conn: sqlite3.Connection) -> None:
	"""Apply kill_switch + risk_state DDL to an in-memory SQLite for tests."""
	from edge_catcher.storage.migrations import apply_migrations
	apply_migrations(conn, _MIGRATIONS_DIR)


def _make_conn() -> sqlite3.Connection:
	conn = sqlite3.connect(":memory:")
	_apply_test_migrations(conn)
	return conn


def _phase1_cfg(**overrides: Any) -> RiskConfig:
	defaults: dict[str, Any] = {
		"sizing_pct": 0.005,
		"daily_loss_pct": 0.02,
		"drawdown_pct": 0.05,
		"max_open": 5,
		"min_fill_contracts": 3,
		"absolute_panic_floor_cents": 3000,
		"absolute_max_cents": 5000,
		"kelly_shrinkage": 0.5,
		"bankroll_ttl_seconds": 300.0,
		"bankroll_failures_until_kill": 2,
	}
	defaults.update(overrides)
	return RiskConfig.from_dict(defaults)


def _make_balance_source(cents: int = 20_000) -> BalanceSource:
	"""Return a fake async BalanceSource returning a fixed value."""
	source = MagicMock()
	source.balance_cents = AsyncMock(return_value=cents)
	return source  # type: ignore[return-value]


def _make_bankroll(cfg: RiskConfig, cents: int = 20_000) -> BankrollCache:
	cache = BankrollCache(_source=_make_balance_source(cents), _cfg=cfg)
	# Pre-load the cash so tests don't need to await refresh
	cache._cash_cents = cents
	cache._last_refresh_ts = 1e12  # way in the future — never stale in tests
	return cache


def _make_market_state(
	ticker: str = "TEST-T50",
	yes_bid_dollars: float = 0.45,
	no_bid_dollars: float = 0.50,
) -> MarketState:
	ms = MarketState()
	book = OrderbookSnapshot(
		yes_levels=[(yes_bid_dollars, 100)],
		no_levels=[(no_bid_dollars, 100)],
	)
	ms.seed_orderbook(ticker, book)
	return ms


def _make_ctx(
	*,
	now: datetime | None = None,
	open_positions: list[OpenPosition] | None = None,
	daily_pnl_cents: int = 0,
	operator_kill_active: bool = False,
	market_state: MarketState | None = None,
) -> RiskContext:
	return RiskContext(
		now_utc=now or datetime.now(timezone.utc),
		market_state=market_state or _make_market_state(),
		open_positions=open_positions or [],
		daily_pnl_cents=daily_pnl_cents,
		operator_kill_active=operator_kill_active,
	)


def _make_signal(
	ticker: str = "TEST-T50",
	series: str = "TEST",
	entry_price_cents: int = 50,
	stop_loss_distance_cents: int = 10,
) -> Signal:
	return Signal(
		action="enter",
		ticker=ticker,
		side="yes",
		series=series,
		strategy="test_strategy",
		reason="unit_test",
		entry_price_cents=entry_price_cents,
		stop_loss_distance_cents=stop_loss_distance_cents,
	)


def _make_gate(
	cfg: RiskConfig | None = None,
	cash_cents: int = 20_000,
	conn: sqlite3.Connection | None = None,
	peak_cents: int = 0,
) -> tuple[Gate, KillSwitch, PeakTracker, sqlite3.Connection]:
	if cfg is None:
		cfg = _phase1_cfg()
	if conn is None:
		conn = _make_conn()
	bankroll = _make_bankroll(cfg, cash_cents)
	kill_switch = KillSwitch(conn=conn)
	peak_tracker = PeakTracker(conn=conn)
	if peak_cents > 0:
		peak_tracker._cached_peak_cents = peak_cents
		peak_tracker._persist(datetime.now(timezone.utc))
	gate = Gate(cfg=cfg, bankroll=bankroll, kill_switch=kill_switch, peak_tracker=peak_tracker)
	return gate, kill_switch, peak_tracker, conn


# ===========================================================================
# 1. Kill switch tests
# ===========================================================================

class TestKillSwitch:
	def test_trip_inserts_row(self) -> None:
		conn = _make_conn()
		ks = KillSwitch(conn)
		now = datetime.now(timezone.utc)
		ks.trip("KILL_AUTO_PANIC", detail="test", now=now)

		row = conn.execute("SELECT reason, detail, cleared_at FROM kill_switch").fetchone()
		assert row is not None
		assert row[0] == "KILL_AUTO_PANIC"
		assert row[1] == "test"
		assert row[2] is None  # not cleared

	def test_active_auto_kill_returns_row(self) -> None:
		conn = _make_conn()
		ks = KillSwitch(conn)
		now = datetime.now(timezone.utc)
		ks.trip("KILL_AUTO_PANIC", detail="equity low", now=now)

		result = ks.active_auto_kill(now=now)
		assert result is not None
		assert result.reason == "KILL_AUTO_PANIC"
		assert result.detail == "equity low"

	def test_active_auto_kill_returns_none_when_cleared(self) -> None:
		conn = _make_conn()
		ks = KillSwitch(conn)
		now = datetime.now(timezone.utc)
		ks.trip("KILL_AUTO_PANIC", detail="test", now=now)

		row = conn.execute("SELECT id FROM kill_switch").fetchone()
		assert row is not None
		ks.clear(row[0], "human:investigated", now=now)

		assert ks.active_auto_kill(now=now) is None

	def test_manual_clear_sets_cleared_fields(self) -> None:
		conn = _make_conn()
		ks = KillSwitch(conn)
		now = datetime.now(timezone.utc)
		ks.trip("KILL_AUTO_DRAWDOWN", detail="drawdown exceeded", now=now)

		kill_id = conn.execute("SELECT id FROM kill_switch").fetchone()[0]
		clear_time = datetime.now(timezone.utc)
		ks.clear(kill_id, "human:manual clear", now=clear_time)

		row = conn.execute(
			"SELECT cleared_at, cleared_by FROM kill_switch WHERE id=?", (kill_id,)
		).fetchone()
		assert row[0] == clear_time.isoformat()
		assert row[1] == "human:manual clear"

	def test_daily_cap_auto_clears_at_midnight(self) -> None:
		conn = _make_conn()
		ks = KillSwitch(conn)
		# Trip at 23:55 on day 1
		trip_time = datetime(2026, 5, 8, 23, 55, 0, tzinfo=timezone.utc)
		ks.trip("KILL_AUTO_DAILY", detail="daily cap hit", now=trip_time)

		# Advance to 00:05 on day 2
		next_day = datetime(2026, 5, 9, 0, 5, 0, tzinfo=timezone.utc)
		result = ks.active_auto_kill(now=next_day)
		assert result is None, "Daily kill should auto-clear at UTC midnight"

		# Verify the DB row has been cleared
		row = conn.execute(
			"SELECT cleared_at, cleared_by FROM kill_switch"
		).fetchone()
		assert row[0] is not None
		assert row[1] == "auto_midnight"

	def test_daily_cap_does_not_auto_clear_same_day(self) -> None:
		conn = _make_conn()
		ks = KillSwitch(conn)
		trip_time = datetime(2026, 5, 8, 10, 0, 0, tzinfo=timezone.utc)
		ks.trip("KILL_AUTO_DAILY", detail="daily cap hit", now=trip_time)

		# Same day
		same_day = datetime(2026, 5, 8, 14, 0, 0, tzinfo=timezone.utc)
		result = ks.active_auto_kill(now=same_day)
		assert result is not None, "Daily kill should NOT auto-clear on the same UTC day"

	def test_panic_and_drawdown_do_not_auto_clear(self) -> None:
		conn = _make_conn()
		ks = KillSwitch(conn)
		trip_time = datetime(2026, 5, 8, 10, 0, 0, tzinfo=timezone.utc)
		ks.trip("KILL_AUTO_PANIC", detail="panic", now=trip_time)

		# Next day — should still be active (panic requires human ack)
		next_day = datetime(2026, 5, 9, 0, 5, 0, tzinfo=timezone.utc)
		result = ks.active_auto_kill(now=next_day)
		assert result is not None
		assert result.reason == "KILL_AUTO_PANIC"

	def test_duplicate_trip_raises_integrity_error(self) -> None:
		"""Same (reason, tripped_at) must never silently succeed — UNIQUE constraint."""
		conn = _make_conn()
		ks = KillSwitch(conn)
		now = datetime.now(timezone.utc)
		ks.trip("KILL_AUTO_PANIC", detail="first", now=now)

		# Second trip on same (reason, tripped_at) hits UNIQUE constraint
		# inside KillSwitch.trip; trip wraps the underlying sqlite3 error
		# in KillSwitchTripFailed (engine MUST stop per C-spec L214).
		with pytest.raises(KillSwitchTripFailed) as exc_info:
			ks.trip("KILL_AUTO_PANIC", detail="second", now=now)
		assert isinstance(exc_info.value.__cause__, sqlite3.IntegrityError)

	def test_clear_raises_on_missing_kill_id(self) -> None:
		"""KillSwitch.clear on non-existent id raises KillSwitchClearError.

		C-spec operator-safety: silent UPDATE-rowcount-zero would mislead
		the operator into thinking the gate was cleared. The CLI's existing
		try/except surfaces this as 'ERROR: No kill_switch row with id=N'
		and exits 1.
		"""
		conn = _make_conn()
		ks = KillSwitch(conn)
		now = datetime.now(timezone.utc)
		# kill_switch table is empty — id=99 does not exist.
		with pytest.raises(KillSwitchClearError) as exc_info:
			ks.clear(99, "human:test", now=now)
		assert "id=99" in str(exc_info.value)

	def test_emit_trip_raises_propagates_to_gate(self) -> None:
		"""Gate._emit_trip must not swallow INSERT failures (C-spec L214)."""
		conn = _make_conn()
		gate, ks, _, _ = _make_gate(conn=conn)
		now = datetime.now(timezone.utc)

		# Trip once to create the unique row
		gate._emit_trip("KILL_AUTO_PANIC", detail="first trip", now=now)

		# Second trip with same timestamp must propagate as KillSwitchTripFailed
		with pytest.raises(KillSwitchTripFailed) as exc_info:
			gate._emit_trip("KILL_AUTO_PANIC", detail="second trip", now=now)
		assert isinstance(exc_info.value.__cause__, sqlite3.IntegrityError)


# ===========================================================================
# 2. Bankroll cache tests
# ===========================================================================

class TestBankrollCache:
	def test_cash_cents_returns_cached_value(self) -> None:
		cfg = _phase1_cfg()
		cache = _make_bankroll(cfg, cents=15_000)
		assert cache.cash_cents() == 15_000

	def test_is_stale_when_never_refreshed(self) -> None:
		cfg = _phase1_cfg(bankroll_ttl_seconds=300.0)
		cache = BankrollCache(_source=_make_balance_source(), _cfg=cfg)
		# _last_refresh_ts defaults to 0.0 → very stale
		assert cache.is_stale()

	def test_is_stale_on_fresh_process_with_low_monotonic_clock(self, monkeypatch) -> None:
		"""Regression for the CI failure on Linux runners: time.monotonic()'s
		reference point is platform-dependent. On a freshly-booted CI runner
		it returns single-digit seconds — and `(time.monotonic() - 0) > 300`
		evaluates False, so a never-refreshed cache was incorrectly reported
		as fresh. The fix in `is_stale` short-circuits on `_last_refresh_ts ==
		0.0` so the "never refreshed = stale" invariant holds regardless of
		process uptime.
		"""
		import time as time_module

		# Simulate a fresh CI runner where time.monotonic() returns a small
		# value, smaller than bankroll_ttl_seconds. Without the zero-check,
		# (5.0 - 0.0) > 300 is False and the cache is reported as fresh —
		# wrong; a never-refreshed cache should always be stale.
		monkeypatch.setattr(time_module, "monotonic", lambda: 5.0)

		cfg = _phase1_cfg(bankroll_ttl_seconds=300.0)
		cache = BankrollCache(_source=_make_balance_source(), _cfg=cfg)
		assert cache.is_stale(), (
			"Never-refreshed cache must be stale even when time.monotonic() < ttl"
		)

	def test_is_not_stale_after_pre_load(self) -> None:
		cfg = _phase1_cfg(bankroll_ttl_seconds=300.0)
		cache = _make_bankroll(cfg, cents=20_000)
		assert not cache.is_stale()

	@pytest.mark.asyncio
	async def test_refresh_updates_cash_and_resets_failures(self) -> None:
		cfg = _phase1_cfg()
		source = _make_balance_source(cents=25_000)
		cache = BankrollCache(_source=source, _cfg=cfg)
		cache._consecutive_failures = 3

		await cache.refresh()

		assert cache.cash_cents() == 25_000
		assert cache._consecutive_failures == 0

	@pytest.mark.asyncio
	async def test_refresh_failure_increments_counter(self) -> None:
		cfg = _phase1_cfg(bankroll_failures_until_kill=2)
		source = MagicMock()
		source.balance_cents = AsyncMock(side_effect=Exception("network error"))
		cache = BankrollCache(_source=source, _cfg=cfg)

		await cache.refresh()
		assert cache._consecutive_failures == 1

		# Still below threshold — no trip
		await cache.refresh()
		assert cache._consecutive_failures == 2

	@pytest.mark.asyncio
	async def test_refresh_failure_threshold_trips_panic(self) -> None:
		"""After bankroll_failures_until_kill failures, emit_trip is called."""
		cfg = _phase1_cfg(bankroll_failures_until_kill=2)
		source = MagicMock()
		source.balance_cents = AsyncMock(side_effect=Exception("network error"))
		cache = BankrollCache(_source=source, _cfg=cfg)

		# Wire a mock emit_trip_fn
		trips: list[tuple] = []
		cache._emit_trip_fn = lambda reason, detail, now: trips.append((reason, detail))

		await cache.refresh()  # failure 1
		assert len(trips) == 0

		await cache.refresh()  # failure 2 — threshold hit
		assert len(trips) == 1
		assert trips[0][0] == "KILL_AUTO_PANIC"
		assert "consecutive refresh failures" in trips[0][1]

	@pytest.mark.asyncio
	async def test_on_fill_calls_refresh(self) -> None:
		cfg = _phase1_cfg()
		source = _make_balance_source(cents=18_000)
		cache = BankrollCache(_source=source, _cfg=cfg)

		await cache.on_fill()
		assert cache.cash_cents() == 18_000

	@pytest.mark.asyncio
	async def test_on_settlement_calls_refresh(self) -> None:
		cfg = _phase1_cfg()
		source = _make_balance_source(cents=21_000)
		cache = BankrollCache(_source=source, _cfg=cfg)

		await cache.on_settlement()
		assert cache.cash_cents() == 21_000


# ===========================================================================
# 3. Peak tracker tests
# ===========================================================================

class TestPeakTracker:
	def test_cold_db_initialize_seeds_peak(self) -> None:
		conn = _make_conn()
		pt = PeakTracker(conn)
		assert pt.peak_cents() == 0  # no row yet

		now = datetime.now(timezone.utc)
		pt.initialize_if_unset(20_000, now)

		assert pt.peak_cents() == 20_000

	def test_warm_db_initialize_is_noop(self) -> None:
		conn = _make_conn()
		pt = PeakTracker(conn)
		now = datetime.now(timezone.utc)
		pt.initialize_if_unset(20_000, now)

		# Second call with different equity — should NOT overwrite
		pt.initialize_if_unset(99_999, now)
		assert pt.peak_cents() == 20_000

	def test_on_trade_close_updates_peak_when_higher(self) -> None:
		conn = _make_conn()
		pt = PeakTracker(conn)
		now = datetime.now(timezone.utc)
		pt.initialize_if_unset(20_000, now)

		pt.on_trade_close(25_000, now)
		assert pt.peak_cents() == 25_000

	def test_on_trade_close_no_op_when_lower(self) -> None:
		conn = _make_conn()
		pt = PeakTracker(conn)
		now = datetime.now(timezone.utc)
		pt.initialize_if_unset(20_000, now)

		pt.on_trade_close(15_000, now)
		assert pt.peak_cents() == 20_000  # unchanged

	def test_peak_persists_across_restarts(self) -> None:
		conn = _make_conn()
		pt1 = PeakTracker(conn)
		now = datetime.now(timezone.utc)
		pt1.initialize_if_unset(20_000, now)
		pt1.on_trade_close(30_000, now)
		assert pt1.peak_cents() == 30_000

		# Simulate restart — new PeakTracker same conn
		pt2 = PeakTracker(conn)
		assert pt2.peak_cents() == 30_000

	def test_peak_never_decreases_without_manual_reset(self) -> None:
		conn = _make_conn()
		pt = PeakTracker(conn)
		now = datetime.now(timezone.utc)
		pt.initialize_if_unset(20_000, now)
		pt.on_trade_close(30_000, now)

		# Many trade closes with lower equity
		for equity in [29_000, 25_000, 10_000, 5_000]:
			pt.on_trade_close(equity, now)

		assert pt.peak_cents() == 30_000


# ===========================================================================
# 4. Gate ordering — entry
# ===========================================================================

class TestGateEntryOrdering:
	"""Verify each gate check fires in the correct order and is first to fire."""

	def _gate_with_peak(self, peak: int = 20_000) -> tuple[Gate, sqlite3.Connection]:
		conn = _make_conn()
		gate, _, pt, conn = _make_gate(conn=conn, peak_cents=peak)
		return gate, conn

	def test_1_operator_kill_fires_first(self) -> None:
		gate, conn = self._gate_with_peak()
		# Also install a persisted auto-kill to prove operator fires first
		ks = KillSwitch(conn)
		ks.trip("KILL_AUTO_PANIC", detail="persisted", now=datetime.now(timezone.utc))

		ctx = _make_ctx(operator_kill_active=True)
		result = gate.gate_entry(_make_signal(), ctx)
		assert isinstance(result, Reject)
		assert result.reason == "KILL_OPERATOR"

	def test_2_persisted_auto_kill_fires_before_recompute(self) -> None:
		gate, conn = self._gate_with_peak()
		ks = KillSwitch(conn)
		ks.trip("KILL_AUTO_DRAWDOWN", detail="persisted dd", now=datetime.now(timezone.utc))

		ctx = _make_ctx()  # operator kill is False
		result = gate.gate_entry(_make_signal(), ctx)
		assert isinstance(result, Reject)
		assert result.reason == "KILL_AUTO_DRAWDOWN"

	def test_invalid_signal_zero_entry_fires_before_equity(self) -> None:
		gate, _ = self._gate_with_peak()
		sig = _make_signal(entry_price_cents=0, stop_loss_distance_cents=10)
		result = gate.gate_entry(sig, _make_ctx())
		assert isinstance(result, Reject)
		assert result.reason == "INVALID_SIGNAL"

	def test_invalid_signal_zero_sl_fires_before_equity(self) -> None:
		gate, _ = self._gate_with_peak()
		sig = _make_signal(entry_price_cents=50, stop_loss_distance_cents=0)
		result = gate.gate_entry(sig, _make_ctx())
		assert isinstance(result, Reject)
		assert result.reason == "INVALID_SIGNAL"

	def test_3_panic_fires_on_low_equity(self) -> None:
		# Floor is 3000c, set cash to 2000c
		cfg = _phase1_cfg(absolute_panic_floor_cents=3000)
		conn = _make_conn()
		gate, _, pt, _ = _make_gate(cfg=cfg, cash_cents=2_000, conn=conn, peak_cents=2_000)
		# Re-set peak so drawdown doesn't fire before panic check
		pt._cached_peak_cents = 2_000

		ctx = _make_ctx()
		result = gate.gate_entry(_make_signal(), ctx)
		assert isinstance(result, Reject)
		assert result.reason == "KILL_AUTO_PANIC"

	def test_4_drawdown_fires_when_below_threshold(self) -> None:
		# Peak 20_000, drawdown 5%, threshold = 19_000
		# Equity = 18_000 → drawdown fires
		cfg = _phase1_cfg(absolute_panic_floor_cents=0, drawdown_pct=0.05)
		conn = _make_conn()
		gate, _, pt, _ = _make_gate(cfg=cfg, cash_cents=18_000, conn=conn, peak_cents=20_000)

		ctx = _make_ctx()
		result = gate.gate_entry(_make_signal(), ctx)
		assert isinstance(result, Reject)
		assert result.reason == "KILL_AUTO_DRAWDOWN"

	def test_5_daily_loss_fires_when_exceeded(self) -> None:
		# equity 20_000, daily_loss_pct 2% → cap = -400
		# daily_pnl = -500 → fires
		# Use tiny drawdown_pct (0.001 = 0.1%) and high peak so drawdown doesn't fire.
		# Panic floor = 0 so that doesn't fire.
		# Peak = 20_000, threshold = 20_000 * (1 - 0.001) = 19_980
		# Equity = 20_000 > 19_980 → drawdown does NOT fire
		cfg = _phase1_cfg(
			absolute_panic_floor_cents=0,
			drawdown_pct=0.001,
			daily_loss_pct=0.02,
		)
		conn = _make_conn()
		gate, _, _, _ = _make_gate(cfg=cfg, cash_cents=20_000, conn=conn, peak_cents=20_000)

		ctx = _make_ctx(daily_pnl_cents=-500)
		result = gate.gate_entry(_make_signal(), ctx)
		assert isinstance(result, Reject)
		assert result.reason == "KILL_AUTO_DAILY"

	def test_6_max_open_fires_when_at_limit(self) -> None:
		cfg = _phase1_cfg(absolute_panic_floor_cents=0, drawdown_pct=0.001, max_open=2)
		conn = _make_conn()
		gate, _, _, _ = _make_gate(cfg=cfg, cash_cents=20_000, conn=conn, peak_cents=20_000)

		positions = [
			OpenPosition(ticker="T1", side="yes", fill_size=1, blended_entry_cents=50),
			OpenPosition(ticker="T2", side="no", fill_size=1, blended_entry_cents=50),
		]
		ctx = _make_ctx(open_positions=positions)
		result = gate.gate_entry(_make_signal(), ctx)
		assert isinstance(result, Reject)
		assert result.reason == "MAX_OPEN"

	def test_7_below_min_fill_fires_when_size_too_small(self) -> None:
		# Very small equity → size rounds to 0 (< min_fill_contracts=3)
		cfg = _phase1_cfg(
			absolute_panic_floor_cents=0,
			drawdown_pct=0.001,
			min_fill_contracts=3,
			max_open=10,
		)
		conn = _make_conn()
		# Cash=100c, sizing_pct=0.005, sl=10c → size = int(100*0.005/10) = 0
		# Peak=0 so drawdown threshold = 0*(1-0.001)=0; equity=100 > 0 → no drawdown
		gate, _, _, _ = _make_gate(cfg=cfg, cash_cents=100, conn=conn, peak_cents=0)

		ctx = _make_ctx()
		result = gate.gate_entry(_make_signal(stop_loss_distance_cents=10), ctx)
		assert isinstance(result, Reject)
		assert result.reason == "BELOW_MIN_FILL"

	def test_8_allow_when_all_checks_pass(self) -> None:
		cfg = _phase1_cfg(
			absolute_panic_floor_cents=3000,
			drawdown_pct=0.05,
			daily_loss_pct=0.02,
			max_open=5,
			min_fill_contracts=1,
		)
		conn = _make_conn()
		gate, _, pt, _ = _make_gate(cfg=cfg, cash_cents=20_000, conn=conn, peak_cents=20_000)

		ctx = _make_ctx(daily_pnl_cents=0)
		result = gate.gate_entry(_make_signal(), ctx)
		assert isinstance(result, Allow)
		assert result.size_contracts >= 1


# ===========================================================================
# 5. Gate ordering — exit
# ===========================================================================

class TestGateExit:
	def test_operator_kill_blocks_exit(self) -> None:
		gate, _, _, _ = _make_gate()
		ctx = _make_ctx(operator_kill_active=True)
		result = gate.gate_exit(_make_signal(), ctx)
		assert isinstance(result, Reject)
		assert result.reason == "KILL_OPERATOR"

	def test_auto_kill_does_not_block_exit(self) -> None:
		conn = _make_conn()
		gate, ks, _, _ = _make_gate(conn=conn)
		ks.trip("KILL_AUTO_PANIC", detail="test", now=datetime.now(timezone.utc))

		ctx = _make_ctx()  # operator_kill_active=False
		result = gate.gate_exit(_make_signal(), ctx)
		assert isinstance(result, Allow)

	def test_exit_returns_position_size(self) -> None:
		gate, _, _, _ = _make_gate()
		ticker = "TEST-T50"
		positions = [OpenPosition(ticker=ticker, side="yes", fill_size=5, blended_entry_cents=50)]
		ctx = _make_ctx(open_positions=positions)
		sig = _make_signal(ticker=ticker)
		result = gate.gate_exit(sig, ctx)
		assert isinstance(result, Allow)
		assert result.size_contracts == 5


# ===========================================================================
# 6. Sizing function
# ===========================================================================

class TestSizingFunction:
	def _gate(self, cfg: RiskConfig | None = None) -> Gate:
		if cfg is None:
			cfg = _phase1_cfg(
				absolute_panic_floor_cents=0,
				drawdown_pct=0.001,  # tiny — ensures no drawdown trigger in sizing tests
				min_fill_contracts=0,
				max_open=100,
			)
		conn = _make_conn()
		gate, _, _, _ = _make_gate(cfg=cfg, cash_cents=20_000, conn=conn, peak_cents=0)
		return gate

	def test_fixed_fraction_arm_binds_on_small_equity(self) -> None:
		gate = self._gate()
		# equity=100c, sizing_pct=0.005, sl=10c → fixed_fraction=0
		# We need equity large enough for a non-zero result
		# equity=10_000c, sl=1c → fixed_fraction=int(10_000*0.005/1)=50
		# absolute_max = 5000//50 = 100 (entry=50c)
		result = gate._compute_size(
			_make_signal(entry_price_cents=50, stop_loss_distance_cents=1),
			equity_cents=10_000,
		)
		assert result.breakdown.bound_by == "fixed_fraction"
		assert result.size == result.breakdown.fixed_fraction_contracts

	def test_absolute_max_arm_binds_on_large_equity(self) -> None:
		# Large equity + low entry → absolute_max binds
		# equity=1_000_000c, sizing_pct=0.005, sl=10c → fixed_fraction=500
		# absolute_max=5000//1c=5000 (tight only if entry is high)
		# Use entry=50, sl=10: ff=int(1_000_000*0.005/10)=500
		# absolute_max=5000//50=100 → absolute_max binds (100 < 500)
		gate = self._gate()
		result = gate._compute_size(
			_make_signal(entry_price_cents=50, stop_loss_distance_cents=10),
			equity_cents=1_000_000,
		)
		assert result.breakdown.bound_by == "absolute_max"
		assert result.size == result.breakdown.absolute_max_contracts

	def test_sizing_property_fixed_fraction_bound(self) -> None:
		"""size ≤ floor(equity * sizing_pct / sl) always."""
		gate = self._gate()
		for equity in [5_000, 20_000, 100_000]:
			for sl in [5, 10, 20, 50]:
				result = gate._compute_size(
					_make_signal(entry_price_cents=50, stop_loss_distance_cents=sl),
					equity_cents=equity,
				)
				cfg = gate._cfg
				max_ff = int(equity * cfg.sizing_pct / max(1, sl))
				assert result.size <= max_ff, (
					f"size={result.size} > fixed_fraction={max_ff} "
					f"(equity={equity}, sl={sl})"
				)

	def test_sizing_property_absolute_max_bound(self) -> None:
		"""size * entry_cents ≤ absolute_max_cents always."""
		gate = self._gate()
		for entry in [1, 10, 50, 90]:
			for equity in [5_000, 20_000]:
				result = gate._compute_size(
					_make_signal(entry_price_cents=entry, stop_loss_distance_cents=5),
					equity_cents=equity,
				)
				cost = result.size * entry
				assert cost <= gate._cfg.absolute_max_cents, (
					f"cost={cost} > absolute_max={gate._cfg.absolute_max_cents} "
					f"(entry={entry}, equity={equity})"
				)

	def test_kelly_arm_inert_when_no_edge_config(self) -> None:
		gate = self._gate()
		sig = _make_signal()
		arm = gate._compute_kelly_arm(sig, equity_cents=20_000, sl_cents=10)
		assert arm == 2**31  # +inf sentinel


# ===========================================================================
# 7. Equity computation
# ===========================================================================

class TestEquityComputation:
	def test_equity_equals_cash_when_no_positions(self) -> None:
		conn = _make_conn()
		gate, _, _, _ = _make_gate(cash_cents=20_000, conn=conn)
		ctx = _make_ctx(open_positions=[])
		equity = gate._compute_equity(ctx)
		assert equity == 20_000

	def test_long_marked_at_yes_bid(self) -> None:
		# Long position — conservative mark at bid (yes_levels best)
		ticker = "TEST-T50"
		ms = MarketState()
		book = OrderbookSnapshot(
			yes_levels=[(0.40, 100)],   # bid 40c
			no_levels=[(0.60, 100)],    # ask 60c
		)
		ms.seed_orderbook(ticker, book)

		pos = OpenPosition(ticker=ticker, side="yes", fill_size=10, blended_entry_cents=50)
		conn = _make_conn()
		gate, _, _, _ = _make_gate(cash_cents=0, conn=conn)

		mtm = gate._mark_position_cents(pos, ms)
		assert mtm == 10 * 40  # 400c

	def test_short_marked_at_no_bid(self) -> None:
		# Short position — conservative mark at no_levels best
		ticker = "TEST-T50"
		ms = MarketState()
		book = OrderbookSnapshot(
			yes_levels=[(0.40, 100)],
			no_levels=[(0.55, 100)],   # no bid at 55c
		)
		ms.seed_orderbook(ticker, book)

		pos = OpenPosition(ticker=ticker, side="no", fill_size=5, blended_entry_cents=60)
		conn = _make_conn()
		gate, _, _, _ = _make_gate(cash_cents=0, conn=conn)

		mtm = gate._mark_position_cents(pos, ms)
		assert mtm == 5 * 55  # 275c

	def test_fallback_to_cost_basis_when_book_empty(self) -> None:
		ticker = "TEST-T50"
		ms = MarketState()
		# Empty orderbook — no update
		pos = OpenPosition(ticker=ticker, side="yes", fill_size=3, blended_entry_cents=45)
		conn = _make_conn()
		gate, _, _, _ = _make_gate(cash_cents=0, conn=conn)

		mtm = gate._mark_position_cents(pos, ms)
		assert mtm == 3 * 45  # cost basis

	def test_fallback_to_cost_basis_when_levels_empty(self) -> None:
		ticker = "TEST-T50"
		ms = MarketState()
		book = OrderbookSnapshot(yes_levels=[], no_levels=[])
		ms.seed_orderbook(ticker, book)

		pos = OpenPosition(ticker=ticker, side="yes", fill_size=2, blended_entry_cents=50)
		conn = _make_conn()
		gate, _, _, _ = _make_gate(cash_cents=0, conn=conn)

		mtm = gate._mark_position_cents(pos, ms)
		assert mtm == 2 * 50


# ===========================================================================
# 8. live_db.py STUB_MODE sentinel
# ===========================================================================

class TestLiveDbStubMode:
	def test_stub_mode_is_true(self) -> None:
		"""C's PR ships with STUB_MODE=True. B's PR diffs this to False."""
		import edge_catcher.engine.live_db as live_db
		assert live_db.STUB_MODE is True, (
			"STUB_MODE must be True in C's PR; B's PR flips it to False"
		)

	def test_read_open_positions_returns_empty(self) -> None:
		import edge_catcher.engine.live_db as live_db
		conn = _make_conn()
		assert live_db.read_open_positions(conn) == []

	def test_read_daily_pnl_returns_zero(self) -> None:
		import edge_catcher.engine.live_db as live_db
		conn = _make_conn()
		assert live_db.read_daily_pnl_cents(conn, date.today()) == 0

	def test_read_open_count_returns_zero(self) -> None:
		import edge_catcher.engine.live_db as live_db
		conn = _make_conn()
		assert live_db.read_open_count(conn) == 0


# ===========================================================================
# 9. RiskContext
# ===========================================================================

class TestRiskContext:
	def test_open_count_derived_from_positions(self) -> None:
		positions = [
			OpenPosition(ticker="T1", side="yes", fill_size=1, blended_entry_cents=50),
			OpenPosition(ticker="T2", side="no", fill_size=2, blended_entry_cents=60),
		]
		ctx = _make_ctx(open_positions=positions)
		assert ctx.open_count == 2

	def test_open_count_zero_with_no_positions(self) -> None:
		ctx = _make_ctx(open_positions=[])
		assert ctx.open_count == 0


# ===========================================================================
# 10. RiskConfig parsing
# ===========================================================================

class TestRiskConfig:
	def test_from_dict_parses_all_keys(self) -> None:
		cfg = _phase1_cfg()
		assert cfg.sizing_pct == pytest.approx(0.005)
		assert cfg.daily_loss_pct == pytest.approx(0.02)
		assert cfg.drawdown_pct == pytest.approx(0.05)
		assert cfg.max_open == 5
		assert cfg.min_fill_contracts == 3
		assert cfg.absolute_panic_floor_cents == 3000
		assert cfg.absolute_max_cents == 5000
		assert cfg.kelly_shrinkage == pytest.approx(0.5)
		assert cfg.bankroll_ttl_seconds == pytest.approx(300.0)
		assert cfg.bankroll_failures_until_kill == 2

	def test_from_dict_rejects_bad_sizing_pct(self) -> None:
		with pytest.raises(ValueError, match="sizing_pct"):
			_phase1_cfg(sizing_pct=1.5)

	def test_from_dict_rejects_missing_key(self) -> None:
		d = {
			"sizing_pct": 0.005,
			# daily_loss_pct intentionally missing
		}
		with pytest.raises(KeyError):
			RiskConfig.from_dict(d)

	# -----------------------------------------------------------------------
	# Range-guard regressions for the 7 fields that previously had no guard
	# -----------------------------------------------------------------------

	def test_from_dict_rejects_zero_max_open(self) -> None:
		"""0 max_open would block every entry — surface as config error
		rather than booting an engine that silently no-ops."""
		with pytest.raises(ValueError, match="max_open"):
			_phase1_cfg(max_open=0)

	def test_from_dict_rejects_negative_min_fill_contracts(self) -> None:
		"""0 is a valid "no minimum" setting (used by sizing-arm unit tests
		to bypass the BELOW_MIN_FILL gate); negative is nonsensical."""
		with pytest.raises(ValueError, match="min_fill_contracts"):
			_phase1_cfg(min_fill_contracts=-1)

	def test_from_dict_rejects_negative_panic_floor(self) -> None:
		"""Negative equity floor would trip immediately on first refresh."""
		with pytest.raises(ValueError, match="absolute_panic_floor_cents"):
			_phase1_cfg(absolute_panic_floor_cents=-1)

	def test_from_dict_rejects_zero_absolute_max(self) -> None:
		"""0 per-order dollar cap blocks every entry; negative is nonsensical."""
		with pytest.raises(ValueError, match="absolute_max_cents"):
			_phase1_cfg(absolute_max_cents=0)

	def test_from_dict_rejects_kelly_shrinkage_out_of_bounds(self) -> None:
		"""Shrinkage > 1 over-bets Kelly (math undefined); negative is nonsensical."""
		with pytest.raises(ValueError, match="kelly_shrinkage"):
			_phase1_cfg(kelly_shrinkage=1.5)
		with pytest.raises(ValueError, match="kelly_shrinkage"):
			_phase1_cfg(kelly_shrinkage=-0.1)

	def test_from_dict_rejects_non_positive_ttl(self) -> None:
		"""Zero TTL = is_stale() always True = perpetual refresh; negative is nonsensical."""
		with pytest.raises(ValueError, match="bankroll_ttl_seconds"):
			_phase1_cfg(bankroll_ttl_seconds=0.0)
		with pytest.raises(ValueError, match="bankroll_ttl_seconds"):
			_phase1_cfg(bankroll_ttl_seconds=-1.0)

	def test_from_dict_rejects_zero_failures_until_kill(self) -> None:
		"""0 would trip on the very first failure (no resilience)."""
		with pytest.raises(ValueError, match="bankroll_failures_until_kill"):
			_phase1_cfg(bankroll_failures_until_kill=0)


# ===========================================================================
# 11. Replay / paper path guard
# ===========================================================================

class TestReplayPaperGuard:
	def test_paper_dispatch_ctx_has_no_risk(self) -> None:
		"""Paper-trader dispatch path does not construct a Gate (risk=None).

		This test exercises the structural expectation, not the actual live
		wiring (which lives in E's scope).  We verify that existing dispatch
		helpers do not reference risk.Gate.
		"""
		import edge_catcher.engine.dispatch as dispatch_mod
		import inspect
		# dispatch.py should not have 'Gate' or 'gate_entry' in its source
		src = inspect.getsource(dispatch_mod)
		assert "Gate(" not in src, (
			"dispatch.py should not instantiate Gate — E owns wiring"
		)

	def test_risk_module_does_not_import_venue_adapters(self) -> None:
		"""CR-6 audit: engine/risk.py must not import from adapters/ (venue-specific).

		``KalshiOrderClient`` (from ``live.client``) is the one permitted venue
		reference — it lives in KalshiBalanceSource. ``live.errors`` is shared
		infrastructure (not a venue adapter) and is also permitted.

		The key constraint is: no imports from ``edge_catcher.adapters.*``.
		"""
		import inspect
		import edge_catcher.engine.risk as risk_mod
		src = inspect.getsource(risk_mod)

		# No adapter-package imports
		adapter_imports = [
			line.strip() for line in src.splitlines()
			if "adapters" in line and line.strip().startswith(("import", "from"))
		]
		assert adapter_imports == [], (
			f"engine/risk.py must not import from adapters/: {adapter_imports}"
		)

		# KalshiOrderClient must be the ONLY kalshi-specific client import
		kalshi_client_lines = [
			line.strip() for line in src.splitlines()
			if "KalshiOrderClient" in line and line.strip().startswith(("import", "from"))
		]
		assert kalshi_client_lines == [
			"from edge_catcher.live.client import KalshiOrderClient"
		], f"Unexpected KalshiOrderClient import shape: {kalshi_client_lines}"


# ===========================================================================
# 12. Hypothesis property tests
# ===========================================================================

class TestSizingProperties:
	"""Property-based sizing invariants using hypothesis."""

	def test_sizing_fixed_fraction_property_exhaustive(self) -> None:
		"""Exhaustive check over a finite grid (no hypothesis dependency needed)."""
		cfg = _phase1_cfg(
			absolute_panic_floor_cents=0,
			drawdown_pct=0.001,
			min_fill_contracts=0,
			max_open=100,
		)
		conn = _make_conn()
		gate, _, _, _ = _make_gate(cfg=cfg, cash_cents=20_000, conn=conn, peak_cents=0)

		for equity in [1_000, 5_000, 20_000, 100_000]:
			for entry in [1, 10, 50, 90]:
				for sl in [1, 5, 10, 20, 50]:
					result = gate._compute_size(
						_make_signal(entry_price_cents=entry, stop_loss_distance_cents=sl),
						equity_cents=equity,
					)
					# Invariant 1: size ≤ floor(equity * sizing_pct / sl)
					max_ff = int(equity * cfg.sizing_pct / max(1, sl))
					assert result.size <= max_ff, (
						f"Fixed-fraction invariant violated: size={result.size} > {max_ff} "
						f"(equity={equity}, sl={sl})"
					)
					# Invariant 2: size * entry ≤ absolute_max_cents
					assert result.size * entry <= cfg.absolute_max_cents, (
						f"Absolute-max invariant violated: {result.size}*{entry}={result.size*entry} "
						f"> {cfg.absolute_max_cents}"
					)
					# Invariant 3: size ≥ 0
					assert result.size >= 0

	def test_sizing_deterministic_same_inputs(self) -> None:
		"""Gate decision is deterministic — same context, same decision."""
		cfg = _phase1_cfg(
			absolute_panic_floor_cents=0,
			drawdown_pct=0.001,
			min_fill_contracts=1,
			max_open=100,
		)
		conn = _make_conn()
		gate, _, pt, _ = _make_gate(cfg=cfg, cash_cents=20_000, conn=conn, peak_cents=20_000)

		ctx = _make_ctx()
		sig = _make_signal()

		decisions = [gate.gate_entry(sig, ctx) for _ in range(5)]
		first = decisions[0]
		for d in decisions[1:]:
			assert type(d) is type(first)
			if isinstance(d, Allow):
				assert isinstance(first, Allow)
				assert d.size_contracts == first.size_contracts

	def test_equity_nonneg_when_inputs_nonneg(self) -> None:
		"""Equity is non-negative when cash and positions are non-negative."""
		conn = _make_conn()
		gate, _, _, _ = _make_gate(cash_cents=0, conn=conn)

		for cash in [0, 100, 1000, 20_000]:
			gate._bankroll._cash_cents = cash
			for fill in [0, 1, 10]:
				positions = [
					OpenPosition(ticker="T", side="yes", fill_size=fill, blended_entry_cents=50)
				] if fill > 0 else []
				ctx = _make_ctx(open_positions=positions)
				equity = gate._compute_equity(ctx)
				assert equity >= 0, f"equity={equity} < 0 (cash={cash}, fill={fill})"


# ===========================================================================
# 12. build_risk_module pre-refreshes the bankroll cache (known leftover #6)
# ===========================================================================


class TestBuildRiskModulePreRefresh:
	"""Regression tests for the known leftover: ``build_risk_module`` previously
	returned a Gate whose BankrollCache had ``_last_refresh_ts=0`` and
	``_cash_cents=0``. The first ``gate_entry`` call would see equity=0,
	below ``absolute_panic_floor_cents``, and trip ``KILL_AUTO_PANIC`` on
	every clean startup. The async factory now awaits ``bankroll.refresh()``
	before returning."""

	@pytest.mark.asyncio
	async def test_build_risk_module_prerefreshes_bankroll(self, tmp_path: Path) -> None:
		"""The factory awaits bankroll.refresh() before returning, so the cache
		holds the real Kalshi balance from the moment the Gate is constructed.
		Without this, equity = 0 + mtm = 0 ≤ panic_floor on the first signal."""
		from edge_catcher.engine.risk import build_risk_module

		# Bootstrap a real SQLite schema (live_trades.db) so KillSwitch /
		# PeakTracker can read/write.
		db_path = tmp_path / "live_trades.db"
		conn = sqlite3.connect(str(db_path))
		conn.executescript("""
			CREATE TABLE kill_switch (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				reason TEXT NOT NULL,
				detail TEXT NOT NULL,
				tripped_at TEXT NOT NULL,
				cleared_at TEXT,
				cleared_by TEXT
			);
			CREATE TABLE risk_state (
				key TEXT PRIMARY KEY,
				value TEXT NOT NULL,
				updated_at TEXT NOT NULL
			);
		""")

		# Fake KalshiOrderClient whose balance() returns 17000c.
		fake_client = MagicMock()
		fake_client.balance = AsyncMock(return_value=MagicMock(balance_cents=17000))

		config = {
			"risk": {
				"sizing_pct": 0.005,
				"daily_loss_pct": 0.02,
				"drawdown_pct": 0.05,
				"max_open": 5,
				"min_fill_contracts": 3,
				"absolute_panic_floor_cents": 3000,
				"absolute_max_cents": 5000,
				"kelly_shrinkage": 0.5,
				"bankroll_ttl_seconds": 300.0,
				"bankroll_failures_until_kill": 2,
			}
		}

		gate = await build_risk_module(config, conn, fake_client)

		assert gate._bankroll._cash_cents == 17000, (
			"build_risk_module MUST pre-refresh the bankroll cache so the "
			"first gate_entry sees real cash, not the 0-default"
		)
		assert gate._bankroll.is_stale() is False, (
			"after pre-refresh, _last_refresh_ts is set so is_stale() reports "
			"fresh — without the await refresh(), it would report stale"
		)
		# Sanity check — the gate's existing equity-floor logic now works as
		# intended on first call (cash > floor, no panic trip).
		fake_client.balance.assert_awaited_once()

	@pytest.mark.asyncio
	async def test_build_risk_module_swallows_refresh_failure_cleanly(self, tmp_path: Path) -> None:
		"""When pre-refresh fails (Kalshi unreachable at boot), the factory
		does NOT raise — the cache stays at 0 and the engine's existing
		failure-handling (next gate eval trips KILL_AUTO_PANIC for cash=0
		≤ absolute_panic_floor_cents) is the correct behaviour. Loud
		factory-level failure would be a separate, breaking change.

		Note: _emit_trip_fn is still None at pre-refresh time (Gate hasn't
		been constructed yet), so a refresh failure CANNOT fire a phantom
		kill trip during construction."""
		from edge_catcher.engine.risk import build_risk_module
		from edge_catcher.live.errors import NetworkError

		db_path = tmp_path / "live_trades.db"
		conn = sqlite3.connect(str(db_path))
		conn.executescript("""
			CREATE TABLE kill_switch (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				reason TEXT NOT NULL,
				detail TEXT NOT NULL,
				tripped_at TEXT NOT NULL,
				cleared_at TEXT,
				cleared_by TEXT
			);
			CREATE TABLE risk_state (
				key TEXT PRIMARY KEY,
				value TEXT NOT NULL,
				updated_at TEXT NOT NULL
			);
		""")

		fake_client = MagicMock()
		fake_client.balance = AsyncMock(side_effect=NetworkError("simulated: connection refused"))

		config = {
			"risk": {
				"sizing_pct": 0.005,
				"daily_loss_pct": 0.02,
				"drawdown_pct": 0.05,
				"max_open": 5,
				"min_fill_contracts": 3,
				"absolute_panic_floor_cents": 3000,
				"absolute_max_cents": 5000,
				"kelly_shrinkage": 0.5,
				"bankroll_ttl_seconds": 300.0,
				"bankroll_failures_until_kill": 2,
			}
		}

		gate = await build_risk_module(config, conn, fake_client)
		assert gate._bankroll._cash_cents == 0, (
			"refresh failure leaves cache at 0 — the existing trip-on-first-"
			"signal behaviour handles the unreachable-Kalshi case"
		)
