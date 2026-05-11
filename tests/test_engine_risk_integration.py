"""Integration tests for engine/risk.py.

Tests the Gate, BankrollCache, KillSwitch, and PeakTracker interacting with
a real in-memory SQLite DB via real migration DDL.

Per C-spec L750-L756:
  1. End-to-end gate flow (mocked BalanceSource, real Gate)
  2. Kill-switch persistence across restart
  3. Daily auto-clear across midnight
  4. Replay does NOT invoke gate (executor_kind guard)
  5. Paper trader does NOT construct Gate (risk=None in dispatch ctx)

Running::

    pytest tests/test_engine_risk_integration.py -v
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from edge_catcher.engine.executor import OpenPosition
from edge_catcher.engine.market_state import MarketState
from edge_catcher.engine.risk import (
	Allow,
	BankrollCache,
	Gate,
	KillSwitch,
	PeakTracker,
	Reject,
	RiskConfig,
	RiskContext,
	RiskEvent,
)
from edge_catcher.engine.strategy_base import Signal


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

_MIGRATIONS_DIR = Path(__file__).parent.parent / "edge_catcher" / "storage" / "migrations"


def _make_conn() -> sqlite3.Connection:
	from edge_catcher.storage.migrations import apply_migrations
	conn = sqlite3.connect(":memory:")
	apply_migrations(conn, _MIGRATIONS_DIR)
	return conn


def _phase1_cfg(**overrides: Any) -> RiskConfig:
	defaults: dict[str, Any] = {
		"sizing_pct": 0.005,
		"daily_loss_pct": 0.02,
		"drawdown_pct": 0.05,
		"max_open": 5,
		"min_fill_contracts": 1,
		"absolute_panic_floor_cents": 3000,
		"absolute_max_cents": 5000,
		"kelly_shrinkage": 0.5,
		"bankroll_ttl_seconds": 300.0,
		"bankroll_failures_until_kill": 2,
	}
	defaults.update(overrides)
	return RiskConfig.from_dict(defaults)


def _make_mocked_balance_source(cents: int = 20_000) -> Any:
	source = MagicMock()
	source.balance_cents = AsyncMock(return_value=cents)
	return source


def _make_gate_full(
	conn: sqlite3.Connection,
	cash_cents: int = 20_000,
	peak_cents: int = 20_000,
	cfg: RiskConfig | None = None,
) -> Gate:
	if cfg is None:
		cfg = _phase1_cfg()
	bankroll = BankrollCache(
		_source=_make_mocked_balance_source(cash_cents),
		_cfg=cfg,
	)
	bankroll._cash_cents = cash_cents
	bankroll._last_refresh_ts = 1e12  # never stale

	kill_switch = KillSwitch(conn=conn)
	peak_tracker = PeakTracker(conn=conn)
	now = datetime.now(timezone.utc)
	peak_tracker.initialize_if_unset(peak_cents, now)

	return Gate(cfg=cfg, bankroll=bankroll, kill_switch=kill_switch, peak_tracker=peak_tracker)


def _make_ctx(
	*,
	now: datetime | None = None,
	open_positions: list[OpenPosition] | None = None,
	daily_pnl_cents: int = 0,
	operator_kill_active: bool = False,
) -> RiskContext:
	return RiskContext(
		now_utc=now or datetime.now(timezone.utc),
		market_state=MarketState(),
		open_positions=open_positions or [],
		daily_pnl_cents=daily_pnl_cents,
		operator_kill_active=operator_kill_active,
	)


def _make_signal(
	entry_price_cents: int = 50,
	stop_loss_distance_cents: int = 10,
) -> Signal:
	return Signal(
		action="enter",
		ticker="TEST-T50",
		side="yes",
		series="TEST",
		strategy="strat_34",
		reason="unit_test",
		entry_price_cents=entry_price_cents,
		stop_loss_distance_cents=stop_loss_distance_cents,
	)


# ===========================================================================
# Integration test 1: end-to-end gate flow
# ===========================================================================

class TestEndToEndGateFlow:
	def test_allow_on_clean_state(self) -> None:
		"""Real Gate + real DB: signal should Allow when all checks pass."""
		conn = _make_conn()
		gate = _make_gate_full(conn, cash_cents=20_000, peak_cents=20_000)
		ctx = _make_ctx()
		sig = _make_signal()

		result = gate.gate_entry(sig, ctx)
		assert isinstance(result, Allow)
		assert result.size_contracts >= 1

	def test_correct_size_calculation(self) -> None:
		"""Verify size formula end-to-end.

		equity=20_000c, sizing_pct=0.5%, sl=10c → fixed_fraction=10
		absolute_max=5000//50=100 → fixed_fraction binds at 10
		"""
		conn = _make_conn()
		cfg = _phase1_cfg(min_fill_contracts=1)
		gate = _make_gate_full(conn, cash_cents=20_000, peak_cents=20_000, cfg=cfg)
		ctx = _make_ctx()
		sig = _make_signal(entry_price_cents=50, stop_loss_distance_cents=10)

		result = gate.gate_entry(sig, ctx)
		assert isinstance(result, Allow)
		# int(20_000 * 0.005 / 10) = int(10.0) = 10
		assert result.size_contracts == 10
		assert result.sizing_breakdown.bound_by == "fixed_fraction"

	def test_reject_on_invalid_signal(self) -> None:
		conn = _make_conn()
		gate = _make_gate_full(conn)
		ctx = _make_ctx()
		sig = _make_signal(entry_price_cents=0, stop_loss_distance_cents=10)

		result = gate.gate_entry(sig, ctx)
		assert isinstance(result, Reject)
		assert result.reason == "INVALID_SIGNAL"

	def test_multi_trade_equity_tracking(self) -> None:
		"""Multiple positions affect equity computation and sizing."""
		conn = _make_conn()
		cfg = _phase1_cfg(min_fill_contracts=1, max_open=10)
		gate = _make_gate_full(conn, cash_cents=10_000, peak_cents=10_000, cfg=cfg)

		# Two open positions worth 5000c total (MTM at cost basis — no book)
		pos1 = OpenPosition(ticker="T1", side="yes", fill_size=50, blended_entry_cents=50)
		pos2 = OpenPosition(ticker="T2", side="no", fill_size=50, blended_entry_cents=50)

		ctx = _make_ctx(open_positions=[pos1, pos2])
		equity = gate._compute_equity(ctx)
		# cash=10_000 + pos1 MTM + pos2 MTM (fallback to cost basis, no book)
		# pos1: 50 * 50 = 2500c, pos2: 50 * 50 = 2500c
		assert equity == 10_000 + 2_500 + 2_500

	def test_drawdown_trip_path(self) -> None:
		"""Gate trips KILL_AUTO_DRAWDOWN and persists to DB."""
		conn = _make_conn()
		cfg = _phase1_cfg(
			absolute_panic_floor_cents=0,
			drawdown_pct=0.05,
		)
		# Peak = 20_000, threshold = 19_000
		# Equity = 18_000 → drawdown fires
		gate = _make_gate_full(conn, cash_cents=18_000, peak_cents=20_000, cfg=cfg)
		ctx = _make_ctx()
		sig = _make_signal()

		result = gate.gate_entry(sig, ctx)
		assert isinstance(result, Reject)
		assert result.reason == "KILL_AUTO_DRAWDOWN"

		# Verify DB row persisted
		row = conn.execute(
			"SELECT reason, cleared_at FROM kill_switch WHERE cleared_at IS NULL"
		).fetchone()
		assert row is not None
		assert row[0] == "KILL_AUTO_DRAWDOWN"

	def test_risk_event_emitted_on_trip(self) -> None:
		"""Gate emits a RiskEvent to registered callbacks on kill trip."""
		conn = _make_conn()
		cfg = _phase1_cfg(absolute_panic_floor_cents=0, drawdown_pct=0.05)
		gate = _make_gate_full(conn, cash_cents=18_000, peak_cents=20_000, cfg=cfg)

		events: list[RiskEvent] = []
		gate._event_callbacks.append(events.append)

		ctx = _make_ctx()
		gate.gate_entry(_make_signal(), ctx)

		assert len(events) == 1
		assert events[0].kind == "trip"
		assert events[0].reason == "KILL_AUTO_DRAWDOWN"
		assert events[0].severity == "error"


# ===========================================================================
# Integration test 2: kill-switch persistence across restart
# ===========================================================================

class TestKillSwitchPersistenceAcrossRestart:
	def test_trip_persists_across_gate_restart(self) -> None:
		conn = _make_conn()
		# Trip the kill on one Gate instance
		gate1 = _make_gate_full(conn, cash_cents=20_000, peak_cents=20_000)
		now = datetime.now(timezone.utc)
		gate1._kill_switch.trip("KILL_AUTO_PANIC", detail="panic", now=now)

		# New Gate instance from same conn
		gate2 = _make_gate_full(conn, cash_cents=20_000, peak_cents=20_000)
		ctx = _make_ctx()
		result = gate2.gate_entry(_make_signal(), ctx)
		assert isinstance(result, Reject)
		assert result.reason == "KILL_AUTO_PANIC"

	def test_cleared_kill_does_not_persist(self) -> None:
		conn = _make_conn()
		gate1 = _make_gate_full(conn, cash_cents=20_000, peak_cents=20_000)
		now = datetime.now(timezone.utc)
		gate1._kill_switch.trip("KILL_AUTO_PANIC", detail="panic", now=now)

		kill_id = conn.execute("SELECT id FROM kill_switch").fetchone()[0]
		gate1._kill_switch.clear(kill_id, "human:test", now=now)

		# New Gate from same conn — should allow
		gate2 = _make_gate_full(conn, cash_cents=20_000, peak_cents=20_000)
		ctx = _make_ctx()
		result = gate2.gate_entry(_make_signal(), ctx)
		assert isinstance(result, Allow)


# ===========================================================================
# Integration test 3: daily auto-clear across midnight
# ===========================================================================

class TestDailyAutoClears:
	def test_daily_kill_auto_clears_at_utc_midnight(self) -> None:
		conn = _make_conn()
		gate = _make_gate_full(conn, cash_cents=20_000, peak_cents=20_000)

		# Trip at 23:55 UTC
		trip_time = datetime(2026, 5, 8, 23, 55, 0, tzinfo=timezone.utc)
		gate._kill_switch.trip("KILL_AUTO_DAILY", detail="cap", now=trip_time)

		# Verify gate rejects on same day
		ctx_same = _make_ctx(now=datetime(2026, 5, 8, 23, 58, 0, tzinfo=timezone.utc))
		r1 = gate.gate_entry(_make_signal(), ctx_same)
		assert isinstance(r1, Reject)
		assert r1.reason == "KILL_AUTO_DAILY"

		# Advance to next day — gate should Allow
		ctx_next = _make_ctx(now=datetime(2026, 5, 9, 0, 5, 0, tzinfo=timezone.utc))
		r2 = gate.gate_entry(_make_signal(), ctx_next)
		assert isinstance(r2, Allow)

	def test_panic_does_not_auto_clear_at_midnight(self) -> None:
		conn = _make_conn()
		gate = _make_gate_full(conn, cash_cents=20_000, peak_cents=20_000)

		trip_time = datetime(2026, 5, 8, 10, 0, 0, tzinfo=timezone.utc)
		gate._kill_switch.trip("KILL_AUTO_PANIC", detail="panic", now=trip_time)

		# Next day — still blocked
		ctx_next = _make_ctx(now=datetime(2026, 5, 9, 0, 5, 0, tzinfo=timezone.utc))
		r = gate.gate_entry(_make_signal(), ctx_next)
		assert isinstance(r, Reject)
		assert r.reason == "KILL_AUTO_PANIC"


# ===========================================================================
# Integration test 4: replay does NOT invoke gate
# ===========================================================================

class TestReplayDoesNotInvokeGate:
	def test_replay_does_not_invoke_gate(self) -> None:
		"""Replay path must not construct or call Gate.gate_entry (C-spec L755).

		This test is a structural check: the replay backtester's dispatch path
		does not pass a risk parameter.  We verify by inspecting that dispatch.py
		has no Gate instantiation, and that the replay module doesn't import risk.
		"""
		import inspect
		import edge_catcher.engine.replay.backtester as backtester_mod

		# Replay module should NOT import Gate
		src = inspect.getsource(backtester_mod)
		assert "from edge_catcher.engine.risk import" not in src, (
			"Replay backtester must not import from engine.risk"
		)
		assert "Gate(" not in src, (
			"Replay backtester must not instantiate Gate"
		)

	def test_dispatch_has_no_gate_construction(self) -> None:
		"""dispatch.py should not directly instantiate Gate."""
		import inspect
		import edge_catcher.engine.dispatch as dispatch_mod

		src = inspect.getsource(dispatch_mod)
		# dispatch.py wires risk in from the caller (E), not by constructing it
		assert "Gate(" not in src, (
			"dispatch.py must not construct Gate — E handles wiring"
		)


# ===========================================================================
# Integration test 5: paper trader does NOT construct Gate
# ===========================================================================

class TestPaperPathDoesNotConstructGate:
	def test_paper_executor_has_no_risk_import(self) -> None:
		"""PaperExecutor does not import or reference risk.Gate."""
		import inspect
		import edge_catcher.engine.executors.paper as paper_mod

		src = inspect.getsource(paper_mod)
		assert "from edge_catcher.engine.risk import" not in src, (
			"PaperExecutor must not import from engine.risk"
		)
		assert "Gate" not in src, (
			"PaperExecutor must not reference Gate"
		)

	def test_paper_trade_store_has_no_risk_import(self) -> None:
		"""TradeStore (paper) has no Gate reference."""
		import inspect
		import edge_catcher.engine.trade_store as ts_mod

		src = inspect.getsource(ts_mod)
		assert "risk.Gate" not in src and "from edge_catcher.engine.risk" not in src, (
			"trade_store.py must not import from engine.risk"
		)


# ===========================================================================
# Integration test 6: mocked BalanceSource with Gate
# ===========================================================================

class TestMockedBalanceSourceIntegration:
	@pytest.mark.asyncio
	async def test_bankroll_cache_refreshes_from_source(self) -> None:
		"""BankrollCache.refresh() calls BalanceSource.balance_cents()."""
		source = _make_mocked_balance_source(cents=15_500)
		cfg = _phase1_cfg()
		cache = BankrollCache(_source=source, _cfg=cfg)

		await cache.refresh()

		assert cache.cash_cents() == 15_500
		source.balance_cents.assert_called_once()

	def test_gate_uses_bankroll_cache_for_equity(self) -> None:
		"""Gate._compute_equity reads from the bankroll cache, not the source."""
		conn = _make_conn()
		cfg = _phase1_cfg()
		source = _make_mocked_balance_source(cents=20_000)
		bankroll = BankrollCache(_source=source, _cfg=cfg)
		bankroll._cash_cents = 12_500  # Override directly
		bankroll._last_refresh_ts = 1e12

		kill_switch = KillSwitch(conn=conn)
		peak_tracker = PeakTracker(conn=conn)
		peak_tracker.initialize_if_unset(12_500, datetime.now(timezone.utc))

		gate = Gate(cfg=cfg, bankroll=bankroll, kill_switch=kill_switch, peak_tracker=peak_tracker)
		ctx = _make_ctx()
		equity = gate._compute_equity(ctx)

		assert equity == 12_500
		# Source was NOT called (gate reads cached value, not async source)
		source.balance_cents.assert_not_called()
