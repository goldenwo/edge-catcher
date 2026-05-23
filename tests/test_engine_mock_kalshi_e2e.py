"""H1 — mock-Kalshi sizing-wire end-to-end scenario tests.

Drives the live sizing path through ``_handle_enter`` (with a real
``allowed_size``, a real ``SQLiteTradeStore``, and a real ``Gate`` that
ALLOWs + sizes the entry) against ``MockKalshiServer`` for four canonical
outcomes:

  1. Full fill   → pending row CAS-transitions to ``open``; persisted
                   fill_size and blended_entry match the fill.
  2. Partial fill → persisted fill_size == FILLED quantity (not intended size).
  3. Exchange reject (4xx) → pending row CAS-transitions to ``rejected``
                   (non-fatal; engine must not crash).
  4. Timeout      → a ``pending`` row is synthesized carrying the REAL
                   intended_size; deterministic (no flaky wall-clock races).

Design choice: all four tests drive ``_handle_enter`` directly (not
``_handle_signal``) with ``allowed_size=N>0``.  This is the correct seam
for H1:

* ``_handle_enter`` IS the function wired by the sizing-wire PR — it
  now receives ``allowed_size`` from the gate decision and calls
  ``build_entry_order`` (live path) → ``LiveExecutor.place`` → CAS.
* Driving the full ``_handle_signal`` / ``dispatch_message`` path would
  require a complete ``TickContext`` (8+ fields, WS orderbook state) just
  to reach ``_handle_enter``.  That is boilerplate covering code already
  exercised by G1; it would not add coverage of the sizing wire itself.
* The gate wiring (``risk`` / ``risk_ctx_provider``) is already proven
  end-to-end by G1 (the no-op-gate regression guard).  H1's job is to
  prove that a SIZED order actually reaches the executor and that the
  CAS chain (record_intent → place → record_trade / record_pending /
  record_rejected) works correctly with real money amounts.

Gate setup:
  A fresh ``Gate`` with ``_last_refresh_ts = 1e12`` (always fresh) and
  ``bankroll._cash_cents = 20_000`` (Phase-1 caps easily satisfied).  The
  signal uses ``entry_price_cents=50`` + ``stop_loss_distance_cents=10``
  (well within caps) so ``gate_entry`` returns ``Allow(size_contracts > 0)``.

Timeout determinism (spec §8.2 — do NOT mock asyncio.wait_for):
  ``_ENTRY_PLACEMENT_TIMEOUT_SECONDS`` in ``edge_catcher.engine.dispatch``
  is monkeypatched to ``0.05`` s.  ``MockKalshiServer.queue_slow_response``
  arms a server-side ``asyncio.sleep(0.15)`` (3× the ceiling) so the
  timeout fires deterministically before the response arrives.  We gate on
  the SYNTHESIZED PENDING ROW state (a DB query), not on wall-clock elapsed
  time, so the assertion is race-free.
"""
from __future__ import annotations

import asyncio
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

import edge_catcher.engine.dispatch as dispatch_module
from edge_catcher.engine.dispatch import _handle_enter
from edge_catcher.engine.execution import ExecCfg
from edge_catcher.engine.executors.live import LiveExecutor
from edge_catcher.engine.market_state import MarketState, OrderbookSnapshot, TickContext
from edge_catcher.engine.metrics import Metrics
from edge_catcher.engine.risk import (
	BankrollCache,
	Gate,
	KillSwitch,
	PeakTracker,
	RiskConfig,
)
from edge_catcher.engine.strategy_base import Signal
from edge_catcher.live.state import connect_live_trades_db
from edge_catcher.live.store import SQLiteTradeStore

from tests.fixtures.mock_kalshi_server import (
	MockKalshiServer,
	kalshi_201_filled,
	kalshi_201_partial,
	kalshi_400_rejected,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SERIES = "KXSOL15M"
_TICKER = "KXSOL15M-26MAY09H06"
_STRATEGY = "debut-fade"
_NOW = datetime(2026, 5, 23, 1, 0, 0, tzinfo=timezone.utc)

# Phase-1 caps (from the handoff/spec) — gate will Allow within these.
_RISK_CFG_DICT: dict[str, Any] = {
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

# ExecCfg for build_entry_order — minimal slippage, standard exit map.
_EXEC_CFG = ExecCfg(
	entry_slippage_cents=2,
	exit_slippage_cents={"take_profit": 2, "stop_loss": 10, "time_exit": 5},
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _apply_live_schema(conn: sqlite3.Connection) -> None:
	"""Apply migrations so live_trades (and live_schema_migrations) exist."""
	from edge_catcher.storage.migrations import apply_migrations  # noqa: PLC0415
	apply_migrations(conn)


def _make_fresh_gate(conn: sqlite3.Connection) -> Gate:
	"""Build a Gate with a fresh (non-stale) bankroll that ALLOWs entries.

	``_last_refresh_ts = 1e12`` → is_stale() = False (way in the future).
	``bankroll._cash_cents = 20_000`` → $200 bankroll (Phase-1 anchor).
	With sizing_pct=0.005 → 20_000 * 0.005 = 100c → ~2 contracts at 50c.
	All daily / drawdown / max_open checks pass on a clean DB.
	"""
	cfg = RiskConfig.from_dict(_RISK_CFG_DICT)

	source = MagicMock()
	source.balance_cents = AsyncMock(return_value=20_000)
	bankroll = BankrollCache(_source=source, _cfg=cfg)
	bankroll._cash_cents = 20_000
	bankroll._last_refresh_ts = 1e12  # always fresh

	kill_switch = KillSwitch(conn=conn)
	peak_tracker = PeakTracker(conn=conn)

	return Gate(cfg=cfg, bankroll=bankroll, kill_switch=kill_switch, peak_tracker=peak_tracker)


def _make_signal(
	*,
	entry_price_cents: int = 50,
	stop_loss_distance_cents: int = 10,
) -> Signal:
	"""Build a minimal enter Signal within Phase-1 caps."""
	return Signal(
		action="enter",
		ticker=_TICKER,
		side="yes",
		series=_SERIES,
		strategy=_STRATEGY,
		reason="h1-e2e",
		entry_price_cents=entry_price_cents,
		stop_loss_distance_cents=stop_loss_distance_cents,
	)


def _make_tick_ctx(*, yes_ask: int = 50) -> TickContext:
	"""Minimal TickContext — only yes_ask is used by _handle_enter."""
	return TickContext(
		ticker=_TICKER,
		event_ticker="KXSOL15M-26MAY09H06",
		yes_bid=yes_ask - 1,
		yes_ask=yes_ask,
		no_bid=49,
		no_ask=50,
		orderbook=OrderbookSnapshot(yes_levels=[], no_levels=[]),
		price_history=[yes_ask],
		open_positions=[],
		persisted_state={},
		market_metadata={},
		series=_SERIES,
	)


def _make_config(exec_cfg: ExecCfg = _EXEC_CFG) -> dict[str, Any]:
	"""Build the minimal config dict _handle_enter reads."""
	return {
		"executor": "live",
		"_exec_cfg": exec_cfg,
		"_metrics": Metrics(),
	}


def _live_store_and_conn(tmp_path: Path) -> tuple[SQLiteTradeStore, sqlite3.Connection]:
	"""Open a fresh live_trades DB and wrap it in SQLiteTradeStore.

	Returns BOTH the store AND the underlying connection so tests can
	query the DB directly (to assert row status after a CAS transition).
	"""
	db_path = tmp_path / "live_trades.db"
	conn = connect_live_trades_db(db_path)
	store = SQLiteTradeStore(db_path)
	return store, conn


def _row_by_coid(conn: sqlite3.Connection, client_order_id: str) -> dict[str, Any] | None:
	"""Fetch a live_trades row by client_order_id."""
	row = conn.execute(
		"""
		SELECT id, status, intended_size, fill_size, blended_entry,
		       rejection_reason, kalshi_order_id
		FROM live_trades
		WHERE client_order_id = ?
		""",
		(client_order_id,),
	).fetchone()
	if row is None:
		return None
	keys = ["id", "status", "intended_size", "fill_size", "blended_entry",
	        "rejection_reason", "kalshi_order_id"]
	return dict(zip(keys, row))


def _determine_allowed_size(gate: Gate, signal: Signal, conn: sqlite3.Connection) -> int:
	"""Call gate_entry and return the allowed size (asserting it is Allow).

	Builds a minimal RiskContext inline so the test doesn't need to wire the
	full RiskContextProvider (the provider's own DB reads are covered by its
	unit tests; H1 cares only that the sizing path works, not the provider).
	"""
	from edge_catcher.engine.risk import Allow, RiskContext  # noqa: PLC0415

	ctx = RiskContext(
		now_utc=_NOW,
		market_state=MarketState(),
		open_positions=[],
		open_count=0,
		daily_pnl_cents=0,
		operator_kill_active=False,
	)
	decision = gate.gate_entry(signal, ctx)
	assert isinstance(decision, Allow), (
		f"Gate must Allow within Phase-1 caps for H1 gate setup; got {decision!r}"
	)
	assert decision.size_contracts > 0, (
		f"allowed_size must be > 0 (real money path requires a sized order); "
		f"got {decision.size_contracts}"
	)
	return decision.size_contracts


# ---------------------------------------------------------------------------
# Scenario 1 — Full fill → pending→open CAS
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_e2e_full_fill_records_open(
	mock_kalshi_server: MockKalshiServer,
	live_cfg,
	live_audit,
	signing_env,
	tmp_path: Path,
) -> None:
	"""Full fill → pending row CAS-transitions to 'open'; fill_size and
	blended_entry in the DB match the fill returned by the exchange.

	Failure mode prevented: the sizing wire (build_entry_order → place → CAS)
	could silently write the wrong fill_size (e.g. the INTENDED size instead
	of the FILLED size) or a mismatched blended_entry.  This test pins both.
	"""
	store, read_conn = _live_store_and_conn(tmp_path)
	gate_conn = sqlite3.connect(":memory:")
	_apply_live_schema(gate_conn)
	gate = _make_fresh_gate(gate_conn)

	signal = _make_signal()
	allowed_size = _determine_allowed_size(gate, signal, gate_conn)

	# Exchange returns a full fill at 44c (entry_price=50 + 2c slippage = 44c limit;
	# the mock echoes back the limit as the fill price — typical IOC behaviour).
	fill_price = signal.entry_price_cents + _EXEC_CFG.entry_slippage_cents  # 52c
	mock_kalshi_server.queue_response(kalshi_201_filled(
		order_id="ord-h1-full",
		ticker=_TICKER,
		count=allowed_size,
		yes_price=fill_price,
		fills=[{"price": fill_price, "size": allowed_size}],
	))
	client = mock_kalshi_server.make_client(live_cfg, live_audit)
	executor = LiveExecutor(client)

	ctx = _make_tick_ctx(yes_ask=50)
	config = _make_config()

	await _handle_enter(
		signal, ctx, store, config, executor,
		now=_NOW,
		allowed_size=allowed_size,
	)

	# One request reached the mock server.
	assert len(mock_kalshi_server.requests) == 1

	# The pre-place record_intent wrote a pending row; after a full fill the
	# CAS must have transitioned it to 'open'.
	# Find the row by querying all live_trades rows (there is exactly one).
	rows = read_conn.execute(
		"SELECT id, status, intended_size, fill_size, blended_entry_cents FROM live_trades"
	).fetchall()
	assert len(rows) == 1, f"Expected exactly 1 live_trades row; got {rows!r}"
	row_id, status, intended_size, fill_size, blended_entry_cents = rows[0]

	assert status == "open", (
		f"Full fill must CAS pending→open; got status={status!r}"
	)
	assert fill_size == allowed_size, (
		f"Persisted fill_size must equal the filled count ({allowed_size}); "
		f"got {fill_size}"
	)
	assert blended_entry_cents == fill_price, (
		f"Persisted blended_entry_cents must equal the fill price ({fill_price}c); "
		f"got {blended_entry_cents}"
	)
	# intended_size must carry the gate's allowed size (not 0, not inflated).
	assert intended_size == allowed_size, (
		f"intended_size must equal allowed_size ({allowed_size}); got {intended_size}"
	)


# ---------------------------------------------------------------------------
# Scenario 2 — Partial fill → persisted fill_size == FILLED (not intended)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_e2e_partial_fill_size_matches_filled(
	mock_kalshi_server: MockKalshiServer,
	live_cfg,
	live_audit,
	signing_env,
	tmp_path: Path,
) -> None:
	"""Partial fill → persisted fill_size == FILLED quantity (not intended size).

	Failure mode prevented: the CAS update could inadvertently write
	``intended_size`` to the ``fill_size`` column instead of the actual
	``OrderResult.filled_size``.  This test pins the critical distinction.
	"""
	store, read_conn = _live_store_and_conn(tmp_path)
	gate_conn = sqlite3.connect(":memory:")
	_apply_live_schema(gate_conn)
	gate = _make_fresh_gate(gate_conn)

	signal = _make_signal()
	allowed_size = _determine_allowed_size(gate, signal, gate_conn)

	# Partially fill: exchange fills only 1 of the allowed_size contracts.
	filled_count = 1
	assert filled_count < allowed_size, (
		"Test requires allowed_size > 1 so partial < intended is meaningful; "
		f"got allowed_size={allowed_size}.  Adjust Phase-1 caps or signal."
	)
	fill_price = signal.entry_price_cents + _EXEC_CFG.entry_slippage_cents  # 52c
	mock_kalshi_server.queue_response(kalshi_201_partial(
		order_id="ord-h1-partial",
		ticker=_TICKER,
		count=allowed_size,
		filled_count=filled_count,
		fills=[{"price": fill_price, "size": filled_count}],
		limit_price_cents=fill_price,
	))
	client = mock_kalshi_server.make_client(live_cfg, live_audit)
	executor = LiveExecutor(client)

	ctx = _make_tick_ctx(yes_ask=50)
	config = _make_config()

	await _handle_enter(
		signal, ctx, store, config, executor,
		now=_NOW,
		allowed_size=allowed_size,
	)

	assert len(mock_kalshi_server.requests) == 1

	rows = read_conn.execute(
		"SELECT status, intended_size, fill_size FROM live_trades"
	).fetchall()
	assert len(rows) == 1
	status, intended_size, fill_size = rows[0]

	assert status == "open", (
		f"Partial fill must CAS pending→open (Kalshi IOC: any fill > 0 is 'filled'); "
		f"got status={status!r}"
	)
	# THE KEY ASSERTION: fill_size must be the FILLED count, not intended.
	assert fill_size == filled_count, (
		f"Persisted fill_size must equal the FILLED count ({filled_count}), "
		f"NOT the intended/requested size ({allowed_size}); got {fill_size}"
	)
	assert intended_size == allowed_size, (
		f"intended_size must carry the gate's allowed_size ({allowed_size}); "
		f"got {intended_size}"
	)


# ---------------------------------------------------------------------------
# Scenario 3 — Exchange reject (4xx) → CAS pending→rejected; non-fatal
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_e2e_exchange_reject_cas_rejected(
	mock_kalshi_server: MockKalshiServer,
	live_cfg,
	live_audit,
	signing_env,
	tmp_path: Path,
) -> None:
	"""4xx exchange reject → pending row CAS-transitions to 'rejected';
	_handle_enter returns normally (non-fatal — must NOT crash the engine).

	Failure mode prevented: a 4xx from Kalshi could propagate as an uncaught
	exception through _handle_enter, crashing the WS message loop and stopping
	all trading.  The dispatch contract maps 4xx→OrderResult(rejected) and
	calls store.record_rejected; the test pins the non-crash + CAS semantics.
	"""
	store, read_conn = _live_store_and_conn(tmp_path)
	gate_conn = sqlite3.connect(":memory:")
	_apply_live_schema(gate_conn)
	gate = _make_fresh_gate(gate_conn)

	signal = _make_signal()
	allowed_size = _determine_allowed_size(gate, signal, gate_conn)

	status_code, body = kalshi_400_rejected(
		code="invalid_price",
		message="price out of band",
	)
	mock_kalshi_server.queue_status(status_code, body)
	client = mock_kalshi_server.make_client(live_cfg, live_audit)
	executor = LiveExecutor(client)

	ctx = _make_tick_ctx(yes_ask=50)
	config = _make_config()

	# Must NOT raise — a 4xx is non-fatal by design (no position was taken).
	await _handle_enter(
		signal, ctx, store, config, executor,
		now=_NOW,
		allowed_size=allowed_size,
	)

	assert len(mock_kalshi_server.requests) == 1, (
		"4xx must not be retried — only one request must reach the server"
	)

	# The pre-place record_intent wrote a pending row; the rejected branch must
	# CAS it to 'rejected' via store.record_rejected.
	rows = read_conn.execute(
		"SELECT status, intended_size, rejection_reason FROM live_trades"
	).fetchall()
	assert len(rows) == 1, (
		f"Expected exactly 1 live_trades row (record_intent wrote it pre-place, "
		f"record_rejected CAS'd it to rejected); got {rows!r}"
	)
	status, intended_size, rejection_reason = rows[0]

	assert status == "rejected", (
		f"4xx exchange reject must CAS pending→rejected; got status={status!r}"
	)
	assert rejection_reason is not None and "4xx" in rejection_reason, (
		f"rejection_reason must carry the 4xx diagnostic; got {rejection_reason!r}"
	)
	assert intended_size == allowed_size, (
		f"intended_size must carry the gate's allowed_size ({allowed_size}); "
		f"got {intended_size}"
	)


# ---------------------------------------------------------------------------
# Scenario 4 — Timeout → synthesized pending carries REAL intended_size
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_e2e_timeout_synthesizes_pending(
	mock_kalshi_server: MockKalshiServer,
	live_cfg,
	live_audit,
	signing_env,
	tmp_path: Path,
	monkeypatch: pytest.MonkeyPatch,
) -> None:
	"""engine_timeout → a 'pending' row exists carrying the REAL intended_size.

	Failure mode prevented (spec §8.2): before the sizing wire, a timeout
	produced a pending row with intended_size=0 (the paper sentinel) because
	_handle_enter's PAPER path built an OrderRequest with size_contracts=0.
	Post-sizing-wire the LIVE path builds via build_entry_order with the
	real allowed_size, so the synthesized pending row carries the true count
	B's reconciler needs to resolve the order by client_order_id.

	Timeout induction (spec §8.2 — do NOT mock asyncio.wait_for):
	  ``_ENTRY_PLACEMENT_TIMEOUT_SECONDS`` is monkeypatched to 0.05 s.
	  ``MockKalshiServer.queue_slow_response`` arms a 0.15 s server-side
	  ``asyncio.sleep`` (3× the ceiling) so the timeout fires before the
	  response arrives.  We gate on the DB row state (a sync query after
	  await), not on wall-clock elapsed time — deterministic on any machine.
	"""
	store, read_conn = _live_store_and_conn(tmp_path)
	gate_conn = sqlite3.connect(":memory:")
	_apply_live_schema(gate_conn)
	gate = _make_fresh_gate(gate_conn)

	signal = _make_signal()
	allowed_size = _determine_allowed_size(gate, signal, gate_conn)

	# Monkeypatch the module-level timeout constant to 0.05 s (well under the
	# server delay of 0.15 s).  This does NOT mock asyncio.wait_for — the real
	# wait_for fires against the real ceiling; we just shrank the ceiling.
	monkeypatch.setattr(dispatch_module, "_ENTRY_PLACEMENT_TIMEOUT_SECONDS", 0.05)

	# Arm the server with a 0.15 s delay (3× timeout) — the request will hang
	# long enough to trigger the asyncio.TimeoutError in _place_and_persist.
	fill_price = signal.entry_price_cents + _EXEC_CFG.entry_slippage_cents
	mock_kalshi_server.queue_slow_response(
		kalshi_201_filled(
			order_id="ord-h1-timeout",
			ticker=_TICKER,
			count=allowed_size,
			yes_price=fill_price,
		),
		delay_seconds=0.15,
	)
	client = mock_kalshi_server.make_client(live_cfg, live_audit)
	executor = LiveExecutor(client)

	ctx = _make_tick_ctx(yes_ask=50)
	config = _make_config()

	# Must NOT raise — timeout synthesizes pending+None (funds-at-risk handled
	# by B's reconciler, not a fatal crash).
	await asyncio.wait_for(
		_handle_enter(
			signal, ctx, store, config, executor,
			now=_NOW,
			allowed_size=allowed_size,
		),
		timeout=10.0,  # safety net only — the real path completes in <1s
	)

	# Gate: a 'pending' row must exist in the DB (record_intent wrote it
	# pre-place; the timeout branch calls store.record_pending which backfills
	# kalshi_order_id=None — the pre-place row is already pending, so the
	# status stays 'pending').
	rows = read_conn.execute(
		"SELECT status, intended_size, rejection_reason FROM live_trades"
	).fetchall()
	assert len(rows) == 1, (
		f"Expected exactly 1 live_trades row (record_intent pre-place + "
		f"timeout branch leaves it pending); got {rows!r}"
	)
	status, intended_size, rejection_reason = rows[0]

	assert status == "pending", (
		f"Timeout must synthesize/leave a 'pending' row for B's reconciler; "
		f"got status={status!r}"
	)
	# THE KEY POST-SIZING-WIRE ASSERTION: intended_size must be the REAL
	# gate-approved count (not 0, the pre-wire paper sentinel).
	assert intended_size == allowed_size, (
		f"Synthesized pending row must carry the REAL intended_size "
		f"({allowed_size}) so B's reconciler can resolve the true fill via "
		f"client_order_id; got {intended_size} (0 = pre-sizing-wire paper "
		f"sentinel, which would leave B unable to validate the fill amount)"
	)
	# rejection_reason on the DB row: record_intent writes NULL pre-place;
	# the timeout branch calls store.record_pending (C4) which only backfills
	# kalshi_order_id (still None on timeout) — the row's rejection_reason
	# column stays NULL (the engine_timeout diagnostic lives on OrderResult,
	# not on the pending DB row; B's reconciler identifies the row by
	# client_order_id, not by rejection_reason).
	# This is correct behaviour — the row's pending status IS the reconciler's
	# signal; we assert no unexpected terminal status was written instead.
