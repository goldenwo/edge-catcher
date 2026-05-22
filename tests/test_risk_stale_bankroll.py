import sqlite3
import typing
from datetime import datetime, timezone

import pytest

from edge_catcher.engine.metrics import Metrics, _GATE_REJECT_COUNTER
from edge_catcher.engine.risk import (
	BankrollCache,
	Gate,
	GateRejectReason,
	KillSwitch,
	PeakTracker,
	Reject,
	RiskConfig,
	RiskContext,
	Signal,
)


def test_stale_bankroll_is_registered_reason_and_counter():
	# In the GateRejectReason Literal:
	assert "STALE_BANKROLL" in typing.get_args(GateRejectReason)
	# Mapped to a counter key:
	assert _GATE_REJECT_COUNTER["STALE_BANKROLL"] == "risk_gate_rejected_stale_bankroll"
	# And the key is registered (Metrics.inc raises KeyError on unknown keys):
	m = Metrics()
	m.inc("risk_gate_rejected_stale_bankroll")  # must NOT raise


def test_riskcontext_open_count_is_independent_field():
	from datetime import datetime, timezone
	from edge_catcher.engine.risk import RiskContext
	# open_count is a field that can differ from len(open_positions):
	ctx = RiskContext(
		now_utc=datetime(2026, 5, 22, tzinfo=timezone.utc),
		market_state=None,
		open_positions=[],          # zero status='open' rows
		open_count=3,               # but 3 pending rows hold MAX_OPEN slots
		daily_pnl_cents=0,
		operator_kill_active=False,
	)
	assert ctx.open_count == 3
	assert len(ctx.open_positions) == 0


# ===========================================================================
# A3. Gate-time STALE_BANKROLL soft-reject (spec §5.3)
# ===========================================================================

def _apply_schema(conn: sqlite3.Connection) -> None:
	"""Apply migrations so KillSwitch and PeakTracker can read/write."""
	from pathlib import Path  # noqa: PLC0415
	from edge_catcher.storage.migrations import apply_migrations  # noqa: PLC0415
	migrations_dir = Path(__file__).parent.parent / "edge_catcher" / "storage" / "migrations"
	apply_migrations(conn, migrations_dir)


def _make_fresh_gate(*, stale: bool) -> Gate:
	"""Build a Gate whose bankroll cache is fresh or stale on demand.

	``stale=True``  → ``_last_refresh_ts = 0.0`` (never refreshed — is_stale() = True).
	``stale=False`` → ``_last_refresh_ts = 1e12``  (way in the future — always fresh).
	"""
	from unittest.mock import AsyncMock, MagicMock  # noqa: PLC0415

	conn = sqlite3.connect(":memory:")
	_apply_schema(conn)

	cfg_dict = {
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
	cfg = RiskConfig.from_dict(cfg_dict)

	source = MagicMock()
	source.balance_cents = AsyncMock(return_value=20_000)
	bankroll = BankrollCache(_source=source, _cfg=cfg)
	bankroll._cash_cents = 20_000
	bankroll._last_refresh_ts = 0.0 if stale else 1e12  # stale=never-refreshed, fresh=far future

	kill_switch = KillSwitch(conn=conn)
	peak_tracker = PeakTracker(conn=conn)

	return Gate(cfg=cfg, bankroll=bankroll, kill_switch=kill_switch, peak_tracker=peak_tracker)


def _make_valid_signal() -> Signal:
	"""Signal that passes the INVALID_SIGNAL guard (entry=50c, sl=10c)."""
	return Signal(
		action="enter",
		ticker="TEST-T50",
		side="yes",
		series="TEST",
		strategy="test_strategy",
		reason="unit_test",
		entry_price_cents=50,
		stop_loss_distance_cents=10,
	)


def _make_ctx() -> RiskContext:
	"""Minimal RiskContext — no open positions, no kill, passes all pre-staleness checks."""
	return RiskContext(
		now_utc=datetime.now(timezone.utc),
		market_state=None,
		open_positions=[],
		open_count=0,
		daily_pnl_cents=0,
		operator_kill_active=False,
	)


class TestGateEntryStaleBankroll:
	"""A3: gate_entry must soft-reject with STALE_BANKROLL when the bankroll
	cache is too old to trust — before any equity-based computation.
	The fresh path must never produce a STALE_BANKROLL reject.
	"""

	def test_gate_entry_soft_rejects_when_bankroll_stale(self) -> None:
		"""Stale bankroll (never refreshed) → Reject("STALE_BANKROLL") before equity."""
		gate = _make_fresh_gate(stale=True)
		sig = _make_valid_signal()
		ctx = _make_ctx()

		decision = gate.gate_entry(sig, ctx)

		assert isinstance(decision, Reject), f"expected Reject, got {decision!r}"
		assert decision.reason == "STALE_BANKROLL"

	def test_gate_entry_proceeds_when_bankroll_fresh(self) -> None:
		"""Fresh bankroll → gate_entry must NOT return STALE_BANKROLL."""
		gate = _make_fresh_gate(stale=False)
		sig = _make_valid_signal()
		ctx = _make_ctx()

		decision = gate.gate_entry(sig, ctx)

		assert getattr(decision, "reason", None) != "STALE_BANKROLL", (
			f"fresh bankroll should not produce STALE_BANKROLL; got {decision!r}"
		)
