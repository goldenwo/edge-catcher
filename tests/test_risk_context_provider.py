"""Tests for RiskContextProvider (B1).

Proves:
- open_count comes from read_open_count (pending-inclusive).
- open_positions comes from read_open_positions (open-only).
- The two intentionally DIVERGE when pending rows exist.
- operator_kill_active reflects the _OperatorKill .active flag.
- KILL_SWITCH env var overrides a False .active.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from edge_catcher.engine.market_state import MarketState
from edge_catcher.engine.risk_context_provider import RiskContextProvider, _env_kill_active


# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------

def _apply_schema(conn: sqlite3.Connection) -> None:
	"""Apply all migrations so live_trades (and its siblings) exist."""
	from edge_catcher.storage.migrations import apply_migrations  # noqa: PLC0415
	migrations_dir = Path(__file__).parent.parent / "edge_catcher" / "storage" / "migrations"
	apply_migrations(conn, migrations_dir)


def _insert_row(conn: sqlite3.Connection, *, status: str, client_order_id: str) -> None:
	"""Insert a minimal live_trades row with the given status."""
	conn.execute(
		"""
		INSERT INTO live_trades (
			ticker, series, strategy, side,
			intended_size, original_intended_size,
			entry_price_cents, status,
			client_order_id, placed_at_utc
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		""",
		(
			"KXTEST-25", "KXTEST", "test_strat", "yes",
			10, 10,
			45, status,
			client_order_id, "2026-05-22T00:00:00",
		),
	)
	conn.commit()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def live_conn() -> sqlite3.Connection:
	"""In-memory connection with migrations applied.

	Seeded with: 0 rows status='open', 2 rows status='pending'.
	This is the key divergence fixture — open_count==2, open_positions empty.
	"""
	conn = sqlite3.connect(":memory:")
	_apply_schema(conn)
	_insert_row(conn, status="pending", client_order_id="coid-pending-1")
	_insert_row(conn, status="pending", client_order_id="coid-pending-2")
	return conn


# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------

class _FakeKill:
	"""Minimal stand-in for _OperatorKill — only needs .active."""

	def __init__(self, *, active: bool) -> None:
		self.active = active


def _sig() -> object:
	"""Minimal signal stub (provider doesn't inspect signal fields in B1)."""

	class _Sig:
		pass

	return _Sig()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_build_sources_open_count_from_read_open_count(live_conn: sqlite3.Connection) -> None:
	"""open_count == 2 (pending rows count); open_positions == [] (open-only)."""
	ms = MarketState(limit=10)
	provider = RiskContextProvider(conn=live_conn, operator_kill=_FakeKill(active=False), market_state=ms)
	now = datetime(2026, 5, 22, tzinfo=timezone.utc)
	ctx = provider.build(signal=_sig(), now=now)

	assert ctx.open_count == 2           # pending rows count toward MAX_OPEN
	assert len(ctx.open_positions) == 0  # MTM list is status='open' ONLY
	assert ctx.now_utc == now
	assert ctx.market_state is ms        # held ref forwarded — identity check
	assert ctx.operator_kill_active is False


def test_build_reflects_operator_kill_flag(live_conn: sqlite3.Connection) -> None:
	"""operator_kill_active mirrors _OperatorKill.active when env var unset."""
	ms = MarketState(limit=10)
	provider = RiskContextProvider(conn=live_conn, operator_kill=_FakeKill(active=True), market_state=ms)
	ctx = provider.build(
		signal=_sig(),
		now=datetime.now(timezone.utc),
	)
	assert ctx.operator_kill_active is True


def test_build_env_kill_switch_overrides_false_active(
	live_conn: sqlite3.Connection,
	monkeypatch: pytest.MonkeyPatch,
) -> None:
	"""KILL_SWITCH=1 env var forces operator_kill_active True even if .active is False."""
	monkeypatch.setenv("KILL_SWITCH", "1")
	ms = MarketState(limit=10)
	provider = RiskContextProvider(conn=live_conn, operator_kill=_FakeKill(active=False), market_state=ms)
	ctx = provider.build(
		signal=_sig(),
		now=datetime.now(timezone.utc),
	)
	assert ctx.operator_kill_active is True


@pytest.mark.parametrize(
	"value, expected_active",
	[
		# Disable intents must NOT activate the kill — any casing, plus no/off.
		("", False),
		("0", False),
		("false", False),
		("False", False),
		("FALSE", False),
		("  false  ", False),
		("no", False),
		("off", False),
		("OFF", False),
		# Activate contract + any unrecognized value — fail-safe toward halting.
		("1", True),
		("true", True),
		("yes", True),
		("garbage", True),
	],
)
def test_env_kill_active_parses_case_insensitively(
	value: str,
	expected_active: bool,
	monkeypatch: pytest.MonkeyPatch,
) -> None:
	"""bug_008: KILL_SWITCH falsy parsing is case-insensitive and covers no/off.

	Deny-list semantics are preserved (any unrecognized value errs toward
	halting — fail-safe), but common disable tokens (FALSE/no/off in any
	casing) must NOT flip the operator full-stop ON.
	"""
	monkeypatch.setenv("KILL_SWITCH", value)
	assert _env_kill_active() is expected_active
