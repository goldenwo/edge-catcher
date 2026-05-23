"""RiskContextProvider — builds one RiskContext per signal (live only).

Paper path never instantiates this class (paper passes ``risk=None`` and the
dispatch gate short-circuits).  Only the live arm of E's dispatch wiring
constructs this provider.

Spec §3 / §11.3: the provider holds a DIRECT reference to the same live
db_conn B's writers use (single shared connection, sync-in-async).  All three
read functions are pure reads — no writes, no I/O outside the passed
connection.
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from typing import Protocol

from edge_catcher.engine.live_db import (
	read_daily_pnl_cents,
	read_open_count,
	read_open_positions,
)
from edge_catcher.engine.market_state import MarketState
from edge_catcher.engine.risk import RiskContext


class _SupportsActive(Protocol):
	"""Structural contract for the operator-kill flag object."""

	active: bool


def _env_kill_active() -> bool:
	"""Operator full-stop via the KILL_SWITCH env var (spec §6).

	Deny-list semantics: any value that is NOT a recognized falsy token
	activates the kill (fail-safe — an ambiguous value errs toward halting).
	Normalized case-insensitively so common disable intents (FALSE/no/off in
	any casing) do not accidentally flip the full-stop ON.
	"""
	return os.environ.get("KILL_SWITCH", "").strip().lower() not in (
		"",
		"0",
		"false",
		"no",
		"off",
	)


class RiskContextProvider:
	"""Builds one RiskContext per signal (live only).

	Holds a direct reference to the same live db_conn B's writers use (single
	shared connection, sync-in-async — spec §3 / §11.3), the engine-scoped
	operator-kill flag, and the engine's ``MarketState`` instance (the single
	mutable object created at engine startup and updated in place by the WS
	loop).  Holding the reference means ``build`` always sees current orderbook
	state without any per-tick plumbing.

	``open_count`` is sourced from ``read_open_count`` (open+pending+exit_pending
	— pending DELIBERATELY counts toward MAX_OPEN so an in-flight entry holds
	its slot).  ``open_positions`` is sourced from ``read_open_positions``
	(status='open' only — equity MTM).  The two intentionally differ when
	pending rows exist (spec §3).
	"""

	__slots__ = ("_conn", "_operator_kill", "_market_state")

	def __init__(
		self,
		conn: sqlite3.Connection,
		operator_kill: _SupportsActive,
		market_state: MarketState,
	) -> None:
		self._conn = conn
		self._operator_kill = operator_kill  # the _OperatorKill singleton (has .active)
		self._market_state = market_state    # engine-scoped ref; mutated in place by WS loop

	def build(self, signal: object, now: datetime) -> RiskContext:
		"""Build a fresh RiskContext for a single gate evaluation.

		Args:
			signal: The entry/exit Signal (not inspected here; passed through for
				future extensibility and for callers that need the full context).
			now: The current UTC datetime (caller-supplied for testability).

		Returns:
			A frozen RiskContext capturing the current DB state + kill flags.
			``market_state`` is the engine's held reference (current orderbook
			state for equity MTM), not derived from any tick.
		"""
		return RiskContext(
			now_utc=now,
			market_state=self._market_state,
			open_positions=read_open_positions(self._conn),       # status='open' ONLY — equity MTM
			open_count=read_open_count(self._conn),               # open+pending+exit_pending — MAX_OPEN
			daily_pnl_cents=read_daily_pnl_cents(self._conn, now.date()),
			operator_kill_active=self._operator_kill.active or _env_kill_active(),
		)
