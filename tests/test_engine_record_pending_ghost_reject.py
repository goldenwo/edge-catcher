"""Ghost-reject defense for ``RecordPendingFailed`` (spec Tests #29 + #30).

Mirror of the existing ``KillSwitchTripFailed`` coverage:
* runtime template — ``tests/test_dispatch.py``
  ``test_process_tick_reraises_kill_switch_trip_failed`` (+ its counter-test
  ``test_process_tick_swallows_non_kill_switch_exceptions``)
* AST-inspection idiom — ``tests/test_engine_execution.py``
  (``ast.parse(inspect.getsource(...))`` + ``ast.walk``)

There is NO single ``tests/test_engine_kill_switch_ghost_reject.py`` —
verified absent on ``main@d22db0f`` 2026-05-16; earlier spec drafts named it
in error.

Test #29 — ``RecordPendingFailed`` raised from a real ``record_pending``
INSERT must propagate past the broad per-signal ``except Exception`` in
``process_tick`` and past the broad ``except Exception`` in ``engine._ws_loop``
/ the outer reconnect block, so ``run_engine`` terminates rather than
continuing the WS loop against unchanged DB state (a funds-at-risk Kalshi
order stranded with no local row). Asserts both the runtime behaviour and —
via AST inspection — that the three ``except RecordPendingFailed: raise``
clauses exist at the same sites that re-raise ``KillSwitchTripFailed``.

Test #30 — ``record_rejected`` audit-write is best-effort: a failing
``record_rejected`` INSERT does NOT raise ``RecordPendingFailed`` (rejected
rows represent orders Kalshi already rejected — no Kalshi-side position, no
money at risk; a failed INSERT strands only an audit row). Follows the
PR #34 audit-write precedent (commit ``438d843``: log + swallow, do not mask
control flow). Pinned because the round-2 spec review caught a tendency to
over-broaden the ghost-reject scope.
"""
from __future__ import annotations

import ast
import inspect
import sqlite3
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from edge_catcher.engine import engine as engine_module
from edge_catcher.engine.dispatch import process_tick
from edge_catcher.engine.executor import OrderResult
from edge_catcher.engine.market_state import OrderbookSnapshot, TickContext
from edge_catcher.engine.strategy_base import Signal
from edge_catcher.live.state import RecordPendingFailed

_NOW = datetime(2026, 5, 16, 12, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Helpers (mirror tests/test_dispatch.py + test_engine_dispatch_pending_branch.py)
# ---------------------------------------------------------------------------


class _EnterStrategy:
	"""Minimal strategy that emits one enter signal per on_tick call."""

	name = "stub"
	emoji = "🔵"

	def on_tick(self, ctx: TickContext) -> list[Signal]:  # type: ignore[override]
		return [
			Signal(
				action="enter",
				ticker=ctx.ticker,
				side="yes",
				series="X",
				strategy=self.name,
				reason="test",
				entry_price_cents=50,
				stop_loss_distance_cents=8,
			)
		]


def _make_tick_ctx() -> TickContext:
	return TickContext(
		ticker="X",
		event_ticker="EX",
		yes_bid=50,
		yes_ask=51,
		no_bid=49,
		no_ask=50,
		orderbook=OrderbookSnapshot(yes_levels=[], no_levels=[]),
		price_history=[],
		open_positions=[],
		persisted_state={},
		market_metadata={},
	)


def _pending_result() -> OrderResult:
	"""A pending OrderResult — drives dispatch into the record_pending branch."""
	return OrderResult(
		status="pending",
		intended_size=10,
		filled_size=0,
		blended_entry_cents=0,
		fill_pct=0.0,
		slippage_cents=0,
		order_id=None,
		rejection_reason="kalshi_unreachable:test",
	)


def _rejected_result() -> OrderResult:
	"""A rejected OrderResult — drives dispatch into the record_rejected branch."""
	return OrderResult(
		status="rejected",
		intended_size=10,
		filled_size=0,
		blended_entry_cents=0,
		fill_pct=0.0,
		slippage_cents=0,
		order_id=None,
		rejection_reason="kalshi_4xx:400",
	)


class _RaisingPendingStore:
	"""Store whose record_pending raises RecordPendingFailed (simulating a
	real SQLite INSERT failure inside live.state.record_pending)."""

	def __init__(self) -> None:
		self.rejected_calls: list[dict[str, Any]] = []

	def record_pending(self, **kwargs: Any) -> None:
		raise RecordPendingFailed("simulated live_trades INSERT failure")

	def record_rejected(self, **kwargs: Any) -> None:  # pragma: no cover
		self.rejected_calls.append(dict(kwargs))


class _ValueErrorPendingStore:
	"""Store whose record_pending raises a NON-ghost-reject exception — must
	stay swallowed by the per-signal broad except (counter-test)."""

	def record_pending(self, **kwargs: Any) -> None:
		raise ValueError("simulated non-ghost-reject business error")


# ---------------------------------------------------------------------------
# Test #29 (a) — runtime: RecordPendingFailed propagates past process_tick's
# broad per-signal except (mirror of test_process_tick_reraises_kill_switch_trip_failed)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_process_tick_reraises_record_pending_failed() -> None:
	"""Test #29: process_tick MUST re-raise RecordPendingFailed so the engine
	STOPS on a record_pending/record_open INSERT failure (B / PR 5 ghost-reject
	defense). The broad ``except Exception`` for other signal-handling errors
	must NOT swallow this specific exception class — otherwise a funds-at-risk
	Kalshi order is stranded with no local row and the next tick re-enters the
	gate against unchanged DB state.

	Exercises the REAL dispatch pending-branch (not a monkeypatched
	_handle_signal) so the test proves the actual ``except RecordPendingFailed:
	raise`` clause fires for a genuine ``store.record_pending`` failure.
	"""
	store = _RaisingPendingStore()
	executor = MagicMock()
	executor.place = AsyncMock(return_value=_pending_result())

	with pytest.raises(RecordPendingFailed):
		await process_tick(
			_make_tick_ctx(),
			[_EnterStrategy()],
			store,  # type: ignore[arg-type]
			config={},
			executor=executor,
			now=_NOW,
			risk=None,
		)


@pytest.mark.asyncio
async def test_process_tick_swallows_non_record_pending_failed() -> None:
	"""Counter-test for #29: a NON-RecordPendingFailed exception from
	record_pending must STILL be logged + swallowed (preserves the existing
	per-signal isolation behaviour). Pins the re-raise clause as SPECIFIC —
	a future refactor that broadens it to ``except Exception: raise`` would
	fail this test.
	"""
	store = _ValueErrorPendingStore()
	executor = MagicMock()
	executor.place = AsyncMock(return_value=_pending_result())

	# Must NOT raise — process_tick logs and continues.
	await process_tick(
		_make_tick_ctx(),
		[_EnterStrategy()],
		store,  # type: ignore[arg-type]
		config={},
		executor=executor,
		now=_NOW,
		risk=None,
	)


# ---------------------------------------------------------------------------
# Test #29 (b) — AST inspection: the three `except RecordPendingFailed: raise`
# clauses exist at the same sites that re-raise KillSwitchTripFailed.
# Idiom mirrors tests/test_engine_execution.py.
# ---------------------------------------------------------------------------


def _bare_reraise_handlers_for(func: object, exc_name: str) -> list[ast.ExceptHandler]:
	"""Return every ``except <exc_name>:`` handler in *func*'s source whose
	body is exactly a single bare ``raise`` (re-raise of the active exception).
	"""
	tree = ast.parse(inspect.getsource(func))  # type: ignore[arg-type]
	matches: list[ast.ExceptHandler] = []
	for node in ast.walk(tree):
		if not isinstance(node, ast.ExceptHandler):
			continue
		if not isinstance(node.type, ast.Name) or node.type.id != exc_name:
			continue
		if (
			len(node.body) == 1
			and isinstance(node.body[0], ast.Raise)
			and node.body[0].exc is None
			and node.body[0].cause is None
		):
			matches.append(node)
	return matches


def _broad_except_handler(handlers: list[ast.ExceptHandler]) -> ast.ExceptHandler | None:
	"""Find the broad ``except Exception:`` handler in a try's handler list."""
	for h in handlers:
		if isinstance(h.type, ast.Name) and h.type.id == "Exception":
			return h
	return None


def _try_nodes(func: object) -> list[ast.Try]:
	return [n for n in ast.walk(ast.parse(inspect.getsource(func))) if isinstance(n, ast.Try)]


def test_dispatch_process_tick_has_record_pending_reraise() -> None:
	"""process_tick's per-signal try/except must contain
	``except RecordPendingFailed: raise`` positioned BEFORE the broad
	``except Exception`` (so the broad clause cannot swallow it).
	"""
	handlers = _bare_reraise_handlers_for(process_tick, "RecordPendingFailed")
	assert len(handlers) == 1, (
		f"process_tick must have exactly one bare-reraise "
		f"`except RecordPendingFailed: raise`, found {len(handlers)}"
	)

	# Positional check: RecordPendingFailed handler appears before the broad
	# `except Exception` in the SAME try block.
	for try_node in _try_nodes(process_tick):
		names = [
			h.type.id
			for h in try_node.handlers
			if isinstance(h.type, ast.Name)
		]
		if "RecordPendingFailed" in names and "Exception" in names:
			assert names.index("RecordPendingFailed") < names.index("Exception"), (
				"`except RecordPendingFailed` MUST precede `except Exception` "
				"or the broad clause swallows it (funds-at-risk)"
			)
			broad = _broad_except_handler(try_node.handlers)
			assert broad is not None
			return
	pytest.fail(
		"no try block in process_tick contains both RecordPendingFailed and "
		"the broad Exception handler"
	)


def test_engine_ws_loop_and_reconnect_have_record_pending_reraise() -> None:
	"""engine.run_engine (outer reconnect block) AND engine._ws_loop (dispatch
	try/except) must each contain ``except RecordPendingFailed: raise`` —
	the same two engine-module sites that re-raise KillSwitchTripFailed.
	"""
	run_engine_handlers = _bare_reraise_handlers_for(
		engine_module.run_engine, "RecordPendingFailed"
	)
	ws_loop_handlers = _bare_reraise_handlers_for(
		engine_module._ws_loop, "RecordPendingFailed"
	)

	assert len(run_engine_handlers) == 1, (
		f"run_engine outer reconnect block must re-raise RecordPendingFailed "
		f"exactly once, found {len(run_engine_handlers)}"
	)
	assert len(ws_loop_handlers) == 1, (
		f"_ws_loop dispatch try/except must re-raise RecordPendingFailed "
		f"exactly once, found {len(ws_loop_handlers)}"
	)

	# Parity assertion: every site that re-raises KillSwitchTripFailed in the
	# engine module also re-raises RecordPendingFailed (the two ghost-reject
	# defenses must stay symmetric).
	for func in (engine_module.run_engine, engine_module._ws_loop):
		ks = len(_bare_reraise_handlers_for(func, "KillSwitchTripFailed"))
		rp = len(_bare_reraise_handlers_for(func, "RecordPendingFailed"))
		assert ks == rp == 1, (
			f"{func.__name__}: KillSwitchTripFailed re-raises ({ks}) and "
			f"RecordPendingFailed re-raises ({rp}) must both be exactly 1 "
			f"(ghost-reject defenses must stay symmetric)"
		)


def test_record_pending_reraise_precedes_broad_except_in_engine_sites() -> None:
	"""In BOTH engine sites the RecordPendingFailed handler must appear before
	the broad ``except Exception`` handler in its try block (positional
	correctness — a handler after the broad clause would be dead code).
	"""
	for func in (engine_module.run_engine, engine_module._ws_loop):
		found = False
		for try_node in _try_nodes(func):
			names = [
				h.type.id
				for h in try_node.handlers
				if isinstance(h.type, ast.Name)
			]
			if "RecordPendingFailed" in names and "Exception" in names:
				assert names.index("RecordPendingFailed") < names.index("Exception"), (
					f"{func.__name__}: `except RecordPendingFailed` MUST precede "
					f"`except Exception` (else it is unreachable)"
				)
				found = True
		assert found, (
			f"{func.__name__}: no try block contains both RecordPendingFailed "
			f"and the broad Exception handler"
		)


def test_record_pending_failed_is_exception_subclass() -> None:
	"""RecordPendingFailed must be a plain Exception subclass (NOT
	BaseException) so the broad `except Exception` WOULD catch it absent the
	explicit re-raise — which is exactly why the explicit clause is load-bearing.
	"""
	assert issubclass(RecordPendingFailed, Exception)
	assert not issubclass(BaseException, RecordPendingFailed)


# ---------------------------------------------------------------------------
# Test #30 — record_rejected audit-write is best-effort (carve-out).
# A failing record_rejected INSERT must NOT raise RecordPendingFailed and the
# engine must continue. Mirrors test_process_tick_swallows_non_kill_switch_exceptions.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_record_rejected_insert_failure_is_swallowed_not_ghost_reject(
	caplog: pytest.LogCaptureFixture,
) -> None:
	"""Test #30: a stubbed ``record_rejected`` that raises
	``sqlite3.OperationalError`` is caught, logged, and the engine CONTINUES
	(no RecordPendingFailed escapes). Rejected rows = orders Kalshi already
	rejected: no Kalshi-side position, no money at risk; a failed INSERT
	strands only an audit row. PR #34 audit-write precedent (438d843).

	This pins the carve-out so a future change cannot over-broaden the
	ghost-reject defense to cover record_rejected.
	"""

	class _RaisingRejectedStore:
		def record_pending(self, **kwargs: Any) -> None:  # pragma: no cover
			raise AssertionError("rejected path must not touch record_pending")

		def record_rejected(self, **kwargs: Any) -> None:
			raise sqlite3.OperationalError("simulated audit-row INSERT failure")

	store = _RaisingRejectedStore()
	executor = MagicMock()
	executor.place = AsyncMock(return_value=_rejected_result())

	# Must NOT raise RecordPendingFailed (or anything) — the rejected-branch
	# audit write is best-effort. process_tick's broad except logs + continues.
	with caplog.at_level("ERROR"):
		await process_tick(
			_make_tick_ctx(),
			[_EnterStrategy()],
			store,  # type: ignore[arg-type]
			config={},
			executor=executor,
			now=_NOW,
			risk=None,
		)

	# Sanity: the failure surfaced in the log (operator-visible audit gap),
	# never silently dropped.
	assert any(
		record.levelname == "ERROR" for record in caplog.records
	), "record_rejected INSERT failure must produce an ERROR log line"


def test_record_rejected_does_not_raise_record_pending_failed_directly() -> None:
	"""Direct unit assertion of the carve-out at the live.state layer:
	live.state.record_rejected on a broken connection must NOT raise
	RecordPendingFailed (it logs + swallows per the PR #34 precedent),
	whereas record_pending/record_open MUST raise it. Pins the
	raise/swallow asymmetry at the source, independent of dispatch wiring.
	"""
	from edge_catcher.live import state as live_state

	# A closed connection makes every execute() raise sqlite3.ProgrammingError
	# (a sqlite3.Error subclass) — the cleanest way to force the INSERT failure
	# branch without monkeypatching.
	conn = sqlite3.connect(":memory:")
	conn.close()

	# record_rejected: carve-out — must NOT raise RecordPendingFailed.
	live_state.record_rejected(
		conn,
		ticker="X",
		series="S",
		strategy="strat",
		side="yes",
		intended_size=10,
		entry_price_cents=50,
		stop_loss_distance_cents=8,
		client_order_id="strat-X-deadbeef",
		placed_at_utc=_NOW.isoformat(),
		rejection_reason="kalshi_4xx:400",
	)  # no exception = pass

	# record_pending: MUST raise RecordPendingFailed on the same broken conn.
	with pytest.raises(RecordPendingFailed):
		live_state.record_pending(
			conn,
			ticker="X",
			series="S",
			strategy="strat",
			side="yes",
			intended_size=10,
			entry_price_cents=50,
			stop_loss_distance_cents=8,
			client_order_id="strat-X-cafebabe",
			kalshi_order_id=None,
			placed_at_utc=_NOW.isoformat(),
			rejection_reason=None,
		)
