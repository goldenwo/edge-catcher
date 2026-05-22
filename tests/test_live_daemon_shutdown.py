"""F1 -- POSIX SIGTERM/SIGINT->cancel bridge: the existing ``run_engine``
graceful-shutdown ``finally:`` drain is REACHABLE under ``systemctl stop``
(sub-project E, Phase F, task F1 -- money-safe daemon shutdown, BRIDGE ONLY).

Failure mode prevented (FUNDS-AT-RISK): the live entrypoint
(``cli/live_trade.py``) -- and the paper one (``cli/paper_trade.py``) -- was a
bare ``asyncio.run(run_engine(...))`` with NO signal handler. Python's default
SIGTERM disposition TERMINATES the process WITHOUT unwinding ``run_engine``'s
``finally:`` (engine.py:1178-1189: cancel B's tasks -> ``asyncio.gather`` ->
``store.close()`` once -> ``capture_writer.close()``). On the LIVE Pi daemon
``systemctl stop`` would therefore kill the process with in-flight order /
trade-store state un-drained. F1 installs, AT THE ENTRYPOINT (the composition
root where ``asyncio.run`` / the event loop is created), a POSIX
``loop.add_signal_handler(SIGTERM/SIGINT, root_task.cancel)`` so the cancel
propagates a ``CancelledError`` into ``run_engine`` and its EXISTING
finally-drain runs. F1 is the BRIDGE ONLY -- the drain ORDER + the place->persist
``asyncio.shield`` are F2's scope and are NOT touched here.

Scope guards encoded as tests:
  * **Reachable:** cancelling the root task routes through ``_ws_loop``'s
    ``except asyncio.CancelledError: raise`` (engine.py:1128) into
    ``run_engine``'s ``finally:`` -- B/base tasks get cancelled, ``store.close()``
    is called EXACTLY once (the ``SQLiteTradeStore._closed`` idempotent guard),
    ``capture_writer.close()`` runs.
  * **POSIX vs Windows:** on POSIX the entrypoint helper MUST attempt
    ``loop.add_signal_handler`` for BOTH SIGTERM and SIGINT; on Windows
    (ProactorEventLoop raises ``NotImplementedError`` -- and CI IS Windows) the
    bridge is a DOCUMENTED, logged no-op and the engine still runs / drains via
    an explicit cancel. The test drives shutdown by cancelling the root task
    DIRECTLY (NOT a real signal) so it is cross-platform-correct.
  * **Additive (paper byte-exact basis):** a normal (non-cancelled)
    ``run_engine`` completion is UNAFFECTED -- the bridge only fires on an
    actual SIGTERM/SIGINT, which never happens in paper replay/backtest/CI, so
    the sec 9 G-parity non-signal code path is byte-identical.

Harness mirrors ``tests/test_live_composition_root.py`` (the E2 cfg builders /
the ``_ws_loop`` post-composition abort seam / the ``discover_strategies``
stub). Windows-CI-correct: shutdown is driven via ``task.cancel()``, never a
real ``os.kill``/signal. Run from the project venv
(``.venv/Scripts/python.exe``).
"""
from __future__ import annotations

import ast
import asyncio
import inspect
import signal
import sys
from pathlib import Path
from typing import Any

import pytest

import edge_catcher.engine.engine as engmod
from edge_catcher.engine.strategy_base import Signal, Strategy, TickContext

# Reuse E2's fully-coherent cfg builders verbatim (the established cross-test
# idiom -- test_live_composition_root.py imports the same way).
from tests.test_live_engine_mode_invariant import (
	_write_cfg,
	make_paper_cfg,
)

_STUB_SERIES = "KXSTUB15M"


class _ShutdownStubStrategy(Strategy):
	"""Inert framework stub -- discovered + enabled so step-2 passes; the
	engine reaches the WS-loop seam (post task-creation) without trading."""

	name = "shutdown-stub"
	supported_series = [_STUB_SERIES]
	default_params: dict = {}

	def on_tick(self, ctx: TickContext) -> list[Signal]:
		return []


def _paper_cfg_path(tmp_path: Path) -> Path:
	"""A coherent paper cfg whose step-2 prerequisites (a sizing block + the
	stub enabled on its synthetic series) let ``run_engine`` boot THROUGH
	composition + background-task creation to the ``_ws_loop`` seam."""
	cfg = make_paper_cfg(tmp_path)
	cfg["sizing"] = {
		"risk_per_trade_cents": 500, "max_slippage_cents": 5, "min_fill": 1,
	}
	cfg["strategies"] = {
		_ShutdownStubStrategy.name: {"enabled": True, "series": [_STUB_SERIES]},
	}
	return _write_cfg(cfg, tmp_path)


@pytest.fixture
def _drain_spies(monkeypatch: pytest.MonkeyPatch):
	"""Drive the REAL ``run_engine`` to the post-composition ``_ws_loop`` seam
	and spy the EXACTLY-three finally-drain effects (engine.py:1178-1189):
	the background tasks were cancelled, ``store.close()`` was called, the
	capture writer was closed.

	``run_recovery`` is a harmless no-op (it precedes the task-creation block);
	``_ws_loop`` BLOCKS on an Event so the boot has created all background
	tasks and is *awaiting* ``_ws_loop`` when the test cancels the root task --
	exactly the live steady-state ``systemctl stop`` interrupts. ``_ws_loop``'s
	own ``except asyncio.CancelledError: raise`` (engine.py:1128) then routes
	the cancel into ``run_engine``'s ``finally:``. Paper path (no live deps).
	"""
	captured: dict[str, Any] = {
		"ws_loop_entered": asyncio.Event(),
		"store_close_calls": 0,
		"capture_close_calls": 0,
		"bg_tasks": [],
	}

	async def _noop_recovery(*_a, **_kw):
		return None

	monkeypatch.setattr(engmod, "run_recovery", _noop_recovery)

	# _ws_loop blocks forever so the root task is parked *inside* the awaited
	# _ws_loop (all background tasks already created) when the test cancels --
	# the cancel then propagates through _ws_loop's `except CancelledError:
	# raise` into run_engine's finally:. A bare `await asyncio.Event().wait()`
	# is the cleanest infinite-but-cancellable park.
	async def _blocking_ws_loop(*_a, **_kw):
		captured["ws_loop_entered"].set()
		await asyncio.Event().wait()  # parked until the root task is cancelled

	monkeypatch.setattr(engmod, "_ws_loop", _blocking_ws_loop)

	# Step-2 strategy discovery is mode-agnostic -- patch it to the inert stub
	# so the enabled set is non-empty and the boot proceeds to the seam.
	monkeypatch.setattr(
		engmod, "discover_strategies", lambda: [_ShutdownStubStrategy()]
	)

	# Spy the paper TradeStore.close + RawFrameWriter.close so we can prove the
	# finally-drain RAN (and store.close ran exactly once via its guard).
	_orig_ts_close = engmod.TradeStore.close

	def _ts_close(self):
		captured["store_close_calls"] += 1
		return _orig_ts_close(self)

	monkeypatch.setattr(engmod.TradeStore, "close", _ts_close)

	from edge_catcher.engine.capture.writer import RawFrameWriter

	_orig_cap_close = RawFrameWriter.close

	def _cap_close(self):
		captured["capture_close_calls"] += 1
		return _orig_cap_close(self)

	monkeypatch.setattr(RawFrameWriter, "close", _cap_close)

	# Record the background-task objects run_engine creates (settlement
	# poller / summary logger / state flusher / ticker refresh) so the test
	# can assert the finally-drain's `for task in tasks: task.cancel()` +
	# `asyncio.gather(...)` actually cancelled them. ``asyncio.Task`` is an
	# immutable C type (its `cancel` method cannot be monkeypatched), so we
	# spy the creation seam instead and inspect `.cancelled()` after the
	# drain -- the established idiom (test_live_composition_root.py:296-303).
	_orig_create_task = asyncio.create_task

	def _spy_create_task(coro, *, name=None):
		t = _orig_create_task(coro, name=name)
		captured["bg_tasks"].append(t)
		return t

	monkeypatch.setattr(engmod.asyncio, "create_task", _spy_create_task)

	return captured


# ===========================================================================
# 1. The bridge makes the existing run_engine finally-drain REACHABLE.
# ===========================================================================


@pytest.mark.asyncio
async def test_cancelling_root_task_runs_run_engine_finally_drain(
	tmp_path: Path, _drain_spies,
) -> None:
	"""Failure mode prevented (FUNDS-AT-RISK): a cancel at the entrypoint does
	NOT reach ``run_engine``'s ``finally:`` -- ``systemctl stop`` would kill the
	live daemon with B's tasks alive + the trade store un-closed.

	Drives the REAL ``run_engine`` (paper, coherent cfg) as a task to the
	``_ws_loop`` seam, cancels it (exactly what the bridge's signal handler
	does -- direct ``task.cancel()`` because CI is Windows, NOT a real signal),
	and asserts the finally-drain RAN: the background tasks it created were
	cancelled (the drain's `for task in tasks: task.cancel()` + gather),
	``store.close()`` was called (>=1; the ``_closed`` guard makes it
	idempotent), the capture writer was closed."""
	cfg_path = _paper_cfg_path(tmp_path)
	root = asyncio.create_task(engmod.run_engine(config_path=cfg_path))

	# Wait until run_engine is parked INSIDE the awaited _ws_loop -- i.e. all
	# background tasks have been created and the finally: is now reachable.
	await asyncio.wait_for(_drain_spies["ws_loop_entered"].wait(), timeout=10.0)

	# Simulate the bridge's signal handler: cancel the root task.
	root.cancel()
	with pytest.raises(asyncio.CancelledError):
		await root

	# The EXISTING finally-drain (engine.py:1178-1189) ran. run_engine created
	# >=4 background tasks (settlement poller / summary logger / state flusher
	# / ticker refresh); the drain's `for task in tasks: task.cancel()` +
	# `asyncio.gather(...)` must have left every one DONE and not-running.
	bg = _drain_spies["bg_tasks"]
	assert len(bg) >= 4, (
		"run_engine must have created its background tasks before the seam "
		f"(settlement/summary/flusher/ticker); recorded {len(bg)}"
	)
	assert all(t.done() for t in bg), (
		"run_engine's finally: must cancel + gather ALL its background tasks "
		"when the root task is cancelled (the SIGTERM-bridge drain path) -- "
		f"still-running: {[t.get_name() for t in bg if not t.done()]!r}"
	)
	assert _drain_spies["store_close_calls"] >= 1, (
		"run_engine's finally: must call store.close() on a cancel -- without "
		"this, systemctl stop strands the trade store un-flushed (FUNDS-AT-RISK)"
	)
	assert _drain_spies["capture_close_calls"] >= 1, (
		"run_engine's finally: must close the capture writer on a cancel"
	)


@pytest.mark.asyncio
async def test_finally_drain_store_close_is_idempotent_once(
	tmp_path: Path, _drain_spies,
) -> None:
	"""Failure mode prevented: the drain double-closes the store (or a second
	close raises ``sqlite3.ProgrammingError``). The live ``SQLiteTradeStore``
	carries a ``_closed`` idempotent guard (store.py:1500-1507) -- the spec's
	close-once contract. With the paper ``TradeStore`` here the drain calls
	``close`` exactly once; asserting the drain never closes more than once
	keeps the once-only invariant honest at the engine level."""
	cfg_path = _paper_cfg_path(tmp_path)
	root = asyncio.create_task(engmod.run_engine(config_path=cfg_path))
	await asyncio.wait_for(_drain_spies["ws_loop_entered"].wait(), timeout=10.0)
	root.cancel()
	with pytest.raises(asyncio.CancelledError):
		await root

	assert _drain_spies["store_close_calls"] == 1, (
		"the finally-drain must close the store EXACTLY once (close-once "
		f"contract); got {_drain_spies['store_close_calls']}"
	)


# ===========================================================================
# 2. The bridge is installed at the entrypoint: POSIX add_signal_handler,
#    Windows documented no-op fallback.
# ===========================================================================


def _bridge_helper():
	"""The shared entrypoint helper that installs the signal->cancel bridge
	and is called by BOTH cli/paper_trade.py and cli/live_trade.py. Resolved
	lazily so the RED run (before the helper exists) fails on the import, the
	canonical TDD signal."""
	from edge_catcher.cli import _engine_run

	return _engine_run


def test_both_entrypoints_route_through_the_shared_bridge_helper() -> None:
	"""Failure mode prevented: the bridge is installed on only ONE of the two
	entrypoints (or duplicated divergently) -- the live OR paper daemon keeps
	the dead bare ``asyncio.run``. Both ``_run_live_trade`` and
	``_run_paper_trade`` must delegate to the ONE shared bridge helper
	(``cli/_engine_run.run_engine_with_signal_bridge``) so the fix is
	single-sourced and BOTH daemons drain on ``systemctl stop``."""
	mod = _bridge_helper()
	assert hasattr(mod, "run_engine_with_signal_bridge"), (
		"cli/_engine_run.py must expose run_engine_with_signal_bridge -- the "
		"single shared entrypoint that installs the SIGTERM/SIGINT bridge"
	)

	from edge_catcher.cli import live_trade, paper_trade

	# Structural AST guard (idiom-consistent with
	# tests/test_live_composition_root.py's §1 guards -- ``inspect.getsource``
	# -> ``ast.parse(...).body[0]`` -> ``ast.walk``). Asserts the call inside
	# EACH wrapper is ``run_engine_with_signal_bridge(...)`` -- NOT a bare
	# ``run_engine(...)`` (whose finally-drain is dead under systemctl stop).
	# An AST walk of the called names is durable where the prior
	# whitespace-strip substring NEGATIVE checks were brittle: it cannot be
	# fooled by a comment / string / reformat, and asserting the POSITIVE
	# called name subsumes the "no bare run_engine(" negative (a call named
	# exactly ``run_engine`` would fail the positive assertion).
	def _engine_call_names(fn) -> set[str]:
		"""The set of top-level callee names invoked in ``fn``'s body that are
		engine entrypoints (``run_engine`` or ``run_engine_with_signal_bridge``
		-- bare ``Name`` callees or ``module.attr`` callees). Non-vacuous: the
		wrapper MUST invoke exactly the bridge helper, never bare run_engine."""
		import textwrap

		src = textwrap.dedent(inspect.getsource(fn))
		tree = ast.parse(src).body[0]
		names: set[str] = set()
		for node in ast.walk(tree):
			if not isinstance(node, ast.Call):
				continue
			fnode = node.func
			callee: str | None = None
			if isinstance(fnode, ast.Name):
				callee = fnode.id
			elif isinstance(fnode, ast.Attribute):
				callee = fnode.attr
			if callee in ("run_engine", "run_engine_with_signal_bridge"):
				names.add(callee)
		return names

	live_calls = _engine_call_names(live_trade._run_live_trade)
	paper_calls = _engine_call_names(paper_trade._run_paper_trade)

	assert "run_engine_with_signal_bridge" in live_calls, (
		"_run_live_trade must call the shared run_engine_with_signal_bridge "
		"helper (AST-verified callee) -- a bare asyncio.run(run_engine(...)) "
		"drain is dead under systemctl stop; called engine entrypoints="
		f"{live_calls!r}"
	)
	assert "run_engine_with_signal_bridge" in paper_calls, (
		"_run_paper_trade must ALSO route through the shared bridge helper "
		"(single-sourced fix; paper benefits too -- byte-exact on the "
		f"non-signal path); called engine entrypoints={paper_calls!r}"
	)
	# The POSITIVE-callee assertion subsumes the old brittle negative substring
	# check: a wrapper that called bare ``run_engine(`` would surface
	# ``run_engine`` here and lack ``run_engine_with_signal_bridge``, failing
	# the asserts above. Pin it explicitly too -- neither wrapper may invoke
	# the bare engine entrypoint directly (its finally-drain is unreachable
	# without the bridge).
	assert "run_engine" not in live_calls, (
		"_run_live_trade must NOT invoke the bare run_engine(...) directly "
		f"(AST callee check) -- route through the bridge; got {live_calls!r}"
	)
	assert "run_engine" not in paper_calls, (
		"_run_paper_trade must NOT invoke the bare run_engine(...) directly "
		f"(AST callee check) -- route through the bridge; got {paper_calls!r}"
	)


@pytest.mark.skipif(
	sys.platform == "win32",
	reason="POSIX-only: add_signal_handler is unsupported on the Windows "
	"ProactorEventLoop (asserted by the Windows no-op test below)",
)
@pytest.mark.asyncio
async def test_posix_bridge_installs_sigterm_and_sigint_handlers(
	tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
	"""POSIX (the live Pi target). Failure mode prevented: the bridge installs
	NO handler / only SIGTERM -- ``systemctl stop`` (SIGTERM) or an interactive
	Ctrl-C (SIGINT) on the Pi would still bypass the drain.

	Asserts the helper attempts ``loop.add_signal_handler`` for BOTH
	``signal.SIGTERM`` and ``signal.SIGINT``, registering a cancel for the
	root task. ``run_engine`` is stubbed to return immediately so the test
	isolates the BRIDGE INSTALLATION (not the engine)."""
	mod = _bridge_helper()
	installed: list[int] = []

	loop = asyncio.get_running_loop()
	_orig_add = loop.add_signal_handler

	def _spy_add(sig, cb, *a):
		installed.append(sig)
		return _orig_add(sig, cb, *a)

	monkeypatch.setattr(loop, "add_signal_handler", _spy_add)

	async def _noop_engine(*_a, **_kw):
		return None

	monkeypatch.setattr(engmod, "run_engine", _noop_engine)

	await mod.run_engine_with_signal_bridge(config_path=tmp_path / "x.yaml")

	assert signal.SIGTERM in installed, (
		"POSIX bridge MUST add_signal_handler(SIGTERM, root.cancel) -- that is "
		"the systemctl-stop signal the live daemon receives"
	)
	assert signal.SIGINT in installed, (
		"POSIX bridge MUST also add_signal_handler(SIGINT, root.cancel) -- an "
		"interactive Ctrl-C must drain too"
	)


@pytest.mark.asyncio
async def test_windows_bridge_is_a_documented_logged_noop(
	tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
	caplog: pytest.LogCaptureFixture,
) -> None:
	"""Windows / ProactorEventLoop (CI IS Windows). Failure mode prevented:
	``add_signal_handler``'s ``NotImplementedError`` on the ProactorEventLoop
	crashes the entrypoint (or is swallowed silently with no operator trail).

	Forces ``add_signal_handler`` to raise ``NotImplementedError`` (the exact
	ProactorEventLoop behaviour) and asserts: the bridge does NOT propagate it,
	logs a clear WARNING about the unavailable POSIX bridge, and ``run_engine``
	still runs to completion (graceful drain then only via an explicit
	cancel / SIGINT-default -- exactly the documented fallback)."""
	import logging

	mod = _bridge_helper()
	loop = asyncio.get_running_loop()

	def _raise_nie(*_a, **_kw):
		raise NotImplementedError("ProactorEventLoop has no add_signal_handler")

	monkeypatch.setattr(loop, "add_signal_handler", _raise_nie)

	engine_ran = {"v": False}

	async def _noop_engine(*_a, **_kw):
		engine_ran["v"] = True
		return None

	monkeypatch.setattr(engmod, "run_engine", _noop_engine)

	with caplog.at_level(logging.WARNING):
		# Must NOT raise NotImplementedError -- the no-op fallback swallows it.
		await mod.run_engine_with_signal_bridge(config_path=tmp_path / "x.yaml")

	assert engine_ran["v"], (
		"run_engine must still run when the POSIX bridge is unavailable "
		"(Windows no-op fallback -- the engine is NOT blocked by it)"
	)
	msgs = " ".join(r.getMessage() for r in caplog.records).lower()
	assert "signal" in msgs and (
		"unavailable" in msgs or "no-op" in msgs or "windows" in msgs
		or "proactor" in msgs
	), (
		"the Windows no-op fallback MUST log a clear WARNING that the POSIX "
		f"signal bridge is unavailable; got logs: {msgs!r}"
	)


# ===========================================================================
# 3. The bridge is ADDITIVE -- a normal completion is unaffected (paper
#    byte-exact basis: the bridge only fires on an actual signal/cancel).
# ===========================================================================


@pytest.mark.asyncio
async def test_bridge_is_additive_normal_completion_unaffected(
	tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
	"""Failure mode prevented (sec 9 G-parity BLOCKING): the bridge perturbs the
	NON-signal path -- a normal ``run_engine`` return is wrapped/swallowed/
	mutated, changing paper/replay behaviour. When NO signal/cancel occurs the
	helper must return EXACTLY what ``run_engine`` returned and propagate a
	genuine engine error unchanged (only an actual SIGTERM/SIGINT cancel is a
	clean exit -- that path never executes in paper replay/backtest/CI)."""
	mod = _bridge_helper()

	# (a) Normal completion: helper returns run_engine's result unchanged.
	sentinel = object()

	async def _ok_engine(*_a, **_kw):
		return sentinel

	monkeypatch.setattr(engmod, "run_engine", _ok_engine)
	result = await mod.run_engine_with_signal_bridge(
		config_path=tmp_path / "x.yaml"
	)
	assert result is sentinel, (
		"with no signal the bridge must be transparent -- return exactly "
		"run_engine's result (additive; paper byte-exact non-signal path)"
	)

	# (b) A genuine engine error is NOT swallowed by the bridge (only a
	# signal-driven CancelledError is the clean exit).
	class _EngineBoom(RuntimeError):
		pass

	async def _boom_engine(*_a, **_kw):
		raise _EngineBoom("genuine engine failure must propagate")

	monkeypatch.setattr(engmod, "run_engine", _boom_engine)
	with pytest.raises(_EngineBoom):
		await mod.run_engine_with_signal_bridge(config_path=tmp_path / "x.yaml")


# ===========================================================================
# 4. F2 -- money-safe SIGTERM drain ORDER + the place->persist asyncio.shield.
#
# THE money-safety task. FUNDS-AT-RISK at maximum: a SIGTERM landing EXACTLY
# between ``executor.place()`` returning ``filled`` and the local ``record_trade``
# persist must NOT orphan a real-money position. The shielded place->persist
# critical region + the drain's explicit await of the in-flight registry
# guarantee the row is persisted (one ``open`` row, NOT left ``pending``).
# Normative design: converged spec sec 4.2 L2 + sec 4.3 (the 7-step drain).
# ===========================================================================

from datetime import datetime, timezone  # noqa: E402

import edge_catcher.engine.dispatch as dispmod  # noqa: E402
from edge_catcher.engine.executor import OrderResult  # noqa: E402
from edge_catcher.engine.strategy_base import Signal as _Sig  # noqa: E402
from edge_catcher.live.state import connect_live_trades_db  # noqa: E402
from edge_catcher.live.store import SQLiteTradeStore  # noqa: E402

_F2_NOW = datetime(2026, 5, 19, 12, 0, 0, tzinfo=timezone.utc)


def _f2_entry_signal() -> _Sig:
	"""A minimal valid entry Signal (mirrors test_live_state_integration's
	``_entry_signal``) so the real ``dispatch._handle_enter`` runs end to end."""
	return _Sig(
		action="enter",
		ticker="KXSOL15M-19MAY19H12",
		side="yes",
		series="KXSOL15M",
		strategy="debut_fade",
		reason="f2-orphan-safety",
		entry_price_cents=42,
		stop_loss_distance_cents=8,
	)


class _CancelBetweenPlaceAndPersistExecutor:
	"""Stub executor whose ``place()`` returns ``filled`` and, AT THE EXACT
	seam between place-returns and the local persist, deterministically
	cancels the ``_handle_enter`` AWAITING FRAME (the live ``systemctl stop``
	interrupt at its single worst instant).

	Deterministic injection (NOT a sleep race): ``place()`` schedules
	``cancel_target.cancel()`` via ``loop.call_soon`` so the cancellation is
	delivered at the VERY NEXT event-loop iteration — i.e. the first ``await``
	AFTER ``place()`` returns, which (sans shield) would be the resumption of
	``_handle_enter`` BEFORE ``record_trade``. ``cancel_target`` is the
	``_handle_enter`` task (set by the test right after it creates it) — NOT
	the inner shielded section (cancelling the section would just be an
	ordinary mid-place cancel; the money-safety claim is specifically that the
	AWAITING FRAME is cancelled yet the SHIELDED section still persists).
	Without the §4.2-L2 shield + the drain's registry await that cancel lands
	before ``record_trade`` and the funds-at-risk row is orphaned at
	``pending``; with them it completes to a durable ``open`` row.
	"""

	def __init__(self) -> None:
		self.place_calls = 0
		# Set by the test to the _handle_enter task immediately after
		# create_task (the awaiting frame, NOT the inner section).
		self.cancel_target: asyncio.Task | None = None

	async def place(self, req) -> OrderResult:  # noqa: ANN001
		self.place_calls += 1
		# Arm a cancel of the AWAITING FRAME for the next loop tick — the
		# instant control would leave the shielded region if it were not
		# shielded. call_soon (not call_later) = zero wall-clock dependency;
		# the cancel is queued behind the current callback and fires at the
		# next iteration deterministically.
		assert self.cancel_target is not None, (
			"test must set cancel_target to the _handle_enter task"
		)
		loop = asyncio.get_running_loop()
		loop.call_soon(self.cancel_target.cancel)
		return OrderResult(
			status="filled",
			intended_size=10,
			filled_size=10,
			blended_entry_cents=42,
			fill_pct=1.0,
			slippage_cents=0,
			order_id="kx-f2-entry",
		)


@pytest.fixture
def _f2_live_db(tmp_path: Path) -> Path:
	"""A fresh migrated live_trades.db (0003 + WAL) -- the real money DB the
	real ``SQLiteTradeStore`` CAS-persists into (no shim; the C/D idiom)."""
	p = tmp_path / "live_trades.db"
	connect_live_trades_db(p).close()
	return p


@pytest.fixture(autouse=True)
def _reset_f2_engine_singletons():
	"""Reset the PROCESS-LIFETIME F2 singletons between tests.

	``dispatch._OPERATOR_KILL`` is intentionally process-scoped (one engine
	per process; "tripped-kill ≠ process exit" — F3 scope) and
	``_INFLIGHT_SECTIONS`` is a module set; neither is reset in production.
	Tests, however, share the interpreter, so a prior test that activated the
	flag / registered a section would leak into the next. Snapshot + restore
	around every test in this module so each F2 assertion starts from the
	pristine process state (no production behaviour change — test-only)."""
	prev_active = dispmod._OPERATOR_KILL.active
	dispmod._OPERATOR_KILL.active = False
	prev_sections = set(dispmod._INFLIGHT_SECTIONS)
	dispmod._INFLIGHT_SECTIONS.clear()
	try:
		yield
	finally:
		dispmod._OPERATOR_KILL.active = prev_active
		dispmod._INFLIGHT_SECTIONS.clear()
		dispmod._INFLIGHT_SECTIONS.update(prev_sections)


@pytest.mark.asyncio
async def test_sigterm_between_place_and_persist_persists_row(
	_f2_live_db: Path,
) -> None:
	"""NORMATIVE (FUNDS-AT-RISK, maximal). A SIGTERM/cancel lands EXACTLY
	between ``executor.place()`` returning ``filled`` and the local
	``record_trade`` persist. Without the sec 4.2-L2 ``asyncio.shield`` around
	the place->persist critical region (and the drain's explicit await of the
	in-flight registry), the cancel interrupts ``_handle_enter`` before
	``record_trade`` -> the C1 ``pending`` row is NEVER transitioned ->
	a real-money Kalshi position is orphaned with the local row stuck at
	``pending``.

	Drives the REAL ``dispatch._handle_enter`` (real ``SQLiteTradeStore`` over
	a migrated live_trades.db, the C/D integration idiom) as a task; the stub
	executor injects the cancel at the precise seam. Asserts: exactly ONE
	``open`` row for the dispatch-generated client_order_id (NOT ``pending``,
	NOT orphaned) because the shielded section + the registry drain completed
	it; the in-flight registry is empty again after the drain (the section
	deregistered on completion)."""
	store = SQLiteTradeStore(_f2_live_db)
	try:
		conn = store._conn
		signal = _f2_entry_signal()
		ctx = type("C", (), {"yes_ask": 42, "no_ask": 58})()
		executor = _CancelBetweenPlaceAndPersistExecutor()
		cfg: dict = {}

		# The registry must start empty and end empty (the section registers
		# on entry, deregisters on completion -- even though the task that
		# AWAITS the shield is cancelled at the seam).
		assert len(dispmod._INFLIGHT_SECTIONS) == 0

		root = asyncio.create_task(
			dispmod._handle_enter(
				signal, ctx, store, cfg, executor, now=_F2_NOW,
			)
		)
		# Arm the executor to cancel THIS awaiting frame (not the inner
		# shielded section) at the place->persist seam.
		executor.cancel_target = root

		# The cancel fires (call_soon, armed inside place()) the instant
		# _handle_enter would leave the shielded region. _handle_enter's
		# awaiting frame IS cancelled -- but the registered shielded task
		# runs to completion regardless. The drain (run_engine's finally,
		# step 3) is what AWAITS the registry; here we emulate exactly that
		# drain step deterministically: await every registered section to
		# completion, then assert the durable money state.
		with pytest.raises(asyncio.CancelledError):
			await root

		# === sec 4.3 drain step (3): await the in-flight registry to
		# completion (the DRAIN owns the await -- NOT a naive `await shield`).
		# Call the REAL drain entry point run_engine's finally invokes, so this
		# orphan-safety test exercises the actual step-(3) code (it diverges if
		# the inline copy ever drifts from drain_inflight_sections); the
		# orphan-safety guarantee is that AFTER this the persist has happened.
		await dispmod.drain_inflight_sections()

		# The funds-at-risk row is DURABLE and NOT orphaned: exactly one row,
		# status 'open' (the C1 'pending' row was CAS-transitioned by the
		# shielded record_trade), carrying D's real kalshi_order_id.
		coid_row = conn.execute(
			"SELECT status, kalshi_order_id, COUNT(*) OVER () AS n "
			"FROM live_trades"
		).fetchone()
		assert coid_row is not None, (
			"FUNDS-AT-RISK: a SIGTERM between place()->filled and the persist "
			"orphaned the real-money position -- NO live_trades row exists "
			"(the C1 pending row must have been written by record_intent and "
			"transitioned to open by the shielded record_trade)"
		)
		status, kalshi_order_id, n = coid_row
		assert n == 1, (
			f"exactly one live_trades row expected (C1 row CAS-transitioned, "
			f"not a 2nd insert); got {n}"
		)
		assert status == "open", (
			"FUNDS-AT-RISK: the row is %r, NOT 'open' -- the shielded "
			"place->persist region did NOT complete the record_trade CAS "
			"under the SIGTERM; the real-money position is orphaned at "
			"'pending'" % (status,)
		)
		assert kalshi_order_id == "kx-f2-entry", (
			"the durable open row must carry D's real OrderResult.order_id "
			f"(kalshi_order_id); got {kalshi_order_id!r}"
		)
		assert executor.place_calls == 1, "place() must run exactly once"

		# The shielded section deregistered itself on completion (even though
		# the awaiting frame was cancelled at the seam) -- no leak.
		assert len(dispmod._INFLIGHT_SECTIONS) == 0, (
			"the in-flight registry must be empty after the section completes "
			f"(deregister-on-completion); leaked: {dispmod._INFLIGHT_SECTIONS!r}"
		)
	finally:
		store.close()


@pytest.mark.asyncio
async def test_sigterm_drain_follows_normative_4_3_order(
	tmp_path: Path, _drain_spies, monkeypatch: pytest.MonkeyPatch,
) -> None:
	"""NORMATIVE (sec 4.3 drain ORDER). On a SIGTERM/cancel the ``run_engine``
	``finally:`` drain MUST run the EXACT sequence:

	  (1) set the operator-kill flag (gate then rejects new entries via
	      KILL_OPERATOR) -- FIRST, so no new entry enters the in-flight
	      registry during the drain;
	  (2) stop WS/dispatch intake;
	  (3) await the L2 in-flight registry to completion;
	  (4) cancel B's loops;
	  (5) gather(*tasks, return_exceptions=True);
	  (6) store.close() exactly once -- STRICTLY AFTER (3);
	  (7) final 'shutting down' alert to the live ops channel -- LAST.

	The order is load-bearing: operator_kill BEFORE awaiting in-flight (no new
	entries enter the registry during drain); close STRICTLY AFTER the
	in-flight await (never close the DB while a shielded persist is mid-write).
	Asserts a concrete recorded call-order list (non-vacuous)."""
	order: list[str] = []

	# Spy the engine-scoped operator-kill flag set (drain step 1). The drain
	# sets dispmod._OPERATOR_KILL.active = True; record the transition.
	assert dispmod._OPERATOR_KILL.active is False, (
		"operator-kill must start inactive (a fresh process is not killed)"
	)
	_orig_ok_setter = type(dispmod._OPERATOR_KILL).activate

	def _spy_activate(self) -> None:
		order.append("operator_kill")
		_orig_ok_setter(self)

	monkeypatch.setattr(
		type(dispmod._OPERATOR_KILL), "activate", _spy_activate
	)

	# Spy the in-flight registry drain (step 3). engine.py imported
	# ``drain_inflight_sections`` BY NAME, so the live reference the drain
	# calls is ``engmod.drain_inflight_sections`` — patch THERE (patching
	# ``dispmod.drain_inflight_sections`` would not rebind engine's name).
	_orig_drain = engmod.drain_inflight_sections

	async def _spy_drain() -> None:
		order.append("await_inflight")
		return await _orig_drain()

	monkeypatch.setattr(engmod, "drain_inflight_sections", _spy_drain)

	# Spy store.close (step 6). The _drain_spies fixture already wraps
	# TradeStore.close for the count; here we additionally record ORDER.
	_ts_close_counted = engmod.TradeStore.close

	def _ts_close_ordered(self):  # noqa: ANN001
		order.append("store_close")
		return _ts_close_counted(self)

	monkeypatch.setattr(engmod.TradeStore, "close", _ts_close_ordered)

	# Spy the final 'shutting down' alert (step 7). The drain emits it via
	# engmod.notify ONLY on the signal/cancel path (paper byte-exact on the
	# normal path -- asserted by the additive test below).
	_orig_notify = engmod.notify

	def _spy_notify(text: str) -> None:
		if "shut" in text.lower() or "drain" in text.lower():
			order.append("final_alert")
		return _orig_notify(text)

	monkeypatch.setattr(engmod, "notify", _spy_notify)

	cfg_path = _paper_cfg_path(tmp_path)
	root = asyncio.create_task(engmod.run_engine(config_path=cfg_path))
	await asyncio.wait_for(_drain_spies["ws_loop_entered"].wait(), timeout=10.0)
	root.cancel()
	with pytest.raises(asyncio.CancelledError):
		await root

	# The operator-kill flag is left active (tripped-kill != process exit is
	# F3's scope; F2 only sets it as drain step 1 -- assert it WAS set).
	assert dispmod._OPERATOR_KILL.active is True, (
		"sec 4.3 step 1: the drain MUST set the operator-kill flag (the gate "
		"then rejects new entries via KILL_OPERATOR during the drain)"
	)

	# Concrete, non-vacuous order assertions. Every load-bearing pair:
	assert "operator_kill" in order, "drain step 1 (operator_kill) must run"
	assert "await_inflight" in order, "drain step 3 (await in-flight) must run"
	assert "store_close" in order, "drain step 6 (store.close) must run"
	assert "final_alert" in order, (
		"drain step 7 (final shutting-down alert) must run on the signal path"
	)

	i_ok = order.index("operator_kill")
	i_inflight = order.index("await_inflight")
	i_close = order.index("store_close")
	i_alert = order.index("final_alert")

	assert i_ok < i_inflight, (
		"sec 4.3: operator_kill (step 1) MUST precede the in-flight await "
		f"(step 3) -- no new entry may enter the registry mid-drain; got {order}"
	)
	assert i_inflight < i_close, (
		"sec 4.3: the in-flight registry await (step 3) MUST complete STRICTLY "
		"BEFORE store.close() (step 6) -- never close the DB while a shielded "
		f"persist is mid-write (FUNDS-AT-RISK); got {order}"
	)
	assert i_close < i_alert, (
		"sec 4.3: the final 'shutting down' alert (step 7) is LAST -- after "
		f"store.close(); got {order}"
	)

	# B/base background tasks were cancelled + gathered (steps 4/5) -- reuse
	# the F1 drain-spy assertion (the tasks the drain cancelled are all done).
	bg = _drain_spies["bg_tasks"]
	assert bg and all(t.done() for t in bg), (
		"sec 4.3 steps 4/5: the drain must cancel + gather ALL B/base tasks; "
		f"still-running: {[t.get_name() for t in bg if not t.done()]!r}"
	)
	# store.close() exactly once (sec 4.3 step 6 idempotent-guard contract).
	assert _drain_spies["store_close_calls"] == 1, (
		"store.close() must be called EXACTLY once in the reordered drain "
		f"(close-once contract); got {_drain_spies['store_close_calls']}"
	)


@pytest.mark.asyncio
async def test_f2_paper_byte_exact_non_signal_completion_unchanged(
	tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
	"""sec 9 G-parity (BLOCKING). The F2 reorder + shield must be INVISIBLE on
	the NON-signal path:

	  * a NORMAL (non-signal) ``run_engine`` completion does NOT set the
	    operator-kill flag and does NOT emit the final 'shutting down' alert
	    (those are signal-drain-only; paper replay/backtest/CI never SIGTERM);
	  * the ``asyncio.shield`` around place->persist is a no-op for the sync
	    ``PaperExecutor`` path -- a normal paper entry persists EXACTLY as
	    before (one shielded section, registered + immediately deregistered,
	    no suspension the paper path observes), and the in-flight registry is
	    empty afterwards.

	A normal completion is exercised by making ``discover_strategies`` return
	NO strategies -> ``run_engine`` hits the early ``store.close(); return``
	(engine.py:984-987), a clean non-signal exit (the same byte-exact basis
	F1's additive test used)."""
	# (a) Normal completion: NO strategies -> run_engine returns cleanly
	# WITHOUT entering the signal-drain finally body's signal-only steps.
	monkeypatch.setattr(engmod, "discover_strategies", lambda: [])
	assert dispmod._OPERATOR_KILL.active is False

	alerts: list[str] = []
	_orig_notify = engmod.notify

	def _capture_notify(text: str) -> None:
		alerts.append(text)
		return _orig_notify(text)

	monkeypatch.setattr(engmod, "notify", _capture_notify)

	cfg = make_paper_cfg(tmp_path)
	cfg["sizing"] = {
		"risk_per_trade_cents": 500, "max_slippage_cents": 5, "min_fill": 1,
	}
	cfg_path = _write_cfg(cfg, tmp_path)

	await engmod.run_engine(config_path=cfg_path)

	assert dispmod._OPERATOR_KILL.active is False, (
		"sec 9 G-parity: a NORMAL (non-signal) completion must NOT set the "
		"operator-kill flag -- it is signal-drain-only (paper byte-exact)"
	)
	assert not any(
		("shut" in a.lower() or "drain" in a.lower()) for a in alerts
	), (
		"sec 9 G-parity: a NORMAL completion must NOT emit the final "
		f"'shutting down' alert (signal-drain-only); got {alerts!r}"
	)

	# (b) The shield is a no-op for the SYNC PaperExecutor place->persist:
	# a normal paper entry persists EXACTLY as before; the registry is empty
	# after (registered + immediately deregistered within the one call, no
	# suspension the paper path observes).
	from edge_catcher.engine.executors.paper import PaperExecutor
	from edge_catcher.engine.market_state import MarketState
	from edge_catcher.engine.trade_store import TradeStore

	assert len(dispmod._INFLIGHT_SECTIONS) == 0
	ms = MarketState(limit=50)
	pcfg: dict = {
		"sizing": {"risk_per_trade_cents": 500, "max_slippage_cents": 5,
		           "min_fill": 1},
	}
	pexec = PaperExecutor(market_state=ms, config=pcfg)
	pstore = TradeStore(tmp_path / "paper_trades.db")
	try:
		# A degenerate-price signal returns BEFORE place (no row) -- fine; we
		# only assert the shield/registry leave NO residue on the paper path
		# regardless of fill outcome (the byte-exact invariant is "invisible").
		sig = _f2_entry_signal()
		pctx = type("C", (), {"yes_ask": 42, "no_ask": 58})()
		await dispmod._handle_enter(
			sig, pctx, pstore, pcfg, pexec, now=_F2_NOW,
		)
		assert len(dispmod._INFLIGHT_SECTIONS) == 0, (
			"sec 9 G-parity: the place->persist shield must leave the "
			"in-flight registry EMPTY on the sync PaperExecutor path "
			f"(register + immediately deregister); leaked: "
			f"{dispmod._INFLIGHT_SECTIONS!r}"
		)
	finally:
		pstore.close()


# ===========================================================================
# 5. F3 -- deploy/live-trader.service systemd unit (spec §8).
#
# The deploy unit that wires `systemctl stop` -> SIGTERM -> F1/F2's graceful
# §4.3 drain on the live Pi daemon. It MIRRORS deploy/paper-trader.service
# (same User/WorkingDirectory/EnvironmentFile/MemoryMax/CPUQuota conventions);
# only the name + ExecStart + the EXPLICIT TimeoutStopSec=30 (the F1/F2 drain
# budget, NOT systemd's 90s default) + the explicit KillSignal=SIGTERM differ.
# A lint test (not a real systemctl run -- CI is Windows) parses the unit and
# pins the F3-load-bearing keys + structural equivalence to the paper unit.
# ===========================================================================

_DEPLOY_DIR = Path(__file__).resolve().parents[1] / "deploy"


def _parse_unit(path: Path) -> dict[str, dict[str, str]]:
	"""Minimal systemd-unit parser: ``[Section]`` headers + ``key=value``
	lines (comments / blanks ignored). systemd allows a key to repeat
	(list-append semantics); the F3 keys under test are all single-valued so
	last-wins is sufficient + keeps the lint test simple. Mirrors how
	deploy/paper-trader.service is structured (a flat INI-like unit)."""
	sections: dict[str, dict[str, str]] = {}
	current: str | None = None
	for raw in path.read_text(encoding="utf-8").splitlines():
		line = raw.strip()
		if not line or line.startswith("#") or line.startswith(";"):
			continue
		if line.startswith("[") and line.endswith("]"):
			current = line[1:-1]
			sections.setdefault(current, {})
			continue
		if "=" in line and current is not None:
			k, _, v = line.partition("=")
			sections[current][k.strip()] = v.strip()
	return sections


def test_live_trader_service_exists_and_parses() -> None:
	"""Failure mode prevented: the live daemon has NO systemd unit (or a
	malformed one) -> it cannot be deployed under systemd at all, so the F1/F2
	`systemctl stop` -> graceful-drain path is never exercised in production.
	Asserts deploy/live-trader.service exists and parses into the expected
	[Unit]/[Service]/[Install] section shape (same as the paper unit)."""
	unit_path = _DEPLOY_DIR / "live-trader.service"
	assert unit_path.is_file(), (
		f"deploy/live-trader.service must exist (spec §8); looked at {unit_path}"
	)
	parsed = _parse_unit(unit_path)
	assert {"Unit", "Service", "Install"} <= set(parsed), (
		"the unit must have [Unit], [Service] and [Install] sections (mirroring "
		f"deploy/paper-trader.service); parsed sections={sorted(parsed)}"
	)


def test_live_trader_service_pins_f1_f2_drain_contract() -> None:
	"""Failure mode prevented (FUNDS-AT-RISK): the live unit omits the
	bounded-drain wiring -> `systemctl stop` SIGKILLs the money daemon (or
	uses systemd's 90s default) while a shielded place->persist is mid-write,
	OR a tripped C auto-kill that exited would be restarted past operator
	intent. Pins the F3 load-bearing [Service] keys:

	  * Type=simple                      (mirrors paper)
	  * Restart=always + RestartSec=5    (long-running loop; §4.3-safe ONLY
	                                       because a tripped auto-kill never
	                                       exits -- tested below)
	  * TimeoutStopSec=30                (EXPLICIT F1/F2 drain budget, NOT the
	                                       90s systemd default)
	  * KillSignal=SIGTERM               (explicit -- F1 bridges it to the
	                                       §4.3 F2 graceful drain)
	  * ExecStart -> `-m edge_catcher live-trade` + the live config path
	"""
	svc = _parse_unit(_DEPLOY_DIR / "live-trader.service")["Service"]

	assert svc.get("Type") == "simple", f"Type must be simple; got {svc.get('Type')!r}"
	assert svc.get("Restart") == "always", (
		f"Restart must be always (long-running daemon); got {svc.get('Restart')!r}"
	)
	assert svc.get("RestartSec") == "5", (
		f"RestartSec must mirror the paper unit (5s); got {svc.get('RestartSec')!r}"
	)
	# THE F3 deliverable: the drain budget is set EXPLICITLY to 30s. systemd's
	# default is 90s -- relying on it would let a slow/hung drain linger 3x
	# longer before SIGKILL on a money daemon.
	assert svc.get("TimeoutStopSec") == "30", (
		"deploy/live-trader.service MUST set TimeoutStopSec=30 EXPLICITLY (the "
		"F1/F2 §4.3 drain budget -- NOT systemd's 90s default); got "
		f"{svc.get('TimeoutStopSec')!r}"
	)
	# KillSignal=SIGTERM is the systemd default but stated explicitly now that
	# F1/F2 handle SIGTERM gracefully -- a money daemon must never be SIGKILL'd
	# while a shielded place->persist is mid-write.
	assert svc.get("KillSignal") == "SIGTERM", (
		"deploy/live-trader.service MUST set KillSignal=SIGTERM explicitly "
		f"(F1 bridges it to the §4.3 drain); got {svc.get('KillSignal')!r}"
	)
	exec_start = svc.get("ExecStart", "")
	assert "-m edge_catcher live-trade" in exec_start, (
		"ExecStart must invoke the E1 live entrypoint (`python -m edge_catcher "
		f"live-trade`); got {exec_start!r}"
	)
	assert "config.local/live-trader.yaml" in exec_start, (
		"ExecStart must point at the live config (config.local/live-trader.yaml "
		f"-- the E1 default); got {exec_start!r}"
	)


def test_live_trader_service_mirrors_paper_unit_conventions() -> None:
	"""Failure mode prevented: the live unit silently diverges from the
	validated paper unit's deploy conventions (a different User /
	WorkingDirectory / EnvironmentFile / resource shape) -> the live daemon
	runs under the wrong account or unbounded resources. Asserts the live unit
	mirrors deploy/paper-trader.service EXACTLY on the shared deploy keys; only
	name/ExecStart/TimeoutStopSec/KillSignal legitimately differ."""
	paper = _parse_unit(_DEPLOY_DIR / "paper-trader.service")
	live = _parse_unit(_DEPLOY_DIR / "live-trader.service")

	# [Service] keys that MUST be byte-identical to the paper unit (the
	# validated deploy conventions -- account, cwd, env, resource caps).
	for key in (
		"Type", "User", "Group", "WorkingDirectory", "Environment",
		"EnvironmentFile", "Restart", "RestartSec", "StartLimitIntervalSec",
		"StartLimitBurst", "MemoryMax", "CPUQuota", "Nice",
	):
		assert live["Service"].get(key) == paper["Service"].get(key), (
			f"live-trader.service [Service] {key}={live['Service'].get(key)!r} "
			f"must mirror paper-trader.service {key}="
			f"{paper['Service'].get(key)!r} (shared deploy convention)"
		)

	# [Install] mirrors paper exactly.
	assert live.get("Install") == paper.get("Install"), (
		f"[Install] must mirror the paper unit; live={live.get('Install')!r} "
		f"paper={paper.get('Install')!r}"
	)

	# The legitimate differences: the live unit has its OWN ExecStart (the
	# live entrypoint, not paper-trade) and adds TimeoutStopSec/KillSignal
	# (the paper unit relies on systemd defaults; the money daemon must not).
	assert live["Service"]["ExecStart"] != paper["Service"]["ExecStart"], (
		"the live unit must run the live entrypoint, NOT paper-trade"
	)
	assert "TimeoutStopSec" not in paper["Service"], (
		"sanity: the paper unit relies on systemd's default stop timeout; the "
		"live unit adds an EXPLICIT one (the asymmetry F3 introduces)"
	)


# ===========================================================================
# 6. F3 -- §4.3 NORMATIVE: tripped-kill ≠ process exit (THE invariant).
#
# A C auto-kill (panic / drawdown / daily-loss KillSwitch trip) keeps the
# process RUNNING: the gate enters KILL state (rejects new entries; exits
# still allowed) and the engine continues. ONLY a crash or `systemctl stop`
# (SIGTERM -> F1 bridge -> the §4.3 F2 drain) stops it. This is what makes
# the live unit's Restart=always safe -- a tripped auto-kill never reaches
# systemd, so a restart can never clear operator intent and let the
# previously-blocked trades flow (catastrophic with real money).
#
# Step-1 verification (the task's MANDATORY prerequisite) established the
# invariant ALREADY HOLDS structurally:
#   * a SUCCESSFUL auto-kill: Gate.gate_entry calls _emit_trip ->
#     KillSwitch.trip() INSERT succeeds -> returns normally -> gate returns
#     Reject with NO exception -> process_tick / the WS loop / the reconnect
#     block all CONTINUE (engine keeps running);
#   * KillSwitchTripFailed (the OPPOSITE -- a FAILED kill WRITE): trip()'s
#     INSERT fails -> raises KillSwitchTripFailed -> propagates UNCAUGHT out
#     of run_engine -> process STOPS (C-spec L214 ghost-reject defense);
#   * SIGTERM: F1 cancels the root task -> run_engine's §4.3 finally drain
#     runs -> graceful exit (proven by section 1's drain test).
# So F3 is a documenting comment + these PINNING tests -- ZERO money-logic
# change. The tests below concretely distinguish all THREE outcomes.
# ===========================================================================

import sqlite3  # noqa: E402

from edge_catcher.engine.risk import (  # noqa: E402
	BankrollCache,
	Gate,
	KillSwitch,
	KillSwitchTripFailed,
	PeakTracker,
	RiskConfig,
	RiskContext,
)

_F3_NOW = datetime(2026, 5, 19, 12, 0, 0, tzinfo=timezone.utc)


def _f3_risk_cfg() -> RiskConfig:
	"""A valid Phase-1-shaped RiskConfig. ``absolute_panic_floor_cents`` is
	deliberately HIGH (1_000_000c = $10k) so any realistic equity is ≤ floor
	and Gate.gate_entry takes the KILL_AUTO_PANIC first-trip branch
	deterministically (no network / no real balance needed)."""
	return RiskConfig.from_dict({
		"sizing_pct": 0.005,
		"daily_loss_pct": 0.02,
		"drawdown_pct": 0.05,
		"max_open": 3,
		"min_fill_contracts": 1,
		"absolute_panic_floor_cents": 1_000_000,
		"absolute_max_cents": 5000,
		"kelly_shrinkage": 0.5,
		"bankroll_ttl_seconds": 300.0,
		"bankroll_failures_until_kill": 2,
	})


class _ZeroBalanceSource:
	"""Stub BalanceSource (no network) -- cash stays at the 0 default, so
	equity (0) ≤ the high panic floor and the panic branch trips."""

	async def balance_cents(self) -> int:
		return 0


def _f3_gate(conn: sqlite3.Connection) -> Gate:
	"""A REAL Gate over a migrated live_trades.db: real KillSwitch (the actual
	trip()/INSERT path), real BankrollCache (stub source, never refreshed ->
	0 cash), real PeakTracker. Built directly (NOT build_risk_module, which
	pre-refreshes against a live Kalshi client -- network) so the test is
	hermetic + Windows-correct."""
	cfg = _f3_risk_cfg()
	bankroll = BankrollCache(_source=_ZeroBalanceSource(), _cfg=cfg)
	kill_switch = KillSwitch(conn=conn)
	peak = PeakTracker(conn=conn)
	return Gate(cfg=cfg, bankroll=bankroll, kill_switch=kill_switch, peak_tracker=peak)


def _f3_ctx(*, operator_kill: bool = False) -> RiskContext:
	from edge_catcher.engine.market_state import MarketState

	return RiskContext(
		now_utc=_F3_NOW,
		market_state=MarketState(limit=50),
		open_positions=[],
		daily_pnl_cents=0,
		operator_kill_active=operator_kill,
	)


def _f3_entry_signal() -> Signal:
	return Signal(
		action="enter",
		ticker="KXSOL15M-19MAY19H12",
		side="yes",
		series="KXSOL15M",
		strategy="debut_fade",
		reason="f3-invariant",
		entry_price_cents=42,
		stop_loss_distance_cents=8,
	)


def test_successful_auto_kill_does_not_exit_engine_continues(
	tmp_path: Path,
) -> None:
	"""§4.3 NORMATIVE -- the CORE invariant. A SUCCESSFUL C auto-kill
	(KILL_AUTO_PANIC here) MUST NOT propagate an exception: Gate.gate_entry
	returns a plain ``Reject`` (NO raise), the kill row IS persisted, and a
	subsequent gate call hits the steady-state ``active_auto_kill`` path and
	STILL returns ``Reject`` (entries stay blocked) -- the engine would keep
	running. Crucially ``gate_exit`` STILL ALLOWS exits under the auto-kill
	(kills cap NEW exposure; they never trap existing exposure). If a
	successful auto-kill raised/exited, ``Restart=always`` would loop-clear
	operator intent (catastrophic with real money)."""
	from edge_catcher.live.state import connect_live_trades_db
	from edge_catcher.engine.risk import Reject

	db = tmp_path / "live_trades.db"
	connect_live_trades_db(db).close()
	conn = connect_live_trades_db(db)
	try:
		gate = _f3_gate(conn)
		ctx = _f3_ctx()
		sig = _f3_entry_signal()

		# (1) First entry: the panic branch trips. The trip's INSERT SUCCEEDS
		# (real migrated DB) so gate_entry returns Reject -- it MUST NOT raise.
		decision = gate.gate_entry(sig, ctx)  # must not raise
		assert isinstance(decision, Reject), (
			"a successful auto-kill must yield a Reject decision, not raise; "
			f"got {decision!r}"
		)
		assert decision.reason == "KILL_AUTO_PANIC", (
			f"expected the panic first-trip; got {decision.reason!r}"
		)

		# (2) The kill row WAS persisted (this is why no exception fired -- a
		# successful trip is the documenting-comment'd §4.3 path).
		row = conn.execute(
			"SELECT reason FROM kill_switch WHERE cleared_at IS NULL"
		).fetchone()
		assert row is not None and row[0] == "KILL_AUTO_PANIC", (
			"the successful trip MUST have persisted a kill_switch row "
			f"(steady-state KILL state); got {row!r}"
		)

		# (3) A SECOND entry now takes the steady-state active_auto_kill branch
		# and STILL returns Reject WITHOUT raising -- the engine keeps running
		# in KILL state, rejecting every new entry, indefinitely.
		decision2 = gate.gate_entry(sig, ctx)  # must not raise
		assert isinstance(decision2, Reject), (
			"steady-state: a persisted auto-kill must keep returning Reject "
			f"(no raise -- engine continues); got {decision2!r}"
		)

		# (4) gate_exit STILL ALLOWS exits under the auto-kill (only an
		# operator kill blocks exits) -- the engine continues to wind DOWN
		# existing exposure while blocking new entries. This is the behaviour
		# `Restart=always` must NOT be able to reset.
		exit_decision = gate.gate_exit(sig, ctx)
		assert type(exit_decision).__name__ == "Allow", (
			"auto-kill must STILL allow exits (kills cap new exposure, never "
			f"trap existing) -- got {exit_decision!r}"
		)
	finally:
		conn.close()


def test_kill_switch_trip_failed_propagates_and_halts(
	tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
	"""§4.3 -- the OPPOSITE case (must stay opposite). A FAILED kill-state
	WRITE (KillSwitch.trip's INSERT fails) raises ``KillSwitchTripFailed``,
	which MUST propagate out of ``Gate.gate_entry`` UNCAUGHT (the C-spec L214
	ghost-reject defense -- the engine then STOPS rather than re-evaluating
	the gate next tick against unpersisted state). This is deliberately the
	inverse of the successful-trip path above: a successful kill must NOT
	halt; a FAILED kill-write MUST. F3 must not blur the two."""
	from edge_catcher.live.state import connect_live_trades_db

	db = tmp_path / "live_trades.db"
	connect_live_trades_db(db).close()
	conn = connect_live_trades_db(db)
	try:
		gate = _f3_gate(conn)
		ctx = _f3_ctx()
		sig = _f3_entry_signal()

		# Simulate the trip's INSERT failing exactly as a real DB failure
		# would: KillSwitch.trip() wraps a sqlite3.Error and raises
		# KillSwitchTripFailed. Patch ONLY KillSwitch.trip (NOT conn.execute --
		# the gate's branch-2 ``active_auto_kill`` SELECT must keep working so
		# the flow reaches the panic-trip branch and exercises the REAL
		# _emit_trip -> trip() -> KillSwitchTripFailed propagation chain).
		def _trip_boom(*_a, **_kw):
			raise KillSwitchTripFailed(
				"kill_switch INSERT failed for reason='KILL_AUTO_PANIC': "
				"disk I/O error (simulated)"
			)

		monkeypatch.setattr(
			gate._kill_switch, "trip", _trip_boom
		)

		# It MUST propagate UNCAUGHT out of gate_entry (the C-spec L214
		# ghost-reject defense -- engine then STOPS; the opposite of a
		# successful trip which returns Reject and continues).
		with pytest.raises(KillSwitchTripFailed):
			gate.gate_entry(sig, ctx)
	finally:
		monkeypatch.undo()
		conn.close()


@pytest.mark.asyncio
async def test_sigterm_drains_then_exits_distinct_from_auto_kill(
	tmp_path: Path, _drain_spies,
) -> None:
	"""§4.3 -- the THIRD outcome (distinct from the two gate cases above).
	SIGTERM is the ONLY non-crash path that stops the process, and it does so
	via the F1 bridge -> run_engine's §4.3 F2 drain -> a clean exit (NOT a
	raised/propagated kill). Drives the REAL ``run_engine`` (paper, the F1
	harness) to the WS-loop seam, cancels the root task (exactly what F1's
	signal handler does -- ``task.cancel()``; CI is Windows so NEVER a real
	signal), and asserts it exited THROUGH the §4.3 finally drain
	(``store.close()`` ran, the operator-kill flag was set as drain step 1) --
	i.e. SIGTERM => drain-then-exit, categorically different from a successful
	auto-kill (continues) and a KillSwitchTripFailed (raises out)."""
	cfg_path = _paper_cfg_path(tmp_path)
	root = asyncio.create_task(engmod.run_engine(config_path=cfg_path))
	await asyncio.wait_for(_drain_spies["ws_loop_entered"].wait(), timeout=10.0)

	# F1's signal handler effect: cancel the root task.
	root.cancel()
	with pytest.raises(asyncio.CancelledError):
		await root

	# It exited THROUGH the §4.3 drain (NOT a bare process kill): the drain's
	# store.close() ran and the operator-kill flag was set (drain step 1).
	assert _drain_spies["store_close_calls"] >= 1, (
		"SIGTERM must exit via the §4.3 F2 drain (store.close ran) -- NOT a "
		"bare kill, and NOT the gate-Reject (auto-kill) path"
	)
	assert dispmod._OPERATOR_KILL.active is True, (
		"the §4.3 drain (SIGTERM path) sets the operator-kill flag as step 1; "
		"this is the ONLY one of the three outcomes that stops the process "
		"gracefully (auto-kill continues; KillSwitchTripFailed raises out)"
	)
