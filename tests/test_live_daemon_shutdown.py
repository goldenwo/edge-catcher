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

	live_src = inspect.getsource(live_trade._run_live_trade)
	paper_src = inspect.getsource(paper_trade._run_paper_trade)
	assert "run_engine_with_signal_bridge" in live_src, (
		"_run_live_trade must call the shared run_engine_with_signal_bridge "
		"helper (not a bare asyncio.run(run_engine(...)) -- that drain is dead "
		"under systemctl stop)"
	)
	assert "run_engine_with_signal_bridge" in paper_src, (
		"_run_paper_trade must ALSO route through the shared bridge helper "
		"(single-sourced fix; paper benefits too -- byte-exact on the "
		"non-signal path)"
	)
	# Neither entrypoint keeps its own bare asyncio.run(run_engine(...)).
	# The anti-pattern is `asyncio.run(run_engine(` -- the open-paren directly
	# after `run_engine` (the dead bare engine call). The legitimate
	# `asyncio.run(run_engine_with_signal_bridge(` has `_` after `run_engine`
	# so the trailing `(` precisely distinguishes the two.
	assert "asyncio.run(run_engine(" not in live_src.replace(" ", ""), (
		"_run_live_trade must NOT retain the bare asyncio.run(run_engine(...)) "
		"-- it must route through run_engine_with_signal_bridge"
	)
	assert "asyncio.run(run_engine(" not in paper_src.replace(" ", ""), (
		"_run_paper_trade must NOT retain the bare asyncio.run(run_engine(...)) "
		"-- it must route through run_engine_with_signal_bridge"
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
