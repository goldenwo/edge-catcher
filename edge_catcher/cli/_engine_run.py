"""Shared entrypoint run-path: a POSIX SIGTERM/SIGINT -> cancel bridge that
makes ``run_engine``'s existing graceful-shutdown ``finally:`` drain reachable
under ``systemctl stop`` (sub-project E, Phase F, task F1 -- BRIDGE ONLY).

Both ``cli/live_trade.py`` and ``cli/paper_trade.py`` were a bare
``asyncio.run(run_engine(...))`` with NO signal handler. Python's default
SIGTERM disposition terminates the process WITHOUT unwinding ``run_engine``'s
``finally:`` (engine.py: cancel B's background tasks -> ``asyncio.gather`` ->
``store.close()`` once -> ``capture_writer.close()``). For paper that is
tolerable; for the LIVE daemon it is catastrophic -- ``systemctl stop``
would strand in-flight order / trade-store state un-drained (FUNDS-AT-RISK).

This module installs, AT THE COMPOSITION ROOT (where the event loop is
created), ``loop.add_signal_handler(SIGTERM/SIGINT, root_task.cancel)`` so a
``systemctl stop`` (SIGTERM) or interactive Ctrl-C (SIGINT) cancels the root
task; the cancel propagates a ``CancelledError`` into ``run_engine``, whose
``_ws_loop`` re-raises it (engine.py ``except asyncio.CancelledError: raise``)
so the EXISTING ``finally:`` drain runs.

Scope: this is the BRIDGE ONLY. The drain ORDER (operator-kill -> stop-intake
-> await-in-flight -> cancel-B -> gather -> close-once -> final-alert) and the
``asyncio.shield`` over place->persist are F2's scope -- NOT here. F1 ONLY
makes the existing drain reachable via cancellation; it does not modify the
``run_engine`` ``finally:`` body.

POSIX-only: the Windows ``ProactorEventLoop`` has no ``add_signal_handler``
(raises ``NotImplementedError``) and CI is Windows -- the bridge is a
documented, logged no-op there (the engine still runs; a graceful drain is
then only reachable via an explicit cancel / the default SIGINT KeyboardInterrupt).

Additive / sec 9 G-parity: the handler only fires on an ACTUAL SIGTERM/SIGINT,
which never happens in paper replay/backtest/CI -- so the non-signal code path
is byte-identical to the prior bare ``asyncio.run`` and paper behaviour is
unchanged. A signal-driven cancel is a CLEAN exit (the drain already ran in
``run_engine``'s ``finally:``), not a crash; a genuine engine error still
propagates unchanged.
"""
from __future__ import annotations

import asyncio
import logging
import signal
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

# SIGINT is included alongside SIGTERM so an interactive Ctrl-C also
# unwinds the drain (default SIGINT raises KeyboardInterrupt at an arbitrary
# point, which -- like default SIGTERM -- does NOT route through run_engine's
# finally cleanly). add_signal_handler(SIGINT, ...) supersedes that default on
# POSIX so both shutdown signals share the one graceful path.
_SHUTDOWN_SIGNALS = (signal.SIGTERM, signal.SIGINT)


async def run_engine_with_signal_bridge(config_path: Path) -> Any:
	"""Run ``run_engine`` as the root task with a POSIX signal->cancel bridge.

	The single shared entrypoint run-path for BOTH ``paper-trade`` and
	``live-trade`` (E1 made them thin wrappers; the bridge is installed once
	here so both daemons drain on ``systemctl stop`` -- live = critical, paper
	= harmless byte-exact improvement on the non-signal path).

	Behaviour:
	  * Creates the ``run_engine`` root task on the running loop.
	  * Installs ``loop.add_signal_handler(sig, root.cancel)`` for SIGTERM and
	    SIGINT (POSIX). On the Windows ``ProactorEventLoop`` this raises
	    ``NotImplementedError`` -- caught, logged as a WARNING, and the engine
	    runs anyway (documented no-op fallback).
	  * Awaits the root task. A signal-driven ``CancelledError`` means the
	    drain already ran in ``run_engine``'s ``finally:`` -> return cleanly
	    (NOT a crash). Any other exception (a genuine engine failure) is
	    re-raised unchanged. A normal completion returns ``run_engine``'s
	    result transparently (additive -- paper byte-exact).

	Args:
		config_path: Path to the YAML config ``run_engine`` loads (mode is
			DATA in the config per spec sec 2 -- this run-path is mode-agnostic).

	Returns:
		Whatever ``run_engine`` returned on a normal completion, or ``None``
		on a clean signal-driven shutdown.
	"""
	from edge_catcher.engine.engine import run_engine

	loop = asyncio.get_running_loop()
	root_task: asyncio.Task = asyncio.ensure_future(
		run_engine(config_path=config_path)
	)

	_install_signal_bridge(loop, root_task)

	try:
		return await root_task
	except asyncio.CancelledError:
		# A SIGTERM/SIGINT cancelled the root task; run_engine's `finally:`
		# already drained (B tasks cancelled, store.close() once, capture
		# writer closed). This is the money-safe shutdown path -- a CLEAN
		# exit, NOT a crash. Swallow the cancel here at the entrypoint.
		log.info(
			"engine root task cancelled by shutdown signal -- graceful drain "
			"completed in run_engine's finally; exiting cleanly"
		)
		return None


def _install_signal_bridge(
	loop: asyncio.AbstractEventLoop, root_task: asyncio.Task
) -> None:
	"""Wire SIGTERM/SIGINT to cancel ``root_task`` (POSIX); log a no-op
	WARNING on Windows/ProactorEventLoop where it is unsupported.

	``loop.add_signal_handler`` raises ``NotImplementedError`` on the Windows
	``ProactorEventLoop`` (and CI is Windows). The whole bridge is gated by
	one ``try/except NotImplementedError`` -- if signal handlers are
	unavailable the loop is left at its default disposition and the engine
	still runs; a graceful drain is then reachable only via an explicit
	cancel / the default Ctrl-C ``KeyboardInterrupt``. This is the documented
	fallback, surfaced as a single clear operator WARNING.
	"""
	try:
		for sig in _SHUTDOWN_SIGNALS:
			loop.add_signal_handler(sig, root_task.cancel)
		log.info(
			"POSIX signal bridge installed (SIGTERM/SIGINT -> graceful drain "
			"via run_engine finally)"
		)
	except NotImplementedError:
		# Windows / ProactorEventLoop: add_signal_handler is unsupported.
		log.warning(
			"POSIX signal bridge unavailable (Windows/ProactorEventLoop) -- "
			"graceful drain only via an explicit cancel / default SIGINT "
			"KeyboardInterrupt; systemctl-stop SIGTERM draining is POSIX-only"
		)
