"""Tests for the `live-trade` engine subcommand (sub-project E, Phase E1).

E1 adds a THIN CLI wrapper: `live-trade` mirrors `paper-trade` exactly,
changing only the default config path. Mode is DATA resolved from the
config's `executor:` key downstream (E2/E3) — the subcommand itself does
NOT decide or branch on mode (spec §1/§2 keystone).

E2 will extend this module with the fail-closed mode-coherence invariant.
"""
from __future__ import annotations

import argparse
import asyncio
import subprocess
import sys
from pathlib import Path

import pytest
import yaml
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa


def _build_parser() -> argparse.ArgumentParser:
	"""Reconstruct the top-level parser exactly as ``edge_catcher.cli.main``
	does: a subparser group with every command module registered against it.
	"""
	from edge_catcher.cli import (
		backtest,
		download,
		formalize,
		interpret,
		live_trade,
		paper_trade,
		replay_backtest,
		research,
		utils,
	)

	parser = argparse.ArgumentParser()
	sub = parser.add_subparsers(dest="command")
	for module in [
		download,
		backtest,
		research,
		paper_trade,
		live_trade,
		formalize,
		interpret,
		utils,
		replay_backtest,
	]:
		module.register(sub)
	return parser


def test_live_trade_subcommand_registered() -> None:
	"""`live-trade` parses, defaults --config to the gitignored live config,
	and binds a func handler (the thin run_engine wrapper)."""
	parser = _build_parser()
	args = parser.parse_args(["live-trade"])
	assert args.command == "live-trade"
	assert args.config == "config.local/live-trader.yaml"
	assert hasattr(args, "func")


def test_paper_trade_subcommand_unchanged() -> None:
	"""E1 must NOT disturb the existing `paper-trade` subcommand: it is still
	registered with its original default config path and func handler."""
	parser = _build_parser()
	args = parser.parse_args(["paper-trade"])
	assert args.command == "paper-trade"
	assert args.config == "config.local/paper-trader.yaml"
	assert hasattr(args, "func")


def test_live_trade_help_exits_zero() -> None:
	"""`python -m edge_catcher live-trade --help` exits 0 and the subcommand
	is discoverable (end-to-end through the real registration path)."""
	proc = subprocess.run(
		[sys.executable, "-m", "edge_catcher", "live-trade", "--help"],
		capture_output=True,
		text=True,
		timeout=30,
	)
	assert proc.returncode == 0, proc.stderr
	# argparse line-wraps long help text (even inserting a break mid-token);
	# strip ALL whitespace before matching the (space-free) config path.
	assert "--config" in proc.stdout
	squashed = "".join(proc.stdout.split())
	assert "config.local/live-trader.yaml" in squashed


def test_paper_trade_help_still_exits_zero() -> None:
	"""Regression: the `paper-trade` subcommand still works end-to-end and
	its default config path is byte-unchanged."""
	proc = subprocess.run(
		[sys.executable, "-m", "edge_catcher", "paper-trade", "--help"],
		capture_output=True,
		text=True,
		timeout=30,
	)
	assert proc.returncode == 0, proc.stderr
	# argparse line-wraps long help text (even inserting a break mid-token);
	# strip ALL whitespace before matching the (space-free) config path.
	assert "--config" in proc.stdout
	squashed = "".join(proc.stdout.split())
	assert "config.local/paper-trader.yaml" in squashed


# ===========================================================================
# E2 — fail-closed mode-coherence invariant + abort-matrix (spec §2 / §6)
#
# §2 (NORMATIVE): before any WS connect or order placement, `run_engine`
# asserts the declared mode coheres with ALL of: (1) the `executor:` key
# value, (2) the resolved DB path (live ⟺ live_trades*.db; paper ⟺
# paper_trades*.db), (3) creds resolvable via A's auth resolver (live only),
# (4) the live Discord channel(s) resolvable from the unified notifications
# config (live only), (5) the Phase-1 `risk:` caps present (live only).
# Any disagreement → precise RuntimeError BEFORE the first network/order.
#
# §6 (NORMATIVE): the coherence invariant is boot step (2) — called FIRST
# right after config load, BEFORE the executor/store is constructed and
# BEFORE any network/WS/order.
#
# §9 (NORMATIVE): for paper mode the invariant passes cleanly with ZERO
# behavior change (checks 3/4/5 are live-only and skipped; check 2 = paper
# ⟺ paper_trades*.db). The invariant must not perturb the paper path.
# ===========================================================================


# The canonical Phase-1 `risk:` block — the EXACT key set RiskConfig.from_dict
# (edge_catcher/engine/risk.py) requires. Verified against the codebase in
# E2 Step 1: all ten keys are mandatory; *_cents is the real YAML key name
# (spec §8's "absolute_panic_floor: $30" is dollar-prose shorthand — the
# authoritative parser is RiskConfig.from_dict, reused as the §2.5 check so
# there is no drift-prone duplicated key list).
_PHASE1_RISK: dict = {
	"sizing_pct": 0.005,
	"daily_loss_pct": 0.02,
	"drawdown_pct": 0.05,
	"max_open": 5,
	"min_fill_contracts": 3,
	"absolute_panic_floor_cents": 3000,
	"absolute_max_cents": 5000,
	"kelly_shrinkage": 0.5,
	"bankroll_ttl_seconds": 300,
	"bankroll_failures_until_kill": 2,
}

# Live trade-scope signing env-var names. A's design (auth.py docstring): the
# live trader passes trade-scope key var names so a leaked read-only key
# cannot place orders. These mirror the `signing_env` fixture
# (tests/fixtures/mock_kalshi_server.py) — KALSHI_LIVE_* not KALSHI_*.
_LIVE_KEY_ID_ENV = "KALSHI_LIVE_KEY_ID"
_LIVE_PRIVATE_KEY_ENV = "KALSHI_LIVE_PRIVATE_KEY"


def _write_notifications_yaml(
	path: Path, channel_name: str, risk_channel_name: str = "live_risk_alerts"
) -> None:
	"""Write a minimal unified-notifications config with TWO resolvable
	channels (both `file` — zero network, deterministic, the cheapest
	adapter `load_channels` can construct): the general live channel AND
	the DEDICATED risk channel (§2.4/§6 G3 made the latter a mandatory
	coherence dimension for a fully-coherent live config — see
	`_assert_mode_coherence` Check-4b)."""
	cfg = {
		"version": 1,
		"channels": {
			channel_name: {
				"type": "file",
				"path": str(path.parent / "live_alerts.log"),
			},
			risk_channel_name: {
				"type": "file",
				"path": str(path.parent / "live_risk_alerts.log"),
			},
		},
	}
	path.write_text(yaml.safe_dump(cfg), encoding="utf-8")


def make_live_cfg(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict:
	"""Produce a fully-coherent LIVE config dict + the on-disk + env-var
	side state it references. No real network or real Kalshi keys.

	Coherent means all §2 checks pass:
	  1. executor: live
	  2. db_path points at a live_trades*.db
	  3. creds resolvable — a throwaway RSA-2048 keypair in the trade-scope
	     env vars A's auth resolver reads
	  4. live Discord channel resolvable from a unified notifications.yaml
	  4b. the DEDICATED risk channel resolvable (§2.4/§6 G3 — a kill-switch
	      trip's only operator signal; mandatory for a coherent live config)
	  5. the Phase-1 `risk:` caps all present
	"""
	# (3) throwaway signing creds in the live trade-scope env vars.
	key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
	pem = key.private_bytes(
		encoding=serialization.Encoding.PEM,
		format=serialization.PrivateFormat.PKCS8,
		encryption_algorithm=serialization.NoEncryption(),
	)
	monkeypatch.setenv(_LIVE_KEY_ID_ENV, "test-live-key")
	monkeypatch.setenv(_LIVE_PRIVATE_KEY_ENV, pem.decode())

	# (4 / 4b) a resolvable general live channel AND a resolvable dedicated
	# risk channel in a unified notifications config (Check-4 + Check-4b).
	notify_path = tmp_path / "notifications.yaml"
	_write_notifications_yaml(
		notify_path, "live_pnl_discord", "live_risk_alerts"
	)

	return {
		"executor": "live",
		# (2) a live_trades*.db path under tmp.
		"db_path": str(tmp_path / "live_trades.db"),
		"notifications": {
			"config_path": str(notify_path),
			"live_channel": "live_pnl_discord",
			# (4b) the dedicated risk channel — §2.4/§6 G3 made this a
			# mandatory coherence dimension for a fully-coherent live config.
			"live_risk_channel": "live_risk_alerts",
			"live_key_id_env": _LIVE_KEY_ID_ENV,
			"live_private_key_env": _LIVE_PRIVATE_KEY_ENV,
		},
		# (5) the Phase-1 caps (deep-copied so break_field can't bleed
		# across cfgs within a parametrized run).
		"risk": dict(_PHASE1_RISK),
		# Minimal strategies block so config load is well-formed; the
		# coherence invariant runs BEFORE strategy discovery so the
		# content here is irrelevant to E2.
		"strategies": {},
	}


def make_paper_cfg(tmp_path: Path) -> dict:
	"""A fully-coherent PAPER config: executor: paper + a paper_trades*.db.
	Checks 3/4/5 are live-only and skipped — no creds/channels/caps needed.
	"""
	return {
		"executor": "paper",
		"db_path": str(tmp_path / "paper_trades.db"),
		"strategies": {},
	}


def break_field(cfg: dict, which: str, tmp_path: Path) -> None:
	"""Mismatch EXACTLY one of the five §2 coherence dimensions in place,
	leaving the other four coherent. Each break is the realistic
	wrong-mode footgun the invariant must catch BEFORE any money moves.
	"""
	if which == "executor":
		# executor: key says paper while every other live signal (db,
		# creds, channel, caps) says live → ambiguous mode of record.
		cfg["executor"] = "paper"
	elif which == "db":
		# executor: live but the DB path is a paper DB → a live run
		# would write real-money rows into the paper DB.
		cfg["db_path"] = str(tmp_path / "paper_trades.db")
	elif which == "creds":
		# executor: live but the trade-scope signing creds do not
		# resolve → live trading with no auth → every order 401s.
		cfg["notifications"]["live_key_id_env"] = "DOES_NOT_EXIST_KEY_ID"
		cfg["notifications"]["live_private_key_env"] = "DOES_NOT_EXIST_PRIVATE_KEY"
	elif which == "channel":
		# executor: live but the named live channel is absent from the
		# notifications config → live alerts/risk events go nowhere.
		cfg["notifications"]["live_channel"] = "channel_that_is_not_defined"
	elif which == "caps":
		# executor: live but a Phase-1 cap is missing from the risk:
		# block → the gate cannot be constructed → unbounded exposure.
		del cfg["risk"]["absolute_max_cents"]
	else:  # pragma: no cover - guards a typo in the parametrize list
		raise AssertionError(f"unknown break field: {which!r}")


def _write_cfg(cfg: dict, tmp_path: Path) -> Path:
	"""Serialize the config dict to a YAML file run_engine can load."""
	path = tmp_path / "engine.yaml"
	path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
	return path


class _NetworkTouched(AssertionError):
	"""Raised by the spies if the abort path reaches a network/order seam.

	If this ever fires, the coherence gate is bypassable — a wrong-mode
	live start got far enough to connect a socket or place an order. That
	is the exact funds-safety failure E2 exists to make impossible.
	"""


@pytest.fixture
def network_spies(monkeypatch: pytest.MonkeyPatch):
	"""Spy the two real side-effect seams in run_engine's downstream:

	  * ``websockets.connect`` (the WS connect at engine.py:_ws_loop) and
	    ``run_recovery`` (the first REST network, called before the WS
	    loop) — proves "abort BEFORE WS connect / before any network".
	  * ``PaperExecutor.place`` / ``LiveExecutor.place`` — proves "abort
	    BEFORE any executor.place".

	Each spy raises ``_NetworkTouched`` instead of doing real I/O, so a
	bypassed gate fails LOUD rather than hanging on a real socket.
	"""
	import edge_catcher.engine.engine as engmod

	def _boom_ws(*_a, **_kw):
		raise _NetworkTouched("websockets.connect reached on the abort path")

	async def _boom_recovery(*_a, **_kw):
		raise _NetworkTouched("run_recovery (REST network) reached on the abort path")

	monkeypatch.setattr(engmod.websockets, "connect", _boom_ws)
	monkeypatch.setattr(engmod, "run_recovery", _boom_recovery)

	from edge_catcher.engine.executors.paper import PaperExecutor

	async def _boom_place(self, *_a, **_kw):
		raise _NetworkTouched("executor.place reached on the abort path")

	monkeypatch.setattr(PaperExecutor, "place", _boom_place)
	try:
		from edge_catcher.engine.executors.live import LiveExecutor

		monkeypatch.setattr(LiveExecutor, "place", _boom_place)
	except ImportError:  # pragma: no cover - live executor always present in E
		pass
	return monkeypatch


# ---------------------------------------------------------------------------
# The NORMATIVE abort-matrix (§2 acceptance test)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("broken", ["executor", "db", "creds", "channel", "caps"])
def test_coherence_invariant_aborts_before_network(
	broken: str,
	tmp_path: Path,
	monkeypatch: pytest.MonkeyPatch,
	network_spies,
) -> None:
	"""Declared live config with EXACTLY one of the five §2 dimensions
	mismatched → run_engine raises a precise RuntimeError naming the
	failed coherence check, BEFORE any WS connect / REST / executor.place.

	This is the end-to-end §2 acceptance criterion: the abort is the REAL
	run_engine abort (not the helper in isolation), and the network spies
	prove zero side effects on the abort path.
	"""
	from edge_catcher.engine.engine import run_engine

	cfg = make_live_cfg(tmp_path, monkeypatch)
	break_field(cfg, broken, tmp_path)
	cfg_path = _write_cfg(cfg, tmp_path)

	with pytest.raises(RuntimeError, match="coherence") as excinfo:
		asyncio.run(run_engine(config_path=cfg_path))

	# The error must name WHICH dimension failed (operator-actionable).
	assert broken in str(excinfo.value).lower() or {
		"executor": "executor",
		"db": "db",
		"creds": "cred",
		"channel": "channel",
		"caps": "cap",
	}[broken] in str(excinfo.value).lower(), (
		f"RuntimeError must name the failed check; got: {excinfo.value!r}"
	)


def test_all_coherent_live_cfg_passes_the_invariant(
	tmp_path: Path,
	monkeypatch: pytest.MonkeyPatch,
) -> None:
	"""A fully-coherent live config PASSES the coherence step (§2: the
	all-coherent live config must not raise at the coherence step).

	Tested directly against _assert_mode_coherence so this asserts the
	coherence contract precisely without depending on E3's downstream
	composition (which is not wired in E2). The end-to-end abort proof
	above already covers "the invariant is wired into run_engine".
	"""
	from edge_catcher.engine.engine import _assert_mode_coherence

	cfg = make_live_cfg(tmp_path, monkeypatch)
	# Must NOT raise — every one of the five dimensions is coherent.
	_assert_mode_coherence(cfg)


@pytest.mark.parametrize("broken", ["executor", "db", "creds", "channel", "caps"])
def test_assert_mode_coherence_rejects_each_live_mismatch(
	broken: str,
	tmp_path: Path,
	monkeypatch: pytest.MonkeyPatch,
) -> None:
	"""Unit-level mirror of the abort-matrix directly on the helper: each
	single-dimension live mismatch raises RuntimeError(match="coherence").
	Read-only — calling it has no side effects and touches no network.
	"""
	from edge_catcher.engine.engine import _assert_mode_coherence

	cfg = make_live_cfg(tmp_path, monkeypatch)
	break_field(cfg, broken, tmp_path)
	with pytest.raises(RuntimeError, match="coherence"):
		_assert_mode_coherence(cfg)


def test_coherent_paper_cfg_passes_with_zero_live_checks(
	tmp_path: Path,
) -> None:
	"""§9 G-parity: a coherent paper config passes the invariant cleanly.
	Checks 3/4/5 are live-only and skipped; check 2 = paper ⟺
	paper_trades*.db. No creds/channels/caps required for paper — the
	invariant must not perturb the paper path.
	"""
	from edge_catcher.engine.engine import _assert_mode_coherence

	# No monkeypatched creds, no notifications.yaml, no risk: block —
	# proves the live-only checks are genuinely skipped for paper.
	_assert_mode_coherence(make_paper_cfg(tmp_path))


def test_paper_cfg_with_live_db_path_is_rejected(tmp_path: Path) -> None:
	"""The paper-side mirror of check 2: executor: paper but a
	live_trades*.db path → reject (a paper run must never touch the
	real-money DB). Symmetric fail-closed."""
	from edge_catcher.engine.engine import _assert_mode_coherence

	cfg = make_paper_cfg(tmp_path)
	cfg["db_path"] = str(tmp_path / "live_trades.db")
	with pytest.raises(RuntimeError, match="coherence"):
		_assert_mode_coherence(cfg)


def test_unknown_executor_value_is_rejected(tmp_path: Path) -> None:
	"""An `executor:` value that is neither `live` nor `paper` is itself a
	coherence failure — fail closed rather than defaulting to a mode."""
	from edge_catcher.engine.engine import _assert_mode_coherence

	cfg = make_paper_cfg(tmp_path)
	cfg["executor"] = "wat"
	with pytest.raises(RuntimeError, match="coherence"):
		_assert_mode_coherence(cfg)


# ===========================================================================
# G1 — live boot wires the provider + exec_cfg + bankroll refresh task
#
# These extend the §2 coherence harness to the §6 composition: a coherent live
# boot must (1) stash a typed ExecCfg in config["_exec_cfg"] (the live
# _handle_enter consumer reads it), (2) thread a RiskContextProvider into
# _ws_loop alongside the gate, and (3) start a "bankroll_refresh" task. Paper
# must do NONE of this (G-parity — risk=None, risk_ctx_provider=None reach
# _ws_loop, no _exec_cfg, no refresh task).
# ===========================================================================


@pytest.fixture
def _g1_boot_spies(monkeypatch: pytest.MonkeyPatch):
	"""Drive ``run_engine`` to the post-composition ``_ws_loop`` seam and
	capture what G1 wires: the ``_ws_loop`` ``risk``/``risk_ctx_provider``
	kwargs, every ``create_task`` name, and (after boot) ``config['_exec_cfg']``.

	Mirrors ``test_live_composition_root._compose_spies``: ``run_recovery`` is a
	no-op; the live-only B helpers are no-op-stubbed at their lazy-import source;
	``build_risk_module`` / ``KalshiOrderClient`` are faked (no Kalshi);
	``discover_strategies`` returns an inert stub so step-2 passes. ``_ws_loop``
	is stubbed to RECORD its kwargs then raise a ``BaseException`` sentinel
	(escapes the ``while True`` ``except Exception`` reconnect-forever catch).
	"""
	captured: dict = {
		"ws_loop_kwargs": None,
		"task_names": [],
		"refresh_loop_call": None,  # {bankroll, interval, warn_after} once started
		"built_gate": None,         # gate returned by _fake_build_risk (for identity)
	}

	import edge_catcher.engine.engine as engmod

	class _ComposeDone(BaseException):
		pass

	async def _noop_recovery(*_a, **_kw):
		return None

	monkeypatch.setattr(engmod, "run_recovery", _noop_recovery)

	async def _spy_ws_loop(*_a, **kw):
		captured["ws_loop_kwargs"] = kw
		raise _ComposeDone("captured _ws_loop kwargs — stop at the WS seam")

	monkeypatch.setattr(engmod, "_ws_loop", _spy_ws_loop)

	import edge_catcher.live.reconciliation as _reconmod

	async def _noop_async(*_a, **_kw):
		return None

	async def _noop_startup_reconcile(*_a, **_kw):
		# Honour the real ``-> StartupReconcileReport`` contract — the live boot
		# now consumes the return for the reconcile-alert Discord fan-out; a
		# clean report = no notification (poll_pending_rows_loop still -> None).
		return _reconmod.StartupReconcileReport()

	monkeypatch.setattr(_reconmod, "startup_reconcile", _noop_startup_reconcile)
	monkeypatch.setattr(_reconmod, "poll_pending_rows_loop", _noop_async)

	# Inert stub strategy on a synthetic series so step-2 discovery passes
	# (mode-agnostic — identical paper/live, NOT the seam under test).
	from edge_catcher.engine.strategy_base import Signal, Strategy, TickContext

	class _StubStrategy(Strategy):
		name = "g1-mode-stub"
		supported_series = ["KXSTUB15M"]
		default_params: dict = {}

		def on_tick(self, ctx: TickContext) -> list[Signal]:
			return []

	monkeypatch.setattr(engmod, "discover_strategies", lambda: [_StubStrategy()])

	# Fake the live-only wiring helpers at their lazy-import source modules.
	import edge_catcher.engine.risk as _riskmod
	import edge_catcher.live.client as _clientmod

	async def _fake_build_risk(*_a, **_kw):
		from unittest.mock import MagicMock

		gate = MagicMock(name="Gate")
		gate._bankroll = MagicMock(name="BankrollCache")
		captured["built_gate"] = gate
		return gate

	monkeypatch.setattr(_riskmod, "build_risk_module", _fake_build_risk)

	class _FakeKalshiClient:
		def __init__(self, *a, **kw):
			pass

		async def close(self):
			return None

	monkeypatch.setattr(_clientmod, "KalshiOrderClient", _FakeKalshiClient)

	# bankroll_refresh_loop must not actually sleep/refresh — replace with an
	# inert coroutine factory so the started task name is observable without I/O.
	# Args are captured by the factory wrapper (called synchronously before
	# create_task schedules the coro) so the live-boot assertion can verify
	# interval=ttl/2 and warn_after derivation without wall-clock waits.
	def _capturing_refresh_loop(bankroll, *, interval, warn_after):
		captured["refresh_loop_call"] = {
			"bankroll": bankroll,
			"interval": interval,
			"warn_after": warn_after,
		}

		async def _noop() -> None:
			return None

		return _noop()

	monkeypatch.setattr(engmod, "bankroll_refresh_loop", _capturing_refresh_loop)

	# Spy create_task to record every started task name.
	_orig_create_task = asyncio.create_task

	def _spy_create_task(coro, *, name=None):
		captured["task_names"].append(name)
		return _orig_create_task(coro, name=name)

	monkeypatch.setattr(engmod.asyncio, "create_task", _spy_create_task)

	captured["_ComposeDone"] = _ComposeDone
	return captured


def _g1_live_cfg(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict:
	"""A fully-coherent live cfg + the §8 execution: block + step-2 prereqs."""
	cfg = make_live_cfg(tmp_path, monkeypatch)
	cfg["execution"] = {
		"entry_slippage_cents": 2,
		"exit_slippage_cents": {
			"take_profit": 1, "stop_loss": 1, "time_exit": 1,
		},
	}
	cfg["sizing"] = {
		"risk_per_trade_cents": 500, "max_slippage_cents": 5, "min_fill": 1,
	}
	cfg["strategies"] = {
		"g1-mode-stub": {"enabled": True, "series": ["KXSTUB15M"]},
	}
	return cfg


def test_live_boot_wires_provider_exec_cfg_refresh_task(
	tmp_path: Path, monkeypatch: pytest.MonkeyPatch, _g1_boot_spies,
) -> None:
	"""Live boot: ``config['_exec_cfg']`` is a typed ``ExecCfg``; a
	``RiskContextProvider`` is threaded into ``_ws_loop`` alongside the gate; a
	task named ``bankroll_refresh`` is started. THE no-op-gate composition.
	"""
	from edge_catcher.engine.engine import run_engine
	from edge_catcher.engine.execution import ExecCfg
	from edge_catcher.engine.risk_context_provider import RiskContextProvider

	cfg = _g1_live_cfg(tmp_path, monkeypatch)
	cfg_path = _write_cfg(cfg, tmp_path)

	loaded: dict = {}
	# Capture the SAME config dict the engine threads downstream so we can
	# assert _exec_cfg landed on it (run_engine loads its own dict from YAML).
	import edge_catcher.engine.engine as engmod

	_orig_load = engmod.load_config

	def _capture_load(p):
		c = _orig_load(p)
		loaded["config"] = c
		return c

	monkeypatch.setattr(engmod, "load_config", _capture_load)

	with pytest.raises(_g1_boot_spies["_ComposeDone"]):
		asyncio.run(run_engine(config_path=cfg_path))

	# (1) The live config carries a typed ExecCfg for _handle_enter's live path.
	config = loaded["config"]
	assert isinstance(config.get("_exec_cfg"), ExecCfg), (
		"live boot must stash a typed ExecCfg in config['_exec_cfg'] — the live "
		f"_handle_enter reads it to build the sized order; got {config.get('_exec_cfg')!r}"
	)

	# (2) A RiskContextProvider AND the gate were threaded into _ws_loop.
	kw = _g1_boot_spies["ws_loop_kwargs"]
	assert kw is not None, "_ws_loop was never reached"
	assert kw.get("risk") is not None, (
		"live _ws_loop must receive the gate (risk=) — the no-op-gate fix"
	)
	assert isinstance(kw.get("risk_ctx_provider"), RiskContextProvider), (
		"live _ws_loop must receive a RiskContextProvider (risk_ctx_provider=) "
		f"so the gate sees per-signal context; got {kw.get('risk_ctx_provider')!r}"
	)

	# (3) The bankroll refresh task is running (live-only) AND its wiring args
	# are correct.  A broken interval=ttl/2 derivation or wrong warn_after would
	# leave the gate's bankroll perpetually stale (STALE_BANKROLL trips every
	# entry) or warn too late (after the kill threshold).
	names = [n for n in _g1_boot_spies["task_names"] if n]
	assert "bankroll_refresh" in names, (
		"live boot must start the 'bankroll_refresh' task (E1 loop) so the "
		f"bankroll cache stays fresh; started tasks={names!r}"
	)

	risk_cfg = cfg.get("risk", {})
	ttl = float(risk_cfg.get("bankroll_ttl_seconds", 300))
	failures_until_kill = int(risk_cfg.get("bankroll_failures_until_kill", 2))
	expected_interval = ttl / 2
	expected_warn_after = failures_until_kill - 1

	refresh_call = _g1_boot_spies["refresh_loop_call"]
	assert refresh_call is not None, (
		"bankroll_refresh_loop must have been called (task started) during live boot"
	)
	assert refresh_call["interval"] == expected_interval, (
		f"bankroll_refresh_loop interval must be ttl/2={expected_interval!r}; "
		f"got {refresh_call['interval']!r}"
	)
	assert refresh_call["warn_after"] == expected_warn_after, (
		f"bankroll_refresh_loop warn_after must be failures_until_kill-1="
		f"{expected_warn_after!r}; got {refresh_call['warn_after']!r}"
	)

	# Identity check: refresh loop must receive the gate's own bankroll cache,
	# not a copy or a different object.
	gate = _g1_boot_spies["built_gate"]
	assert gate is not None, "_fake_build_risk must have been called during live boot"
	assert refresh_call["bankroll"] is gate._bankroll, (
		"bankroll_refresh_loop must be started with the gate's own _bankroll "
		"cache (identity check); a different object would leave a stale backup "
		"while the gate reads a different (never-refreshed) one"
	)


def test_paper_boot_no_gate_no_provider_no_refresh_task(
	tmp_path: Path, _g1_boot_spies,
) -> None:
	"""G-parity: paper boot threads ``risk=None`` AND ``risk_ctx_provider=None``
	into ``_ws_loop``, leaves NO ``_exec_cfg`` on the config, and starts NO
	``bankroll_refresh`` task. Byte-exact with pre-G1 paper.
	"""
	from edge_catcher.engine.engine import run_engine

	cfg = make_paper_cfg(tmp_path)
	cfg["sizing"] = {
		"risk_per_trade_cents": 500, "max_slippage_cents": 5, "min_fill": 1,
	}
	cfg["strategies"] = {
		"g1-mode-stub": {"enabled": True, "series": ["KXSTUB15M"]},
	}
	cfg_path = _write_cfg(cfg, tmp_path)

	loaded: dict = {}
	import edge_catcher.engine.engine as engmod

	_orig_load = engmod.load_config

	def _capture_load(p):
		c = _orig_load(p)
		loaded["config"] = c
		return c

	import unittest.mock as _mock
	with _mock.patch.object(engmod, "load_config", _capture_load):
		with pytest.raises(_g1_boot_spies["_ComposeDone"]):
			asyncio.run(run_engine(config_path=cfg_path))

	# Paper config must NOT carry _exec_cfg (live-only — keeps _handle_enter on
	# the byte-exact allowed_size=None paper path).
	config = loaded["config"]
	assert "_exec_cfg" not in config, (
		"paper config must NOT carry _exec_cfg (live-only; its presence would "
		"divert _handle_enter off the byte-exact paper path)"
	)

	# risk AND risk_ctx_provider reach _ws_loop as None (G-parity).
	kw = _g1_boot_spies["ws_loop_kwargs"]
	assert kw is not None, "_ws_loop was never reached"
	assert kw.get("risk") is None, "paper _ws_loop must pass risk=None (G-parity)"
	assert kw.get("risk_ctx_provider") is None, (
		"paper _ws_loop must pass risk_ctx_provider=None (G-parity)"
	)

	# No bankroll_refresh task (live-only).
	names = [n for n in _g1_boot_spies["task_names"] if n]
	assert "bankroll_refresh" not in names, (
		f"paper must NOT start the bankroll_refresh task (live-only); tasks={names!r}"
	)
