"""E3 — mode-driven composition root + SC-D3 exit-via-executor + signing-env
single-source tests (sub-project E, Phase E3 — the keystone-completion task).

Three bundled obligations (spec §1/§2/§3/§6 + §10 SC-D3):

* **Obl 3 (Minor#1):** the live trade-scope signing env-var names
  (``KALSHI_LIVE_KEY_ID`` / ``KALSHI_LIVE_PRIVATE_KEY``) are single-sourced as
  canonical constants in ``adapters/kalshi/auth.py`` and imported by BOTH the
  E2 coherence gate (``engine.py``) AND the shipped ``live/client.py`` signer —
  the gate exists to catch signer/config drift, so they MUST share one source.

* **Obl 1 (composition root — §1/§3/§6):** at the ``run_engine`` composition
  root, AFTER E2's coherence gate, branch ONCE on ``config["executor"]``:
  ``live`` ⇒ ``LiveExecutor`` (built from B's ``KalshiOrderClient`` via A's
  ``live/config.py``) + ``SQLiteTradeStore`` + ``await build_risk_module`` +
  ``validate_exec_cfg(config["execution"])`` + start B's reconciler/poller
  background tasks; ``paper`` ⇒ ``PaperExecutor`` + paper ``TradeStore`` + NONE
  of B's tasks (byte-exact today). ONE branch at the root; everything
  downstream stays mode-agnostic (§1 — no ``isinstance``/mode branch leaked).

* **Obl 2 (SC-D3 — §10/§3/§1):** ``PaperExecutor.place`` grows a sell/exit
  path; dispatch ``_handle_exit`` threads ``executor``/``config`` and routes
  the exit via ``executor.place(exit_req)``; the AUTHORITATIVE close is B's
  async ``on_fill_event``/reconciler (started by Obl 1 in live mode). PAPER
  exit stays BYTE-EXACT vs today's ``store.exit_trade(trade_id, ctx_bid)``
  (THE G-parity risk — mandatory K2 11/11).

Harness mirrors ``tests/test_live_engine_mode_invariant.py`` (E2 cfg builders /
network spies) + ``tests/test_live_exit_settlement_routing.py`` (the paper-
byte-exact oracle + the §1 structural AST guards). Run from the project venv
(``.venv/Scripts/python.exe``).
"""
from __future__ import annotations

import ast
import asyncio
import inspect
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from edge_catcher.engine import dispatch as _dispatch_mod
from edge_catcher.engine import engine as _engine_mod
from edge_catcher.engine.dispatch import _handle_exit
from edge_catcher.engine.executor import OrderRequest
from edge_catcher.engine.strategy_base import Signal, Strategy, TickContext
from edge_catcher.engine.trade_store import InMemoryTradeStore, TradeStore


# A framework-only stub strategy on a synthetic series (NOT a real/private
# strategy — keeps this TRACKED test free of any strategy-logic leakage; same
# pattern as ``tests/test_engine.py::StubStrategy``). The Obl-1 composition
# tests drive the REAL ``run_engine`` PAST composition to the network-abort
# seam, so the boot must clear the mode-AGNOSTIC step-2 strategy-discovery
# gate (``get_enabled_strategies`` → ``validate_sizing_config`` + a non-empty
# enabled set). That gate is identical paper-vs-live (NOT the seam under
# test); patching ``discover_strategies`` to this stub + a coherent ``sizing:``
# block lets BOTH modes reach ``_ComposeDone`` so the composition spies can be
# asserted. It never trades (the engine aborts at ``run_recovery`` first).
_COMPOSE_STUB_SERIES = "KXSTUB15M"


class _ComposeStubStrategy(Strategy):
	"""Inert framework stub — discovered + enabled so step-2 passes; the
	engine aborts at the network seam before any on_tick fires."""

	name = "compose-stub"
	supported_series = [_COMPOSE_STUB_SERIES]
	default_params: dict = {}

	def on_tick(self, ctx: TickContext) -> list[Signal]:
		return []

# Reuse E2's fully-coherent cfg builders + network spies verbatim so the two
# composition-root test rigs stay in lock-step (the established idiom).
from tests.test_live_engine_mode_invariant import (  # noqa: E402
	_write_cfg,
	make_live_cfg,
	make_paper_cfg,
	network_spies,  # noqa: F401 — pytest fixture, used by name
)

_NOW = datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc)
_LATER = datetime(2026, 5, 18, 12, 5, 0, tzinfo=timezone.utc)


# ===========================================================================
# Obligation 3 — live signing env-var names are single-sourced in auth.py
# ===========================================================================


def test_live_signing_env_constants_single_sourced_in_auth() -> None:
	"""Failure mode prevented (config/signer drift — the exact drift E2's
	coherence gate exists to catch): the E2 gate and the live ``client.py``
	signer carry SEPARATE hard-coded ``KALSHI_LIVE_*`` string literals, so a
	rename in one silently bypasses the gate (a leaked read-only key could
	place orders, or live boot 401s with the gate falsely green).

	Asserts both reference the SAME canonical ``auth`` module constants
	(identity, not just equal-string)."""
	from edge_catcher.adapters.kalshi import auth as _auth
	from edge_catcher.engine import engine as _eng

	# Canonical constants exist on auth.py (the module that already owns the
	# read-only KALSHI_KEY_ID / KALSHI_PRIVATE_KEY defaults).
	assert _auth.KALSHI_LIVE_KEY_ID_ENV == "KALSHI_LIVE_KEY_ID"
	assert _auth.KALSHI_LIVE_PRIVATE_KEY_ENV == "KALSHI_LIVE_PRIVATE_KEY"

	# The E2 gate's module-level defaults ARE the auth constants (identity).
	assert _eng._DEFAULT_LIVE_KEY_ID_ENV is _auth.KALSHI_LIVE_KEY_ID_ENV, (
		"engine.py's live-key-id default must BE auth.KALSHI_LIVE_KEY_ID_ENV "
		"(single source — the gate catches signer drift only if they share it)"
	)
	assert _eng._DEFAULT_LIVE_PRIVATE_KEY_ENV is _auth.KALSHI_LIVE_PRIVATE_KEY_ENV


def test_client_signer_uses_auth_live_constants_not_literals() -> None:
	"""Failure mode prevented: ``live/client.py``'s ``_request`` signer still
	hard-codes the ``KALSHI_LIVE_*`` literals (lines ~372-373), so it is NOT
	single-sourced with the gate. Asserts the client module imports the auth
	constants AND the literal strings no longer appear as bare constants in
	``_request``'s source."""
	from edge_catcher.adapters.kalshi import auth as _auth
	from edge_catcher.live import client as _client

	# The client module resolves the canonical auth constants (imported or
	# attribute-referenced) — same object identity as auth's.
	assert getattr(_client, "KALSHI_LIVE_KEY_ID_ENV", None) is _auth.KALSHI_LIVE_KEY_ID_ENV or any(
		v is _auth.KALSHI_LIVE_KEY_ID_ENV for v in vars(_client).values()
	), "live/client.py must reference auth.KALSHI_LIVE_KEY_ID_ENV (single source)"

	# The signer no longer carries the bare string literals — they were
	# replaced by the imported constant. Scan _request's AST for the literals
	# (dedent — a method's source carries class-body indentation).
	import textwrap

	src = textwrap.dedent(inspect.getsource(_client.KalshiOrderClient._request))
	tree = ast.parse(src)
	literals = {
		n.value
		for n in ast.walk(tree)
		if isinstance(n, ast.Constant) and isinstance(n.value, str)
	}
	assert "KALSHI_LIVE_KEY_ID" not in literals, (
		"_request must NOT hard-code the 'KALSHI_LIVE_KEY_ID' literal — use "
		"the canonical auth constant so gate & signer cannot drift"
	)
	assert "KALSHI_LIVE_PRIVATE_KEY" not in literals, (
		"_request must NOT hard-code the 'KALSHI_LIVE_PRIVATE_KEY' literal"
	)


# ===========================================================================
# Obligation 1 — mode-driven composition root (ONE branch at run_engine)
# ===========================================================================


@pytest.fixture
def _compose_spies(monkeypatch: pytest.MonkeyPatch):
	"""Spy the composition seams so we observe WHICH components run_engine
	wires WITHOUT real network. The abort seam is ``_ws_loop`` — the
	post-composition boundary reached AFTER §6 boot steps 4
	(startup_reconcile) and 5 (B's reconciler/poller task) so those are
	observable, but BEFORE any real WS/auth/network (``_ws_loop`` itself calls
	the real ``make_auth_headers()`` + ``websockets.connect``). NOT
	``run_recovery`` (that is BEFORE steps 4/5 — aborting there would never see
	the B-task wiring this fixture must assert). ``run_recovery`` is instead a
	harmless no-op so the boot proceeds through steps 4/5 without real REST;
	the live-only ``startup_reconcile`` / ``poll_pending_rows_loop`` are
	no-op-stubbed at their lazy-import source (``edge_catcher.live
	.reconciliation``) so the test isolates the COMPOSITION decision (which
	components/tasks were wired) without running B's reconciliation logic
	against mocks.

	Captures: the constructed executor class, the constructed store class, the
	background task names started, and whether build_risk_module /
	validate_exec_cfg were called.
	"""
	captured: dict = {
		"executor_cls": None,
		"store_cls": None,
		"task_names": [],
		"build_risk_called": False,
		"validate_exec_called": False,
		"kalshi_client_built": False,
	}

	import edge_catcher.engine.engine as engmod

	# --- Abort at ``_ws_loop``: the clean post-composition seam. ``run_engine``
	# creates the §6 step-5 reconciler/poller task (``asyncio.create_task`` at
	# the `live_runtime is not None` block) BEFORE the ``while True`` that
	# ``await``s ``_ws_loop`` — so stubbing ``_ws_loop`` to raise lets the
	# create_task spy observe step-5's task NAME while entering NO real
	# WS/auth/network code (``_ws_loop`` itself calls the real
	# ``make_auth_headers()`` + real ``websockets.connect`` — both must be
	# bypassed, not just connect; aborting INSIDE connect would still execute
	# the real ``make_auth_headers()`` that precedes it and, with only the
	# trade-scope creds in env, raise/stall before the seam). run_recovery
	# (called BEFORE steps 4/5) is a harmless no-op so the boot reaches here.
	#
	# CRITICAL: ``_ComposeDone`` MUST extend ``BaseException`` (NOT
	# ``Exception``). The ``run_engine`` WS ``while True`` wraps ``_ws_loop``
	# in a broad ``except Exception:`` that LOGS + ``asyncio.sleep(
	# reconnect_delay)`` + RETRIES forever (engine.py:1172 — by design: the
	# live engine must survive a transient WS error). An ``Exception``-derived
	# sentinel would be swallowed there and the test would HANG in the
	# 30s-reconnect loop. ``BaseException`` (like ``asyncio.CancelledError`` /
	# ``KeyboardInterrupt``, which the loop deliberately lets through for the
	# same reason) escapes that catch-all and propagates cleanly out of
	# ``run_engine`` to ``pytest.raises``.
	class _ComposeDone(BaseException):
		pass

	async def _noop_recovery(*_a, **_kw):
		return None

	monkeypatch.setattr(engmod, "run_recovery", _noop_recovery)

	async def _boom_ws_loop(*_a, **_kw):
		raise _ComposeDone("composition + §6 steps 4/5 complete — stop at _ws_loop")

	monkeypatch.setattr(engmod, "_ws_loop", _boom_ws_loop)

	# Live-only §6 step-4/5 helpers are lazy-imported from
	# edge_catcher.live.reconciliation at call time — no-op-stub them THERE so
	# composition is isolated (the create_task spy still records step-5's task
	# NAME; the stub coro just returns immediately when that task is awaited).
	import edge_catcher.live.reconciliation as _reconmod

	async def _noop_startup_reconcile(*_a, **_kw):
		return None

	async def _noop_poll_pending_rows_loop(*_a, **_kw):
		return None

	monkeypatch.setattr(
		_reconmod, "startup_reconcile", _noop_startup_reconcile
	)
	monkeypatch.setattr(
		_reconmod, "poll_pending_rows_loop", _noop_poll_pending_rows_loop
	)

	# Step-2 strategy discovery is mode-AGNOSTIC (identical paper/live — NOT
	# the seam under test); patch it to the inert framework stub so both
	# modes clear the non-empty-enabled-set + validate_sizing_config gate and
	# reach _ComposeDone with the composition spies populated. (run_engine
	# binds discover_strategies via `from .discovery import` — patch the name
	# in the engine module's own namespace, the established idiom.)
	monkeypatch.setattr(
		engmod, "discover_strategies", lambda: [_ComposeStubStrategy()]
	)

	# --- Spy executor constructors.
	from edge_catcher.engine.executors.paper import PaperExecutor

	_orig_paper_init = PaperExecutor.__init__

	def _paper_init(self, *a, **kw):
		captured["executor_cls"] = "PaperExecutor"
		_orig_paper_init(self, *a, **kw)

	monkeypatch.setattr(PaperExecutor, "__init__", _paper_init)

	from edge_catcher.engine.executors.live import LiveExecutor

	_orig_live_init = LiveExecutor.__init__

	def _live_init(self, *a, **kw):
		captured["executor_cls"] = "LiveExecutor"
		_orig_live_init(self, *a, **kw)

	monkeypatch.setattr(LiveExecutor, "__init__", _live_init)

	# --- Spy store constructors.
	_orig_ts_init = engmod.TradeStore.__init__

	def _ts_init(self, *a, **kw):
		captured["store_cls"] = "TradeStore"
		_orig_ts_init(self, *a, **kw)

	monkeypatch.setattr(engmod.TradeStore, "__init__", _ts_init)

	from edge_catcher.live.store import SQLiteTradeStore

	_orig_sqlite_init = SQLiteTradeStore.__init__

	def _sqlite_init(self, *a, **kw):
		captured["store_cls"] = "SQLiteTradeStore"
		_orig_sqlite_init(self, *a, **kw)

	monkeypatch.setattr(SQLiteTradeStore, "__init__", _sqlite_init)

	# --- Spy asyncio.create_task to record EVERY task name started.
	_orig_create_task = asyncio.create_task

	def _spy_create_task(coro, *, name=None):
		captured["task_names"].append(name)
		return _orig_create_task(coro, name=name)

	monkeypatch.setattr(engmod.asyncio, "create_task", _spy_create_task)

	# --- Spy the live-only wiring helpers (build_risk_module / validate_exec_cfg
	# / KalshiOrderClient). `_compose_live` resolves these via LAZY
	# `from edge_catcher.<...> import <name>` statements (the established
	# paper-only-import-resilience pattern), so the bound name lives in each
	# SOURCE module's namespace at call time — patch THERE (not engmod, which
	# never holds these names; a stale engmod patch would no-op and let the
	# REAL build_risk_module hit Kalshi).
	import edge_catcher.engine.risk as _riskmod
	import edge_catcher.engine.execution as _execmod
	import edge_catcher.live.client as _clientmod

	async def _fake_build_risk(*_a, **_kw):
		captured["build_risk_called"] = True
		return MagicMock(name="Gate")

	monkeypatch.setattr(_riskmod, "build_risk_module", _fake_build_risk)

	def _fake_validate_exec(cfg):
		captured["validate_exec_called"] = True
		return MagicMock(name="ExecCfg")

	monkeypatch.setattr(_execmod, "validate_exec_cfg", _fake_validate_exec)

	class _FakeKalshiClient:
		def __init__(self, *a, **kw):
			captured["kalshi_client_built"] = True

		async def close(self):
			return None

	monkeypatch.setattr(_clientmod, "KalshiOrderClient", _FakeKalshiClient)

	captured["_ComposeDone"] = _ComposeDone
	return captured


def test_live_cfg_composes_live_executor_sqlite_store_and_b_tasks(
	tmp_path: Path, monkeypatch: pytest.MonkeyPatch, _compose_spies,
) -> None:
	"""Failure mode prevented (live trading is NOT real): ``executor: live``
	still hard-defaults to ``PaperExecutor`` + the paper store and never
	starts B's async lifecycle — real-money signals would resolve as
	paper fills with no reconciler, OR the live store/risk gate is never
	constructed. THE keystone-completion assertion.

	A fully-coherent live cfg → ``run_engine`` constructs ``LiveExecutor`` +
	``SQLiteTradeStore`` + ``await build_risk_module`` + ``validate_exec_cfg``
	+ starts B's reconciler/poller background task(s)."""
	from edge_catcher.engine.engine import run_engine

	cfg = make_live_cfg(tmp_path, monkeypatch)
	# §8: the execution: block lives in the live-trader.yaml the engine loads.
	cfg["execution"] = {
		"entry_slippage_cents": 2,
		"exit_slippage_cents": {
			"take_profit": 1,
			"stop_loss": 1,
			"time_exit": 1,
		},
	}
	# Mode-agnostic step-2 boot prerequisites (NOT the seam under test —
	# identical paper/live): a coherent sizing: block + the stub enabled on
	# its synthetic series so get_enabled_strategies returns a non-empty set
	# and the boot proceeds THROUGH composition to the _ComposeDone seam.
	cfg["sizing"] = {
		"risk_per_trade_cents": 500, "max_slippage_cents": 5, "min_fill": 1,
	}
	cfg["strategies"] = {
		_ComposeStubStrategy.name: {
			"enabled": True, "series": [_COMPOSE_STUB_SERIES],
		}
	}
	cfg_path = _write_cfg(cfg, tmp_path)

	with pytest.raises(_compose_spies["_ComposeDone"]):
		asyncio.run(run_engine(config_path=cfg_path))

	assert _compose_spies["executor_cls"] == "LiveExecutor", (
		"executor: live MUST construct LiveExecutor (not the PaperExecutor "
		f"hard-default); got {_compose_spies['executor_cls']!r}"
	)
	assert _compose_spies["store_cls"] == "SQLiteTradeStore", (
		"executor: live MUST construct the live SQLiteTradeStore; got "
		f"{_compose_spies['store_cls']!r}"
	)
	assert _compose_spies["kalshi_client_built"], (
		"LiveExecutor must be built from a KalshiOrderClient (A's live config)"
	)
	assert _compose_spies["build_risk_called"], (
		"live boot must await build_risk_module (§6 step 3 — the risk gate)"
	)
	assert _compose_spies["validate_exec_called"], (
		"live boot must validate_exec_cfg(config['execution']) at T0 (§6)"
	)
	# B's reconciler/phantom-pending poller must be among the started tasks.
	names = [n for n in _compose_spies["task_names"] if n]
	assert any("reconcil" in n or "pending" in n for n in names), (
		"live boot must start B's reconciler / phantom-pending poller "
		f"background task; started tasks={names!r}"
	)


def test_paper_cfg_composes_paper_executor_paper_store_and_no_b_tasks(
	tmp_path: Path, _compose_spies,
) -> None:
	"""Failure mode prevented (G-parity BLOCKING): the composition branch
	perturbs the paper path — paper no longer gets ``PaperExecutor`` + the
	paper ``TradeStore``, or it starts B's live-only async tasks (which would
	change paper behaviour / touch a live DB). ``executor: paper`` MUST be
	byte-exact today: ``PaperExecutor`` + paper ``TradeStore`` + NONE of B's
	reconciler/poller tasks."""
	from edge_catcher.engine.engine import run_engine

	cfg = make_paper_cfg(tmp_path)
	# Same mode-agnostic step-2 prerequisites as the live case (identical
	# paper/live — NOT the seam under test) so the paper boot likewise
	# proceeds THROUGH composition to _ComposeDone.
	cfg["sizing"] = {
		"risk_per_trade_cents": 500, "max_slippage_cents": 5, "min_fill": 1,
	}
	cfg["strategies"] = {
		_ComposeStubStrategy.name: {
			"enabled": True, "series": [_COMPOSE_STUB_SERIES],
		}
	}
	cfg_path = _write_cfg(cfg, tmp_path)

	with pytest.raises(_compose_spies["_ComposeDone"]):
		asyncio.run(run_engine(config_path=cfg_path))

	assert _compose_spies["executor_cls"] == "PaperExecutor"
	assert _compose_spies["store_cls"] == "TradeStore"
	assert not _compose_spies["build_risk_called"], (
		"paper must NOT build the risk module (live-only — §6)"
	)
	assert not _compose_spies["validate_exec_called"], (
		"paper must NOT validate_exec_cfg (live-only — §6)"
	)
	assert not _compose_spies["kalshi_client_built"], (
		"paper must NOT construct a KalshiOrderClient (live-only)"
	)
	names = [n for n in _compose_spies["task_names"] if n]
	assert not any("reconcil" in n or "pending" in n for n in names), (
		"paper must start NONE of B's reconciler/poller tasks (byte-exact "
		f"today — §1/§9); started tasks={names!r}"
	)


def test_composition_branch_is_single_and_at_the_root() -> None:
	"""Failure mode prevented (§1 keystone violation): the live-vs-paper
	decision leaks into multiple sites / downstream of the composition root.
	The §1 keystone is that mode is decided ONCE at the composition root and
	the difference is WHICH components were wired, never a per-call branch.

	Asserts: (a) dispatch.py contains NO ``isinstance(executor, …)`` /
	mode-token If-test (mode-agnostic downstream); (b) ``run_engine``'s
	executor-mode branch keys on the ``config['executor']`` value exactly once
	(the single root branch)."""
	# (a) Downstream (dispatch.py) has no mode/isinstance discriminator.
	disp_src = inspect.getsource(_dispatch_mod)
	disp_tree = ast.parse(disp_src)
	_MODE_TOKENS = {
		"is_live", "executor_kind", "sqlitetradestore", "inmemorytradestore",
	}

	class _V(ast.NodeVisitor):
		isinstance_on_executor = False
		mode_if = False

		def visit_If(self, n: ast.If) -> None:
			for sub in ast.walk(n.test):
				if (
					isinstance(sub, ast.Call)
					and isinstance(sub.func, ast.Name)
					and sub.func.id == "isinstance"
					and sub.args
					and isinstance(sub.args[0], ast.Name)
					and sub.args[0].id == "executor"
				):
					self.isinstance_on_executor = True
				if isinstance(sub, ast.Name) and sub.id.lower() in _MODE_TOKENS:
					self.mode_if = True
				if (
					isinstance(sub, ast.Attribute)
					and sub.attr.lower() in _MODE_TOKENS
				):
					self.mode_if = True
			self.generic_visit(n)

	v = _V()
	v.visit(disp_tree)
	assert not v.isinstance_on_executor, (
		"dispatch.py must NOT contain isinstance(executor, …) — §1 keystone: "
		"the difference is WHICH executor/store was wired at boot, never a "
		"per-call branch in the mode-agnostic router"
	)
	assert not v.mode_if, (
		"dispatch.py must NOT branch on a mode/is_live/store-type token (§1)"
	)

	# (b) run_engine branches on config['executor'] exactly once.
	re_src = inspect.getsource(_engine_mod.run_engine)
	re_tree = ast.parse(re_src).body[0]
	executor_mode_ifs = 0
	for node in ast.walk(re_tree):
		if isinstance(node, ast.If):
			for sub in ast.walk(node.test):
				# `config.get("executor")` / `config["executor"]` / a local
				# `mode`/`executor_kind` bound from it — the single root branch.
				if (
					isinstance(sub, ast.Constant)
					and sub.value == "executor"
				):
					executor_mode_ifs += 1
					break
				if (
					isinstance(sub, ast.Name)
					and sub.id in ("mode", "executor_kind", "executor_mode")
				):
					executor_mode_ifs += 1
					break
	assert executor_mode_ifs == 1, (
		"run_engine must branch on the config 'executor' mode EXACTLY ONCE "
		f"(the single composition-root branch — §1/§3); found {executor_mode_ifs}"
	)


# ===========================================================================
# Obligation 2 — SC-D3: live exit via executor + PaperExecutor sell path,
# PAPER byte-EXACT vs today's store.exit_trade(trade_id, ctx_bid)
# ===========================================================================


def _exit_signal(trade_id: int, side: str = "yes") -> Signal:
	return Signal(
		action="exit",
		ticker="KXSOL15M-26MAY18H12",
		side=side,
		series="KXSOL15M",
		strategy="debut_fade",
		reason="e3-exit",
		trade_id=trade_id,
	)


def _ctx(yes_bid: int = 60, no_bid: int = 40) -> MagicMock:
	"""_handle_exit reads ctx.yes_bid / ctx.no_bid (selling hits the bid)."""
	return MagicMock(yes_bid=yes_bid, no_bid=no_bid)


def _run_exit(signal, ctx, store, *, now, executor, config) -> None:
	"""Drive the REAL async ``dispatch._handle_exit`` (SC-D3/E3: it awaits
	``executor.place`` for the exit order and takes executor/config)."""
	asyncio.run(
		_handle_exit(
			signal, ctx, store, now=now, executor=executor, config=config,
		)
	)


def _seed_paper_open_row(
	store: TradeStore | InMemoryTradeStore,
	*,
	side: str = "yes",
	entry: int = 42,
	fill_size: int = 10,
) -> int:
	return store.record_trade(
		ticker="KXSOL15M-26MAY18H12",
		entry_price=entry,
		strategy="debut_fade",
		side=side,
		series_ticker="KXSOL15M",
		intended_size=fill_size,
		fill_size=fill_size,
		blended_entry=entry,
		fill_pct=1.0,
		slippage_cents=0,
		now=_NOW,
	)


def test_paper_executor_has_sell_exit_path() -> None:
	"""Failure mode prevented (G-parity BLOCKING precondition): PaperExecutor
	is still entries-only, so routing an exit through executor.place runs
	entry-sizing on a paper exit (paper behaviour change). PaperExecutor.place
	must accept an exit (action='sell') OrderRequest and return a defined,
	non-entry-sized result so dispatch can resolve the paper exit
	synchronously (byte-exact, below)."""
	from edge_catcher.engine.executors.paper import PaperExecutor
	from edge_catcher.engine.market_state import MarketState

	ex = PaperExecutor(market_state=MarketState(), config={"sizing": {
		"risk_per_trade_cents": 200, "max_slippage_cents": 2, "min_fill": 3,
	}})
	exit_req = OrderRequest(
		ticker="KXSOL15M-26MAY18H12",
		series="KXSOL15M",
		side="yes",
		size_contracts=10,
		limit_price_cents=60,
		strategy="debut_fade",
		client_order_id="cid-exit-1",
		action="sell",
	)
	result = asyncio.run(ex.place(exit_req))
	# A defined OrderResult (never raises); the sell path must NOT run the
	# entry book-walk (which would FillSkip on the empty MarketState and
	# return 'rejected' with entry semantics). It is a paper-exit ACK whose
	# semantics let dispatch close at the ctx bid (byte-exact, next test).
	assert result is not None
	assert result.status in ("filled", "pending", "rejected")
	# The paper sell-path result must be the dedicated exit-ACK, NOT the
	# entry-sizing 'rejected'/'stale_book' shape (that would mean the
	# entries-only body ran on an exit).
	assert result.rejection_reason != "stale_book", (
		"PaperExecutor sell path must NOT fall through to the entry book-walk "
		"(empty MarketState → stale/empty book) — it must short-circuit as a "
		"paper-exit ACK before resolve_fill"
	)


@pytest.mark.parametrize("store_kind", ["paper_sqlite", "in_memory"])
def test_paper_exit_via_dispatch_is_byte_exact_vs_pre_e3(
	store_kind: str, tmp_path: Path,
) -> None:
	"""THE K2 G-parity assertion. Failure mode prevented (G-parity BLOCKING —
	K2 11/11): E3's executor-routed exit perturbs the paper/replay close
	(status/pnl/exit_price/timing). Drives the REAL ``dispatch._handle_exit``
	(now executor/config-threaded) against the paper ``TradeStore`` (SQLite)
	and the replay ``InMemoryTradeStore`` and asserts the close is EXACTLY the
	pre-E3 paper arithmetic — an INDEPENDENT oracle recomputed here from the
	public fee model (NOT read back from the store): paper ``exit_trade``
	pnl = fill_size*(exit - effective_entry) - entry_fee - exit_fee; status
	won iff pnl>0; exit_price = the ctx bid; exit_time = the threaded now."""
	from edge_catcher.engine.executors.paper import PaperExecutor
	from edge_catcher.engine.market_state import MarketState
	from edge_catcher.adapters.kalshi.fees import STANDARD_FEE

	if store_kind == "paper_sqlite":
		store: TradeStore | InMemoryTradeStore = TradeStore(
			tmp_path / "paper_trades.db"
		)
	else:
		store = InMemoryTradeStore()
	tid = _seed_paper_open_row(store, side="yes", entry=42, fill_size=10)

	# Independent pre-E3 oracle — recomputed, NOT read back.
	pre = store.get_trade_by_id(tid)
	assert pre is not None and pre["status"] == "open"
	entry_fee = int(pre["entry_fee_cents"])
	exit_fee = int(STANDARD_FEE.calculate(60, 10))
	expected_pnl = 10 * (60 - 42) - entry_fee - exit_fee
	expected_status = (
		"won" if expected_pnl > 0 else ("lost" if expected_pnl < 0 else "scratch")
	)

	# Drive the REAL dispatch exit handler WITH the executor/config threaded
	# (E3's SC-D3 deliverable). yes-side row → sells into ctx.yes_bid=60.
	paper_exec = PaperExecutor(market_state=MarketState(), config={})
	_run_exit(
		_exit_signal(tid, "yes"), _ctx(yes_bid=60), store, now=_LATER,
		executor=paper_exec, config={},
	)

	closed = store.get_trade_by_id(tid)
	assert closed is not None
	assert closed["status"] == expected_status
	assert closed["pnl_cents"] == expected_pnl, (
		f"paper exit pnl must be BYTE-EXACT pre-E3 arithmetic ({store_kind}); "
		f"expected {expected_pnl} got {closed['pnl_cents']}"
	)
	assert closed["exit_price"] == 60, "exit_price must be the ctx bid (60¢)"
	assert closed["exit_time"] == _LATER.isoformat(), (
		"exit_time must be the threaded `now` (byte-exact replay parity)"
	)
	# Paper schema has NO exit_reason column — proves this is the paper-shaped
	# UPDATE, NOT the live B-CAS path (which sets exit_reason='ws_exit_fill').
	assert "exit_reason" not in closed


def test_paper_exit_idempotent_double_close_still_noop(tmp_path: Path) -> None:
	"""Failure mode prevented (G-parity): E3's executor threading breaks
	paper's ``WHERE id=? AND status='open'`` idempotency — a duplicate
	dispatch exit overwrites the first close. A second close must STILL be a
	silent no-op leaving the first close intact (byte-exact with pre-E3)."""
	from edge_catcher.engine.executors.paper import PaperExecutor
	from edge_catcher.engine.market_state import MarketState

	store = TradeStore(tmp_path / "paper_trades.db")
	tid = _seed_paper_open_row(store, side="yes", entry=42, fill_size=10)
	paper_exec = PaperExecutor(market_state=MarketState(), config={})

	_run_exit(
		_exit_signal(tid, "yes"), _ctx(yes_bid=60), store, now=_NOW,
		executor=paper_exec, config={},
	)
	first = store.get_trade_by_id(tid)
	assert first is not None and first["status"] == "won"

	_run_exit(
		_exit_signal(tid, "yes"), _ctx(yes_bid=5), store, now=_LATER,
		executor=paper_exec, config={},
	)
	after = store.get_trade_by_id(tid)
	assert after == first, (
		"a second paper exit must be an idempotent no-op (WHERE status='open')"
		f" — row must stay the first close; first={first} after={after}"
	)


def test_live_exit_places_exit_via_executor_and_b_async_owns_close(
	tmp_path: Path,
) -> None:
	"""Failure mode prevented (SC-D3 funds-at-risk): in live mode a strategy/
	TP-SL exit does NOT place an exit order via the executor — the real-money
	position is never closed on Kalshi (only the local idempotent backstop
	runs). Asserts ``_handle_exit`` calls ``executor.place(exit_req)`` with an
	exit-shaped (action='sell') OrderRequest for the position, AND the
	idempotent C5 ``store.exit_trade`` backstop still runs (the
	non-authoritative race-safe close — B's async on_fill_event/reconciler is
	the AUTHORITY, started by Obl 1 in live mode)."""
	from edge_catcher.live.store import SQLiteTradeStore
	from edge_catcher.live.state import connect_live_trades_db  # noqa: F401

	store = SQLiteTradeStore(tmp_path / "live_trades.db")
	try:
		# Seed a realistic live OPEN row via the C1→C2 production flow.
		store.record_intent(
			ticker="KXSOL15M-26MAY18H12",
			series="KXSOL15M",
			strategy="debut_fade",
			side="yes",
			intended_size=5,
			entry_price_cents=5,
			stop_loss_distance_cents=3,
			client_order_id="cid-e3-live",
			placed_at_utc=_NOW.isoformat(),
		)
		tid = store.record_trade(
			ticker="KXSOL15M-26MAY18H12",
			entry_price=42,
			strategy="debut_fade",
			side="yes",
			series_ticker="KXSOL15M",
			intended_size=10,
			fill_size=10,
			blended_entry=42,
			fill_pct=1.0,
			slippage_cents=0,
			now=_NOW,
			client_order_id="cid-e3-live",
			kalshi_order_id="kx-e3-live",
		)

		placed: list[OrderRequest] = []

		async def _place(req: OrderRequest):
			placed.append(req)
			# LiveExecutor returns IOC; the AUTHORITATIVE close is B's async
			# on_fill_event/reconciler. Return a benign pending so dispatch
			# does not treat it as an entry fill.
			from edge_catcher.engine.executor import OrderResult

			return OrderResult(
				status="pending",
				intended_size=req.size_contracts,
				filled_size=0,
				blended_entry_cents=0,
				fill_pct=0.0,
				slippage_cents=0,
			)

		executor = MagicMock()
		executor.place = AsyncMock(side_effect=_place)

		# A minimal execution cfg covering the default exit kind so the exit
		# OrderRequest builder can resolve the slippage.
		exec_cfg = {
			"entry_slippage_cents": 2,
			"exit_slippage_cents": {
				"take_profit": 1, "stop_loss": 1, "time_exit": 1,
			},
		}
		_run_exit(
			_exit_signal(tid, "yes"), _ctx(yes_bid=60), store, now=_LATER,
			executor=executor, config={"execution": exec_cfg},
		)

		# (a) An exit order was placed via the executor.
		assert len(placed) == 1, (
			"live exit MUST place exactly one exit order via executor.place "
			f"(SC-D3 / spec §3 :534); placed={placed!r}"
		)
		req = placed[0]
		assert req.action == "sell", "the exit order must be a SELL (exit)"
		assert req.ticker == "KXSOL15M-26MAY18H12"
		assert req.side == "yes"
		assert req.size_contracts == 10, (
			"the exit order closes the full open position size"
		)

		# (b) The idempotent C5 store.exit_trade backstop still ran (the
		# non-authoritative race-safe close — B/Kalshi-truth is authority).
		conn = store._conn
		conn.row_factory = __import__("sqlite3").Row
		row = conn.execute(
			"SELECT status, exit_reason FROM live_trades WHERE id=?", (tid,)
		).fetchone()
		conn.row_factory = None
		assert row["status"] == "won", (
			"the C5 idempotent store.exit_trade backstop must still book the "
			"close (race-safe non-authoritative; B's async path is authority)"
		)
		assert row["exit_reason"] == "ws_exit_fill", (
			"C5 store.exit_trade routes to B's record_close CAS "
			"(exit_reason='ws_exit_fill') — the idempotent backstop"
		)
	finally:
		store.close()


def test_handle_exit_threads_executor_but_has_no_mode_branch() -> None:
	"""Failure mode prevented (§1 keystone violation): E3 threads executor/
	config into ``_handle_exit`` (SC-D3) but ALSO adds an
	``if isinstance(store, SQLiteTradeStore)`` / ``if is_live:`` mode branch
	to special-case the live close. SC-D3/§1: the executor is the seam —
	``_handle_exit`` stays mode-AGNOSTIC (paper's PaperExecutor resolves the
	exit → today's close; live's LiveExecutor returns IOC → B's async closes;
	the unconditional ``store.exit_trade`` is C5's idempotent backstop).

	Asserts ``_handle_exit`` now accepts executor/config params (SC-D3
	deliverable) AND structurally contains NO mode/isinstance/live/paper
	If-test (the legitimate ``signal.side``/``trade_id is None``/``blended``
	branches are not mode discriminators)."""
	sig = inspect.signature(_dispatch_mod._handle_exit)
	assert "executor" in sig.parameters, (
		"_handle_exit must now accept an `executor` param (SC-D3: route the "
		"live exit via executor.place — E3's deliverable)"
	)
	assert "config" in sig.parameters, (
		"_handle_exit must now accept a `config` param (SC-D3: the exit "
		"OrderRequest builder needs the execution: cfg)"
	)

	src = inspect.getsource(_dispatch_mod._handle_exit)
	tree = ast.parse(src).body[0]
	_MODE_TOKENS = {
		"is_live", "live", "paper", "mode", "executor_kind", "isinstance",
		"sqlitetradestore", "inmemorytradestore", "tradestore",
	}

	class _V(ast.NodeVisitor):
		hit = False

		def _scan(self, test: ast.AST) -> None:
			for sub in ast.walk(test):
				if isinstance(sub, ast.Name) and sub.id.lower() in _MODE_TOKENS:
					self.hit = True
				elif (
					isinstance(sub, ast.Attribute)
					and sub.attr.lower() in _MODE_TOKENS
				):
					self.hit = True
				elif (
					isinstance(sub, ast.Call)
					and isinstance(sub.func, ast.Name)
					and sub.func.id == "isinstance"
				):
					self.hit = True

		def visit_If(self, n: ast.If) -> None:
			self._scan(n.test)
			self.generic_visit(n)

		def visit_IfExp(self, n: ast.IfExp) -> None:
			self._scan(n.test)
			self.generic_visit(n)

	v = _V()
	v.visit(tree)
	assert not v.hit, (
		"_handle_exit must contain NO isinstance/mode/is_live/store-type "
		"branch — §1 keystone: the executor is the live-vs-paper seam, the "
		"unconditional store.exit_trade is C5's idempotent backstop; E3 must "
		"NOT special-case the live close with a per-call mode branch"
	)
