"""G3 — `_handle_risk_event` → dedicated live risk channel, wired pre-gate
in §6 boot step-3 (sub-project E, Phase G3 / spec §6).

A ``RiskEvent`` is a kill-switch / auto-panic trip — the ONLY way a human
operator learns real-money trading has HALTED. It MUST reach the operator.
G3 replaces the placeholder ``_handle_risk_event`` (which only ``log.warning``s)
with a real ``send()`` to a **dedicated live risk channel**, resolved via the
SAME unified ``notifications.yaml`` / ``load_channels`` mechanism the §2.4
coherence gate already uses (spec §6: "channels resolved per §2.4's invariant
clause"; "the live channels resolvable (§2.4)" — plural).

Four obligations (spec §6):

* **(a) routing:** a ``RiskEvent`` trip routes a ``Notification`` carrying
  kind/reason/severity to the dedicated risk channel via the unified
  ``send()`` (sync — ``Gate.trip()`` and ``send()`` are both sync; the
  callback never schedules an async task the way G1's ``notify`` does).
* **(b) boot-time-trip:** the callback + dedicated channel are resolved /
  registered in §6 boot step-3 (inside ``_compose_live``) BEFORE any gate
  evaluation (reconcile in step-4, trading in step-5), so a trip occurring
  at/just-after risk-module construction still reaches the channel.
* **(c) fail-closed coherence:** ``executor=live`` with no/unresolvable
  ``notifications.live_risk_channel`` aborts boot with a precise error
  (additive to E2's Check-4 — the existing ``live_channel`` branch is
  untouched); the all-coherent live config passes.
* **(d) paper untouched:** paper composition NEVER registers
  ``_handle_risk_event`` / resolves a risk channel (live-only surface —
  K2 G-parity: the paper trade-row path is byte-unchanged).

Harness reuses E2's fully-coherent cfg builders / network spies and E3's
``_compose_spies`` rig (the established idiom). Run from the project venv
(``.venv/Scripts/python.exe``).
"""
from __future__ import annotations

import ast
import asyncio
import inspect
from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml

import edge_catcher.engine.engine as engmod
from edge_catcher.engine.risk import RiskEvent
from edge_catcher.notifications.envelope import DeliveryResult, Notification

# Reuse E2's fully-coherent cfg builders + network spies and E3's compose rig
# verbatim so all three rigs stay in lock-step (the established idiom).
from tests.test_live_engine_mode_invariant import (  # noqa: E402
	_write_cfg,
	make_live_cfg,
	make_paper_cfg,
)
from tests.test_live_composition_root import (  # noqa: E402,F401
	_COMPOSE_STUB_SERIES,
	_ComposeStubStrategy,
	_compose_spies,  # noqa: F401 — pytest fixture, used by name
)

_NOW = datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc)


class _SpyChannel:
	"""Records every Notification handed to it (the unified-layer adapter
	contract: ``send(Notification) -> DeliveryResult``)."""

	def __init__(self, name: str = "risk_spy") -> None:
		self.name = name
		self.sent: list[Notification] = []

	def send(self, notification: Notification) -> DeliveryResult:
		self.sent.append(notification)
		return DeliveryResult(channel_name=self.name, success=True, latency_ms=0.1)


def _risk_event(
	kind: str = "trip",
	reason: str = "KILL_AUTO_PANIC",
	detail: str = "equity 2500c ≤ absolute_panic_floor 3000c",
	severity: str = "error",
) -> RiskEvent:
	return RiskEvent(
		kind=kind,
		reason=reason,
		detail=detail,
		severity=severity,
		occurred_at=_NOW,
	)


def _add_risk_channel(cfg: dict, tmp_path: Path, name: str = "live_risk_discord") -> None:
	"""Add/override the dedicated risk channel to a DIFFERENTLY-named
	resolvable channel.

	NOTE: the shared E2 ``make_live_cfg`` already writes a default
	``live_risk_alerts`` risk channel + sets ``notifications.live_risk_
	channel`` (§2.4/§6 G3 made it a mandatory coherence dimension). This
	helper appends an additional resolvable channel and repoints
	``live_risk_channel`` at it — exercising a non-default risk-channel name
	(idempotent / harmless when the default already resolves)."""
	notify_path = Path(cfg["notifications"]["config_path"])
	raw = yaml.safe_load(notify_path.read_text(encoding="utf-8"))
	raw["channels"][name] = {
		"type": "file",
		"path": str(tmp_path / "risk_alerts.log"),
	}
	notify_path.write_text(yaml.safe_dump(raw), encoding="utf-8")
	cfg["notifications"]["live_risk_channel"] = name


# ===========================================================================
# (a) routing — a RiskEvent trip → Notification on the dedicated risk channel
# ===========================================================================


def test_handle_risk_event_sends_notification_to_dedicated_channel() -> None:
	"""Failure mode prevented (operator-safety): a kill-switch trip silently
	"goes nowhere" — the operator never learns real-money trading HALTED.

	With the dedicated risk channel bound (step-3 analog), ``_handle_risk_event``
	delivers a ``Notification`` carrying kind/reason/severity to that channel
	via the unified ``send()`` — SYNCHRONOUSLY (no event loop required: the
	Gate-callback contract is sync; ``send()`` is sync)."""
	spy = _SpyChannel()
	engmod._configure_risk_channel([spy])
	try:
		# Called with NO running event loop on purpose — proves it is a
		# direct sync send(), not a loop.create_task() like G1's notify.
		engmod._handle_risk_event(_risk_event())
	finally:
		engmod._configure_risk_channel([])

	assert len(spy.sent) == 1, (
		"a RiskEvent trip MUST deliver exactly one Notification to the "
		f"dedicated risk channel synchronously; got {spy.sent!r}"
	)
	note = spy.sent[0]
	assert isinstance(note, Notification)
	blob = f"{note.title} {note.body}".lower()
	# The operator-facing message must carry the actionable trip facts.
	assert "kill_auto_panic" in blob, f"reason missing from alert: {note!r}"
	assert "absolute_panic_floor" in blob or "2500c" in blob, (
		f"detail missing from alert: {note!r}"
	)
	assert "trip" in blob, f"kind missing from alert: {note!r}"
	# Severity must map RiskEvent.severity → Notification.severity (error).
	assert note.severity == "error", (
		f"RiskEvent severity must map onto the Notification; got {note.severity!r}"
	)


def test_handle_risk_event_never_raises_even_if_channel_unbound() -> None:
	"""Failure mode prevented: the Gate swallows callback exceptions
	(risk.py ~:944-948), so a raising ``_handle_risk_event`` would be
	silently lost AND could mask the trip path. With NO channel bound it
	must be a clean no-op (defensive — the §2.4 gate guarantees a live
	channel, this is the paper/unconfigured analog), never raising."""
	engmod._configure_risk_channel([])
	engmod._handle_risk_event(_risk_event())  # must not raise


def test_handle_risk_event_is_sync_not_async() -> None:
	"""Spec §6: ``Gate.trip()`` and ``send()`` are both sync — the callback
	must be a plain sync function (the Gate calls ``cb(event)`` directly,
	risk.py ~:944), NOT a coroutine function that would never be awaited."""
	assert not inspect.iscoroutinefunction(engmod._handle_risk_event), (
		"_handle_risk_event MUST stay sync — the Gate invokes it directly "
		"in trip() (sync); a coroutine would be created-but-never-awaited "
		"and the alert would silently never send"
	)


def test_handle_risk_event_no_longer_a_logging_only_placeholder() -> None:
	"""Failure mode prevented: G3 was a no-op — ``_handle_risk_event`` still
	only ``log.warning``s and never calls the unified ``send()``. Asserts the
	function body actually references the unified ``send`` (the routing wire),
	not just logging."""
	src = inspect.getsource(engmod._handle_risk_event)
	tree = ast.parse(src).body[0]
	calls_send = any(
		isinstance(n, ast.Call)
		and (
			(isinstance(n.func, ast.Name) and n.func.id == "send")
			or (isinstance(n.func, ast.Attribute) and n.func.attr == "send")
		)
		for n in ast.walk(tree)
	)
	assert calls_send, (
		"_handle_risk_event must call the unified notifications send() — it is "
		"no longer the logging-only placeholder (spec §6 G3 routing)"
	)


# ===========================================================================
# (b) boot-time-trip — channel + callback wired in step-3 BEFORE any gate eval
# ===========================================================================


def test_boot_time_trip_reaches_dedicated_channel_before_reconcile(
	tmp_path: Path, monkeypatch: pytest.MonkeyPatch, _compose_spies,  # noqa: F811 — pytest fixture injected by name (imported at module top for registration)
) -> None:
	"""THE operator-safety acceptance test. Failure mode prevented: the
	startup balance read is already ≤ ``absolute_panic_floor`` (Kalshi
	unreachable at boot ⇒ cache 0 ⇒ KILL_AUTO_PANIC on the first
	``gate_entry`` inside ``startup_reconcile``), but the risk channel /
	callback was wired LATE (G1's post-compose slot or later) so the trip
	fires into a void and the operator never learns live trading aborted.

	Drives the REAL ``run_engine`` and, the instant ``build_risk_module``
	returns inside step-3 ``_compose_live`` (BEFORE step-4 reconcile / step-5
	trading), fires a RiskEvent through the constructed Gate's callback list.
	It MUST already be routed to the dedicated channel."""
	captured: list[Notification] = []

	class _RiskSpy:
		name = "live_risk_discord"

		def send(self, n: Notification) -> DeliveryResult:
			captured.append(n)
			return DeliveryResult(channel_name=self.name, success=True)

	# build_risk_module is stubbed by _compose_spies to return a MagicMock
	# Gate. Re-stub it so the returned Gate carries a REAL callback list and,
	# the moment _compose_live appends _handle_risk_event + binds the channel,
	# a boot-time trip fired through that list reaches the bound channel.
	import edge_catcher.engine.risk as _riskmod

	class _FakeGate:
		def __init__(self) -> None:
			self._event_callbacks: list = []
			# step-4 startup_reconcile (no-op-stubbed by _compose_spies)
			# still has `live_runtime.gate._bankroll` evaluated as a call-arg
			# at its call site — so the attribute must exist for the boot to
			# proceed PAST step-4 to the _ComposeDone seam. The trip already
			# fired + was captured in step-3, strictly BEFORE this is touched.
			self._bankroll = object()

		def fire_boot_trip(self) -> None:
			ev = _risk_event(reason="KILL_AUTO_PANIC")
			for cb in self._event_callbacks:
				cb(ev)

	fake_gate = _FakeGate()

	async def _fake_build_risk(*_a, **_kw):
		_compose_spies["build_risk_called"] = True
		return fake_gate

	monkeypatch.setattr(_riskmod, "build_risk_module", _fake_build_risk)

	# Spy load_channels so the dedicated risk channel resolves to our spy
	# (zero disk/network) — patched at the engine module's resolution site.
	import edge_catcher.notifications as _notifmod

	_orig_load = _notifmod.load_channels

	def _fake_load_channels(path):
		ch = dict(_orig_load(path)) if Path(path).exists() else {}
		ch["live_risk_discord"] = _RiskSpy()
		ch.setdefault("live_pnl_discord", _RiskSpy())
		return ch

	monkeypatch.setattr(_notifmod, "load_channels", _fake_load_channels)

	# The instant step-3 finishes wiring (callback appended + channel bound),
	# fire a boot-time trip THROUGH the Gate — this stands in for the
	# step-4 reconcile gate_entry that trips on a 0 bankroll. We hook it by
	# wrapping _compose_live to fire right after it returns (still BEFORE the
	# step-4 startup_reconcile / step-5 _ws_loop seam _compose_spies aborts at).
	_orig_compose = engmod._compose_live

	async def _compose_then_trip(*a, **kw):
		result = await _orig_compose(*a, **kw)
		# At this point step-3 is COMPLETE: the callback is registered and
		# (G3) the dedicated channel is bound. Fire the boot-time trip.
		fake_gate.fire_boot_trip()
		return result

	monkeypatch.setattr(engmod, "_compose_live", _compose_then_trip)

	cfg = make_live_cfg(tmp_path, monkeypatch)
	_add_risk_channel(cfg, tmp_path)
	cfg["execution"] = {
		"entry_slippage_cents": 2,
		"exit_slippage_cents": {"take_profit": 1, "stop_loss": 1, "time_exit": 1},
	}
	cfg["sizing"] = {
		"risk_per_trade_cents": 500, "max_slippage_cents": 5, "min_fill": 1,
	}
	cfg["strategies"] = {
		_ComposeStubStrategy.name: {"enabled": True, "series": [_COMPOSE_STUB_SERIES]},
	}
	cfg_path = _write_cfg(cfg, tmp_path)

	with pytest.raises(_compose_spies["_ComposeDone"]):
		asyncio.run(run_engine_path(cfg_path))

	assert captured, (
		"a boot-time trip fired the instant step-3 finished (BEFORE step-4 "
		"reconcile / step-5 trading) MUST already reach the dedicated risk "
		"channel — the callback + channel are wired in step-3, not later. "
		"An empty capture means a boot-time kill-switch trip is silently lost."
	)
	blob = f"{captured[0].title} {captured[0].body}".lower()
	assert "kill_auto_panic" in blob


def run_engine_path(cfg_path: Path):
	"""Thin indirection so the patched ``engmod._compose_live`` is the one
	``run_engine`` resolves (it calls the module-global name)."""
	from edge_catcher.engine.engine import run_engine

	return run_engine(config_path=cfg_path)


# ===========================================================================
# (c) fail-closed coherence — additive to E2 Check-4 (live_risk_channel)
# ===========================================================================


def test_live_cfg_missing_live_risk_channel_aborts_coherence(
	tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
	"""Failure mode prevented (operator-safety): ``executor=live`` boots with
	NO dedicated risk channel configured — a future kill-switch trip would
	have nowhere to go and the operator would never learn trading HALTED.
	§2.4/§6 ("the live channels resolvable" — plural): a missing
	``notifications.live_risk_channel`` MUST abort boot (fail-closed)."""
	from edge_catcher.engine.engine import _assert_mode_coherence

	cfg = make_live_cfg(tmp_path, monkeypatch)
	# The shared builder now sets a coherent live_risk_channel (§2.4/§6 G3
	# made it mandatory). Simulate the footgun: operator forgot the key —
	# remove it, leaving the general live_channel set + resolvable.
	del cfg["notifications"]["live_risk_channel"]
	assert "live_risk_channel" not in cfg["notifications"]
	with pytest.raises(RuntimeError, match="coherence") as exc:
		_assert_mode_coherence(cfg)
	msg = str(exc.value).lower()
	assert "risk" in msg and "channel" in msg, (
		"the abort must name the dedicated RISK channel specifically "
		f"(operator-actionable); got: {exc.value!r}"
	)


def test_live_cfg_unresolvable_live_risk_channel_aborts_coherence(
	tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
	"""``live_risk_channel`` is named but NOT defined in notifications.yaml →
	abort (same fail-closed shape as the existing ``live_channel`` miss)."""
	from edge_catcher.engine.engine import _assert_mode_coherence

	cfg = make_live_cfg(tmp_path, monkeypatch)
	cfg["notifications"]["live_risk_channel"] = "risk_channel_not_defined"
	with pytest.raises(RuntimeError, match="coherence") as exc:
		_assert_mode_coherence(cfg)
	assert "risk" in str(exc.value).lower()


def test_all_coherent_live_cfg_with_risk_channel_passes(
	tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
	"""A fully-coherent live config — including a resolvable dedicated
	``live_risk_channel`` — PASSES the coherence step (the additive Check-4
	extension must not false-positive on a correct config)."""
	from edge_catcher.engine.engine import _assert_mode_coherence

	cfg = make_live_cfg(tmp_path, monkeypatch)
	_add_risk_channel(cfg, tmp_path)
	_assert_mode_coherence(cfg)  # must NOT raise


def test_existing_live_channel_check_still_enforced_additively(
	tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
	"""Regression: the G3 Check-4 extension is ADDITIVE — the pre-existing
	``live_channel`` miss MUST still abort (G3 must not weaken/replace the
	shipped E2 ``live_channel`` branch). Risk channel present + resolvable,
	general live_channel broken → still abort on the general channel."""
	from edge_catcher.engine.engine import _assert_mode_coherence

	cfg = make_live_cfg(tmp_path, monkeypatch)
	_add_risk_channel(cfg, tmp_path)
	cfg["notifications"]["live_channel"] = "general_channel_not_defined"
	with pytest.raises(RuntimeError, match="coherence"):
		_assert_mode_coherence(cfg)


def test_coherent_paper_cfg_needs_no_risk_channel(tmp_path: Path) -> None:
	"""§9 G-parity: paper has NO risk-event surface — a coherent paper config
	(no notifications block at all) still passes; the live-only risk-channel
	check must be genuinely skipped for paper."""
	from edge_catcher.engine.engine import _assert_mode_coherence

	_assert_mode_coherence(make_paper_cfg(tmp_path))


# ===========================================================================
# (d) paper untouched — live-only surface (K2 G-parity)
# ===========================================================================


def test_paper_compose_never_registers_risk_callback_or_resolves_channel(
	tmp_path: Path, _compose_spies,  # noqa: F811 — pytest fixture injected by name (imported at module top for registration)
) -> None:
	"""Failure mode prevented (G-parity BLOCKING): G3 leaks the risk-channel
	resolution / ``_handle_risk_event`` registration into the paper path —
	paper boot would resolve a risk channel or register the live-only
	callback, perturbing the byte-exact paper trade-row path.

	A paper boot must NOT bind the dedicated risk channel and must NOT
	register ``_handle_risk_event`` anywhere (live-only — spec §6 "paper has
	no analog")."""
	from edge_catcher.engine.engine import run_engine

	# Sentinel: nothing bound the risk channel before the paper boot.
	engmod._configure_risk_channel([])

	resolved_calls: list = []
	_orig_resolve = engmod._resolve_risk_channel

	def _spy_resolve(config):
		resolved_calls.append(config.get("executor"))
		return _orig_resolve(config)

	import pytest as _pt
	mp = _pt.MonkeyPatch()
	mp.setattr(engmod, "_resolve_risk_channel", _spy_resolve)
	try:
		cfg = make_paper_cfg(tmp_path)
		cfg["sizing"] = {
			"risk_per_trade_cents": 500, "max_slippage_cents": 5, "min_fill": 1,
		}
		cfg["strategies"] = {
			_ComposeStubStrategy.name: {
				"enabled": True, "series": [_COMPOSE_STUB_SERIES],
			}
		}
		cfg_path = _write_cfg(cfg, tmp_path)
		with _pt.raises(_compose_spies["_ComposeDone"]):
			asyncio.run(run_engine(config_path=cfg_path))
	finally:
		mp.undo()

	# The risk-channel resolver was NEVER invoked for a paper boot (it is
	# called only inside the live composition branch).
	assert resolved_calls == [], (
		"paper boot MUST NOT resolve a dedicated risk channel (live-only "
		f"surface — §6/§9 G-parity); resolver saw: {resolved_calls!r}"
	)
	# And the module risk-channel binding stayed empty (paper never binds it).
	assert engmod._risk_channels == [], (
		"paper boot MUST NOT bind the dedicated risk channel binding "
		f"(byte-exact paper path — §9); got {engmod._risk_channels!r}"
	)


def _names_used_in_code(fn) -> set[str]:
	"""AST-extract every executable Name/Attribute identifier in ``fn``'s
	source (comments + string literals are NOT in the AST — so an
	explanatory ``# … _handle_risk_event slot …`` comment in run_engine's
	body is correctly ignored; only a real reference counts)."""
	tree = ast.parse(inspect.getsource(fn)).body[0]
	names: set[str] = set()
	for n in ast.walk(tree):
		if isinstance(n, ast.Name):
			names.add(n.id)
		elif isinstance(n, ast.Attribute):
			names.add(n.attr)
	return names


def test_risk_callback_only_appended_in_live_composition_source() -> None:
	"""Structural §1/§9 guard: ``_handle_risk_event`` is referenced in
	executable code ONLY inside ``_compose_live`` (the live-only step-3
	path). ``run_engine``'s body — which the paper branch also runs — must
	contain NO executable reference to it (an explanatory comment is fine;
	this is AST-based so comments/strings are correctly ignored). Proves the
	live-only surface by source, not just runtime."""
	# It is referenced (registered) in the live-only step-3 helper ...
	assert "_handle_risk_event" in _names_used_in_code(engmod._compose_live), (
		"_handle_risk_event must be registered inside _compose_live (step-3)"
	)
	# ... and NOT in run_engine's executable body (the paper branch lives
	# there too — a registration there would perturb the paper path). AST
	# ignores the explanatory `# §6 _handle_risk_event slot` comment.
	assert "_handle_risk_event" not in _names_used_in_code(engmod.run_engine), (
		"_handle_risk_event must be wired in _compose_live (live-only step-3), "
		"NOT in run_engine's executable body where the paper branch reaches it"
	)


def test_risk_channel_resolution_uses_section_2_4_mechanism() -> None:
	"""Spec §6: the dedicated risk channel is "resolved per §2.4's invariant
	clause" — i.e. the SAME unified ``load_channels`` mechanism, not a second
	bespoke loader. Asserts the resolver references ``load_channels`` (the
	§2.4 mechanism G1/E2 use), single-sourcing channel resolution."""
	src = inspect.getsource(engmod._resolve_risk_channel)
	assert "load_channels" in src, (
		"the dedicated risk channel must resolve via the unified load_channels "
		"(the §2.4 mechanism) — no second bespoke channel loader (spec §6)"
	)
