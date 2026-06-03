"""Pure-function builders that turn a Signal + sized decision into an OrderRequest.

This module is the AUTHORITATIVE home for live-execution support types and
helpers that sit between the strategy layer (engine/strategy_base.py) and the
executor layer (engine/executor.py + engine/executors/live.py):

  - ``ExecCfg`` — typed view of ``live-trader.yaml``'s execution: section.
  - ``OpenPosition`` — re-exported from ``engine/executor.py``. The canonical
    definition lives there so C's ``RiskContext`` (which holds the position
    list) and D's exit-order builder share one type; mypy ``--strict`` and
    runtime ``isinstance`` checks both succeed across the cross-module call.
  - ``build_entry_order`` / ``build_exit_order`` — pure builders that translate
    Signal + size + cfg into the engine's typed ``OrderRequest``.
  - ``_make_client_order_id`` — idempotency-key generator.
  - ``_series_of`` — ticker-prefix parser.
  - ``validate_exec_cfg`` — startup validator (called by E at T0 boot).

All builders are pure: no I/O, no DB, no logging side effects. ``now`` is
threaded in by the caller (typically dispatch.py) so the function under test
is otherwise input-pure. The single non-determinism is the uuid4 suffix on
the client_order_id; production code MUST NOT rely on ID determinism, and
tests that need it mock ``uuid.uuid4`` (standard pattern).
"""
from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import Literal, Mapping, cast, get_args

from edge_catcher.engine.executor import OpenPosition, OrderRequest
from edge_catcher.engine.strategy_base import ExitKind, Signal
from edge_catcher.live.venue import sanitize_client_order_id_component

# Re-export so callers can ``from edge_catcher.engine.execution import OpenPosition``
# alongside the builders that consume it. The canonical definition lives in
# engine/executor.py — see module docstring above.
__all__ = ["ENTRY_TIF", "EXIT_TIF", "ExecCfg", "OpenPosition", "build_entry_order",
           "build_exit_order", "entry_spread_too_wide", "validate_exec_cfg"]

# Kalshi time-in-force value used for both entries and exits in Phase 1.
# IOC = "fill at the limit immediately and cancel any unfilled remainder";
# matches the taker-with-cap intent of paper's ``walk_book_with_ceiling``.
# Kept as a module-level literal so a future GTC option lands as an additive
# ``ExecCfg.entry_tif`` field without scattering string constants.
ENTRY_TIF: Literal["ioc"] = "ioc"
EXIT_TIF: Literal["ioc"] = "ioc"


@dataclass(frozen=True, slots=True)
class ExecCfg:
	"""Typed subset of ``live-trader.yaml``'s ``execution:`` section, produced
	by ``validate_exec_cfg``. Both slippage values are non-negative integers
	in cents.

	``exit_slippage_cents`` is typed ``Mapping`` (not ``dict``) because
	``validate_exec_cfg`` wraps the assembled dict in ``MappingProxyType``
	to make the immutability promise of ``frozen=True`` total: shallow
	frozen-ness alone would let a caller mutate ``cfg.exit_slippage_cents
	["stop_loss"] = 999`` and silently flip live order limits mid-stream."""

	entry_slippage_cents: int
	exit_slippage_cents: Mapping[ExitKind, int]
	# Live-only entry spread gate headroom (cents). 0 => skip when spread alone
	# reaches the stop. Optional in config (default 0). See spec
	# 2026-05-25-live-spread-entry-gate-design.md.
	entry_spread_stop_buffer_cents: int = 0


def _series_of(ticker: str) -> str:
	"""Extract the series prefix from a Kalshi ticker.

	Kalshi tickers are ``<SERIES>-<EVENT>``, e.g. ``KXBTC15M-26APR10-T1234``
	(matching the existing docstring convention in ``engine/market_state.py``).
	Derived inline rather than depending on B's reconciliation module so D
	can build exit orders from a position alone without a B-side helper.

	A ticker with no hyphen returns the whole string (defensive — covers
	any future series whose naming drops the event suffix).
	"""
	return ticker.split("-", 1)[0]


# Charset + length contract for the client_order_id field. This is a
# SELF-IMPOSED URL-safe invariant (mirrored by
# ``live.venue._CLIENT_ORDER_ID_PATTERN``), NOT a Kalshi requirement — Kalshi's
# create-order API documents no charset/length constraint on client_order_id
# (its canonical value is a uuid4, and the client's own fallback is
# ``str(uuid.uuid4())``). We keep ids URL-safe so they survive JSON encoding,
# log rendering, and the audit trail unambiguously.
#
# ``_CLIENT_ORDER_ID_CHARSET`` validates the in-repo ``strategy`` component (a
# disallowed char there is OUR bug -> reject loudly). The VENUE-supplied
# ``ticker`` is instead SANITIZED via the shared
# ``live.venue.sanitize_client_order_id_component`` helper (Kalshi scalar/range
# markets legitimately encode a decimal strike, e.g. ``B0.6099500``) so the order
# stays placeable instead of dying in dispatch's catch-all. The reconciler's
# orphan-recovery id uses that same helper — one charset definition, two callers.
_CLIENT_ORDER_ID_CHARSET = re.compile(r"[A-Za-z0-9_-]+")
_CLIENT_ORDER_ID_MAX_LEN = 80


def _make_client_order_id(strategy: str, ticker: str, now: datetime) -> str:
	"""Build the Kalshi idempotency key for an order.

	Args:
		strategy: Strategy name. MUST match ``[A-Za-z0-9_-]+``.
		ticker:   The Kalshi ticker the order targets. For exits, callers
			MUST pass ``pos.ticker`` (NOT ``sig.ticker``) — the two can drift
			if a strategy emits an exit signal tagged for a different ticker
			than the open position; the order is for ``pos.ticker``, so the
			idempotency key must be too.
		now: Wall-clock for the millisecond timestamp component.

	Format: ``{strategy}-{ticker}-{ms_ts}-{uuid4_hex8}``.

	**The 8-char uuid suffix is REQUIRED for collision-safety.** Without it,
	two signals scheduled within the same WS-frame millisecond would generate
	identical client_order_ids; Kalshi's idempotency layer would return the
	SAME Order object for both POSTs, silently double-counting one fill in
	B's eventual schema and corrupting C's bankroll accounting until the
	panic floor catches it. Funds-at-risk bug per the v1.6.0 round-4 review.

	**Determinism contract:** same ``(strategy, ticker, now)`` produces a NEW
	string each call (uuid4 ensures uniqueness). Tests that need deterministic
	IDs must mock ``uuid.uuid4`` — production code MUST NOT rely on determinism.

	Raises:
		ValueError: when ``strategy`` contains a character outside
			``[A-Za-z0-9_-]``, when ``ticker`` is empty, or when the assembled
			ID exceeds 80 characters. The ``ticker`` is otherwise SANITIZED
			(disallowed chars -> ``-``), NOT rejected: Kalshi scalar/range
			markets carry a decimal strike (e.g. ``KXXRP-26JUN0223-B0.6099500``)
			and the raw ticker still reaches the venue in the order body — only
			the URL-safe id needs the substitution. Rejecting it would silently
			drop every order on such a market via dispatch's catch-all.
	"""
	if not _CLIENT_ORDER_ID_CHARSET.fullmatch(strategy):
		# Strategy names are authored in-repo: a disallowed char is OUR bug, so
		# reject loudly rather than mangle the operator-facing identifier.
		# (Tickers, sanitized below, come from the venue — not our call to fix.)
		raise ValueError(
			f"_make_client_order_id: strategy must match [A-Za-z0-9_-]+, "
			f"got {strategy!r}"
		)
	if not ticker:
		# An empty ticker means there is no market to target — a genuine
		# upstream bug (the order body's ticker would be empty too and Kalshi
		# would reject). Surface it rather than build a ticker-less id.
		raise ValueError("_make_client_order_id: ticker must be non-empty")
	# Tickers come from the VENUE, not from us. Kalshi scalar/range markets
	# legitimately encode a decimal strike in the ticker (e.g.
	# ``KXXRP-26JUN0223-B0.6099500``); the raw ticker still reaches Kalshi in
	# the order body (the wire layer never charset-checks it — it IS the market
	# id), so only the URL-safe client_order_id needs the dot gone. The shared
	# venue helper substitutes disallowed chars with ``-`` so the order stays
	# placeable; the uuid4 suffix below keeps the id unique even if two tickers
	# sanitize to the same stem.
	safe_ticker = sanitize_client_order_id_component(ticker)
	oid = f"{strategy}-{safe_ticker}-{int(now.timestamp() * 1000)}-{uuid.uuid4().hex[:8]}"
	if len(oid) > _CLIENT_ORDER_ID_MAX_LEN:
		raise ValueError(
			f"_make_client_order_id: assembled id length {len(oid)} > "
			f"{_CLIENT_ORDER_ID_MAX_LEN} (strategy={strategy!r}, ticker={ticker!r})"
		)
	return oid


def build_entry_order(
	sig: Signal,
	allowed_size: int,
	cfg: ExecCfg,
	now: datetime,
) -> OrderRequest:
	"""Build the engine OrderRequest for an entry.

	Limit price walks UP from the signal's ``entry_price_cents`` by
	``cfg.entry_slippage_cents`` — taker-with-cap semantics matching paper's
	``walk_book_with_ceiling``. Result is clamped into Kalshi's 1..99 cent
	band; any out-of-band proposal from the strategy is silently corrected
	rather than allowed to reach the wire and 4xx-reject.

	``now`` is required for the idempotency key. The function is input-pure
	apart from the uuid4 suffix on ``client_order_id`` (see
	``_make_client_order_id``).

	Args:
		sig:          The entry Signal emitted by a strategy. ``entry_price_cents``
			and ``stop_loss_distance_cents`` MUST be populated; missing fields
			raise ``ValueError``.
		allowed_size: Contract count from C's gate decision. MUST be > 0;
			0 / negative raises ``ValueError``.
		cfg:          ExecCfg with ``entry_slippage_cents``.
		now:          Wall-clock for the idempotency key.

	Returns:
		Engine ``OrderRequest`` with ``action="buy"``, clamped limit, and a
		freshly-generated ``client_order_id``.

	Raises:
		ValueError: on missing required Signal fields or non-positive
		``allowed_size``. Defense in depth — C's gate should never produce
		``Allow(size <= 0)``, and strategy authors emitting live signals
		are expected to populate the price/stop fields. Loud failure beats
		silent corruption (e.g. sending ``count=0`` or ``count=-N`` to
		Kalshi).
	"""
	if allowed_size <= 0:
		# Defense in depth — C's gate should never produce Allow(size <= 0),
		# but a sign bug there reaching here would silently send count=0 or
		# count=-N to Kalshi.
		raise ValueError(
			f"build_entry_order: allowed_size must be > 0, got {allowed_size} "
			f"(strategy={sig.strategy}, ticker={sig.ticker})"
		)
	if sig.entry_price_cents is None or sig.stop_loss_distance_cents is None:
		raise ValueError(
			f"build_entry_order: missing required fields "
			f"(entry_price_cents={sig.entry_price_cents!r}, "
			f"stop_loss_distance_cents={sig.stop_loss_distance_cents!r}, "
			f"strategy={sig.strategy})"
		)
	# Clamp into Kalshi's 1..99 cent band. Defense in depth — a strategy
	# whose entry_price_cents is already at 99 plus a 2c slippage would
	# otherwise produce limit=101, which Kalshi 4xx-rejects (operator noise)
	# rather than a clean local clamp. We never want a wire-side rejection
	# for something we can clamp client-side.
	limit = max(1, min(99, sig.entry_price_cents + cfg.entry_slippage_cents))
	return OrderRequest(
		ticker=sig.ticker,
		series=sig.series,
		side=cast(Literal["yes", "no"], sig.side),
		size_contracts=allowed_size,
		limit_price_cents=limit,
		strategy=sig.strategy,
		client_order_id=_make_client_order_id(sig.strategy, sig.ticker, now),
		action="buy",
	)


def entry_spread_too_wide(spread_cents: int, protective_stop_cents: int, buffer_cents: int) -> bool:
	"""True when the bid-ask spread is wide enough to (near-)trip the protective
	stop on a taker fill: an IOC entry books at the ask but marks at the bid, so
	it starts -(spread) underwater and stops out the instant spread >= stop.
	``buffer_cents`` reserves headroom below the stop (0 = skip only when the
	spread alone reaches the stop)."""
	return spread_cents >= protective_stop_cents - buffer_cents


def build_exit_order(
	pos: OpenPosition,
	sig: Signal,
	cfg: ExecCfg,
	now: datetime,
) -> OrderRequest:
	"""Build the engine OrderRequest for an exit.

	Limit price moves the sell-acceptance threshold by
	``cfg.exit_slippage_cents[exit_kind]``. The slippage is *wider* for
	``stop_loss`` than for ``take_profit`` so the position actually closes
	in fast markets — operational intent locked at spec time.

	Direction handling: for a yes-side long, exit = sell yes and we accept
	down to ``target - slippage``. For a no-side short, exit = sell no and
	we accept up to ``target + slippage``. The ``direction_sign``
	formulation captures both.

	Result is clamped into Kalshi's 1..99 cent band as a defense-in-depth
	measure against any unanticipated target × slippage combination that
	would otherwise push the limit out of range.

	Args:
		pos:  The open position being closed. ``fill_size`` MUST be > 0.
		sig:  The exit Signal emitted by a strategy. ``target_price_cents``
			and ``exit_kind`` MUST be populated.
		cfg:  ExecCfg with ``exit_slippage_cents`` covering the signal's
			``exit_kind``.
		now:  Wall-clock for the idempotency key.

	Returns:
		Engine ``OrderRequest`` with ``action="sell"``, clamped limit, size
		drawn from ``pos.fill_size``, and a freshly-generated
		``client_order_id``.

	Raises:
		ValueError: on missing required Signal fields, ``pos.fill_size <= 0``,
		or an ``exit_kind`` not present in ``cfg.exit_slippage_cents``. The
		latter check fails explicitly rather than KeyError-crashing the
		dispatch loop on a bogus literal.
	"""
	if pos.fill_size <= 0:
		# Defense in depth — exiting a position with no contracts is a B-side
		# bug; treat as loud failure rather than send count=0 to Kalshi.
		raise ValueError(
			f"build_exit_order: pos.fill_size must be > 0, got {pos.fill_size} "
			f"(ticker={pos.ticker}, strategy={sig.strategy})"
		)
	if sig.ticker != pos.ticker:
		# The exit order is for ``pos.ticker``; if the strategy emitted an
		# exit signal tagged for a different ticker, that's a strategy bug.
		# Surface loudly rather than silently building an order whose
		# client_order_id and request body disagree on the target ticker.
		raise ValueError(
			f"build_exit_order: sig.ticker ({sig.ticker!r}) must equal "
			f"pos.ticker ({pos.ticker!r}) — strategy is closing the wrong "
			f"position (strategy={sig.strategy})"
		)
	if sig.target_price_cents is None or sig.exit_kind is None:
		raise ValueError(
			f"build_exit_order: missing required fields "
			f"(target_price_cents={sig.target_price_cents!r}, "
			f"exit_kind={sig.exit_kind!r}, strategy={sig.strategy})"
		)
	if sig.exit_kind not in cfg.exit_slippage_cents:
		# Runtime guard — mypy catches non-literal exit_kind at type-check
		# time, but a string-typed Signal field can still slip through.
		# Fail explicitly rather than KeyError-crashing the dispatch loop.
		raise ValueError(
			f"build_exit_order: exit_kind {sig.exit_kind!r} not in "
			f"cfg.exit_slippage_cents (known: {sorted(cfg.exit_slippage_cents)}); "
			f"strategy={sig.strategy}"
		)
	slippage = cfg.exit_slippage_cents[sig.exit_kind]
	# For a long (yes), exit = sell yes. We accept down to target - slippage.
	# For a short (no), exit = sell no. Same shape — direction comes from side.
	direction_sign = -1 if pos.side == "yes" else +1
	limit = sig.target_price_cents + direction_sign * slippage
	# Clamp to Kalshi's valid 1..99 cents range (defense in depth — strategies
	# should not propose a target outside this band, but a tight book can push
	# us past; rejecting via clamp is safer than letting Kalshi 4xx).
	limit = max(1, min(99, limit))
	return OrderRequest(
		ticker=pos.ticker,
		series=_series_of(pos.ticker),
		side=pos.side,
		size_contracts=pos.fill_size,
		limit_price_cents=limit,
		strategy=sig.strategy,
		# Use pos.ticker explicitly — for exits the order is for the open
		# position, so the idempotency key must agree with the request body.
		client_order_id=_make_client_order_id(sig.strategy, pos.ticker, now),
		action="sell",
	)


def validate_exec_cfg(cfg: dict[str, object]) -> ExecCfg:
	"""Validate the parsed ``execution:`` block from ``live-trader.yaml`` and
	return a typed ``ExecCfg``. Called by E during T0 boot, BEFORE the WS
	loop starts — catches config drift at engine boot rather than at first
	order placement.

	Required keys + invariants:
	  - ``cfg["entry_slippage_cents"]``: int >= 0. ``TypeError`` on non-int
	    (e.g. YAML string ``"2"``). ``ValueError`` on negative.
	  - ``cfg["exit_slippage_cents"]``: dict mapping EVERY ``ExitKind``
	    literal to an int >= 0. ``TypeError`` on non-dict (e.g. YAML loaded
	    as a list). ``ValueError`` on missing kinds or negative values.

	Adding a new ``ExitKind`` literal (e.g. ``partial_exit``) without also
	adding it to config is a startup failure here, not a runtime surprise
	at first exit signal.
	"""
	entry = cfg.get("entry_slippage_cents")
	# ``bool`` is a subclass of ``int`` in Python — accepting True/False here
	# would silently coerce them to 1/0 slippage. Reject explicitly.
	if not isinstance(entry, int) or isinstance(entry, bool):
		raise TypeError(
			f"execution.entry_slippage_cents must be int, "
			f"got {type(entry).__name__}: {entry!r}"
		)
	if entry < 0:
		raise ValueError(
			f"execution.entry_slippage_cents must be >= 0, got {entry}"
		)

	exits = cfg.get("exit_slippage_cents")
	if not isinstance(exits, dict):
		raise TypeError(
			f"execution.exit_slippage_cents must be dict, "
			f"got {type(exits).__name__}: {exits!r}"
		)

	known_kinds: tuple[str, ...] = get_args(ExitKind)
	missing = [k for k in known_kinds if k not in exits]
	if missing:
		raise ValueError(
			f"execution.exit_slippage_cents missing required ExitKind(s): "
			f"{sorted(missing)} (known: {sorted(known_kinds)})"
		)

	# Validate each kind's value. Unknown extra keys are tolerated (forward
	# compat for staging a new ExitKind in config before promoting the
	# Literal) — they just don't get used.
	typed_exits: dict[ExitKind, int] = {}
	for kind in known_kinds:
		value = exits[kind]
		if not isinstance(value, int) or isinstance(value, bool):
			raise TypeError(
				f"execution.exit_slippage_cents[{kind!r}] must be int, "
				f"got {type(value).__name__}: {value!r}"
			)
		if value < 0:
			raise ValueError(
				f"execution.exit_slippage_cents[{kind!r}] must be >= 0, got {value}"
			)
		typed_exits[cast(ExitKind, kind)] = value

	# Wrap in MappingProxyType so the frozen=True invariant is total — without
	# the wrap, ``cfg.exit_slippage_cents["stop_loss"] = 999`` would silently
	# succeed and corrupt live order limits mid-stream.

	# Optional live spread-gate buffer. Absent key => 0. bool is an int
	# subclass; reject it explicitly (True/False would coerce to 1/0).
	buffer_cents = cfg.get("entry_spread_stop_buffer_cents", 0)
	if not isinstance(buffer_cents, int) or isinstance(buffer_cents, bool):
		raise TypeError(
			f"execution.entry_spread_stop_buffer_cents must be int, "
			f"got {type(buffer_cents).__name__}: {buffer_cents!r}"
		)
	if buffer_cents < 0:
		raise ValueError(
			f"execution.entry_spread_stop_buffer_cents must be >= 0, got {buffer_cents}"
		)

	return ExecCfg(
		entry_slippage_cents=entry,
		exit_slippage_cents=MappingProxyType(typed_exits),
		entry_spread_stop_buffer_cents=buffer_cents,
	)
