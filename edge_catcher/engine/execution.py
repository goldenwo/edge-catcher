"""Pure-function builders that turn a Signal + sized decision into an OrderRequest.

This module is the AUTHORITATIVE home for live-execution support types and
helpers that sit between the strategy layer (engine/strategy_base.py) and the
executor layer (engine/executor.py + engine/executors/live.py):

  - ``ExecCfg`` — typed view of ``live-trader.yaml``'s execution: section.
  - ``OpenPosition`` — read-only view of an open trade; consumed by C's sizing
    gate and D's exit-order builder. Mirrors the shape in
    ``engine/executor.py:OpenPosition`` but is owned here so D can build exit
    orders from a position alone without a B-side helper.
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

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, cast, get_args

from edge_catcher.engine.executor import OrderRequest
from edge_catcher.engine.strategy_base import ExitKind, Signal

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
	in cents."""

	entry_slippage_cents: int
	exit_slippage_cents: dict[ExitKind, int]


@dataclass(frozen=True, slots=True)
class OpenPosition:
	"""Read-only view of an open live trade.

	AUTHORITATIVE definition lives in this module — C imports from here, and
	B's reconciliation populates the field set when reading from the live
	trades store. The four fields below are the entire C+D read surface; B's
	persistent schema may have additional columns (e.g. ``kalshi_order_id``,
	timestamps) that this view doesn't expose."""

	ticker: str
	side: Literal["yes", "no"]
	fill_size: int
	blended_entry_cents: int


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


def _make_client_order_id(sig: Signal, now: datetime) -> str:
	"""Build the Kalshi idempotency key for an order.

	Format: ``{strategy}-{ticker}-{ms_ts}-{uuid4_hex8}``.

	**The 8-char uuid suffix is REQUIRED for collision-safety.** Without it,
	two signals scheduled within the same WS-frame millisecond would generate
	identical client_order_ids; Kalshi's idempotency layer would return the
	SAME Order object for both POSTs, silently double-counting one fill in
	B's eventual schema and corrupting C's bankroll accounting until the
	panic floor catches it. Funds-at-risk bug per the v1.6.0 round-4 review.

	**Determinism contract:** same ``(Signal, now)`` produces a NEW string each
	call (uuid4 ensures uniqueness). Tests that need deterministic IDs must
	mock ``uuid.uuid4`` — production code MUST NOT rely on determinism.

	**Length budget:** worst-case strategy + ticker (~70 chars total) +
	1+13+1+8 = 23 → well within the 80-char ceiling enforced by the
	``_CLIENT_ORDER_ID_PATTERN`` regex in ``edge_catcher.live.client``.
	"""
	return f"{sig.strategy}-{sig.ticker}-{int(now.timestamp() * 1000)}-{uuid.uuid4().hex[:8]}"


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
		client_order_id=_make_client_order_id(sig, now),
		action="buy",
	)


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
		client_order_id=_make_client_order_id(sig, now),
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

	return ExecCfg(entry_slippage_cents=entry, exit_slippage_cents=typed_exits)
