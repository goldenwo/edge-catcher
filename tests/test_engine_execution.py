"""Unit tests for engine.execution — order-request builders, idempotency key,
config validation, and the helpers that sit between strategy signals and the
typed executor protocol.

Coverage map (per D spec L744-L778):
	#1   build_entry_order happy path + clamp
	#2   build_entry_order missing fields + size guard
	#3   build_exit_order per exit_kind (per-kind slippage + direction sign)
	#4   build_exit_order clamp
	#5   build_exit_order missing fields + size guard + unknown exit_kind
	#15  AST regression — builders' action= kwarg may be a literal IFF the
	     hardcoding is intentional (entry → "buy", exit → "sell"); the AST
	     check pins the invariant for agent 3b.B's ``_to_kalshi_request``
	     where ANY string literal would be a Round-1-style bug.
	#16  build_entry_order action="buy" / build_exit_order action="sell"
	#18  validate_exec_cfg startup validation
	#19  build_entry_order limit clamping (entry=98 + slippage=5 → 99, not 103)
	#20  _make_client_order_id collision-safety + length budget
	#21  _series_of parsing

Methodology note (per session 2026-05-10 rule "tests must prove the fix
prevents the failure mode it claims"): each test docstring states the
specific failure mode it guards against. A passing test without that
proof is theatre."""
from __future__ import annotations

import ast
import dataclasses
import inspect
import re
from datetime import datetime, timezone

import pytest

from edge_catcher.engine.execution import (
	ExecCfg,
	OpenPosition,
	_make_client_order_id,
	_series_of,
	build_entry_order,
	build_exit_order,
	validate_exec_cfg,
)
from edge_catcher.engine.strategy_base import ExitKind, Signal


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


_NOW = datetime(2026, 5, 11, 12, 0, 0, tzinfo=timezone.utc)


def _entry_signal(
	*,
	ticker: str = "KXSOL15M-26MAY09H06",
	side: str = "yes",
	series: str = "KXSOL15M",
	strategy: str = "debut_fade",
	entry_price_cents: int | None = 42,
	stop_loss_distance_cents: int | None = 8,
) -> Signal:
	return Signal(
		action="enter",
		ticker=ticker,
		side=side,
		series=series,
		strategy=strategy,
		reason="test",
		entry_price_cents=entry_price_cents,
		stop_loss_distance_cents=stop_loss_distance_cents,
	)


def _exit_signal(
	*,
	strategy: str = "debut_fade",
	target_price_cents: int | None = 50,
	exit_kind: ExitKind | None = "take_profit",
) -> Signal:
	return Signal(
		action="exit",
		ticker="KXSOL15M-26MAY09H06",  # not used by exit builder; pos.ticker wins
		side="yes",
		series="KXSOL15M",
		strategy=strategy,
		reason="test",
		target_price_cents=target_price_cents,
		exit_kind=exit_kind,
		trade_id=42,
	)


def _exec_cfg(
	*,
	entry: int = 2,
	tp: int = 2,
	sl: int = 10,
	te: int = 5,
) -> ExecCfg:
	return ExecCfg(
		entry_slippage_cents=entry,
		exit_slippage_cents={
			"take_profit": tp,
			"stop_loss": sl,
			"time_exit": te,
		},
	)


# --------------------------------------------------------------------------
# Test #1 + #19 — build_entry_order happy path + clamp
# --------------------------------------------------------------------------


def test_entry_happy_path_returns_expected_order_shape() -> None:
	"""Failure mode: builder forgets a required OrderRequest field or
	mis-routes one (e.g. swaps strategy and series). Asserts the full
	wire-payload shape end-to-end."""
	sig = _entry_signal(entry_price_cents=42)
	req = build_entry_order(sig, allowed_size=10, cfg=_exec_cfg(entry=2), now=_NOW)
	assert req.ticker == "KXSOL15M-26MAY09H06"
	assert req.series == "KXSOL15M"
	assert req.side == "yes"
	assert req.size_contracts == 10
	assert req.limit_price_cents == 44                  # 42 + 2
	assert req.strategy == "debut_fade"
	assert req.action == "buy"
	# client_order_id format: {strategy}-{ticker}-{ms_ts}-{uuid8}
	assert req.client_order_id.startswith("debut_fade-KXSOL15M-26MAY09H06-")


def test_entry_limit_clamped_to_99_when_signal_at_ceiling() -> None:
	"""Failure mode (#19): entry_price=98 + slippage=5 produces limit=103,
	Kalshi 4xx-rejects. Without the clamp, the order never reaches the
	venue; with the clamp, the order is at least submitted at 99 (taker
	hits the top of book deterministically)."""
	sig = _entry_signal(entry_price_cents=98)
	req = build_entry_order(sig, allowed_size=5, cfg=_exec_cfg(entry=5), now=_NOW)
	assert req.limit_price_cents == 99


def test_entry_limit_clamped_at_already_ceiling() -> None:
	"""Failure mode: signal at 99 with any slippage > 0 must NOT produce 100+.
	The clamp catches this exactly at the boundary."""
	sig = _entry_signal(entry_price_cents=99)
	req = build_entry_order(sig, allowed_size=5, cfg=_exec_cfg(entry=2), now=_NOW)
	assert req.limit_price_cents == 99


def test_entry_limit_clamped_to_1_when_signal_at_floor() -> None:
	"""Failure mode: a pathologically low signal (entry_price=0 or negative)
	combined with a negative-slippage misconfig would push limit below 1.
	Defense in depth — the lower clamp keeps us in Kalshi's 1..99 band."""
	# validate_exec_cfg refuses negative slippage at boot, so this is a
	# constructed pathological case to exercise the floor clamp.
	cfg = ExecCfg(entry_slippage_cents=-50, exit_slippage_cents={
		"take_profit": 0, "stop_loss": 0, "time_exit": 0,
	})
	sig = _entry_signal(entry_price_cents=5)
	req = build_entry_order(sig, allowed_size=1, cfg=cfg, now=_NOW)
	assert req.limit_price_cents == 1


def test_entry_no_clamp_when_in_band() -> None:
	"""Failure mode: clamp accidentally rounds in-band values. entry=2 +
	slippage=2 → limit=4, no clamp applied."""
	sig = _entry_signal(entry_price_cents=2)
	req = build_entry_order(sig, allowed_size=5, cfg=_exec_cfg(entry=2), now=_NOW)
	assert req.limit_price_cents == 4


# --------------------------------------------------------------------------
# Test #2 — build_entry_order missing fields + size guard
# --------------------------------------------------------------------------


def test_entry_rejects_zero_allowed_size() -> None:
	"""Failure mode: a sign bug in C's gate reaches D with allowed_size=0,
	and we silently send count=0 to Kalshi. Loud failure beats silent
	corruption."""
	sig = _entry_signal()
	with pytest.raises(ValueError, match="allowed_size must be > 0"):
		build_entry_order(sig, allowed_size=0, cfg=_exec_cfg(), now=_NOW)


def test_entry_rejects_negative_allowed_size() -> None:
	"""Failure mode: same as the zero case, but for negative — Python's
	``-1 contracts`` would silently route into Kalshi's count parameter
	which expects positive ints."""
	sig = _entry_signal()
	with pytest.raises(ValueError, match="allowed_size must be > 0"):
		build_entry_order(sig, allowed_size=-1, cfg=_exec_cfg(), now=_NOW)


def test_entry_rejects_missing_entry_price() -> None:
	"""Failure mode: a strategy authored for paper-only emits an entry
	signal with entry_price_cents=None, and we'd otherwise crash on the
	None + int arithmetic. Validate explicitly so the dispatch loop logs a
	clear ValueError instead of a TypeError."""
	sig = _entry_signal(entry_price_cents=None)
	with pytest.raises(ValueError, match="missing required fields"):
		build_entry_order(sig, allowed_size=5, cfg=_exec_cfg(), now=_NOW)


def test_entry_rejects_missing_stop_loss_distance() -> None:
	"""Failure mode: live-eligible strategy forgets to populate
	stop_loss_distance_cents. C's sizing reads it; D's builder enforces it.
	Without this check, C would already have rejected with a different
	error — D's check is the second line of defense."""
	sig = _entry_signal(stop_loss_distance_cents=None)
	with pytest.raises(ValueError, match="missing required fields"):
		build_entry_order(sig, allowed_size=5, cfg=_exec_cfg(), now=_NOW)


# --------------------------------------------------------------------------
# Test #3 — build_exit_order per exit_kind + direction sign
# --------------------------------------------------------------------------


def _open_long(blended_entry_cents: int = 42, fill_size: int = 10) -> OpenPosition:
	"""Long position on the yes side — exit sells yes, accepts down."""
	return OpenPosition(
		ticker="KXSOL15M-26MAY09H06",
		side="yes",
		fill_size=fill_size,
		blended_entry_cents=blended_entry_cents,
	)


def _open_short(blended_entry_cents: int = 42, fill_size: int = 10) -> OpenPosition:
	"""Short position on the no side — exit sells no, accepts up."""
	return OpenPosition(
		ticker="KXSOL15M-26MAY09H06",
		side="no",
		fill_size=fill_size,
		blended_entry_cents=blended_entry_cents,
	)


def test_exit_long_take_profit_subtracts_slippage() -> None:
	"""Failure mode: long-side exit applies + instead of - to slippage, so
	we sell ABOVE target — order never fills in normal conditions. With
	the correct direction sign, target=50, slippage=2 → limit=48."""
	pos = _open_long()
	sig = _exit_signal(target_price_cents=50, exit_kind="take_profit")
	req = build_exit_order(pos, sig, _exec_cfg(tp=2), _NOW)
	assert req.limit_price_cents == 48
	assert req.action == "sell"


def test_exit_long_stop_loss_uses_wider_slippage() -> None:
	"""Failure mode: stop_loss uses the same 2c slippage as take_profit so
	the SL fails to close in a fast market. SL must use cfg's wider 10c
	value to actually exit."""
	pos = _open_long()
	sig = _exit_signal(target_price_cents=30, exit_kind="stop_loss")
	req = build_exit_order(pos, sig, _exec_cfg(sl=10), _NOW)
	assert req.limit_price_cents == 20  # 30 - 10


def test_exit_long_time_exit_uses_te_slippage() -> None:
	"""Failure mode: time_exit slips into the SL bucket and uses 10c,
	overpaying the exit envelope. time_exit gets its own 5c slippage."""
	pos = _open_long()
	sig = _exit_signal(target_price_cents=40, exit_kind="time_exit")
	req = build_exit_order(pos, sig, _exec_cfg(te=5), _NOW)
	assert req.limit_price_cents == 35  # 40 - 5


def test_exit_short_take_profit_adds_slippage() -> None:
	"""Failure mode: short-side exit subtracts slippage instead of adding,
	so the no-side cover never fills. direction_sign = +1 for side='no'."""
	pos = _open_short()
	sig = _exit_signal(target_price_cents=50, exit_kind="take_profit")
	req = build_exit_order(pos, sig, _exec_cfg(tp=2), _NOW)
	assert req.limit_price_cents == 52
	assert req.action == "sell"


def test_exit_uses_position_size_not_signal_size() -> None:
	"""Failure mode (Test #18 signature divergence): build_exit_order
	derives size from pos.fill_size, NOT from any field on the Signal.
	Confirms the two builders are semantically distinct — entry takes an
	allowed_size kwarg; exit takes a position."""
	pos = _open_long(fill_size=7)
	sig = _exit_signal()
	req = build_exit_order(pos, sig, _exec_cfg(), _NOW)
	assert req.size_contracts == 7


def test_exit_signature_diverges_from_entry() -> None:
	"""Failure mode (Test #18): a future refactor merges entry/exit
	builders behind a single ``build_order`` and accidentally requires
	``allowed_size`` on exits too — bypassing the pos.fill_size
	source-of-truth. Inspect-based assertion that the two builders take
	different parameter sets."""
	entry_params = inspect.signature(build_entry_order).parameters
	exit_params = inspect.signature(build_exit_order).parameters
	assert "allowed_size" in entry_params, "entry must take allowed_size from C's gate"
	assert "allowed_size" not in exit_params, "exit must derive size from pos.fill_size"
	assert "pos" in exit_params, "exit must take an OpenPosition"
	assert "pos" not in entry_params, "entry must NOT take a position — it's opening one"


# --------------------------------------------------------------------------
# Test #4 — build_exit_order clamp
# --------------------------------------------------------------------------


def test_exit_long_clamped_to_floor_on_extreme_sl() -> None:
	"""Failure mode (#4): target=5 + side=yes + stop_loss slippage=10 →
	limit=-5 without the clamp; Kalshi 4xx-rejects. The clamp keeps the
	order in-band at 1c."""
	pos = _open_long()
	sig = _exit_signal(target_price_cents=5, exit_kind="stop_loss")
	req = build_exit_order(pos, sig, _exec_cfg(sl=10), _NOW)
	assert req.limit_price_cents == 1


def test_exit_short_clamped_to_ceiling_on_extreme_sl() -> None:
	"""Failure mode (#4 mirror): target=95 + side=no + slippage=10 →
	limit=105 without clamp. The upper clamp keeps it at 99."""
	pos = _open_short()
	sig = _exit_signal(target_price_cents=95, exit_kind="stop_loss")
	req = build_exit_order(pos, sig, _exec_cfg(sl=10), _NOW)
	assert req.limit_price_cents == 99


# --------------------------------------------------------------------------
# Test #5 — build_exit_order missing fields + size guard + unknown kind
# --------------------------------------------------------------------------


def test_exit_rejects_zero_fill_size_position() -> None:
	"""Failure mode: B's reconciliation hands us a position with fill_size=0
	(maybe a phantom row from a failed write). We'd send count=0 to Kalshi.
	Loud failure beats silent no-op."""
	pos = OpenPosition(
		ticker="KX-X", side="yes", fill_size=0, blended_entry_cents=42,
	)
	sig = _exit_signal()
	with pytest.raises(ValueError, match="pos.fill_size must be > 0"):
		build_exit_order(pos, sig, _exec_cfg(), _NOW)


def test_exit_rejects_missing_target_price() -> None:
	"""Failure mode: exit signal forgets target_price_cents and we'd crash
	on None + int. Explicit ValueError keeps the dispatch loop alive."""
	pos = _open_long()
	sig = _exit_signal(target_price_cents=None)
	with pytest.raises(ValueError, match="missing required fields"):
		build_exit_order(pos, sig, _exec_cfg(), _NOW)


def test_exit_rejects_missing_exit_kind() -> None:
	"""Failure mode: exit signal forgets exit_kind. Same explicit-failure
	rationale as the missing-target-price test."""
	pos = _open_long()
	sig = _exit_signal(exit_kind=None)
	with pytest.raises(ValueError, match="missing required fields"):
		build_exit_order(pos, sig, _exec_cfg(), _NOW)


def test_exit_rejects_bogus_exit_kind_runtime() -> None:
	"""Failure mode: a bogus string-typed exit_kind (mypy-suppressed in a
	test strategy) reaches the dict lookup and KeyError-crashes the
	dispatch loop. Explicit runtime guard raises ValueError before the
	KeyError fires."""
	pos = _open_long()
	# mypy literal type would reject "bogus_kind"; we deliberately bypass
	# the type system to exercise the runtime guard.
	sig = dataclasses.replace(
		_exit_signal(),
		exit_kind="bogus_kind",  # type: ignore[arg-type]
	)
	with pytest.raises(ValueError, match="not in cfg.exit_slippage_cents"):
		build_exit_order(pos, sig, _exec_cfg(), _NOW)


# --------------------------------------------------------------------------
# Test #15 — AST no-hardcode regression (builders' scope)
#
# The PRIMARY hardcode-check belongs on Agent 3b.B's ``_to_kalshi_request``
# (NOT in this PR). However, this test pins the invariant for the BUILDERS
# in our scope: entry → action="buy" and exit → action="sell" are
# semantically hardcoded BY DESIGN (the builder's job is to map intent ↔
# wire action). The test asserts that these are the ONLY two literals
# present and they are scoped correctly (buy in entry only, sell in exit
# only). A future refactor that swaps them or introduces a third literal
# anywhere fails this test.
# --------------------------------------------------------------------------


def _action_kw_literals_in(func: object) -> list[str]:
	"""Return all string-literal values passed as ``action=...`` keyword
	arguments inside ``func``'s source."""
	tree = ast.parse(inspect.getsource(func))  # type: ignore[arg-type]
	found: list[str] = []
	for node in ast.walk(tree):
		if isinstance(node, ast.keyword) and node.arg == "action":
			if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
				found.append(node.value.value)
	return found


def test_entry_builder_action_literal_is_only_buy() -> None:
	"""Failure mode: a future refactor introduces an ``action="sell"``
	branch in build_entry_order (e.g. to handle inverted-side entries) and
	silently routes some entries as sells. The AST scan catches it — the
	only literal allowed in build_entry_order is "buy"."""
	literals = _action_kw_literals_in(build_entry_order)
	assert literals == ["buy"], (
		f"build_entry_order must hardcode action='buy' EXACTLY ONCE, "
		f"got literals: {literals}"
	)


def test_exit_builder_action_literal_is_only_sell() -> None:
	"""Failure mode: a future refactor introduces an ``action="buy"`` branch
	in build_exit_order (e.g. to handle position-flip logic) and silently
	doubles a long position. The AST scan catches it — the only literal
	allowed in build_exit_order is "sell"."""
	literals = _action_kw_literals_in(build_exit_order)
	assert literals == ["sell"], (
		f"build_exit_order must hardcode action='sell' EXACTLY ONCE, "
		f"got literals: {literals}"
	)


# --------------------------------------------------------------------------
# Test #16 — explicit action= assertions (paired with #15's AST scan)
# --------------------------------------------------------------------------


def test_entry_action_is_buy() -> None:
	"""Failure mode (paired with #15): the AST literal is "buy" but a
	conditional branch in the builder ends up returning action="sell" at
	runtime. Wire-payload assertion."""
	sig = _entry_signal()
	req = build_entry_order(sig, allowed_size=1, cfg=_exec_cfg(), now=_NOW)
	assert req.action == "buy"


def test_exit_action_is_sell() -> None:
	"""Failure mode (paired with #15): mirror of the entry case."""
	pos = _open_long()
	sig = _exit_signal()
	req = build_exit_order(pos, sig, _exec_cfg(), _NOW)
	assert req.action == "sell"


# --------------------------------------------------------------------------
# Test #18 — validate_exec_cfg startup validation
# --------------------------------------------------------------------------


def test_validate_exec_cfg_happy_path_returns_typed() -> None:
	"""Failure mode: validator accepts a config but produces an ExecCfg
	with the wrong slippage values mapped to the wrong kinds. Round-trip
	check on the typed result."""
	cfg = validate_exec_cfg({
		"entry_slippage_cents": 2,
		"exit_slippage_cents": {
			"take_profit": 2,
			"stop_loss": 10,
			"time_exit": 5,
		},
	})
	assert isinstance(cfg, ExecCfg)
	assert cfg.entry_slippage_cents == 2
	assert cfg.exit_slippage_cents["take_profit"] == 2
	assert cfg.exit_slippage_cents["stop_loss"] == 10
	assert cfg.exit_slippage_cents["time_exit"] == 5


@pytest.mark.parametrize("missing_kind", ["take_profit", "stop_loss", "time_exit"])
def test_validate_exec_cfg_missing_kind_raises(missing_kind: str) -> None:
	"""Failure mode: adding a new ExitKind literal without updating config
	produces a silent KeyError at first matching exit signal. The
	startup validator catches it at boot."""
	exits = {"take_profit": 2, "stop_loss": 10, "time_exit": 5}
	exits.pop(missing_kind)
	with pytest.raises(ValueError, match="missing required ExitKind"):
		validate_exec_cfg({"entry_slippage_cents": 2, "exit_slippage_cents": exits})


def test_validate_exec_cfg_negative_entry_slippage_raises() -> None:
	"""Failure mode: operator typos a leading minus into the config and the
	limit-clamp test alone won't catch it (clamp is defense in depth, not
	a primary validator). Boot-time refusal."""
	with pytest.raises(ValueError, match="entry_slippage_cents must be >= 0"):
		validate_exec_cfg({
			"entry_slippage_cents": -1,
			"exit_slippage_cents": {"take_profit": 2, "stop_loss": 10, "time_exit": 5},
		})


def test_validate_exec_cfg_string_entry_slippage_raises_type_error() -> None:
	"""Failure mode: YAML loads ``entry_slippage_cents: "2"`` (quoted) as a
	string, and Python's ``str + int`` arithmetic crashes at first order.
	Boot-time refusal."""
	with pytest.raises(TypeError, match="entry_slippage_cents must be int"):
		validate_exec_cfg({
			"entry_slippage_cents": "2",
			"exit_slippage_cents": {"take_profit": 2, "stop_loss": 10, "time_exit": 5},
		})


def test_validate_exec_cfg_bool_entry_slippage_rejected() -> None:
	"""Failure mode: ``True``/``False`` are instances of ``int`` in Python
	(bool is a subclass), so ``isinstance(True, int) == True``. Without an
	explicit bool check, ``entry_slippage_cents: yes`` (YAML truthy →
	Python True) would silently coerce to slippage=1."""
	with pytest.raises(TypeError, match="entry_slippage_cents must be int"):
		validate_exec_cfg({
			"entry_slippage_cents": True,
			"exit_slippage_cents": {"take_profit": 2, "stop_loss": 10, "time_exit": 5},
		})


def test_validate_exec_cfg_list_exit_slippage_raises_type_error() -> None:
	"""Failure mode: YAML drift produces a list instead of a dict. The
	validator must surface it as a TypeError on the section, not crash on
	the first ``.get`` deeper in."""
	with pytest.raises(TypeError, match="exit_slippage_cents must be dict"):
		validate_exec_cfg({
			"entry_slippage_cents": 2,
			"exit_slippage_cents": ["take_profit", 2],
		})


def test_validate_exec_cfg_negative_exit_slippage_raises() -> None:
	"""Failure mode: operator typos a minus into one of the exit kinds.
	Boot-time refusal — same rationale as the entry-slippage variant."""
	with pytest.raises(ValueError, match=r"\['take_profit'\] must be >= 0"):
		validate_exec_cfg({
			"entry_slippage_cents": 2,
			"exit_slippage_cents": {"take_profit": -1, "stop_loss": 10, "time_exit": 5},
		})


# --------------------------------------------------------------------------
# Test #20 — _make_client_order_id collision-safety + length budget
# --------------------------------------------------------------------------


def test_make_client_order_id_distinct_in_same_millisecond() -> None:
	"""Failure mode (round-4 review): two signals scheduled within the same
	WS-frame millisecond generate identical client_order_ids; Kalshi's
	idempotency layer returns the SAME Order object for both POSTs,
	silently double-counting one fill. The uuid4 suffix is the regression
	guard — without it, this test fails."""
	sig = _entry_signal()
	a = _make_client_order_id(sig, _NOW)
	b = _make_client_order_id(sig, _NOW)
	assert a != b, (
		"client_order_id must be unique per call even when (Signal, now) "
		"are identical — uuid4 suffix MUST be present"
	)


def test_make_client_order_id_format_matches_kalshi_regex() -> None:
	"""Failure mode: a stray space or punctuation slips into the format,
	and Kalshi's POST /orders 4xx-rejects on client_order_id validation.
	Asserts the value matches the {1,80}-char URL-safe regex pinned in
	live/client.py:_CLIENT_ORDER_ID_PATTERN."""
	sig = _entry_signal()
	oid = _make_client_order_id(sig, _NOW)
	# Mirrors live.client._CLIENT_ORDER_ID_PATTERN.
	assert re.match(r"^[A-Za-z0-9_-]{1,80}$", oid), (
		f"client_order_id {oid!r} fails Kalshi-side regex"
	)


def test_make_client_order_id_within_80_char_worst_case() -> None:
	"""Failure mode: a long strategy name + long ticker pushes the ID past
	Kalshi's 80-char ceiling and the POST is rejected. Worst case spec'd
	at ~70 chars; this assertion gives 10 chars of headroom.

	Uses synthetic worst-case strings (NOT real strategy/ticker names) so
	this tracked test file doesn't leak internal strategy iteration history.
	The 24-char strategy + 24-char ticker exercises the same length budget
	as any real worst-case pairing.
	"""
	# Worst-case strategy name + worst-case ticker — synthetic, not real names.
	sig = _entry_signal(
		ticker="KXTESTSERIESLONG-T12M-NL",  # 24 chars (matches longest real-series shape)
		strategy="test_strategy_long_name",  # 23 chars
	)
	oid = _make_client_order_id(sig, _NOW)
	assert len(oid) <= 80, (
		f"client_order_id length {len(oid)} exceeds Kalshi's 80-char ceiling: {oid!r}"
	)


def test_make_client_order_id_embeds_strategy_and_ticker() -> None:
	"""Failure mode: the format silently drops a required component (e.g.
	strategy), reducing operator-grep ability in audit logs. The format
	contract is binding — strategy and ticker MUST be present."""
	sig = _entry_signal(strategy="debut_fade", ticker="KXSOL15M-26MAY09H06")
	oid = _make_client_order_id(sig, _NOW)
	assert oid.startswith("debut_fade-KXSOL15M-26MAY09H06-")


# --------------------------------------------------------------------------
# Test #21 — _series_of parsing
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
	"ticker,expected_series",
	[
		("KXSOL15M-26MAY09H06", "KXSOL15M"),
		("KXSPOTIFYARTISTD-T12M", "KXSPOTIFYARTISTD"),
		# Edge case — no hyphen at all. Defensive: return whole string.
		("KXNOEVENT", "KXNOEVENT"),
		# Edge case — multiple hyphens. Only the FIRST split is taken.
		("KX-EVENT-SUFFIX-MORE", "KX"),
	],
)
def test_series_of_parses_kalshi_ticker(ticker: str, expected_series: str) -> None:
	"""Failure mode: a future refactor uses ``rsplit("-", 1)`` instead of
	``split("-", 1)`` and returns ``KXSOL15M-26MAY09H06`` → ``26MAY09H06``
	instead of ``KXSOL15M``. The forward-split contract is binding for
	exit-order routing."""
	assert _series_of(ticker) == expected_series
