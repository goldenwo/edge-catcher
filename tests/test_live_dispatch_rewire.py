"""Dispatch pre-place ``record_intent`` rewire tests (sub-project E / L1).

Pins the §3 keystone + §3.1 funds-at-risk safety property:

* §3 / §4.2 (L1, structural): ``_handle_enter`` calls
  ``store.record_intent(...)`` UNCONDITIONALLY (no mode branch) IMMEDIATELY
  before ``await executor.place(req)``. The protocol absorbs paper/live:
  paper = strict no-op, live = pre-place durable ``pending`` INSERT keyed by
  ``client_order_id``. This makes every severed place→persist recoverable by
  B's reconciler — "no untracked real-money position" holds even if async
  code is imperfect.

* §3.1 (FATAL pre-place): a ``record_intent`` failure raises
  ``RecordPendingFailed`` which MUST propagate so the entry ABORTS BEFORE
  ``await executor.place(req)`` — no order is sent, nothing is at risk, a
  hard stop strands nothing. This is STRONGER than the old post-place
  ghost-reject (the order was already on Kalshi there). The exception must
  NOT be swallowed by a new try/except around ``record_intent`` — it
  propagates to ``process_tick``'s existing
  ``except RecordPendingFailed: raise`` ghost-reject site.

* §9 G-parity: the unconditional call is byte-exact-invisible for
  paper/replay (A2's ``record_intent`` is ``return None``). The paper-no-op
  test pins that dispatch does NOT branch on mode and a no-op store changes
  nothing.

Scope boundary: ``test_engine_dispatch_pending_branch.py`` pins the
post-place pending-branch kwarg contract; THIS file pins the PRE-place
``record_intent`` call ordering + fatal propagation — disjoint surfaces.
Harness (``_live_entry_signal`` / ``_ctx`` / ``_config_with_metrics``)
mirrors that file so the two dispatch test rigs stay in lock-step.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from edge_catcher.engine.dispatch import _handle_enter
from edge_catcher.engine.executor import OrderResult
from edge_catcher.engine.strategy_base import Signal

# RecordPendingFailed is the real exception the live store raises from a
# failed pre-place INSERT. Import it the same runtime/sentinel way dispatch
# does so this test pins the ACTUAL ghost-reject type, not a local stand-in.
from edge_catcher.engine.dispatch import RecordPendingFailed

_NOW = datetime(2026, 5, 11, 12, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# _OrderRecordingStore — records the ORDER of lifecycle calls by name.
#
# A single ordered log across record_intent / record_trade / record_pending /
# record_rejected lets us assert the exact call SEQUENCE dispatch produces
# (record_intent strictly BEFORE place; place before the filled-arm
# record_trade). Hand-rolled (not MagicMock) so a missing call is a visible
# gap in `.calls`, never a silently-auto-created attribute.
# ---------------------------------------------------------------------------


class _OrderRecordingStore:
	"""Captures lifecycle call order; record_intent is a no-op by default
	(paper-style) unless ``intent_raises`` is set (live-failure simulation)."""

	def __init__(self, *, intent_raises: BaseException | None = None) -> None:
		self.calls: list[str] = []
		self.intent_kwargs: list[dict[str, Any]] = []
		self._intent_raises = intent_raises

	def record_intent(self, **kwargs: Any) -> None:
		# Record the kwargs BEFORE a (simulated) failure so the fatal test can
		# still assert dispatch built the call correctly even when it raises.
		self.intent_kwargs.append(dict(kwargs))
		self.calls.append("record_intent")
		if self._intent_raises is not None:
			raise self._intent_raises

	def record_trade(self, **kwargs: Any) -> int:
		self.calls.append("record_trade")
		return 1  # synthetic trade id

	def record_pending(self, **kwargs: Any) -> None:
		self.calls.append("record_pending")

	def record_rejected(self, **kwargs: Any) -> None:
		self.calls.append("record_rejected")


def _live_entry_signal(
	*,
	ticker: str = "KXSOL15M-26MAY09H06",
	series: str = "KXSOL15M",
	side: str = "yes",
	strategy: str = "debut_fade",
	entry_price_cents: int = 42,
	stop_loss_distance_cents: int = 8,
) -> Signal:
	"""Entry Signal with all live-execution fields populated (mirrors
	test_engine_dispatch_pending_branch._live_entry_signal)."""
	return Signal(
		action="enter",
		ticker=ticker,
		side=side,
		series=series,
		strategy=strategy,
		reason="live-entry-signal",
		entry_price_cents=entry_price_cents,
		stop_loss_distance_cents=stop_loss_distance_cents,
	)


def _ctx(yes_ask: int = 42, no_ask: int = 58) -> MagicMock:
	"""Minimal TickContext stub — dispatch reads yes_ask/no_ask/orderbook
	before delegating to executor.place (mirrors the sibling test file)."""
	return MagicMock(
		yes_ask=yes_ask,
		no_ask=no_ask,
		orderbook=MagicMock(depth=5),
	)


def _config_with_metrics() -> dict[str, Any]:
	return {"_metrics": MagicMock()}


def _filled_result() -> OrderResult:
	return OrderResult(
		status="filled",
		intended_size=10,
		filled_size=10,
		blended_entry_cents=42,
		fill_pct=1.0,
		slippage_cents=0,
		book_depth=5,
		book_snapshot="[]",
	)


# ---------------------------------------------------------------------------
# (1) Unconditional call ORDER: record_intent → place → record_trade
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_record_intent_called_before_place_then_record_trade() -> None:
	"""Failure mode prevented: dispatch places the order BEFORE durably
	recording intent — a severed place→persist then strands a real-money
	Kalshi order with no row for B's reconciler to find (§3.1 funds-at-risk).

	Asserts the exact sequence: record_intent (pre-place) → executor.place →
	record_trade (filled arm)."""
	store = _OrderRecordingStore()
	seq: list[str] = []

	async def _place(_req: Any) -> OrderResult:
		seq.append("place")
		return _filled_result()

	executor = MagicMock()
	executor.place = AsyncMock(side_effect=_place)
	# Interleave the store's call log with the place marker via a shared list.
	store.calls = seq

	await _handle_enter(_live_entry_signal(), _ctx(), store, _config_with_metrics(), executor, now=_NOW)

	assert seq == ["record_intent", "place", "record_trade"], (
		"record_intent MUST be called UNCONDITIONALLY and strictly BEFORE "
		f"executor.place, then record_trade on the filled arm; got {seq!r}"
	)
	# record_intent built with the verified Protocol kwargs sourced correctly.
	assert len(store.intent_kwargs) == 1
	k = store.intent_kwargs[0]
	assert k["ticker"] == "KXSOL15M-26MAY09H06"
	assert k["series"] == "KXSOL15M"
	assert k["strategy"] == "debut_fade"
	assert k["side"] == "yes"
	# Pre-sizing PLACEHOLDER — same sizing-deferred convention as the
	# engine-timeout pending row (dispatch.py:370-378): req.size_contracts==0.
	assert k["intended_size"] == 0
	# entry_price_cents / stop_loss_distance_cents = ORIGINAL Signal intent.
	assert k["entry_price_cents"] == 42
	assert k["stop_loss_distance_cents"] == 8
	# client_order_id is the one minted into the OrderRequest pre-place.
	assert isinstance(k["client_order_id"], str)
	assert k["client_order_id"].startswith("debut_fade-KXSOL15M-26MAY09H06-")
	# placed_at_utc is the THREADED `now` as an ISO-8601 string (B parses it).
	assert k["placed_at_utc"] == _NOW.isoformat()
	parsed = datetime.fromisoformat(k["placed_at_utc"])
	assert parsed.tzinfo is not None


# ---------------------------------------------------------------------------
# (2) Paper-no-op keystone: dispatch does NOT branch on mode
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_paper_style_noop_store_same_order_no_error() -> None:
	"""Failure mode prevented: dispatch grows a mode branch (``if live:``)
	around record_intent. The §1 keystone is that the PROTOCOL absorbs the
	difference — paper's record_intent is a strict no-op and dispatch calls
	it unconditionally. With a no-op store the sequence + behavior are
	IDENTICAL to the live-store path and nothing raises (G-parity safety:
	the call is byte-exact-invisible to paper/replay)."""
	store = _OrderRecordingStore()  # record_intent = no-op (intent_raises=None)
	seq: list[str] = []

	async def _place(_req: Any) -> OrderResult:
		seq.append("place")
		return _filled_result()

	executor = MagicMock()
	executor.place = AsyncMock(side_effect=_place)
	store.calls = seq

	# Must not raise — paper no-op record_intent has zero side effects.
	await _handle_enter(_live_entry_signal(), _ctx(), store, _config_with_metrics(), executor, now=_NOW)

	assert seq == ["record_intent", "place", "record_trade"], (
		"With a paper-style no-op store the call order is UNCHANGED — dispatch "
		f"must not branch on mode (§1 keystone); got {seq!r}"
	)


# ---------------------------------------------------------------------------
# (3) §3.1 FATAL: record_intent failure aborts BEFORE place (nothing sent)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_record_intent_failure_aborts_before_place_and_propagates() -> None:
	"""Failure mode prevented (THE funds-at-risk safety property): a failed
	pre-place persistence does NOT abort the entry, so executor.place still
	sends a real-money order that has no durable local row — exactly the
	ghost-reject the pre-place INSERT exists to prevent.

	Asserts: RecordPendingFailed propagates out of _handle_enter (entry
	aborts) AND executor.place was NEVER awaited (no order sent ⇒ a hard
	engine stop strands nothing). This is STRONGER than the post-place
	ghost-reject. The exception must reach process_tick's existing
	``except RecordPendingFailed: raise`` site — dispatch must NOT wrap
	record_intent in a RecordPendingFailed-catching try/except."""
	store = _OrderRecordingStore(intent_raises=RecordPendingFailed("simulated pre-place INSERT failure"))
	executor = MagicMock()
	executor.place = AsyncMock(return_value=_filled_result())

	with pytest.raises(RecordPendingFailed):
		await _handle_enter(_live_entry_signal(), _ctx(), store, _config_with_metrics(), executor, now=_NOW)

	# No order was sent — the entry aborted BEFORE executor.place.
	executor.place.assert_not_awaited()
	# record_intent was attempted (and is the LAST/only lifecycle call) — no
	# record_trade / record_pending followed because the entry aborted.
	assert store.calls == ["record_intent"], (
		"record_intent must be the only lifecycle call — the raise aborts the "
		f"entry before place and any post-place store write; got {store.calls!r}"
	)
	# Dispatch still built the call correctly even though it raised.
	assert len(store.intent_kwargs) == 1
	assert store.intent_kwargs[0]["intended_size"] == 0
