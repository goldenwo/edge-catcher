"""Dispatch pending-branch tests — pin the D↔B record_pending kwarg contract.

This test file is the ONLY guard against D-vs-B drift before PR 5 (B's
state machine) merges. It asserts that dispatch's pending-branch handler
calls ``store.record_pending`` with EVERY kwarg the locked signature
defines, with the value sourced from the RIGHT place:

* identity → from the Signal (``ticker``, ``series``, ``strategy``,
  ``side``, ``stop_loss_distance_cents``)
* original strategy intent → ``entry_price_cents`` from the Signal
  (NOT D's slippage-adjusted ``limit_price_cents``)
* order state → from D's OrderResult (``intended_size``, ``rejection_reason``)
* identifiers → ``client_order_id`` from the OrderRequest D built,
  ``kalshi_order_id`` from OrderResult.order_id (None on NetworkError,
  preserved on malformed-fills)
* timestamp → ISO-8601 UTC string via ``datetime.now(timezone.utc).isoformat()``

A future refactor that swaps ``entry_price_cents`` → ``limit_price_cents``,
renames a kwarg, or skips the call on NetworkError will surface here as a
loud failure BEFORE PR 5 lands and B's reconciler reads the row.

Scope boundary: ``tests/test_engine_dispatch_executor_wiring.py`` asserts
THAT ``executor.place`` was called and with what shape. THIS file asserts
WHAT happens AFTER on the pending branch (record_pending kwargs) —
disjoint assertion surfaces.

Test inventory (per agent 3b.C scope, D spec L673-L685):
* (a) NetworkError → record_pending with kalshi_order_id=None,
      rejection_reason="kalshi_unreachable:..."
* (b) Malformed-fills → record_pending with kalshi_order_id=<preserved>,
      rejection_reason="kalshi_malformed_fills"
* (c) entry_price_cents passes through verbatim from Signal — NOT mutated
      by D's builder; proves L679 invariant
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from edge_catcher.engine.dispatch import _handle_enter
from edge_catcher.engine.executor import OrderResult
from edge_catcher.engine.strategy_base import Signal


_NOW = datetime(2026, 5, 11, 12, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# StubStore — captures record_pending kwargs verbatim
#
# We deliberately use a hand-rolled stub instead of MagicMock.record_pending
# because B's record_pending signature is THE cross-PR contract. A typo in
# the assertion (e.g. asserting kwargs.get("entry_price_cents", "MISSING") ==
# Signal value) would silently pass on a missing kwarg — the stub forces
# kwarg presence by KeyError-ing into the dict.
# ---------------------------------------------------------------------------


class _RecordPendingCalls(list):
	"""List of recorded kwargs dicts; raises on attribute access miss."""

	def __getattr__(self, key: str) -> Any:
		raise AttributeError(
			f"_RecordPendingCalls is a list, not an object: tried .{key}"
		)


class _StubStore:
	"""Stand-in for B's eventual TradeStore that captures record_pending +
	record_rejected kwargs.

	Also provides record_trade (no-op) so the dispatch handler's existing
	filled-branch code path doesn't blow up during these tests — but we only
	assert against record_pending / record_rejected. Any test that triggers
	record_trade is out-of-scope for this file (covered by
	test_engine_dispatch_executor_wiring).
	"""

	def __init__(self) -> None:
		self.pending_calls: _RecordPendingCalls = _RecordPendingCalls()
		self.rejected_calls: list[dict[str, Any]] = []
		self.trade_calls: list[dict[str, Any]] = []

	def record_trade(self, **kwargs: Any) -> int:
		self.trade_calls.append(kwargs)
		return 1  # synthetic trade id

	def get_trade_by_id(self, trade_id: int) -> dict[str, Any]:
		"""Paper-shaped no-op read: a just-record_trade'd id reads back as an
		'open' row (paper TradeStore INSERTs literal 'open'). Present so
		dispatch's D2 filled-arm durable-status re-read (spec §4.2 / §1
		keystone) doesn't AttributeError — the same test-harness-conformance
		analogue as the record_intent no-op above (D1). Returning 'open' keeps
		the filled path on the pre-D2 celebratory branch, so these
		pending/rejected-focused tests assert UNCHANGED behavior (NO side
		effect on what this file pins)."""
		return {"id": trade_id, "status": "open"}

	def record_intent(self, **kwargs: Any) -> None:
		"""No-op: paper/replay have no pre-place state (synchronous fills).
		Present so dispatch's unconditional pre-place call (spec §3 keystone /
		D1) doesn't AttributeError — the test-harness analogue of the paper
		``TradeStoreProtocol.record_intent`` no-op. These tests assert
		paper-shaped dispatch behavior, so this introduces NO side effect."""
		return None

	def record_pending(self, **kwargs: Any) -> None:
		"""Capture kwargs verbatim. B's eventual implementation writes a row;
		our stub just records what dispatch passed."""
		self.pending_calls.append(dict(kwargs))

	def record_rejected(self, **kwargs: Any) -> None:
		"""Mirror of record_pending capture for the rejected-branch audit row."""
		self.rejected_calls.append(dict(kwargs))


def _make_pending_result(
	*,
	intended_size: int = 10,
	filled_size: int = 0,
	order_id: str | None = None,
	rejection_reason: str = "kalshi_unreachable:test",
	blended_entry_cents: int = 0,
	fill_pct: float = 0.0,
) -> OrderResult:
	"""Build a pending OrderResult covering the two D paths.

	* NetworkError path: order_id=None, blended=0, filled_size=0
	* Malformed-fills path: order_id="ord-...", blended=0, filled_size>0
	"""
	return OrderResult(
		status="pending",
		intended_size=intended_size,
		filled_size=filled_size,
		blended_entry_cents=blended_entry_cents,
		fill_pct=fill_pct,
		slippage_cents=0,
		order_id=order_id,
		rejection_reason=rejection_reason,
	)


def _live_entry_signal(
	*,
	ticker: str = "KXSOL15M-26MAY09H06",
	series: str = "KXSOL15M",
	side: str = "yes",
	strategy: str = "debut_fade",
	entry_price_cents: int = 42,
	stop_loss_distance_cents: int = 8,
) -> Signal:
	"""Build an entry Signal with ALL live-execution fields populated.

	The pending branch contract requires entry_price_cents and
	stop_loss_distance_cents to pass through to record_pending verbatim —
	tests that omit them are exercising a different invariant (paper
	compatibility, covered by test_engine_dispatch_executor_wiring).
	"""
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
	"""Minimal TickContext stub — dispatch only reads yes_ask / no_ask /
	orderbook.depth on the pending path before delegating to executor.place."""
	return MagicMock(
		yes_ask=yes_ask,
		no_ask=no_ask,
		orderbook=MagicMock(depth=5),
	)


def _config_with_metrics() -> dict[str, Any]:
	return {"_metrics": MagicMock()}


# ---------------------------------------------------------------------------
# (a) NetworkError → record_pending with order_id=None
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_network_error_writes_pending_with_kalshi_order_id_none() -> None:
	"""Failure mode prevented: D returns ``status=pending, order_id=None`` on
	NetworkError but dispatch silently drops the row (the pending branch is
	a bare ``pass`` as it was pre-D). B can never reconcile a phantom Kalshi
	order because there's no row to look up — funds-at-risk.

	Asserts that record_pending IS called and that ``kalshi_order_id=None``
	flows through correctly (B's reconciler discriminates on this to fall
	back to client_order_id lookup)."""
	store = _StubStore()
	executor = MagicMock()
	executor.place = AsyncMock(return_value=_make_pending_result(
		order_id=None,
		rejection_reason="kalshi_unreachable:connection refused",
	))
	sig = _live_entry_signal()

	await _handle_enter(sig, _ctx(), store, _config_with_metrics(), executor, now=_NOW)

	assert len(store.pending_calls) == 1, (
		"record_pending MUST be called on NetworkError path — funds-at-risk "
		"reconciliation depends on the row existing"
	)
	kwargs = store.pending_calls[0]
	# Identity fields — from Signal
	assert kwargs["ticker"] == "KXSOL15M-26MAY09H06"
	assert kwargs["series"] == "KXSOL15M"
	assert kwargs["strategy"] == "debut_fade"
	assert kwargs["side"] == "yes"
	assert kwargs["stop_loss_distance_cents"] == 8
	# Order state — from OrderResult
	assert kwargs["intended_size"] == 10
	assert kwargs["rejection_reason"] == "kalshi_unreachable:connection refused"
	# entry_price_cents is the ORIGINAL strategy intent (NOT D's slippage-
	# adjusted limit). Pinned per D spec L679.
	assert kwargs["entry_price_cents"] == 42
	# kalshi_order_id is None — B reconciles via client_order_id
	assert kwargs["kalshi_order_id"] is None
	# client_order_id is the one D built and sent to Kalshi
	assert isinstance(kwargs["client_order_id"], str)
	assert kwargs["client_order_id"].startswith("debut_fade-KXSOL15M-26MAY09H06-")
	# placed_at_utc is an ISO-8601 UTC string (B parses it)
	assert isinstance(kwargs["placed_at_utc"], str)
	# Must round-trip through fromisoformat (defense against operator typos
	# like raw datetime objects sneaking in)
	parsed = datetime.fromisoformat(kwargs["placed_at_utc"])
	assert parsed.tzinfo is not None
	# record_trade is NOT called on the pending path
	assert store.trade_calls == []


# ---------------------------------------------------------------------------
# (b) Malformed-fills → record_pending with kalshi_order_id preserved
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_malformed_fills_writes_pending_with_kalshi_order_id_preserved() -> None:
	"""Failure mode prevented: malformed-fills produces ``status=pending`` BUT
	with a real Kalshi-side order_id (Kalshi accepted the order; we just
	can't trust the fills array). If dispatch DROPS the order_id when calling
	record_pending, B's reconciler can't fetch the order by ID — it has to
	guess via client_order_id, slower and less reliable.

	Asserts that kalshi_order_id is preserved from OrderResult.order_id."""
	store = _StubStore()
	executor = MagicMock()
	executor.place = AsyncMock(return_value=_make_pending_result(
		intended_size=10,
		filled_size=5,  # Kalshi reported partial fill
		order_id="ord-kx-malformed-abc-123",
		rejection_reason="kalshi_malformed_fills",
		blended_entry_cents=0,
		fill_pct=0.5,
	))
	sig = _live_entry_signal(entry_price_cents=42)

	await _handle_enter(sig, _ctx(), store, _config_with_metrics(), executor, now=_NOW)

	assert len(store.pending_calls) == 1
	kwargs = store.pending_calls[0]
	assert kwargs["rejection_reason"] == "kalshi_malformed_fills"
	# kalshi_order_id is preserved — this is the only path where it flows
	# from D's OrderResult.order_id into B's storage. If a future refactor
	# accidentally hardcodes None here, B's reconciler degrades silently.
	assert kwargs["kalshi_order_id"] == "ord-kx-malformed-abc-123"
	# intended_size from OrderResult (NOT signal — D's pipeline post-sizing)
	assert kwargs["intended_size"] == 10
	# entry_price_cents from Signal — original intent, untouched
	assert kwargs["entry_price_cents"] == 42


# ---------------------------------------------------------------------------
# (c) entry_price_cents pass-through invariant
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_entry_price_cents_passes_through_verbatim_not_mutated_by_builder() -> None:
	"""Failure mode prevented: a future refactor swaps Signal.entry_price_cents
	for OrderRequest.limit_price_cents (D's slippage-adjusted value) when
	calling record_pending. The two are deliberately different — D walks the
	limit up to honour entry_slippage_cents, but B's reconciler needs the
	STRATEGY'S original intended entry to compute correct PnL on settlement.

	If this test fails, search the dispatch.py edit for ``limit_price_cents``
	in the record_pending call — that's the wrong field. Per D spec L679."""
	store = _StubStore()
	executor = MagicMock()
	# D's pipeline would walk entry=42 + slippage=2 → limit=44. We don't
	# care about D's slippage logic here — we care that whatever D builds,
	# dispatch passes the ORIGINAL ``Signal.entry_price_cents`` (42) to
	# record_pending, NOT D's limit_price_cents (44).
	executor.place = AsyncMock(return_value=_make_pending_result(
		order_id=None,
		rejection_reason="kalshi_unreachable:test",
	))
	sig = _live_entry_signal(entry_price_cents=42)

	await _handle_enter(sig, _ctx(), store, _config_with_metrics(), executor, now=_NOW)

	assert len(store.pending_calls) == 1
	kwargs = store.pending_calls[0]
	# Pinned: the value in record_pending is Signal.entry_price_cents,
	# NOT the OrderRequest.limit_price_cents that D would have produced.
	assert kwargs["entry_price_cents"] == 42, (
		"entry_price_cents must be the ORIGINAL Signal value (42), NOT D's "
		"slippage-adjusted limit_price_cents. Per D spec L679. If this test "
		"fails, the pending-branch kwarg was likely swapped to "
		"req.limit_price_cents — that's wrong; restore sig.entry_price_cents."
	)


# ---------------------------------------------------------------------------
# Sanity: dispatch's filled-path remains untouched after the pending branch
# is added. test_engine_dispatch_executor_wiring already covers filled-path
# wiring; this is a lightweight cross-check that the pending branch doesn't
# accidentally fire on a filled OrderResult.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_filled_path_does_not_call_record_pending() -> None:
	"""Failure mode prevented: the pending-branch addition accidentally also
	fires on the filled path (e.g. via a stale ``elif`` ladder). record_pending
	should ONLY be called when status=="pending"."""
	store = _StubStore()
	executor = MagicMock()
	executor.place = AsyncMock(return_value=OrderResult(
		status="filled",
		intended_size=10,
		filled_size=10,
		blended_entry_cents=42,
		fill_pct=1.0,
		slippage_cents=0,
		book_depth=5,
		book_snapshot="[]",
	))
	sig = _live_entry_signal()

	await _handle_enter(sig, _ctx(), store, _config_with_metrics(), executor, now=_NOW)

	assert store.pending_calls == [], (
		"record_pending must NOT be called on the filled path — only on pending."
	)
	# record_trade WAS called (sanity check)
	assert len(store.trade_calls) == 1


@pytest.mark.asyncio
async def test_rejected_path_does_not_call_record_pending() -> None:
	"""Failure mode prevented: a status="rejected" OrderResult somehow ends up
	flowing into the pending branch (e.g. via a typo in the match arm)."""
	store = _StubStore()
	executor = MagicMock()
	executor.place = AsyncMock(return_value=OrderResult(
		status="rejected",
		intended_size=10,
		filled_size=0,
		blended_entry_cents=0,
		fill_pct=0.0,
		slippage_cents=0,
		rejection_reason="stale_book",
	))
	sig = _live_entry_signal()

	await _handle_enter(sig, _ctx(), store, _config_with_metrics(), executor, now=_NOW)

	assert store.pending_calls == []
	assert store.trade_calls == []


# ---------------------------------------------------------------------------
# (d) placed_at_utc uses the THREADED `now`, not a wall-clock read
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_placed_at_utc_uses_threaded_now_not_wall_clock() -> None:
	"""Failure mode prevented: dispatch's pending branch reads ``datetime.now()``
	internally instead of using the ``now`` parameter threaded down from the
	WS loop / replay dispatcher. The module invariant at L14-L18 explicitly
	forbids handlers from reading the wall clock — during replay, ``now`` is
	sourced from the captured bundle's ``recv_ts`` so replay produces a
	byte-identical ``placed_at_utc`` to the original live execution.

	Without this fix, replaying a pending row from a captured bundle would
	stamp it with replay-time wall clock (today) instead of the original
	live-execution timestamp, breaking replay-live parity for B's
	reconciliation audit trail and any downstream consumer that reads
	``placed_at_utc`` as "when the order was placed at Kalshi".

	The assertion ``placed_at_utc == _NOW.isoformat()`` fails if dispatch
	regresses to ``datetime.now()``: today's wall clock won't equal _NOW
	(which is fixed at 2026-05-11 12:00:00 UTC).
	"""
	store = _StubStore()
	executor = MagicMock()
	executor.place = AsyncMock(return_value=_make_pending_result(
		order_id=None,
		rejection_reason="kalshi_unreachable:connection refused",
	))
	sig = _live_entry_signal()

	await _handle_enter(sig, _ctx(), store, _config_with_metrics(), executor, now=_NOW)

	assert len(store.pending_calls) == 1
	kwargs = store.pending_calls[0]
	assert kwargs["placed_at_utc"] == _NOW.isoformat(), (
		f"placed_at_utc must equal threaded now.isoformat() ({_NOW.isoformat()}) "
		f"— got {kwargs['placed_at_utc']!r}. A regression to datetime.now() would "
		"break replay-live parity per the dispatch module invariant at L14-L18."
	)


# ---------------------------------------------------------------------------
# (d) Kwarg SET drift guard — record_pending kwargs are EXACTLY the locked 11
# ---------------------------------------------------------------------------


_LOCKED_RECORD_PENDING_KWARGS: frozenset[str] = frozenset({
	"ticker",
	"series",
	"strategy",
	"side",
	"intended_size",
	"entry_price_cents",
	"stop_loss_distance_cents",
	"client_order_id",
	"kalshi_order_id",
	"placed_at_utc",
	"rejection_reason",
})


@pytest.mark.asyncio
async def test_record_pending_kwarg_set_is_exactly_locked_eleven() -> None:
	"""Failure mode prevented: a future dispatch refactor adds an experimental
	``notes=`` or ``intended_size_dollars=`` kwarg to ``store.record_pending(...)``
	that B's PR 5 schema doesn't accept. The per-key assertions above pass
	(they only check presence + value), but PR 5 fails at runtime with
	``unexpected keyword argument``. This test locks the SET so a kwarg
	added or removed surfaces here, BEFORE the cross-PR drift hits B.
	"""
	store = _StubStore()
	executor = MagicMock()
	executor.place = AsyncMock(return_value=_make_pending_result(
		order_id="ord-set-test",
		rejection_reason="kalshi_unreachable:test",
	))
	sig = _live_entry_signal()

	await _handle_enter(sig, _ctx(), store, _config_with_metrics(), executor, now=_NOW)

	assert len(store.pending_calls) == 1
	kwargs = store.pending_calls[0]
	actual = set(kwargs.keys())
	missing = _LOCKED_RECORD_PENDING_KWARGS - actual
	extra = actual - _LOCKED_RECORD_PENDING_KWARGS
	assert not missing and not extra, (
		f"record_pending kwarg drift detected. "
		f"Missing: {sorted(missing)!r}. Extra: {sorted(extra)!r}. "
		f"This test is the cross-PR contract between D (this PR's dispatch) "
		f"and B (PR 5's state machine impl) — if the set has legitimately "
		f"changed, update _LOCKED_RECORD_PENDING_KWARGS and bump the spec."
	)
	# Per-kwarg TYPE locking (Reviewer B-F1): the SET test alone misses a
	# refactor that passes the right kwarg name with the wrong type — e.g.
	# ``placed_at_utc=now`` (datetime) instead of ``placed_at_utc=now.isoformat()``
	# (str) — which would silently pass the SET test and break PR 5's SQLite
	# INSERT at runtime. Lock the types here so any value-shape drift surfaces.
	expected_types: dict[str, tuple[type, ...]] = {
		"ticker": (str,),
		"series": (str,),
		"strategy": (str,),
		"side": (str,),
		"intended_size": (int,),
		"entry_price_cents": (int, type(None)),
		"stop_loss_distance_cents": (int, type(None)),
		"client_order_id": (str,),
		"kalshi_order_id": (str, type(None)),
		"placed_at_utc": (str,),
		"rejection_reason": (str, type(None)),
	}
	for key, allowed in expected_types.items():
		assert isinstance(kwargs[key], allowed), (
			f"record_pending kwarg {key!r} has wrong type: "
			f"got {type(kwargs[key]).__name__} ({kwargs[key]!r}), "
			f"expected one of {[t.__name__ for t in allowed]!r}. "
			f"PR 5's SQLite INSERT will reject this at runtime."
		)


# ---------------------------------------------------------------------------
# (e) Rejected branch writes audit row via store.record_rejected
# ---------------------------------------------------------------------------


def _make_rejected_result(*, rejection_reason: str = "kalshi_4xx:400") -> OrderResult:
	"""Rejected OrderResult — Kalshi authoritatively rejected, no order_id."""
	return OrderResult(
		status="rejected",
		intended_size=10,
		filled_size=0,
		blended_entry_cents=0,
		fill_pct=0.0,
		slippage_cents=0,
		rejection_reason=rejection_reason,
		order_id=None,
	)


@pytest.mark.asyncio
async def test_rejected_branch_writes_audit_row() -> None:
	"""Failure mode prevented: a Kalshi 4xx rejection (or absolute_max_exceeded,
	ioc_zero_fill, invalid_intended_size) lands as ``status="rejected"`` but
	dispatch only logged + bumped a metric — no durable audit row. Operators
	triaging "why did my strategy stop trading?" had to grep rotating
	process logs. The dispatch now persists a record_rejected row so F's
	UI surfaces the rejection history and B's reconciler has full audit.

	stale_book is excluded (paper-side path; its existing log+metric is the
	source of truth for paper).
	"""
	store = _StubStore()
	executor = MagicMock()
	executor.place = AsyncMock(return_value=_make_rejected_result(
		rejection_reason="kalshi_4xx:400"
	))
	sig = _live_entry_signal()

	await _handle_enter(sig, _ctx(), store, _config_with_metrics(), executor, now=_NOW)

	assert len(store.rejected_calls) == 1, (
		"record_rejected MUST be called for non-stale_book rejections so F's "
		"UI and operator triage have a durable audit row"
	)
	kwargs = store.rejected_calls[0]
	assert kwargs["ticker"] == "KXSOL15M-26MAY09H06"
	assert kwargs["rejection_reason"] == "kalshi_4xx:400"
	assert kwargs["client_order_id"]  # was generated even though Kalshi rejected
	assert kwargs["placed_at_utc"] == _NOW.isoformat()


@pytest.mark.asyncio
async def test_rejected_stale_book_skips_audit_row() -> None:
	"""Mirror of the above — stale_book rejections (paper-only path) do
	NOT call record_rejected. The existing metric/log is the truth source
	for paper-mode rejections; no live_trades.db row needed."""
	store = _StubStore()
	executor = MagicMock()
	executor.place = AsyncMock(return_value=_make_rejected_result(
		rejection_reason="stale_book"
	))
	sig = _live_entry_signal()

	await _handle_enter(sig, _ctx(), store, _config_with_metrics(), executor, now=_NOW)

	assert len(store.rejected_calls) == 0


# ---------------------------------------------------------------------------
# (e2) record_rejected drift guard — kwarg SET locked to the 10 names
# ---------------------------------------------------------------------------


_LOCKED_RECORD_REJECTED_KWARGS: frozenset[str] = frozenset({
	"ticker",
	"series",
	"strategy",
	"side",
	"intended_size",
	"entry_price_cents",
	"stop_loss_distance_cents",
	"client_order_id",
	"placed_at_utc",
	"rejection_reason",
})


@pytest.mark.asyncio
async def test_record_rejected_kwarg_set_is_exactly_locked_ten() -> None:
	"""Mirror of the record_pending drift guard for the new record_rejected
	Protocol surface. PR 5's SQLite impl must accept exactly these 10 kwargs
	(no kalshi_order_id — rejected = no Kalshi-side order). A future dispatch
	refactor that adds an experimental kwarg would silently pass the per-key
	tests above but break PR 5's INSERT.

	Also locks types so passing wrong-type values (e.g. ``placed_at_utc=now``
	datetime instead of isoformat string) surfaces here rather than at PR 5
	runtime.
	"""
	store = _StubStore()
	executor = MagicMock()
	executor.place = AsyncMock(return_value=_make_rejected_result(
		rejection_reason="kalshi_4xx:400"
	))
	sig = _live_entry_signal()

	await _handle_enter(sig, _ctx(), store, _config_with_metrics(), executor, now=_NOW)

	assert len(store.rejected_calls) == 1
	kwargs = store.rejected_calls[0]
	actual = set(kwargs.keys())
	missing = _LOCKED_RECORD_REJECTED_KWARGS - actual
	extra = actual - _LOCKED_RECORD_REJECTED_KWARGS
	assert not missing and not extra, (
		f"record_rejected kwarg drift detected. "
		f"Missing: {sorted(missing)!r}. Extra: {sorted(extra)!r}."
	)
	expected_types: dict[str, tuple[type, ...]] = {
		"ticker": (str,),
		"series": (str,),
		"strategy": (str,),
		"side": (str,),
		"intended_size": (int,),
		"entry_price_cents": (int, type(None)),
		"stop_loss_distance_cents": (int, type(None)),
		"client_order_id": (str,),
		"placed_at_utc": (str,),
		"rejection_reason": (str,),  # rejected rows ALWAYS carry a reason
	}
	for key, allowed in expected_types.items():
		assert isinstance(kwargs[key], allowed), (
			f"record_rejected kwarg {key!r} has wrong type: "
			f"got {type(kwargs[key]).__name__} ({kwargs[key]!r}), "
			f"expected one of {[t.__name__ for t in allowed]!r}."
		)


# ---------------------------------------------------------------------------
# (f) Unhandled OrderResult.status falls through to the exhaustiveness else
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unhandled_status_logs_and_increments_metric(
	caplog: pytest.LogCaptureFixture,
) -> None:
	"""Failure mode prevented: PR 5 adds a new status literal (e.g.
	``"lost_truth"``) to OrderResult but forgets to add a matching dispatch
	branch. Without the else arm, the new variant silently no-ops with no
	audit row + no notification + no metric — exactly the funds-at-risk
	scenario the protocol-growth invariant warns about. The else arm fires
	a loud log + metric so the dispatch-side miss surfaces immediately.
	"""
	# Bypass the OrderResult Literal type to simulate a future variant.
	bogus = OrderResult.__new__(OrderResult)
	object.__setattr__(bogus, "status", "lost_truth")
	object.__setattr__(bogus, "intended_size", 10)
	object.__setattr__(bogus, "filled_size", 0)
	object.__setattr__(bogus, "blended_entry_cents", 0)
	object.__setattr__(bogus, "fill_pct", 0.0)
	object.__setattr__(bogus, "slippage_cents", 0)
	object.__setattr__(bogus, "book_depth", None)
	object.__setattr__(bogus, "book_snapshot", None)
	object.__setattr__(bogus, "rejection_reason", None)
	object.__setattr__(bogus, "order_id", None)

	metrics = MagicMock()
	config = {"_metrics": metrics}
	store = _StubStore()
	executor = MagicMock()
	executor.place = AsyncMock(return_value=bogus)
	sig = _live_entry_signal()

	import logging
	with caplog.at_level(logging.ERROR, logger="edge_catcher.engine.dispatch"):
		await _handle_enter(sig, _ctx(), store, config, executor, now=_NOW)

	# No audit row should be written (the variant has no defined branch)
	assert len(store.pending_calls) == 0
	assert len(store.rejected_calls) == 0
	# Loud log + metric must fire so the dispatch-side miss is visible
	assert any("unhandled OrderResult.status" in rec.message for rec in caplog.records)
	metrics.inc.assert_any_call("entries_unhandled_status")


# ---------------------------------------------------------------------------
# (g) wait_for timeout on executor.place synthesizes pending+None (B-F6)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_executor_place_timeout_synthesizes_pending(monkeypatch: pytest.MonkeyPatch) -> None:
	"""Failure mode prevented: a bug in ``KalshiOrderClient`` (infinite retry,
	missing sub-timeout) makes ``executor.place`` block forever. The entire
	WS message loop blocks behind dispatch_message → _handle_enter → place.
	The dispatch-layer wait_for caps the call; on timeout we synthesize a
	pending+None row so B's reconciler can resolve via client_order_id
	without the engine grinding to a halt.

	The test monkeypatches the module constant to 0.01s so the test runs
	fast — the production value (60s) is documented at the constant's
	definition in dispatch.py.
	"""
	from edge_catcher.engine import dispatch as dispatch_mod
	monkeypatch.setattr(dispatch_mod, "_ENTRY_PLACEMENT_TIMEOUT_SECONDS", 0.01)

	async def _hang(req: Any) -> OrderResult:
		import asyncio as _aio
		await _aio.sleep(10)  # never completes within the test
		raise AssertionError("unreachable")

	store = _StubStore()
	executor = MagicMock()
	executor.place = _hang
	sig = _live_entry_signal()

	await _handle_enter(sig, _ctx(), store, _config_with_metrics(), executor, now=_NOW)

	# Pending row synthesized at the dispatch layer
	assert len(store.pending_calls) == 1
	kwargs = store.pending_calls[0]
	assert kwargs["rejection_reason"] is not None
	assert kwargs["rejection_reason"].startswith("engine_timeout:")
	assert kwargs["kalshi_order_id"] is None


@pytest.mark.asyncio
async def test_executor_place_timeout_with_real_live_executor(
	monkeypatch: pytest.MonkeyPatch,
) -> None:
	"""Reviewer A-F4: the previous test uses a hand-rolled ``_hang`` coroutine
	that NEVER goes through ``LiveExecutor.place``'s
	``except asyncio.CancelledError: raise`` carve-out. ``asyncio.wait_for``
	cancels the wrapped coroutine on timeout — the carve-out re-raises
	CancelledError, and Python's wait_for must translate that to TimeoutError.
	If a future refactor breaks that translation (e.g. the carve-out catches
	a different exception class first), dispatch would see CancelledError
	instead of TimeoutError and the broad ``except asyncio.TimeoutError`` arm
	would NOT fire — funds-at-risk. This test exercises the real LiveExecutor
	→ wait_for cancellation → TimeoutError → pending-synthesis chain.
	"""
	import asyncio as _aio

	from edge_catcher.engine import dispatch as dispatch_mod
	from edge_catcher.engine.executors.live import LiveExecutor

	monkeypatch.setattr(dispatch_mod, "_ENTRY_PLACEMENT_TIMEOUT_SECONDS", 0.05)

	class _HangingKalshiClient:
		"""Real-shape Kalshi client whose .place() hangs forever — exercises
		the LiveExecutor.place catch-all + CancelledError carve-out under
		wait_for cancellation."""

		def __init__(self) -> None:
			self.calls: list = []

		async def place(self, req):  # type: ignore[no-untyped-def]
			self.calls.append(req)
			await _aio.sleep(10)  # never completes
			raise AssertionError("unreachable")

	hanging_client = _HangingKalshiClient()
	live_executor = LiveExecutor(hanging_client)  # type: ignore[arg-type]

	store = _StubStore()
	sig = _live_entry_signal()

	start = _aio.get_event_loop().time()
	await _handle_enter(sig, _ctx(), store, _config_with_metrics(), live_executor, now=_NOW)
	elapsed = _aio.get_event_loop().time() - start

	# Prompt return — control got back to dispatch before the 10s sleep
	# completed. Without wait_for translating CancelledError → TimeoutError,
	# dispatch's except branch wouldn't fire and we'd hang.
	assert elapsed < 1.0, (
		f"_handle_enter took {elapsed:.2f}s — expected prompt return after "
		"wait_for cancellation. CancelledError may be propagating instead "
		"of being translated to TimeoutError."
	)
	# Pending row synthesized — proves the dispatch-layer ``except
	# asyncio.TimeoutError`` arm fired (NOT a CancelledError leak).
	assert len(store.pending_calls) == 1
	kwargs = store.pending_calls[0]
	assert kwargs["rejection_reason"] is not None
	assert kwargs["rejection_reason"].startswith("engine_timeout:")
	# LiveExecutor was actually called (not short-circuited).
	assert len(hanging_client.calls) == 1
