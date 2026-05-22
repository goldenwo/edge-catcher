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
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from edge_catcher.engine import dispatch as _dispatch_mod
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

	def get_trade_by_id(self, trade_id: int) -> dict[str, Any]:
		# Paper-style stub: a just-record_trade'd id ALWAYS reads back as an
		# 'open' row (paper TradeStore INSERTs literal 'open'). This is the
		# G-parity basis for D2's mode-agnostic durable-status notify branch —
		# the 'open' arm (the pre-D2 celebratory path) fires here. NOT appended
		# to `.calls`: that log tracks lifecycle WRITES (record_intent / place /
		# record_trade) whose ORDER D1's tests pin; a read is transparent to it
		# so D1's exact-sequence assertions stay valid unchanged by D2.
		return {"id": trade_id, "status": "open"}

	def record_pending(self, **kwargs: Any) -> None:
		self.calls.append("record_pending")

	def record_rejected(self, **kwargs: Any) -> None:
		self.calls.append("record_rejected")


def _live_entry_signal(
	*,
	ticker: str = "KXSOL15M-26MAY09H06",
	series: str = "KXSOL15M",
	side: str = "yes",
	strategy: str = "strat_34",
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
	assert k["strategy"] == "strat_34"
	assert k["side"] == "yes"
	# Pre-sizing PLACEHOLDER — same sizing-deferred convention as the
	# engine-timeout pending row (dispatch.py:370-378): req.size_contracts==0.
	assert k["intended_size"] == 0
	# entry_price_cents / stop_loss_distance_cents = ORIGINAL Signal intent.
	assert k["entry_price_cents"] == 42
	assert k["stop_loss_distance_cents"] == 8
	# client_order_id is the one minted into the OrderRequest pre-place.
	assert isinstance(k["client_order_id"], str)
	assert k["client_order_id"].startswith("strat_34-KXSOL15M-26MAY09H06-")
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


# ===========================================================================
# D2 — filled-arm identity-key threading + lost-CAS false-"filled" suppression
#
# Part 1 (§3 `:400 filled` row): dispatch's filled arm calls the EXISTING
# record_trade UNCONDITIONALLY (no mode branch — §1 keystone) but must now
# pass `client_order_id=req.client_order_id` + `kalshi_order_id=result.order_id`
# so the live store can CAS-transition the C1 pending row pending→open keyed
# by client_order_id and record the Kalshi id. Paper/InMemory accept-and-ignore
# the two kwargs (C2) ⇒ G-parity safe.
#
# Part 2 (§4.2 / §3.1 — the C2-code-review lost-CAS race): B's reconciler can
# transition the C1 row pending→rejected_post_hoc (Kalshi-truth: TTL elapsed,
# `list_orders` found no order) BEFORE dispatch's filled branch runs. The live
# record_trade→transition_pending_to_open is a CAS on WHERE status='pending'
# so it correctly NO-OPs (durable money state authoritative & untouched,
# exactly one row) — NOT a fund-loss. But record_trade still returns the
# located row_id, so a naive filled arm would log "entry filled" + fire the
# celebratory notify() for a row the durable record holds as
# rejected_post_hoc. D2 makes the notification reflect the DURABLE PERSISTED
# status (re-read via get_trade_by_id) — mode-agnostically (branch on
# persisted truth, never on paper-vs-live). 'open' ⇒ celebratory notify
# byte-identical to pre-D2 (paper ALWAYS yields 'open' here ⇒ K2 byte-exact);
# non-'open' ⇒ a DISTINCT non-celebratory record, never raise (operator-trust,
# not fatal — money state already authoritative), uniform with the C3/C4/C5
# lost-CAS observability taxonomy.
# ===========================================================================


def _filled_result_with_order_id(order_id: str = "KAL-ORD-D2-001") -> OrderResult:
	"""A `filled` OrderResult carrying the Kalshi order id D's place() always
	returns (executor.py:74 `order_id`). dispatch's filled arm must thread it
	through as record_trade(kalshi_order_id=result.order_id)."""
	return OrderResult(
		status="filled",
		intended_size=10,
		filled_size=10,
		blended_entry_cents=42,
		fill_pct=1.0,
		slippage_cents=0,
		book_depth=5,
		book_snapshot="[]",
		order_id=order_id,
	)


def _make_live_store(tmp_path: Path):
	"""Construct a REAL live SQLiteTradeStore over a tmp live_trades.db (same
	construction E uses — class + path, mirroring paper TradeStore.__init__).
	Imported lazily so paper/replay-only collection never imports live.*."""
	from edge_catcher.live.store import SQLiteTradeStore

	return SQLiteTradeStore(tmp_path / "live_trades.db")


# ---------------------------------------------------------------------------
# D2 Part 1 — filled arm threads client_order_id + kalshi_order_id into the
# EXISTING record_trade call; the real live store CAS-transitions C1
# pending→open keyed by client_order_id. End-to-end against a real store.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_filled_arm_threads_identity_keys_and_cas_transitions_to_open(
	tmp_path: Path,
) -> None:
	"""Failure mode prevented: dispatch's filled arm calls record_trade WITHOUT
	client_order_id/kalshi_order_id, so the live store cannot locate the C1
	pending row to CAS pending→open — the durable row stays 'pending' forever
	(unreconcilable funds-at-risk state) or record_trade raises.

	Drives the REAL live SQLiteTradeStore: D1's unconditional pre-place
	record_intent seeds the C1 'pending' row keyed by client_order_id; the
	filled arm must thread client_order_id=req.client_order_id +
	kalshi_order_id=result.order_id so the live record_trade CAS-transitions
	exactly that row to 'open' (one row, kalshi id recorded). No mode branch."""
	store = _make_live_store(tmp_path)
	try:
		executor = MagicMock()
		executor.place = AsyncMock(return_value=_filled_result_with_order_id("KAL-FILL-1"))

		await _handle_enter(
			_live_entry_signal(), _ctx(), store, _config_with_metrics(), executor, now=_NOW
		)

		# Exactly ONE durable row for this entry — no competing insert.
		rows = store._conn.execute(
			"SELECT id, status, client_order_id, kalshi_order_id "
			"FROM live_trades"
		).fetchall()
		assert len(rows) == 1, (
			f"the filled arm must CAS-transition the single C1 row, never "
			f"insert a second; got {rows!r}"
		)
		row_id, status, coid, kalshi_id = rows[0]
		# CAS pending→open landed — proves record_trade received the correct
		# client_order_id (it locates the C1 row by it) AND kalshi_order_id
		# (the live filled path requires a real one or raises).
		assert status == "open", (
			f"durable row must be 'open' after the filled arm CAS — record_trade "
			f"must have been passed client_order_id+kalshi_order_id; got {status!r}"
		)
		assert coid is not None and coid.startswith("strat_34-KXSOL15M-26MAY09H06-")
		assert kalshi_id == "KAL-FILL-1", (
			"kalshi_order_id on the durable row must be the executor result's "
			f"order_id threaded through record_trade; got {kalshi_id!r}"
		)
		# get_trade_by_id (mode-agnostic re-read D2's notify branch uses) sees
		# the same durable 'open' status.
		durable = store.get_trade_by_id(row_id)
		assert durable is not None and durable["status"] == "open"
	finally:
		store.close()


# ---------------------------------------------------------------------------
# D2 Part 2 (NORMATIVE) — lost-CAS race: B's reconciler already moved the C1
# row pending→rejected_post_hoc (Kalshi-truth) BEFORE the filled arm. The
# filled arm must NOT fire the celebratory "filled" notify; it must emit a
# DISTINCT non-celebratory record, never raise, and leave the durable row
# (the authoritative money state) UNCHANGED.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_filled_arm_lost_cas_race_suppresses_false_filled_alert(
	tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
	"""Failure mode prevented (operator-TRUST): B's Kalshi-truth reconciler has
	already resolved the C1 row to rejected_post_hoc; an IOC `filled`
	OrderResult then reaches the filled arm. record_trade's CAS correctly
	NO-OPs (money state authoritative, untouched, one row) but returns the
	located row_id — a naive filled arm fires a celebratory "filled" Discord
	alert for a position the durable record holds as REJECTED. D2 reads the
	durable persisted status and suppresses the false alert.

	Asserts: NO celebratory notify(); a DISTINCT non-celebratory record
	(log ERROR carrying client_order_id + the actual status + §4.2 wording);
	no raise; the durable row UNCHANGED (still rejected_post_hoc, one row —
	money state authoritative & untouched: D2 changes ONLY the notification)."""
	from edge_catcher.live.state import transition_pending_to_rejected

	store = _make_live_store(tmp_path)
	try:
		signal = _live_entry_signal()

		# Spy the notify path (dispatch calls the module-level `notify`).
		notified: list[str] = []
		monkeypatch.setattr(_dispatch_mod, "notify", lambda text: notified.append(text))

		# `_make_client_order_id` appends a NON-deterministic uuid4 suffix (by
		# design — collision-safety; execution.py:103-114), so we cannot
		# pre-seed by re-minting the coid. Instead inject B's reconciler race
		# at its REALISTIC moment: dispatch's UNCONDITIONAL pre-place
		# record_intent INSERTs the C1 'pending' row, then `await
		# executor.place(req)` is the window in which B's Kalshi-truth
		# reconciler (TTL elapsed, list_orders found no order) transitions THAT
		# SAME row pending→rejected_post_hoc — BEFORE the filled arm's
		# record_trade runs. The place() side-effect models exactly that
		# ordering on dispatch's own freshly-inserted row.
		captured: dict[str, Any] = {}

		async def _place_then_b_races(req: Any) -> OrderResult:
			# req.client_order_id is the coid dispatch minted + record_intent'd.
			row = store._conn.execute(
				"SELECT id, status FROM live_trades WHERE client_order_id = ?",
				(req.client_order_id,),
			).fetchone()
			assert row is not None, (
				"D1's unconditional pre-place record_intent must have INSERTed "
				"the C1 row before place()"
			)
			captured["coid"] = req.client_order_id
			captured["row_id"] = int(row[0])
			assert row[1] == "pending"
			# B's Kalshi-truth reconciler RACES here (real B path;
			# `ttl_no_kalshi_order` ⇒ rejected_post_hoc per state.py:931).
			transition_pending_to_rejected(
				store._conn,
				captured["row_id"],
				kalshi_order_id=None,
				rejection_reason="ttl_no_kalshi_order",
			)
			assert (
				store._conn.execute(
					"SELECT status FROM live_trades WHERE id = ?",
					(captured["row_id"],),
				).fetchone()[0]
				== "rejected_post_hoc"
			)
			# IOC nonetheless returns `filled` (the lost-CAS premise).
			return _filled_result_with_order_id("KAL-LOSTCAS-1")

		executor = MagicMock()
		executor.place = AsyncMock(side_effect=_place_then_b_races)

		import logging

		with caplog.at_level(logging.ERROR, logger=_dispatch_mod.log.name):
			# Must NOT raise — operator-trust, not fatal; money state is
			# already authoritative (§3.1).
			await _handle_enter(
				signal, _ctx(), store, _config_with_metrics(), executor, now=_NOW
			)

		coid = captured["coid"]
		c1_row_id = captured["row_id"]

		# (a) The celebratory "filled" notify MUST NOT fire for a durably
		# rejected row. _format_enter_message renders "PAPER BUY"/cost — assert
		# NO notification carries that celebratory content.
		assert not any(
			("PAPER BUY" in n) or ("cost)" in n) for n in notified
		), (
			f"the celebratory 'filled' notify must be SUPPRESSED when the "
			f"durable row is not 'open'; got notifications={notified!r}"
		)
		# (b) A DISTINCT non-celebratory record IS emitted, carrying
		# client_order_id + the actual durable status + the §4.2 attribution.
		# (log ERROR — uniform with the C3/C4/C5 lost-CAS taxonomy in store.py.)
		lostcas_logs = [
			r.getMessage()
			for r in caplog.records
			if r.levelno >= logging.ERROR and "rejected_post_hoc" in r.getMessage()
		]
		assert lostcas_logs, (
			"a DISTINCT non-celebratory lost-CAS record (log ERROR) must be "
			f"emitted; ERROR records={[r.getMessage() for r in caplog.records]!r}"
		)
		msg = lostcas_logs[0]
		assert coid in msg, f"the record must carry client_order_id; got {msg!r}"
		assert "KAL-LOSTCAS-1" in msg, (
			f"the record must carry the IOC kalshi_order_id; got {msg!r}"
		)
		assert "rejected_post_hoc" in msg, (
			f"the record must carry the actual durable status; got {msg!r}"
		)
		assert "§4.2" in msg, (
			f"the record must cite §4.2 (B reconciler / Kalshi-truth "
			f"authoritative); got {msg!r}"
		)
		# If a distinct lost-CAS notification was emitted it must itself be
		# NON-celebratory (no "PAPER BUY"/cost) — already covered by (a), which
		# scans ALL notifications.

		# (c) The durable money state is UNCHANGED — still rejected_post_hoc,
		# exactly one row (the CAS no-op never clobbered it; D2 changed ONLY
		# the notification, NOT the money logic — §4.2).
		final_rows = store._conn.execute(
			"SELECT id, status FROM live_trades"
		).fetchall()
		assert len(final_rows) == 1, (
			f"exactly one durable row — the lost CAS must not insert a second; "
			f"got {final_rows!r}"
		)
		assert final_rows[0][0] == c1_row_id
		assert final_rows[0][1] == "rejected_post_hoc", (
			"the authoritative money state must be UNTOUCHED (B's Kalshi-truth "
			f"reconciler owns it — §4.2); got {final_rows[0][1]!r}"
		)
	finally:
		store.close()


# ---------------------------------------------------------------------------
# D2 Paper byte-exact guard — with a paper-style store (record_trade INSERTs
# 'open'; get_trade_by_id returns 'open'), the filled arm fires the SAME
# celebratory "filled" notify as before D2. Proves the new status-branch is
# byte-exact-invisible to paper/replay (mandatory K2 11/11).
# ---------------------------------------------------------------------------


class _PaperStyleStore:
	"""record_intent no-op (paper); record_trade returns a synthetic id;
	get_trade_by_id ALWAYS returns status='open' — exactly paper/InMemory
	semantics post-record_trade (trade_store.py:336 INSERTs 'open';
	get_trade_by_id reads it back). The D2 status-branch must therefore take
	the 'open' arm and fire the celebratory notify byte-identically."""

	def __init__(self) -> None:
		self.record_trade_kwargs: dict[str, Any] | None = None

	def record_intent(self, **kwargs: Any) -> None:
		return None

	def record_trade(self, **kwargs: Any) -> int:
		self.record_trade_kwargs = dict(kwargs)
		return 77  # synthetic paper trade id

	def get_trade_by_id(self, trade_id: int) -> dict[str, Any]:
		# Paper/InMemory ALWAYS yield an 'open' row for a just-record_trade'd
		# id — the G-parity basis for D2's mode-agnostic status branch.
		return {"id": trade_id, "status": "open"}


@pytest.mark.asyncio
async def test_paper_style_filled_arm_fires_identical_celebratory_notify(
	monkeypatch: pytest.MonkeyPatch,
) -> None:
	"""Failure mode prevented: D2's new durable-status branch changes paper's
	"filled" notify content/timing (G-parity BLOCKING — K2 11/11 byte-exact).

	With a paper-style store (get_trade_by_id → status='open'), the filled arm
	must take the 'open' arm and fire the EXACT celebratory notify the pre-D2
	code did. The expected line is computed from the SAME _format_enter_message
	helper dispatch uses, so this pins byte-identical content. Also asserts the
	2 additive identity kwargs are passed (paper accept-and-ignores them — C2)."""
	from edge_catcher.engine.dispatch import _format_enter_message

	store = _PaperStyleStore()
	notified: list[str] = []
	monkeypatch.setattr(_dispatch_mod, "notify", lambda text: notified.append(text))

	executor = MagicMock()
	executor.place = AsyncMock(return_value=_filled_result_with_order_id("KAL-PAPER-1"))

	signal = _live_entry_signal()
	await _handle_enter(signal, _ctx(), store, _config_with_metrics(), executor, now=_NOW)

	# Byte-identical celebratory notify (the pre-D2 behavior). entry display
	# price = blended (42) ; fill_size = 10 ; trade_id = 77 (the store's id).
	_, expected_notify = _format_enter_message(
		strategy=signal.strategy,
		series=signal.series,
		ticker=signal.ticker,
		side=signal.side,
		fill_size=10,
		entry_price=42,
		trade_id=77,
		bullet="🔵",
	)
	assert notified == [expected_notify], (
		"paper-style store must fire the SAME celebratory 'filled' notify as "
		f"pre-D2 (byte-exact, K2); expected {[expected_notify]!r} got {notified!r}"
	)
	# Part 1: the 2 additive identity kwargs are threaded (paper ignores them).
	assert store.record_trade_kwargs is not None
	assert store.record_trade_kwargs["client_order_id"].startswith(
		"strat_34-KXSOL15M-26MAY09H06-"
	)
	assert store.record_trade_kwargs["kalshi_order_id"] == "KAL-PAPER-1"
