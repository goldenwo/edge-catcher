"""Unit tests for edge_catcher.live.reconciliation — the ambiguous-state
recovery surfaces (sub-project B / v1.6.0 PR 5, Agent 4.B).

Spec §Reconciliation logic + §Reconciliation tests #15-#21, plus the
real-money ``min_ts`` mandate and an idempotency driver.

Every test runs against a REAL migrated SQLite DB
(``live/state.connect_live_trades_db`` applies 0003 + WAL); the DB is NEVER
mocked and ``live.state`` is NEVER stubbed — these assert actual row-state
transitions end to end through the merged 4.A write API. The Kalshi side is a
counting fake (no httpx) so the "exactly one ``list_orders`` per cycle" and
"no ``positions()`` on reconnect" invariants are observable.
"""
from __future__ import annotations

import asyncio
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from edge_catcher.adapters.kalshi.fees import STANDARD_FEE
from edge_catcher.live import reconciliation as recon
from edge_catcher.live.client import Order, Position
from edge_catcher.live.reconciliation import (
	_RECONCILE_LOOKBACK_SECONDS,
	StartupReconcileReport,
	poll_pending_rows_loop,
	reconnect_reconcile,
	startup_reconcile,
)
from edge_catcher.live.state import (
	connect_live_trades_db,
	record_open,
	record_pending,
)

_NOW = datetime(2026, 5, 16, 12, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def conn(tmp_path: Path):
	"""A real on-disk live_trades.db with 0003 applied + WAL enabled."""
	c = connect_live_trades_db(tmp_path / "live_trades.db")
	yield c
	c.close()


def _iso(dt: datetime) -> str:
	return dt.isoformat()


def _recent() -> datetime:
	"""A ``placed_at`` that is young relative to *real* wall-clock now.

	The reconcilers reason about genuine elapsed time
	(``datetime.now(timezone.utc)``), NOT the frozen ``_NOW`` test anchor —
	so a row that must stay inside its TTL window has to be timestamped
	against the actual clock. This mirrors the production guarantee exactly.
	"""
	return datetime.now(timezone.utc) - timedelta(seconds=1)


def _stale() -> datetime:
	"""A ``placed_at`` comfortably past the 90s phantom TTL vs wall-clock
	now (exercises the real TTL boundary, not an incidentally-ancient row)."""
	return datetime.now(timezone.utc) - timedelta(seconds=600)


def _status(conn: sqlite3.Connection, row_id: int) -> str:
	row = conn.execute(
		"SELECT status FROM live_trades WHERE id = ?", (row_id,)
	).fetchone()
	assert row is not None, f"row {row_id} missing"
	return row[0]


def _row(conn: sqlite3.Connection, row_id: int) -> dict[str, object]:
	conn.row_factory = sqlite3.Row
	r = conn.execute(
		"SELECT * FROM live_trades WHERE id = ?", (row_id,)
	).fetchone()
	conn.row_factory = None
	assert r is not None, f"row {row_id} missing"
	return dict(r)


def _seed_pending(
	conn: sqlite3.Connection,
	*,
	coid: str,
	placed_at: datetime,
	kalshi_order_id: str | None = None,
	ticker: str = "KXSOL15M-26MAY16H12",
	intended_size: int = 10,
) -> int:
	return record_pending(
		conn,
		ticker=ticker,
		series="KXSOL15M",
		strategy="strat-34",
		side="yes",
		intended_size=intended_size,
		entry_price_cents=40,
		stop_loss_distance_cents=None,
		client_order_id=coid,
		kalshi_order_id=kalshi_order_id,
		placed_at_utc=_iso(placed_at),
	)


def _seed_open(
	conn: sqlite3.Connection,
	*,
	coid: str,
	ticker: str = "KXSOL15M-26MAY16H12",
	fill_size: int = 10,
) -> int:
	return record_open(
		conn,
		ticker=ticker,
		series="KXSOL15M",
		strategy="strat-34",
		side="yes",
		intended_size=fill_size,
		fill_size=fill_size,
		entry_price_cents=40,
		blended_entry_cents=40,
		slippage_cents=0,
		fill_pct=1.0,
		stop_loss_distance_cents=20,
		client_order_id=coid,
		kalshi_order_id="kid-open",
		placed_at_utc=_iso(_NOW),
		entry_time=_iso(_NOW),
		entry_fee_cents=17,
	)


def _make_exit_pending(conn: sqlite3.Connection, row_id: int) -> None:
	"""Drive an open row to exit_pending directly (no 4.A helper for the
	open→exit_pending edge — that is D's dispatch path, out of 4.B scope)."""
	conn.execute(
		"UPDATE live_trades SET status='exit_pending' WHERE id=?", (row_id,)
	)
	conn.commit()


def _order(
	*,
	order_id: str,
	client_order_id: str,
	status: str,
	count: int = 10,
	filled_count: int = 0,
	ticker: str = "KXSOL15M-26MAY16H12",
	limit_price_cents: int = 40,
) -> Order:
	return Order(
		order_id=order_id,
		ticker=ticker,
		side="yes",
		action="buy",
		count=count,
		limit_price_cents=limit_price_cents,
		time_in_force="ioc",
		status=status,
		filled_count=filled_count,
		client_order_id=client_order_id,
		raw={},
	)


class FakeClient:
	"""Counting Kalshi client double.

	Records every ``list_orders`` kwargs dict and counts ``positions`` calls
	so the one-call-per-cycle and no-positions-on-reconnect invariants are
	directly assertable. No HTTP — returns canned ``Order`` / ``Position``
	lists set by the test.
	"""

	def __init__(
		self,
		*,
		orders: list[Order] | None = None,
		positions: list[Position] | None = None,
	) -> None:
		self._orders = orders or []
		self._positions = positions or []
		self.list_orders_calls: list[dict[str, object]] = []
		self.positions_call_count = 0

	async def list_orders(
		self,
		*,
		status: str | None = None,
		limit: int = 200,
		cursor: str | None = None,
		min_ts: int | None = None,
	) -> list[Order]:
		self.list_orders_calls.append(
			{
				"status": status,
				"limit": limit,
				"cursor": cursor,
				"min_ts": min_ts,
			}
		)
		return list(self._orders)

	async def positions(self) -> list[Position]:
		self.positions_call_count += 1
		return list(self._positions)


class FakeBankrollCache:
	def __init__(self, *, fail: bool = False) -> None:
		self.fail = fail
		self.refresh_count = 0

	async def refresh(self) -> None:
		self.refresh_count += 1
		if self.fail:
			raise RuntimeError("balance fetch failed")


# ---------------------------------------------------------------------------
# #15 — startup: Kalshi has a position, we have no local row
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_15_startup_kalshi_position_no_local_row_inserts_open(
	conn: sqlite3.Connection, caplog
) -> None:
	client = FakeClient(
		positions=[
			Position(
				ticker="KXSOL15M-26MAY16H12",
				side="yes",
				count=7,
				average_price_cents=44,
				raw={},
			)
		]
	)
	with caplog.at_level(logging.WARNING):
		report = await startup_reconcile(client, conn, FakeBankrollCache())

	# Return-contract assertion: a frozen+slots StartupReconcileReport
	# (immutable value object per spec §Quality bar).
	assert isinstance(report, StartupReconcileReport)
	with pytest.raises((AttributeError, TypeError)):
		report.alerts = 99  # type: ignore[misc]  # frozen — must reject

	rows = conn.execute(
		"SELECT id, status, fill_size, ticker, blended_entry_cents "
		"FROM live_trades"
	).fetchall()
	assert len(rows) == 1
	_id, status, fill_size, ticker, blended = rows[0]
	assert status == "open"
	assert fill_size == 7
	assert ticker == "KXSOL15M-26MAY16H12"
	assert blended == 44
	# Orphan still incurred Kalshi's taker fee (spec §283): the recovered
	# row's entry_fee_cents is STANDARD_FEE on the avg cost basis, not 0.
	orphan_row = _row(conn, _id)
	assert orphan_row["entry_fee_cents"] == int(
		round(STANDARD_FEE.calculate(44, 7))
	)
	assert orphan_row["entry_fee_cents"] > 0
	assert orphan_row["strategy"] == "reconcile-orphan"
	assert report.orphan_positions_recovered == 1
	assert report.alerts == 1
	assert any(
		"orphan" in r.message.lower() for r in caplog.records
	), "expected an orphan-recovery alert log"


# ---------------------------------------------------------------------------
# #16 — startup: we have open, Kalshi has no position
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_16_startup_local_open_kalshi_missing_marks_lost_truth(
	conn: sqlite3.Connection, caplog
) -> None:
	row_id = _seed_open(conn, coid="strat-34-KXSOL15M-aaa")
	client = FakeClient(positions=[], orders=[])

	with caplog.at_level(logging.WARNING):
		report = await startup_reconcile(client, conn, FakeBankrollCache())

	assert _status(conn, row_id) == "lost_truth"
	assert report.lost_truth == 1
	assert report.alerts >= 1


# ---------------------------------------------------------------------------
# #17 — startup: local pending matched to Kalshi filled → open
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_17_startup_pending_matched_filled_resolves_to_open(
	conn: sqlite3.Connection,
) -> None:
	coid = "strat-34-KXSOL15M-bbb"
	row_id = _seed_pending(conn, coid=coid, placed_at=_recent())
	client = FakeClient(
		orders=[
			_order(
				order_id="kid-1",
				client_order_id=coid,
				status="executed",
				count=10,
				filled_count=10,
			)
		],
		positions=[
			Position(
				ticker="KXSOL15M-26MAY16H12",
				side="yes",
				count=10,
				average_price_cents=41,
				raw={},
			)
		],
	)

	report = await startup_reconcile(client, conn, FakeBankrollCache())

	row = _row(conn, row_id)
	assert row["status"] == "open"
	assert row["kalshi_order_id"] == "kid-1"
	assert row["fill_size"] == 10
	assert row["blended_entry_cents"] == 40  # IOC limit (no VWAP via REST)
	# Entry fee MUST be computed via STANDARD_FEE (spec §283), NOT 0.
	# ceil(0.07 * 10 * 0.40 * 0.60 * 100) = ceil(16.8) = 17.
	expected_fee = int(round(STANDARD_FEE.calculate(40, 10)))
	assert expected_fee == 17
	assert row["entry_fee_cents"] == 17, (
		"reconcile-recovered fill must charge the Kalshi taker fee, not 0 "
		"(spec §283) — a 0 here overstates P&L"
	)
	# 4.A's transition_pending_to_open seeds remaining = entry_fee_cents.
	assert row["entry_fee_remaining_cents"] == 17
	assert row["slippage_cents"] == 0  # REST Order has no fill-vs-limit delta
	assert report.pending_resolved == 1


# ---------------------------------------------------------------------------
# #18 — startup: local pending past TTL, no Kalshi order → rejected_post_hoc
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_18_startup_pending_past_ttl_no_order_rejected_post_hoc(
	conn: sqlite3.Connection,
) -> None:
	row_id = _seed_pending(
		conn, coid="strat-34-KXSOL15M-ccc", placed_at=_stale()
	)
	client = FakeClient(orders=[], positions=[])

	report = await startup_reconcile(client, conn, FakeBankrollCache())

	row = _row(conn, row_id)
	assert row["status"] == "rejected_post_hoc"
	assert row["rejection_reason"] == "ttl_no_kalshi_order"
	assert report.pending_post_hoc_rejected == 1


@pytest.mark.asyncio
async def test_18b_startup_pending_within_ttl_no_order_left_pending(
	conn: sqlite3.Connection,
) -> None:
	"""A young pending row with no Kalshi match must NOT be rejected — it is
	still inside its TTL window (race-free real-money guard).

	``placed_at`` is anchored to *real* wall-clock now (not the frozen
	``_NOW``): ``startup_reconcile`` reasons about genuine elapsed time
	(``datetime.now``), so a "young" assertion must be young relative to
	the actual clock — that is precisely the production guarantee."""
	row_id = _seed_pending(
		conn, coid="strat-34-KXSOL15M-ddd", placed_at=_recent()
	)
	client = FakeClient(orders=[], positions=[])

	report = await startup_reconcile(client, conn, FakeBankrollCache())

	assert _status(conn, row_id) == "pending"
	assert report.pending_post_hoc_rejected == 0


# ---------------------------------------------------------------------------
# #19 — poller: exactly ONE list_orders() call per cycle regardless of count
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_19_poller_one_list_orders_call_per_cycle(
	conn: sqlite3.Connection,
) -> None:
	# Five pending rows, none matchable on Kalshi (all young → no transition).
	for i in range(5):
		_seed_pending(
			conn,
			coid=f"strat-34-KXSOL15M-p{i}",
			placed_at=_recent(),
		)
	client = FakeClient(orders=[], positions=[])

	task = asyncio.create_task(
		poll_pending_rows_loop(
			client, conn, poll_interval=0.01, ttl_seconds=90.0
		)
	)
	# Let a few cycles run.
	await asyncio.sleep(0.05)
	task.cancel()
	with pytest.raises(asyncio.CancelledError):
		await task

	assert len(client.list_orders_calls) >= 2, "loop should have cycled"
	# The hard invariant: each cycle issues exactly ONE list_orders call.
	# 5 pending rows must NOT produce 5 calls/cycle. positions() is never
	# called by the poller.
	assert client.positions_call_count == 0
	# Each recorded call is a single batched scan (we can't pin the exact
	# count under a timing race, but the per-cycle ratio is 1: assert no
	# call carries a cursor — i.e. the poller never page-walks).
	assert all(c["cursor"] is None for c in client.list_orders_calls)


@pytest.mark.asyncio
async def test_19b_poller_one_call_even_with_many_pending_single_cycle(
	conn: sqlite3.Connection,
) -> None:
	"""Deterministic per-cycle assertion: invoke the batch reconcile once
	directly (no loop timing) with N pending rows; assert exactly 1 call."""
	for i in range(8):
		_seed_pending(
			conn, coid=f"strat-34-KXSOL15M-q{i}", placed_at=_recent()
		)
	client = FakeClient(orders=[], positions=[])

	await recon._reconcile_pending_batch(client, conn, ttl_seconds=90.0)

	assert len(client.list_orders_calls) == 1
	assert client.positions_call_count == 0


# ---------------------------------------------------------------------------
# #20 — poller: exit_pending past TTL → revert to open
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_20_poller_exit_pending_past_ttl_reverts_to_open(
	conn: sqlite3.Connection,
) -> None:
	row_id = _seed_pending(
		conn, coid="strat-34-KXSOL15M-ex1", placed_at=_stale()
	)
	# Promote to open then to exit_pending (placed_at stays old).
	conn.execute(
		"UPDATE live_trades SET status='open', fill_size=10, "
		"blended_entry_cents=40, entry_fee_cents=17, "
		"entry_fee_remaining_cents=17 WHERE id=?",
		(row_id,),
	)
	conn.commit()
	_make_exit_pending(conn, row_id)
	client = FakeClient(orders=[], positions=[])

	await recon._reconcile_pending_batch(client, conn, ttl_seconds=90.0)

	assert _status(conn, row_id) == "open"


@pytest.mark.asyncio
async def test_20b_poller_pending_matched_filled_resolves(
	conn: sqlite3.Connection,
) -> None:
	"""Poller positive path: a pending row WITH a Kalshi executed match
	resolves to open even before TTL."""
	coid = "strat-34-KXSOL15M-ex2"
	row_id = _seed_pending(conn, coid=coid, placed_at=_recent())
	client = FakeClient(
		orders=[
			_order(
				order_id="kid-9",
				client_order_id=coid,
				status="executed",
				filled_count=10,
			)
		]
	)

	await recon._reconcile_pending_batch(client, conn, ttl_seconds=90.0)

	row = _row(conn, row_id)
	assert row["status"] == "open"
	assert row["kalshi_order_id"] == "kid-9"


# ---------------------------------------------------------------------------
# #21 — reconnect_reconcile: fast version skips position-orphan check
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_21_reconnect_reconcile_skips_positions_call(
	conn: sqlite3.Connection,
) -> None:
	coid = "strat-34-KXSOL15M-rc1"
	row_id = _seed_pending(conn, coid=coid, placed_at=_recent())
	client = FakeClient(
		orders=[
			_order(
				order_id="kid-rc",
				client_order_id=coid,
				status="executed",
				filled_count=10,
			)
		],
		positions=[
			# A position the orphan-detector WOULD recover — proves reconnect
			# does NOT look at positions (no spurious orphan row).
			Position(
				ticker="KXOTHER-1",
				side="yes",
				count=3,
				average_price_cents=50,
				raw={},
			)
		],
	)

	await reconnect_reconcile(client, conn)

	assert client.positions_call_count == 0, (
		"reconnect_reconcile must NOT call positions() — orphan detection "
		"is deferred to the next full startup reconcile"
	)
	# It still resolves the pending row (the fast subset DOES do this).
	assert _status(conn, row_id) == "open"
	# And it did NOT insert an orphan row for KXOTHER-1.
	assert (
		conn.execute(
			"SELECT COUNT(*) FROM live_trades WHERE ticker='KXOTHER-1'"
		).fetchone()[0]
		== 0
	)


# ---------------------------------------------------------------------------
# Real-money mandate — startup_reconcile MUST pass a non-None min_ts
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_startup_reconcile_passes_min_ts_to_list_orders(
	conn: sqlite3.Connection,
) -> None:
	client = FakeClient(orders=[], positions=[])

	before = datetime.now(timezone.utc)
	await startup_reconcile(client, conn, FakeBankrollCache())
	after = datetime.now(timezone.utc)

	assert len(client.list_orders_calls) == 1
	min_ts = client.list_orders_calls[0]["min_ts"]
	assert min_ts is not None, (
		"REAL-MONEY MANDATE: startup_reconcile MUST bound list_orders with "
		"min_ts or a filled order off page 1 → phantom rejected_post_hoc"
	)
	assert isinstance(min_ts, int), "min_ts must be Unix seconds (int)"
	# It must equal (now − _RECONCILE_LOOKBACK_SECONDS) in Unix seconds.
	lo = int(
		(before - timedelta(seconds=_RECONCILE_LOOKBACK_SECONDS)).timestamp()
	)
	hi = int(
		(after - timedelta(seconds=_RECONCILE_LOOKBACK_SECONDS)).timestamp()
	)
	assert lo <= min_ts <= hi


@pytest.mark.asyncio
async def test_reconnect_and_poller_may_omit_min_ts(
	conn: sqlite3.Connection,
) -> None:
	"""The fast reconnect path + the 30s poller MAY use the default (no
	min_ts) — Phase-1 volume is far below one page over minutes. Pin the
	documented assumption so a future change is a conscious one."""
	client = FakeClient(orders=[], positions=[])

	await reconnect_reconcile(client, conn)
	await recon._reconcile_pending_batch(client, conn, ttl_seconds=90.0)

	assert len(client.list_orders_calls) == 2
	assert all(c["min_ts"] is None for c in client.list_orders_calls)


# ---------------------------------------------------------------------------
# Idempotency — running the same reconcile twice changes nothing the 2nd time
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_startup_reconcile_is_idempotent(
	conn: sqlite3.Connection,
) -> None:
	# A mix: one pending→executed, one open→lost_truth, one orphan position.
	coid_p = "strat-34-KXSOL15M-idem-p"
	pend_id = _seed_pending(conn, coid=coid_p, placed_at=_recent())
	open_id = _seed_open(
		conn,
		coid="strat-34-KXSOL15M-idem-o",
		ticker="KXETH15M-26MAY16H12",
	)
	client = FakeClient(
		orders=[
			_order(
				order_id="kid-idem",
				client_order_id=coid_p,
				status="executed",
				filled_count=10,
			)
		],
		positions=[
			# Matches the pending row's ticker (so pend→open is consistent).
			Position(
				ticker="KXSOL15M-26MAY16H12",
				side="yes",
				count=10,
				average_price_cents=40,
				raw={},
			),
			# Orphan: no local row.
			Position(
				ticker="KXNEW-1",
				side="yes",
				count=2,
				average_price_cents=33,
				raw={},
			),
		],
	)

	r1 = await startup_reconcile(client, conn, FakeBankrollCache())
	snap1 = conn.execute(
		"SELECT id, status, kalshi_order_id, fill_size, ticker "
		"FROM live_trades ORDER BY id"
	).fetchall()

	r2 = await startup_reconcile(client, conn, FakeBankrollCache())
	snap2 = conn.execute(
		"SELECT id, status, kalshi_order_id, fill_size, ticker "
		"FROM live_trades ORDER BY id"
	).fetchall()

	# First pass acted; second pass is a pure no-op on row state.
	assert snap1 == snap2, (
		f"reconcile not idempotent: {snap1!r} != {snap2!r}"
	)
	assert _status(conn, pend_id) == "open"
	assert _status(conn, open_id) == "lost_truth"
	# Exactly one orphan row for KXNEW-1 (NOT a second one on re-run).
	assert (
		conn.execute(
			"SELECT COUNT(*) FROM live_trades WHERE ticker='KXNEW-1'"
		).fetchone()[0]
		== 1
	)
	# Second run recovers no NEW orphans / resolves no NEW pendings.
	assert r2.orphan_positions_recovered == 0
	assert r2.pending_resolved == 0
	assert r1.orphan_positions_recovered == 1


@pytest.mark.asyncio
async def test_orphan_already_actioned_does_not_halt_engine(
	conn: sqlite3.Connection,
) -> None:
	"""Real-money false-halt guard: orphan recovered on pass 1, operator
	then CLOSES it (status leaves 'open'), Kalshi still reports the
	position. Pass 2 must be a pure no-op — NOT a UNIQUE-violation
	``RecordPendingFailed`` that would HALT the live engine on a benign
	operator action."""
	pos = Position(
		ticker="KXORPH-1",
		side="yes",
		count=4,
		average_price_cents=30,
		raw={},
	)
	client = FakeClient(orders=[], positions=[pos])

	# Pass 1: recover the orphan.
	r1 = await startup_reconcile(client, conn, FakeBankrollCache())
	assert r1.orphan_positions_recovered == 1
	orphan = conn.execute(
		"SELECT id FROM live_trades WHERE ticker='KXORPH-1'"
	).fetchone()
	assert orphan is not None
	orphan_id = int(orphan[0])

	# Operator closes the recovered orphan (e.g. settled it manually).
	conn.execute(
		"UPDATE live_trades SET status='won', exit_price_cents=100, "
		"pnl_cents=280, exit_time=? WHERE id=?",
		(_NOW.isoformat(), orphan_id),
	)
	conn.commit()

	# Pass 2: Kalshi STILL reports the position. Must NOT raise / halt.
	r2 = await startup_reconcile(client, conn, FakeBankrollCache())

	assert r2.orphan_positions_recovered == 0, (
		"orphan already actioned — recovery must be a no-op, not re-INSERT"
	)
	# Still exactly one KXORPH-1 row, still closed (NOT re-opened).
	rows = conn.execute(
		"SELECT id, status FROM live_trades WHERE ticker='KXORPH-1'"
	).fetchall()
	assert len(rows) == 1
	assert rows[0][1] == "won"


# ---------------------------------------------------------------------------
# Bankroll refresh failure is FATAL (spec step 1)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_startup_reconcile_bankroll_refresh_failure_is_fatal(
	conn: sqlite3.Connection,
) -> None:
	client = FakeClient(orders=[], positions=[])
	with pytest.raises(RuntimeError, match="balance fetch failed"):
		await startup_reconcile(
			client, conn, FakeBankrollCache(fail=True)
		)
	# It must fail BEFORE touching Kalshi (cash seed is a precondition).
	assert client.list_orders_calls == []
	assert client.positions_call_count == 0


# ---------------------------------------------------------------------------
# Defensive status mapping — resting/pending (rare w/ IOC) treated as rejected
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pending_matched_kalshi_rejected_resolves_rejected(
	conn: sqlite3.Connection,
) -> None:
	coid = "strat-34-KXSOL15M-rej"
	row_id = _seed_pending(conn, coid=coid, placed_at=_recent())
	client = FakeClient(
		orders=[
			_order(
				order_id="kid-r",
				client_order_id=coid,
				status="rejected",
			)
		]
	)

	await recon._reconcile_pending_batch(client, conn, ttl_seconds=90.0)

	row = _row(conn, row_id)
	assert row["status"] == "rejected"
	assert row["rejection_reason"] != "ttl_no_kalshi_order"


@pytest.mark.asyncio
async def test_pending_matched_kalshi_resting_defensively_rejected(
	conn: sqlite3.Connection, caplog
) -> None:
	coid = "strat-34-KXSOL15M-rest"
	row_id = _seed_pending(conn, coid=coid, placed_at=_recent())
	client = FakeClient(
		orders=[
			_order(
				order_id="kid-rest",
				client_order_id=coid,
				status="resting",
			)
		]
	)

	with caplog.at_level(logging.WARNING):
		await recon._reconcile_pending_batch(
			conn=conn, client=client, ttl_seconds=90.0
		)

	assert _status(conn, row_id) == "rejected"
	assert any(
		"resting" in r.message.lower() for r in caplog.records
	), "expected a defensive-rejection warning for a resting IOC order"


# ---------------------------------------------------------------------------
# Reconcile-recovered PARTIAL fill must write the TRUE fill_pct (I1).
#
# Kalshi IOC orders are marked status='executed' even on a partial fill (IOC
# fills what it can, cancels the remainder). The matched-pending→open path
# records the real fill_size but historically hardcoded fill_pct=1.0, so a
# 3-of-10 reconcile-recovered fill was mis-reported as a clean 100% fill —
# defeating slippage/partial-fill analysis on exactly the rows reconciliation
# exists to recover. fill_pct's contract (DDL 0003 / spec ~L126) is
# fill_size / intended_size; the value must mirror executors/live.py
# :_clamp_fill_pct (raw ratio, NOT rounded; div-by-zero→0.0; clamp upper→1.0).
# intended_size itself stays the original pending value (immutable).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reconcile_partial_fill_writes_true_fill_pct(
	conn: sqlite3.Connection,
) -> None:
	"""Pending intended_size=10; Kalshi reports an executed IOC that only
	filled 3. Post-reconcile the row is 'open' with fill_size==3 and
	fill_pct == 0.3 (the real fraction, NOT 1.0); intended_size stays 10."""
	coid = "strat-34-KXSOL15M-partial"
	row_id = _seed_pending(
		conn, coid=coid, placed_at=_recent(), intended_size=10
	)
	client = FakeClient(
		orders=[
			_order(
				order_id="kid-partial",
				client_order_id=coid,
				status="executed",  # IOC: 'executed' even on a partial fill
				count=10,
				filled_count=3,
			)
		]
	)

	await recon._reconcile_pending_batch(client, conn, ttl_seconds=90.0)

	row = _row(conn, row_id)
	assert row["status"] == "open"
	assert row["kalshi_order_id"] == "kid-partial"
	assert row["fill_size"] == 3, "the real partial fill_size is recorded"
	# The hard assertion this whole test exists for: the TRUE fraction, not
	# the old hardcoded 1.0. _clamp_fill_pct returns the raw ratio (3/10)
	# WITHOUT rounding, so the stored value is exactly 0.3.
	assert row["fill_pct"] == pytest.approx(3 / 10), (
		f"reconcile-recovered partial fill must write the true "
		f"fill_pct=fill_size/intended_size (0.3), not 1.0; got "
		f"{row['fill_pct']!r}"
	)
	assert row["fill_pct"] != 1.0, (
		"a 3-of-10 partial fill mis-reported as a clean 100% fill defeats "
		"slippage/partial-fill analysis (I1)"
	)
	# intended_size is the original pending value — never mutated by the
	# resolution (only the derived ratio is computed from it).
	assert row["intended_size"] == 10
	assert row["original_intended_size"] == 10


@pytest.mark.asyncio
async def test_reconcile_full_fill_still_writes_fill_pct_1(
	conn: sqlite3.Connection,
) -> None:
	"""Regression guard for I1: a genuine full fill (filled_count==count==
	intended_size) must still produce fill_pct == 1.0 — the fix computes the
	real fraction, it does not break the common 100% path."""
	coid = "strat-34-KXSOL15M-fullfrac"
	row_id = _seed_pending(
		conn, coid=coid, placed_at=_recent(), intended_size=10
	)
	client = FakeClient(
		orders=[
			_order(
				order_id="kid-full",
				client_order_id=coid,
				status="executed",
				count=10,
				filled_count=10,
			)
		]
	)

	await recon._reconcile_pending_batch(client, conn, ttl_seconds=90.0)

	row = _row(conn, row_id)
	assert row["status"] == "open"
	assert row["fill_size"] == 10
	assert row["fill_pct"] == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# M1 — defend against ANY zero-fill 'executed' phantom.
#
# _kalshi_outcome returns 'open' for status=='executed' BEFORE the
# filled_count >= count check, so any 'executed' order that genuinely filled
# zero contracts (filled_count==0 — whether count is 0 OR positive) would
# recover a phantom 'open' that never drains (no WS event for a zero-fill
# order; every later reconcile re-matches it 'executed' → never TTL'd) — an
# unbounded MAX_OPEN slot leak with no operator signal. fill_size is the TRUE
# filled_count (never `or count`); the matched-pending path routes an
# effective zero fill to rejected (reason 'reconcile_zero_fill') with a
# WARNING.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reconcile_zero_fill_executed_phantom_rejected_not_open(
	conn: sqlite3.Connection, caplog
) -> None:
	"""Pending + Kalshi Order(status='executed', filled_count=0, count=0):
	the row must go 'rejected' (reason 'reconcile_zero_fill') with a WARNING
	logged — NOT a phantom 'open' that leaks a MAX_OPEN slot forever."""
	coid = "strat-34-KXSOL15M-zerofill"
	row_id = _seed_pending(conn, coid=coid, placed_at=_recent())
	client = FakeClient(
		orders=[
			_order(
				order_id="kid-zero",
				client_order_id=coid,
				status="executed",
				count=0,
				filled_count=0,
			)
		]
	)

	with caplog.at_level(logging.WARNING):
		await recon._reconcile_pending_batch(
			client, conn, ttl_seconds=90.0
		)

	row = _row(conn, row_id)
	assert row["status"] == "rejected", (
		"a count=0 'executed' phantom must be defensively rejected, not "
		"recovered as a never-draining phantom 'open' (M1)"
	)
	# Distinct, clear reason — and specifically NOT the TTL reason: a matched
	# order is resolved immediately and never reaches the TTL branch, so this
	# also proves it was not mis-routed there (and not a phantom 'open').
	assert row["rejection_reason"] == "reconcile_zero_fill", (
		"the defensive zero-fill rejection must carry a clear, distinct "
		"reason (NOT 'ttl_no_kalshi_order' — it never reaches the TTL "
		"branch) and the row must NOT be a phantom 'open'"
	)
	assert any(
		"reconcile_zero_fill" in r.message
		or ("zero" in r.message.lower() and "kid-zero" in r.message)
		for r in caplog.records
		if r.levelno >= logging.WARNING
	), "expected a WARNING naming the zero-fill order id + coid"


@pytest.mark.asyncio
async def test_reconcile_executed_with_filled_count_but_zero_count_recovers(
	conn: sqlite3.Connection,
) -> None:
	"""Boundary guard for M1: a degenerate count=0 but filled_count=3
	'executed' order still has a real fill (fill_size = filled_count = 3),
	so it must recover as 'open' — the M1 guard keys on the EFFECTIVE
	fill_size<=0 (the true filled_count), not on count alone."""
	coid = "strat-34-KXSOL15M-zcount-nz-fill"
	row_id = _seed_pending(
		conn, coid=coid, placed_at=_recent(), intended_size=10
	)
	client = FakeClient(
		orders=[
			_order(
				order_id="kid-zc",
				client_order_id=coid,
				status="executed",
				count=0,
				filled_count=3,
			)
		]
	)

	await recon._reconcile_pending_batch(client, conn, ttl_seconds=90.0)

	row = _row(conn, row_id)
	assert row["status"] == "open", (
		"a real fill (filled_count=3) must still recover even if count=0 — "
		"M1 guards on effective fill_size<=0, not count"
	)
	assert row["fill_size"] == 3
	assert row["fill_pct"] == pytest.approx(3 / 10)


@pytest.mark.asyncio
async def test_reconcile_zero_fill_executed_with_positive_count_rejected_not_phantom_open(
	conn: sqlite3.Connection, caplog
) -> None:
	"""M1 (broadened): Kalshi Order(status='executed', count=10,
	filled_count=0) — a requested order that genuinely filled ZERO contracts —
	must be defensively rejected (reason 'reconcile_zero_fill'), NOT recovered
	as a phantom 'open' of the full requested size. _kalshi_outcome returns
	'open' off status=='executed' regardless of filled_count, so the
	matched-pending zero-fill defense must key on the TRUE filled_count, never
	fall back to the requested count (the old `filled_count or count` booked a
	phantom count-sized 'open' that never drains: no WS event for it, and
	every later reconcile re-matches it 'executed' so the TTL branch is
	unreachable — an unbounded MAX_OPEN slot leak with wrong equity)."""
	coid = "strat-34-KXSOL15M-zerofill-poscount"
	row_id = _seed_pending(
		conn, coid=coid, placed_at=_recent(), intended_size=10
	)
	client = FakeClient(
		orders=[
			_order(
				order_id="kid-zero-pc",
				client_order_id=coid,
				status="executed",
				count=10,
				filled_count=0,
			)
		]
	)

	with caplog.at_level(logging.WARNING):
		await recon._reconcile_pending_batch(
			client, conn, ttl_seconds=90.0
		)

	row = _row(conn, row_id)
	assert row["status"] == "rejected", (
		"a count=10 but filled_count=0 'executed' order genuinely filled "
		"nothing — it must be defensively rejected, NOT booked as a phantom "
		"full-size 'open' (M1 must key on the true fill, not `or count`)"
	)
	assert row["rejection_reason"] == "reconcile_zero_fill", (
		"the zero-fill rejection must carry the distinct 'reconcile_zero_fill' "
		"reason (NOT a phantom 'open', NOT the TTL reason)"
	)
	assert any(
		"reconcile_zero_fill" in r.message
		or ("zero" in r.message.lower() and "kid-zero-pc" in r.message)
		for r in caplog.records
		if r.levelno >= logging.WARNING
	), "expected a WARNING naming the zero-fill order id + coid"


@pytest.mark.asyncio
async def test_reconcile_executed_with_zero_price_left_pending_not_phantom_basis(
	conn: sqlite3.Connection, caplog
) -> None:
	"""A1: a matched Kalshi Order(status='executed', filled_count=10) whose
	limit_price_cents is 0 (Kalshi's order JSON omitted yes_price/no_price →
	_parse_order coerces it to 0) must NOT be booked pending→open at a 0¢
	blended cost basis — that silently corrupts won/lost + P&L on
	reconcile-recovered rows (record_partial_exit's NULL guard does not catch
	a non-NULL 0). A live order limit is never 0¢; treat it as 'no
	trustworthy price' and leave the row PENDING (NOT rejected: the order
	filled, Kalshi holds the contracts — rejecting would orphan a real
	position). A young row retries next reconcile / its WS fill; a stale one
	TTLs and is recovered via positions()."""
	coid = "strat-34-KXSOL15M-zeroprice"
	row_id = _seed_pending(
		conn, coid=coid, placed_at=_recent(), intended_size=10
	)
	client = FakeClient(
		orders=[
			_order(
				order_id="kid-zprice",
				client_order_id=coid,
				status="executed",
				count=10,
				filled_count=10,
				limit_price_cents=0,
			)
		]
	)

	with caplog.at_level(logging.WARNING):
		await recon._reconcile_pending_batch(
			client, conn, ttl_seconds=90.0
		)

	row = _row(conn, row_id)
	assert row["status"] == "pending", (
		"a matched 'executed' order with no trustworthy price (0¢ limit) "
		"must leave the row PENDING for WS / next-reconcile / positions() "
		"recovery — NEVER phantom-open at a fabricated 0¢ cost basis, and "
		"NEVER reject (the contracts are really held)"
	)
	assert any(
		"trustworthy" in r.message.lower()
		or ("limit_price_cents=0" in r.message and "kid-zprice" in r.message)
		for r in caplog.records
		if r.levelno >= logging.WARNING
	), "expected a WARNING that the matched order exposed no trustworthy price"


# ---------------------------------------------------------------------------
# I2.3 — ordering interaction: a pending row resolved to 'open' in the
# matched path whose ticker positions() does NOT return in the SAME
# startup_reconcile pass must end 'lost_truth' (matrix row 2), deterministically.
#
# This is the subtle same-pass interaction the reviewer flagged: the matched
# branch (rows 3-5) runs BEFORE the open/lost-truth scan (rows 1-2/6) inside
# one _apply_startup_matrix call, so a pending row that resolves to 'open'
# becomes visible to the lost-truth scan in the same pass; with no matching
# Kalshi position it is correctly marked lost_truth. Pin the ordering so a
# future refactor that reorders the matrix sub-steps fails loudly.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_startup_pending_resolved_open_then_lost_truth_same_pass(
	conn: sqlite3.Connection,
) -> None:
	"""Pending matched to a Kalshi executed order (→ resolves 'open' in the
	rows-3-5 sub-step) but positions() returns NO position for that ticker:
	in the SAME startup pass the row must end 'lost_truth' (matrix row 2),
	deterministically — resolve-then-lost_truth, exactly once."""
	coid = "strat-34-KXSOL15M-resolve-then-lost"
	row_id = _seed_pending(conn, coid=coid, placed_at=_recent())
	client = FakeClient(
		orders=[
			_order(
				order_id="kid-rtl",
				client_order_id=coid,
				status="executed",
				count=10,
				filled_count=10,
			)
		],
		# Deliberately NO position for KXSOL15M-26MAY16H12: Kalshi's order
		# log confirms the fill but positions() does not show the position
		# (e.g. it settled/closed between the order scan and the position
		# scan, or a genuine truth divergence).
		positions=[],
	)

	report = await startup_reconcile(client, conn, FakeBankrollCache())

	row = _row(conn, row_id)
	assert row["status"] == "lost_truth", (
		"same-pass: pending resolves to 'open' (rows 3-5) then the row-2 "
		"scan finds no Kalshi position → lost_truth, deterministically"
	)
	# It WAS resolved first (the matched-pending sub-step ran and counted it)
	# and THEN marked lost_truth in the same pass — both counters fire once.
	assert report.pending_resolved == 1, (
		"the matched-pending resolution happened (rows 3-5 ran before the "
		"lost-truth scan in the same pass)"
	)
	assert report.lost_truth == 1
	assert report.alerts == 1
	# Determinism: a second identical pass is a pure no-op (lost_truth is
	# terminal; the pending row is gone; no NEW counts).
	report2 = await startup_reconcile(client, conn, FakeBankrollCache())
	assert _status(conn, row_id) == "lost_truth"
	assert report2.pending_resolved == 0
	assert report2.lost_truth == 0


# ---------------------------------------------------------------------------
# M2 — startup-path TTL→rejected_post_hoc logs at WARNING (operator-actionable
# at boot); the steady-state poller path stays at INFO.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_startup_ttl_rejection_logs_warning(
	conn: sqlite3.Connection, caplog
) -> None:
	"""A stale-at-boot pending row TTL'd by startup_reconcile is more
	anomalous than a steady-state poller TTL — it must log at WARNING so the
	operator sees it (M2)."""
	row_id = _seed_pending(
		conn, coid="strat-34-KXSOL15M-bootttl", placed_at=_stale()
	)
	client = FakeClient(orders=[], positions=[])

	with caplog.at_level(logging.INFO):
		await startup_reconcile(client, conn, FakeBankrollCache())

	assert _status(conn, row_id) == "rejected_post_hoc"
	ttl_recs = [
		r
		for r in caplog.records
		if "TTL" in r.message and "rejected_post_hoc" in r.message
	]
	assert ttl_recs, "expected a startup TTL→rejected_post_hoc log line"
	assert any(r.levelno >= logging.WARNING for r in ttl_recs), (
		"startup-path TTL rejection must be WARNING (operator-actionable at "
		"boot), not INFO (M2)"
	)


@pytest.mark.asyncio
async def test_poller_ttl_rejection_stays_info(
	conn: sqlite3.Connection, caplog
) -> None:
	"""Contrast for M2: the steady-state poller TTL→rejected_post_hoc path
	stays at INFO (a routine 30s-poller TTL is not boot-anomalous)."""
	row_id = _seed_pending(
		conn, coid="strat-34-KXSOL15M-pollttl", placed_at=_stale()
	)
	client = FakeClient(orders=[], positions=[])

	with caplog.at_level(logging.INFO):
		await recon._reconcile_pending_batch(
			client, conn, ttl_seconds=90.0
		)

	assert _status(conn, row_id) == "rejected_post_hoc"
	ttl_recs = [
		r
		for r in caplog.records
		if "TTL" in r.message and "rejected_post_hoc" in r.message
	]
	assert ttl_recs, "expected a poller TTL→rejected_post_hoc log line"
	assert all(r.levelno == logging.INFO for r in ttl_recs), (
		"the poller-path TTL rejection must stay INFO (steady-state, not "
		"boot-anomalous) — only the startup path is WARNING (M2)"
	)


# ---------------------------------------------------------------------------
# Matrix row 6 — "Both agree on position | UPDATE reconciled_at_utc; continue"
# (spec §332 row 6). The agreeing row's last-verified observability timestamp
# MUST be refreshed; running reconcile twice just re-touches it (idempotent —
# no spurious state transition).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_startup_both_agree_open_row_bumps_reconciled_at_utc(
	conn: sqlite3.Connection,
) -> None:
	"""Local 'open' row + a matching Kalshi position = matrix row 6. Before
	the wiring this was a deliberate no-op (reconciled_at_utc stayed NULL on
	the steady-state path); the spec mandates it be stamped. The row must NOT
	change status (no spurious transition) — only reconciled_at_utc moves."""
	ticker = "KXSOL15M-26MAY16H12"
	row_id = _seed_open(conn, coid="strat-34-KXSOL15M-agree", ticker=ticker)
	assert _row(conn, row_id)["reconciled_at_utc"] is None

	client = FakeClient(
		orders=[],
		positions=[
			Position(
				ticker=ticker,
				side="yes",
				count=10,
				average_price_cents=40,
				raw={},
			)
		],
	)

	report = await startup_reconcile(client, conn, FakeBankrollCache())

	row = _row(conn, row_id)
	assert row["status"] == "open", (
		"a both-agree row must NOT change status (row 6 = continue)"
	)
	assert row["reconciled_at_utc"] is not None, (
		"matrix row 6 mandates reconciled_at_utc be stamped on agreeing rows "
		"(spec §332) — the steady-state path must no longer leave it NULL"
	)
	# Both-agree is neither an orphan recovery nor a lost-truth.
	assert report.orphan_positions_recovered == 0
	assert report.lost_truth == 0


@pytest.mark.asyncio
async def test_startup_both_agree_reconciled_at_utc_is_idempotent(
	conn: sqlite3.Connection,
) -> None:
	"""Two startup passes on a both-agree row: the 2nd pass just re-touches
	reconciled_at_utc (advances or holds it) — never a spurious transition,
	status stays 'open', and the value remains non-NULL across both passes."""
	ticker = "KXETH15M-26MAY16H12"
	row_id = _seed_open(
		conn, coid="strat-34-KXETH15M-agree2", ticker=ticker
	)
	client = FakeClient(
		orders=[],
		positions=[
			Position(
				ticker=ticker,
				side="yes",
				count=10,
				average_price_cents=40,
				raw={},
			)
		],
	)

	r1 = await startup_reconcile(client, conn, FakeBankrollCache())
	after_first = _row(conn, row_id)["reconciled_at_utc"]
	assert after_first is not None

	r2 = await startup_reconcile(client, conn, FakeBankrollCache())
	after_second = _row(conn, row_id)["reconciled_at_utc"]

	assert after_second is not None, (
		"the 2nd pass must re-touch (still non-NULL), not clear the timestamp"
	)
	assert _status(conn, row_id) == "open", (
		"idempotent: no spurious transition on the 2nd both-agree pass"
	)
	# No NEW orphan / lost-truth / pending action on either pass.
	assert (r1.orphan_positions_recovered, r1.lost_truth) == (0, 0)
	assert (r2.orphan_positions_recovered, r2.lost_truth) == (0, 0)


@pytest.mark.asyncio
async def test_poller_both_agree_pending_match_bumps_reconciled_at_utc(
	conn: sqlite3.Connection,
) -> None:
	"""Poller path, matrix row 6: a pending row WITH a matching Kalshi order
	(here Kalshi-confirmed filled → resolves to 'open') is "confirmed
	still-active against Kalshi" — reconciled_at_utc MUST be stamped on the
	now-active row. (A row with NO Kalshi match is NOT row 6 and is left
	with a NULL reconciled_at_utc — covered by the contrast assertion.)"""
	coid = "strat-34-KXSOL15M-pollagree"
	row_id = _seed_pending(conn, coid=coid, placed_at=_recent())
	assert _row(conn, row_id)["reconciled_at_utc"] is None

	client = FakeClient(
		orders=[
			_order(
				order_id="kid-pa",
				client_order_id=coid,
				status="executed",
				filled_count=10,
			)
		]
	)

	await recon._reconcile_pending_batch(client, conn, ttl_seconds=90.0)

	row = _row(conn, row_id)
	assert row["status"] == "open", "Kalshi-filled match resolves to open"
	assert row["reconciled_at_utc"] is not None, (
		"a Kalshi-confirmed (matched) row is matrix row 6 — its "
		"reconciled_at_utc must be stamped by the poller path"
	)


@pytest.mark.asyncio
async def test_poller_no_kalshi_match_leaves_reconciled_at_utc_null(
	conn: sqlite3.Connection,
) -> None:
	"""Contrast / guard: a young pending row with NO Kalshi order is NOT
	"both agree" (Kalshi does not confirm it) — it must be left untouched,
	including a NULL reconciled_at_utc. Proves the row-6 bump is gated on an
	actual Kalshi match, not applied blindly to every scanned row."""
	row_id = _seed_pending(
		conn, coid="strat-34-KXSOL15M-nomatch", placed_at=_recent()
	)
	client = FakeClient(orders=[], positions=[])

	await recon._reconcile_pending_batch(client, conn, ttl_seconds=90.0)

	row = _row(conn, row_id)
	assert row["status"] == "pending", "young unmatched row stays pending"
	assert row["reconciled_at_utc"] is None, (
		"no Kalshi match = not row 6 — reconciled_at_utc must stay NULL"
	)
