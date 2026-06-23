import asyncio
from datetime import datetime, timezone

from edge_catcher.engine.executor import OrderRequest
from edge_catcher.engine.executors.paper import PaperExecutor
from edge_catcher.engine.market_state import MarketState, OrderbookSnapshot
from edge_catcher.engine.replay.latency_fill import PendingFillQueue, resolve_matured_fills
from edge_catcher.engine.strategy_base import Signal
from edge_catcher.engine.trade_store import InMemoryTradeStore

def _dt(s): return datetime(2026, 6, 22, 16, 0, s, tzinfo=timezone.utc)

def test_queue_seq_order_and_total_enqueued():
	q = PendingFillQueue()
	q.enqueue(req="A", entry_price=50, signal="sA", arrival_time=_dt(2))
	q.enqueue(req="B", entry_price=40, signal="sB", arrival_time=_dt(1))  # earlier arrival, later enqueue
	q.enqueue(req="C", entry_price=60, signal="sC", arrival_time=_dt(9))
	matured = q.drain(_dt(2))
	assert [m.req for m in matured] == ["A", "B"]      # ENQUEUE (seq) order, deterministic
	assert q.total_enqueued == 3                       # lifetime counter (T6 denominator)
	assert [m.req for m in q.drain(_dt(9))] == ["C"]
	assert q.drain(_dt(9)) == [] and q.total_enqueued == 3

def test_drain_boundary_and_empty():
	q = PendingFillQueue()
	assert q.drain(_dt(30)) == []       # empty queue
	q.enqueue(req="X", entry_price=50, signal="s", arrival_time=_dt(5))
	assert q.drain(_dt(4)) == []        # not yet matured (arrival 5 > now 4)
	assert len(q) == 1                  # un-matured order stays pending
	assert [m.req for m in q.drain(_dt(5))] == ["X"]  # exactly at boundary (<=)
	assert len(q) == 0 and q.total_enqueued == 1      # drained; lifetime counter holds


# ---------------------------------------------------------------------------
# Behavioral tests — resolve_matured_fills drain against evolved book
# ---------------------------------------------------------------------------
# Config: risk_per_trade_cents=100, limit=46, NO side.
# For NO-side buy, implied ask = 100 - round(yes_bid_price_dollars * 100).
# yes_levels=[(0.56, qty)] → implied NO ask = 44¢ (≤ limit 46 → fills).
# yes_levels=[(0.54, qty)] → implied NO ask = 46¢ (== limit 46 → fills).
# yes_levels=[] → no implied NO liquidity → no fill → no row written.
#
# Stale-book gate: abs(best_implied_ask - entry_price_cents) ≤ 10 required.
# entry_price_cents passed to resolve_fill = req.limit_price_cents = 46.
# ask 44 → |44-46|=2 ≤ 10 → PASS. ask 46 → |46-46|=0 → PASS.
# ---------------------------------------------------------------------------

CFG = {
	"sizing": {
		"risk_per_trade_cents": 100,
		"max_slippage_cents": 2,
		"min_fill": 1,
		"require_fresh_book": True,
	}
}


def _t(s: int) -> datetime:
	return datetime(2026, 6, 22, 16, 0, s, tzinfo=timezone.utc)


def _sig() -> Signal:
	return Signal(
		action="enter",
		ticker="KXE",
		side="no",
		series="KXETH15M",
		entry_price_cents=44,
		stop_loss_distance_cents=0,
		strategy="t",
		reason="t",
	)


def _req() -> OrderRequest:
	return OrderRequest(
		ticker="KXE",
		series="KXETH15M",
		side="no",
		size_contracts=0,
		limit_price_cents=46,
		strategy="t",
		client_order_id="t-1",
		action="buy",
	)


def _drain_against(yes_levels_at_arrival: list) -> InMemoryTradeStore:
	"""Build real MarketState+PaperExecutor+InMemoryTradeStore, enqueue one
	entry, seed the evolved book, then drain at arrival_time (Δ=0)."""
	ms = MarketState()
	ms.seed_orderbook("KXE", OrderbookSnapshot(yes_levels=yes_levels_at_arrival, no_levels=[]))
	ex = PaperExecutor(market_state=ms, config=CFG)
	store = InMemoryTradeStore()
	q = PendingFillQueue()
	q.enqueue(req=_req(), entry_price=44, signal=_sig(), arrival_time=_t(1))
	asyncio.run(resolve_matured_fills(q, _t(1), ex, store))
	return store


def test_no_fill_when_touch_gone() -> None:
	"""Empty yes_levels → no implied NO liquidity → no fill → no row."""
	store = _drain_against([])
	assert len(store.all_trades()) == 0


def test_fill_when_touch_persists() -> None:
	"""yes_levels=[(0.56, 10)] → NO implied ask 44 ≤ limit 46 → fill recorded."""
	store = _drain_against([(0.56, 10)])
	trades = store.all_trades()
	assert len(trades) == 1


def test_fill_at_worse_price_within_limit() -> None:
	"""yes_levels=[(0.54, 10)] → NO implied ask 46 == limit 46 → still fills."""
	store = _drain_against([(0.54, 10)])
	trades = store.all_trades()
	assert len(trades) == 1


def test_partial_fill_on_thin_book() -> None:
	"""Thin book forces partial fill (fill_size < intended_size); row is recorded.

	risk_per_trade_cents=100, entry_price_cents=44 → raw_size = 100//44 = 2.
	Seeding yes_levels=[(0.56, 1)] → only 1 contract of NO liquidity available.
	PaperExecutor sizes to 2, walks the book, fills 1 (min_fill=1 → passes gate).
	The drain records the row (status=="filled"); fill_size < intended_size.
	"""
	store = _drain_against([(0.56, 1)])
	trades = store.all_trades()
	assert len(trades) == 1
	row = trades[0]
	# fill_size should be 1 (only 1 qty available); intended_size should be 2
	assert row["fill_size"] == 1
	assert row["fill_size"] < row["intended_size"]
