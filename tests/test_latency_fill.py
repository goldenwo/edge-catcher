from datetime import datetime, timezone
from edge_catcher.engine.replay.latency_fill import PendingFillQueue

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
