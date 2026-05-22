import sqlite3
from edge_catcher.engine.trade_store import TradeStore


def test_paper_record_intent_is_strict_noop(tmp_path):
	db = tmp_path / "paper.db"
	s = TradeStore(db)
	before = sqlite3.connect(db).execute(
		"SELECT count(*) FROM paper_trades").fetchone()[0]
	s.record_intent(ticker="T", series="S", strategy="x", side="yes",
		intended_size=1, entry_price_cents=5,
		stop_loss_distance_cents=3, client_order_id="cid-1",
		placed_at_utc="2026-05-18T00:00:00+00:00")
	after = sqlite3.connect(db).execute(
		"SELECT count(*) FROM paper_trades").fetchone()[0]
	assert before == after  # zero rows written — true no-op
