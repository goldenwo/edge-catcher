from edge_catcher.engine.trade_store import TradeStoreProtocol


def test_record_intent_is_in_protocol():
	assert hasattr(TradeStoreProtocol, "record_intent")
	# additive-only: every pre-existing member still present
	for m in ("record_trade", "record_pending", "record_rejected",
	          "exit_trade", "settle_trade", "get_trade_by_id", "get_open_trades"):
		assert hasattr(TradeStoreProtocol, m), m
