"""Type-growth tests for Phase 2a maker execution (SPEC §4.1-§4.3)."""
from edge_catcher.engine.executor import OrderRequest, OrderResult
from edge_catcher.engine.strategy_base import Signal


def test_signal_defaults_preserve_taker_shape():
	sig = Signal(action="enter", ticker="KXTEST-1", side="yes",
	             series="KXTEST", strategy="s", reason="r")
	assert sig.exec_style == "taker"
	assert sig.rest_ttl_seconds is None
	assert sig.cancel_before_close_seconds is None


def test_signal_maker_fields_settable():
	sig = Signal(action="enter", ticker="KXTEST-1", side="no",
	             series="KXTEST", strategy="s", reason="r",
	             entry_price_cents=15, exec_style="maker",
	             rest_ttl_seconds=300, cancel_before_close_seconds=900)
	assert sig.exec_style == "maker"
	assert sig.rest_ttl_seconds == 300


def test_order_request_tif_defaults_ioc():
	req = OrderRequest(ticker="KXTEST-1", series="KXTEST", side="yes",
	                   size_contracts=1, limit_price_cents=50,
	                   strategy="s", client_order_id="cid-1")
	assert req.time_in_force == "ioc"


def test_order_result_accepts_resting_status():
	res = OrderResult(status="resting", intended_size=5, filled_size=0,
	                  blended_entry_cents=0, fill_pct=0.0, slippage_cents=0,
	                  order_id="oid-1")
	assert res.status == "resting"
