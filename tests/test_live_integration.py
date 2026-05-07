"""Live integration test for sub-project A.

Gated by LIVE_TRADER_LIVE_TEST=1 — DO NOT run in CI.
Requires KALSHI_KEY_ID + KALSHI_PRIVATE_KEY to be set with prod creds.

Test flow:
  1. balance() — proves auth works
  2. place() at 1¢ on a market guaranteed not to fill (IOC) — verifies sign + send
  3. cancel() the order — verifies DELETE path
  4. assert audit log has 3 rows (balance, place, cancel)

Cost: 1 contract @ 1¢ = $0.01 max exposure. Kalshi fee on a non-fill IOC = $0.
"""
from __future__ import annotations

import json
import os
import uuid

import pytest

from edge_catcher.live.audit import AuditLogger
from edge_catcher.live.client import KalshiOrderClient, OrderRequest
from edge_catcher.live.config import LiveConfig

# Designated test ticker — must be a real, low-volume Kalshi market.
# User must export this env var pointing to a market that's open and won't auto-fill at 1¢.
TEST_TICKER_ENV = "LIVE_TRADER_TEST_TICKER"


@pytest.mark.live
@pytest.mark.skipif(
	os.environ.get("LIVE_TRADER_LIVE_TEST") != "1",
	reason="Live integration test only runs when LIVE_TRADER_LIVE_TEST=1",
)
def test_place_and_cancel_against_prod(tmp_path):
	ticker = os.environ.get(TEST_TICKER_ENV)
	assert ticker, f"Set {TEST_TICKER_ENV} to a Kalshi ticker before running"

	audit_path = tmp_path / "live_audit.jsonl"
	cfg = LiveConfig(audit_log_path=audit_path, cli_max_order_dollars=1.0)
	audit = AuditLogger(cfg.audit_log_path)

	with KalshiOrderClient(cfg, audit) as c:
		# 1. Balance — proves auth + GET
		bal = c.balance()
		assert bal.balance_cents >= 0

		# 2. Place an order GUARANTEED not to fill (1¢ IOC on yes side; Kalshi book ask is typically >1¢)
		coid = str(uuid.uuid4())
		req = OrderRequest(
			ticker=ticker,
			action="buy",
			side="yes",
			count=1,
			limit_price_cents=1,
			time_in_force="ioc",
			client_order_id=coid,
		)
		try:
			order = c.place(req)
			# IOC with no crossing book: status often "canceled" with filled_count=0; treated as success here
			assert order.order_id, "place returned no order_id"
		except Exception as e:
			# If Kalshi rejects (e.g. price below tick size), fail clearly
			pytest.fail(f"place() failed: {e}")

		# 3. Try to cancel (expected: OrderAlreadyFinal if IOC already finalized; OK either way)
		try:
			c.cancel(order.order_id)
		except Exception as e:
			# IOC orders that didn't fill are already final; OrderAlreadyFinal is acceptable
			from edge_catcher.live.errors import OrderAlreadyFinal
			assert isinstance(e, OrderAlreadyFinal), f"unexpected error: {e}"

	# 4. Audit log: balance + place + (cancel-attempt) = 3 rows
	lines = audit_path.read_text().strip().split("\n")
	assert len(lines) >= 3
	ops = [json.loads(line)["op"] for line in lines]
	assert "balance" in ops
	assert "place" in ops
	assert "cancel" in ops
