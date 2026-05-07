"""Live integration test for sub-project A.

Gated by LIVE_TRADER_LIVE_TEST=1 — DO NOT run in CI.
Requires KALSHI_LIVE_KEY_ID + KALSHI_LIVE_PRIVATE_KEY (trade-scope key) to be
set with prod creds. The paper trader's read-only key (KALSHI_KEY_ID /
KALSHI_PRIVATE_KEY) is NOT used by this test — see two-key separation
(`live/client.py` _request → make_auth_headers with KALSHI_LIVE_*).

Test flow:
  1. balance() — proves auth works on the trade-scope key
  2. place() at 1¢ GTC on a designated low-volume market — order RESTS on the
     book (will not fill: ask is far above 1¢). Verifies sign + send + the
     `time_in_force` short→verbose translation (`gtc` → `good_till_canceled`).
  3. cancel() the resting order — verifies DELETE + signing.
  4. assert audit log has 3 rows (balance, place, cancel)

Why GTC, not IOC: Kalshi's IOC semantics on a non-crossing limit reject the
order entirely with `fill_or_kill_insufficient_resting_volume` (Kalshi reuses
the FOK error code for both). For a "place + cancel" smoke test, we want the
order to actually rest so cancel has something to operate on. GTC at 1¢
against a 94¢-ask book is a passive resting order — zero risk of fill.

Cost: 1 contract × 1¢ exposure = $0.01 max if it filled, but it won't. Real
expected cost: $0 (no fill, no fee on cancel).
"""
from __future__ import annotations

import json
import os
import uuid

import pytest

from edge_catcher.live.audit import AuditLogger
from edge_catcher.live.client import KalshiOrderClient, OrderRequest
from edge_catcher.live.config import LiveConfig
from edge_catcher.live.errors import OrderAlreadyFinal

# Designated test ticker — must be a real, low-volume Kalshi market.
# Caller must export this env var pointing to a market that's open and has
# yes_ask >> 1¢ so a 1¢ GTC buy can't possibly fill.
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

	order_id: str | None = None
	try:
		with KalshiOrderClient(cfg, audit) as c:
			# 1. Balance — proves auth + GET on the trade-scope key
			bal = c.balance()
			assert bal.balance_cents >= 0

			# 2. Place a GTC limit at 1¢ — order will REST on the book.
			coid = str(uuid.uuid4())
			req = OrderRequest(
				ticker=ticker,
				action="buy",
				side="yes",
				count=1,
				limit_price_cents=1,
				time_in_force="gtc",
				client_order_id=coid,
			)
			order = c.place(req)
			assert order.order_id, "place returned no order_id"
			order_id = order.order_id

			# 3. Cancel the resting order. Must succeed — we just placed it
			# and it cannot have filled (ask is far above 1¢).
			try:
				result = c.cancel(order_id)
				assert result.status in ("canceled", "executed", "deleted"), (
					f"unexpected cancel status: {result.status}"
				)
				order_id = None  # cancel succeeded; nothing to clean up
			except OrderAlreadyFinal:
				# Defensive: if Kalshi raced ahead and finalized the order,
				# this is acceptable — the audit row still records both place
				# and cancel attempts.
				order_id = None
	finally:
		# Last-ditch cleanup: if we placed an order but never cancelled it,
		# try once more with a fresh client. Don't let the test leave a
		# resting order on the user's Kalshi account.
		if order_id is not None:
			try:
				with KalshiOrderClient(cfg, audit) as cleanup:
					cleanup.cancel(order_id)
			except Exception:
				pass  # best-effort; surface in audit log

	# 4. Audit log: balance + place + cancel = 3 rows minimum
	lines = audit_path.read_text(encoding="utf-8").strip().split("\n")
	assert len(lines) >= 3, f"expected ≥3 audit rows, got {len(lines)}"
	ops = [json.loads(line)["op"] for line in lines]
	assert "balance" in ops
	assert "place" in ops
	assert "cancel" in ops
