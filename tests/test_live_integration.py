"""Live integration test for sub-project A.

Gated by LIVE_TRADER_LIVE_TEST=1 — DO NOT run in CI.
Requires KALSHI_LIVE_KEY_ID + KALSHI_LIVE_PRIVATE_KEY (trade-scope key) to be
set with prod creds. The paper trader's read-only key (KALSHI_KEY_ID /
KALSHI_PRIVATE_KEY) is NOT used by this test — see two-key separation
(`live/client.py` _request → make_auth_headers with KALSHI_LIVE_*).

Test flow:
  1. balance() — proves auth works on the trade-scope key.
  2. Read the market's orderbook (public REST, no auth needed) and pick a
     bid price = best_yes_bid - 1¢, capped at the CLI dollar cap. This rests
     just below the inside of the existing bid stack: very low fill risk, but
     accepted by Kalshi (as opposed to a 1¢ bid which Kalshi rejects when
     the contra side is empty or the bid is far below the inside, with the
     misleadingly-named `fill_or_kill_insufficient_resting_volume` error).
  3. place() the GTC bid — order rests on the book.
  4. cancel() the resting order.
  5. Audit log assertions.

If the market has NO yes-bids (one-sided book), the test skips with a clear
message so the operator picks a more liquid ticker.

Cost: at most `cli_max_order_dollars` if the bid filled (it won't — we bid
1¢ below the inside which is also below the inside ask). Real expected cost: $0.
"""
from __future__ import annotations

import json
import os
import uuid

import httpx
import pytest

from edge_catcher.live.audit import AuditLogger
from edge_catcher.live.client import KalshiOrderClient, OrderRequest
from edge_catcher.live.config import LiveConfig
from edge_catcher.live.errors import OrderAlreadyFinal

# Designated test ticker — must be a real, active Kalshi market with yes-bids
# on the orderbook (so we can place a non-crossing GTC bid).
TEST_TICKER_ENV = "LIVE_TRADER_TEST_TICKER"

# Kalshi public REST base for orderbook reads (no auth required).
_KALSHI_PUBLIC_BASE = "https://api.elections.kalshi.com/trade-api/v2"


def _read_best_yes_bid_cents(ticker: str) -> int | None:
	"""Fetch the orderbook and return the best YES bid in cents.

	Returns None if the yes-bid side is empty (one-sided book) — caller should
	skip the test in that case.
	"""
	url = f"{_KALSHI_PUBLIC_BASE}/markets/{ticker}/orderbook"
	r = httpx.get(url, timeout=10)
	r.raise_for_status()
	ofp = (r.json().get("orderbook_fp") or {})
	yes_bids = ofp.get("yes_dollars") or []
	if not yes_bids:
		return None
	# yes_bids = [["0.8500", "10.00"], ["0.9200", "200.00"], ...]
	best = max(float(level[0]) for level in yes_bids)
	return int(round(best * 100))


@pytest.mark.live
@pytest.mark.skipif(
	os.environ.get("LIVE_TRADER_LIVE_TEST") != "1",
	reason="Live integration test only runs when LIVE_TRADER_LIVE_TEST=1",
)
def test_place_and_cancel_against_prod(tmp_path):
	ticker = os.environ.get(TEST_TICKER_ENV)
	assert ticker, f"Set {TEST_TICKER_ENV} to a Kalshi ticker before running"

	# Discover a safe bid price: 1 tick below the inside yes-bid, so the order
	# rests on the book without crossing.
	best_yes_bid = _read_best_yes_bid_cents(ticker)
	if best_yes_bid is None:
		pytest.skip(
			f"Market {ticker} has no yes-bids on the book (one-sided). "
			f"Pick a more liquid ticker via {TEST_TICKER_ENV}."
		)
	bid_price = max(1, best_yes_bid - 1)

	audit_path = tmp_path / "live_audit.jsonl"
	cfg = LiveConfig(audit_log_path=audit_path, cli_max_order_dollars=1.0)
	# CLI cap may be tighter than the bid price implies — caps in dollars,
	# bid in cents. Sanity check before placing real orders.
	exposure_dollars = bid_price / 100.0
	if exposure_dollars > cfg.cli_max_order_dollars:
		pytest.skip(
			f"Best yes-bid is {best_yes_bid}c on {ticker}; bidding 1 tick "
			f"below ({bid_price}c = ${exposure_dollars:.2f}) exceeds the "
			f"CLI cap (${cfg.cli_max_order_dollars:.2f}). Pick a market "
			f"with a lower inside bid."
		)
	audit = AuditLogger(cfg.audit_log_path)

	order_id: str | None = None
	try:
		with KalshiOrderClient(cfg, audit) as c:
			# 1. Balance — proves auth + GET on the trade-scope key
			bal = c.balance()
			assert bal.balance_cents >= 0

			# 2. Place a GTC limit at (best_yes_bid - 1¢) — rests on the book
			# just below the inside bid, won't cross the ask, very low fill risk.
			coid = str(uuid.uuid4())
			req = OrderRequest(
				ticker=ticker,
				action="buy",
				side="yes",
				count=1,
				limit_price_cents=bid_price,
				time_in_force="gtc",
				client_order_id=coid,
			)
			order = c.place(req)
			assert order.order_id, "place returned no order_id"
			order_id = order.order_id

			# 3. Cancel the resting order. Should succeed — order is alive.
			try:
				result = c.cancel(order_id)
				assert result.status in ("canceled", "executed", "deleted"), (
					f"unexpected cancel status: {result.status}"
				)
				order_id = None
			except OrderAlreadyFinal:
				# Defensive: if Kalshi raced ahead and finalized (e.g. someone
				# crossed our bid in the milliseconds between place + cancel),
				# the audit row still records both attempts.
				order_id = None
	finally:
		# Last-ditch cleanup: don't leave a resting order on the user's account.
		if order_id is not None:
			try:
				with KalshiOrderClient(cfg, audit) as cleanup:
					cleanup.cancel(order_id)
			except Exception:
				pass  # best-effort; already logged in audit

	# 4. Audit log: balance + place + cancel = 3 rows minimum
	lines = audit_path.read_text(encoding="utf-8").strip().split("\n")
	assert len(lines) >= 3, f"expected ≥3 audit rows, got {len(lines)}"
	ops = [json.loads(line)["op"] for line in lines]
	assert "balance" in ops
	assert "place" in ops
	assert "cancel" in ops
