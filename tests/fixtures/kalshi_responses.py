"""Ground-truth Kalshi REST response fixtures — captured VERBATIM from the live
Pi audit log (``data/live_audit.jsonl``) on 2026-05-24.

These are the EXACT 201 bodies Kalshi returned for the 6 real IOC orders the
live daemon placed during the smoothness-test cutover. They are the source of
truth for the create-order wire shape.

The prior *assumed* shape (``filled_count`` / ``fills[]`` / ``yes_price``) was
fictional: every executed fill was misread as ``ioc_zero_fill`` and orphaned a
real position. The CR-5 mock copied the same fiction, so the parity gate
false-passed. These captured bodies pin the real shape so that can't recur.

Real create-order 201 shape — the order object is nested under ``"order"``:

* ``fill_count_fp`` / ``initial_count_fp`` / ``remaining_count_fp`` — STRING
  decimals, e.g. ``"6.00"`` (Phase-1 binary contracts are whole numbers).
* ``status`` — ``"executed"`` | ``"resting"`` | ``"canceled"``.
* ``yes_price_dollars`` / ``no_price_dollars`` — STRING dollars (``"0.1700"``);
  ×100 → integer cents. This is the order's LIMIT price, echoed back.
* ``taker_fill_cost_dollars`` — STRING dollars, the aggregate cost of the taker
  fills on the bought side. Blended fill price (cents) =
  ``round(taker_fill_cost_dollars * 100 / fill_count)``. There is NO per-fill
  ``fills`` array — only this aggregate.
* ``taker_fees_dollars`` / ``maker_*`` — STRING dollars.
* ``created_time`` / ``last_update_time`` — ISO-8601 (NOT ``created_ts``).
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# The 6 real create-order 201 bodies (inner "order" dicts). Verbatim capture.
# ---------------------------------------------------------------------------

_SOL_1600_NO = {
	"action": "buy", "book_side": "ask",
	"client_order_id": "debut-fade-KXSOL15M-26MAY241600-00-1779652454276-4291e6ee",
	"created_time": "2026-05-24T19:54:14.74227Z",
	"fill_count_fp": "6.00", "initial_count_fp": "6.00", "remaining_count_fp": "0.00",
	"last_update_time": "2026-05-24T19:54:14.74227Z",
	"maker_fees_dollars": "0.000000", "maker_fill_cost_dollars": "0.000000",
	"no_price_dollars": "0.1700", "yes_price_dollars": "0.8300",
	"order_id": "9ac7286c-6e3c-47b6-ad5c-188e0d7371ea",
	"outcome_side": "no", "side": "no", "status": "executed", "subaccount_number": 0,
	"taker_fees_dollars": "0.060000", "taker_fill_cost_dollars": "0.900000",
	"ticker": "KXSOL15M-26MAY241600-00", "type": "limit",
	"user_id": "db7a007b-7e27-4306-9ffe-9b283082a6d8",
}

_ETH_1600_NO = {
	"action": "buy", "book_side": "ask",
	"client_order_id": "debut-fade-KXETH15M-26MAY241600-00-1779652455276-a5209bdb",
	"created_time": "2026-05-24T19:54:15.720467Z",
	"fill_count_fp": "24.00", "initial_count_fp": "24.00", "remaining_count_fp": "0.00",
	"last_update_time": "2026-05-24T19:54:15.720467Z",
	"maker_fees_dollars": "0.000000", "maker_fill_cost_dollars": "0.000000",
	"no_price_dollars": "0.0600", "yes_price_dollars": "0.9400",
	"order_id": "7e2aaca5-0c07-46a4-89b3-1de1622ba10f",
	"outcome_side": "no", "side": "no", "status": "executed", "subaccount_number": 0,
	"taker_fees_dollars": "0.068000", "taker_fill_cost_dollars": "0.912000",
	"ticker": "KXETH15M-26MAY241600-00", "type": "limit",
	"user_id": "db7a007b-7e27-4306-9ffe-9b283082a6d8",
}

_ETH_1615_YES = {
	"action": "buy", "book_side": "bid",
	"client_order_id": "debut-fade-KXETH15M-26MAY241615-15-1779653056276-a9672799",
	"created_time": "2026-05-24T20:04:16.72002Z",
	"fill_count_fp": "8.00", "initial_count_fp": "8.00", "remaining_count_fp": "0.00",
	"last_update_time": "2026-05-24T20:04:16.72002Z",
	"maker_fees_dollars": "0.000000", "maker_fill_cost_dollars": "0.000000",
	"no_price_dollars": "0.8600", "yes_price_dollars": "0.1400",
	"order_id": "d494ee30-f68e-4454-9fce-3aa29700d382",
	"outcome_side": "yes", "side": "yes", "status": "executed", "subaccount_number": 0,
	"taker_fees_dollars": "0.060000", "taker_fill_cost_dollars": "0.960000",
	"ticker": "KXETH15M-26MAY241615-15", "type": "limit",
	"user_id": "db7a007b-7e27-4306-9ffe-9b283082a6d8",
}

_SOL_1615_YES = {
	"action": "buy", "book_side": "bid",
	"client_order_id": "debut-fade-KXSOL15M-26MAY241615-15-1779653056736-5eae551d",
	"created_time": "2026-05-24T20:04:17.149831Z",
	"fill_count_fp": "5.00", "initial_count_fp": "5.00", "remaining_count_fp": "0.00",
	"last_update_time": "2026-05-24T20:04:17.149831Z",
	"maker_fees_dollars": "0.000000", "maker_fill_cost_dollars": "0.000000",
	"no_price_dollars": "0.8100", "yes_price_dollars": "0.1900",
	"order_id": "6da222b5-5e5e-4bce-964f-11e3d4bc475a",
	"outcome_side": "yes", "side": "yes", "status": "executed", "subaccount_number": 0,
	"taker_fees_dollars": "0.050000", "taker_fill_cost_dollars": "0.850000",
	"ticker": "KXSOL15M-26MAY241615-15", "type": "limit",
	"user_id": "db7a007b-7e27-4306-9ffe-9b283082a6d8",
}

_SOL_1630_YES = {
	"action": "buy", "book_side": "bid",
	"client_order_id": "debut-fade-KXSOL15M-26MAY241630-30-1779653963281-90988db5",
	"created_time": "2026-05-24T20:19:23.839513Z",
	"fill_count_fp": "2.00", "initial_count_fp": "2.00", "remaining_count_fp": "0.00",
	"last_update_time": "2026-05-24T20:19:23.839513Z",
	"maker_fees_dollars": "0.000000", "maker_fill_cost_dollars": "0.000000",
	"no_price_dollars": "0.6200", "yes_price_dollars": "0.3800",
	"order_id": "d6e53319-0bdb-4f1f-99a5-221e1f65620a",
	"outcome_side": "yes", "side": "yes", "status": "executed", "subaccount_number": 0,
	"taker_fees_dollars": "0.040000", "taker_fill_cost_dollars": "0.700000",
	"ticker": "KXSOL15M-26MAY241630-30", "type": "limit",
	"user_id": "db7a007b-7e27-4306-9ffe-9b283082a6d8",
}

_SOL_1645_NO = {
	"action": "buy", "book_side": "ask",
	"client_order_id": "debut-fade-KXSOL15M-26MAY241645-45-1779654867279-d8d188b7",
	"created_time": "2026-05-24T20:34:27.745827Z",
	"fill_count_fp": "2.00", "initial_count_fp": "2.00", "remaining_count_fp": "0.00",
	"last_update_time": "2026-05-24T20:34:27.745827Z",
	"maker_fees_dollars": "0.000000", "maker_fill_cost_dollars": "0.000000",
	"no_price_dollars": "0.4300", "yes_price_dollars": "0.5700",
	"order_id": "6d310085-d474-4487-93c5-f5509bcbb78e",
	"outcome_side": "no", "side": "no", "status": "executed", "subaccount_number": 0,
	"taker_fees_dollars": "0.040000", "taker_fill_cost_dollars": "0.820000",
	"ticker": "KXSOL15M-26MAY241645-45", "type": "limit",
	"user_id": "db7a007b-7e27-4306-9ffe-9b283082a6d8",
}

# Inner "order" dicts, in placement order.
PLACE_201_ORDERS = [
	_SOL_1600_NO, _ETH_1600_NO, _ETH_1615_YES, _SOL_1615_YES, _SOL_1630_YES, _SOL_1645_NO,
]

# Full HTTP 201 bodies (order nested under "order"), as KalshiOrderClient.place
# receives them from ``response.get("order", response)``.
PLACE_201_BODIES = [{"order": o} for o in PLACE_201_ORDERS]

# What _parse_order MUST yield for each body above (same order).
#   limit_price_cents = round(<side>_price_dollars * 100)
#   avg_fill_price_cents = round(taker_fill_cost_dollars * 100 / fill_count)
PLACE_201_EXPECTED = [
	{"order_id": "9ac7286c-6e3c-47b6-ad5c-188e0d7371ea", "side": "no",  "filled_count": 6,  "count": 6,  "limit_price_cents": 17, "avg_fill_price_cents": 15, "status": "executed"},
	{"order_id": "7e2aaca5-0c07-46a4-89b3-1de1622ba10f", "side": "no",  "filled_count": 24, "count": 24, "limit_price_cents": 6,  "avg_fill_price_cents": 4,  "status": "executed"},
	{"order_id": "d494ee30-f68e-4454-9fce-3aa29700d382", "side": "yes", "filled_count": 8,  "count": 8,  "limit_price_cents": 14, "avg_fill_price_cents": 12, "status": "executed"},
	{"order_id": "6da222b5-5e5e-4bce-964f-11e3d4bc475a", "side": "yes", "filled_count": 5,  "count": 5,  "limit_price_cents": 19, "avg_fill_price_cents": 17, "status": "executed"},
	{"order_id": "d6e53319-0bdb-4f1f-99a5-221e1f65620a", "side": "yes", "filled_count": 2,  "count": 2,  "limit_price_cents": 38, "avg_fill_price_cents": 35, "status": "executed"},
	{"order_id": "6d310085-d474-4487-93c5-f5509bcbb78e", "side": "no",  "filled_count": 2,  "count": 2,  "limit_price_cents": 43, "avg_fill_price_cents": 41, "status": "executed"},
]

# (body, expected) tuples for pytest.mark.parametrize.
PLACE_201_CASES = list(zip(PLACE_201_BODIES, PLACE_201_EXPECTED))


# ---------------------------------------------------------------------------
# Positions response (GET /portfolio/positions) — captured verbatim. All three
# real positions were flat (position_fp "0.00"); a NONZERO-position fixture for
# _parse_position is built synthetically in the test from this real shape.
# ---------------------------------------------------------------------------

POSITIONS_BODY = {
	"cursor": "",
	"event_positions": [
		{"event_exposure_dollars": "0.000000", "event_ticker": "KXUSTESTSREADING-26", "fees_paid_dollars": "0.240000", "realized_pnl_dollars": "-0.140000", "total_cost_dollars": "7.140000", "total_cost_shares_fp": "14.00"},
		{"event_exposure_dollars": "0.000000", "event_ticker": "KXAISPIKE-26B", "fees_paid_dollars": "1.448100", "realized_pnl_dollars": "-25.671900", "total_cost_dollars": "102.671900", "total_cost_shares_fp": "154.00"},
		{"event_exposure_dollars": "0.000000", "event_ticker": "KXCODINGMODEL-26DEC", "fees_paid_dollars": "0.470000", "realized_pnl_dollars": "-1.740000", "total_cost_dollars": "30.740000", "total_cost_shares_fp": "58.00"},
	],
	"market_positions": [
		{"fees_paid_dollars": "0.240000", "last_updated_ts": "2026-05-24T15:58:37.377499Z", "market_exposure_dollars": "0.000000", "position_fp": "0.00", "realized_pnl_dollars": "-0.140000", "resting_orders_count": 0, "ticker": "KXUSTESTSREADING-26-SD", "total_traded_dollars": "7.140000"},
		{"fees_paid_dollars": "1.448100", "last_updated_ts": "2026-05-24T15:58:26.920859Z", "market_exposure_dollars": "0.000000", "position_fp": "0.00", "realized_pnl_dollars": "-25.671900", "resting_orders_count": 0, "ticker": "KXAISPIKE-26B-1550", "total_traded_dollars": "102.671900"},
		{"fees_paid_dollars": "0.470000", "last_updated_ts": "2026-05-16T21:21:19.883745Z", "market_exposure_dollars": "0.000000", "position_fp": "0.00", "realized_pnl_dollars": "-1.740000", "resting_orders_count": 0, "ticker": "KXCODINGMODEL-26DEC-GOOG", "total_traded_dollars": "30.740000"},
	],
}

# One real list_orders element (a prior sell/exit) — same inner shape as a
# place body's "order" object, used to confirm _parse_order works for the
# reconciler's list_orders path too.
LIST_ORDERS_SELL_ELEMENT = {
	"action": "sell", "book_side": "ask", "client_order_id": "",
	"created_time": "2026-05-24T15:58:37.377427Z",
	"fill_count_fp": "7.00", "initial_count_fp": "7.00", "remaining_count_fp": "0.00",
	"last_update_time": "2026-05-24T15:58:37.377427Z",
	"maker_fees_dollars": "0.000000", "maker_fill_cost_dollars": "0.000000",
	"no_price_dollars": "0.9900", "yes_price_dollars": "0.0100",
	"order_id": "7973a122-3cc8-40ce-a857-00fbf2043f18",
	"outcome_side": "no", "side": "yes", "status": "executed", "subaccount_number": 0,
	"taker_fees_dollars": "0.120000", "taker_fill_cost_dollars": "2.660000",
	"ticker": "KXUSTESTSREADING-26-SD", "type": "market",
	"user_id": "db7a007b-7e27-4306-9ffe-9b283082a6d8",
}
