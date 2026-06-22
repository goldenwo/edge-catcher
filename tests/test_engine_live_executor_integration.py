"""End-to-end integration tests for :class:`LiveExecutor` against MockKalshiServer.

Per D spec L785-L797: five tests covering the four canonical Kalshi response
shapes (201 happy, 201 partial, 503 → NetworkError, 400 → OrderRejected) plus
the strategy-through-dispatch end-to-end path.

These tests exercise the full :class:`KalshiOrderClient`.place pipeline:

* Signed Authorization header is computed and sent
* Request body is JSON-encoded with Kalshi's canonical shape (yes_price /
  no_price, action, side, ticker, count, time_in_force, client_order_id)
* Response status drives the LiveExecutor.place branch (filled / pending /
  rejected) per the conservative error-mapping policy

The fixture (``tests/fixtures/mock_kalshi_server.py``) is designed for reuse
by sub-project E's CR-5 — the response shapes are parameterizable, not
hardcoded into a single one-off ``MockTransport`` handler.
"""

from __future__ import annotations

import json

import pytest

from edge_catcher.engine.executor import OrderRequest
from edge_catcher.engine.executors.live import LiveExecutor

from tests.fixtures.mock_kalshi_server import (
	MockKalshiServer,
	kalshi_201_filled,
	kalshi_201_partial,
	kalshi_400_rejected,
)

# Fixtures (mock_kalshi_server / live_cfg / live_audit / signing_env /
# zero_backoff) live in ``tests/fixtures/mock_kalshi_server.py`` and are
# registered via the top-level ``tests/conftest.py``'s ``pytest_plugins``
# declaration — pytest's canonical home for the registration to silence
# the "module already imported" rewrite warning.


# ---------------------------------------------------------------------------
# Test request builder — single canonical Phase-1 shape for integration tests
# ---------------------------------------------------------------------------


def _engine_request(
	*,
	ticker: str = "KXSOL15M-26MAY09H06",
	series: str = "KXSOL15M",
	side: str = "yes",
	size: int = 10,
	limit: int = 5,
	strategy: str = "debut_fade",
	client_order_id: str = "debut_fade-KXSOL15M-26MAY09H06-1715195456789-abc12345",
	action: str = "buy",
) -> OrderRequest:
	return OrderRequest(
		ticker=ticker,
		series=series,
		side=side,  # type: ignore[arg-type]
		size_contracts=size,
		limit_price_cents=limit,
		strategy=strategy,
		client_order_id=client_order_id,
		action=action,  # type: ignore[arg-type]
	)


# ---------------------------------------------------------------------------
# Integration #1 — Happy path: 201 with single full fill
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_integration_happy_path_filled(
	mock_kalshi_server: MockKalshiServer,
	live_cfg,
	live_audit,
	signing_env,
) -> None:
	"""Failure mode prevented: the full chain (engine OrderRequest → A's
	wire payload → signing → A's response parse → D's translate) silently
	corrupts a field — e.g. side rendered as ``"YES"`` instead of
	``"yes"``, or count cast through a float that loses precision. The
	end-to-end assertion catches it.

	Validates:
	* Authorization header is present (auth.py was invoked)
	* Body fields match what D's builder produced
	* OrderResult is filled with correct blended/fill_pct/slippage
	"""
	mock_kalshi_server.queue_response(kalshi_201_filled(
		order_id="ord-mock-happy",
		ticker="KXSOL15M-26MAY09H06",
		count=10,
		yes_price=5,
	))
	client = mock_kalshi_server.make_client(live_cfg, live_audit)
	executor = LiveExecutor(client)

	result = await executor.place(_engine_request(size=10, limit=5))

	# OrderResult shape
	assert result.status == "filled"
	assert result.intended_size == 10
	assert result.filled_size == 10
	assert result.blended_entry_cents == 5
	assert result.fill_pct == pytest.approx(1.0)
	assert result.slippage_cents == 0
	assert result.order_id == "ord-mock-happy"
	assert result.rejection_reason is None

	# Wire-level assertions — proves the chain actually went through
	assert len(mock_kalshi_server.requests) == 1
	req = mock_kalshi_server.requests[0]
	assert req.method == "POST"
	assert req.url.path == "/trade-api/v2/portfolio/events/orders"
	assert "KALSHI-ACCESS-SIGNATURE" in req.headers, (
		"Authorization signature must be present — auth.make_auth_headers "
		"was not invoked or its result was dropped"
	)
	body = json.loads(req.content)
	assert body["ticker"] == "KXSOL15M-26MAY09H06"
	# V2 single-YES-book shape: buy-yes → bid; price is fixed-point YES dollars;
	# count is a fixed-point string; `action`/`yes_price` are gone.
	assert "action" not in body
	assert body["side"] == "bid"
	assert body["count"] == "10.00"
	assert body["price"] == "0.0500"
	assert body["self_trade_prevention_type"] == "taker_at_cross"
	# A's wire layer translates "ioc" → "immediate_or_cancel" — verbatim
	# (Kalshi rejects the short form per test_place_translates_tif_short_to_kalshi_verbose)
	assert body["time_in_force"] == "immediate_or_cancel"
	# client_order_id flows verbatim through the chain (idempotency contract)
	assert body["client_order_id"] == "debut_fade-KXSOL15M-26MAY09H06-1715195456789-abc12345"


# ---------------------------------------------------------------------------
# Integration #2 — Partial IOC: 201 with multi-fill blended price
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_integration_partial_ioc_fill(
	mock_kalshi_server: MockKalshiServer,
	live_cfg,
	live_audit,
	signing_env,
) -> None:
	"""Failure mode prevented: A's Order.raw doesn't preserve the per-fill
	array exactly as Kalshi returned it, so D's ``_translate_order``
	computes blended against a corrupted shape. End-to-end test ensures
	the round-trip is faithful.

	Math: fills = [5c × 5, 6c × 2] → blended = (25 + 12) / 7 = 5.28… → 5.
	"""
	mock_kalshi_server.queue_response(kalshi_201_partial(
		order_id="ord-mock-partial",
		count=10,
		filled_count=7,
		limit_price_cents=5,
		fills=[
			{"price": 5, "size": 5},
			{"price": 6, "size": 2},
		],
	))
	client = mock_kalshi_server.make_client(live_cfg, live_audit)
	executor = LiveExecutor(client)

	result = await executor.place(_engine_request(size=10, limit=5))

	assert result.status == "filled"
	assert result.intended_size == 10
	assert result.filled_size == 7
	assert result.blended_entry_cents == 5  # round(37/7) = 5
	assert result.fill_pct == pytest.approx(0.7)
	# Blended == limit → 0 slippage on the IOC outcome
	assert result.slippage_cents == 0
	assert result.order_id == "ord-mock-partial"


# ---------------------------------------------------------------------------
# Integration #3 — 503 storm → NetworkError → pending+None
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_integration_503_storm_maps_to_pending_unknown(
	mock_kalshi_server: MockKalshiServer,
	live_cfg,
	live_audit,
	signing_env,
	zero_backoff,
) -> None:
	"""Failure mode prevented: a 5xx-storm exhausts A's retries and raises
	:class:`NetworkError`, but D fails to map it to ``pending``. Without
	the pending mapping, B can't reconcile a potentially-placed order →
	funds-at-risk.

	The ``zero_backoff`` fixture stubs A's ``asyncio.sleep`` so the test
	completes in <100ms instead of waiting through the real ~62s
	exponential-backoff budget.

	Sticky-tail response queueing means every retry sees the same 503 —
	all max_retries+1 attempts (default 6) fail, NetworkError fires.
	"""
	mock_kalshi_server.queue_503_storm()
	client = mock_kalshi_server.make_client(live_cfg, live_audit)
	executor = LiveExecutor(client)

	result = await executor.place(_engine_request(size=10, limit=5))

	assert result.status == "pending"
	assert result.order_id is None, (
		"NetworkError → pending must have order_id=None (Kalshi-side state "
		"UNKNOWN; B reconciles via client_order_id, not by ID)"
	)
	assert result.filled_size == 0
	assert result.blended_entry_cents == 0
	assert result.rejection_reason is not None
	# A's NetworkError formatting: ``HTTP 503 after N retries``. D wraps in
	# either ``kalshi_unreachable:`` (caught from ``except NetworkError``) or
	# ``kalshi_5xx_unknown_state:503`` (caught from ``except KalshiAPIError``
	# which is the parent of NetworkError). Both are valid pending mappings —
	# the funds-at-risk semantics are identical (Kalshi state unknown).
	assert (
		result.rejection_reason.startswith("kalshi_unreachable:")
		or result.rejection_reason.startswith("kalshi_5xx_unknown_state:")
	), (
		f"Expected pending mapping for 5xx-exhaustion, got: "
		f"{result.rejection_reason!r}"
	)

	# Retry budget was actually exercised — at least 2 attempts (initial +
	# 1 retry). Default max_retries=5 → 6 attempts; we assert >= 2 so the
	# test isn't brittle to LiveConfig defaults changing.
	assert len(mock_kalshi_server.requests) >= 2, (
		f"Expected retry budget to be exercised, only saw "
		f"{len(mock_kalshi_server.requests)} request(s)"
	)


# ---------------------------------------------------------------------------
# Integration #4 — 400 → OrderRejected → rejected with kalshi_4xx:400
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_integration_400_maps_to_rejected(
	mock_kalshi_server: MockKalshiServer,
	live_cfg,
	live_audit,
	signing_env,
) -> None:
	"""Failure mode prevented: Kalshi's authoritative 4xx rejection (validation,
	business rule) silently triggers a retry path, wasting rate-limit budget
	and potentially placing the same rejected order twice (Kalshi
	idempotency-keyed but the operator gets paged on the duplicate audit
	row anyway).

	D's contract: 4xx → ``OrderResult(rejected, "kalshi_4xx:<status>")``,
	NO retry, ``order_id=None`` (Kalshi never accepted).
	"""
	status, body = kalshi_400_rejected(
		code="invalid_price",
		message="price out of band",
	)
	mock_kalshi_server.queue_status(status, body)
	client = mock_kalshi_server.make_client(live_cfg, live_audit)
	executor = LiveExecutor(client)

	result = await executor.place(_engine_request(size=10, limit=5))

	assert result.status == "rejected"
	assert result.rejection_reason == "kalshi_4xx:400"
	assert result.order_id is None
	assert result.filled_size == 0
	# A's client should NOT have retried the 4xx (per its own contract);
	# only one request reached the server.
	assert len(mock_kalshi_server.requests) == 1, (
		f"4xx must not be retried — saw {len(mock_kalshi_server.requests)} requests"
	)


# ---------------------------------------------------------------------------
# Integration #5 — End-to-end: build_entry_order → LiveExecutor → OrderResult
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_integration_end_to_end_builder_to_executor(
	mock_kalshi_server: MockKalshiServer,
	live_cfg,
	live_audit,
	signing_env,
) -> None:
	"""Failure mode prevented: D's ``build_entry_order`` produces an
	OrderRequest with a field shape LiveExecutor can't translate (e.g.
	missing ``action``, or ``side`` typed as Literal but builder produced
	a plain str). End-to-end test pins the contract — from a strategy
	Signal all the way through to a Kalshi response.

	This is the test that proves the SEAM between D's pure-function
	builders and D's I/O executor is clean. If a future refactor breaks
	the seam (e.g. changes OrderRequest.action to require a non-default),
	this test fails.
	"""
	from datetime import datetime, timezone

	from edge_catcher.engine.execution import ExecCfg, build_entry_order
	from edge_catcher.engine.strategy_base import Signal

	# Build a Signal a strategy might emit (with all live-execution fields).
	sig = Signal(
		action="enter",
		ticker="KXSOL15M-26MAY09H06",
		side="yes",
		series="KXSOL15M",
		strategy="debut_fade",
		reason="live-entry",
		entry_price_cents=42,
		stop_loss_distance_cents=8,
	)
	cfg = ExecCfg(
		entry_slippage_cents=2,
		exit_slippage_cents={"take_profit": 2, "stop_loss": 10, "time_exit": 5},
	)
	now = datetime(2026, 5, 11, 12, 0, 0, tzinfo=timezone.utc)
	req = build_entry_order(sig, allowed_size=10, cfg=cfg, now=now)

	# D's builder applied 2c slippage: 42 + 2 = 44c limit
	assert req.limit_price_cents == 44
	assert req.action == "buy"

	# Wire through MockKalshiServer
	mock_kalshi_server.queue_response(kalshi_201_filled(
		order_id="ord-e2e-1",
		ticker="KXSOL15M-26MAY09H06",
		count=10,
		yes_price=44,
		fills=[{"price": 44, "size": 10}],
	))
	client = mock_kalshi_server.make_client(live_cfg, live_audit)
	executor = LiveExecutor(client)

	result = await executor.place(req)

	# Full round-trip: Signal → OrderRequest → Kalshi → Order → OrderResult
	assert result.status == "filled"
	assert result.order_id == "ord-e2e-1"
	assert result.blended_entry_cents == 44
	assert result.filled_size == 10
	assert result.fill_pct == pytest.approx(1.0)

	# Wire-side: the client_order_id D's builder generated is what hit Kalshi
	body = json.loads(mock_kalshi_server.requests[0].content)
	assert body["client_order_id"] == req.client_order_id
	# Should be in the {strategy}-{ticker}-{ms_ts}-{uuid8} format
	assert body["client_order_id"].startswith("debut_fade-KXSOL15M-26MAY09H06-")
