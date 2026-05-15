"""Unit tests for :mod:`edge_catcher.engine.executors.live`.

Covers spec test inventory #7-#13, #15, #17 (v1.6.0 PR 4 / sub-project D):

* #7  — happy path (mocked KalshiOrderClient returns a well-formed Order)
* #8  — partial-IOC fill (multi-fill blended price)
* #9  — zero-fill IOC → rejected
* #10 — :class:`OrderRejected` (4xx) → rejected with ``kalshi_4xx:<status>``
* #11 — :class:`NetworkError` → pending with ``kalshi_unreachable:...``,
        order_id=None (B reconciles via client_order_id)
* #12 — :class:`CapExceededError` → rejected with ``absolute_max_exceeded``
* #13 — malformed fills (Kalshi returned filled_count>0 with bad fills) →
        pending with ``kalshi_malformed_fills``, order_id preserved
* #15 — AST regression: ``_to_kalshi_request`` must not hardcode ``"buy"``/
        ``"sell"`` literals (action MUST come from ``req.action``)
* #17 — malformed fills is pending (NOT filled) — does not pretend to know
        the price

Test methodology: every test names the failure mode it prevents. Each result-
assertion checks the *defined* :class:`OrderResult` rejection_reason / status
the dispatch layer needs to route correctly.
"""

from __future__ import annotations

import ast
import asyncio
import inspect

import pytest

from edge_catcher.engine.executor import OrderRequest, OrderResult
from edge_catcher.engine.executors.live import (
	LiveExecutor,
	_to_kalshi_request,
)
from edge_catcher.live.client import Order
from edge_catcher.live.errors import (
	CapExceededError,
	KalshiAPIError,
	NetworkError,
	OrderRejected,
)


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


class FakeKalshiClient:
	"""Minimal async stand-in for :class:`KalshiOrderClient`.

	Tests configure ``.return_value`` (Order to return) OR ``.raise_value``
	(exception to raise). Captures every call's request for assertion.
	"""

	def __init__(self) -> None:
		self.return_value: Order | None = None
		self.raise_value: BaseException | None = None
		self.calls: list = []

	async def place(self, req):  # type: ignore[no-untyped-def]
		self.calls.append(req)
		if self.raise_value is not None:
			raise self.raise_value
		assert self.return_value is not None, "Test setup error: no return_value/raise_value"
		return self.return_value


def _make_request(
	*,
	ticker: str = "KXSOL15M-26MAY09H06",
	series: str = "KXSOL15M",
	side: str = "yes",
	size: int = 10,
	limit: int = 5,
	strategy: str = "debut-fade",
	client_order_id: str = "debut-fade-KXSOL15M-1715195456789-abc12345",
	action: str = "buy",
) -> OrderRequest:
	"""Construct an engine ``OrderRequest`` with sensible Phase-1 defaults."""
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


def _make_order(
	*,
	order_id: str = "ord-kx-abc-123",
	ticker: str = "KXSOL15M-26MAY09H06",
	side: str = "yes",
	action: str = "buy",
	count: int = 10,
	limit_price: int = 5,
	filled_count: int = 10,
	fills: list | None = None,
	status: str = "executed",
) -> Order:
	"""Construct a :class:`Order` matching what KalshiOrderClient.place returns.

	The ``raw`` dict mirrors Kalshi's wire response shape (the per-fill array
	lives inside ``raw["fills"]`` — spec L408).
	"""
	raw: dict = {
		"order_id": order_id,
		"ticker": ticker,
		"side": side,
		"action": action,
		"count": count,
		"yes_price": limit_price,
		"status": status,
		"filled_count": filled_count,
	}
	if fills is not None:
		raw["fills"] = fills
	return Order(
		order_id=order_id,
		ticker=ticker,
		side=side,  # type: ignore[arg-type]
		action=action,  # type: ignore[arg-type]
		count=count,
		limit_price_cents=limit_price,
		time_in_force="ioc",
		status=status,
		filled_count=filled_count,
		raw=raw,
	)


# ---------------------------------------------------------------------------
# #7 — Happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_place_happy_path_single_fill():
	"""KalshiOrderClient returns a full IOC fill → OrderResult(filled, slippage=0).

	Failure mode prevented: silent demotion of a perfectly-filled order to
	pending/rejected. Asserts the canonical happy-path Order shape (raw["fills"]
	list with price+size dicts) round-trips cleanly through _translate_order.
	"""
	client = FakeKalshiClient()
	client.return_value = _make_order(
		filled_count=10,
		limit_price=5,
		fills=[{"price": 5, "size": 10}],
	)
	executor = LiveExecutor(client)  # type: ignore[arg-type]
	req = _make_request(size=10, limit=5)

	result = await executor.place(req)

	assert isinstance(result, OrderResult)
	assert result.status == "filled"
	assert result.intended_size == 10
	assert result.filled_size == 10
	assert result.blended_entry_cents == 5
	assert result.fill_pct == pytest.approx(1.0)
	assert result.slippage_cents == 0
	assert result.rejection_reason is None
	assert result.order_id == "ord-kx-abc-123"


# ---------------------------------------------------------------------------
# #8 — Partial-IOC fill (multi-fill blended price)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_place_partial_ioc_blended_price():
	"""Partial IOC across two price levels → volume-weighted blended price.

	Failure mode prevented: collapsing a walked-book fill to single-price
	leaks the walked-volume slippage signal F's slippage-distribution chart
	depends on.

	Math: fill at 5¢ × 5 contracts + fill at 6¢ × 2 contracts = 37¢ over 7
	contracts → round(37/7) = round(5.285…) = 5¢.
	"""
	client = FakeKalshiClient()
	client.return_value = _make_order(
		filled_count=7,
		limit_price=5,
		fills=[
			{"price": 5, "size": 5},
			{"price": 6, "size": 2},
		],
	)
	executor = LiveExecutor(client)  # type: ignore[arg-type]
	req = _make_request(size=10, limit=5)

	result = await executor.place(req)

	assert result.status == "filled"
	assert result.intended_size == 10
	assert result.filled_size == 7
	assert result.blended_entry_cents == 5
	assert result.fill_pct == pytest.approx(0.7)
	# Blended (5) - limit (5) = 0; signed slippage when walk lands at limit.
	assert result.slippage_cents == 0
	assert result.order_id == "ord-kx-abc-123"


@pytest.mark.asyncio
async def test_place_partial_ioc_blended_price_with_walk():
	"""Walked-book fill: blended price ABOVE limit → positive slippage.

	Fills: 5¢×3 + 7¢×2 = 29¢ over 5 → round(29/5) = 6¢. Limit was 5¢; signed
	slippage = blended - limit = +1¢ (paid 1¢ per contract above the limit).
	"""
	client = FakeKalshiClient()
	client.return_value = _make_order(
		filled_count=5,
		limit_price=5,
		fills=[
			{"price": 5, "size": 3},
			{"price": 7, "size": 2},
		],
	)
	executor = LiveExecutor(client)  # type: ignore[arg-type]
	req = _make_request(size=10, limit=5)

	result = await executor.place(req)

	assert result.status == "filled"
	assert result.filled_size == 5
	assert result.blended_entry_cents == 6
	assert result.slippage_cents == 1


# ---------------------------------------------------------------------------
# #9 — Zero-fill IOC → rejected
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_place_zero_fill_ioc_rejected():
	"""IOC at limit found no liquidity → OrderResult(rejected, ioc_zero_fill).

	Failure mode prevented: writing a phantom fill row when Kalshi accepted
	the order but matched zero contracts. ``order_id=None`` because no fill
	means no position to reconcile.
	"""
	client = FakeKalshiClient()
	client.return_value = _make_order(
		order_id="ord-kx-empty",
		filled_count=0,
		fills=[],
	)
	executor = LiveExecutor(client)  # type: ignore[arg-type]
	req = _make_request(size=10, limit=5)

	result = await executor.place(req)

	assert result.status == "rejected"
	assert result.rejection_reason == "ioc_zero_fill"
	assert result.intended_size == 10
	assert result.filled_size == 0
	assert result.blended_entry_cents == 0
	assert result.fill_pct == 0.0
	assert result.slippage_cents == 0
	assert result.order_id is None


# ---------------------------------------------------------------------------
# #10 — OrderRejected (Kalshi 4xx) → rejected
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_place_kalshi_4xx_rejected():
	"""Kalshi 4xx → OrderResult(rejected, kalshi_4xx:<status>).

	Failure mode prevented: retrying a Kalshi-authoritative rejection wastes
	rate-limit budget on a guaranteed-failure path. ``order_id=None`` because
	Kalshi never accepted; nothing to reconcile.
	"""
	client = FakeKalshiClient()
	client.raise_value = OrderRejected(
		400, '{"error":"validation"}', "/trade-api/v2/portfolio/orders"
	)
	executor = LiveExecutor(client)  # type: ignore[arg-type]
	req = _make_request(size=10, limit=5)

	result = await executor.place(req)

	assert result.status == "rejected"
	assert result.rejection_reason == "kalshi_4xx:400"
	assert result.order_id is None
	assert result.filled_size == 0
	assert result.intended_size == 10


@pytest.mark.asyncio
async def test_place_kalshi_4xx_preserves_status_code():
	"""422 (unprocessable) yields ``kalshi_4xx:422`` so the audit/ops trail
	can distinguish 400-validation from 401-auth from 422-state-conflict."""
	client = FakeKalshiClient()
	client.raise_value = OrderRejected(
		422, '{"error":"insufficient_balance"}', "/trade-api/v2/portfolio/orders"
	)
	executor = LiveExecutor(client)  # type: ignore[arg-type]
	req = _make_request()

	result = await executor.place(req)

	assert result.rejection_reason == "kalshi_4xx:422"


# ---------------------------------------------------------------------------
# #11 — NetworkError → pending
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_place_network_error_pending():
	"""NetworkError → OrderResult(pending, kalshi_unreachable, order_id=None).

	Failure mode prevented: raising into dispatch crashes the coroutine and
	strands the live order with no record. B reconciles via
	``client_order_id`` on the next poll/WS event — D guarantees per-attempt
	uniqueness of client_order_id (see ``_make_client_order_id`` in Agent
	3b.A's execution.py); B uses a Signal-derived natural key for dedup.

	Sub-invariants for B-reconciliation contract:
	  * ``intended_size == req.size_contracts`` (B knows what size to reconcile to)
	  * Status is ``pending`` (NOT rejected) — Kalshi-side state is UNKNOWN
	"""
	client = FakeKalshiClient()
	client.raise_value = NetworkError("test connection refused after 5 retries")
	executor = LiveExecutor(client)  # type: ignore[arg-type]
	req = _make_request(size=10, limit=5)

	result = await executor.place(req)

	assert result.status == "pending"
	assert result.rejection_reason is not None
	assert "kalshi_unreachable" in result.rejection_reason
	assert "test connection refused" in result.rejection_reason
	assert result.order_id is None
	assert result.intended_size == 10  # B needs this for reconciliation
	assert result.filled_size == 0


@pytest.mark.asyncio
async def test_place_network_error_does_not_propagate():
	"""Concrete proof: ``place()`` swallows NetworkError. Dispatch never sees
	the exception — it always receives a defined OrderResult to route on."""
	client = FakeKalshiClient()
	client.raise_value = NetworkError("transient DNS failure")
	executor = LiveExecutor(client)  # type: ignore[arg-type]
	req = _make_request()

	# If place() re-raised, the next line would propagate the exception
	# and pytest would mark this test as errored instead of just asserting.
	result = await executor.place(req)
	assert result is not None
	assert isinstance(result, OrderResult)


# ---------------------------------------------------------------------------
# Kalshi 5xx semantics — KalshiAPIError after retries (NOT in original test
# inventory but required by the task brief's anti-pattern guard #5)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_place_kalshi_5xx_pending_unknown():
	"""Kalshi 5xx after retries → pending (NOT rejected).

	Failure mode prevented: treating 5xx as REJECTED would silently drop the
	order from B's reconciliation set; if Kalshi actually accepted it we'd
	have an orphaned live position. Mark as pending; B reconciles.
	"""
	client = FakeKalshiClient()
	client.raise_value = KalshiAPIError(
		503, '{"error":"upstream_unavailable"}', "/trade-api/v2/portfolio/orders"
	)
	executor = LiveExecutor(client)  # type: ignore[arg-type]
	req = _make_request(size=10, limit=5)

	result = await executor.place(req)

	assert result.status == "pending"
	assert result.rejection_reason is not None
	assert "kalshi_5xx_unknown_state" in result.rejection_reason
	assert "503" in result.rejection_reason
	assert result.order_id is None


# ---------------------------------------------------------------------------
# #12 — CapExceededError → rejected
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_place_cap_exceeded_rejected():
	"""CapExceededError → rejected with ``absolute_max_exceeded``.

	Failure mode prevented: C's sizing pipeline producing a size > $50
	ABSOLUTE_MAX should never silently place. Defense in depth: surface as
	a typed rejection so audit/ops trail captures the C/D inconsistency.
	"""
	client = FakeKalshiClient()
	client.raise_value = CapExceededError(
		exposure_dollars=75.0, cap_dollars=50.0, cap_name="ABSOLUTE_MAX"
	)
	executor = LiveExecutor(client)  # type: ignore[arg-type]
	req = _make_request(size=150, limit=50)

	result = await executor.place(req)

	assert result.status == "rejected"
	assert result.rejection_reason == "absolute_max_exceeded"
	assert result.order_id is None


# ---------------------------------------------------------------------------
# #13 / #17 — Malformed fills → pending (NOT filled)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_place_malformed_fills_missing_array_pending():
	"""filled_count>0 but no ``fills`` key in raw → pending + order_id preserved.

	Failure mode prevented: pretending we know the blended price ("perfect
	fill at limit") would corrupt B's slippage tracking and F's slippage
	chart. Order WAS placed (we have the order_id); B re-fetches and
	reconciles the true price.
	"""
	client = FakeKalshiClient()
	# Note: _make_order with fills=None produces an Order whose raw dict has
	# NO "fills" key — exactly the malformed shape we're guarding against.
	client.return_value = _make_order(
		order_id="ord-kx-malformed",
		filled_count=5,
		fills=None,
	)
	executor = LiveExecutor(client)  # type: ignore[arg-type]
	req = _make_request(size=10, limit=5)

	result = await executor.place(req)

	assert result.status == "pending"
	assert result.rejection_reason == "kalshi_malformed_fills"
	# Order ID MUST be preserved — B reconciles via order_id, not client_order_id.
	assert result.order_id == "ord-kx-malformed"
	assert result.intended_size == 10
	assert result.filled_size == 5
	# Blended-price is 0-sentinel; we don't lie about a price we never saw.
	assert result.blended_entry_cents == 0
	# fill_pct still reflects the reported partial.
	assert result.fill_pct == pytest.approx(0.5)


@pytest.mark.asyncio
async def test_place_malformed_fills_empty_list_pending():
	"""filled_count>0 with ``fills=[]`` → pending (defensive)."""
	client = FakeKalshiClient()
	client.return_value = _make_order(
		order_id="ord-kx-empty-fills",
		filled_count=5,
		fills=[],
	)
	executor = LiveExecutor(client)  # type: ignore[arg-type]
	req = _make_request(size=10, limit=5)

	result = await executor.place(req)

	# filled_count=0 wouldn't reach here — the zero-fill branch fires first.
	# This is the filled_count>0 + empty-array shape.
	assert result.status == "rejected" if result.filled_size == 0 else "pending"
	# Actually the FakeOrder above has filled_count=5 but fills=[]; ensure
	# we go through the pending branch.
	assert result.status == "pending"
	assert result.rejection_reason == "kalshi_malformed_fills"
	assert result.order_id == "ord-kx-empty-fills"


@pytest.mark.asyncio
async def test_place_malformed_fills_non_dict_entries_pending():
	"""``fills`` is a list of strings (wrong shape) → pending."""
	client = FakeKalshiClient()
	client.return_value = _make_order(
		order_id="ord-kx-bad-fills",
		filled_count=5,
		fills=["not-a-dict-entry"],  # type: ignore[list-item]
	)
	executor = LiveExecutor(client)  # type: ignore[arg-type]
	req = _make_request()

	result = await executor.place(req)

	assert result.status == "pending"
	assert result.rejection_reason == "kalshi_malformed_fills"
	assert result.order_id == "ord-kx-bad-fills"


@pytest.mark.asyncio
async def test_place_malformed_fills_missing_price_key_pending():
	"""``fills`` entries missing 'price' key → pending."""
	client = FakeKalshiClient()
	client.return_value = _make_order(
		order_id="ord-kx-no-price",
		filled_count=5,
		fills=[{"size": 5}],
	)
	executor = LiveExecutor(client)  # type: ignore[arg-type]
	req = _make_request()

	result = await executor.place(req)

	assert result.status == "pending"
	assert result.rejection_reason == "kalshi_malformed_fills"


# ---------------------------------------------------------------------------
# Client-order-id idempotency contract (proves we forward the key)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_place_forwards_client_order_id_to_kalshi_request():
	"""``client_order_id`` MUST be forwarded into the Kalshi request body.

	Failure mode prevented: if D dropped the idempotency key, two retries
	of the same Signal would land as two distinct orders on Kalshi — funds
	at risk for the price of one. Test asserts the value flows through
	unmodified.
	"""
	client = FakeKalshiClient()
	client.return_value = _make_order(filled_count=10, fills=[{"price": 5, "size": 10}])
	executor = LiveExecutor(client)  # type: ignore[arg-type]
	req = _make_request(client_order_id="debut-fade-KXSOL15M-1715195456789-abc12345")

	await executor.place(req)

	assert len(client.calls) == 1
	kalshi_req = client.calls[0]
	# The Kalshi request must preserve our idempotency key verbatim.
	assert kalshi_req.client_order_id == "debut-fade-KXSOL15M-1715195456789-abc12345"


# ---------------------------------------------------------------------------
# #15 — AST regression: _to_kalshi_request must not hardcode "buy"/"sell"
# ---------------------------------------------------------------------------


def test_to_kalshi_request_does_not_hardcode_action():
	"""AST check: ``action=`` kwarg in ``_to_kalshi_request`` MUST be an
	attribute access on ``req``, NEVER a string literal.

	Failure mode prevented: a refactor accidentally re-introduces
	``action="buy"`` literal, silently inverting all sells to buys (the
	round-1-caught bug). The naive ``inspect.getsource`` substring search
	would false-positive on docstrings containing ``"buy"``; AST inspection
	is precise.

	Spec L763-L775.
	"""
	source = inspect.getsource(_to_kalshi_request)
	tree = ast.parse(source)
	violations = []
	for node in ast.walk(tree):
		if isinstance(node, ast.keyword) and node.arg == "action":
			# The kwarg value MUST be an attribute access (req.action), not a literal.
			if isinstance(node.value, ast.Constant) and node.value.value in ("buy", "sell"):
				violations.append(node.value.value)
	assert not violations, (
		f"_to_kalshi_request hardcodes action={violations!r}; must read from req.action"
	)


def test_to_kalshi_request_action_is_attribute_access():
	"""Positive form of #15: ``action=`` IS an ``ast.Attribute`` over ``req``.

	Stricter than the negative test — guards against future refactors that
	swap ``req.action`` for ``something_else.action`` or a function call.
	"""
	source = inspect.getsource(_to_kalshi_request)
	tree = ast.parse(source)
	found_action_kwarg = False
	for node in ast.walk(tree):
		if isinstance(node, ast.keyword) and node.arg == "action":
			found_action_kwarg = True
			assert isinstance(node.value, ast.Attribute), (
				f"action= must be an attribute access, got {ast.dump(node.value)}"
			)
			assert isinstance(node.value.value, ast.Name), (
				f"action= must access an attribute on a Name node, got {ast.dump(node.value)}"
			)
			assert node.value.value.id == "req", (
				f"action= must read from `req`, got `{node.value.value.id}`"
			)
			assert node.value.attr == "action"
	assert found_action_kwarg, "no action= kwarg found in _to_kalshi_request"


# ---------------------------------------------------------------------------
# Round-trip translation: buy/sell from engine OrderRequest reaches Kalshi
# ---------------------------------------------------------------------------


def test_to_kalshi_request_forwards_buy_action():
	"""``OrderRequest(action="buy")`` → ``KalshiOrderRequest(action="buy")``."""
	req = _make_request(action="buy", side="yes")
	kalshi_req = _to_kalshi_request(req)
	assert kalshi_req.action == "buy"
	assert kalshi_req.side == "yes"


def test_to_kalshi_request_forwards_sell_action():
	"""``OrderRequest(action="sell")`` → ``KalshiOrderRequest(action="sell")``.

	Without this end-to-end forwarding, sells would silently route as buys —
	the catastrophic round-1 bug pattern.
	"""
	req = _make_request(action="sell", side="yes")
	kalshi_req = _to_kalshi_request(req)
	assert kalshi_req.action == "sell"
	assert kalshi_req.side == "yes"


def test_to_kalshi_request_uses_ioc():
	"""All engine orders are IOC (Phase 1 invariant — no GTC entries)."""
	req = _make_request()
	kalshi_req = _to_kalshi_request(req)
	assert kalshi_req.time_in_force == "ioc"


def test_to_kalshi_request_passes_size_and_limit():
	"""``size_contracts`` → ``count``, ``limit_price_cents`` → ``limit_price_cents``."""
	req = _make_request(size=42, limit=37)
	kalshi_req = _to_kalshi_request(req)
	assert kalshi_req.count == 42
	assert kalshi_req.limit_price_cents == 37


# ---------------------------------------------------------------------------
# #17 — Error map coverage (parameterised — every Kalshi exception class)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
	"exception, expected_status, reason_substring",
	[
		# Kalshi authoritatively rejected (4xx): rejected, no retry.
		(
			OrderRejected(400, "{}", "/p"),
			"rejected",
			"kalshi_4xx:400",
		),
		(
			OrderRejected(422, "{}", "/p"),
			"rejected",
			"kalshi_4xx:422",
		),
		# 5xx after retries: pending (unknown Kalshi-side state).
		(
			KalshiAPIError(503, "{}", "/p"),
			"pending",
			"kalshi_5xx_unknown_state:503",
		),
		(
			KalshiAPIError(500, "{}", "/p"),
			"pending",
			"kalshi_5xx_unknown_state:500",
		),
		# Network failure: pending (unknown state — B reconciles).
		(
			NetworkError("ECONNRESET"),
			"pending",
			"kalshi_unreachable",
		),
		# Pre-flight cap breach: rejected (defense in depth from C's sizing).
		(
			CapExceededError(exposure_dollars=100.0, cap_dollars=50.0, cap_name="ABSOLUTE_MAX"),
			"rejected",
			"absolute_max_exceeded",
		),
	],
	ids=[
		"OrderRejected_400",
		"OrderRejected_422",
		"KalshiAPIError_503",
		"KalshiAPIError_500",
		"NetworkError",
		"CapExceededError",
	],
)
@pytest.mark.asyncio
async def test_error_map_coverage(exception, expected_status, reason_substring):
	"""Every exception class in :mod:`edge_catcher.live.errors` that can
	escape from :class:`KalshiOrderClient.place` MUST land on a defined
	:class:`OrderResult` with the expected status + reason.

	Failure mode prevented: an un-handled exception class would propagate
	out of ``LiveExecutor.place``, crashing dispatch and stranding the
	live order. This test pins the full error-map closure.
	"""
	client = FakeKalshiClient()
	client.raise_value = exception
	executor = LiveExecutor(client)  # type: ignore[arg-type]
	req = _make_request()

	result = await executor.place(req)

	assert result.status == expected_status, (
		f"{type(exception).__name__} → status={result.status!r}, "
		f"expected {expected_status!r}"
	)
	assert result.rejection_reason is not None
	assert reason_substring in result.rejection_reason, (
		f"{type(exception).__name__} → reason={result.rejection_reason!r}, "
		f"expected substring {reason_substring!r}"
	)


# ---------------------------------------------------------------------------
# Defensive parsing — Order.raw is not a dict (Kalshi wire-shape drift)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_place_raw_not_dict_pending():
	"""If Kalshi response shape drifts and ``order.raw`` is not a dict,
	we must NOT crash. Demote to pending + preserve order_id."""
	client = FakeKalshiClient()
	bad = _make_order(order_id="ord-bad-raw", filled_count=5, fills=None)
	# Forcibly corrupt raw to a non-dict — simulates a future Kalshi schema change.
	object.__setattr__(bad, "raw", "this is not a dict")
	client.return_value = bad
	executor = LiveExecutor(client)  # type: ignore[arg-type]
	req = _make_request(size=10, limit=5)

	result = await executor.place(req)

	assert result.status == "pending"
	assert result.rejection_reason == "kalshi_malformed_fills"
	assert result.order_id == "ord-bad-raw"


# ---------------------------------------------------------------------------
# Exception propagation contract — never re-raises, EXCEPT CancelledError
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_place_propagates_cancelled_error() -> None:
	"""Failure mode prevented: a future refactor adds a broad ``except
	Exception`` BEFORE the ``except asyncio.CancelledError: raise`` guard, or
	removes the explicit CancelledError handler entirely.

	``asyncio.CancelledError`` is the cooperative-cancellation signal sent
	when a task is cancelled (engine shutdown, supervisor kill, etc.).
	Swallowing it inside ``place()`` would deadlock the dispatch coroutine
	at shutdown because the cancel scope never collapses.

	The fix: explicitly re-raise CancelledError BEFORE the defensive
	``except Exception`` catch-all. This test pins that ordering — if
	someone reorders the except clauses (Python checks them in source
	order; CancelledError is a subclass of BaseException, not Exception,
	so the order matters less in Python 3.8+ but the explicit guard
	doubles as documentation).
	"""
	client = FakeKalshiClient()
	client.raise_value = asyncio.CancelledError()
	executor = LiveExecutor(client)  # type: ignore[arg-type]
	req = _make_request(size=10, limit=5)

	with pytest.raises(asyncio.CancelledError):
		await executor.place(req)


@pytest.mark.asyncio
async def test_place_unexpected_exception_routes_to_pending_unknown() -> None:
	"""Failure mode prevented: an unmapped exception type (e.g., a future
	Kalshi client version raising a new exception class, an OSError that
	NetworkError didn't wrap, an AttributeError from a malformed Order
	object) propagates out of ``place()``, crashing the dispatch coroutine
	AFTER the order may have been sent to Kalshi.

	The module contract (place() docstring: "Every exception path returns a
	defined OrderResult — never re-raises") requires every unexpected
	exception to be routed to pending+None so B can reconcile via
	client_order_id. Without the catch-all, dispatch crashes and no pending
	row is written — the order is stranded on Kalshi with no local record
	(funds-at-risk).

	The catch-all sits AFTER the typed handlers (OrderRejected, NetworkError,
	etc.) so typed errors get their specific rejection_reason; unmapped
	types get ``unexpected_exception:<ClassName>``.
	"""
	# Use RuntimeError as a stand-in for "unmapped exception subtype".
	# Any non-mapped Exception subclass would exercise the same path.
	client = FakeKalshiClient()
	client.raise_value = RuntimeError("simulated unmapped client exception")
	executor = LiveExecutor(client)  # type: ignore[arg-type]
	req = _make_request(size=10, limit=5)

	result = await executor.place(req)

	assert result.status == "pending", (
		"Unexpected exceptions must route to pending — B reconciles via "
		"client_order_id when Kalshi-side state is unknown"
	)
	assert result.order_id is None, (
		"order_id MUST be None on the unexpected-exception path: we don't "
		"know whether Kalshi got the request, so we can't make up an order_id"
	)
	assert result.rejection_reason is not None
	assert result.rejection_reason.startswith("unexpected_exception:"), (
		f"rejection_reason should encode the exception class name to aid "
		f"on-call triage; got {result.rejection_reason!r}"
	)
	assert "RuntimeError" in result.rejection_reason


@pytest.mark.asyncio
async def test_place_attribute_error_from_malformed_order_routes_to_pending() -> None:
	"""Failure mode prevented: a Kalshi client version returns an Order object
	missing an expected attribute (e.g., ``filled_count``), causing
	``_translate_order`` to raise ``AttributeError``. Without the catch-all,
	this propagates and crashes dispatch. With it, we route to pending+None
	with a descriptive rejection_reason.
	"""

	class _BadClient:
		async def place(self, req):  # type: ignore[no-untyped-def]
			raise AttributeError("simulated: Order missing .filled_count")

	executor = LiveExecutor(_BadClient())  # type: ignore[arg-type]
	req = _make_request(size=10, limit=5)

	result = await executor.place(req)

	assert result.status == "pending"
	assert result.order_id is None
	assert result.rejection_reason is not None
	assert "AttributeError" in result.rejection_reason


# ---------------------------------------------------------------------------
# #R2-cleanup — _translate_order zero-size guard (A-F1)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_translate_order_rejects_zero_size_contracts() -> None:
	"""Failure mode prevented: a sizing bug upstream produces an OrderRequest
	with size_contracts=0; the catch-all in place() would mask a
	ZeroDivisionError from ``fill_pct = filled / size`` as
	``unexpected_exception:ZeroDivisionError``, hiding the real upstream
	defect. The zero-guard returns ``rejected`` with a precise reason so
	on-call sees the actual sizing failure.

	Setup: req.size_contracts = 0 but Kalshi reports filled_count > 0
	(defensive impossible-from-real-Kalshi shape).
	"""
	client = FakeKalshiClient()
	client.return_value = _make_order(
		filled_count=5,
		count=10,
		limit_price=5,
		fills=[{"price": 5, "size": 5}],
	)
	executor = LiveExecutor(client)  # type: ignore[arg-type]
	req = _make_request(size=0, limit=5)  # size_contracts=0 — the defect

	result = await executor.place(req)

	assert result.status == "rejected"
	assert result.rejection_reason is not None
	assert result.rejection_reason.startswith("invalid_intended_size:")


# ---------------------------------------------------------------------------
# #R2-cleanup — _translate_order fill_pct overfill clamp (A-F2, known #3)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_translate_order_clamps_overfill_fill_pct_to_one() -> None:
	"""Failure mode prevented: Kalshi reports filled_count > size_contracts
	(IOC wire-shape drift). Raw ``filled / size`` produces fill_pct > 1.0,
	silently corrupting F's slippage chart and analytics that read this as
	a probability. The clamp preserves the raw ``filled_size`` (truth) but
	bounds the ratio. A WARN log fires so the data-quality drift is visible.
	"""
	client = FakeKalshiClient()
	client.return_value = _make_order(
		filled_count=12,  # overfill: more than requested
		limit_price=5,
		fills=[{"price": 5, "size": 12}],
	)
	executor = LiveExecutor(client)  # type: ignore[arg-type]
	req = _make_request(size=10, limit=5)

	result = await executor.place(req)

	assert result.status == "filled"
	assert result.filled_size == 12  # truth preserved
	assert result.fill_pct == 1.0   # ratio clamped


# ---------------------------------------------------------------------------
# #R2-cleanup — _translate_order sell-side slippage sign convention (known #2)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_translate_order_sell_slippage_positive_when_received_less() -> None:
	"""Lock the unified sign convention: positive slippage = worse than
	limit regardless of buy/sell. Previously ``slippage = blended - limit``
	gave positive=bad for buys but negative=bad for sells; F's chart had
	to know the action to interpret the sign. Now both sides agree.

	Setup: SELL action, limit=50¢, blended=48¢ → received 2¢ less than
	limit → slippage = +2 (bad, regardless of side).
	"""
	client = FakeKalshiClient()
	client.return_value = _make_order(
		filled_count=10,
		limit_price=50,
		fills=[{"price": 48, "size": 10}],
	)
	executor = LiveExecutor(client)  # type: ignore[arg-type]
	req = _make_request(size=10, limit=50, action="sell")

	result = await executor.place(req)

	assert result.status == "filled"
	assert result.blended_entry_cents == 48
	assert result.slippage_cents == 2, (
		"sell-side slippage should be limit - blended (positive when we "
		"received less than asked)"
	)


@pytest.mark.asyncio
async def test_translate_order_buy_slippage_positive_when_paid_more() -> None:
	"""Mirror of the sell test: BUY with blended > limit produces positive
	slippage. Locks the convention symmetrically — the buy path's sign
	already had this behaviour pre-fix; this test pins it so a future
	refactor doesn't break the buy semantics while fixing the sell side.
	"""
	client = FakeKalshiClient()
	client.return_value = _make_order(
		filled_count=10,
		limit_price=50,
		fills=[{"price": 52, "size": 10}],
	)
	executor = LiveExecutor(client)  # type: ignore[arg-type]
	req = _make_request(size=10, limit=50, action="buy")

	result = await executor.place(req)

	assert result.status == "filled"
	assert result.blended_entry_cents == 52
	assert result.slippage_cents == 2
