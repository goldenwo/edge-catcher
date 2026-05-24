"""MockKalshiServer — httpx.MockTransport-based fixture for live-executor tests.

Provides an in-process simulator of Kalshi's order-placement REST endpoint so
:class:`edge_catcher.engine.executors.live.LiveExecutor` integration tests can
exercise end-to-end paths (sign → request → response → translate) without
touching the network.

Designed for reuse by sub-project E's CR-5 (live vs replay parity tests).
Response shapes are parameterizable rather than hardcoded so E can drive its
own scenarios through the same fixture without copy-paste.

Conventions matched:
* Uses :class:`httpx.MockTransport` (NOT pytest-httpx), mirroring
  ``tests/test_live_client.py``'s pattern. pytest-httpx is not in the
  project's dependency set.
* Seeds throwaway signing env vars via the ``signing_env`` pattern from
  ``tests/test_live_client.py:142`` so :func:`make_auth_headers` succeeds
  without operator-supplied secrets.
* Monkeypatches A's async backoff (``asyncio.sleep`` in
  ``edge_catcher.live.client``) to a no-op so 503-exhaustion tests run in
  well under 100ms instead of the production ~62s.

H1 extension (2026-05-23):
* ``MockKalshiServer`` gains an optional ``response_delay_seconds`` field.
  When set, the transport handler is an async handler that ``await``s
  ``asyncio.sleep(response_delay_seconds)`` before returning the response.
  ``httpx.MockTransport`` accepts both sync and async handlers — existing
  users are unaffected (the field defaults to ``None`` / sync path).
* Use ``queue_slow_response`` to both set the delay and queue the response
  body in one call, keeping existing callers free of the new field.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any, Callable

import httpx
import pytest

from edge_catcher.live.audit import AuditLogger
from edge_catcher.live.client import KalshiOrderClient
from edge_catcher.live.config import LiveConfig


# ---------------------------------------------------------------------------
# Response builders — parameterizable shapes
# ---------------------------------------------------------------------------


def kalshi_201_filled(
	*,
	order_id: str = "ord-mock-filled",
	ticker: str = "KXSOL15M-26MAY09H06",
	side: str = "yes",
	action: str = "buy",
	count: int = 10,
	yes_price: int = 5,
	filled_count: int | None = None,
	fills: list[dict[str, int]] | None = None,
	client_order_id: str | None = None,
) -> dict[str, Any]:
	"""Build Kalshi's REAL 201 response body for a filled order.

	Mirrors the production wire shape captured from the live audit log
	(``{"order": {...}}``): counts are fixed-point STRINGS (``fill_count_fp`` /
	``initial_count_fp``), the limit price is a dollar STRING
	(``{side}_price_dollars``), and the blended cost is the AGGREGATE
	``taker_fill_cost_dollars`` — there is NO per-fill array.

	``yes_price`` (cents) is the order's own-side limit; ``fills`` is the
	caller's book-walk (cents) folded into the aggregate the real API returns:
	``taker_fill_cost_dollars = Σ price·size`` and ``fill_count = Σ size``. This
	preserves parity — ``_parse_order`` recovers blended =
	``round(cost·100 / fill_count)`` = ``blended_price_cents(fills)``.
	"""
	if filled_count is None:
		filled_count = count
	if fills is None:
		fills = [{"price": yes_price, "size": filled_count}]
	total_cost_cents = sum(f["price"] * f["size"] for f in fills)
	# Kalshi reports both sides' prices (complementary); _parse_order reads the
	# order's own side. yes_price is the own-side limit in cents.
	own = f"{yes_price / 100:.4f}"
	comp = f"{(100 - yes_price) / 100:.4f}"
	order: dict[str, Any] = {
		"order_id": order_id,
		"ticker": ticker,
		"side": side,
		"action": action,
		"initial_count_fp": f"{count}.00",
		"fill_count_fp": f"{filled_count}.00",
		"remaining_count_fp": f"{count - filled_count}.00",
		"yes_price_dollars": own if side == "yes" else comp,
		"no_price_dollars": own if side == "no" else comp,
		"taker_fill_cost_dollars": f"{total_cost_cents / 100:.6f}",
		"taker_fees_dollars": "0.000000",
		"time_in_force": "immediate_or_cancel",
		"status": "executed" if filled_count == count else "resting",
	}
	if client_order_id is not None:
		order["client_order_id"] = client_order_id
	return {"order": order}


def kalshi_201_partial(
	*,
	order_id: str = "ord-mock-partial",
	ticker: str = "KXSOL15M-26MAY09H06",
	side: str = "yes",
	count: int = 10,
	filled_count: int = 7,
	fills: list[dict[str, int]] | None = None,
	limit_price_cents: int = 5,
) -> dict[str, Any]:
	"""Build Kalshi's 201 response body for a partial-IOC fill.

	Default fills emulate a two-level walk: 5c×5 + 6c×2 = 7 contracts at
	weighted-average 5c (matches D's test #8 shape).
	"""
	if fills is None:
		fills = [
			{"price": limit_price_cents, "size": 5},
			{"price": limit_price_cents + 1, "size": 2},
		]
	return kalshi_201_filled(
		order_id=order_id,
		ticker=ticker,
		side=side,
		count=count,
		yes_price=limit_price_cents,
		filled_count=filled_count,
		fills=fills,
	)


def kalshi_503_unavailable() -> tuple[int, dict[str, Any]]:
	"""Build Kalshi's 503 service-unavailable response shape.

	Returned as a (status, body) tuple so the handler in MockKalshiServer can
	thread it into :class:`httpx.Response`. A's retry layer treats 503 as a
	retriable upstream failure (max_retries × exponential backoff); the test
	monkeypatches sleep to zero so exhaustion completes in <100ms.
	"""
	return 503, {"error": {"code": "service_unavailable", "message": "kalshi down"}}


def kalshi_400_rejected(
	*,
	code: str = "invalid_price",
	message: str = "price out of band",
) -> tuple[int, dict[str, Any]]:
	"""Build Kalshi's 400 validation-rejection response shape.

	A's client maps the place-op 4xx into :class:`OrderRejected`; LiveExecutor
	translates that into ``OrderResult(rejected, "kalshi_4xx:400")``.
	"""
	return 400, {"error": {"code": code, "message": message}}


# ---------------------------------------------------------------------------
# MockKalshiServer — handler-builder + lifecycle wrapper
# ---------------------------------------------------------------------------


@dataclass
class MockKalshiServer:
	"""In-process Kalshi REST simulator built on :class:`httpx.MockTransport`.

	Test usage:

	.. code-block:: python

	    server = MockKalshiServer()
	    server.queue_response(kalshi_201_filled(order_id="ord-1"))
	    client = server.make_client(cfg, audit)
	    order = await client.place(req)
	    assert server.requests[-1].url.path.endswith("/portfolio/orders")

	Response queueing:
	* If only one response is queued, it is returned for every request.
	* If multiple responses are queued, each request consumes the next one
	  (FIFO). Once exhausted, the last response is reused — convenient for
	  503-storm tests that want every retry to see the same 503.

	The class is intentionally a thin orchestrator: each test composes the
	specific Kalshi response shape it needs from the ``kalshi_*`` builder
	functions above. This keeps the fixture parameterizable for E's CR-5.
	"""

	# Queued (status, body) pairs. Each request pops the head; if exhausted,
	# the last one is reused (sticky-tail behavior — matches the 503-storm
	# semantics where every retry sees the same outage).
	_responses: list[tuple[int, dict[str, Any]]] = field(default_factory=list)
	# All received request objects, in arrival order.
	requests: list[httpx.Request] = field(default_factory=list)
	# H1 extension: optional delay injected before each response.  When set,
	# the transport switches to an async handler (httpx.MockTransport supports
	# both sync and async).  Existing callers leave this None → sync handler,
	# zero behavior change.
	response_delay_seconds: float | None = None
	# CR-5 opt-in: when True, the handler answers each request by matching the
	# request body's client_order_id to a queued response's coid (out-of-order
	# safe) and synthesises a fully-filled echo for any unqueued coid — the
	# fresh exit-order coid generated inside _handle_exit, which the harness
	# never queues.  When False (default) the server keeps the strict
	# FIFO/sticky-tail behaviour the error-storm tests depend on.  Contract:
	# tests/test_mock_kalshi_server.py.
	match_by_client_order_id: bool = False

	def queue_response(
		self,
		body: dict[str, Any],
		*,
		status: int = 201,
	) -> None:
		"""Queue a single response body with status code (default 201).

		The body shape must match Kalshi's actual wire response — use the
		``kalshi_*`` builders above for canonical shapes.
		"""
		self._responses.append((status, body))

	def queue_status(self, status: int, body: dict[str, Any] | None = None) -> None:
		"""Queue a (status, body) pair — convenience for explicit error codes.

		Used by 503 / 400 / 5xx tests where the status is the assertion target.
		"""
		self._responses.append((status, body or {}))

	def queue_503_storm(self) -> None:
		"""Queue a single 503 response that the sticky-tail behavior will
		replay for every retry attempt. Used by the NetworkError-after-retries
		test (Integration #3)."""
		status, body = kalshi_503_unavailable()
		self.queue_status(status, body)

	def queue_slow_response(
		self,
		body: dict[str, Any],
		*,
		status: int = 201,
		delay_seconds: float,
	) -> None:
		"""Queue a response body AND arm the server-side delay (H1 timeout test).

		Sets ``self.response_delay_seconds`` so the async transport handler
		sleeps ``delay_seconds`` before returning.  The transport switches to
		async mode automatically when this field is non-None; existing callers
		that never call this method are unaffected (sync handler, no sleep).

		Use this instead of ``queue_response`` when you need the server to
		respond slowly enough to trigger ``_ENTRY_PLACEMENT_TIMEOUT_SECONDS``.
		"""
		self.response_delay_seconds = delay_seconds
		self._responses.append((status, body))

	def transport(self) -> httpx.MockTransport:
		"""Build an :class:`httpx.MockTransport` that serves the queued responses.

		The handler captures every request into ``self.requests`` so tests
		can assert headers (signed Authorization), path, body, etc.

		Response selection depends on ``match_by_client_order_id``:
		* False (default) — strict FIFO/sticky-tail via ``_consume_response``.
		* True (CR-5) — coid-matched via ``_coid_matched_response`` (queued
		  response whose coid matches the request, else a synthesised echo).

		When ``response_delay_seconds`` is set (H1 timeout test), the handler
		is async and ``await``s ``asyncio.sleep(delay)`` before returning.
		``httpx.MockTransport`` accepts both sync and async callables — existing
		callers that leave the field ``None`` get the same sync handler as before.
		"""
		delay = self.response_delay_seconds

		def _respond(request: httpx.Request) -> httpx.Response:
			if self.match_by_client_order_id:
				return self._coid_matched_response(request)
			return self._consume_response()

		if delay is not None:
			async def _async_handler(request: httpx.Request) -> httpx.Response:
				self.requests.append(request)
				await asyncio.sleep(delay)
				return _respond(request)

			return httpx.MockTransport(_async_handler)

		def _handler(request: httpx.Request) -> httpx.Response:
			self.requests.append(request)
			return _respond(request)

		return httpx.MockTransport(_handler)

	def _consume_response(self) -> httpx.Response:
		"""FIFO/sticky-tail consumption (default mode).

		Pops the head while more than one response remains; reuses the tail
		when only one is left (sticky-tail — the 503-storm semantics); returns
		500 when the queue is empty.
		"""
		if not self._responses:
			return httpx.Response(
				500,
				json={"error": {"message": "MockKalshiServer: no response queued"}},
			)
		if len(self._responses) > 1:
			status, body = self._responses.pop(0)
		else:
			status, body = self._responses[0]
		return httpx.Response(status, json=body)

	def _coid_matched_response(self, request: httpx.Request) -> httpx.Response:
		"""CR-5 mode: answer by matching the request's client_order_id.

		Returns (and consumes) the queued response whose order ``client_order_id``
		equals the request's; a matched response is used once. An unmatched coid
		— the fresh exit-order coid ``_handle_exit`` generates, which the harness
		never queues — yields a 201 echo of the request (``filled_count`` ==
		requested count, a valid ``fills`` array) so ``LiveExecutor`` translates
		it as a clean fill and the replay completes with no 500/retry stall.
		"""
		coid = self._request_coid(request)
		if coid is not None:
			for i, (status, body) in enumerate(self._responses):
				if self._response_coid(body) == coid:
					self._responses.pop(i)
					return httpx.Response(status, json=body)
		return httpx.Response(201, json=self._synthesise_echo(request))

	@staticmethod
	def _request_coid(request: httpx.Request) -> str | None:
		"""Extract ``client_order_id`` from a place request body (None if absent
		or the body isn't JSON — e.g. a cancel/status GET with no content)."""
		try:
			payload = json.loads(request.content)
		except (ValueError, TypeError):
			return None
		coid = payload.get("client_order_id") if isinstance(payload, dict) else None
		return coid if isinstance(coid, str) else None

	@staticmethod
	def _response_coid(body: dict[str, Any]) -> str | None:
		"""Extract the order ``client_order_id`` from a queued response body."""
		order = body.get("order") if isinstance(body, dict) else None
		coid = order.get("client_order_id") if isinstance(order, dict) else None
		return coid if isinstance(coid, str) else None

	@staticmethod
	def _synthesise_echo(request: httpx.Request) -> dict[str, Any]:
		"""Build a fully-filled 201 body echoing the placed order.

		Used for an unqueued (exit) coid so the place still translates cleanly.
		The fill price is the request's limit (``yes_price`` / ``no_price``); the
		exit result is discarded by ``_handle_exit``, so the exact price is
		immaterial — what matters is a positive fill at a real cost basis.
		Delegates to :func:`kalshi_201_filled` for the real wire shape.
		"""
		try:
			payload = json.loads(request.content)
		except (ValueError, TypeError):
			payload = {}
		if not isinstance(payload, dict):
			payload = {}
		count = int(payload.get("count", 0) or 0)
		side = payload.get("side", "yes")
		price = payload.get("yes_price") if side == "yes" else payload.get("no_price")
		price = int(price or 0)
		coid = payload.get("client_order_id")
		return kalshi_201_filled(
			order_id=f"echo-{coid}" if coid else "echo-order",
			ticker=payload.get("ticker", ""),
			side=side,
			action=payload.get("action", "buy"),
			count=count,
			filled_count=count,
			yes_price=price,
			fills=[{"price": price, "size": count}] if count > 0 else [],
			client_order_id=coid,
		)

	def make_client(
		self,
		cfg: LiveConfig,
		audit: AuditLogger,
	) -> KalshiOrderClient:
		"""Construct a :class:`KalshiOrderClient` wired to this server's transport.

		Mirrors ``tests/test_live_client.py:make_mock_client`` pattern —
		swap the client's ``_http`` for an AsyncClient with our MockTransport.
		"""
		client = KalshiOrderClient(cfg, audit)
		client._http = httpx.AsyncClient(
			base_url=cfg.kalshi_rest_base,
			timeout=cfg.http_timeout_seconds,
			headers={"Accept": "application/json"},
			transport=self.transport(),
		)
		return client


# ---------------------------------------------------------------------------
# pytest fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_kalshi_server() -> MockKalshiServer:
	"""Fresh MockKalshiServer per test — no cross-test state."""
	return MockKalshiServer()


@pytest.fixture
def signing_env(monkeypatch: pytest.MonkeyPatch) -> None:
	"""Seed throwaway live-trader signing env vars.

	Generates a fresh RSA-2048 keypair per test (cheap on modern hardware,
	~50ms) and writes the PEM + key id to the env vars A's
	``make_auth_headers`` reads. No real Kalshi credentials are required for
	any test that uses this fixture.

	Mirrors ``tests/test_live_client.py:signing_env``.
	"""
	from cryptography.hazmat.primitives import serialization
	from cryptography.hazmat.primitives.asymmetric import rsa

	key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
	pem = key.private_bytes(
		encoding=serialization.Encoding.PEM,
		format=serialization.PrivateFormat.PKCS8,
		encryption_algorithm=serialization.NoEncryption(),
	)
	monkeypatch.setenv("KALSHI_LIVE_KEY_ID", "test-live-key")
	monkeypatch.setenv("KALSHI_LIVE_PRIVATE_KEY", pem.decode())


@pytest.fixture
def zero_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
	"""Monkeypatch A's retry backoff (``asyncio.sleep``) to a no-op.

	Required for the 503-storm test (Integration #3): without it, A's
	exponential backoff (``min(60, 2**retries + 0.1*retries)``) would burn
	~62s waiting between retries before raising :class:`NetworkError`.
	With sleep stubbed, the test completes in <100ms.

	Patches the module-level alias that A's client imports — same target as
	``tests/test_live_client.py:test_place_429_then_201_succeeds_with_retry``.
	"""

	async def _no_sleep(seconds: float) -> None:
		return None

	monkeypatch.setattr("edge_catcher.live.client.asyncio.sleep", _no_sleep)


@pytest.fixture
def live_cfg(tmp_path: Any) -> LiveConfig:
	"""LiveConfig with a tmp_path-scoped audit log.

	max_retries is left at the production default (5) so the 503-storm test
	exercises the realistic retry budget. Tests that need a different value
	(e.g. ``max_retries=0`` to exhaust immediately) can call
	``cfg.model_copy(update={...})``.
	"""
	return LiveConfig(audit_log_path=tmp_path / "audit.jsonl")


@pytest.fixture
def live_audit(live_cfg: LiveConfig) -> AuditLogger:
	"""AuditLogger pointing at the tmp_path audit file."""
	return AuditLogger(live_cfg.audit_log_path)


# ---------------------------------------------------------------------------
# Convenience constructor for E's CR-5 reuse: build a server pre-loaded with
# any response shape. Keeps the surface small so E's spec can wire its own
# scenarios without duplicating handler boilerplate.
# ---------------------------------------------------------------------------


def make_server_with(
	response_builder: Callable[..., dict[str, Any]] | None = None,
	*,
	status: int = 201,
	**builder_kwargs: Any,
) -> MockKalshiServer:
	"""Construct a server pre-loaded with a single response from ``response_builder``.

	Convenience for E's CR-5 parity test to compose one-liners like
	``make_server_with(kalshi_201_filled, order_id="...")``. Keeps the
	fixture's surface minimal while still allowing parameterization.
	"""
	server = MockKalshiServer()
	if response_builder is None:
		return server
	body = response_builder(**builder_kwargs)
	server.queue_response(body, status=status)
	return server
