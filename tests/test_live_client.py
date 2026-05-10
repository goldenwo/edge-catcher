"""Tests for edge_catcher.live.client — KalshiOrderClient + dataclasses."""
from __future__ import annotations

import json

import httpx
import pytest

from edge_catcher.live.client import (
	Balance,  # noqa: F401 — used by Tasks 7-9 tests
	CancelResult,  # noqa: F401 — used by Tasks 7-9 tests
	KalshiOrderClient,
	Order,  # noqa: F401 — used by Tasks 7-9 tests
	OrderRequest,
	Position,  # noqa: F401 — used by Tasks 7-9 tests
)
from edge_catcher.live.config import LiveConfig
from edge_catcher.live.audit import AuditLogger
from edge_catcher.live.errors import CapExceededError, KalshiAPIError, OrderAlreadyFinal, OrderRejected


@pytest.fixture
def cfg(tmp_path):
	return LiveConfig(audit_log_path=tmp_path / "audit.jsonl")


@pytest.fixture
def audit(tmp_path):
	return AuditLogger(tmp_path / "audit.jsonl")


def test_order_request_exposure_dollars():
	req = OrderRequest(
		ticker="X", action="buy", side="yes", count=10, limit_price_cents=5,
	)
	assert req.exposure_dollars == 0.50


def test_order_request_default_tif_gtc():
	req = OrderRequest(ticker="X", action="buy", side="yes", count=1, limit_price_cents=1)
	assert req.time_in_force == "gtc"


def test_order_request_accepts_uuid4_client_order_id():
	import uuid
	req = OrderRequest(
		ticker="X", action="buy", side="yes", count=1, limit_price_cents=1,
		client_order_id=str(uuid.uuid4()),
	)
	assert req.client_order_id is not None


def test_order_request_accepts_dispatch_style_client_order_id():
	# Mirrors edge_catcher.engine.dispatch._make_client_order_id format:
	# `{strategy}-{ticker}-{ms_ts}`.
	req = OrderRequest(
		ticker="KXSOL15M-26MAY09H06", action="buy", side="yes",
		count=1, limit_price_cents=1,
		client_order_id="debut-fade-KXSOL15M-1715195456789",
	)
	assert req.client_order_id == "debut-fade-KXSOL15M-1715195456789"


def test_order_request_accepts_underscores_and_hyphens():
	req = OrderRequest(
		ticker="X", action="buy", side="yes", count=1, limit_price_cents=1,
		client_order_id="bt_v2-abc_123-XYZ",
	)
	assert req.client_order_id == "bt_v2-abc_123-XYZ"


@pytest.mark.parametrize("bad", [
	"",                                        # empty
	"a" * 65,                                  # too long (max 64)
	"has spaces",                              # whitespace
	"with/slash",                              # forward slash
	"with\\backslash",                         # backslash
	"with.dot",                                # dot
	"with:colon",                              # colon
	"with@at",                                 # at-sign
	"unicode-ümläut",                          # non-ASCII
	"newline\nin-id",                          # control char
	"<script>alert(1)</script>",               # injection-shaped
])
def test_order_request_rejects_invalid_client_order_id(bad):
	with pytest.raises(ValueError, match="client_order_id must match"):
		OrderRequest(
			ticker="X", action="buy", side="yes", count=1, limit_price_cents=1,
			client_order_id=bad,
		)


def test_order_request_none_client_order_id_passes():
	# None is the default — auto-generated UUID4 is assigned in place() later.
	req = OrderRequest(ticker="X", action="buy", side="yes", count=1, limit_price_cents=1)
	assert req.client_order_id is None


@pytest.mark.asyncio
async def test_client_init_and_close(cfg, audit):
	client = KalshiOrderClient(cfg, audit)
	await client.close()


@pytest.mark.asyncio
async def test_client_context_manager(cfg, audit):
	async with KalshiOrderClient(cfg, audit) as c:
		assert c is not None


# ---------------------------------------------------------------------------
# Task 7 — place() tests
# ---------------------------------------------------------------------------


def make_mock_client(cfg, audit, transport):
	"""Helper: build a KalshiOrderClient with a custom MockTransport.

	httpx.MockTransport works identically for AsyncClient — same protocol,
	same handler signature.
	"""
	c = KalshiOrderClient(cfg, audit)
	c._http = httpx.AsyncClient(
		base_url=cfg.kalshi_rest_base,
		timeout=cfg.http_timeout_seconds,
		headers={"Accept": "application/json"},
		transport=transport,
	)
	return c


@pytest.fixture
def signing_env(monkeypatch):
	"""Set KALSHI_LIVE_KEY_ID + KALSHI_LIVE_PRIVATE_KEY for tests that exercise signing.

	The live trader's client.py reads from the LIVE-suffixed env vars (so a
	leaked read-only paper-trader key cannot place orders). Tests must mirror
	that — set the LIVE vars, not the default ones.
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


@pytest.mark.asyncio
async def test_place_exceeds_absolute_max_raises_before_http(cfg, audit, signing_env):
	called = []
	def handler(request: httpx.Request) -> httpx.Response:
		called.append(request)
		return httpx.Response(201, json={})
	transport = httpx.MockTransport(handler)
	c = make_mock_client(cfg, audit, transport)
	# 1000 contracts × 50¢ = $500 > ABSOLUTE_MAX $50
	req = OrderRequest(ticker="X", action="buy", side="yes", count=1000, limit_price_cents=50)
	with pytest.raises(CapExceededError, match="ABSOLUTE_MAX"):
		await c.place(req)
	assert called == []  # no HTTP issued


@pytest.mark.asyncio
async def test_place_happy_path(cfg, audit, signing_env, tmp_path):
	"""201 response yields an Order; audit row records success."""
	cfg_with_audit = cfg.model_copy(update={"audit_log_path": tmp_path / "audit.jsonl"})
	audit_logger = AuditLogger(cfg_with_audit.audit_log_path)
	captured = []
	def handler(request: httpx.Request) -> httpx.Response:
		captured.append(request)
		return httpx.Response(201, json={"order": {
			"order_id": "ord-123",
			"ticker": "X",
			"side": "yes",
			"action": "buy",
			"count": 10,
			"yes_price": 5,
			"time_in_force": "gtc",
			"status": "resting",
			"client_order_id": "did-not-set-this-yet",
		}})
	c = make_mock_client(cfg_with_audit, audit_logger, httpx.MockTransport(handler))
	req = OrderRequest(ticker="X", action="buy", side="yes", count=10, limit_price_cents=5)
	order = await c.place(req)
	assert order.order_id == "ord-123"
	assert order.count == 10
	# Path used must include the prefix
	assert captured[0].url.path == "/trade-api/v2/portfolio/orders"
	# Sig was generated for that exact path (header present; sig verification covered separately)
	assert "KALSHI-ACCESS-SIGNATURE" in captured[0].headers
	# Audit row exists
	audit_lines = (cfg_with_audit.audit_log_path).read_text().strip().split("\n")
	assert len(audit_lines) == 1
	row = json.loads(audit_lines[0])
	assert row["op"] == "place"
	assert row["outcome"] == "success"


@pytest.mark.asyncio
async def test_signed_path_matches_sent_path(cfg, audit, signing_env, tmp_path):
	"""Regression: httpx must NOT redirect /portfolio/orders to skip the prefix."""
	cfg_with_audit = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})
	audit_logger = AuditLogger(cfg_with_audit.audit_log_path)
	captured_url = []
	def handler(request: httpx.Request) -> httpx.Response:
		captured_url.append(str(request.url))
		return httpx.Response(201, json={"order": {"order_id": "x", "status": "resting"}})
	c = make_mock_client(cfg_with_audit, audit_logger, httpx.MockTransport(handler))
	req = OrderRequest(ticker="X", action="buy", side="yes", count=1, limit_price_cents=1)
	await c.place(req)
	# Full URL must include /trade-api/v2/portfolio/orders
	assert "/trade-api/v2/portfolio/orders" in captured_url[0]


@pytest.mark.asyncio
async def test_place_does_not_send_buy_max_cost(cfg, audit, signing_env, tmp_path):
	"""buy_max_cost is NOT sent — Kalshi's enforcement of it is opaque enough
	that even principal+estimated_fee gets rejected. Cap safety is provided by
	ABSOLUTE_MAX_ORDER_DOLLARS (library) + cli_max_order_dollars (CLI).
	Discovered during integration testing — see PR #24 history.
	"""
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})
	captured_body = []
	def handler(request: httpx.Request) -> httpx.Response:
		captured_body.append(json.loads(request.content))
		return httpx.Response(201, json={"order": {"order_id": "x", "status": "resting"}})
	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	req = OrderRequest(ticker="X", action="buy", side="yes", count=10, limit_price_cents=5)
	await c.place(req)
	assert "buy_max_cost" not in captured_body[0]
	assert captured_body[0]["yes_price"] == 5


@pytest.mark.asyncio
async def test_place_translates_tif_short_to_kalshi_verbose(cfg, audit, signing_env, tmp_path):
	"""Wire format of time_in_force must be Kalshi's verbose underscored values
	(`good_till_canceled` / `immediate_or_cancel` / `fill_or_kill`), NOT our
	short Pythonic CLI form (`gtc`/`ioc`/`fok`). Regression guard: integration
	test caught Kalshi rejecting `ioc` with `Field validation 'oneof'` error.
	"""
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})
	captured_bodies: list[dict] = []
	def handler(request: httpx.Request) -> httpx.Response:
		captured_bodies.append(json.loads(request.content))
		return httpx.Response(201, json={"order": {"order_id": "x", "status": "resting"}})
	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))

	for short, verbose in (
		("gtc", "good_till_canceled"),
		("ioc", "immediate_or_cancel"),
		("fok", "fill_or_kill"),
	):
		captured_bodies.clear()
		req = OrderRequest(
			ticker="X", action="buy", side="yes", count=1,
			limit_price_cents=1, time_in_force=short,
		)
		await c.place(req)
		assert captured_bodies[0]["time_in_force"] == verbose, (
			f"short {short!r} should translate to verbose {verbose!r} on the wire, "
			f"got {captured_bodies[0]['time_in_force']!r}"
		)


@pytest.mark.asyncio
async def test_place_sell_also_omits_buy_max_cost(cfg, audit, signing_env, tmp_path):
	"""Sanity check: sells continue to omit buy_max_cost (we don't send it for
	any action — see test_place_does_not_send_buy_max_cost)."""
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})
	captured_body = []
	def handler(request: httpx.Request) -> httpx.Response:
		captured_body.append(json.loads(request.content))
		return httpx.Response(201, json={"order": {"order_id": "x", "status": "resting"}})
	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	req = OrderRequest(ticker="X", action="sell", side="yes", count=10, limit_price_cents=5)
	await c.place(req)
	assert "buy_max_cost" not in captured_body[0]


@pytest.mark.asyncio
async def test_place_4xx_raises_order_rejected(cfg, audit, signing_env, tmp_path):
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})
	def handler(request: httpx.Request) -> httpx.Response:
		return httpx.Response(400, json={"error": {"code": "bad", "message": "no"}})
	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	req = OrderRequest(ticker="X", action="buy", side="yes", count=1, limit_price_cents=1)
	with pytest.raises(OrderRejected) as exc:
		await c.place(req)
	assert exc.value.status == 400


@pytest.mark.asyncio
async def test_place_429_then_201_succeeds_with_retry(cfg, audit, signing_env, tmp_path, monkeypatch):
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl", "max_retries": 3})
	# Skip the real backoff sleep — patch asyncio.sleep so retry timing is
	# instant. The async path uses `await asyncio.sleep(...)`, not time.sleep.
	async def _no_sleep(s):  # noqa: ARG001
		return None
	monkeypatch.setattr("edge_catcher.live.client.asyncio.sleep", _no_sleep)
	calls = [0]
	def handler(request: httpx.Request) -> httpx.Response:
		calls[0] += 1
		if calls[0] == 1:
			return httpx.Response(429, json={"error": {"message": "slow down"}})
		return httpx.Response(201, json={"order": {"order_id": "x", "status": "resting"}})
	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	req = OrderRequest(ticker="X", action="buy", side="yes", count=1, limit_price_cents=1)
	order = await c.place(req)
	assert order.order_id == "x"
	# Audit row records retries=1
	row = json.loads((cfg2.audit_log_path).read_text().strip().split("\n")[-1])
	assert row["retries"] == 1
	assert row["outcome"] == "success"


# ---------------------------------------------------------------------------
# Task 8 — cancel() tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancel_happy_path(cfg, audit, signing_env, tmp_path):
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})
	captured = []
	def handler(request: httpx.Request) -> httpx.Response:
		captured.append(request)
		return httpx.Response(200, json={"order": {"order_id": "ord-1", "status": "canceled"}})
	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	result = await c.cancel("ord-1")
	assert result.order_id == "ord-1"
	assert result.status == "canceled"
	# DELETE method used
	assert captured[0].method == "DELETE"
	assert captured[0].url.path == "/trade-api/v2/portfolio/orders/ord-1"


@pytest.mark.asyncio
async def test_cancel_404_raises_order_already_final(cfg, audit, signing_env, tmp_path):
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})
	def handler(request: httpx.Request) -> httpx.Response:
		return httpx.Response(404, json={"error": {"message": "order_not_found"}})
	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	with pytest.raises(OrderAlreadyFinal) as exc:
		await c.cancel("missing-id")
	assert exc.value.status == 404


@pytest.mark.asyncio
async def test_cancel_409_raises_order_already_final(cfg, audit, signing_env, tmp_path):
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})
	def handler(request: httpx.Request) -> httpx.Response:
		return httpx.Response(409, json={"error": {"message": "already filled"}})
	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	with pytest.raises(OrderAlreadyFinal):
		await c.cancel("filled-id")


@pytest.mark.asyncio
async def test_cancel_other_4xx_raises_kalshi_api_error(cfg, audit, signing_env, tmp_path):
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})
	def handler(request: httpx.Request) -> httpx.Response:
		return httpx.Response(401, json={"error": {"message": "unauthorized"}})
	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	with pytest.raises(KalshiAPIError) as exc:
		await c.cancel("any-id")
	# Specifically NOT OrderAlreadyFinal
	assert not isinstance(exc.value, OrderAlreadyFinal)


# ---------------------------------------------------------------------------
# Task 9 — status() / balance() / positions() tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_status_happy_path(cfg, audit, signing_env, tmp_path):
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})
	def handler(request: httpx.Request) -> httpx.Response:
		assert request.method == "GET"
		assert request.url.path == "/trade-api/v2/portfolio/orders/ord-1"
		return httpx.Response(200, json={"order": {
			"order_id": "ord-1", "ticker": "X", "side": "yes", "action": "buy",
			"count": 10, "yes_price": 5, "time_in_force": "gtc",
			"status": "resting", "filled_count": 3,
		}})
	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	o = await c.status("ord-1")
	assert o.order_id == "ord-1"
	assert o.filled_count == 3


@pytest.mark.asyncio
async def test_balance_happy_path(cfg, audit, signing_env, tmp_path):
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})
	def handler(request: httpx.Request) -> httpx.Response:
		return httpx.Response(200, json={"balance": 19500})
	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	bal = await c.balance()
	assert bal.balance_cents == 19500


@pytest.mark.asyncio
async def test_positions_empty(cfg, audit, signing_env, tmp_path):
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})
	def handler(request: httpx.Request) -> httpx.Response:
		return httpx.Response(200, json={"market_positions": []})
	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	assert await c.positions() == []


@pytest.mark.asyncio
async def test_positions_non_empty(cfg, audit, signing_env, tmp_path):
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})
	def handler(request: httpx.Request) -> httpx.Response:
		return httpx.Response(200, json={"market_positions": [
			{"ticker": "X", "position": 10, "average_position_cost": 5},
			{"ticker": "Y", "position": -3, "average_position_cost": 95},
		]})
	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	positions = await c.positions()
	assert len(positions) == 2
	assert positions[0].ticker == "X"
	assert positions[0].count == 10
	assert positions[0].side == "yes"
	# Negative position interpreted as no-side
	assert positions[1].side == "no"
	assert positions[1].count == 3
