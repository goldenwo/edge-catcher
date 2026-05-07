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


def test_client_init_and_close(cfg, audit):
	client = KalshiOrderClient(cfg, audit)
	client.close()


def test_client_context_manager(cfg, audit):
	with KalshiOrderClient(cfg, audit) as c:
		assert c is not None


# ---------------------------------------------------------------------------
# Task 7 — place() tests
# ---------------------------------------------------------------------------


def make_mock_client(cfg, audit, transport):
	"""Helper: build a KalshiOrderClient with a custom MockTransport."""
	c = KalshiOrderClient(cfg, audit)
	c._http = httpx.Client(
		base_url=cfg.kalshi_rest_base,
		timeout=cfg.http_timeout_seconds,
		headers={"Accept": "application/json"},
		transport=transport,
	)
	return c


@pytest.fixture
def signing_env(monkeypatch):
	"""Set KALSHI_KEY_ID + KALSHI_PRIVATE_KEY for tests that exercise signing."""
	from cryptography.hazmat.primitives import serialization
	from cryptography.hazmat.primitives.asymmetric import rsa
	key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
	pem = key.private_bytes(
		encoding=serialization.Encoding.PEM,
		format=serialization.PrivateFormat.PKCS8,
		encryption_algorithm=serialization.NoEncryption(),
	)
	monkeypatch.setenv("KALSHI_KEY_ID", "test-key")
	monkeypatch.setenv("KALSHI_PRIVATE_KEY", pem.decode())


def test_place_exceeds_absolute_max_raises_before_http(cfg, audit, signing_env):
	called = []
	def handler(request: httpx.Request) -> httpx.Response:
		called.append(request)
		return httpx.Response(201, json={})
	transport = httpx.MockTransport(handler)
	c = make_mock_client(cfg, audit, transport)
	# 1000 contracts × 50¢ = $500 > ABSOLUTE_MAX $50
	req = OrderRequest(ticker="X", action="buy", side="yes", count=1000, limit_price_cents=50)
	with pytest.raises(CapExceededError, match="ABSOLUTE_MAX"):
		c.place(req)
	assert called == []  # no HTTP issued


def test_place_happy_path(cfg, audit, signing_env, tmp_path):
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
	order = c.place(req)
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


def test_signed_path_matches_sent_path(cfg, audit, signing_env, tmp_path):
	"""Regression: httpx must NOT redirect /portfolio/orders to skip the prefix."""
	cfg_with_audit = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})
	audit_logger = AuditLogger(cfg_with_audit.audit_log_path)
	captured_url = []
	def handler(request: httpx.Request) -> httpx.Response:
		captured_url.append(str(request.url))
		return httpx.Response(201, json={"order": {"order_id": "x", "status": "resting"}})
	c = make_mock_client(cfg_with_audit, audit_logger, httpx.MockTransport(handler))
	req = OrderRequest(ticker="X", action="buy", side="yes", count=1, limit_price_cents=1)
	c.place(req)
	# Full URL must include /trade-api/v2/portfolio/orders
	assert "/trade-api/v2/portfolio/orders" in captured_url[0]


def test_place_buy_includes_buy_max_cost(cfg, audit, signing_env, tmp_path):
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})
	captured_body = []
	def handler(request: httpx.Request) -> httpx.Response:
		captured_body.append(json.loads(request.content))
		return httpx.Response(201, json={"order": {"order_id": "x", "status": "resting"}})
	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	req = OrderRequest(ticker="X", action="buy", side="yes", count=10, limit_price_cents=5)
	c.place(req)
	# 10 × 5¢ = 50¢ exposure → buy_max_cost = 50 cents
	assert captured_body[0]["buy_max_cost"] == 50
	assert captured_body[0]["yes_price"] == 5


def test_place_sell_omits_buy_max_cost(cfg, audit, signing_env, tmp_path):
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})
	captured_body = []
	def handler(request: httpx.Request) -> httpx.Response:
		captured_body.append(json.loads(request.content))
		return httpx.Response(201, json={"order": {"order_id": "x", "status": "resting"}})
	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	req = OrderRequest(ticker="X", action="sell", side="yes", count=10, limit_price_cents=5)
	c.place(req)
	# Sells do NOT set buy_max_cost (Kalshi rejects unknown-context fields)
	assert "buy_max_cost" not in captured_body[0]


def test_place_4xx_raises_order_rejected(cfg, audit, signing_env, tmp_path):
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})
	def handler(request: httpx.Request) -> httpx.Response:
		return httpx.Response(400, json={"error": {"code": "bad", "message": "no"}})
	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	req = OrderRequest(ticker="X", action="buy", side="yes", count=1, limit_price_cents=1)
	with pytest.raises(OrderRejected) as exc:
		c.place(req)
	assert exc.value.status == 400


def test_place_429_then_201_succeeds_with_retry(cfg, audit, signing_env, tmp_path, monkeypatch):
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl", "max_retries": 3})
	# Skip the real backoff sleep
	monkeypatch.setattr("edge_catcher.live.client.time.sleep", lambda s: None)
	calls = [0]
	def handler(request: httpx.Request) -> httpx.Response:
		calls[0] += 1
		if calls[0] == 1:
			return httpx.Response(429, json={"error": {"message": "slow down"}})
		return httpx.Response(201, json={"order": {"order_id": "x", "status": "resting"}})
	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	req = OrderRequest(ticker="X", action="buy", side="yes", count=1, limit_price_cents=1)
	order = c.place(req)
	assert order.order_id == "x"
	# Audit row records retries=1
	row = json.loads((cfg2.audit_log_path).read_text().strip().split("\n")[-1])
	assert row["retries"] == 1
	assert row["outcome"] == "success"


# ---------------------------------------------------------------------------
# Task 8 — cancel() tests
# ---------------------------------------------------------------------------


def test_cancel_happy_path(cfg, audit, signing_env, tmp_path):
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})
	captured = []
	def handler(request: httpx.Request) -> httpx.Response:
		captured.append(request)
		return httpx.Response(200, json={"order": {"order_id": "ord-1", "status": "canceled"}})
	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	result = c.cancel("ord-1")
	assert result.order_id == "ord-1"
	assert result.status == "canceled"
	# DELETE method used
	assert captured[0].method == "DELETE"
	assert captured[0].url.path == "/trade-api/v2/portfolio/orders/ord-1"


def test_cancel_404_raises_order_already_final(cfg, audit, signing_env, tmp_path):
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})
	def handler(request: httpx.Request) -> httpx.Response:
		return httpx.Response(404, json={"error": {"message": "order_not_found"}})
	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	with pytest.raises(OrderAlreadyFinal) as exc:
		c.cancel("missing-id")
	assert exc.value.status == 404


def test_cancel_409_raises_order_already_final(cfg, audit, signing_env, tmp_path):
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})
	def handler(request: httpx.Request) -> httpx.Response:
		return httpx.Response(409, json={"error": {"message": "already filled"}})
	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	with pytest.raises(OrderAlreadyFinal):
		c.cancel("filled-id")


def test_cancel_other_4xx_raises_kalshi_api_error(cfg, audit, signing_env, tmp_path):
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})
	def handler(request: httpx.Request) -> httpx.Response:
		return httpx.Response(401, json={"error": {"message": "unauthorized"}})
	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	with pytest.raises(KalshiAPIError) as exc:
		c.cancel("any-id")
	# Specifically NOT OrderAlreadyFinal
	assert not isinstance(exc.value, OrderAlreadyFinal)
