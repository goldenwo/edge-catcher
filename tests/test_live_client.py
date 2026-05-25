"""Tests for edge_catcher.live.client — KalshiOrderClient + dataclasses."""
from __future__ import annotations

import asyncio
import json
import logging
import sys
import time

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
from edge_catcher.live.errors import (
	CapExceededError,
	KalshiAPIError,
	NetworkError,
	OrderAlreadyFinal,
	OrderRejected,
)
from tests.fixtures.kalshi_responses import PLACE_201_BODIES, PLACE_201_CASES


@pytest.fixture
def cfg(tmp_path):
	return LiveConfig(audit_log_path=tmp_path / "audit.jsonl")


@pytest.fixture
def audit(tmp_path):
	return AuditLogger(tmp_path / "audit.jsonl")


def test_avg_fill_cents_matches_blended_price_cents_at_midpoint():
	"""``_avg_fill_cents`` (live REST aggregate-cost VWAP) MUST agree with
	``fill_math.blended_price_cents`` (the single-source-of-truth per-fill VWAP)
	so replay-live parity holds byte-exact — including at a .5¢ midpoint, where
	ROUND_HALF_UP and Python ``round()``'s half-even diverge by 1¢.

	Fills [10¢×1, 11¢×1] → aggregate ``taker_fill_cost_dollars`` $0.21 over 2
	contracts → VWAP 10.5¢. Both paths must round the SAME way (half-even,
	matching the paper/replay source of truth) → 10¢, not 11¢."""
	from edge_catcher.engine.fill_math import blended_price_cents
	from edge_catcher.live.client import _avg_fill_cents

	fills = [{"price": 10, "size": 1}, {"price": 11, "size": 1}]
	aggregate_cost_dollars = "0.21"  # Σ(price·size)/100 = (10 + 11)/100
	fill_count = 2

	assert _avg_fill_cents(aggregate_cost_dollars, fill_count) == blended_price_cents(fills)
	assert _avg_fill_cents(aggregate_cost_dollars, fill_count) == 10


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
	"a" * 81,                                  # too long (max 80)
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
			"initial_count_fp": "10.00",
			"fill_count_fp": "10.00",
			"remaining_count_fp": "0.00",
			"yes_price_dollars": "0.0500",
			"taker_fill_cost_dollars": "0.500000",
			"time_in_force": "gtc",
			"status": "executed",
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
# Real Kalshi create-order 201 shape — regression guard for the fill-parse bug.
# The live daemon misread EVERY executed fill as ioc_zero_fill because
# _parse_order read fictional fields (filled_count / yes_price / count) instead
# of Kalshi's real schema (fill_count_fp / *_price_dollars / initial_count_fp /
# taker_fill_cost_dollars). Bodies are captured verbatim from the live Pi audit
# log; see tests/fixtures/kalshi_responses.py.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("body,expected", PLACE_201_CASES)
def test_parse_order_real_kalshi_shape(cfg, audit, body, expected):
	"""_parse_order maps the REAL create-order 201 fields, not the old fiction.

	Failure mode prevented: fill_count_fp/initial_count_fp/*_price_dollars/
	taker_fill_cost_dollars being ignored → filled_count=0 → ioc_zero_fill →
	orphaned real position.
	"""
	client = KalshiOrderClient(cfg, audit)
	order = client._parse_order(body["order"])
	assert order.order_id == expected["order_id"]
	assert order.status == expected["status"]
	assert order.side == expected["side"]
	# fill_count_fp "6.00" (str) → 6, NOT data["filled_count"] (absent → 0).
	assert order.filled_count == expected["filled_count"]
	# initial_count_fp → original order size, NOT data["count"] (absent → 0).
	assert order.count == expected["count"]
	# {yes,no}_price_dollars "0.1700" → 17¢, NOT data["yes_price"]/["no_price"].
	assert order.limit_price_cents == expected["limit_price_cents"]
	# Blended cost basis = round(taker_fill_cost_dollars*100 / fill_count); the
	# real entry price (often better than the limit for an IOC).
	assert order.avg_fill_price_cents == expected["avg_fill_price_cents"]


def test_parse_order_zero_fill_canceled_shape(cfg, audit):
	"""An IOC that matched nothing: status canceled, fill_count_fp '0.00'.

	(Synthetic — the live run had no zero-fills — but pins the genuine
	zero-fill shape so _translate_order's ioc_zero_fill branch stays reachable.)
	"""
	client = KalshiOrderClient(cfg, audit)
	inner = dict(PLACE_201_BODIES[0]["order"])
	inner.update({"fill_count_fp": "0.00", "remaining_count_fp": "6.00",
	              "status": "canceled", "taker_fill_cost_dollars": "0.000000",
	              "taker_fees_dollars": "0.000000"})
	order = client._parse_order(inner)
	assert order.filled_count == 0
	assert order.avg_fill_price_cents == 0
	assert order.status == "canceled"
	# limit price still parses (the order existed, just didn't fill).
	assert order.limit_price_cents == 17


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
			"initial_count_fp": "10.00", "fill_count_fp": "3.00",
			"remaining_count_fp": "7.00", "yes_price_dollars": "0.0500",
			"taker_fill_cost_dollars": "0.150000", "time_in_force": "gtc",
			"status": "resting",
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


def test_order_request_accepts_80_char_client_order_id():
	"""80-char client_order_id is accepted — D-spec worst-case format boundary."""
	req = OrderRequest(
		ticker="X", action="buy", side="yes", count=1, limit_price_cents=1,
		client_order_id="a" * 80,
	)
	assert len(req.client_order_id) == 80


def test_order_request_rejects_81_char_client_order_id():
	"""81-char client_order_id must be rejected — one over the 80-char limit."""
	with pytest.raises(ValueError, match="client_order_id must match"):
		OrderRequest(
			ticker="X", action="buy", side="yes", count=1, limit_price_cents=1,
			client_order_id="a" * 81,
		)


def test_order_request_accepts_64_char_client_order_id():
	"""64-char client_order_id still accepted — regression guard from PR #28."""
	req = OrderRequest(
		ticker="X", action="buy", side="yes", count=1, limit_price_cents=1,
		client_order_id="a" * 64,
	)
	assert len(req.client_order_id) == 64


@pytest.mark.asyncio
async def test_positions_non_empty(cfg, audit, signing_env, tmp_path):
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})
	def handler(request: httpx.Request) -> httpx.Response:
		# Real GET /portfolio/positions market_positions shape: a SIGNED
		# fixed-point position_fp + market_exposure_dollars (the cost basis of
		# the open position). avg cost = exposure*100 / |position|.
		return httpx.Response(200, json={"market_positions": [
			{"ticker": "X", "position_fp": "10.00", "market_exposure_dollars": "0.500000"},
			{"ticker": "Y", "position_fp": "-3.00", "market_exposure_dollars": "2.850000"},
		]})
	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	positions = await c.positions()
	assert len(positions) == 2
	assert positions[0].ticker == "X"
	assert positions[0].count == 10
	assert positions[0].side == "yes"
	assert positions[0].average_price_cents == 5   # 0.50 / 10 → 5c
	# Negative position interpreted as no-side
	assert positions[1].side == "no"
	assert positions[1].count == 3
	assert positions[1].average_price_cents == 95  # 2.85 / 3 → 95c


# ---------------------------------------------------------------------------
# Audit-write off-loading regression — sub-project E pre-flight
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_place_audit_write_does_not_block_event_loop(cfg, signing_env, tmp_path, monkeypatch):
	"""Audit-log file I/O must not block the engine event loop.

	Once sub-project E ships, the engine's persistent event loop services
	WS receives, the 30s reconciler poll, and order placement on the same
	loop. A blocking ``open + write + close`` inside ``_request`` would stall
	all of those during a slow disk write — Pi SD-card spikes can hit tens
	of ms, which is enough to drop a WS tick during a live trade and
	violate the "no errors with live money" lens.

	We monkeypatch ``AuditLogger.write`` to ``time.sleep(0.100)`` (a 100 ms
	stall, ~10× a real Pi spike), then schedule a sentinel coroutine that
	wants to sleep for 30 ms while ``place()`` runs. The sentinel's actual
	wall-clock sleep duration is the signal: with the write off-loaded to
	a worker thread, the sentinel wakes on time (~30 ms); with the write
	blocking the loop, the sentinel can't be resumed until the 100 ms
	stall completes, so its sleep balloons to ~100 ms.
	"""
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})
	audit_logger = AuditLogger(cfg2.audit_log_path)
	original_write = audit_logger.write

	def slow_write(event):
		time.sleep(0.100)
		original_write(event)

	monkeypatch.setattr(audit_logger, "write", slow_write)

	def handler(request: httpx.Request) -> httpx.Response:
		return httpx.Response(201, json={"order": {"order_id": "x", "status": "resting"}})

	c = make_mock_client(cfg2, audit_logger, httpx.MockTransport(handler))
	req = OrderRequest(ticker="X", action="buy", side="yes", count=1, limit_price_cents=1)

	loop = asyncio.get_running_loop()
	sentinel_t0 = 0.0
	sentinel_t1 = 0.0
	target_sleep_s = 0.030

	async def sentinel():
		nonlocal sentinel_t0, sentinel_t1
		sentinel_t0 = loop.time()
		await asyncio.sleep(target_sleep_s)
		sentinel_t1 = loop.time()

	sentinel_task = asyncio.create_task(sentinel())
	# Yield once so the sentinel reaches its asyncio.sleep before place() runs.
	await asyncio.sleep(0)
	await c.place(req)
	await sentinel_task

	actual_sleep_ms = (sentinel_t1 - sentinel_t0) * 1000.0
	overrun_ms = actual_sleep_ms - (target_sleep_s * 1000.0)
	# Platform-aware threshold balancing production-target sensitivity (Pi/Linux
	# catches partial regressions) against CI runner scheduler variance.
	# Windows ProactorEventLoop: ~16 ms scheduler quantum, ~5-15 ms unblocked
	# overrun observed → 35 ms band.
	# Linux SelectorEventLoop on loaded GitHub Actions: up to ~17-20 ms unblocked
	# overrun observed (vs ~5 ms on quiet Pi) → 30 ms band gives ~10 ms CI
	# headroom while still catching ~40 ms+ partial loop stalls on Pi.
	# A fully blocked loop adds ~70 ms of overrun (full 100 ms slow_write minus
	# the 30 ms target) — well outside both bands.
	threshold_ms = 35.0 if sys.platform.startswith("win") else 30.0
	assert overrun_ms < threshold_ms, (
		f"sentinel asyncio.sleep({target_sleep_s*1000:.0f}ms) actually took "
		f"{actual_sleep_ms:.1f}ms — event loop was blocked for {overrun_ms:.1f}ms "
		f"during slow audit-write (slow_write=100ms; threshold={threshold_ms:.0f}ms "
		f"for {sys.platform})."
	)
	# Audit row was still written through the worker thread.
	assert cfg2.audit_log_path.exists()
	assert cfg2.audit_log_path.read_text().strip() != ""


# ---------------------------------------------------------------------------
# Audit-write fault isolation — guards _request against audit I/O failures so
# disk-full / permission errors during compliance logging don't kill orders
# the venue already accepted (success path) or mask the real exception
# (4xx / NetworkError paths). Sequenced ahead of sub-project E to keep the
# engine daemon's persistent event loop free of audit-induced crashes.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_place_returns_order_when_audit_write_fails(
	cfg, audit, signing_env, tmp_path, caplog,
):
	"""201 succeeded; OSError from audit must be swallowed + logged, not raised.

	Engine on persistent loop (sub-project E) cannot tolerate audit I/O killing
	a placement the venue already accepted — that strands a real Kalshi order
	with no record on our side and no retry path.
	"""
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})

	def handler(request: httpx.Request) -> httpx.Response:
		return httpx.Response(201, json={"order": {
			"order_id": "ord-1", "ticker": "X", "side": "yes", "action": "buy",
			"count": 1, "yes_price": 1, "time_in_force": "gtc", "status": "resting",
		}})

	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))

	async def _explode_audit(**_kwargs: object) -> None:
		raise OSError("disk full")
	c._write_audit_async = _explode_audit  # type: ignore[method-assign]

	req = OrderRequest(ticker="X", action="buy", side="yes", count=1, limit_price_cents=1)

	with caplog.at_level(logging.ERROR, logger="edge_catcher.live.client"):
		order = await c.place(req)

	assert order.order_id == "ord-1"
	assert any("audit_write_failed_after_success" in r.message for r in caplog.records), (
		"audit failure on success path must be logged at ERROR level"
	)


@pytest.mark.asyncio
async def test_place_4xx_still_raises_order_rejected_when_audit_write_fails(
	cfg, audit, signing_env, tmp_path, caplog,
):
	"""4xx must still raise OrderRejected even if audit-write also fails.

	Audit failure must not mask the real placement error — caller's reconcile /
	error-handling path keys off the typed exception.
	"""
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})

	def handler(request: httpx.Request) -> httpx.Response:
		return httpx.Response(400, json={"error": {"message": "invalid_price"}})

	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))

	async def _explode_audit(**_kwargs: object) -> None:
		raise OSError("disk full")
	c._write_audit_async = _explode_audit  # type: ignore[method-assign]

	req = OrderRequest(ticker="X", action="buy", side="yes", count=1, limit_price_cents=1)

	with caplog.at_level(logging.ERROR, logger="edge_catcher.live.client"):
		with pytest.raises(OrderRejected) as exc:
			await c.place(req)

	assert exc.value.status == 400
	assert any("audit_write_failed_after_http_error" in r.message for r in caplog.records), (
		"audit failure on 4xx path must be logged at ERROR level before re-raise"
	)


@pytest.mark.asyncio
async def test_network_error_still_raises_when_audit_write_fails(
	cfg, audit, signing_env, tmp_path, caplog, monkeypatch,
):
	"""Exhausted network retries must still raise NetworkError when audit fails.

	Same shape as the 4xx case for the third call site in `_request`.
	"""
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl", "max_retries": 0})

	# Skip backoff so the test doesn't burn real wall time. max_retries=0
	# means we exhaust on the first attempt, but defend in depth anyway.
	async def _no_sleep(_s: float) -> None:
		return None
	monkeypatch.setattr("edge_catcher.live.client.asyncio.sleep", _no_sleep)

	def handler(request: httpx.Request) -> httpx.Response:
		raise httpx.ConnectError("can't reach kalshi")

	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))

	async def _explode_audit(**_kwargs: object) -> None:
		raise OSError("disk full")
	c._write_audit_async = _explode_audit  # type: ignore[method-assign]

	req = OrderRequest(ticker="X", action="buy", side="yes", count=1, limit_price_cents=1)

	with caplog.at_level(logging.ERROR, logger="edge_catcher.live.client"):
		with pytest.raises(NetworkError):
			await c.place(req)

	assert any("audit_write_failed_after_network_error" in r.message for r in caplog.records), (
		"audit failure on network-error path must be logged at ERROR level before re-raise"
	)
