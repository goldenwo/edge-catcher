"""TDD spec for the Kalshi V2 create-order migration (client.py).

Kalshi hard-deprecated the legacy ``POST /trade-api/v2/portfolio/orders``
create endpoint (HTTP 410 ``deprecated_v1_order_endpoint``, 2026-06-22). The
replacement is ``POST /trade-api/v2/portfolio/events/orders`` with a reshaped
body (``side`` = ``bid``/``ask`` on the single YES book, a single fixed-point
``price`` in dollars, fixed-point ``count`` string, required
``self_trade_prevention_type``) and a flat response (``fill_count`` /
``remaining_count`` / ``average_fill_price`` — no ``{"order": ...}`` wrapper,
no ``status``).

These tests pin BOTH the request mapping and the response parsing, including
the load-bearing NO-side price inversion (a ``buy no @ 46`` is sent as
``ask @ 0.54`` and the fill price must be recorded back as 46¢, not 54¢, or the
real-money cost basis is corrupted). Mappings are anchored to Kalshi's own
historical ``book_side``/``yes_price_dollars`` echoes (see the design note
docs/superpowers/specs/2026-06-22-kalshi-v2-order-endpoint-migration.md §3).
"""
from __future__ import annotations

import json
import logging
from typing import Any

import httpx
import pytest

from edge_catcher.live.audit import AuditLogger
from edge_catcher.live.client import KalshiOrderClient
from edge_catcher.live.config import LiveConfig
from edge_catcher.live.venue import OrderRequest

V2_CREATE_PATH = "/trade-api/v2/portfolio/events/orders"


@pytest.fixture
def cfg(tmp_path: Any) -> LiveConfig:
	return LiveConfig(audit_log_path=tmp_path / "audit.jsonl")


@pytest.fixture
def audit(tmp_path: Any) -> AuditLogger:
	return AuditLogger(tmp_path / "audit.jsonl")


@pytest.fixture
def signing_env(monkeypatch: pytest.MonkeyPatch) -> None:
	"""Seed throwaway live-trader signing env vars (LIVE-suffixed, per client.py)."""
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


def v2_response(
	*,
	order_id: str = "ord-v2-mock",
	client_order_id: str | None = None,
	fill_count: str = "0.00",
	remaining_count: str = "0.00",
	average_fill_price: str | None = None,
	average_fee_paid: str | None = None,
	ts_ms: int = 1715793600123,
) -> dict[str, Any]:
	"""Kalshi's flat V2 create-order response body (no ``order`` wrapper)."""
	return {
		"order_id": order_id,
		"client_order_id": client_order_id,
		"fill_count": fill_count,
		"remaining_count": remaining_count,
		"average_fill_price": average_fill_price,
		"average_fee_paid": average_fee_paid,
		"ts_ms": ts_ms,
	}


def make_client(
	cfg: LiveConfig,
	audit: AuditLogger,
	captured: list[httpx.Request],
	body: dict[str, Any],
	*,
	status: int = 201,
) -> KalshiOrderClient:
	"""Build a KalshiOrderClient whose transport captures the request and
	returns ``body`` with ``status``."""

	def handler(request: httpx.Request) -> httpx.Response:
		captured.append(request)
		return httpx.Response(status, json=body)

	client = KalshiOrderClient(cfg, audit)
	client._http = httpx.AsyncClient(
		base_url=cfg.kalshi_rest_base,
		timeout=cfg.http_timeout_seconds,
		headers={"Accept": "application/json"},
		transport=httpx.MockTransport(handler),
	)
	return client


def sent_body(captured: list[httpx.Request]) -> dict[str, Any]:
	return json.loads(captured[-1].content)


# ---------------------------------------------------------------------------
# Request: path + body shape
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_place_posts_to_v2_events_orders_path(cfg, audit, signing_env):
	captured: list[httpx.Request] = []
	client = make_client(cfg, audit, captured, v2_response())
	await client.place(OrderRequest(
		ticker="KXSOL15M-26JUN220015-15", action="buy", side="yes",
		count=2, limit_price_cents=46, time_in_force="ioc",
		client_order_id="strat-coid-1",
	))
	assert captured[-1].url.path == V2_CREATE_PATH
	assert captured[-1].method == "POST"
	# Signing must still fire for the new path.
	assert "KALSHI-ACCESS-SIGNATURE" in captured[-1].headers


@pytest.mark.asyncio
async def test_place_body_buy_yes_maps_to_bid(cfg, audit, signing_env):
	captured: list[httpx.Request] = []
	client = make_client(cfg, audit, captured, v2_response())
	await client.place(OrderRequest(
		ticker="KXETH15M-26JUN220015-15", action="buy", side="yes",
		count=2, limit_price_cents=46, time_in_force="ioc",
		client_order_id="coid-buy-yes",
	))
	body = sent_body(captured)
	assert body["side"] == "bid"
	assert body["price"] == "0.4600"
	assert body["count"] == "2.00"
	assert body["ticker"] == "KXETH15M-26JUN220015-15"
	assert body["time_in_force"] == "immediate_or_cancel"
	assert body["self_trade_prevention_type"] == "taker_at_cross"
	assert body["client_order_id"] == "coid-buy-yes"
	# Legacy fields must be GONE.
	assert "action" not in body
	assert "yes_price" not in body
	assert "no_price" not in body


@pytest.mark.asyncio
async def test_place_body_sell_yes_maps_to_ask(cfg, audit, signing_env):
	captured: list[httpx.Request] = []
	client = make_client(cfg, audit, captured, v2_response())
	await client.place(OrderRequest(
		ticker="X", action="sell", side="yes", count=1, limit_price_cents=46,
		time_in_force="ioc", client_order_id="coid-sell-yes",
	))
	body = sent_body(captured)
	assert body["side"] == "ask"
	assert body["price"] == "0.4600"


@pytest.mark.asyncio
async def test_place_body_buy_no_maps_to_ask_at_complement(cfg, audit, signing_env):
	"""buy no @ 46¢ ≡ ask (sell YES) @ (100-46)=54¢. Anchored to the May-24
	live echo: buy no @ 17 → book_side ask, yes_price_dollars 0.8300."""
	captured: list[httpx.Request] = []
	client = make_client(cfg, audit, captured, v2_response())
	await client.place(OrderRequest(
		ticker="X", action="buy", side="no", count=2, limit_price_cents=46,
		time_in_force="ioc", client_order_id="coid-buy-no",
	))
	body = sent_body(captured)
	assert body["side"] == "ask"
	assert body["price"] == "0.5400"


@pytest.mark.asyncio
async def test_place_body_sell_no_maps_to_bid_at_complement(cfg, audit, signing_env):
	"""sell no @ 15¢ ≡ bid (buy YES) @ 85¢. Anchored to the Jun-07 live echo:
	sell no @ 15 → book_side bid, yes_price_dollars 0.8500."""
	captured: list[httpx.Request] = []
	client = make_client(cfg, audit, captured, v2_response())
	await client.place(OrderRequest(
		ticker="KXSOL15M-26JUN072000-00", action="sell", side="no",
		count=1, limit_price_cents=15, time_in_force="ioc",
		client_order_id="coid-sell-no",
	))
	body = sent_body(captured)
	assert body["side"] == "bid"
	assert body["price"] == "0.8500"


@pytest.mark.asyncio
async def test_place_body_count_is_fixed_point_string_multidigit(cfg, audit, signing_env):
	captured: list[httpx.Request] = []
	client = make_client(cfg, audit, captured, v2_response())
	await client.place(OrderRequest(
		ticker="X", action="buy", side="no", count=24, limit_price_cents=6,
		time_in_force="ioc", client_order_id="coid-24",
	))
	body = sent_body(captured)
	assert body["count"] == "24.00"
	assert body["price"] == "0.9400"  # buy no @ 6 → ask @ 94


@pytest.mark.asyncio
async def test_place_does_not_send_buy_max_cost_v2(cfg, audit, signing_env):
	captured: list[httpx.Request] = []
	client = make_client(cfg, audit, captured, v2_response())
	await client.place(OrderRequest(
		ticker="X", action="buy", side="yes", count=2, limit_price_cents=46,
		time_in_force="ioc", client_order_id="coid-bmc",
	))
	assert "buy_max_cost" not in sent_body(captured)


# ---------------------------------------------------------------------------
# Response: parse the flat V2 body → normalized Order
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_place_parses_filled_yes_order(cfg, audit, signing_env):
	captured: list[httpx.Request] = []
	client = make_client(cfg, audit, captured, v2_response(
		order_id="ord-yes", fill_count="2.00", remaining_count="0.00",
		average_fill_price="0.4600",
	))
	order = await client.place(OrderRequest(
		ticker="X", action="buy", side="yes", count=2, limit_price_cents=46,
		time_in_force="ioc", client_order_id="coid-fy",
	))
	assert order.order_id == "ord-yes"
	assert order.filled_count == 2
	assert order.count == 2
	assert order.avg_fill_price_cents == 46
	assert order.side == "yes"
	assert order.status == "executed"


@pytest.mark.asyncio
async def test_place_parses_filled_no_order_inverts_price(cfg, audit, signing_env):
	"""THE load-bearing test: a buy-no fills on the YES book at 0.54, and the
	recorded cost basis must be the NO price 46¢ — never the raw YES 54¢."""
	captured: list[httpx.Request] = []
	client = make_client(cfg, audit, captured, v2_response(
		order_id="ord-no", fill_count="2.00", remaining_count="0.00",
		average_fill_price="0.5400",
	))
	order = await client.place(OrderRequest(
		ticker="X", action="buy", side="no", count=2, limit_price_cents=46,
		time_in_force="ioc", client_order_id="coid-fn",
	))
	assert order.filled_count == 2
	assert order.avg_fill_price_cents == 46  # 100 - 54, NOT 54
	assert order.side == "no"


@pytest.mark.asyncio
async def test_place_parses_no_order_price_improvement_inverts(cfg, audit, signing_env):
	"""Price improvement on a buy-no: fills at YES 0.56 → NO 44¢ (< 46 limit)."""
	captured: list[httpx.Request] = []
	client = make_client(cfg, audit, captured, v2_response(
		fill_count="2.00", remaining_count="0.00", average_fill_price="0.5600",
	))
	order = await client.place(OrderRequest(
		ticker="X", action="buy", side="no", count=2, limit_price_cents=46,
		time_in_force="ioc", client_order_id="coid-imp",
	))
	assert order.avg_fill_price_cents == 44


@pytest.mark.asyncio
async def test_place_parses_zero_fill(cfg, audit, signing_env):
	"""IOC zero-fill: no status field; null average_fill_price → 0 sentinel."""
	captured: list[httpx.Request] = []
	client = make_client(cfg, audit, captured, v2_response(
		fill_count="0.00", remaining_count="2.00", average_fill_price=None,
	))
	order = await client.place(OrderRequest(
		ticker="X", action="buy", side="yes", count=2, limit_price_cents=46,
		time_in_force="ioc", client_order_id="coid-zf",
	))
	assert order.filled_count == 0
	assert order.avg_fill_price_cents == 0


@pytest.mark.asyncio
async def test_place_parses_partial_fill(cfg, audit, signing_env):
	"""Partial IOC: count recovered from fill_count + remaining_count."""
	captured: list[httpx.Request] = []
	client = make_client(cfg, audit, captured, v2_response(
		fill_count="1.00", remaining_count="1.00", average_fill_price="0.4600",
	))
	order = await client.place(OrderRequest(
		ticker="X", action="buy", side="yes", count=2, limit_price_cents=46,
		time_in_force="ioc", client_order_id="coid-pf",
	))
	assert order.filled_count == 1
	assert order.count == 2
	assert order.avg_fill_price_cents == 46
	assert order.status == "partially_filled"


# ---------------------------------------------------------------------------
# Robustness (defense-in-depth — from the adversarial code review)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_place_tolerates_order_wrapper_defensively(cfg, audit, signing_env):
	"""If the create response arrives wrapped in {"order": ...} (legacy shape, or
	a proxy/quirk/future revert), a REAL fill must NOT be misread as a phantom
	zero-fill reject (funds-at-risk: a filled position dropped). The parser
	unwraps defensively, mirroring the legacy `_parse_order` tolerance."""
	captured: list[httpx.Request] = []
	wrapped = {"order": v2_response(
		order_id="ord-wrapped", fill_count="2.00", remaining_count="0.00",
		average_fill_price="0.4600",
	)}
	client = make_client(cfg, audit, captured, wrapped)
	order = await client.place(OrderRequest(
		ticker="X", action="buy", side="yes", count=2, limit_price_cents=46,
		time_in_force="ioc", client_order_id="coid-wrap",
	))
	assert order.order_id == "ord-wrapped"
	assert order.filled_count == 2
	assert order.avg_fill_price_cents == 46


@pytest.mark.asyncio
async def test_place_partial_field_count_falls_back_to_request(cfg, audit, signing_env):
	"""If only one of fill_count/remaining_count is present, recover the original
	size from the request — never filled+0 (which would understate the size)."""
	captured: list[httpx.Request] = []
	body = {
		"order_id": "ord-pf", "fill_count": "2.00",  # remaining_count omitted
		"average_fill_price": "0.4600", "ts_ms": 1,
	}
	client = make_client(cfg, audit, captured, body)
	order = await client.place(OrderRequest(
		ticker="X", action="buy", side="yes", count=5, limit_price_cents=46,
		time_in_force="ioc", client_order_id="coid-pf2",
	))
	assert order.filled_count == 2
	assert order.count == 5  # original size from the request, NOT 2 + 0


@pytest.mark.asyncio
async def test_place_boundary_fill_sentinels_and_warns(cfg, audit, signing_env, caplog):
	"""A real fill at the 0/100¢ caller-side boundary collapses onto the no-cost
	sentinel (0) AND is logged, so it never passes silently. Downstream
	(_translate_order) then fails safe to pending+reconcile rather than recording
	a bogus basis. Here: buy no @ 1¢ filling at YES 1.0000 → caller-side NO 0¢."""
	captured: list[httpx.Request] = []
	body = v2_response(
		order_id="ord-boundary", fill_count="2.00", remaining_count="0.00",
		average_fill_price="1.0000",
	)
	client = make_client(cfg, audit, captured, body)
	with caplog.at_level(logging.WARNING, logger="edge_catcher.live.client"):
		order = await client.place(OrderRequest(
			ticker="X", action="buy", side="no", count=2, limit_price_cents=1,
			time_in_force="ioc", client_order_id="coid-bnd",
		))
	assert order.filled_count == 2
	assert order.avg_fill_price_cents == 0
	assert any("no usable cost basis" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_place_yes_zero_boundary_sentinels_and_warns(cfg, audit, signing_env, caplog):
	"""Symmetric boundary: a buy-yes filling at YES 0.0000 (caller-side 0¢) also
	collapses onto the no-cost sentinel and warns (covers the yes_cents==0 arm of
	the guard, mirroring the no-side yes_cents==100 case above)."""
	captured: list[httpx.Request] = []
	body = v2_response(
		order_id="ord-yzero", fill_count="2.00", remaining_count="0.00",
		average_fill_price="0.0000",
	)
	client = make_client(cfg, audit, captured, body)
	with caplog.at_level(logging.WARNING, logger="edge_catcher.live.client"):
		order = await client.place(OrderRequest(
			ticker="X", action="buy", side="yes", count=2, limit_price_cents=1,
			time_in_force="ioc", client_order_id="coid-yz",
		))
	assert order.filled_count == 2
	assert order.avg_fill_price_cents == 0
	assert any("no usable cost basis" in r.message for r in caplog.records)
