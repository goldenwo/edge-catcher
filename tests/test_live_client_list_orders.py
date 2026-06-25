"""Tests for KalshiOrderClient.list_orders — the GET /portfolio/orders primitive.

Added by PR 5 / Task 4.B-pre. Sub-project A shipped the client without a
list-orders primitive; B's reconciliation module (4.B) needs
``await client.list_orders(...)`` to resolve phantom-pending rows by
``client_order_id`` (poller) and to pull active + recently-completed orders
at startup. This file covers the primitive in isolation; reconciliation logic
is out of scope here.

Conventions mirrored from ``tests/test_live_client.py``:
* ``httpx.MockTransport`` (NOT pytest-httpx — not in the dependency set).
* ``make_mock_client`` swaps the client's ``_http`` for a MockTransport-backed
  ``httpx.AsyncClient``.
* ``signing_env`` seeds throwaway RSA-2048 LIVE-suffixed signing env vars.
* The real ``_parse_order`` is exercised (never mocked) so the per-element
  parse path is the production path.

Wire shape verified against the Kalshi OpenAPI spec (GET /portfolio/orders →
``GetOrdersResponse {orders: [Order], cursor: string}``; query params
``status`` / ``limit`` / ``cursor``) and the repo's existing Kalshi pagination
convention (``edge_catcher/adapters/kalshi/adapter.py`` uses ``cursor`` /
``data.get("cursor")``; ``client.positions()`` unwraps a single collection key).
"""
from __future__ import annotations

import json
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest

from edge_catcher.adapters.kalshi import auth as kalshi_auth
from edge_catcher.live.audit import AuditLogger
from edge_catcher.live.client import KalshiOrderClient
from edge_catcher.live.venue import Order
from edge_catcher.live.config import LiveConfig
from edge_catcher.live.errors import KalshiAPIError, OrderAlreadyFinal


@pytest.fixture
def cfg(tmp_path):
	return LiveConfig(audit_log_path=tmp_path / "audit.jsonl")


@pytest.fixture
def audit(tmp_path):
	return AuditLogger(tmp_path / "audit.jsonl")


@pytest.fixture
def signing_env(monkeypatch):
	"""Seed throwaway LIVE-suffixed signing env vars (mirrors test_live_client)."""
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


def make_mock_client(cfg, audit, transport):
	c = KalshiOrderClient(cfg, audit)
	c._http = httpx.AsyncClient(
		base_url=cfg.kalshi_rest_base,
		timeout=cfg.http_timeout_seconds,
		headers={"Accept": "application/json"},
		transport=transport,
	)
	return c


def _order_json(
	*,
	order_id: str,
	client_order_id: str,
	status: str = "resting",
	count: int = 10,
	filled_count: int = 0,
	side: str = "yes",
	yes_price: int = 5,
) -> dict:
	"""Kalshi GET /portfolio/orders element shape — the REAL inner order shape
	(identical to what create-order / get-order return): fixed-point count
	STRINGS, dollar price STRINGS, aggregate taker fill cost; no per-fill
	array."""
	own = f"{yes_price / 100:.4f}"
	comp = f"{(100 - yes_price) / 100:.4f}"
	return {
		"order_id": order_id,
		"client_order_id": client_order_id,
		"ticker": "KXSOL15M-26MAY09H06",
		"side": side,
		"action": "buy",
		"initial_count_fp": f"{count}.00",
		"fill_count_fp": f"{filled_count}.00",
		"remaining_count_fp": f"{count - filled_count}.00",
		"yes_price_dollars": own if side == "yes" else comp,
		"no_price_dollars": own if side == "no" else comp,
		"taker_fill_cost_dollars": f"{yes_price * filled_count / 100:.6f}",
		"time_in_force": "immediate_or_cancel",
		"status": status,
		"created_time": "2026-05-16T18:00:00Z",
	}


# ---------------------------------------------------------------------------
# 1. Happy path — multi-order body parsed via the real _parse_order
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_orders_happy_path_parses_all(cfg, audit, signing_env, tmp_path):
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})
	body = {
		"orders": [
			_order_json(
				order_id="ord-1", client_order_id="strat-a-KXSOL15M-1",
				status="resting", count=10, filled_count=0,
			),
			_order_json(
				order_id="ord-2", client_order_id="strat-a-KXSOL15M-2",
				status="executed", count=8, filled_count=8,
			),
		],
		"cursor": "",
	}

	captured: list[httpx.Request] = []

	def handler(request: httpx.Request) -> httpx.Response:
		captured.append(request)
		return httpx.Response(200, json=body)

	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	orders = await c.list_orders()

	assert isinstance(orders, list)
	assert all(isinstance(o, Order) for o in orders)
	assert [o.order_id for o in orders] == ["ord-1", "ord-2"]
	assert [o.client_order_id for o in orders] == [
		"strat-a-KXSOL15M-1", "strat-a-KXSOL15M-2",
	]
	assert [o.status for o in orders] == ["resting", "executed"]
	assert orders[0].filled_count == 0
	assert orders[1].filled_count == 8
	# Hits the canonical GET /portfolio/orders path (prefix included).
	assert captured[0].method == "GET"
	assert captured[0].url.path == "/trade-api/v2/portfolio/orders"
	# Audit row written for the new op.
	row = json.loads(cfg2.audit_log_path.read_text().strip().split("\n")[-1])
	assert row["op"] == "list_orders"
	assert row["outcome"] == "success"


# ---------------------------------------------------------------------------
# 2. status= filter transmitted as the correct query param
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_orders_status_filter_in_query(cfg, audit, signing_env, tmp_path):
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})
	captured: list[httpx.Request] = []

	def handler(request: httpx.Request) -> httpx.Response:
		captured.append(request)
		return httpx.Response(200, json={"orders": [], "cursor": ""})

	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	await c.list_orders(status="resting", limit=50)

	q = parse_qs(urlsplit(str(captured[0].url)).query)
	assert q["status"] == ["resting"]
	assert q["limit"] == ["50"]
	# cursor not sent when None
	assert "cursor" not in q


@pytest.mark.asyncio
async def test_list_orders_cursor_passed_through(cfg, audit, signing_env, tmp_path):
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})
	captured: list[httpx.Request] = []

	def handler(request: httpx.Request) -> httpx.Response:
		captured.append(request)
		return httpx.Response(200, json={"orders": [], "cursor": ""})

	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	await c.list_orders(cursor="opaque-cursor-token")

	q = parse_qs(urlsplit(str(captured[0].url)).query)
	assert q["cursor"] == ["opaque-cursor-token"]
	# Single HTTP call — list_orders does NOT internally follow the cursor.
	assert len(captured) == 1


@pytest.mark.asyncio
async def test_list_orders_default_no_status_param(cfg, audit, signing_env, tmp_path):
	"""Default invocation omits status (4.B startup/poller wants all recent
	orders) but always sends a bounded limit."""
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})
	captured: list[httpx.Request] = []

	def handler(request: httpx.Request) -> httpx.Response:
		captured.append(request)
		return httpx.Response(200, json={"orders": [], "cursor": ""})

	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	await c.list_orders()

	q = parse_qs(urlsplit(str(captured[0].url)).query)
	assert "status" not in q
	assert q["limit"] == ["200"]  # generous bounded default


# ---------------------------------------------------------------------------
# 3. Empty result -> []
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_orders_empty(cfg, audit, signing_env, tmp_path):
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})

	def handler(request: httpx.Request) -> httpx.Response:
		return httpx.Response(200, json={"orders": [], "cursor": ""})

	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	assert await c.list_orders() == []


@pytest.mark.asyncio
async def test_list_orders_missing_orders_key_is_empty(cfg, audit, signing_env, tmp_path):
	"""Defensive: an absent ``orders`` key yields [] (mirrors positions()'s
	``response.get("market_positions", [])`` convention) rather than raising."""
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})

	def handler(request: httpx.Request) -> httpx.Response:
		return httpx.Response(200, json={"cursor": ""})

	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	assert await c.list_orders() == []


# ---------------------------------------------------------------------------
# 4. Signing-path parity (HIGHEST PRIORITY, real-money correctness)
#
# Kalshi RSA signing signs ts+method+path with the query string STRIPPED
# (auth.py:48 `path.split("?",1)[0]`; proven in test_kalshi_auth.py). The
# module's invariant (client._KALSHI_REST_PREFIX docstring + _request comment)
# is that the string passed to make_auth_headers is the SAME string sent on
# the wire. With query params this means: the query must be baked into the
# single path string, NOT appended by httpx via a separate params= dict.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_orders_signed_path_is_byte_identical_to_sent_url(
	cfg, audit, signing_env, tmp_path, monkeypatch,
):
	"""The exact string handed to make_auth_headers must equal the path+query
	httpx actually puts on the wire — including the query string."""
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})

	signed_paths: list[str] = []
	real_make_auth_headers = kalshi_auth.make_auth_headers

	def _spy_make_auth_headers(method, path, **kw):
		signed_paths.append(path)
		return real_make_auth_headers(method, path, **kw)

	# client.py imports make_auth_headers by name, so patch the binding in
	# client's namespace (same target the rest of the suite uses for sleep).
	monkeypatch.setattr(
		"edge_catcher.live.client.make_auth_headers", _spy_make_auth_headers
	)

	sent_targets: list[str] = []

	def handler(request: httpx.Request) -> httpx.Response:
		# raw_path is the exact bytes on the wire incl. the query string.
		sent_targets.append(request.url.raw_path.decode())
		return httpx.Response(200, json={"orders": [], "cursor": ""})

	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	await c.list_orders(status="resting", limit=50, cursor="cur-xyz")

	assert len(signed_paths) == 1
	assert len(sent_targets) == 1
	# Byte-identical: the signed path string == the wire path+query string.
	assert signed_paths[0] == sent_targets[0], (
		f"signing-path parity violated: signed {signed_paths[0]!r} != "
		f"sent {sent_targets[0]!r} — Kalshi RSA auth would mismatch"
	)
	# And it is the fully-qualified, query-bearing path (not bare).
	assert signed_paths[0].startswith("/trade-api/v2/portfolio/orders?")
	q = parse_qs(urlsplit(signed_paths[0]).query)
	assert q["status"] == ["resting"]
	assert q["limit"] == ["50"]
	assert q["cursor"] == ["cur-xyz"]


@pytest.mark.asyncio
async def test_list_orders_no_separate_httpx_params_dict(cfg, audit, signing_env, tmp_path):
	"""Regression: the query must be baked into the request URL, never appended
	by httpx out of band. With the query in the path string and no params=,
	httpx must not double-encode or duplicate keys."""
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})
	captured: list[httpx.Request] = []

	def handler(request: httpx.Request) -> httpx.Response:
		captured.append(request)
		return httpx.Response(200, json={"orders": [], "cursor": ""})

	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	await c.list_orders(status="resting", limit=10)

	url = urlsplit(str(captured[0].url))
	q = parse_qs(url.query)
	# Each key appears exactly once (no httpx-vs-manual duplication).
	assert q["status"] == ["resting"]
	assert q["limit"] == ["10"]
	assert url.path == "/trade-api/v2/portfolio/orders"


# ---------------------------------------------------------------------------
# 5. Single page only — list_orders does not internally follow the cursor.
#    (Spec §(c): "ONE call to client.list_orders() per cycle". The contract
#    explicitly prefers a single bounded fetch over internal cursor-follow.)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_orders_single_page_even_when_cursor_returned(
	cfg, audit, signing_env, tmp_path,
):
	"""Server returns a non-empty cursor (more pages exist); list_orders still
	issues exactly ONE HTTP call and returns only the first page."""
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})
	calls = [0]

	def handler(request: httpx.Request) -> httpx.Response:
		calls[0] += 1
		return httpx.Response(200, json={
			"orders": [_order_json(order_id="ord-1", client_order_id="coid-1")],
			"cursor": "there-is-more",
		})

	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	orders = await c.list_orders()

	assert calls[0] == 1  # exactly one REST call per invocation
	assert len(orders) == 1
	assert orders[0].order_id == "ord-1"


# ---------------------------------------------------------------------------
# 6. Error / 4xx path consistent with status()/positions() (generic
#    KalshiAPIError via the shared _request dispatch — NOT the place/cancel
#    special-case exceptions, no bespoke handling).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_orders_4xx_raises_kalshi_api_error(cfg, audit, signing_env, tmp_path):
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})

	def handler(request: httpx.Request) -> httpx.Response:
		return httpx.Response(401, json={"error": {"message": "unauthorized"}})

	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	with pytest.raises(KalshiAPIError) as exc:
		await c.list_orders()
	assert exc.value.status == 401
	# Generic dispatch — NOT the cancel-only OrderAlreadyFinal subclass.
	assert not isinstance(exc.value, OrderAlreadyFinal)


@pytest.mark.asyncio
async def test_list_orders_4xx_404_is_generic_not_already_final(
	cfg, audit, signing_env, tmp_path,
):
	"""404 for list_orders must NOT be mapped to OrderAlreadyFinal — that
	mapping is cancel-op-specific. list_orders uses op='list_orders' so it
	falls through to the generic KalshiAPIError, same as status()/positions()."""
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})

	def handler(request: httpx.Request) -> httpx.Response:
		return httpx.Response(404, json={"error": {"message": "not found"}})

	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	with pytest.raises(KalshiAPIError) as exc:
		await c.list_orders()
	assert exc.value.status == 404
	assert not isinstance(exc.value, OrderAlreadyFinal)


# ---------------------------------------------------------------------------
# 7. min_ts recency bound (real-money robustness fix #1)
#
# Without a server-side time bound, an extended-downtime startup reconcile can
# have >limit orders; a genuine pending row's matching (possibly FILLED) Kalshi
# order falls off page 1 -> reconciler finds no match -> TTL marks it
# rejected_post_hoc -> stranded real-money position + phantom rejection. 4.B's
# startup reconcile MUST pass min_ts (bounded to "since now - lookback") so the
# working set is server-side bounded. Mirrors the repo precedent:
# edge_catcher/adapters/kalshi/adapter.py passes min_ts (Unix seconds) to the
# same Kalshi API family for GetTrades.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_orders_min_ts_in_query_when_supplied(cfg, audit, signing_env, tmp_path):
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})
	captured: list[httpx.Request] = []

	def handler(request: httpx.Request) -> httpx.Response:
		captured.append(request)
		return httpx.Response(200, json={"orders": [], "cursor": ""})

	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	await c.list_orders(min_ts=1_715_000_000)

	q = parse_qs(urlsplit(str(captured[0].url)).query)
	# Unix seconds, transmitted verbatim (same convention as adapter.py min_ts).
	assert q["min_ts"] == ["1715000000"]
	assert q["limit"] == ["200"]  # default unchanged


@pytest.mark.asyncio
async def test_list_orders_min_ts_absent_when_not_supplied(cfg, audit, signing_env, tmp_path):
	"""Default invocation (low-volume 30s poller path) omits min_ts entirely —
	None is dropped, never sent as an empty value."""
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})
	captured: list[httpx.Request] = []

	def handler(request: httpx.Request) -> httpx.Response:
		captured.append(request)
		return httpx.Response(200, json={"orders": [], "cursor": ""})

	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	await c.list_orders()

	q = parse_qs(urlsplit(str(captured[0].url)).query)
	assert "min_ts" not in q


@pytest.mark.asyncio
async def test_list_orders_signed_path_byte_identical_with_min_ts(
	cfg, audit, signing_env, tmp_path, monkeypatch,
):
	"""Signing-path parity MUST still hold with min_ts present: the exact string
	handed to make_auth_headers must equal the path+query httpx puts on the wire,
	including the min_ts query component. (Kalshi RSA signing strips the query;
	the module invariant is the stronger 'signed string == sent string'.)"""
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})

	signed_paths: list[str] = []
	real_make_auth_headers = kalshi_auth.make_auth_headers

	def _spy_make_auth_headers(method, path, **kw):
		signed_paths.append(path)
		return real_make_auth_headers(method, path, **kw)

	monkeypatch.setattr(
		"edge_catcher.live.client.make_auth_headers", _spy_make_auth_headers
	)

	sent_targets: list[str] = []

	def handler(request: httpx.Request) -> httpx.Response:
		sent_targets.append(request.url.raw_path.decode())
		return httpx.Response(200, json={"orders": [], "cursor": ""})

	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	await c.list_orders(status="resting", limit=50, cursor="cur-xyz", min_ts=1_715_000_000)

	assert len(signed_paths) == 1
	assert len(sent_targets) == 1
	# Byte-identical: signed path string == wire path+query string, min_ts incl.
	assert signed_paths[0] == sent_targets[0], (
		f"signing-path parity violated with min_ts: signed {signed_paths[0]!r} "
		f"!= sent {sent_targets[0]!r} — Kalshi RSA auth would mismatch"
	)
	assert signed_paths[0].startswith("/trade-api/v2/portfolio/orders?")
	q = parse_qs(urlsplit(signed_paths[0]).query)
	assert q["status"] == ["resting"]
	assert q["limit"] == ["50"]
	assert q["cursor"] == ["cur-xyz"]
	assert q["min_ts"] == ["1715000000"]


# ---------------------------------------------------------------------------
# 8. Malformed element skip-and-log (real-money robustness fix #2)
#
# One bad element in `orders` must NOT abort the whole batch: an all-or-nothing
# comprehension would propagate out of list_orders -> out of B's
# _reconcile_pending_batch -> caught only by the spec's top-level except
# Exception which SKIPS THE ENTIRE CYCLE, pushing every genuine pending row
# toward its 90s TTL -> rejected_post_hoc on possibly-filled orders.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_orders_skips_malformed_element_logs_and_continues(
	cfg, audit, signing_env, tmp_path, caplog,
):
	"""orders: [valid, "not-a-dict", valid] -> returns the 2 valid Orders, logs
	a warning, does NOT raise."""
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})
	body = {
		"orders": [
			_order_json(order_id="ord-1", client_order_id="strat-a-KXSOL15M-1"),
			"not-a-dict",  # malformed: _parse_order would AttributeError on .get
			_order_json(order_id="ord-2", client_order_id="strat-a-KXSOL15M-2"),
		],
		"cursor": "",
	}

	def handler(request: httpx.Request) -> httpx.Response:
		return httpx.Response(200, json=body)

	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	with caplog.at_level("WARNING"):
		orders = await c.list_orders()

	# The two well-formed elements survive; the bad one is skipped (not fatal).
	assert [o.order_id for o in orders] == ["ord-1", "ord-2"]
	assert all(isinstance(o, Order) for o in orders)
	assert any(
		"list_orders" in r.message and r.levelname == "WARNING"
		for r in caplog.records
	), f"expected a list_orders skip warning, got {[r.message for r in caplog.records]}"


@pytest.mark.asyncio
async def test_list_orders_all_malformed_returns_empty_not_raises(
	cfg, audit, signing_env, tmp_path, caplog,
):
	"""Pathological all-bad page -> [] (so B's reconcile cycle proceeds with no
	matches rather than aborting every phantom-pending row that cycle)."""
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})
	body = {"orders": [None, 42, ["nested"]], "cursor": ""}

	def handler(request: httpx.Request) -> httpx.Response:
		return httpx.Response(200, json=body)

	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	with caplog.at_level("WARNING"):
		orders = await c.list_orders()

	assert orders == []
	assert sum(r.levelname == "WARNING" for r in caplog.records) == 3


# ---------------------------------------------------------------------------
# 9. limit clamp (Minor #5) — a typo'd tiny/huge limit in 4.B must not
#    silently shrink/blow the reconciliation window. Kalshi documented max
#    page size is 1000.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_orders_limit_clamped_low(cfg, audit, signing_env, tmp_path):
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})
	captured: list[httpx.Request] = []

	def handler(request: httpx.Request) -> httpx.Response:
		captured.append(request)
		return httpx.Response(200, json={"orders": [], "cursor": ""})

	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	await c.list_orders(limit=0)

	q = parse_qs(urlsplit(str(captured[0].url)).query)
	assert q["limit"] == ["1"]  # clamped up to the floor


@pytest.mark.asyncio
async def test_list_orders_limit_clamped_high(cfg, audit, signing_env, tmp_path):
	cfg2 = cfg.model_copy(update={"audit_log_path": tmp_path / "a.jsonl"})
	captured: list[httpx.Request] = []

	def handler(request: httpx.Request) -> httpx.Response:
		captured.append(request)
		return httpx.Response(200, json={"orders": [], "cursor": ""})

	c = make_mock_client(cfg2, AuditLogger(cfg2.audit_log_path), httpx.MockTransport(handler))
	await c.list_orders(limit=10_000_000)

	q = parse_qs(urlsplit(str(captured[0].url)).query)
	assert q["limit"] == ["1000"]  # clamped down to Kalshi documented max
