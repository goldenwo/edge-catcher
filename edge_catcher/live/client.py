"""Kalshi REST order placement client — Python API."""

from __future__ import annotations
import asyncio
import functools
import logging
import re
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Literal
from urllib.parse import urlencode

import httpx

from edge_catcher.adapters.kalshi.auth import make_auth_headers
from edge_catcher.live.audit import AuditLogger, AuditEvent
from edge_catcher.live.config import (
	LiveConfig,
	ABSOLUTE_MAX_ORDER_DOLLARS,
)
from edge_catcher.live.errors import (
	CapExceededError,
	KalshiAPIError,
	NetworkError,
	OrderAlreadyFinal,
	OrderRejected,
)

log = logging.getLogger(__name__)

# Kalshi REST API path prefix. Owned by this module so signing uses the
# exact path sent on the wire (httpx base_url + leading-/ behaviour is
# version-dependent — keep base_url host-only and prepend here).
_KALSHI_REST_PREFIX = "/trade-api/v2"

# Kalshi's create-order API expects verbose underscored time-in-force names
# (`good_till_canceled`, `immediate_or_cancel`, `fill_or_kill`). Our public
# OrderRequest API uses short Pythonic names (`gtc`/`ioc`/`fok`) for CLI
# ergonomics; we translate at the wire boundary in _build_place_body.
_TIF_TO_KALSHI: dict[str, str] = {
	"gtc": "good_till_canceled",
	"ioc": "immediate_or_cancel",
	"fok": "fill_or_kill",
}

OrderAction = Literal["buy", "sell"]
OrderSide = Literal["yes", "no"]
OrderType = Literal["limit"]  # 'market' explicitly excluded — see Q9 in design notes
TimeInForce = Literal["gtc", "ioc", "fok"]

# client_order_id is forwarded to Kalshi as the idempotency key. Restrict to
# URL-safe alphanumerics + ``-_`` so the value survives JSON encoding, log
# rendering, and any downstream system that consumes the audit trail without
# ambiguity. 80 chars covers the D-spec L214 worst-case format
# ``{strategy}-{ticker}-{ms_ts}-{uuid8}``. The canonical producer lives at
# ``edge_catcher/engine/execution.py:_make_client_order_id`` which charset-
# and length-validates against this same regex before assembly, so a 4xx
# from this layer would indicate a strategy/ticker that bypassed the builder.
_CLIENT_ORDER_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,80}$")


@dataclass
class OrderRequest:
	"""Caller-side typed input for KalshiOrderClient.place()."""

	ticker: str
	action: OrderAction
	side: OrderSide
	count: int
	limit_price_cents: int  # always required (no market orders)
	time_in_force: TimeInForce = "gtc"
	# client_order_id is auto-generated on place() if absent.
	client_order_id: str | None = None

	def __post_init__(self) -> None:
		if self.client_order_id is not None and not _CLIENT_ORDER_ID_PATTERN.match(self.client_order_id):
			raise ValueError(
				f"client_order_id must match {_CLIENT_ORDER_ID_PATTERN.pattern}, "
				f"got {self.client_order_id!r}"
			)

	@property
	def exposure_dollars(self) -> float:
		"""Maximum cost of this order in dollars (count × limit_price / 100)."""
		return self.count * self.limit_price_cents / 100.0


@dataclass
class Order:
	"""Kalshi order as returned by POST /orders or GET /orders/{id}."""

	order_id: str
	ticker: str
	side: OrderSide
	action: OrderAction
	count: int
	limit_price_cents: int
	time_in_force: TimeInForce
	status: str  # Kalshi values: 'pending' / 'resting' / 'executed' / 'canceled' / 'rejected'
	filled_count: int = 0
	created_ts: str = ""  # ISO-8601 from Kalshi
	client_order_id: str | None = None
	raw: dict = field(default_factory=dict)  # full API response, for forward-compat


@dataclass
class CancelResult:
	order_id: str
	status: str  # 'canceled' / already-final
	raw: dict = field(default_factory=dict)


@dataclass
class Balance:
	balance_cents: int  # available cash, in cents
	raw: dict = field(default_factory=dict)


@dataclass
class Position:
	ticker: str
	side: OrderSide
	count: int
	average_price_cents: int
	raw: dict = field(default_factory=dict)


class KalshiOrderClient:
	"""Asynchronous order client. Use one instance per process lifetime.

	Async by design — the engine signal-flow path awaits ``executor.place(...)``
	for both paper and live executors (sub-project D's LiveExecutor wraps this
	client). HTTP I/O uses ``httpx.AsyncClient``; retry backoff uses
	``asyncio.sleep`` so the surrounding event loop is never blocked.

	Thread-safety: ``httpx.AsyncClient`` is task-safe within a single event
	loop; the audit logger is locked. The engine in sub-project E creates a
	single client shared across the loop.
	"""

	def __init__(self, config: LiveConfig, audit: AuditLogger) -> None:
		self._config = config
		self._audit = audit
		self._http = httpx.AsyncClient(
			base_url=config.kalshi_rest_base,
			timeout=config.http_timeout_seconds,
			headers={"Accept": "application/json"},
		)

	async def close(self) -> None:
		await self._http.aclose()

	async def __aenter__(self) -> "KalshiOrderClient":
		return self

	async def __aexit__(self, *args: object) -> None:
		await self.close()

	# ------------------------------------------------------------------
	# Public API
	# ------------------------------------------------------------------

	async def place(self, req: OrderRequest) -> Order:
		"""Place a Kalshi limit order. Enforces ABSOLUTE_MAX_ORDER_DOLLARS hard cap.

		The CLI applies a separate, lower cap before this is reached; see cli.py.
		"""
		if req.exposure_dollars > ABSOLUTE_MAX_ORDER_DOLLARS:
			raise CapExceededError(
				req.exposure_dollars, ABSOLUTE_MAX_ORDER_DOLLARS, "ABSOLUTE_MAX"
			)
		if req.client_order_id is None:
			req.client_order_id = str(uuid.uuid4())

		body = self._build_place_body(req)
		path = "/portfolio/orders"
		response = await self._post(path, body, op="place", client_order_id=req.client_order_id)
		# OrderRejected on 4xx is mapped inside _request via op-specific dispatch.
		# Kalshi returns {"order": {...}} on success.
		order_json = response.get("order", response)
		return self._parse_order(order_json, fallback_request=req)

	async def cancel(self, order_id: str) -> CancelResult:
		path = f"/portfolio/orders/{order_id}"
		response = await self._delete(path, op="cancel")
		return CancelResult(
			order_id=order_id,
			status=(response.get("order", {}) or {}).get("status", "canceled"),
			raw=response,
		)

	async def status(self, order_id: str) -> Order:
		path = f"/portfolio/orders/{order_id}"
		response = await self._get(path, op="status")
		return self._parse_order(response.get("order", response))

	async def balance(self) -> Balance:
		path = "/portfolio/balance"
		response = await self._get(path, op="balance")
		return Balance(
			balance_cents=int(response.get("balance", 0)),
			raw=response,
		)

	async def positions(self) -> list[Position]:
		path = "/portfolio/positions"
		response = await self._get(path, op="positions")
		raw = response.get("market_positions", [])
		return [self._parse_position(p) for p in raw]

	async def list_orders(
		self,
		*,
		status: str | None = None,
		limit: int = 200,
		cursor: str | None = None,
	) -> list[Order]:
		"""List portfolio orders via ``GET /portfolio/orders``.

		Single bounded page per invocation — this method issues exactly one
		REST call and does NOT internally follow the pagination cursor.
		Sub-project B's phantom-pending poller relies on "ONE call to
		``list_orders()`` per cycle" (it then matches locally by
		``client_order_id``); a generous default ``limit`` covers the
		recent-orders working set without forcing pagination. Callers that
		genuinely need a later page pass the ``cursor`` token Kalshi returns
		in the response envelope's ``cursor`` field back in via the
		``cursor`` argument; advancing pages is the caller's responsibility,
		kept out of this primitive to keep REST traffic bounded.

		``status`` is an optional server-side filter (Kalshi values:
		``resting`` / ``executed`` / ``canceled``). Omitted by default so
		B's startup/reconnect reconcile sees active *and* recently-completed
		orders in one call.

		Returns parsed :class:`Order` objects (empty list when none). Each
		carries ``client_order_id`` / ``order_id`` / ``status`` /
		``filled_count`` — sufficient for B's reconciliation decision matrix.
		The full Kalshi element is preserved on ``Order.raw`` for
		forward-compat. 4xx/5xx flow through the shared ``_request`` dispatch
		(generic :class:`KalshiAPIError`), identical to ``status()`` /
		``positions()`` — no bespoke error handling.
		"""
		# Query params are threaded through _request so they are baked into
		# the single signed-and-sent path string (Kalshi RSA signing strips
		# the query before signing, but the module's invariant is that the
		# string handed to make_auth_headers is byte-identical to the wire
		# path — see _request). None values are dropped (not sent as empty).
		params: dict[str, str | int] = {"limit": limit}
		if status is not None:
			params["status"] = status
		if cursor is not None:
			params["cursor"] = cursor
		response = await self._get("/portfolio/orders", op="list_orders", params=params)
		raw = response.get("orders", [])
		return [self._parse_order(o) for o in raw]

	# ------------------------------------------------------------------
	# Internal request layer
	# ------------------------------------------------------------------

	async def _post(self, path: str, body: dict, op: str, client_order_id: str | None) -> dict:
		return await self._request("POST", path, op=op, json=body, client_order_id=client_order_id)

	async def _get(
		self,
		path: str,
		op: str,
		*,
		params: dict[str, str | int] | None = None,
	) -> dict:
		return await self._request("GET", path, op=op, params=params)

	async def _delete(self, path: str, op: str) -> dict:
		return await self._request("DELETE", path, op=op)

	async def _request(
		self,
		method: str,
		path: str,
		*,
		op: str,
		json: dict | None = None,
		client_order_id: str | None = None,
		params: dict[str, str | int] | None = None,
	) -> dict:
		# Build the full URL path explicitly so signing and sending use the
		# exact same string. Keep base_url host-only; prepend the prefix here.
		#
		# Query params (GET filters/pagination) are encoded INTO this single
		# string before make_auth_headers / the wire request — never passed
		# to httpx as a separate `params=` dict. Kalshi RSA signing strips
		# the query before signing (adapters.kalshi.auth), but the module
		# invariant is stronger: the string handed to make_auth_headers is
		# byte-identical to the path+query httpx puts on the wire, so the
		# audit trail and the "signed == sent" regression guard hold. A
		# separate httpx `params=` would be appended AFTER signing and break
		# that invariant. `urlencode` gives deterministic, stable ordering
		# (dict insertion order in py3.7+); None-valued params are dropped by
		# the callers, so nothing is sent as an empty value.
		full_path = _KALSHI_REST_PREFIX + path
		if params:
			full_path = f"{full_path}?{urlencode(params)}"
		retries = 0
		started = time.monotonic()
		last_error: str | None = None
		response_status: int | None = None
		response_body: dict | None = None

		while True:
			try:
				# Live trader uses a separate trade-scope Kalshi key so a leak
				# of the paper trader's read-only key (KALSHI_KEY_ID) cannot
				# place orders. Both keys live in `.env`; auth.py reads them
				# by env-var name.
				headers = make_auth_headers(
					method,
					full_path,
					key_id_env="KALSHI_LIVE_KEY_ID",
					private_key_env="KALSHI_LIVE_PRIVATE_KEY",
				)
				resp = await self._http.request(method, full_path, json=json, headers=headers)
				response_status = resp.status_code
				try:
					response_body = resp.json() if resp.content else {}
				except ValueError:
					response_body = {"_raw_text": resp.text[:500]}

				if 200 <= resp.status_code < 300:
					# Audit-write failures must NOT mask a successful order placement
					# — the venue already accepted the order. Sub-project E's engine
					# runs on a persistent event loop where a propagated audit error
					# would crash the dispatch coroutine and strand the live order
					# with no record. Log + swallow; compliance/ops sees the ERROR
					# entry without losing the response at the caller.
					try:
						await self._write_audit_async(
							op=op,
							method=method,
							path=full_path,
							client_order_id=client_order_id,
							request=json or {},
							response_status=response_status,
							response_body=response_body,
							started=started,
							outcome="success",
							error=None,
							retries=retries,
						)
					except Exception:
						log.exception(
							"audit_write_failed_after_success op=%s client_order_id=%s",
							op,
							client_order_id,
						)
					return response_body or {}

				if resp.status_code == 429 or resp.status_code >= 500:
					if retries >= self._config.max_retries:
						last_error = f"HTTP {resp.status_code} after {retries} retries"
						break
					backoff = min(60.0, (2 ** retries) + 0.1 * retries)
					await asyncio.sleep(backoff)
					retries += 1
					continue

				# 4xx (non-429): fail loud, surface Kalshi error verbatim.
				# Audit-write failure here must not mask the typed Kalshi
				# exception below — callers (and B's reconciler) key off the
				# exception class to decide retry/idempotency behaviour.
				try:
					await self._write_audit_async(
						op=op,
						method=method,
						path=full_path,
						client_order_id=client_order_id,
						request=json or {},
						response_status=response_status,
						response_body=response_body,
						started=started,
						outcome="http_error",
						error=resp.text[:500],
						retries=retries,
					)
				except Exception:
					log.exception(
						"audit_write_failed_after_http_error op=%s status=%s client_order_id=%s",
						op,
						response_status,
						client_order_id,
					)
				# Op-specific 4xx mapping. B's reconciliation loop relies on
				# OrderAlreadyFinal being distinguishable from auth/validation
				# errors so it can treat repeated cancels as idempotent no-ops.
				if op == "place":
					raise OrderRejected(resp.status_code, resp.text, full_path)
				if op == "cancel" and resp.status_code in (404, 409):
					raise OrderAlreadyFinal(resp.status_code, resp.text, full_path)
				raise KalshiAPIError(resp.status_code, resp.text, full_path)

			except (httpx.TimeoutException, httpx.NetworkError) as e:
				if retries >= self._config.max_retries:
					last_error = f"{type(e).__name__}: {e}"
					break
				await asyncio.sleep(min(60.0, (2 ** retries) + 0.1 * retries))
				retries += 1

		# Network/timeout retries exhausted — same fault-isolation contract as
		# the 4xx path: audit failure must not mask the NetworkError.
		try:
			await self._write_audit_async(
				op=op,
				method=method,
				path=full_path,
				client_order_id=client_order_id,
				request=json or {},
				response_status=response_status,
				response_body=response_body,
				started=started,
				outcome="network_error" if response_status is None else "http_error",
				error=last_error,
				retries=retries,
			)
		except Exception:
			log.exception(
				"audit_write_failed_after_network_error op=%s client_order_id=%s",
				op,
				client_order_id,
			)
		raise NetworkError(last_error or "unknown")

	def _write_audit(
		self,
		*,
		op: str,
		method: str,
		path: str,
		client_order_id: str | None,
		request: dict,
		response_status: int | None,
		response_body: dict | None,
		started: float,
		outcome: str,
		error: str | None,
		retries: int,
	) -> None:
		self._audit.write(AuditEvent(
			ts=AuditLogger.now_iso(),
			op=op,
			method=method,
			path=path,
			client_order_id=client_order_id,
			request=request,
			response_status=response_status,
			response_body=response_body,
			duration_ms=(time.monotonic() - started) * 1000.0,
			outcome=outcome,
			error=error,
			retries=retries,
		))

	async def _write_audit_async(self, **kwargs: Any) -> None:
		# AuditLogger.write does sync open/write/close under a threading.Lock.
		# Off-load to the default thread pool so the engine event loop (which
		# also services WS receives and the reconciler poll in sub-project E)
		# is never blocked by SD-card write stalls during a live order.
		loop = asyncio.get_running_loop()
		await loop.run_in_executor(None, functools.partial(self._write_audit, **kwargs))

	# ------------------------------------------------------------------
	# Parsing helpers
	# ------------------------------------------------------------------

	def _build_place_body(self, req: OrderRequest) -> dict:
		# Kalshi's CreateOrderRequest schema does NOT include a `type` field —
		# the presence of yes_price/no_price implicitly indicates a limit order.
		# Sending an unexpected `type` causes Kalshi to reject with the
		# misleadingly-named `fill_or_kill_insufficient_resting_volume` error.
		# (Discovered during integration testing — see PR #24's journey.)
		body: dict = {
			"action": req.action,
			"count": req.count,
			"side": req.side,
			"ticker": req.ticker,
			"time_in_force": _TIF_TO_KALSHI[req.time_in_force],
			"client_order_id": req.client_order_id,
		}
		if req.side == "yes":
			body["yes_price"] = req.limit_price_cents
		else:
			body["no_price"] = req.limit_price_cents
		# `buy_max_cost` is intentionally NOT sent. Kalshi treats it as a hard
		# total-cost ceiling and rejects with the misleadingly-named
		# `fill_or_kill_insufficient_resting_volume` error if our value is even
		# 1¢ below their internal calculation (rounding/fee drift). Cap safety
		# is already provided by two other layers:
		#   1. ABSOLUTE_MAX_ORDER_DOLLARS = $50, hardcoded in client.place()
		#   2. cli_max_order_dollars, enforced in cli._do_place()
		# Sub-project D (execution policy) can revisit buy_max_cost as a
		# Kalshi-side third layer if the rounding rule is ever published.
		return body

	def _parse_order(self, data: dict, fallback_request: OrderRequest | None = None) -> Order:
		side: OrderSide = data.get("side", fallback_request.side if fallback_request else "yes")
		action: OrderAction = data.get("action", fallback_request.action if fallback_request else "buy")
		price_cents = (
			data.get("yes_price") if side == "yes" else data.get("no_price")
		) or 0
		return Order(
			order_id=data.get("order_id", ""),
			ticker=data.get("ticker", fallback_request.ticker if fallback_request else ""),
			side=side,
			action=action,
			count=int(data.get("count", 0)),
			limit_price_cents=int(price_cents),
			time_in_force=data.get("time_in_force", "gtc"),
			status=data.get("status", "pending"),
			filled_count=int(data.get("filled_count", 0)),
			created_ts=data.get("created_ts", ""),
			client_order_id=data.get("client_order_id"),
			raw=data,
		)

	def _parse_position(self, data: dict) -> Position:
		side: OrderSide = "yes" if int(data.get("position", 0)) >= 0 else "no"
		return Position(
			ticker=data.get("ticker", ""),
			side=side,
			count=abs(int(data.get("position", 0))),
			average_price_cents=int(data.get("average_position_cost", 0)),
			raw=data,
		)
