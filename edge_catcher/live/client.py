"""Kalshi REST order placement client — Python API."""

from __future__ import annotations
import asyncio
import functools
import logging
import time
import uuid
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN, ROUND_HALF_UP
from typing import Any
from urllib.parse import urlencode

import httpx

from edge_catcher.adapters.kalshi.auth import (
	KALSHI_LIVE_KEY_ID_ENV,
	KALSHI_LIVE_PRIVATE_KEY_ENV,
	make_auth_headers,
)
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
from edge_catcher.live.venue import (
	Balance,
	CancelResult,
	Order,
	OrderAction,
	OrderRequest,
	OrderSide,
	Position,
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

def _fp_to_int(value: object) -> int:
	"""Parse a Kalshi fixed-point count STRING (e.g. ``"6.00"``) to ``int``.

	Kalshi reports order counts as fixed-point decimal strings. Phase-1 binary
	contracts are whole numbers; round half-up defensively. Tolerates ``None``
	and numeric inputs; returns 0 on anything unparseable.
	"""
	if value is None:
		return 0
	try:
		return int(Decimal(str(value)).to_integral_value(rounding=ROUND_HALF_UP))
	except (InvalidOperation, ValueError):
		return 0


def _dollars_to_cents(value: object) -> int:
	"""Parse a Kalshi dollar STRING (e.g. ``"0.1700"``) to integer cents.

	Returns 0 when absent/unparseable (caller decides on a fallback).
	"""
	if value is None:
		return 0
	try:
		return int((Decimal(str(value)) * 100).to_integral_value(rounding=ROUND_HALF_UP))
	except (InvalidOperation, ValueError):
		return 0


def _avg_fill_cents(cost_dollars: object, fill_count: int) -> int:
	"""Volume-weighted average fill price in cents.

	Kalshi's create-order response carries NO per-fill array — only the
	aggregate ``taker_fill_cost_dollars`` (cost of the taker fills on the
	bought side). The blended cost basis is therefore ``cost / fill_count``.
	Returns 0 when nothing filled or the cost is unavailable, matching the
	``blended_price_cents`` 0-sentinel convention downstream code expects.

	Rounds HALF-EVEN to match ``fill_math.blended_price_cents`` — the single
	source of truth the paper/replay per-fill path uses (via Python ``round``).
	This aggregate path and that per-fill path therefore agree byte-exact,
	including at a .5¢ VWAP midpoint where ROUND_HALF_UP would diverge by 1¢ and
	break replay-live parity.
	"""
	if fill_count <= 0 or cost_dollars is None:
		return 0
	try:
		cost_cents = Decimal(str(cost_dollars)) * 100
		return int((cost_cents / fill_count).to_integral_value(rounding=ROUND_HALF_EVEN))
	except (InvalidOperation, ValueError, ZeroDivisionError):
		return 0


class KalshiOrderClient:
	"""Asynchronous order client. Use one instance per process lifetime.

	Async by design — the engine signal-flow path awaits ``executor.place(...)``
	for both paper and live executors (sub-project D's LiveExecutor wraps this
	client). HTTP I/O uses ``httpx.AsyncClient``; retry backoff uses
	``asyncio.sleep`` so the surrounding event loop is never blocked.

	Thread-safety: ``httpx.AsyncClient`` is task-safe within a single event
	loop; the audit logger is locked. The engine in sub-project E creates a
	single client shared across the loop.

	This is the Kalshi implementation of the venue-neutral
	:class:`~edge_catcher.live.venue.LiveVenueClient` contract — the engine's
	live layer (executor, reconciler) depends on that Protocol, not this
	concrete class, so a second venue is added without touching them.
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

	async def market_meta(self, ticker: str) -> dict:
		"""Public market metadata (``status`` / ``result`` / ``expiration_time``)
		for a ticker, via ``GET /markets/{ticker}``.

		Reuses the engine's REST market-meta fetcher — the SAME parser the
		settlement poller's ``check_market_result`` is built on — so B's startup
		reconcile can tell a SETTLED-but-purged position (leave 'open' for the
		settlement poller) from a genuine truth-loss (mark lost_truth) for a
		ticker absent from ``positions()`` (C2). Market data is public, so this
		is an UNSIGNED GET on the shared httpx client (no order auth). The lazy
		import avoids a client↔engine import cycle at module load.
		"""
		from edge_catcher.engine.recovery import fetch_market_meta
		return await fetch_market_meta(self._http, ticker)

	async def list_orders(
		self,
		*,
		status: str | None = None,
		limit: int = 200,
		cursor: str | None = None,
		min_ts: int | None = None,
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

		**Recency bound and the single-page assumption.** The result is one
		page only and is server-side bounded by recency *only when* ``min_ts``
		(Unix seconds; Kalshi ``GET /portfolio/orders`` accepts it exactly as
		``GetTrades`` does — see ``adapters/kalshi/adapter.py``) is supplied.
		Without ``min_ts`` this method assumes Kalshi returns orders
		newest-first and that the recent working set fits within ``limit``.
		If that ordering assumption ever breaks, an unbounded scan can have
		>``limit`` orders and a genuine ``pending`` row's matching (possibly
		*filled*) Kalshi order falls off page 1 — the reconciler then finds
		no match and, after TTL, marks it ``rejected_post_hoc`` (stranded
		real-money position + phantom rejection). Therefore **4.B's startup
		reconcile MUST pass ``min_ts``** (bounded to e.g. "since now − the
		reconcile lookback"); the low-volume 30s phantom-pending poller MAY
		rely on the default (its working set is small by construction). This
		mirrors the documented-assumption convention at
		``adapters/kalshi/adapter.py`` (newest-first NOTE on ``GetTrades``).

		``status`` is an optional server-side filter passed through verbatim
		(Kalshi ``GET /portfolio/orders`` values: ``resting`` / ``executed``
		/ ``canceled``; the order lifecycle additionally surfaces ``pending``
		/ ``rejected`` in element bodies, which B's decision matrix keys off
		``Order.status`` for). Omitted by default so B's startup/reconnect
		reconcile sees active *and* recently-completed orders in one call.

		``limit`` is clamped to ``[1, 1000]`` (Kalshi documented max page
		size) so a typo'd value in a caller cannot silently shrink or blow
		the reconciliation window.

		Returns parsed :class:`Order` objects (empty list when none). Each
		carries ``client_order_id`` / ``order_id`` / ``status`` /
		``filled_count`` — sufficient for B's reconciliation decision matrix.
		The full Kalshi element is preserved on ``Order.raw`` for
		forward-compat. A single malformed element is skipped (logged at
		WARNING) rather than aborting the whole batch — an all-or-nothing
		parse would propagate out of B's ``_reconcile_pending_batch`` and
		skip the entire reconcile cycle, pushing genuine pending rows toward
		their TTL. 4xx/5xx flow through the shared ``_request`` dispatch
		(generic :class:`KalshiAPIError`), identical to ``status()`` /
		``positions()`` — no bespoke error handling.
		"""
		# Clamp before building params: floor 1 (a 0/negative would send an
		# empty/invalid window), ceil 1000 (Kalshi documented max page size —
		# a larger value is silently capped server-side anyway; clamp here so
		# the signed/sent string reflects the real window).
		limit = max(1, min(limit, 1000))
		# Query params are threaded through _request so they are baked into
		# the single signed-and-sent path string (Kalshi RSA signing strips
		# the query before signing; the module invariant is that the string
		# handed to make_auth_headers is byte-identical to the wire path —
		# see _request). None values are dropped (never sent as empty).
		params: dict[str, str | int] = {"limit": limit}
		if status is not None:
			params["status"] = status
		if cursor is not None:
			params["cursor"] = cursor
		if min_ts is not None:
			params["min_ts"] = min_ts
		response = await self._get("/portfolio/orders", op="list_orders", params=params)
		raw = response.get("orders", [])
		# Defensive per-element parse: _parse_order is defensive-by-design for
		# missing FIELDS, but a non-dict element makes its `.get` raise. This
		# primitive is B's sole safety net for pending rows with
		# kalshi_order_id IS NULL; one bad element must not strand every
		# phantom-pending row that cycle. Skip-and-log, return the rest.
		orders: list[Order] = []
		for element in raw:
			try:
				orders.append(self._parse_order(element))
			except (AttributeError, TypeError, ValueError, KeyError) as exc:
				log.warning(
					"list_orders: skipping malformed order element "
					"(%s: %s) — element=%.200r",
					type(exc).__name__,
					exc,
					element,
				)
		return orders

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
				# by env-var name. The env-var NAMES are the canonical auth
				# constants — the SAME objects the §2 coherence gate
				# (engine.py) checks — so signer & gate cannot drift apart
				# (single source; spec Obl-3 / Minor#1).
				headers = make_auth_headers(
					method,
					full_path,
					key_id_env=KALSHI_LIVE_KEY_ID_ENV,
					private_key_env=KALSHI_LIVE_PRIVATE_KEY_ENV,
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
		"""Map a Kalshi order object (the inner ``"order"`` dict, also each
		``list_orders`` element) to an :class:`Order`.

		Kalshi's real wire shape (NOT the pre-fix assumed shape):
		  * ``fill_count_fp`` / ``initial_count_fp`` — fixed-point count STRINGS.
		  * ``{yes,no}_price_dollars`` — the order's limit price as a dollar
		    STRING; ×100 → cents.
		  * ``taker_fill_cost_dollars`` — aggregate taker fill cost (no per-fill
		    array); the blended average is ``cost / fill_count``.
		  * ``created_time`` — ISO-8601 (not ``created_ts``).
		"""
		side: OrderSide = data.get("side", fallback_request.side if fallback_request else "yes")
		action: OrderAction = data.get("action", fallback_request.action if fallback_request else "buy")

		filled_count = _fp_to_int(data.get("fill_count_fp"))
		# initial_count_fp is the order's original size; the reconciler compares
		# filled_count >= count, so count must be that original size.
		count = _fp_to_int(data.get("initial_count_fp"))

		# The limit price echoes back as {side}_price_dollars. Fall back to the
		# request's limit only when the field is absent (forward-compat / minimal
		# bodies) — never silently zero a real-money cost basis.
		price_field = "yes_price_dollars" if side == "yes" else "no_price_dollars"
		limit_price_cents = _dollars_to_cents(data.get(price_field))
		if limit_price_cents == 0 and fallback_request is not None:
			limit_price_cents = fallback_request.limit_price_cents

		return Order(
			order_id=data.get("order_id", ""),
			ticker=data.get("ticker", fallback_request.ticker if fallback_request else ""),
			side=side,
			action=action,
			count=count,
			limit_price_cents=limit_price_cents,
			time_in_force=data.get("time_in_force", "gtc"),
			status=data.get("status", "pending"),
			filled_count=filled_count,
			avg_fill_price_cents=_avg_fill_cents(data.get("taker_fill_cost_dollars"), filled_count),
			created_ts=data.get("created_time", ""),
			client_order_id=data.get("client_order_id"),
			raw=data,
		)

	def _parse_position(self, data: dict) -> Position:
		"""Map a Kalshi ``market_positions`` element to a :class:`Position`.

		Real wire shape: ``position_fp`` is a SIGNED fixed-point count STRING
		(``"10.00"`` long-yes, ``"-3.00"`` long-no); ``market_exposure_dollars``
		is the cost basis of the open position as a dollar STRING. The average
		entry price is therefore ``market_exposure / |position|``. (NOT the
		pre-fix ``position`` / ``average_position_cost`` integer fields, which
		Kalshi does not return — they parsed to 0 and silently mis-recovered any
		real orphan position as flat.)
		"""
		position = _fp_to_int(data.get("position_fp"))
		count = abs(position)
		side: OrderSide = "yes" if position >= 0 else "no"
		exposure_cents = _dollars_to_cents(data.get("market_exposure_dollars"))
		average_price_cents = round(exposure_cents / count) if count > 0 else 0
		return Position(
			ticker=data.get("ticker", ""),
			side=side,
			count=count,
			average_price_cents=average_price_cents,
			raw=data,
		)
