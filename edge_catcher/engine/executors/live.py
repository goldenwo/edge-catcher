"""Live-mode Executor.

Wraps :class:`KalshiOrderClient` and translates Kalshi REST responses into the
engine's :class:`OrderResult`. Conservative error-mapping policy: every failure
mode produces a *defined* :class:`OrderResult` — never propagates out of
``place()`` — so the dispatch layer can route uniformly to ``filled`` /
``pending`` / ``rejected`` branches and B's reconciler can resolve the true
Kalshi-side state for any ``pending`` row.

Funds-at-risk lens:

* Network failure → ``pending`` + ``order_id=None``. Kalshi-side state is
  UNKNOWN; B reconciles by ``client_order_id``.
* Kalshi 4xx → ``rejected`` (authoritative — don't retry).
* Kalshi 5xx / unmapped exception → ``pending`` + ``order_id=None`` (treated
  identically to NetworkError; we never lie about a placement we can't confirm).
* Order placed but malformed fills array → ``pending`` + ``order_id=<known>``.
  B reconciles the true blended price via ``order_id``.
* IOC zero-fill → ``rejected`` with ``ioc_zero_fill`` (no fill = no exposure).
"""

from __future__ import annotations

import logging
from typing import cast

from edge_catcher.engine.executor import OrderRequest, OrderResult
from edge_catcher.live.client import (
	KalshiOrderClient,
	Order,
	OrderRequest as KalshiOrderRequest,
)
from edge_catcher.live.errors import (
	CapExceededError,
	KalshiAPIError,
	NetworkError,
	OrderRejected,
)

from edge_catcher.engine.fill_math import FillEvent, blended_price_cents


log = logging.getLogger(__name__)


class LiveExecutor:
	"""Engine-facing live executor.

	Holds a single :class:`KalshiOrderClient` for the process lifetime (see
	the client's docstring re: per-process semantics). ``place()`` is async
	because the underlying client is async-native; dispatch awaits the call
	from its async context.
	"""

	def __init__(self, client: KalshiOrderClient) -> None:
		self._client = client

	async def place(self, req: OrderRequest) -> OrderResult:
		"""Place a Kalshi order and translate the response.

		Every exception path returns a defined :class:`OrderResult` — never
		re-raises — so dispatch's status-discriminator can route uniformly.
		"""
		try:
			order = await self._client.place(_to_kalshi_request(req))
		except OrderRejected as e:
			# Kalshi authoritatively rejected (4xx). Don't retry; no exposure.
			return _make_rejected(req, reason=f"kalshi_4xx:{e.status}")
		except CapExceededError:
			# C's sizing should never produce sizes that exceed
			# ABSOLUTE_MAX_ORDER_DOLLARS. Defense in depth: if it ever does,
			# don't place; surface the inconsistency.
			return _make_rejected(req, reason="absolute_max_exceeded")
		except NetworkError as e:
			# Kalshi-side state unknown. pending + order_id=None — B reconciles
			# by client_order_id on the next poll/WS event.
			return _make_pending_unknown(req, reason=f"kalshi_unreachable:{e}")
		except KalshiAPIError as e:
			# 5xx after retries exhausted. Same semantics as NetworkError —
			# we don't know whether Kalshi accepted the order. B reconciles.
			return _make_pending_unknown(
				req, reason=f"kalshi_5xx_unknown_state:{e.status}"
			)
		return _translate_order(order, req)


def _to_kalshi_request(req: OrderRequest) -> KalshiOrderRequest:
	"""Translate engine :class:`OrderRequest` → Kalshi wire request.

	**INVARIANT:** the ``action`` kwarg MUST be ``req.action`` — never a
	hardcoded literal. A regression-guard test (``test_to_kalshi_request_does_not_hardcode_action``)
	asserts this via AST inspection. Reason: a sign bug in dispatch (Buy vs
	Sell flipped at signal generation) must NOT be papered over here. The
	round-1 caught bug silently inverted sells to buys (funds-at-risk).
	"""
	return KalshiOrderRequest(
		ticker=req.ticker,
		action=req.action,
		side=req.side,
		count=req.size_contracts,
		limit_price_cents=req.limit_price_cents,
		time_in_force="ioc",
		client_order_id=req.client_order_id,
	)


def _translate_order(order: Order, req: OrderRequest) -> OrderResult:
	"""Map a Kalshi-returned :class:`Order` to engine :class:`OrderResult`.

	Branches:
	* ``filled_count == 0`` → rejected (``ioc_zero_fill``).
	* ``filled_count > 0`` but ``raw["fills"]`` missing/malformed → pending
	  with ``order_id`` preserved (B reconciles true blended price).
	* Happy path: blended price from per-fill array; status=filled.
	"""
	# Zero fill — IOC didn't get any liquidity at our limit. Reject.
	if order.filled_count == 0:
		return OrderResult(
			status="rejected",
			intended_size=req.size_contracts,
			filled_size=0,
			blended_entry_cents=0,
			fill_pct=0.0,
			slippage_cents=0,
			rejection_reason="ioc_zero_fill",
			order_id=None,
		)

	# Parse the per-fill array from order.raw — Kalshi returns it as
	# raw["fills"]: [{"price": int, "size": int}, ...] when fills exist.
	# A missing/malformed shape falls through to the pending branch below;
	# B will reconcile by order_id. We cast at the boundary because the
	# Kalshi wire shape is dynamically typed but we've validated it has the
	# FillEvent shape (price+size keys) before passing to fill_math.
	fills: list[FillEvent] = []
	try:
		raw_fills = order.raw.get("fills") if isinstance(order.raw, dict) else None
		if isinstance(raw_fills, list):
			# Light-touch validation: every entry must have integer price+size.
			# Malformed entries (missing keys, wrong types) demote to pending.
			if all(
				isinstance(f, dict) and "price" in f and "size" in f for f in raw_fills
			):
				fills = cast(list[FillEvent], raw_fills)
	except (AttributeError, TypeError):
		fills = []

	if not fills:
		# Kalshi reported a fill count but didn't give us a usable fills array.
		# Under the zero-error lens we DO NOT pretend we know the price — a
		# silent "perfect fill" lie masks data-quality issues from B's
		# reconciliation and F's slippage chart. Mark pending so B re-fetches
		# the order by order_id and reconciles the true blended price.
		log.warning(
			"Kalshi order %s has filled_count=%d but no/malformed fills "
			"array — returning pending so B reconciles the true blended price",
			order.order_id,
			order.filled_count,
		)
		return OrderResult(
			status="pending",
			intended_size=req.size_contracts,
			filled_size=order.filled_count,
			blended_entry_cents=0,
			fill_pct=order.filled_count / req.size_contracts,
			slippage_cents=0,
			rejection_reason="kalshi_malformed_fills",
			order_id=order.order_id or None,
		)

	blended = blended_price_cents(fills)
	fill_pct = order.filled_count / req.size_contracts
	# Signed slippage: for a buy, blended > limit means we paid more than
	# the limit (Kalshi's actual matched price). For a sell, blended < limit
	# means we accepted less. F's slippage-distribution chart reads this
	# directly; sign carries direction.
	slippage = blended - req.limit_price_cents
	return OrderResult(
		status="filled",
		intended_size=req.size_contracts,
		filled_size=order.filled_count,
		blended_entry_cents=blended,
		fill_pct=fill_pct,
		slippage_cents=slippage,
		order_id=order.order_id or None,
	)


def _make_rejected(req: OrderRequest, *, reason: str) -> OrderResult:
	"""Build a rejected :class:`OrderResult` with the standard zero-fill shape."""
	return OrderResult(
		status="rejected",
		intended_size=req.size_contracts,
		filled_size=0,
		blended_entry_cents=0,
		fill_pct=0.0,
		slippage_cents=0,
		rejection_reason=reason,
		order_id=None,
	)


def _make_pending_unknown(req: OrderRequest, *, reason: str) -> OrderResult:
	"""Build a pending :class:`OrderResult` for the unknown-state Kalshi paths.

	``order_id`` is ``None`` because we never received a successful 2xx — B
	must dedupe via the natural Signal key, not ``client_order_id`` (D
	guarantees per-attempt uniqueness; B owns dedup).
	"""
	return OrderResult(
		status="pending",
		intended_size=req.size_contracts,
		filled_size=0,
		blended_entry_cents=0,
		fill_pct=0.0,
		slippage_cents=0,
		rejection_reason=reason,
		order_id=None,
	)
