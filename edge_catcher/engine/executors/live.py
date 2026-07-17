"""Live-mode Executor.

Wraps a :class:`~edge_catcher.live.venue.LiveVenueClient` (Kalshi today) and
translates its order responses into the engine's :class:`OrderResult`.
Conservative error-mapping policy: every failure
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
* Order filled but Kalshi reports no usable fill cost → ``pending`` +
  ``order_id=<known>``. B reconciles the true blended basis via ``order_id``.
* IOC zero-fill → ``rejected`` with ``ioc_zero_fill`` (no fill = no exposure).
"""

from __future__ import annotations

import asyncio
import logging

from edge_catcher.engine.executor import OrderRequest, OrderResult
from edge_catcher.live.venue import (
	LiveVenueClient,
	Order,
	OrderRequest as KalshiOrderRequest,
)
from edge_catcher.live.errors import (
	CapExceededError,
	KalshiAPIError,
	NetworkError,
	OrderRejected,
)

from edge_catcher.engine.fill_math import signed_slippage_cents


log = logging.getLogger(__name__)


class LiveExecutor:
	"""Engine-facing live executor.

	Holds a single :class:`~edge_catcher.live.venue.LiveVenueClient` (Kalshi
	today) for the process lifetime (see the client's docstring re: per-process
	semantics). ``place()`` is async because the underlying client is
	async-native; dispatch awaits the call from its async context. Typed to the
	venue Protocol — NOT the concrete client — so a second venue needs no change
	here.
	"""

	def __init__(self, client: LiveVenueClient) -> None:
		self._client = client

	async def place(self, req: OrderRequest) -> OrderResult:
		"""Place a Kalshi order and translate the response.

		Every exception path returns a defined :class:`OrderResult` — never
		re-raises — so dispatch's status-discriminator can route uniformly.
		The one exception is ``asyncio.CancelledError``: cooperative cancellation
		must propagate so the engine can shut down cleanly; we treat it as
		an out-of-band control signal, not an order failure.
		"""
		try:
			order = await self._client.place(_to_kalshi_request(req))
		except asyncio.CancelledError:
			# Cooperative cancellation — engine is shutting down. Re-raise
			# WITHOUT writing a pending row: there's no reliable way to know
			# whether Kalshi received the request, but B's startup-reconcile
			# (B-spec L286-L361) will pick up any orphan by client_order_id
			# on next boot via the periodic poll. Swallowing CancelledError
			# here would deadlock the dispatch coroutine on shutdown.
			raise
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
		except Exception as e:
			# Defensive catch-all per the "never re-raises" contract above.
			# Possible causes: future Kalshi client raises an exception type
			# not in edge_catcher.live.errors, AttributeError on a malformed
			# Order object, an OSError that NetworkError didn't wrap, etc.
			# Funds-at-risk semantics: we don't know whether the order
			# reached Kalshi, so route to pending+None (UNKNOWN state) and
			# let B reconcile via client_order_id. Logging includes the
			# exception class so on-call can triage.
			log.exception(
				"LiveExecutor.place: unexpected %s — routing to pending+None "
				"(client_order_id=%s)", type(e).__name__, req.client_order_id,
			)
			return _make_pending_unknown(
				req, reason=f"unexpected_exception:{type(e).__name__}"
			)
		return _translate_order(order, req)


def _to_kalshi_request(req: OrderRequest) -> KalshiOrderRequest:
	"""Translate engine :class:`OrderRequest` → Kalshi wire request.

	**INVARIANT:** the ``action`` kwarg MUST be ``req.action`` — never a
	hardcoded literal. A regression-guard test (``test_to_kalshi_request_does_not_hardcode_action``)
	asserts this via AST inspection. Reason: a sign bug in dispatch (Buy vs
	Sell flipped at signal generation) must NOT be papered over here. The
	round-1 caught bug silently inverted sells to buys (funds-at-risk).

	``time_in_force`` is passed through VERBATIM from ``req.time_in_force``
	(SPEC §4.2). The BUILDER that constructed the OrderRequest — which knows
	entry-vs-exit and taker-vs-maker — sets TIF: the taker builders rely on
	the dataclass default ``"ioc"``; ``build_maker_entry_order`` sets
	``"gtc"`` explicitly. This function no longer infers anything. The old
	action-name-keyed ``buy → ENTRY_TIF / sell → EXIT_TIF`` mapping (and the
	``ENTRY_TIF != EXIT_TIF`` landmine its own docstring flagged per the
	PR #38 pass-3 review, G1) is retired — Phase 2a made TIF intent explicit
	on the request itself, which was that note's prescribed fix.
	"""
	tif = req.time_in_force
	return KalshiOrderRequest(
		ticker=req.ticker,
		action=req.action,
		side=req.side,
		count=req.size_contracts,
		limit_price_cents=req.limit_price_cents,
		time_in_force=tif,
		client_order_id=req.client_order_id,
	)


def _translate_order(order: Order, req: OrderRequest) -> OrderResult:
	"""Map a Kalshi-returned :class:`Order` to engine :class:`OrderResult`.

	Branches:
	* ``req.size_contracts <= 0`` → rejected (``invalid_intended_size``).
	  Defense in depth — D's builders refuse to produce size<=0 OrderRequests,
	  but the catch-all in ``place()`` would mask a downstream
	  ``ZeroDivisionError`` here as ``unexpected_exception:ZeroDivisionError``,
	  hiding the real sizing bug. Loud reject surfaces the upstream defect.
	* ``filled_count == 0`` → rejected (``ioc_zero_fill``).
	* ``filled_count > 0`` but no usable ``avg_fill_price_cents`` → pending
	  with ``order_id`` preserved (B reconciles the true blended basis).
	* Happy path: blended = ``avg_fill_price_cents`` (Kalshi's aggregate taker
	  fill cost / fill count — no per-fill array exists); status=filled.
	"""
	# Defense in depth — size_contracts <= 0 must never reach here from the
	# builders, but the divisions below would mask the bug as a div-by-zero
	# rerouted through the catch-all in place(). Surface loudly instead.
	if req.size_contracts <= 0:
		return _make_rejected(
			req, reason=f"invalid_intended_size:{req.size_contracts}"
		)

	# Zero fill. For an IOC this is terminal — no liquidity at our limit,
	# reject. For a GTC the venue reports as resting (or pending-accepted),
	# zero fill is the SUCCESS case: the order is on the book (SPEC §9).
	# Reachability: the §4.4 live-mode maker guard blocks GTC requests from
	# live dispatch in 2a — this branch is the frozen contract 2b builds
	# against, unit-tested offline against documented-shape fixtures whose
	# diff vs a real captured GTC response is 2b acceptance criterion #1.
	if order.filled_count == 0:
		if req.time_in_force == "gtc" and order.status in ("resting", "pending"):
			return OrderResult(
				status="resting",
				intended_size=req.size_contracts,
				filled_size=0,
				blended_entry_cents=0,
				fill_pct=0.0,
				slippage_cents=0,
				order_id=order.order_id or None,
			)
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

	# Kalshi's create-order response carries the blended cost as an AGGREGATE
	# (taker_fill_cost_dollars / fill_count), surfaced on the Order as
	# avg_fill_price_cents — there is NO per-fill array. If a positive fill
	# count comes back without a usable cost basis, do NOT fabricate a 0¢
	# "perfect fill" (that silently corrupts B's reconciliation and F's
	# slippage chart). Mark pending so B re-fetches by order_id and reconciles
	# the true blended basis (zero-error lens).
	blended = order.avg_fill_price_cents
	if blended <= 0:
		log.warning(
			"Kalshi order %s has filled_count=%d but no trustworthy fill cost "
			"(avg_fill_price_cents=%d) — returning pending so B reconciles the "
			"true blended price",
			order.order_id,
			order.filled_count,
			blended,
		)
		return OrderResult(
			status="pending",
			intended_size=req.size_contracts,
			filled_size=order.filled_count,
			blended_entry_cents=0,
			fill_pct=_clamp_fill_pct(order.filled_count, req.size_contracts, order.order_id),
			slippage_cents=0,
			rejection_reason="kalshi_missing_fill_cost",
			order_id=order.order_id or None,
		)

	fill_pct = _clamp_fill_pct(order.filled_count, req.size_contracts, order.order_id)
	slippage = signed_slippage_cents(
		blended=blended, limit=req.limit_price_cents, action=req.action
	)
	# Partial fill at placement on a still-resting GTC (SPEC §4.3/§9): the
	# crossed portion filled at a usable cost basis, the remainder rests —
	# status "resting" with the fill fields populated, NOT "filled".
	if req.time_in_force == "gtc" and order.status in ("resting", "pending"):
		return OrderResult(
			status="resting",
			intended_size=req.size_contracts,
			filled_size=order.filled_count,
			blended_entry_cents=blended,
			fill_pct=fill_pct,
			slippage_cents=slippage,
			order_id=order.order_id or None,
		)
	return OrderResult(
		status="filled",
		intended_size=req.size_contracts,
		filled_size=order.filled_count,
		blended_entry_cents=blended,
		fill_pct=fill_pct,
		slippage_cents=slippage,
		order_id=order.order_id or None,
	)


def _clamp_fill_pct(filled_count: int, size_contracts: int, order_id: str | None) -> float:
	"""Return fill_pct clamped to [0.0, 1.0]; log a warning on overfill.

	Failure mode: Kalshi IOC semantics SHOULD cap matched quantity at
	``count``, but a wire-shape drift returning ``filled_count > size_contracts``
	would silently produce ``fill_pct > 1.0`` and corrupt F's slippage
	chart + analytics that read this field as a probability. The raw
	``filled_size`` is still recorded (truth of record); only the derived
	ratio is clamped.
	"""
	if size_contracts <= 0:
		# Defense in depth — the caller is responsible for refusing size<=0
		# before reaching here, but never return a NaN from a div-by-zero.
		return 0.0
	raw = filled_count / size_contracts
	if raw > 1.0:
		log.warning(
			"Kalshi order %s overfilled: filled_count=%d > size_contracts=%d "
			"(fill_pct clamped to 1.0; filled_size preserved)",
			order_id, filled_count, size_contracts,
		)
		return 1.0
	return raw


# Slippage sign convention now lives in fill_math.signed_slippage_cents so
# PaperExecutor and LiveExecutor share one source of truth (Reviewer A-F2):
# without the shared helper, paper.py:153 used `blended - best_price_cents`
# (positive=bad for buys only) while live.py used the sign-flipped version
# for sells. Latent today (paper is buy-only) but breaks the moment any
# sell-side execution routes through paper (e.g., replay of live exit fills).


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

	``order_id`` is ``None`` because we never received a successful 2xx.

	Two B-side operations consume this row and they use different keys:

	* **Reconciliation** (per pending row): B polls Kalshi for the row's
	  ``client_order_id`` via ``KalshiOrderClient.status(...)`` — that's the
	  only key Kalshi knows about pre-2xx, so it's the only one that can
	  resolve pending → filled/rejected. See module docstring + dispatch
	  comment in ``engine/dispatch.py:_handle_enter``.

	* **Dedup across retries of the same signal**: ``_make_client_order_id``
	  appends a uuid4 suffix per call (D-spec L214 collision-safety), so two
	  retries of the same Signal in the same millisecond produce TWO
	  different ``client_order_id`` values. B must therefore dedup retries
	  via the natural Signal key (e.g. ``(strategy, ticker, ms_bucket)``),
	  not ``client_order_id``. D guarantees per-attempt uniqueness; B owns
	  dedup across attempts.
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
