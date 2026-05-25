"""Venue-neutral live-execution contract.

The normalized value objects + the :class:`LiveVenueClient` Protocol that the
engine's live-execution layer (executor, reconciler) depends on. A prediction-
market venue is integrated by implementing ``LiveVenueClient`` and populating
these value objects from its own wire shapes — ``KalshiOrderClient``
(:mod:`edge_catcher.live.client`) is the first implementation; Polymarket and
others would add their own client without touching the executor or reconciler.

Stdlib-only by design: this module is the LEAF of the live-execution dependency
graph (``client.py`` imports FROM here, never the reverse), so a new venue
client imports the contract without pulling in Kalshi specifics, and there is
no import cycle.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Literal, Protocol

OrderAction = Literal["buy", "sell"]
OrderSide = Literal["yes", "no"]
OrderType = Literal["limit"]  # 'market' explicitly excluded — see Q9 in design notes
TimeInForce = Literal["gtc", "ioc", "fok"]

# client_order_id is forwarded to the venue as the idempotency key. Restrict to
# URL-safe alphanumerics + ``-_`` so the value survives JSON encoding, log
# rendering, and any downstream system that consumes the audit trail without
# ambiguity. 80 chars covers the D-spec L214 worst-case format
# ``{strategy}-{ticker}-{ms_ts}-{uuid8}``. The canonical producer lives at
# ``edge_catcher/engine/execution.py:_make_client_order_id`` which charset- and
# length-validates against this same regex before assembly, so a 4xx from a
# venue would indicate a strategy/ticker that bypassed the builder.
_CLIENT_ORDER_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,80}$")


@dataclass
class OrderRequest:
	"""Caller-side typed input for :meth:`LiveVenueClient.place`."""

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
	"""A venue order (normalized shape). Each venue's client maps its create-order
	response + list-order element onto this — e.g. Kalshi's POST /orders."""

	order_id: str
	ticker: str
	side: OrderSide
	action: OrderAction
	count: int
	limit_price_cents: int
	time_in_force: TimeInForce
	status: str  # normalized venue status (Kalshi: pending/resting/executed/canceled/rejected)
	filled_count: int = 0
	# Volume-weighted average fill price in cents (the real cost basis). 0 when
	# nothing filled or cost is unavailable. Often better than ``limit_price_cents``
	# for an IOC that took resting liquidity; ``_translate_order`` and the
	# reconciler use it as the blended cost basis. Derived per venue (Kalshi: the
	# aggregate ``taker_fill_cost_dollars`` / ``fill_count_fp`` — no per-fill array).
	avg_fill_price_cents: int = 0
	created_ts: str = ""  # ISO-8601
	client_order_id: str | None = None
	raw: dict = field(default_factory=dict)  # full venue response, for forward-compat


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


class LiveVenueClient(Protocol):
	"""The async order/account contract a live trading venue must satisfy.

	``KalshiOrderClient`` is the first implementation. The engine's
	:class:`~edge_catcher.engine.executors.live.LiveExecutor` and B's reconciler
	depend on THIS Protocol (structurally), never a concrete venue class — so a
	new venue (Polymarket, …) is integrated by implementing these methods and
	populating the value objects above, with no change to the executor or the
	reconciler. Declared as a ``Protocol`` (not a base class) so an
	implementation needs no import of this module and mypy verifies conformance
	at the call site (e.g. where ``LiveExecutor`` is handed a
	``KalshiOrderClient``). Lifecycle (``close`` / async-context) is the concrete
	client's concern, owned by the engine that constructs it — out of this
	venue-operations contract.
	"""

	async def place(self, req: OrderRequest) -> Order: ...

	async def cancel(self, order_id: str) -> CancelResult: ...

	async def status(self, order_id: str) -> Order: ...

	async def balance(self) -> Balance: ...

	async def positions(self) -> list[Position]: ...

	async def list_orders(
		self,
		*,
		status: str | None = ...,
		limit: int = ...,
		cursor: str | None = ...,
		min_ts: int | None = ...,
	) -> list[Order]: ...

	async def market_meta(self, ticker: str) -> dict: ...
