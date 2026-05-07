"""Kalshi REST order placement client — Python API."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

import httpx

from edge_catcher.adapters.kalshi.auth import make_auth_headers  # noqa: F401
from edge_catcher.live.audit import AuditLogger, AuditEvent  # noqa: F401
from edge_catcher.live.config import LiveConfig, ABSOLUTE_MAX_ORDER_DOLLARS  # noqa: F401
from edge_catcher.live.errors import (  # noqa: F401
	CapExceededError,
	KalshiAPIError,
	NetworkError,
	OrderAlreadyFinal,
	OrderRejected,
)

_KALSHI_REST_PREFIX = "/trade-api/v2"

OrderAction = Literal["buy", "sell"]
OrderSide = Literal["yes", "no"]
OrderType = Literal["limit"]
TimeInForce = Literal["gtc", "ioc", "fok"]


@dataclass
class OrderRequest:
	ticker: str
	action: OrderAction
	side: OrderSide
	count: int
	limit_price_cents: int
	time_in_force: TimeInForce = "gtc"
	client_order_id: str | None = None

	@property
	def exposure_dollars(self) -> float:
		return self.count * self.limit_price_cents / 100.0


@dataclass
class Order:
	order_id: str
	ticker: str
	side: OrderSide
	action: OrderAction
	count: int
	limit_price_cents: int
	time_in_force: TimeInForce
	status: str
	filled_count: int = 0
	created_ts: str = ""
	client_order_id: str | None = None
	raw: dict = field(default_factory=dict)


@dataclass
class CancelResult:
	order_id: str
	status: str
	raw: dict = field(default_factory=dict)


@dataclass
class Balance:
	balance_cents: int
	raw: dict = field(default_factory=dict)


@dataclass
class Position:
	ticker: str
	side: OrderSide
	count: int
	average_price_cents: int
	raw: dict = field(default_factory=dict)


class KalshiOrderClient:
	def __init__(self, config: LiveConfig, audit: AuditLogger) -> None:
		self._config = config
		self._audit = audit
		self._http = httpx.Client(
			base_url=config.kalshi_rest_base,
			timeout=config.http_timeout_seconds,
			headers={"Accept": "application/json"},
		)

	def close(self) -> None:
		self._http.close()

	def __enter__(self) -> "KalshiOrderClient":
		return self

	def __exit__(self, *args: object) -> None:
		self.close()

	# --- verbs (filled in Tasks 7-9) -----------------------------------

	def place(self, req: OrderRequest) -> Order:
		raise NotImplementedError

	def cancel(self, order_id: str) -> CancelResult:
		raise NotImplementedError

	def status(self, order_id: str) -> Order:
		raise NotImplementedError

	def balance(self) -> Balance:
		raise NotImplementedError

	def positions(self) -> list[Position]:
		raise NotImplementedError
