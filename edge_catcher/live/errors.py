"""Exception hierarchy for the live order placement layer."""

from __future__ import annotations


class LiveError(Exception):
	"""Base — all live/ exceptions inherit from this."""


class ConfigError(LiveError):
	"""Raised on invalid live-trader.yaml or missing env vars."""


class CapExceededError(LiveError):
	"""Raised when an order's exposure exceeds a configured dollar cap."""

	def __init__(self, exposure_dollars: float, cap_dollars: float, cap_name: str) -> None:
		super().__init__(
			f"Order exposure ${exposure_dollars:.2f} exceeds {cap_name} ${cap_dollars:.2f}"
		)
		self.exposure_dollars = exposure_dollars
		self.cap_dollars = cap_dollars
		self.cap_name = cap_name


class KalshiAPIError(LiveError):
	"""Raised on non-2xx response from Kalshi after retries exhausted."""

	def __init__(self, status: int, body: str, path: str) -> None:
		# Truncate body to match the audit log's body cap so log/grep are consistent.
		super().__init__(f"Kalshi {status} on {path}: {body[:500]}")
		self.status = status
		self.body = body
		self.path = path


class OrderRejected(KalshiAPIError):
	"""Raised on 4xx during order placement (validation or business-rule rejection).

	Caller should NOT retry; the order was not accepted by Kalshi.
	"""


class OrderAlreadyFinal(KalshiAPIError):
	"""Raised when cancel targets an order that is already filled, cancelled, or rejected.

	Maps to Kalshi 404 (order_not_found) or 409 (state-conflict) on cancel/modify.
	Sub-project B's reconciliation loop will hit this routinely as it sweeps stale
	open orders against Kalshi truth — treat as an idempotent no-op, not an error.
	"""


class NetworkError(LiveError):
	"""Raised on network failure after retries exhausted."""
