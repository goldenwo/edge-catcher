"""MockKalshiWS — in-process async stand-in for Kalshi's account-scope
WebSocket, for sub-project B's WS-handler + integration tests (PR 5 / 4.C).

There is no real socket. A test ``await ws.emit_*()`` synthesises the exact
``msg`` dict shape Kalshi pushes on the account scope and routes it straight
to the registered handler coroutine (``live.ws_handlers.on_fill_event`` /
``on_order_status_event`` / ``on_settlement_event``). Because the handler is
``await``\\ ed inline, ``emit_*()`` returns only AFTER the handler has fully
processed the event — the test sees a synchronous, deterministic outcome
(the row's post-state is observable immediately after the ``await``). This
is precisely the property the Risk #9 lost-race tests need: a test can
``emit_settlement(...)`` then ``emit_fill(...)`` and assert the second is a
4.A-CAS no-op, with zero scheduler nondeterminism.

``emit_disconnect()`` models the WS dropping and reconnecting: it awaits the
registered reconnect coroutine (production E wires this to
``live.reconciliation.reconnect_reconcile``; tests pass a partial bound to a
counting fake client + the DB). This lets the integration tests prove the
reconnect path runs reconciliation without standing up a real engine.

Conventions matched (mirrors ``tests/fixtures/mock_kalshi_server.py``):
* In-process, no network, no pytest-httpx.
* Parameterizable event shapes via the ``kalshi_ws_*`` builders so E's CR-5
  parity work can compose its own scenarios without copy-paste.
* A ``pytest`` fixture (``mock_kalshi_ws``) yields a fresh instance per test.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

import pytest

# Handler signature E's WS loop dispatches to (msg, db, store_callbacks).
# ``db`` is an opaque sqlite3.Connection at this layer; typed ``Any`` so the
# fixture has no import cycle with live.ws_handlers (the tests pass the real
# connection + a real StoreCallbacks).
WSHandler = Callable[[dict[str, Any], Any, Any], Awaitable[None]]
ReconnectCoro = Callable[[], Awaitable[None]]


# ---------------------------------------------------------------------------
# Event-shape builders — the exact account-scope dict shapes Kalshi pushes.
# Kept as free functions (mirrors ``kalshi_201_*`` in mock_kalshi_server.py)
# so a test composes only the fields it asserts on.
# ---------------------------------------------------------------------------


def kalshi_ws_fill(
	*,
	client_order_id: str,
	kalshi_order_id: str,
	filled_count: int,
	fills: list[dict[str, int]],
	ticker: str | None = None,
	side: str | None = None,
) -> dict[str, Any]:
	"""A Kalshi ``fill`` event body.

	``fills`` is the per-fill array (``[{"price": int, "size": int}, ...]``)
	the handler blends via ``fill_math.blended_price_cents`` — same shape as
	the REST order's ``raw["fills"]`` so a WS-path row matches a
	dispatch-path row byte-for-byte. ``ticker``/``side`` are included for the
	exit-fill path (an exit order's own ``client_order_id`` is fresh, so the
	handler resolves the parent open row by ticker+side)."""
	msg: dict[str, Any] = {
		"client_order_id": client_order_id,
		"order_id": kalshi_order_id,
		"filled_count": filled_count,
		"fills": fills,
	}
	if ticker is not None:
		msg["ticker"] = ticker
	if side is not None:
		msg["side"] = side
	return msg


def kalshi_ws_order_status(
	*,
	client_order_id: str,
	status: str,
	rejection_reason: str | None = None,
	kalshi_order_id: str | None = None,
) -> dict[str, Any]:
	"""A Kalshi ``order_status`` event body. ``status`` is the Kalshi order
	state (``rejected``/``canceled``/...); only rejection-flavoured states
	are actioned by the handler (→ ``pending → rejected``)."""
	msg: dict[str, Any] = {
		"client_order_id": client_order_id,
		"status": status,
	}
	if rejection_reason is not None:
		msg["rejection_reason"] = rejection_reason
	if kalshi_order_id is not None:
		msg["order_id"] = kalshi_order_id
	return msg


def kalshi_ws_settlement(
	*,
	ticker: str,
	settlement_price_cents: int,
) -> dict[str, Any]:
	"""A Kalshi ``market_settlement`` event body. ``settlement_price_cents``
	is the binary resolved price (100 = YES won, 0 = NO won)."""
	return {"ticker": ticker, "settlement_price": settlement_price_cents}


# ---------------------------------------------------------------------------
# MockKalshiWS — in-process dispatcher
# ---------------------------------------------------------------------------


@dataclass
class MockKalshiWS:
	"""In-process async WS stand-in.

	Wire up once per test::

	    ws = MockKalshiWS()
	    ws.register(
	        db=conn,
	        store_callbacks=cbs,
	        on_fill=on_fill_event,
	        on_order_status=on_order_status_event,
	        on_settlement=on_settlement_event,
	        reconnect=functools.partial(reconnect_reconcile, fake_client, conn),
	    )
	    await ws.emit_fill(client_order_id="...", kalshi_order_id="...",
	                       filled_count=10, fills=[{"price": 41, "size": 10}])
	    # row post-state is observable here — emit returned after the handler.

	Every ``emit_*`` records the emitted ``msg`` (``self.emitted``) and the
	event kind (``self.events``) so a test can assert ordering / replay.
	``emit_disconnect`` increments ``self.disconnect_count`` and awaits the
	registered reconnect coroutine (reconciliation in production).
	"""

	# Registered handler coroutines + the DB / callbacks they are invoked
	# with. ``register`` must be called before any ``emit_*``.
	_db: Any = None
	_store_callbacks: Any = None
	_on_fill: WSHandler | None = None
	_on_order_status: WSHandler | None = None
	_on_settlement: WSHandler | None = None
	_reconnect: ReconnectCoro | None = None

	# Observability for assertions.
	emitted: list[dict[str, Any]] = field(default_factory=list)
	events: list[str] = field(default_factory=list)
	disconnect_count: int = 0

	def register(
		self,
		*,
		db: Any,
		store_callbacks: Any,
		on_fill: WSHandler | None = None,
		on_order_status: WSHandler | None = None,
		on_settlement: WSHandler | None = None,
		reconnect: ReconnectCoro | None = None,
	) -> None:
		"""Bind the handler coroutines + the DB/callbacks they receive.

		Production E performs the equivalent subscription wiring against the
		real Kalshi WS; this method is the test-side seam. Any handler left
		``None`` makes the corresponding ``emit_*`` raise (a test that emits
		an event it did not wire is a test bug, surfaced loudly)."""
		self._db = db
		self._store_callbacks = store_callbacks
		self._on_fill = on_fill
		self._on_order_status = on_order_status
		self._on_settlement = on_settlement
		self._reconnect = reconnect

	async def emit_fill(
		self,
		*,
		client_order_id: str,
		kalshi_order_id: str,
		filled_count: int,
		fills: list[dict[str, int]],
		ticker: str | None = None,
		side: str | None = None,
	) -> None:
		"""Push a ``fill`` event and await the registered fill handler.

		Returns only after the handler has finished — the affected row's
		post-state is observable immediately on return (the deterministic
		property the lost-race tests rely on)."""
		if self._on_fill is None:
			raise RuntimeError(
				"MockKalshiWS.emit_fill: no on_fill handler registered "
				"(call register(on_fill=...) first)"
			)
		msg = kalshi_ws_fill(
			client_order_id=client_order_id,
			kalshi_order_id=kalshi_order_id,
			filled_count=filled_count,
			fills=fills,
			ticker=ticker,
			side=side,
		)
		self.emitted.append(msg)
		self.events.append("fill")
		await self._on_fill(msg, self._db, self._store_callbacks)

	async def emit_order_status(
		self,
		*,
		client_order_id: str,
		status: str,
		rejection_reason: str | None = None,
		kalshi_order_id: str | None = None,
	) -> None:
		"""Push an ``order_status`` event and await the registered handler."""
		if self._on_order_status is None:
			raise RuntimeError(
				"MockKalshiWS.emit_order_status: no on_order_status handler "
				"registered (call register(on_order_status=...) first)"
			)
		msg = kalshi_ws_order_status(
			client_order_id=client_order_id,
			status=status,
			rejection_reason=rejection_reason,
			kalshi_order_id=kalshi_order_id,
		)
		self.emitted.append(msg)
		self.events.append("order_status")
		await self._on_order_status(msg, self._db, self._store_callbacks)

	async def emit_settlement(
		self,
		*,
		ticker: str,
		settlement_price_cents: int,
	) -> None:
		"""Push a ``market_settlement`` event and await the registered
		settlement handler (which also fires E's wired bankroll callback)."""
		if self._on_settlement is None:
			raise RuntimeError(
				"MockKalshiWS.emit_settlement: no on_settlement handler "
				"registered (call register(on_settlement=...) first)"
			)
		msg = kalshi_ws_settlement(
			ticker=ticker,
			settlement_price_cents=settlement_price_cents,
		)
		self.emitted.append(msg)
		self.events.append("settlement")
		await self._on_settlement(msg, self._db, self._store_callbacks)

	async def emit_disconnect(self) -> None:
		"""Model a WS drop+reconnect: await the registered reconnect
		coroutine (production E wires this to
		``reconciliation.reconnect_reconcile``). Returns after reconciliation
		has finished so the test can assert recovered row state."""
		self.disconnect_count += 1
		self.events.append("disconnect")
		if self._reconnect is None:
			raise RuntimeError(
				"MockKalshiWS.emit_disconnect: no reconnect coroutine "
				"registered (call register(reconnect=...) first)"
			)
		await self._reconnect()


@pytest.fixture
def mock_kalshi_ws() -> MockKalshiWS:
	"""Fresh MockKalshiWS per test — no cross-test state."""
	return MockKalshiWS()
