"""Unit tests for MockKalshiServer's CR-5 coid-matching mode.

CR-5 replays a captured bundle through the live dispatch path against an
in-process :class:`MockKalshiServer`.  On a REAL bundle the live run places
both entry orders AND exit orders (``_handle_exit`` places a sell through the
executor unconditionally, dispatch.py:1038).  The exit order's
``client_order_id`` is generated fresh INSIDE ``_handle_exit`` — the test
harness cannot pre-queue a response keyed to it.

The default FIFO/sticky-tail server (used by the live-client error tests)
returns responses in queue order regardless of which order is being placed,
so an exit place consumes/reuses a response meant for an entry — wrong, and
on an empty queue it returns 500 which trips A's retry backoff (a multi-second
stall per exit place on a real bundle).

This module specifies the opt-in ``match_by_client_order_id`` mode that CR-5
needs:

  * a placed order is answered by the queued response whose ``client_order_id``
    matches the request — even when responses were queued out of place order;
  * an order whose coid matches NO queued response (the fresh exit coid) gets a
    synthesised fully-filled echo of the request (valid 201, ``filled_count`` ==
    requested count, a ``fills`` array) so ``LiveExecutor`` translates it
    cleanly and the replay completes — no 500, no retry stall;
  * the default (flag off) behaviour is byte-for-byte the FIFO/sticky-tail the
    existing error-storm tests depend on.
"""
from __future__ import annotations

import pytest

from edge_catcher.live.client import OrderRequest
from tests.fixtures.mock_kalshi_server import MockKalshiServer, kalshi_201_filled


def _buy(coid: str, *, count: int, price: int = 50) -> OrderRequest:
	return OrderRequest(
		ticker="KXTEST-T1",
		action="buy",
		side="yes",
		count=count,
		limit_price_cents=price,
		time_in_force="ioc",
		client_order_id=coid,
	)


def _sell(coid: str, *, count: int, price: int = 48) -> OrderRequest:
	return OrderRequest(
		ticker="KXTEST-T1",
		action="sell",
		side="yes",
		count=count,
		limit_price_cents=price,
		time_in_force="ioc",
		client_order_id=coid,
	)


@pytest.mark.asyncio
async def test_coid_matching_returns_response_for_matching_coid_out_of_order(
	live_cfg, live_audit, signing_env
):
	"""With coid-matching on, a placed order is answered by the queued response
	whose client_order_id matches it — even when placed out of FIFO order."""
	server = MockKalshiServer(match_by_client_order_id=True)
	server.queue_response(
		kalshi_201_filled(order_id="ord-A", client_order_id="coid-A",
		                  count=3, fills=[{"price": 50, "size": 3}])
	)
	server.queue_response(
		kalshi_201_filled(order_id="ord-B", client_order_id="coid-B",
		                  count=7, fills=[{"price": 60, "size": 7}])
	)
	client = server.make_client(live_cfg, live_audit)
	try:
		# Place coid-B FIRST. FIFO would hand back the head (ord-A); coid-matching
		# must return ord-B.
		order_b = await client.place(_buy("coid-B", count=7, price=60))
		assert order_b.order_id == "ord-B"
		assert order_b.client_order_id == "coid-B"
		# coid-A is still available and matches on its own place.
		order_a = await client.place(_buy("coid-A", count=3, price=50))
		assert order_a.order_id == "ord-A"
	finally:
		await client.close()


@pytest.mark.asyncio
async def test_coid_matching_synthesises_filled_echo_for_unqueued_coid(
	live_cfg, live_audit, signing_env
):
	"""An order whose coid matches no queued response (the fresh exit coid) gets
	a fully-filled echo of the request — not a 500/stall — and does NOT consume
	the queued entry response."""
	server = MockKalshiServer(match_by_client_order_id=True)
	# Only the entry response is queued; the exit coid is never queued.
	server.queue_response(
		kalshi_201_filled(order_id="ord-entry", client_order_id="entry-coid",
		                  count=3, fills=[{"price": 50, "size": 3}])
	)
	client = server.make_client(live_cfg, live_audit)
	try:
		exit_order = await client.place(_sell("exit-coid-1", count=3, price=48))
		# Fully-filled echo (real wire shape): fill count + a usable aggregate
		# cost basis so LiveExecutor translates it as filled, not pending.
		assert exit_order.filled_count == 3
		assert exit_order.client_order_id == "exit-coid-1"
		assert exit_order.avg_fill_price_cents == 48, "echo must carry a usable fill cost basis"
		assert exit_order.raw.get("fill_count_fp") == "3.00"
		# The synthesised echo must NOT have consumed the queued entry response.
		entry_order = await client.place(_buy("entry-coid", count=3, price=50))
		assert entry_order.order_id == "ord-entry"
	finally:
		await client.close()


@pytest.mark.asyncio
async def test_default_mode_remains_fifo_sticky_tail(
	live_cfg, live_audit, signing_env
):
	"""Regression guard: with the flag OFF (default) responses are served strict
	FIFO regardless of coid — the behaviour the error-storm tests depend on."""
	server = MockKalshiServer()  # default: match_by_client_order_id is False
	server.queue_response(
		kalshi_201_filled(order_id="ord-1", client_order_id="cA",
		                  count=1, fills=[{"price": 50, "size": 1}])
	)
	server.queue_response(
		kalshi_201_filled(order_id="ord-2", client_order_id="cB",
		                  count=1, fills=[{"price": 50, "size": 1}])
	)
	client = server.make_client(live_cfg, live_audit)
	try:
		# FIFO: the first place gets the head (ord-1) even though its coid is cB.
		o1 = await client.place(_buy("cB", count=1))
		assert o1.order_id == "ord-1"
		o2 = await client.place(_buy("cA", count=1))
		assert o2.order_id == "ord-2"
	finally:
		await client.close()
