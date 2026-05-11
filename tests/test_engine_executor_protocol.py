"""Tests for engine.executor — Executor protocol + value objects."""
from __future__ import annotations

import pytest

from edge_catcher.engine.executor import Executor, OpenPosition, OrderRequest, OrderResult


def test_order_request_is_frozen():
	req = OrderRequest(
		ticker="KXSOL15M-25-T1",
		series="KXSOL15M",
		side="yes",
		size_contracts=10,
		limit_price_cents=42,
		strategy="strat-34",
		client_order_id="strat-34-KXSOL15M-1715000000000",
	)
	# CPython 3.11 raises FrozenInstanceError on assignment to a frozen
	# slotted dataclass (3.12 raises AttributeError). Accept either; do NOT
	# use bare Exception which would mask unrelated regressions.
	from dataclasses import FrozenInstanceError
	with pytest.raises((FrozenInstanceError, AttributeError)):
		req.size_contracts = 20  # type: ignore[misc]


def test_order_request_uses_slots():
	req = OrderRequest(
		ticker="x", series="x", side="yes", size_contracts=1,
		limit_price_cents=1, strategy="x", client_order_id="x",
	)
	# CPython 3.11 raises TypeError instead of AttributeError for frozen+slotted
	# dataclasses (bug fixed in 3.12); accept both to stay version-agnostic.
	with pytest.raises((AttributeError, TypeError)):
		req.unknown_field = 1  # type: ignore[attr-defined]


def test_order_result_filled_shape():
	r = OrderResult(
		status="filled",
		intended_size=10,
		filled_size=10,
		blended_entry_cents=42,
		fill_pct=1.0,
		slippage_cents=0,
		book_depth=5,
		book_snapshot='[[0.42, 100]]',
	)
	assert r.status == "filled"
	assert r.rejection_reason is None


def test_order_result_rejected_shape_zero_sentinels():
	r = OrderResult(
		status="rejected",
		intended_size=10,
		filled_size=0,
		blended_entry_cents=0,  # placeholder, not sentinel
		fill_pct=0.0,
		slippage_cents=0,
		rejection_reason="stale_book",
	)
	assert r.status == "rejected"
	assert r.rejection_reason == "stale_book"
	assert r.book_depth is None
	assert r.book_snapshot is None


def test_order_result_pending_allows_partial_fill():
	"""Per spec §R9-F1: status='pending' AND filled_size > 0 is valid for
	Kalshi GTC partial-fill-resting orders."""
	r = OrderResult(
		status="pending",
		intended_size=10,
		filled_size=4,                  # partial fill
		blended_entry_cents=42,
		fill_pct=0.4,
		slippage_cents=0,
	)
	assert r.status == "pending"
	assert r.filled_size == 4


@pytest.mark.asyncio
async def test_executor_protocol_structural_typing():
	"""Anything with `async def place(req) -> OrderResult` satisfies Executor."""
	class FakeExecutor:
		async def place(self, req: OrderRequest) -> OrderResult:
			return OrderResult(
				status="filled", intended_size=req.size_contracts,
				filled_size=req.size_contracts, blended_entry_cents=req.limit_price_cents,
				fill_pct=1.0, slippage_cents=0,
			)
	exec_obj: Executor = FakeExecutor()
	result = await exec_obj.place(OrderRequest(
		ticker="x", series="x", side="yes", size_contracts=1,
		limit_price_cents=1, strategy="x", client_order_id="x",
	))
	assert result.status == "filled"


# ---------------------------------------------------------------------------
# PR 2 field-additions — OrderRequest.action, OrderResult.order_id, OpenPosition
# ---------------------------------------------------------------------------


def test_order_request_action_defaults_to_buy() -> None:
	"""OrderRequest.action defaults to 'buy' — preserves existing call sites."""
	req = OrderRequest(
		ticker="x", series="x", side="yes", size_contracts=1,
		limit_price_cents=1, strategy="x", client_order_id="x",
	)
	assert req.action == "buy"


def test_order_request_action_can_be_sell() -> None:
	req = OrderRequest(
		ticker="x", series="x", side="yes", size_contracts=1,
		limit_price_cents=1, strategy="x", client_order_id="x",
		action="sell",
	)
	assert req.action == "sell"


def test_order_result_order_id_defaults_to_none() -> None:
	"""OrderResult.order_id defaults to None — preserves existing paper-path call sites."""
	r = OrderResult(
		status="filled",
		intended_size=1,
		filled_size=1,
		blended_entry_cents=50,
		fill_pct=1.0,
		slippage_cents=0,
	)
	assert r.order_id is None


def test_order_result_order_id_can_be_set() -> None:
	r = OrderResult(
		status="filled",
		intended_size=1,
		filled_size=1,
		blended_entry_cents=50,
		fill_pct=1.0,
		slippage_cents=0,
		order_id="ord-abc123",
	)
	assert r.order_id == "ord-abc123"


def test_open_position_shape() -> None:
	"""OpenPosition is a frozen+slots dataclass with 4 required fields."""
	pos = OpenPosition(
		ticker="KXSOL15M-25-T1",
		side="yes",
		fill_size=10,
		blended_entry_cents=42,
	)
	assert pos.ticker == "KXSOL15M-25-T1"
	assert pos.side == "yes"
	assert pos.fill_size == 10
	assert pos.blended_entry_cents == 42


def test_open_position_is_frozen() -> None:
	"""OpenPosition must be immutable."""
	pos = OpenPosition(ticker="X", side="no", fill_size=5, blended_entry_cents=60)
	from dataclasses import FrozenInstanceError
	with pytest.raises((FrozenInstanceError, AttributeError)):
		pos.fill_size = 99  # type: ignore[misc]


def test_open_position_uses_slots() -> None:
	pos = OpenPosition(ticker="X", side="yes", fill_size=1, blended_entry_cents=50)
	with pytest.raises((AttributeError, TypeError)):
		pos.unknown_field = 1  # type: ignore[attr-defined]
