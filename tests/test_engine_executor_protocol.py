"""Tests for engine.executor — Executor protocol + value objects."""
from __future__ import annotations

import pytest

from edge_catcher.engine.executor import Executor, OrderRequest, OrderResult


def test_order_request_is_frozen():
	req = OrderRequest(
		ticker="KXSOL15M-25-T1",
		series="KXSOL15M",
		side="yes",
		size_contracts=10,
		limit_price_cents=42,
		strategy="debut-fade",
		client_order_id="debut-fade-KXSOL15M-1715000000000",
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


def test_executor_protocol_structural_typing():
	"""Anything with `def place(req) -> OrderResult` satisfies Executor."""
	class FakeExecutor:
		def place(self, req: OrderRequest) -> OrderResult:
			return OrderResult(
				status="filled", intended_size=req.size_contracts,
				filled_size=req.size_contracts, blended_entry_cents=req.limit_price_cents,
				fill_pct=1.0, slippage_cents=0,
			)
	exec_obj: Executor = FakeExecutor()
	result = exec_obj.place(OrderRequest(
		ticker="x", series="x", side="yes", size_contracts=1,
		limit_price_cents=1, strategy="x", client_order_id="x",
	))
	assert result.status == "filled"
