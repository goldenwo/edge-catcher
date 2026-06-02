"""Wiring tests for the drawdown-gate close hooks (spec §3.3)."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

import edge_catcher.engine.engine as engmod
from edge_catcher.engine.dispatch import _handle_exit
from edge_catcher.engine.engine import _settlement_poller
from edge_catcher.engine.executor import OrderResult


@pytest.mark.asyncio
async def test_settlement_poller_awaits_close_hook_after_settle(monkeypatch) -> None:
	"""After a confirmed settlement, the poller awaits record_close_hook(now)
	exactly once (spec §3.3a). FAILS pre-fix: the poller has no such param."""
	trade = {
		"id": 1, "ticker": "KXTEST15M-T50", "strategy": "s", "side": "yes",
		"series_ticker": "KXTEST15M", "entry_time": None,
	}
	store = MagicMock()
	# One open trade on the first poll, then none (so we don't re-settle forever).
	store.get_open_trades.side_effect = [[trade], [], [], []]
	store.settle_trade = MagicMock()
	store.get_trade_by_id.return_value = {
		"status": "won", "pnl_cents": 100, "blended_entry": 50,
		"entry_price": 50, "fill_size": 3, "entry_fee_cents": 0, "exit_price": 100,
		"side": "yes",
	}
	store.save_state = MagicMock()

	monkeypatch.setattr(engmod, "check_market_result", AsyncMock(return_value="yes"))
	monkeypatch.setattr(engmod, "notify", lambda *a, **k: None)

	hook_calls: list[datetime] = []

	async def spy_hook(now: datetime) -> None:
		hook_calls.append(now)

	task = asyncio.create_task(
		_settlement_poller(
			store, MagicMock(), [], {}, interval=0,
			record_close_hook=spy_hook,
		)
	)
	await asyncio.sleep(0.05)
	task.cancel()
	try:
		await task
	except asyncio.CancelledError:
		pass

	assert len(hook_calls) == 1, "hook must be awaited once per confirmed settle"
	assert store.settle_trade.call_count == 1


# ---------------------------------------------------------------------------
# Task 4: _handle_exit return-bool + _handle_signal ratchet (spec §3.3b)
# ---------------------------------------------------------------------------


def _exit_signal():
	from edge_catcher.engine.strategy_base import Signal
	return Signal(
		action="exit", ticker="KXTEST15M-T50", side="yes", series="KXTEST15M",
		strategy="s", reason="stop_loss", trade_id=1,
	)


class _TickStub:
	yes_bid = 50
	no_bid = 50


@pytest.mark.asyncio
async def test_handle_exit_returns_true_on_confirmed_full_fill() -> None:
	store = MagicMock()
	store.get_trade_by_id.return_value = {
		"fill_size": 3, "blended_entry": 50, "entry_price": 50,
		"pnl_cents": 30, "entry_fee_cents": 0,
	}
	store.exit_trade = MagicMock()
	executor = MagicMock()
	executor.place = AsyncMock(return_value=OrderResult(
		status="filled", intended_size=3, filled_size=3,
		blended_entry_cents=50, fill_pct=1.0, slippage_cents=0,
	))
	closed = await _handle_exit(
		_exit_signal(), _TickStub(), store, "🔵",
		now=datetime.now(timezone.utc), executor=executor, config={},
	)
	assert closed is True


@pytest.mark.asyncio
async def test_handle_exit_returns_false_on_partial_fill() -> None:
	store = MagicMock()
	store.get_trade_by_id.return_value = {"fill_size": 3, "blended_entry": 50}
	executor = MagicMock()
	executor.place = AsyncMock(return_value=OrderResult(
		status="filled", intended_size=3, filled_size=1,  # 1 < 3 → partial
		blended_entry_cents=50, fill_pct=0.34, slippage_cents=0,
	))
	closed = await _handle_exit(
		_exit_signal(), _TickStub(), store, "🔵",
		now=datetime.now(timezone.utc), executor=executor, config={},
	)
	assert closed is False


@pytest.mark.asyncio
async def test_handle_exit_returns_false_on_missing_trade_id() -> None:
	from edge_catcher.engine.strategy_base import Signal
	sig = Signal(reason="stop_loss", strategy="s", ticker="KXTEST15M-T50", series="KXTEST15M",
	             side="yes", action="exit", trade_id=None)
	closed = await _handle_exit(
		sig, _TickStub(), MagicMock(), "🔵",
		now=datetime.now(timezone.utc), executor=MagicMock(), config={},
	)
	assert closed is False
