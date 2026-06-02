"""Wiring tests for the drawdown-gate close hooks (spec §3.3)."""
from __future__ import annotations

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

import edge_catcher.engine.engine as engmod
from edge_catcher.engine.engine import _settlement_poller


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
