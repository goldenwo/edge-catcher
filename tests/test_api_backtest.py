"""Basic tests for backtest API models and state management."""

import json
from api.tasks import BacktestTaskState, backtest_states, get_backtest_state, is_backtest_running


def test_backtest_state_lifecycle():
    """BacktestTaskState tracks running → complete lifecycle."""
    backtest_states.clear()
    state = BacktestTaskState(task_id="test-1")
    backtest_states["test-1"] = state

    assert get_backtest_state("test-1") is state
    assert get_backtest_state("nonexistent") is None
    assert not is_backtest_running()

    state.running = True
    assert is_backtest_running()

    state.running = False
    state.result = {"total_trades": 10}
    assert not is_backtest_running()
    assert state.result["total_trades"] == 10

    backtest_states.clear()


def test_backtest_state_progress_fields():
    """BacktestTaskState has progress tracking fields."""
    state = BacktestTaskState(task_id="test-prog")
    assert state.trades_processed == 0
    assert state.trades_estimated == 0
    assert state.net_pnl_cents == 0

    state.trades_processed = 5000
    state.trades_estimated = 20000
    state.net_pnl_cents = 150
    assert state.trades_processed == 5000
