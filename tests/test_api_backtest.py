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
