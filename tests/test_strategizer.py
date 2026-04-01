"""Tests for AI strategizer module. No actual API calls — all mocked."""
from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from edge_catcher.ai.strategizer import (
    _build_user_prompt,
    _parse_strategy_response,
    strategize,
)


SAMPLE_RESPONSE = '''\
Here's a strategy based on your hypothesis:

```python
from edge_catcher.runner.strategies import Strategy, Signal

class TimedDecay(Strategy):
    name = 'timed-decay'

    def __init__(self, min_price: int = 70, max_price: int = 95):
        self.min_price = min_price
        self.max_price = max_price

    def on_trade(self, trade, market, portfolio):
        if (self.min_price <= trade.yes_price <= self.max_price
                and not portfolio.has_position(trade.ticker, self.name)):
            return [Signal(
                action='buy', ticker=trade.ticker, side='yes',
                price=trade.yes_price, size=1,
                reason=f'yes_price={trade.yes_price} in range',
            )]
        return []
```

This strategy buys YES contracts when the price is in the sweet spot range.
'''


def test_parse_strategy_response():
    """Extracts Python code and strategy name from LLM response."""
    code, name = _parse_strategy_response(SAMPLE_RESPONSE)
    assert "class TimedDecay" in code
    assert name == "timed-decay"


def test_parse_strategy_response_no_code():
    """Raises ValueError when no Python code block found."""
    with pytest.raises(ValueError, match="No Python"):
        _parse_strategy_response("Here is some text without code blocks.")


def test_build_user_prompt():
    """User prompt includes hypothesis config and analysis results."""
    hyp_config = {"name": "Test", "market": "kalshi", "rationale": "testing"}
    analysis = {"verdict": "EDGE_EXISTS", "fee_adjusted_edge": 0.05}
    prompt = _build_user_prompt(hyp_config, analysis)
    assert "Test" in prompt
    assert "EDGE_EXISTS" in prompt


def test_strategize_success():
    """Full strategize flow with mocked LLM client."""
    mock_client = MagicMock()
    mock_client.complete.return_value = SAMPLE_RESPONSE

    # Mock the DB lookups for hypothesis config and analysis results
    with patch("edge_catcher.ai.strategizer._get_hypothesis_config") as mock_hyp, \
         patch("edge_catcher.ai.strategizer._get_analysis_result") as mock_result:
        mock_hyp.return_value = {"name": "Test Hyp", "market": "kalshi"}
        mock_result.return_value = {"verdict": "EDGE_EXISTS", "fee_adjusted_edge": 0.05}

        result = strategize("test_hyp", None, mock_client, Path("data/test.db"))

    assert result["error"] is None
    assert "class TimedDecay" in result["code"]
    assert result["strategy_name"] == "timed-decay"
    mock_client.complete.assert_called_once()
