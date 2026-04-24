"""Tests for strategizer.generate_from_hypothesis()."""

from unittest.mock import MagicMock
from edge_catcher.ai.strategizer import generate_from_hypothesis
from edge_catcher.research.test_runner import TestResult, EDGE_EXISTS


class TestGenerateFromHypothesis:
    def test_returns_code_and_name(self):
        client = MagicMock()
        client.complete.return_value = (
            "```python\nfrom edge_catcher.runner.strategies import Strategy, Signal\n\n"
            "class TestStrategy(Strategy):\n"
            "\tname = 'test-strat'\n"
            "\tdef on_trade(self, trade, market, portfolio):\n"
            "\t\treturn []\n```"
        )
        client._resolve_model.return_value = "test-model"
        client.last_usage = {"input_tokens": 100, "output_tokens": 200}

        test_result = TestResult(
            verdict=EDGE_EXISTS, z_stat=3.5,
            fee_adjusted_edge=0.02,
            detail={"buckets": [{"range": [0.01, 0.10], "z": 3.5}]},
        )
        code, name = generate_from_hypothesis(
            hypothesis_config={
                "test_type": "price_bucket_bias",
                "series": "SER_A", "db": "kalshi.db",
                "rationale": "test",
                "params": {"buckets": [[0.01, 0.10]]},
            },
            test_result=test_result,
            profiles=[],
            client=client,
        )
        assert "class" in code
        assert isinstance(name, str)
        assert len(name) > 0

    def test_calls_llm_with_evidence(self):
        client = MagicMock()
        client.complete.return_value = (
            "```python\nfrom edge_catcher.runner.strategies import Strategy, Signal\n\n"
            "class X(Strategy):\n"
            "\tname = 'x'\n"
            "\tdef on_trade(self, t, m, p):\n"
            "\t\treturn []\n```"
        )
        client._resolve_model.return_value = "m"
        client.last_usage = {}

        test_result = TestResult(
            verdict=EDGE_EXISTS, z_stat=4.1,
            fee_adjusted_edge=0.03,
            detail={"buckets": [{"range": [0.01, 0.10], "z": 4.1}]},
        )
        generate_from_hypothesis(
            hypothesis_config={"test_type": "price_bucket_bias", "series": "S", "db": "k.db",
                "rationale": "r", "params": {}},
            test_result=test_result,
            profiles=[],
            client=client,
        )
        call_args = client.complete.call_args
        user_prompt = call_args[0][1]
        assert "4.1" in user_prompt or "EDGE_EXISTS" in user_prompt

    def test_uses_existing_parse(self):
        """Verifies it delegates to _parse_strategy_response."""
        client = MagicMock()
        # Valid strategy code that _parse_strategy_response can extract
        client.complete.return_value = (
            "```python\nfrom edge_catcher.runner.strategies import Strategy, Signal\n\n"
            "class MyStrat(Strategy):\n"
            "\tname = 'my-strat'\n"
            "\tdef on_trade(self, trade, market, portfolio):\n"
            "\t\treturn []\n```"
        )
        client._resolve_model.return_value = "m"
        client.last_usage = {}

        test_result = TestResult(verdict=EDGE_EXISTS, z_stat=3.0, fee_adjusted_edge=0.01, detail={})
        code, name = generate_from_hypothesis(
            hypothesis_config={"test_type": "t", "series": "S", "db": "d.db",
                "rationale": "r", "params": {}},
            test_result=test_result,
            profiles=[],
            client=client,
        )
        assert "my-strat" in name or "MyStrat" in name or "my_strat" in name
