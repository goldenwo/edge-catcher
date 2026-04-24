# tests/test_llm_ideator.py
"""Tests for edge_catcher.research.llm_ideator module."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from edge_catcher.research.llm_ideator import LLMIdeator
from edge_catcher.research.tracker import Tracker
from edge_catcher.research.audit import AuditLog


def _seed_tracker(tracker: Tracker) -> None:
	"""Seed tracker with enough results for ideation (≥10)."""
	from tests.test_research import _make_result
	for i in range(12):
		verdict = "promote" if i < 2 else ("explore" if i < 5 else "kill")
		r = _make_result(
			strategy=f"S{i % 3}",
			series=f"SER{i}",
			verdict=verdict,
			verdict_reason=f"reason-{i}",
			sharpe=2.5 if verdict == "promote" else (1.5 if verdict == "explore" else 0.3),
			win_rate=0.9 if verdict == "promote" else (0.86 if verdict == "explore" else 0.5),
		)
		tracker.save_result(r)


class TestLLMIdeatorBuildPrompt:
	def test_build_prompt_includes_results(self, tmp_path):
		tracker = Tracker(tmp_path / "research.db")
		audit = AuditLog(tmp_path / "research.db")
		_seed_tracker(tracker)

		ideator = LLMIdeator(tracker=tracker, audit=audit, client=MagicMock())
		prompt = ideator.build_ideation_prompt(
			available_strategies=["A", "B", "C"],
			series_map={"data/kalshi-btc.db": ["SERIES_A", "SERIES_E"]},
		)
		assert "promote" in prompt.lower()
		assert "kill" in prompt.lower()
		assert "reason-" in prompt

	def test_build_prompt_includes_strategies(self, tmp_path):
		tracker = Tracker(tmp_path / "research.db")
		audit = AuditLog(tmp_path / "research.db")
		_seed_tracker(tracker)

		ideator = LLMIdeator(tracker=tracker, audit=audit, client=MagicMock())
		prompt = ideator.build_ideation_prompt(
			available_strategies=["MyStrat", "AnotherStrat"],
			series_map={"data/kalshi-btc.db": ["SERIES_A"]},
		)
		assert "MyStrat" in prompt
		assert "AnotherStrat" in prompt


class TestLLMIdeatorParseResponse:
	def test_parse_valid_response(self, tmp_path):
		tracker = Tracker(tmp_path / "research.db")
		audit = AuditLog(tmp_path / "research.db")
		ideator = LLMIdeator(tracker=tracker, audit=audit, client=MagicMock())

		response = json.dumps({
			"reasoning": "testing",
			"existing_strategy_hypotheses": [
				{"strategy": "A", "series": "SERIES_A", "db_path": "data/kalshi-btc.db", "rationale": "r"}
			],
			"novel_strategy_proposals": [
				{"name": "new-strat", "description": "buy low sell high", "rationale": "r"}
			],
		})
		existing, novel = ideator.parse_response(response)
		assert len(existing) == 1
		assert existing[0]["strategy"] == "A"
		assert len(novel) == 1
		assert novel[0]["name"] == "new-strat"

	def test_parse_malformed_json(self, tmp_path):
		tracker = Tracker(tmp_path / "research.db")
		audit = AuditLog(tmp_path / "research.db")
		ideator = LLMIdeator(tracker=tracker, audit=audit, client=MagicMock())

		existing, novel = ideator.parse_response("not json at all")
		assert existing == []
		assert novel == []

	def test_parse_response_with_markdown_fencing(self, tmp_path):
		tracker = Tracker(tmp_path / "research.db")
		audit = AuditLog(tmp_path / "research.db")
		ideator = LLMIdeator(tracker=tracker, audit=audit, client=MagicMock())

		raw = '```json\n{"reasoning":"x","existing_strategy_hypotheses":[],"novel_strategy_proposals":[]}\n```'
		existing, novel = ideator.parse_response(raw)
		# Should handle fenced JSON gracefully
		assert existing == []
		assert novel == []


class TestLLMIdeatorIdeate:
	def test_ideate_calls_llm_and_returns_hypotheses(self, tmp_path):
		tracker = Tracker(tmp_path / "research.db")
		audit = AuditLog(tmp_path / "research.db")
		_seed_tracker(tracker)

		mock_client = MagicMock()
		mock_client.complete.return_value = json.dumps({
			"reasoning": "testing",
			"existing_strategy_hypotheses": [
				{"strategy": "S0", "series": "SER0", "db_path": "data/kalshi-btc.db", "rationale": "r"}
			],
			"novel_strategy_proposals": [],
		})
		mock_client.last_usage = {"input_tokens": 100, "output_tokens": 50}
		mock_client._resolve_model.return_value = "test-model"

		ideator = LLMIdeator(tracker=tracker, audit=audit, client=mock_client)
		hypotheses, novel = ideator.ideate(
			available_strategies=["S0", "S1", "S2"],
			series_map={"data/kalshi-btc.db": ["SER0", "SER1"]},
			start_date="2025-01-01",
			end_date="2025-12-31",
		)
		assert len(hypotheses) == 1
		assert hypotheses[0].strategy == "S0"
		assert hypotheses[0].tags == ["source:llm_ideated"]
		mock_client.complete.assert_called_once()

		# Verify decision was audited
		decisions = audit.list_decisions()
		assert len(decisions) == 1

	def test_ideate_insufficient_results(self, tmp_path):
		tracker = Tracker(tmp_path / "research.db")
		audit = AuditLog(tmp_path / "research.db")
		# No results seeded — should fail

		ideator = LLMIdeator(tracker=tracker, audit=audit, client=MagicMock())
		with pytest.raises(ValueError, match="Not enough data"):
			ideator.ideate(
				available_strategies=["A"],
				series_map={"data/kalshi-btc.db": ["SERIES_A"]},
				start_date="2025-01-01",
				end_date="2025-12-31",
			)


class TestCoverageTrackingIncludesDbPath:
	def test_ideation_prompt_coverage_includes_db_path(self):
		"""Untested combos should distinguish between same series in different DBs."""
		tracker = MagicMock()
		tracker.list_results.return_value = [
			{"strategy": "A", "series": "SERIES_A", "db_path": "data/kalshi-btc.db",
			 "verdict": "explore", "verdict_reason": "test", "sharpe": 1.2,
			 "win_rate": 0.55, "net_pnl_cents": 100, "total_trades": 80,
			 "tags": "[]", "validation_details": None},
		]

		ideator = LLMIdeator(tracker=tracker, audit=MagicMock(), client=MagicMock())

		series_map = {
			"data/kalshi-btc.db": ["SERIES_A"],
			"data/coinbase.db": ["SERIES_A"],  # same series name, different DB
		}

		prompt = ideator.build_ideation_prompt(["A"], series_map)

		# Should show 1 untested combo: A × SERIES_A in coinbase.db
		assert "1 remaining" in prompt or "Untested Combinations (1" in prompt


class TestSummarizeRefinements:
	def test_summarize_refinements_single_sample_is_neutral(self):
		"""A refinement group with only 1 result should not count as improved."""
		import json
		from edge_catcher.research.llm_ideator import LLMIdeator

		results = [
			{
				"strategy": "FooV2", "verdict": "explore", "sharpe": 1.5,
				"tags": json.dumps(["source:llm_refinement", "parent_strategy:Foo", "iteration:1"]),
				"total_trades": 80, "win_rate": 0.55, "net_pnl_cents": 200,
				"series": "X", "db_path": "d.db",
			}
		]

		summary = LLMIdeator._summarize_refinements(None, results)
		assert "Improved: 0/1" in summary or "Inconclusive: 1" in summary
