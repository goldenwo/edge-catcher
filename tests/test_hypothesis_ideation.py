"""Tests for LLMIdeator.ideate_hypotheses()."""

import json
import pytest
from unittest.mock import MagicMock
from edge_catcher.research.llm_ideator import LLMIdeator


@pytest.fixture
def ideator():
	tracker = MagicMock()
	tracker.list_hypothesis_results.return_value = []
	tracker.list_hypothesis_kills.return_value = []
	audit = MagicMock()
	client = MagicMock()
	client._resolve_model.return_value = "test-model"
	client.last_usage = {"input_tokens": 100, "output_tokens": 200}
	journal = MagicMock()
	journal.get_recent_entries.return_value = []
	return LLMIdeator(tracker=tracker, audit=audit, client=client, journal=journal)


class TestIdeateHypotheses:
	def test_returns_list_of_configs(self, ideator):
		ideator.client.complete.return_value = json.dumps({
			"analysis": "test",
			"hypotheses": [{
				"test_type": "price_bucket_bias",
				"series": "SER_A", "db": "kalshi.db",
				"rationale": "test",
				"params": {"buckets": [[0.01, 0.10]], "min_n_per_bucket": 30, "fee_model": "standard"},
				"thresholds": {"clustered_z_stat": 3.0, "min_fee_adjusted_edge": 0.0},
			}],
		})
		result = ideator.ideate_hypotheses(
			context_block="test context",
			hypothesis_kill_registry=[],
			journal=ideator.journal,
			available_test_types=["price_bucket_bias", "lifecycle_bias"],
		)
		assert len(result) == 1
		assert result[0]["test_type"] == "price_bucket_bias"

	def test_filters_killed_patterns(self, ideator):
		ideator.client.complete.return_value = json.dumps({
			"analysis": "test",
			"hypotheses": [
				{"test_type": "price_bucket_bias", "series": "SER_A", "db": "k.db",
				 "rationale": "t", "params": {}, "thresholds": {}},
				{"test_type": "lifecycle_bias", "series": "SER_B", "db": "k.db",
				 "rationale": "t", "params": {}, "thresholds": {}},
			],
		})
		killed = [{"pattern_key": "price_bucket_bias:SER_A:k.db", "permanent": 1}]
		result = ideator.ideate_hypotheses(
			context_block="test",
			hypothesis_kill_registry=killed,
			journal=ideator.journal,
			available_test_types=["price_bucket_bias", "lifecycle_bias"],
		)
		assert len(result) == 1
		assert result[0]["test_type"] == "lifecycle_bias"

	def test_filters_invalid_test_type(self, ideator):
		ideator.client.complete.return_value = json.dumps({
			"analysis": "test",
			"hypotheses": [
				{"test_type": "nonexistent_type", "series": "S", "db": "k.db",
				 "rationale": "t", "params": {}, "thresholds": {}},
			],
		})
		result = ideator.ideate_hypotheses(
			context_block="test",
			hypothesis_kill_registry=[],
			journal=ideator.journal,
			available_test_types=["price_bucket_bias"],
		)
		assert result == []

	def test_invalid_json_returns_empty(self, ideator):
		ideator.client.complete.return_value = "not json"
		result = ideator.ideate_hypotheses(
			context_block="test",
			hypothesis_kill_registry=[],
			journal=ideator.journal,
			available_test_types=["price_bucket_bias"],
		)
		assert result == []

	def test_records_audit_decision(self, ideator):
		ideator.client.complete.return_value = json.dumps({
			"analysis": "test", "hypotheses": [],
		})
		ideator.ideate_hypotheses(
			context_block="test",
			hypothesis_kill_registry=[],
			journal=ideator.journal,
			available_test_types=["price_bucket_bias"],
		)
		ideator.audit.record_decision.assert_called_once()
