"""Tests for the generic test runner framework."""

import pytest
from edge_catcher.research.test_runner import (
	TestResult, StatisticalTest, TestRunner,
	EDGE_EXISTS, NO_EDGE, INSUFFICIENT_DATA, EDGE_NOT_TRADEABLE,
)


class TestTestResult:
	def test_construction(self):
		r = TestResult(
			verdict=EDGE_EXISTS,
			z_stat=3.5,
			fee_adjusted_edge=0.02,
			detail={"buckets": []},
		)
		assert r.verdict == EDGE_EXISTS
		assert r.z_stat == 3.5

	def test_verdict_constants(self):
		assert EDGE_EXISTS == "EDGE_EXISTS"
		assert NO_EDGE == "NO_EDGE"
		assert INSUFFICIENT_DATA == "INSUFFICIENT_DATA"
		assert EDGE_NOT_TRADEABLE == "EDGE_NOT_TRADEABLE"


class TestTestRunner:
	def test_unknown_test_type_raises(self):
		runner = TestRunner()
		with pytest.raises(ValueError, match="Unknown test type"):
			runner.run("nonexistent_type", None, "SER", {}, {})
