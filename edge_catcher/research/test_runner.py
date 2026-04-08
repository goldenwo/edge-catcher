"""Generic statistical test runner for hypothesis validation."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import ClassVar

# Verdict constants
EDGE_EXISTS = "EDGE_EXISTS"
NO_EDGE = "NO_EDGE"
INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
EDGE_NOT_TRADEABLE = "EDGE_NOT_TRADEABLE"


@dataclass
class TestResult:
	"""Result of a statistical hypothesis test."""
	verdict: str
	z_stat: float
	fee_adjusted_edge: float
	detail: dict


class StatisticalTest:
	"""Base class for all statistical test types."""
	name: ClassVar[str] = ""

	def run(
		self,
		conn: sqlite3.Connection,
		series: str,
		params: dict,
		thresholds: dict,
	) -> TestResult:
		raise NotImplementedError


class TestRunner:
	"""Dispatches hypothesis configs to the appropriate StatisticalTest."""

	def __init__(self) -> None:
		self.test_types: dict[str, StatisticalTest] = {}
		self._register_defaults()

	def _register_defaults(self) -> None:
		"""Register built-in test types. Called during __init__."""
		# Test types registered as they are implemented in Tasks 3-6
		pass

	def register(self, name: str, test: StatisticalTest) -> None:
		self.test_types[name] = test

	def run(
		self,
		test_type: str,
		conn: sqlite3.Connection,
		series: str,
		params: dict,
		thresholds: dict,
	) -> TestResult:
		if test_type not in self.test_types:
			raise ValueError(f"Unknown test type: {test_type}. Available: {list(self.test_types.keys())}")
		return self.test_types[test_type].run(conn, series, params, thresholds)
