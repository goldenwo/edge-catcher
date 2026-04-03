# edge_catcher/research/grid_planner.py
"""Combinatorial grid planner: strategies × series × databases."""

from __future__ import annotations

import logging
from collections import Counter

from .hypothesis import Hypothesis
from .tracker import Tracker

logger = logging.getLogger(__name__)


class GridPlanner:
	def __init__(self, tracker: Tracker) -> None:
		self.tracker = tracker

	def generate(
		self,
		strategies: list[str],
		series_map: dict[str, list[str]],
		start_date: str,
		end_date: str,
		fee_pct: float = 1.0,
	) -> list[Hypothesis]:
		"""Generate all strategy × series × db combos, deduped against Tracker.

		Returns hypotheses ordered: warm leads (strategies with prior
		promote/explore) first, then by least-tested series.
		"""
		if not strategies or not series_map:
			return []

		# Find warm strategies (those with prior promote/explore results)
		warm_strategies = self._find_warm_strategies()

		# Build the full grid
		hypotheses: list[Hypothesis] = []
		for strategy in strategies:
			for db_path, series_list in series_map.items():
				for series in series_list:
					h = Hypothesis(
						strategy=strategy,
						series=series,
						db_path=db_path,
						start_date=start_date,
						end_date=end_date,
						fee_pct=fee_pct,
						tags=["source:grid"],
					)
					if not self.tracker.is_tested(h):
						hypotheses.append(h)

		# Order: warm leads first, then by least-tested series
		series_test_counts = self._count_tested_series()
		hypotheses.sort(
			key=lambda h: (
				0 if h.strategy in warm_strategies else 1,
				series_test_counts.get(h.series, 0),
			)
		)

		logger.info(
			"GridPlanner: %d hypotheses (%d strategies × %d db/series combos, after dedup)",
			len(hypotheses), len(strategies),
			sum(len(sl) for sl in series_map.values()),
		)
		return hypotheses

	def _find_warm_strategies(self) -> set[str]:
		"""Return strategy names that have at least one promote or explore result."""
		results = self.tracker.list_results()
		return {
			r["strategy"] for r in results
			if r["verdict"] in ("promote", "explore")
		}

	def _count_tested_series(self) -> dict[str, int]:
		"""Return {series: count_of_tests} for ordering by coverage."""
		results = self.tracker.list_results()
		counts: dict[str, int] = Counter()
		for r in results:
			counts[r["series"]] += 1
		return counts
