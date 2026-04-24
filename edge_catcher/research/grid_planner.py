# edge_catcher/research/grid_planner.py
"""Combinatorial grid planner: strategies × series × databases."""

from __future__ import annotations

import logging
from collections import Counter
from pathlib import Path

from .data_source_config import make_ds
from .hypothesis import Hypothesis
from .tracker import Tracker

logger = logging.getLogger(__name__)


# Per-series slippage defaults are NOT hardcoded here — tune them to your own
# observed spreads per series in your market. The framework uses a single
# conservative default. To override per-series, subclass GridPlanner and
# override slippage_for_series() or set a _SERIES_SLIPPAGE dict in
# config.local/slippage.py (not tracked).
_SERIES_SLIPPAGE: dict[str, int] = {}
_DEFAULT_SLIPPAGE = 2


def slippage_for_series(series: str) -> int:
	"""Return one-sided slippage in cents for a series, falling back to the
	conservative default. See `_SERIES_SLIPPAGE` above for provenance."""
	return _SERIES_SLIPPAGE.get(series, _DEFAULT_SLIPPAGE)


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
		force: bool = False,
	) -> list[Hypothesis]:
		"""Generate all strategy × series × db combos, deduped against Tracker.

		Returns hypotheses ordered: warm leads (strategies with prior
		promote/explore) first, then by least-tested series.
		"""
		if not strategies or not series_map:
			return []

		# Load all results once to avoid N+1 queries
		results = self.tracker.list_results()

		# Build set of already-tested dedup keys (skip when force=True)
		tested_keys: set[tuple] = set()
		if not force:
			for r in results:
				tested_keys.add((
					r["strategy"], r["series"], r["db_path"],
					r["start_date"], r["end_date"], r["fee_pct"],
				))

			# Also include hypotheses without results (pending from prior LLM runs)
			pending = self.tracker.list_pending()
			for p in pending:
				tested_keys.add((
					p["strategy"], p["series"], p["db_path"],
					p["start_date"], p["end_date"], p["fee_pct"],
				))

		# Find warm strategies (those with prior promote/explore results)
		warm_strategies = {
			r["strategy"] for r in results
			if r["verdict"] in ("promote", "explore")
		}

		# Count tested series for coverage ordering
		series_test_counts: dict[str, int] = Counter()
		for r in results:
			series_test_counts[r["series"]] += 1

		# Build the full grid, filtering against tested keys
		hypotheses: list[Hypothesis] = []
		for strategy in strategies:
			for db_path, series_list in series_map.items():
				for series in series_list:
					dedup_key = (strategy, series, db_path,
								 start_date, end_date, fee_pct)
					if dedup_key not in tested_keys:
						hypotheses.append(Hypothesis(
							strategy=strategy,
							data_sources=make_ds(db=Path(db_path).name, series=series),
							start_date=start_date,
							end_date=end_date,
							fee_pct=fee_pct,
							tags=["source:grid"],
							slippage_cents=slippage_for_series(series),
						))

		# Order: warm leads first, then by least-tested series
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
