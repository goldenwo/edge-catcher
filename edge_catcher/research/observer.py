# edge_catcher/research/observer.py
"""ResearchObserver: journal/registry side-effects extracted from LoopOrchestrator."""

from __future__ import annotations

import json
import logging
from collections import Counter, defaultdict
from typing import TYPE_CHECKING

from .hypothesis import HypothesisResult
from .tracker import Tracker

if TYPE_CHECKING:
	from .journal import ResearchJournal

logger = logging.getLogger(__name__)


class ResearchObserver:
	def __init__(
		self,
		tracker: Tracker,
		run_id: str,
	) -> None:
		self.tracker = tracker
		self.run_id = run_id

	# ------------------------------------------------------------------
	# Phase outcomes
	# ------------------------------------------------------------------

	def write_phase_outcomes(
		self,
		journal: "ResearchJournal",
		results: list[HypothesisResult],
		phase: str,
	) -> None:
		"""Write outcome journal entries — one per strategy, aggregated across series."""
		by_strategy: dict[str, list[HypothesisResult]] = defaultdict(list)
		for r in results:
			by_strategy[r.hypothesis.strategy].append(r)

		for strategy, strat_results in by_strategy.items():
			verdicts: dict[str, int] = defaultdict(int)
			series_list = []
			best_sharpe = 0.0
			for r in strat_results:
				verdicts[r.verdict] += 1
				series_list.append(r.hypothesis.series)
				if r.status == "ok":
					best_sharpe = max(best_sharpe, r.sharpe)

			journal.write_entry(self.run_id, "outcome", {
				"phase": phase,
				"strategy": strategy,
				"series": series_list,
				"verdicts": dict(verdicts),
				"best_sharpe": best_sharpe,
			})

		# Near-miss observation: highest-Sharpe killed strategy
		all_kills = [r for r in results if r.verdict == "kill" and r.status == "ok"]
		if all_kills:
			best_kill = max(all_kills, key=lambda r: r.sharpe)
			# Fetch validation details from tracker
			val_details_str = ""
			tracker_result = self.tracker.get_result_by_id(best_kill.hypothesis.id)
			if tracker_result and tracker_result.get("validation_details"):
				try:
					gates = json.loads(tracker_result["validation_details"])
					failed_gates = [g for g in gates if not g.get("passed", True)]
					if failed_gates:
						gate = failed_gates[0]
						val_details_str = f" failed {gate['gate']}: {json.dumps(gate.get('details', {}))}"
				except (json.JSONDecodeError, KeyError):
					pass

			journal.write_entry(self.run_id, "observation", {
				"pattern": (
					f"NEAR-MISS: {best_kill.hypothesis.strategy} scored Sharpe "
					f"{best_kill.sharpe:.2f} but was killed{val_details_str}"
				),
				"evidence": (
					f"series={best_kill.hypothesis.series}, trades={best_kill.total_trades}, "
					f"verdict_reason={best_kill.verdict_reason}"
				),
			})

	# ------------------------------------------------------------------
	# Kill registry
	# ------------------------------------------------------------------

	def update_kill_registry(self) -> None:
		"""Upsert strategies with kill_rate >= 0.8 across >= 3 series into the kill registry.

		Excludes strategies with any promote or review verdict.
		"""
		results = self.tracker.list_results()
		by_strategy: dict[str, list[dict]] = defaultdict(list)
		for r in results:
			by_strategy[r["strategy"]].append(r)

		for strategy, strat_results in by_strategy.items():
			verdicts = [r["verdict"] for r in strat_results]
			if "promote" in verdicts or "review" in verdicts:
				continue

			series_tested = len(set(r["series"] for r in strat_results))
			if series_tested < 3:
				continue

			kill_count = verdicts.count("kill")
			kill_rate = kill_count / len(strat_results)
			if kill_rate < 0.8:
				continue

			reasons = [r["verdict_reason"] for r in strat_results if r["verdict"] == "kill"]
			reason_counts = Counter(reasons).most_common(5)
			reason_summary = json.dumps([f"{reason}: {count}x" for reason, count in reason_counts])

			self.tracker.upsert_kill_registry(
				strategy=strategy,
				kill_count=kill_count,
				series_tested=series_tested,
				kill_rate=kill_rate,
				reason_summary=reason_summary,
			)

	# ------------------------------------------------------------------
	# Dead strategy cleanup
	# ------------------------------------------------------------------

	def cleanup_dead_strategies(self) -> None:
		"""Remove dead strategy code from strategies_local.py."""
		from edge_catcher.runner.strategy_parser import (
			cleanup_dead_strategies, STRATEGIES_LOCAL_PATH, list_strategies,
		)

		registry = self.tracker.list_kill_registry(permanent_only=True)
		if not registry:
			return

		# Safety check: exclude strategies with any promote/review verdict
		results = self.tracker.list_results()
		has_good_verdict = set()
		for r in results:
			if r["verdict"] in ("promote", "review"):
				has_good_verdict.add(r["strategy"])

		# Also exclude parents of active refinement chains
		for r in results:
			strategy = r["strategy"]
			if r["verdict"] != "kill":
				# Check if this is a refinement (e.g., StratV2 → parent is Strat)
				for suffix in ("V2", "V3", "V4", "V5", "V6", "V7", "V8", "V9", "V10"):
					if strategy.endswith(suffix):
						parent = strategy[:-len(suffix)]
						has_good_verdict.add(parent)

		dead_strategy_names = [
			e["strategy"] for e in registry
			if e["strategy"] not in has_good_verdict
		]

		if not dead_strategy_names:
			return

		# Map strategy names (snake_case) to class names (CamelCase)
		known = list_strategies(file_path=STRATEGIES_LOCAL_PATH)
		name_to_class = {s["name"]: s["class_name"] for s in known}
		dead_class_names = [
			name_to_class[n] for n in dead_strategy_names
			if n in name_to_class
		]

		if dead_class_names:
			removed = cleanup_dead_strategies(STRATEGIES_LOCAL_PATH, dead_class_names)
			if removed:
				logger.info("Cleaned up %d dead strategies from strategies_local.py: %s", len(removed), removed)

	# ------------------------------------------------------------------
	# Journal summary
	# ------------------------------------------------------------------

	def write_journal_summary(
		self,
		journal: "ResearchJournal",
		all_results: list[HypothesisResult],
		prev_content: dict | None = None,
	) -> str:
		"""Write trajectory + observation entries at end of run. Returns trajectory_status."""
		from .journal import ResearchJournal
		from .loop import LoopOrchestrator

		# Build result dicts for trajectory classification
		run_results = [
			{"run_id": self.run_id, "verdict": r.verdict, "sharpe": r.sharpe}
			for r in all_results if r.status == "ok"
		]

		# Compute trajectory using the provided prev_content (avoid duplicate DB read)
		prev_trajectory = prev_content
		trajectory_status = ResearchJournal.classify_trajectory(
			self.run_id, run_results, prev_trajectory,
		)

		# Write trajectory entry
		journal.write_entry(self.run_id, "trajectory", {
			"status": trajectory_status,
			"total_sessions": prev_trajectory.get("total_sessions", 0) + 1 if prev_trajectory else 1,
			"promote_rate": (
				sum(1 for r in all_results if r.verdict in ("promote", "review"))
				/ max(len(all_results), 1)
			),
			"promote_rate_prev": prev_trajectory.get("promote_rate") if prev_trajectory else None,
			"best_sharpe_this_run": max((r.sharpe for r in all_results if r.status == "ok"), default=0.0),
			"best_sharpe_overall": max(
				max((r.sharpe for r in all_results if r.status == "ok"), default=0.0),
				prev_trajectory.get("best_sharpe_overall", 0.0) if prev_trajectory else 0.0,
			),
			"new_promotes": sum(1 for r in all_results if r.verdict == "promote"),
			"new_reviews": sum(1 for r in all_results if r.verdict == "review"),
			"new_explores": sum(1 for r in all_results if r.verdict == "explore"),
			"new_kills": sum(1 for r in all_results if r.verdict == "kill"),
			"consecutive_stuck": LoopOrchestrator._compute_consecutive_stuck(trajectory_status, prev_content),
		})

		# Write observation entries for promoted results
		for r in all_results:
			if r.verdict in ("promote", "review"):
				# Fetch gate margins for richer observations
				gate_summary = ""
				tracker_result = self.tracker.get_result_by_id(r.hypothesis.id)
				if tracker_result and tracker_result.get("validation_details"):
					try:
						gates = json.loads(tracker_result["validation_details"])
						gate_parts = []
						for g in gates:
							if g.get("passed"):
								d = g.get("details", {})
								name = g["gate"]
								if name == "monte_carlo" and "p_value" in d:
									gate_parts.append(f"mc_p={d['p_value']:.2f}")
								elif name == "deflated_sharpe" and "dsr_margin" in d:
									gate_parts.append(f"dsr={d['dsr_margin']:.2f}")
								elif name == "temporal_consistency":
									pw = d.get("profitable_windows", "?")
									tw = d.get("total_windows", "?")
									gate_parts.append(f"temporal={pw}/{tw}")
								elif name == "param_sensitivity":
									np_ = d.get("neighbors_passing", "?")
									nt = d.get("neighbors_tested", "?")
									gate_parts.append(f"sensitivity={np_}/{nt}")
						if gate_parts:
							gate_summary = f" [{', '.join(gate_parts)}]"
					except (json.JSONDecodeError, KeyError):
						pass

				journal.write_entry(self.run_id, "observation", {
					"pattern": f"PROMOTED: {r.hypothesis.strategy} succeeds with Sharpe {r.sharpe:.2f}{gate_summary}",
					"evidence": (
						f"trades={r.total_trades}, win_rate={r.win_rate:.0%}, "
						f"series={r.hypothesis.series}, pnl={r.net_pnl_cents:.0f}¢"
					),
				})

		# Write observation entries for high kill-rate strategies
		kill_counts: dict[str, dict] = defaultdict(lambda: {"kills": 0, "total": 0})
		for r in all_results:
			kill_counts[r.hypothesis.strategy]["total"] += 1
			if r.verdict == "kill":
				kill_counts[r.hypothesis.strategy]["kills"] += 1
		for strategy, counts in kill_counts.items():
			if counts["total"] >= 3 and counts["kills"] / counts["total"] > 0.8:
				journal.write_entry(self.run_id, "observation", {
					"pattern": f"strategy {strategy} killed on {counts['kills']}/{counts['total']} series",
					"evidence": "high kill rate suggests fundamental issue with strategy logic",
				})

		# Write observation entries for low trade-count strategies
		trade_counts: dict[str, list[int]] = defaultdict(list)
		for r in all_results:
			if r.status == "ok":
				trade_counts[r.hypothesis.strategy].append(r.total_trades)
		for strategy, trades_list in trade_counts.items():
			if len(trades_list) >= 2 and sum(trades_list) / len(trades_list) < 50:
				avg = sum(trades_list) / len(trades_list)
				journal.write_entry(self.run_id, "observation", {
					"pattern": f"strategy {strategy} averaged {avg:.0f} trades across {len(trades_list)} series",
					"evidence": "low trade count may indicate overly restrictive entry conditions",
				})

		return trajectory_status
