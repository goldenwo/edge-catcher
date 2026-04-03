# edge_catcher/research/loop.py
"""LoopOrchestrator: sequences grid and LLM phases with budget controls."""

from __future__ import annotations

import importlib
import json
import logging
import re
import time
from pathlib import Path

from .agent import ResearchAgent
from .audit import AuditLog
from .grid_planner import GridPlanner
from .hypothesis import HypothesisResult
from .llm_ideator import LLMIdeator
from .reporter import Reporter
from .run_queue import RunQueue
from .tracker import Tracker

logger = logging.getLogger(__name__)


class LoopOrchestrator:
	def __init__(
		self,
		research_db: str = "data/research.db",
		start_date: str = "2025-01-01",
		end_date: str = "2025-12-31",
		max_runs: int = 100,
		max_time_minutes: float | None = None,
		parallel: int = 1,
		fee_pct: float = 1.0,
		max_llm_calls: int = 10,
		grid_only: bool = False,
		llm_only: bool = False,
		output_path: str | None = None,
	) -> None:
		if grid_only and llm_only:
			raise ValueError("Cannot use both --grid-only and --llm-only")

		self.research_db = research_db
		self.start_date = start_date
		self.end_date = end_date
		self.max_runs = max_runs
		self.max_time_seconds = max_time_minutes * 60 if max_time_minutes else None
		self.parallel = parallel
		self.fee_pct = fee_pct
		self.max_llm_calls = max_llm_calls
		self.grid_only = grid_only
		self.llm_only = llm_only
		self.output_path = output_path

		self.tracker = Tracker(research_db)
		self.audit = AuditLog(research_db)

	def run(self) -> tuple[int, list[HypothesisResult]]:
		"""Execute the research loop. Returns (exit_code, all_results).

		Exit codes: 0=completed, 1=error, 2=partial (budget exhausted).
		"""
		start_time = time.monotonic()
		self.audit.record_integrity(
			checkpoint="loop_start",
			result_hash="",
			result_count=len(self.tracker.list_results()),
		)

		agent = ResearchAgent(tracker=self.tracker)
		queue = RunQueue(agent=agent, audit=self.audit, parallel=self.parallel)

		strategies = self._discover_strategies()
		series_map = self._discover_series()

		all_results: list[HypothesisResult] = []
		runs_used = 0

		# ── Phase 1: Grid ─────────────────────────────────────────────────
		if not self.llm_only:
			planner = GridPlanner(tracker=self.tracker)
			grid_hypotheses = planner.generate(
				strategies=strategies,
				series_map=series_map,
				start_date=self.start_date,
				end_date=self.end_date,
				fee_pct=self.fee_pct,
			)

			budget_for_grid = self.max_runs
			grid_batch = grid_hypotheses[:budget_for_grid]

			remaining_time = self._remaining_time(start_time)
			grid_results = queue.submit(
				grid_batch, phase="grid", max_time_seconds=remaining_time,
			)
			all_results.extend(grid_results)
			runs_used += len(grid_results)

			logger.info(
				"Grid phase: %d/%d hypotheses completed",
				len(grid_results), len(grid_hypotheses),
			)

		# ── Integrity Checkpoint ──────────────────────────────────────────
		tracker_results = self.tracker.list_results()
		result_hash = self.audit.compute_result_hash(tracker_results)
		self.audit.record_integrity(
			checkpoint="post_grid",
			result_hash=result_hash,
			result_count=len(tracker_results),
		)

		# ── Phase 2: LLM ─────────────────────────────────────────────────
		llm_results: list[HypothesisResult] = []
		if not self.grid_only:
			remaining_budget = self.max_runs - runs_used
			if remaining_budget <= 0:
				logger.info("No budget remaining for LLM phase")
			elif len(tracker_results) < 10:
				if self.llm_only:
					logger.error(
						"Not enough data for LLM ideation (%d results, need >=10)",
						len(tracker_results),
					)
					return 1, all_results
				logger.info(
					"Skipping LLM phase: only %d results (need >=10)",
					len(tracker_results),
				)
			else:
				llm_results = self._run_llm_phase(
					agent, queue, strategies, series_map,
					remaining_budget, start_time,
				)
				all_results.extend(llm_results)
				runs_used += len(llm_results)

		# ── Report ────────────────────────────────────────────────────────
		self.audit.record_integrity(
			checkpoint="loop_end",
			result_hash=self.audit.compute_result_hash(
				self.tracker.list_results()
			),
			result_count=len(self.tracker.list_results()),
		)

		if all_results and self.output_path:
			reporter = Reporter()
			report = reporter.generate_report(all_results)
			reporter.save(report, self.output_path)

		# Determine exit code
		grid_remaining = 0
		if not self.llm_only:
			planner = GridPlanner(tracker=self.tracker)
			remaining_grid = planner.generate(
				strategies=strategies,
				series_map=series_map,
				start_date=self.start_date,
				end_date=self.end_date,
				fee_pct=self.fee_pct,
			)
			grid_remaining = len(remaining_grid)

		pending_llm = len(self.tracker.list_pending())
		has_remaining_work = grid_remaining > 0 or pending_llm > 0

		if has_remaining_work and runs_used >= self.max_runs:
			exit_code = 2  # partial — budget exhausted
		else:
			exit_code = 0

		logger.info(
			"Loop complete: %d runs, exit_code=%d, grid_remaining=%d, llm_pending=%d",
			runs_used, exit_code, grid_remaining, pending_llm,
		)
		return exit_code, all_results

	def _run_llm_phase(
		self,
		agent: ResearchAgent,
		queue: RunQueue,
		strategies: list[str],
		series_map: dict[str, list[str]],
		budget: int,
		start_time: float,
	) -> list[HypothesisResult]:
		"""Run the LLM ideation phase. Returns results from LLM-proposed hypotheses."""
		results: list[HypothesisResult] = []

		# First drain any pending (unexecuted) hypotheses from previous runs
		pending = self.tracker.list_pending()
		if pending:
			from .hypothesis import Hypothesis
			pending_hypotheses = [
				Hypothesis(
					id=p["id"],
					strategy=p["strategy"],
					series=p["series"],
					db_path=p["db_path"],
					start_date=p["start_date"],
					end_date=p["end_date"],
					fee_pct=p["fee_pct"],
					tags=json.loads(p["tags"]) if isinstance(p["tags"], str) else (p["tags"] or []),
				)
				for p in pending
			]
			batch = pending_hypotheses[:budget]
			remaining_time = self._remaining_time(start_time)
			pending_results = queue.submit(
				batch, phase="llm", max_time_seconds=remaining_time,
			)
			results.extend(pending_results)
			budget -= len(pending_results)

		if budget <= 0:
			return results

		# Verify integrity before ideation
		current_results = self.tracker.list_results()
		current_hash = self.audit.compute_result_hash(current_results)
		integrity_checks = self.audit.list_integrity_checks()
		post_grid = [c for c in integrity_checks if c["checkpoint"] == "post_grid"]
		if post_grid and post_grid[0]["result_hash"] != current_hash:
			logger.error("Integrity check failed: results modified since grid phase")
			return results

		# Run LLM ideation
		try:
			from edge_catcher.ai.client import LLMClient
			client = LLMClient()
			ideator = LLMIdeator(
				tracker=self.tracker, audit=self.audit, client=client,
			)
			hypotheses, novel_proposals = ideator.ideate(
				available_strategies=strategies,
				series_map=series_map,
				start_date=self.start_date,
				end_date=self.end_date,
				fee_pct=self.fee_pct,
			)
		except Exception as exc:
			logger.error("LLM ideation failed: %s", exc)
			return results

		# Process novel strategy proposals (generate code via strategizer)
		llm_calls_used = 1  # the ideation call itself
		for proposal in novel_proposals:
			if llm_calls_used >= self.max_llm_calls:
				break
			try:
				hypotheses.extend(
					self._generate_novel_strategy(
						proposal, client, strategies, series_map,
					)
				)
				llm_calls_used += 1
			except Exception as exc:
				logger.warning("Novel strategy generation failed: %s", exc)

		# Persist all LLM hypotheses so they can be resumed
		for h in hypotheses:
			self.tracker.save_hypothesis(h)

		# Run them
		batch = hypotheses[:budget]
		remaining_time = self._remaining_time(start_time)
		llm_results = queue.submit(
			batch, phase="llm", max_time_seconds=remaining_time,
		)
		results.extend(llm_results)

		return results

	def _generate_novel_strategy(
		self,
		proposal: dict,
		client,
		strategies: list[str],
		series_map: dict[str, list[str]],
	) -> list["Hypothesis"]:
		"""Generate strategy code from a novel proposal and return hypotheses to test."""
		from edge_catcher.ai.strategizer import _parse_strategy_response
		from edge_catcher.runner.strategy_parser import (
			validate_strategy_code, save_strategy, list_strategies,
			STRATEGIES_LOCAL_PATH, STRATEGIES_LOCAL_MODULE,
		)
		from .hypothesis import Hypothesis

		# Use the strategizer prompt directly with the client
		system_prompt = (
			Path(__file__).parent.parent / "ai" / "prompts" / "strategizer_system.txt"
		).read_text()
		user_prompt = (
			f"Generate a trading strategy based on this idea:\n\n"
			f"**Name:** {proposal['name']}\n"
			f"**Description:** {proposal['description']}\n"
			f"**Rationale:** {proposal.get('rationale', '')}\n\n"
			f"Generate a strategy class that trades this edge."
		)

		response = client.complete(system_prompt, user_prompt, task="strategizer")

		# Extract code and strategy name using the existing strategizer parser
		try:
			code, strategy_name = _parse_strategy_response(response)
		except ValueError as exc:
			logger.warning("Failed to parse strategizer response: %s", exc)
			return []

		# Validate
		ok, error = validate_strategy_code(code)
		if not ok:
			logger.warning("Generated strategy failed validation: %s", error)
			return []

		# Save to strategies_local.py
		result = save_strategy(code, strategy_name, STRATEGIES_LOCAL_PATH)
		if not result.get("ok"):
			logger.warning("Failed to save strategy: %s", result.get("error"))
			return []

		# Reload module to pick up new strategy
		try:
			mod = importlib.import_module(STRATEGIES_LOCAL_MODULE)
			importlib.reload(mod)
		except Exception as exc:
			logger.warning("Failed to reload strategies_local: %s", exc)

		# Generate hypotheses for the new strategy across available data
		hypotheses: list[Hypothesis] = []
		for db_path, series_list in series_map.items():
			for series in series_list[:3]:  # limit to 3 series per novel strategy
				hypotheses.append(Hypothesis(
					strategy=strategy_name,
					series=series,
					db_path=db_path,
					start_date=self.start_date,
					end_date=self.end_date,
					fee_pct=self.fee_pct,
					tags=["source:llm_novel_strategy"],
					notes=proposal.get("description", ""),
				))

		logger.info("Generated novel strategy '%s' with %d hypotheses",
					strategy_name, len(hypotheses))
		return hypotheses

	def _discover_strategies(self) -> list[str]:
		"""Discover available strategy names via strategy_parser."""
		from edge_catcher.runner.strategy_parser import (
			list_strategies, STRATEGIES_PUBLIC_PATH, STRATEGIES_LOCAL_PATH,
		)
		strats = list_strategies(STRATEGIES_PUBLIC_PATH)
		strats += list_strategies(STRATEGIES_LOCAL_PATH)
		names = [s["name"] for s in strats if s["name"] != "example"]
		return list(dict.fromkeys(names))  # dedupe preserving order

	def _discover_series(self) -> dict[str, list[str]]:
		"""Discover available databases and series."""
		agent = ResearchAgent(tracker=self.tracker)
		return agent._discover_all_series()

	def _remaining_time(self, start_time: float) -> float | None:
		if self.max_time_seconds is None:
			return None
		elapsed = time.monotonic() - start_time
		return max(0.0, self.max_time_seconds - elapsed)
