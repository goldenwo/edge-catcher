# edge_catcher/research/loop.py
"""LoopOrchestrator: sequences ideate, expand, and refine phases with budget controls."""

from __future__ import annotations

import hashlib
import importlib
import json
import logging
import math
import re
import sys
import threading
import time
import uuid
from collections import Counter, defaultdict
from pathlib import Path
from typing import Callable

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
		start_date: str | None = None,
		end_date: str | None = None,
		max_runs: int = 0,
		max_time_minutes: float | None = None,
		parallel: int = 1,
		fee_pct: float = 1.0,
		max_llm_calls: int = 10,
		grid_only: bool = False,
		llm_only: bool = False,
		output_path: str | None = None,
		force: bool = False,
		max_refinements: int = 3,
		refine_only: bool = False,
		max_stuck_runs: int = 3,
		cancel_event: threading.Event | None = None,
		on_progress: Callable[[str, int, int], None] | None = None,
	) -> None:
		if grid_only and llm_only:
			raise ValueError("Cannot use both --grid-only and --llm-only")

		self.research_db = research_db
		self.start_date = start_date
		self.end_date = end_date
		self.max_runs = max_runs if max_runs > 0 else sys.maxsize
		self.max_time_seconds = max_time_minutes * 60 if max_time_minutes else None
		self.parallel = parallel
		self.fee_pct = fee_pct
		self.max_llm_calls = max_llm_calls
		self.grid_only = grid_only
		self.llm_only = llm_only
		self.output_path = output_path
		self.force = force
		self.max_refinements = max_refinements
		self.refine_only = refine_only
		self.max_stuck_runs = max_stuck_runs
		self.cancel_event = cancel_event
		self.on_progress = on_progress

		self.tracker = Tracker(research_db)
		self.audit = AuditLog(research_db)
		self.run_id = str(uuid.uuid4())
		self._cached_results: list[dict] | None = None

	def _list_results(self, refresh: bool = False) -> list[dict]:
		"""Return cached tracker results, refreshing if requested."""
		if self._cached_results is None or refresh:
			self._cached_results = self.tracker.list_results()
		return self._cached_results

	@staticmethod
	def _compute_consecutive_stuck(
		trajectory_status: str,
		prev_trajectory: dict | None,
	) -> int:
		"""Compute consecutive stuck counter from current status and previous trajectory."""
		prev_count = (prev_trajectory or {}).get("consecutive_stuck", 0)
		if trajectory_status == "improving":
			return 0
		return prev_count + 1

	@staticmethod
	def _compute_budgets(
		max_runs: int,
		grid_only: bool,
		consecutive_stuck: int = 0,
	) -> dict[str, int]:
		"""Compute phase budgets, shifting toward ideation when stuck."""
		if grid_only:
			return {"ideate": 0, "expand": max_runs, "refine": 0}

		if consecutive_stuck >= 2:
			# Shift: 60% ideate, 20% expand, 20% refine
			budget_ideate = max(1, int(max_runs * 0.6))
			budget_expand = max(1, int(max_runs * 0.2))
		else:
			# Normal: 40% ideate, 40% expand, 20% refine
			budget_ideate = max(1, int(max_runs * 0.4))
			budget_expand = max(1, int(max_runs * 0.4))

		budget_refine = max_runs - budget_ideate - budget_expand
		return {"ideate": budget_ideate, "expand": budget_expand, "refine": budget_refine}

	def run(self) -> tuple[int, list[HypothesisResult]]:
		"""Execute the research loop. Returns (exit_code, all_results).

		Exit codes: 0=completed, 1=error, 2=partial (budget exhausted).
		"""
		from .journal import ResearchJournal
		journal = ResearchJournal(db_path=self.research_db)
		self._journal = journal

		start_time = time.monotonic()
		self.audit.record_integrity(
			checkpoint="loop_start",
			result_hash="",
			result_count=len(self._list_results()),
		)

		agent = ResearchAgent(tracker=self.tracker, force=self.force)
		queue = RunQueue(agent=agent, audit=self.audit, parallel=self.parallel)

		strategies = self._discover_strategies()
		series_map = self._discover_series()

		all_results: list[HypothesisResult] = []
		runs_used = 0
		llm_calls_used = 0

		# ── Read previous trajectory for stuck detection ──────────────────
		prev_trajectory = journal.get_latest_trajectory()
		prev_content = prev_trajectory  # already a dict or None
		self._consecutive_stuck = (prev_content or {}).get("consecutive_stuck", 0)

		# ── Budget allocation ────────────────────────────────────────────
		budgets = self._compute_budgets(self.max_runs, self.grid_only, self._consecutive_stuck)
		budget_ideate = budgets["ideate"]
		budget_expand = budgets["expand"]
		budget_refine = budgets["refine"]

		if self._consecutive_stuck >= 2:
			logger.info(
				"Stuck detected (%d consecutive) — shifting to exploration-heavy budget",
				self._consecutive_stuck,
			)

		# ── Phase 1: Context + Ideate (or Grid if grid_only) ─────────
		if self._cancelled():
			return 2, all_results
		if self.grid_only:
			# Legacy: full grid sweep
			planner = GridPlanner(tracker=self.tracker)
			grid_hypotheses = planner.generate(
				strategies=strategies,
				series_map=series_map,
				start_date=self.start_date,
				end_date=self.end_date,
				fee_pct=self.fee_pct,
				force=self.force,
			)
			grid_batch = grid_hypotheses[:budget_expand]
			remaining_time = self._remaining_time(start_time)
			grid_results = queue.submit(
				grid_batch, phase="grid", max_time_seconds=remaining_time,
			)
			all_results.extend(grid_results)
			runs_used += len(grid_results)
			self._write_phase_outcomes(journal, grid_results, "grid")
			self._update_kill_registry()
			self._report_progress("grid", len(all_results), self.max_runs)
		elif not self.refine_only:
			ideate_results, llm_calls_used = self._run_ideate_phase(
				agent, queue, strategies, series_map,
				budget_ideate, start_time,
			)
			all_results.extend(ideate_results)
			runs_used += len(ideate_results)
			self._write_phase_outcomes(journal, ideate_results, "ideate")
			self._update_kill_registry()
			self._report_progress("ideate", len(all_results), self.max_runs)

		# ── Integrity Checkpoint ─────────────────────────────────────
		tracker_results = self._list_results(refresh=True)
		result_hash = self.audit.compute_result_hash(tracker_results)
		checkpoint_name = "post_grid" if self.grid_only else "post_ideate"
		self.audit.record_integrity(
			checkpoint=checkpoint_name,
			result_hash=result_hash,
			result_count=len(tracker_results),
		)

		# ── Phase 2: Expand Winners ──────────────────────────────────
		if self._cancelled():
			return 2, all_results
		if not self.grid_only and not self.refine_only and not self.llm_only:
			remaining_budget = min(budget_expand, self.max_runs - runs_used)
			if remaining_budget > 0:
				expand_results = self._run_expand_phase(
					agent, queue, all_results, series_map,
					remaining_budget, start_time,
				)
				all_results.extend(expand_results)
				runs_used += len(expand_results)
				self._write_phase_outcomes(journal, expand_results, "expand")
				self._update_kill_registry()
				self._report_progress("expand", len(all_results), self.max_runs)

		# ── Phase 3: Refine ──────────────────────────────────────────
		if self._cancelled():
			return 2, all_results
		if not self.grid_only:
			remaining_budget = min(budget_refine, self.max_runs - runs_used)
			remaining_llm = self.max_llm_calls - llm_calls_used
			if remaining_budget > 0 and remaining_llm > 0:
				refine_results = self._run_refinement_phase(
					agent, queue, series_map,
					remaining_budget, remaining_llm, start_time,
				)
				all_results.extend(refine_results)
				runs_used += len(refine_results)
				self._write_phase_outcomes(journal, refine_results, "refine")
				self._update_kill_registry()
				self._report_progress("refine", len(all_results), self.max_runs)

		# ── Journal summary ───────────────────────────────────────────────
		trajectory_status = self._write_journal_summary(journal, all_results, prev_content)

		# ── Circuit breaker check ────────────────────────────────────────
		new_stuck_count = self._compute_consecutive_stuck(trajectory_status, prev_content)
		circuit_breaker_tripped = (
			self.max_stuck_runs > 0
			and new_stuck_count >= self.max_stuck_runs + 2
		)

		# ── Report ────────────────────────────────────────────────────────
		end_results = self._list_results(refresh=True)
		self.audit.record_integrity(
			checkpoint="loop_end",
			result_hash=self.audit.compute_result_hash(end_results),
			result_count=len(end_results),
		)

		if all_results and self.output_path:
			reporter = Reporter()
			report = reporter.generate_report(all_results)
			reporter.save(report, self.output_path)

		# Dead code cleanup
		if not self.grid_only:
			self._cleanup_dead_strategies()

		if circuit_breaker_tripped:
			# +2 because the budget shift kicks in at 2, then we allow max_stuck_runs more
			logger.error(
				"Loop terminated: stuck for %d consecutive runs with no promotes. "
				"Review kill registry and consider new data sources or manual reset.",
				new_stuck_count,
			)
			return 3, all_results

		# Determine exit code
		grid_remaining = 0
		if self.grid_only:
			planner = GridPlanner(tracker=self.tracker)
			remaining_grid = planner.generate(
				strategies=strategies,
				series_map=series_map,
				start_date=self.start_date,
				end_date=self.end_date,
				fee_pct=self.fee_pct,
				force=self.force,
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

	def _run_ideate_phase(
		self,
		agent: ResearchAgent,
		queue: RunQueue,
		strategies: list[str],
		series_map: dict[str, list[str]],
		budget: int,
		start_time: float,
	) -> tuple[list[HypothesisResult], int]:
		"""Run the LLM ideation phase with context engine. Returns (results, llm_calls_used)."""
		from .context_engine import ContextEngine
		results: list[HypothesisResult] = []

		# Build market context from the same DB paths the loop already knows about
		db_paths = list(series_map.keys())
		data_dir = str(Path(db_paths[0]).parent) if db_paths else "data"
		engine = ContextEngine(data_dir=data_dir)
		profiles = engine.profile_all(db_paths)
		context_block = engine.build_context_block(profiles)

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
			return results, 0

		# Verify integrity before ideation — compare against the most recent
		# post_ideate checkpoint from this run (list is DESC, so last appended = first)
		current_results = self._list_results(refresh=True)
		current_hash = self.audit.compute_result_hash(current_results)
		integrity_checks = self.audit.list_integrity_checks()
		# Find the post_ideate checkpoint that was recorded AFTER the most recent
		# loop_start (i.e., from this invocation, not a prior one)
		loop_starts = [c for c in integrity_checks if c["checkpoint"] == "loop_start"]
		this_run_start = loop_starts[0]["created_at"] if loop_starts else ""
		post_ideate = [
			c for c in integrity_checks
			if c["checkpoint"] == "post_ideate" and c["created_at"] >= this_run_start
		]
		if post_ideate and post_ideate[0]["result_hash"] != current_hash:
			logger.error("Integrity check failed: results modified since grid phase")
			return results, 0

		# Run LLM ideation
		try:
			from edge_catcher.ai.client import LLMClient
			client = LLMClient()
			ideator = LLMIdeator(
				tracker=self.tracker, audit=self.audit, client=client,
				journal=self._journal,
			)
			logger.info("Starting LLM ideation call...")
			hypotheses, novel_proposals = ideator.ideate(
				available_strategies=strategies,
				series_map=series_map,
				start_date=self.start_date,
				end_date=self.end_date,
				fee_pct=self.fee_pct,
				context_block=context_block,
				profiles=profiles,
			)
		except Exception as exc:
			logger.error("LLM ideation failed: %s", exc)
			return results, 0

		# Process novel strategy proposals (generate code via strategizer)
		logger.info(
			"LLM ideation returned %d existing hypotheses, %d novel proposals",
			len(hypotheses), len(novel_proposals),
		)
		llm_calls_used = 1  # the ideation call itself
		for proposal in novel_proposals:
			if llm_calls_used >= self.max_llm_calls:
				break
			logger.info("Generating novel strategy %d/%d: %s",
				llm_calls_used, min(len(novel_proposals), self.max_llm_calls - 1),
				proposal.get("name", "unknown"),
			)
			try:
				hypotheses.extend(
					self._generate_novel_strategy(
						proposal, client, strategies, series_map,
					)
				)
				llm_calls_used += 1
			except Exception as exc:
				logger.warning("Novel strategy generation failed: %s", exc)

			# Add hypotheses for target_series specified in novel proposals
			for proposal in novel_proposals:
				target_series = proposal.get("target_series", [])
				if target_series:
					from .hypothesis import Hypothesis as _H
					for series in target_series:
						for db_path, series_list in series_map.items():
							if series in series_list:
								hypotheses.append(_H(
									strategy=proposal["name"],
									series=series,
									db_path=db_path,
									start_date=self.start_date,
									end_date=self.end_date,
									fee_pct=self.fee_pct,
									tags=["source:llm_novel_strategy"],
								))
								break

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

		return results, llm_calls_used

	def _run_expand_phase(
		self,
		agent: ResearchAgent,
		queue: RunQueue,
		phase_results: list[HypothesisResult],
		series_map: dict[str, list[str]],
		budget: int,
		start_time: float,
	) -> list[HypothesisResult]:
		"""Expand promoted/reviewed strategies to structurally related series."""
		from .context_engine import ContextEngine
		from .hypothesis import Hypothesis

		winners = [r for r in phase_results if r.verdict in ("promote", "review")]
		if not winners:
			logger.info("No winners to expand")
			return []

		db_paths = list(series_map.keys())
		data_dir = str(Path(db_paths[0]).parent) if db_paths else "data"
		engine = ContextEngine(data_dir=data_dir)
		profiles = engine.profile_all(db_paths)

		expansion_hypotheses: list[Hypothesis] = []
		for winner in winners:
			related = engine.find_related_series(
				winner.hypothesis.series, profiles,
				same_asset_class=True,
				same_settlement_freq=False,
			)
			for series, db_path in related:
				h = Hypothesis(
					strategy=winner.hypothesis.strategy,
					series=series,
					db_path=db_path,
					start_date=self.start_date,
					end_date=self.end_date,
					fee_pct=self.fee_pct,
					tags=["source:expansion", f"parent_strategy:{winner.hypothesis.strategy}"],
				)
				if not self.tracker.is_tested(h):
					expansion_hypotheses.append(h)

		batch = expansion_hypotheses[:budget]
		if not batch:
			logger.info("No untested expansion hypotheses")
			return []

		logger.info("Expanding %d winners to %d related series", len(winners), len(batch))
		remaining_time = self._remaining_time(start_time)
		return queue.submit(batch, phase="expand", max_time_seconds=remaining_time)

	def _run_refinement_phase(
		self,
		agent: ResearchAgent,
		queue: RunQueue,
		series_map: dict[str, list[str]],
		budget: int,
		llm_budget: int,
		start_time: float,
	) -> list[HypothesisResult]:
		"""Phase 3: iteratively refine 'explore'-verdict LLM-generated strategies."""
		from edge_catcher.ai.strategizer import _parse_strategy_response
		from edge_catcher.runner.strategy_parser import (
			validate_strategy_code, save_strategy, list_strategies,
			STRATEGIES_LOCAL_PATH, STRATEGIES_LOCAL_MODULE,
		)
		from .hypothesis import Hypothesis

		results: list[HypothesisResult] = []
		llm_calls_used = 0

		# Find refinement candidates: strategies with explore but no promote
		cached = self._list_results()
		candidates = self._find_refinement_candidates(cached)
		if not candidates:
			logger.info("Refinement phase: no candidates found")
			return results

		logger.info("Refinement phase: %d candidate strategies", len(candidates))

		try:
			from edge_catcher.ai.client import LLMClient
			client = LLMClient()
		except Exception as exc:
			logger.error("Failed to create LLM client for refinement: %s", exc)
			return results

		refiner_system = (
			Path(__file__).parent.parent / "ai" / "prompts" / "refiner_system.txt"
		).read_text()

		for strategy_name in candidates:
			if budget <= 0 or llm_calls_used >= llm_budget:
				break
			remaining_time = self._remaining_time(start_time)
			if remaining_time is not None and remaining_time <= 0:
				break

			# Determine starting version: find latest refinement with actual code
			existing_version = self._count_existing_refinements(strategy_name, cached)
			if existing_version >= self.max_refinements:
				logger.info(
					"Strategy '%s' already has %d refinement(s), skipping",
					strategy_name, existing_version,
				)
				continue

			# Walk backwards from latest version to find one with actual code
			current_name = strategy_name
			start_iteration = 1
			if existing_version > 0:
				for v in range(existing_version + 1, 0, -1):
					candidate = f"{strategy_name}V{v}"
					if agent.read_strategy_code(candidate):
						current_name = candidate
						start_iteration = v
						break

			for iteration in range(start_iteration, self.max_refinements + 1):
				if budget <= 0 or llm_calls_used >= llm_budget:
					break
				remaining_time = self._remaining_time(start_time)
				if remaining_time is not None and remaining_time <= 0:
					break

				logger.info(
					"Refining '%s' (iteration %d/%d)",
					current_name, iteration, self.max_refinements,
				)

				# Read current strategy source
				code = agent.read_strategy_code(current_name)
				if not code:
					logger.warning("Cannot read source for '%s', skipping", current_name)
					break

				# Gather results for current version
				strat_results = self.tracker.list_results_for_strategy(current_name)
				if not strat_results:
					logger.warning("No results for '%s', skipping", current_name)
					break

				# Build refinement prompt
				user_prompt = self._build_refinement_prompt(
					code, strat_results, strategy_name, iteration,
				)

				prompt_hash = hashlib.sha256(
					("refiner" + refiner_system + user_prompt).encode()
				).hexdigest()

				try:
					response = client.complete(
						refiner_system, user_prompt, task="refiner",
					)
				except Exception as exc:
					logger.warning("Refinement LLM call failed: %s", exc)
					break
				llm_calls_used += 1

				model_str = client._resolve_model("refiner") or ""
				usage = client.last_usage
				token_count = usage.get("input_tokens", 0) + usage.get("output_tokens", 0)

				# Parse and validate
				try:
					new_code, new_name = _parse_strategy_response(response)
				except ValueError as exc:
					logger.warning("Failed to parse refinement response: %s", exc)
					self.audit.record_decision(
						prompt_hash=prompt_hash,
						prompt_text=user_prompt,
						response_text=response,
						parsed_output={"error": str(exc), "iteration": iteration},
						model=model_str,
						token_count=token_count,
					)
					break

				ok, error = validate_strategy_code(new_code)
				if not ok:
					logger.warning("Refined strategy failed validation: %s", error)
					self.audit.record_decision(
						prompt_hash=prompt_hash,
						prompt_text=user_prompt,
						response_text=response,
						parsed_output={
							"code": new_code, "strategy_name": new_name,
							"validation_ok": False, "error": error,
							"iteration": iteration,
						},
						model=model_str,
						token_count=token_count,
					)
					break

				self.audit.record_decision(
					prompt_hash=prompt_hash,
					prompt_text=user_prompt,
					response_text=response,
					parsed_output={
						"code": new_code, "strategy_name": new_name,
						"validation_ok": True, "iteration": iteration,
						"parent_strategy": strategy_name,
					},
					model=model_str,
					token_count=token_count,
				)

				# Save and reload
				result = save_strategy(new_code, new_name, STRATEGIES_LOCAL_PATH)
				if not result.get("ok"):
					logger.warning("Failed to save refined strategy: %s", result.get("error"))
					break

				try:
					mod = importlib.import_module(STRATEGIES_LOCAL_MODULE)
					importlib.reload(mod)
				except Exception as exc:
					logger.warning("Failed to reload strategies_local: %s", exc)

				available = list_strategies(STRATEGIES_LOCAL_PATH)
				available_names = [s["name"] for s in available]
				if new_name not in available_names:
					logger.warning(
						"Refined strategy '%s' not in strategy map after reload. Available: %s",
						new_name, available_names,
					)
					break

				# Generate hypotheses for same series as original
				tested_series = [
					(r["series"], r["db_path"]) for r in strat_results
				]
				# Deduplicate while preserving order
				seen = set()
				unique_series = []
				for pair in tested_series:
					if pair not in seen:
						seen.add(pair)
						unique_series.append(pair)

				hypotheses: list[Hypothesis] = []
				for series, db_path in unique_series:
					hypotheses.append(Hypothesis(
						strategy=new_name,
						series=series,
						db_path=db_path,
						start_date=self.start_date,
						end_date=self.end_date,
						fee_pct=self.fee_pct,
						tags=[
							"source:llm_refinement",
							f"parent_strategy:{strategy_name}",
							f"iteration:{iteration}",
						],
						notes=f"Refinement iteration {iteration} of {strategy_name}",
					))

				for h in hypotheses:
					self.tracker.save_hypothesis(h)

				batch = hypotheses[:budget]
				remaining_time = self._remaining_time(start_time)
				refine_results = queue.submit(
					batch, phase="refinement", max_time_seconds=remaining_time,
				)
				results.extend(refine_results)
				budget -= len(refine_results)

				# Keep/discard decision — compare against both current and original baseline
				base_results = self.tracker.list_results_for_strategy(strategy_name)
				if self._should_keep_refinement(strat_results, refine_results, baseline_results=base_results):
					logger.info(
						"Refinement '%s' improved over '%s' — keeping",
						new_name, current_name,
					)
					# Check if any result got promoted — stop refining
					if any(r.verdict in ("promote", "review") for r in refine_results):
						logger.info("Refined strategy '%s' promoted! Stopping.", new_name)
						break
					# Continue refining the new version
					current_name = new_name
				else:
					logger.info(
						"Refinement '%s' did not improve — discarding, stopping",
						new_name,
					)
					break

		return results

	def _write_phase_outcomes(
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
				"pattern": f"NEAR-MISS: {best_kill.hypothesis.strategy} scored Sharpe {best_kill.sharpe:.2f} but was killed{val_details_str}",
				"evidence": (
					f"series={best_kill.hypothesis.series}, trades={best_kill.total_trades}, "
					f"verdict_reason={best_kill.verdict_reason}"
				),
			})

	def _update_kill_registry(self) -> None:
		"""Upsert strategies with kill_rate >= 0.8 across >= 3 series into the kill registry.

		Excludes strategies with any promote or review verdict.
		"""
		results = self._list_results(refresh=True)
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

	def _cleanup_dead_strategies(self) -> None:
		"""Remove dead strategy code from strategies_local.py."""
		from edge_catcher.runner.strategy_parser import (
			cleanup_dead_strategies, STRATEGIES_LOCAL_PATH, list_strategies,
		)

		registry = self.tracker.list_kill_registry(permanent_only=True)
		if not registry:
			return

		# Safety check: exclude strategies with any promote/review verdict
		results = self._list_results()
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

	def _write_journal_summary(
		self,
		journal: "ResearchJournal",
		all_results: list[HypothesisResult],
		prev_content: dict | None = None,
	) -> str:
		"""Write trajectory + observation entries at end of run. Returns trajectory_status."""
		from .journal import ResearchJournal

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
			"promote_rate": sum(1 for r in all_results if r.verdict in ("promote", "review")) / max(len(all_results), 1),
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
			"consecutive_stuck": self._compute_consecutive_stuck(trajectory_status, prev_content),
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

	def _find_refinement_candidates(self, all_results: list[dict] | None = None) -> list[str]:
		"""Find strategies with 'explore' verdicts worth refining.

		Includes both LLM-generated and grid strategies.
		Excludes strategies that already have refinement children.
		"""
		if all_results is None:
			all_results = self._list_results()

		already_refined: set[str] = set()
		for r in all_results:
			tags = json.loads(r["tags"]) if isinstance(r["tags"], str) else (r["tags"] or [])
			for tag in tags:
				if tag.startswith("parent_strategy:"):
					already_refined.add(tag.split(":", 1)[1])

		# Group by strategy, check verdicts
		by_strategy: dict[str, set[str]] = defaultdict(set)
		for r in all_results:
			if r.get("status") == "ok":
				by_strategy[r["strategy"]].add(r["verdict"])

		# Only include strategies whose source is readable (LLM-generated
		# strategies in strategies_local.py).  Grid/dynamic strategies have
		# no editable source and cannot be refined by the LLM.
		from .agent import ResearchAgent

		candidates = []
		for strat, verdicts in by_strategy.items():
			if strat in already_refined:
				continue
			if "explore" in verdicts and "promote" not in verdicts:
				if ResearchAgent.read_strategy_code(strat):
					candidates.append(strat)

		return sorted(candidates)

	def _count_existing_refinements(self, base_strategy: str, all_results: list[dict] | None = None) -> int:
		"""Count how many refinement iterations already exist for a base strategy."""
		if all_results is None:
			all_results = self._list_results()
		max_iteration = 0
		for r in all_results:
			tags = json.loads(r["tags"]) if isinstance(r["tags"], str) else (r["tags"] or [])
			has_parent = any(
				tag == f"parent_strategy:{base_strategy}" for tag in tags
			)
			if has_parent:
				for tag in tags:
					if tag.startswith("iteration:"):
						try:
							it = int(tag.split(":", 1)[1])
							max_iteration = max(max_iteration, it)
						except ValueError:
							pass
		return max_iteration

	@staticmethod
	def _should_keep_refinement(
		original_results: list[dict],
		refined_results: list[HypothesisResult],
		baseline_results: list[dict] | None = None,
	) -> bool:
		"""Keep refinement only if it improves over BOTH previous iteration AND original baseline.

		Uses per-trade Sharpe (sharpe / sqrt(trades)) so changing trade frequency
		doesn't bias the comparison.
		"""
		if not refined_results:
			return False

		def _per_trade_sharpe_from_result(r: HypothesisResult) -> float:
			if r.status != "ok" or r.total_trades < 1:
				return 0.0
			return r.sharpe / math.sqrt(r.total_trades)

		def _per_trade_sharpe_from_row(r: dict) -> float:
			if r.get("status") != "ok":
				return 0.0
			trades = r.get("total_trades", 0)
			if trades < 1:
				return 0.0
			return r["sharpe"] / math.sqrt(trades)

		refined_best = max(
			(_per_trade_sharpe_from_result(r) for r in refined_results),
			default=0.0,
		)
		refined_viable = sum(1 for r in refined_results if r.verdict != "kill")

		orig_best = max(
			(_per_trade_sharpe_from_row(r) for r in original_results),
			default=0.0,
		)
		orig_viable = sum(1 for r in original_results if r["verdict"] != "kill")

		if not (refined_best > orig_best or refined_viable > orig_viable):
			return False

		if baseline_results:
			baseline_best = max(
				(_per_trade_sharpe_from_row(r) for r in baseline_results),
				default=0.0,
			)
			baseline_viable = sum(1 for r in baseline_results if r["verdict"] != "kill")
			if refined_best <= baseline_best and refined_viable <= baseline_viable:
				return False

		return True

	@staticmethod
	def _build_refinement_prompt(
		code: str,
		results: list[dict],
		base_strategy_name: str,
		iteration: int,
	) -> str:
		"""Build the user prompt for strategy refinement."""
		version_suffix = f"V{iteration + 1}"
		parts: list[str] = []

		parts.append("## Original Strategy Code")
		parts.append(f"```python\n{code}\n```")

		parts.append("\n## Backtest Results")
		for r in results:
			parts.append(
				f"- {r['series']} (db: {r['db_path']}): "
				f"Sharpe={r['sharpe']:.2f}, Trades={r['total_trades']}, "
				f"PnL={r['net_pnl_cents']:.0f}¢, "
				f"Drawdown={r['max_drawdown_pct']:.1f}%, "
				f"WinRate={r['win_rate']:.1%}, "
				f"Verdict={r['verdict']} ({r['verdict_reason']})"
			)

		# Diagnose issues
		issues: list[str] = []
		for r in results:
			if r["verdict"] == "kill" and r["sharpe"] < 1.0:
				issues.append(
					f"Low Sharpe ({r['sharpe']:.2f}) on {r['series']} — "
					f"consider tightening entry conditions or adding filters"
				)
			if r["total_trades"] < 50:
				issues.append(
					f"Too few trades ({r['total_trades']}) on {r['series']} — "
					f"consider loosening entry thresholds"
				)
			if r["max_drawdown_pct"] > 20:
				issues.append(
					f"High drawdown ({r['max_drawdown_pct']:.1f}%) on {r['series']} — "
					f"consider adding a stop-loss or position size limit"
				)
			if r["verdict"] == "kill" and r["net_pnl_cents"] <= 0:
				issues.append(
					f"Net loss ({r['net_pnl_cents']:.0f}¢) on {r['series']} — "
					f"review entry/exit logic"
				)

		if issues:
			parts.append("\n## Issues to Address")
			for issue in issues:
				parts.append(f"- {issue}")

		parts.append("\n## Constraints")
		parts.append("- Must remain a valid Strategy subclass")
		parts.append("- Keep the same entry signal family, refine thresholds/filters/exits")
		parts.append(
			f"- The refined class MUST use name = \"{base_strategy_name}{version_suffix}\""
		)

		return "\n".join(parts)

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
			compute_code_hash, compute_ast_fingerprint,
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

		prompt_hash = hashlib.sha256(
			("strategizer" + system_prompt + user_prompt).encode()
		).hexdigest()

		response = client.complete(system_prompt, user_prompt, task="strategizer")

		model_str = client._resolve_model("strategizer") or ""
		usage = client.last_usage
		token_count = usage.get("input_tokens", 0) + usage.get("output_tokens", 0)

		# Fix 3: Extract code and strategy name — audit on every exit path
		code: str | None = None
		strategy_name: str | None = None

		try:
			code, strategy_name = _parse_strategy_response(response)
		except ValueError as exc:
			logger.warning("Failed to parse strategizer response: %s", exc)
			self.audit.record_decision(
				prompt_hash=prompt_hash,
				prompt_text=user_prompt,
				response_text=response,
				parsed_output={"code": None, "strategy_name": None,
							   "validation_ok": False, "error": str(exc)},
				model=model_str,
				token_count=token_count,
			)
			return []

		# Fix 2: Log if code's name attribute differs from the proposal name
		proposal_name = proposal.get("name", "")
		if strategy_name != proposal_name:
			logger.warning(
				"Strategy name mismatch: proposal='%s', code='%s'. Using code name.",
				proposal_name, strategy_name,
			)

		# Validate
		ok, error = validate_strategy_code(code)
		if not ok:
			logger.warning("Generated strategy failed validation: %s", error)
			self.audit.record_decision(
				prompt_hash=prompt_hash,
				prompt_text=user_prompt,
				response_text=response,
				parsed_output={"code": code, "strategy_name": strategy_name,
							   "validation_ok": False, "error": error},
				model=model_str,
				token_count=token_count,
			)
			return []

		# Fix 3: Audit successful parse + validation
		self.audit.record_decision(
			prompt_hash=prompt_hash,
			prompt_text=user_prompt,
			response_text=response,
			parsed_output={"code": code, "strategy_name": strategy_name,
						   "validation_ok": True, "error": None},
			model=model_str,
			token_count=token_count,
		)

		# AST fingerprint dedup check
		code_hash = compute_code_hash(code)
		existing_by_hash = self.tracker.check_code_hash(code_hash)
		if existing_by_hash:
			logger.warning(
				"Novel strategy '%s' is a code-level duplicate of '%s' — skipping",
				strategy_name, existing_by_hash,
			)
			return []

		ast_fp = compute_ast_fingerprint(code)
		if ast_fp:
			existing_by_ast = self.tracker.check_fingerprint(ast_fp)
			if existing_by_ast:
				logger.warning(
					"Novel strategy '%s' is structurally identical to '%s' — skipping",
					strategy_name, existing_by_ast,
				)
				return []

		# Save fingerprint for future dedup
		if ast_fp:
			self.tracker.save_fingerprint(ast_fp, strategy_name, code_hash)

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

		# Fix 1: Verify strategy actually registered in the strategy map after reload
		available = list_strategies(STRATEGIES_LOCAL_PATH)
		available_names = [s["name"] for s in available]
		if strategy_name not in available_names:
			logger.warning(
				"Strategy '%s' not found in strategy map after reload. "
				"Available: %s",
				strategy_name, available_names,
			)
			return []

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
		agent = ResearchAgent(tracker=self.tracker, force=self.force)
		return agent._discover_all_series()

	def _remaining_time(self, start_time: float) -> float | None:
		if self.max_time_seconds is None:
			return None
		elapsed = time.monotonic() - start_time
		return max(0.0, self.max_time_seconds - elapsed)

	def _cancelled(self) -> bool:
		return self.cancel_event is not None and self.cancel_event.is_set()

	def _report_progress(self, phase: str, completed: int, total: int) -> None:
		if self.on_progress:
			self.on_progress(phase, completed, total)
