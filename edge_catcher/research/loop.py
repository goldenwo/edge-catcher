# edge_catcher/research/loop.py
"""LoopOrchestrator: sequences ideate, expand, and refine phases with budget controls."""

from __future__ import annotations

import importlib
import json
import logging
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Callable

from .agent import ResearchAgent
from .audit import AuditLog
from .data_source_config import make_ds
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

	@property
	def _is_iterative(self) -> bool:
		"""True when running the full ideate→expand→refine loop.

		Single-pass modes (grid-only, refine-only, llm-only) skip trajectory
		tracking, budget shifting, and the circuit breaker since those are
		designed for multi-invocation convergence detection.
		"""
		return not (self.grid_only or self.refine_only or self.llm_only)

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
		from .observer import ResearchObserver
		journal = ResearchJournal(db_path=self.research_db)
		self._journal = journal

		observer = ResearchObserver(tracker=self.tracker, run_id=self.run_id)
		self._observer = observer

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
		if self._is_iterative:
			prev_trajectory = journal.get_latest_trajectory()
			prev_content = prev_trajectory  # already a dict or None
			self._consecutive_stuck = (prev_content or {}).get("consecutive_stuck", 0)
		else:
			prev_content = None
			self._consecutive_stuck = 0

		# ── Budget allocation ────────────────────────────────────────────
		budgets = self._compute_budgets(self.max_runs, self.grid_only, self._consecutive_stuck)
		budget_ideate = budgets["ideate"]
		budget_expand = budgets["expand"]
		budget_refine = budgets["refine"]

		if self._is_iterative and self._consecutive_stuck >= 2:
			logger.info(
				"Stuck detected (%d consecutive) — shifting to exploration-heavy budget",
				self._consecutive_stuck,
			)

		# ── Phase 1: Context + Ideate (or Grid if grid_only) ─────────
		if self._cancelled():
			logger.info("Loop cancelled by external request before phase 1")
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
			observer.write_phase_outcomes(journal, grid_results, "grid")
			observer.update_kill_registry()
			self._report_progress("grid", len(all_results), self.max_runs)
		elif not self.refine_only:
			if self.llm_only:
				ideate_results, llm_calls_used = self._run_ideate_phase(
					agent, queue, strategies, series_map,
					budget_ideate, start_time,
				)
			else:
				ideate_results, llm_calls_used = self._run_hypothesis_phase(
					agent, queue, strategies, series_map,
					budget_ideate, start_time,
				)
			all_results.extend(ideate_results)
			runs_used += len(ideate_results)
			observer.write_phase_outcomes(journal, ideate_results, "ideate")
			observer.update_kill_registry()
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
			logger.info("Loop cancelled by external request before phase 2")
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
				observer.write_phase_outcomes(journal, expand_results, "expand")
				observer.update_kill_registry()
				self._report_progress("expand", len(all_results), self.max_runs)

		# ── Phase 3: Refine ──────────────────────────────────────────
		if self._cancelled():
			logger.info("Loop cancelled by external request before phase 3")
			return 2, all_results
		if not self.grid_only:
			remaining_budget = min(budget_refine, self.max_runs - runs_used)
			remaining_llm = self.max_llm_calls - llm_calls_used
			if remaining_budget > 0 and remaining_llm > 0:
				from .refinement import RefinementExecutor
				refiner = RefinementExecutor(
					tracker=self.tracker,
					audit=self.audit,
					start_date=self.start_date,
					end_date=self.end_date,
					fee_pct=self.fee_pct,
					max_refinements=self.max_refinements,
					remaining_time_fn=lambda: self._remaining_time(start_time),
				)
				refine_results = refiner.run(
					agent, queue, series_map,
					remaining_budget, remaining_llm,
				)
				all_results.extend(refine_results)
				runs_used += len(refine_results)
				observer.write_phase_outcomes(journal, refine_results, "refine")
				observer.update_kill_registry()
				self._report_progress("refine", len(all_results), self.max_runs)

		# ── Journal summary + circuit breaker (iterative mode only) ──────
		circuit_breaker_tripped = False
		if self._is_iterative:
			trajectory_status = observer.write_journal_summary(journal, all_results, prev_content)
			new_stuck_count = self._compute_consecutive_stuck(trajectory_status, prev_content)
			circuit_breaker_tripped = (
				self.max_stuck_runs > 0
				and new_stuck_count >= self.max_stuck_runs + 2
			)
			observer.cleanup_dead_strategies()

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

	def _run_hypothesis_phase(
		self,
		agent: ResearchAgent,
		queue: RunQueue,
		strategies: list[str],
		series_map: dict[str, list[str]],
		budget: int,
		start_time: float,
	) -> tuple[list[HypothesisResult], int]:
		"""Run hypothesis-driven Phase 1: Hypothesize -> Analyze -> Strategize -> Backtest.

		Returns (results, llm_calls_used).
		"""
		from .context_engine import ContextEngine
		from .hypothesis import Hypothesis
		from .test_runner import TestRunner, EDGE_EXISTS
		from edge_catcher.ai.client import LLMClient
		from edge_catcher.ai.strategizer import generate_from_hypothesis
		from edge_catcher.runner.strategy_parser import (
			validate_strategy_code, save_strategy, list_strategies,
			compute_code_hash, compute_ast_fingerprint,
			STRATEGIES_LOCAL_PATH, STRATEGIES_LOCAL_MODULE,
		)
		from edge_catcher.storage.db import get_connection

		results: list[HypothesisResult] = []
		llm_calls_used = 0

		# ── Build context ────────────────────────────────────────────
		db_paths = list(series_map.keys())
		data_dir = str(Path(db_paths[0]).parent) if db_paths else "data"
		engine = ContextEngine(data_dir=data_dir)
		profiles = engine.profile_all(db_paths)
		context_block = engine.build_context_block(profiles)

		# ── Ideate hypotheses via LLM ────────────────────────────────
		try:
			client = LLMClient()
			ideator = LLMIdeator(
				tracker=self.tracker, audit=self.audit, client=client,
				journal=self._journal,
			)
			runner = TestRunner()

			hypothesis_kills = self.tracker.list_hypothesis_kills(permanent_only=True)
			logger.info("Starting LLM hypothesis ideation...")
			hypothesis_configs = ideator.ideate_hypotheses(
				context_block=context_block,
				hypothesis_kill_registry=hypothesis_kills,
				journal=self._journal,
				available_test_types=list(runner.test_types.keys()),
			)
			llm_calls_used += 1
		except Exception as exc:
			logger.error("LLM hypothesis ideation failed: %s", exc)
			return results, 0

		logger.info("LLM proposed %d hypothesis configs", len(hypothesis_configs))

		# ── Statistical analysis loop ────────────────────────────────
		validated: list[tuple[dict, object]] = []
		tested_count = 0
		for config in hypothesis_configs:
			db_path = str(Path("data") / config["db"])
			try:
				conn = get_connection(Path(db_path))
				test_result = runner.run(
					config["test_type"], conn, config["series"],
					config["params"], config["thresholds"],
				)
				conn.close()
				tested_count += 1
			except Exception as exc:
				logger.warning("Statistical test failed for %s: %s", config.get("test_type"), exc)
				continue

			# Save result to tracker
			self.tracker.save_hypothesis_result(
				test_type=config["test_type"],
				series=config["series"],
				db=config["db"],
				params=config["params"],
				thresholds=config["thresholds"],
				verdict=test_result.verdict,
				z_stat=test_result.z_stat,
				fee_adjusted_edge=test_result.fee_adjusted_edge,
				detail=test_result.detail,
				rationale=config.get("rationale", ""),
			)

			# Update kill registry for non-edge results
			if test_result.verdict != EDGE_EXISTS:
				self.tracker.record_hypothesis_kill(
					config["test_type"], config["series"], config["db"],
					verdict=test_result.verdict,
					params=config["params"],
					z_stat=test_result.z_stat,
				)

			if test_result.verdict == EDGE_EXISTS:
				validated.append((config, test_result))

		logger.info(
			"Hypothesis analysis: %d tested, %d edges found",
			tested_count, len(validated),
		)

		# ── Strategize from validated edges ──────────────────────────
		hypotheses: list[Hypothesis] = []
		for config, test_result in validated:
			if llm_calls_used >= self.max_llm_calls:
				logger.info("LLM call budget exhausted, skipping remaining strategy generation")
				break
			try:
				code, strategy_name = generate_from_hypothesis(
					config, test_result, profiles, client,
				)
				llm_calls_used += 1
			except Exception as exc:
				logger.warning("Strategy generation failed for %s: %s", config.get("test_type"), exc)
				continue

			ok, error = validate_strategy_code(code)
			if not ok:
				logger.warning("Generated strategy failed validation: %s", error)
				continue

			# AST fingerprint dedup check
			code_hash = compute_code_hash(code)
			existing_by_hash = self.tracker.check_code_hash(code_hash)
			if existing_by_hash:
				logger.warning(
					"Hypothesis strategy '%s' is a code-level duplicate of '%s' -- skipping",
					strategy_name, existing_by_hash,
				)
				continue

			ast_fp = compute_ast_fingerprint(code)
			if ast_fp:
				existing_by_ast = self.tracker.check_fingerprint(ast_fp)
				if existing_by_ast:
					logger.warning(
						"Hypothesis strategy '%s' is structurally identical to '%s' -- skipping",
						strategy_name, existing_by_ast,
					)
					continue

			# Save fingerprint for future dedup
			if ast_fp:
				self.tracker.save_fingerprint(ast_fp, strategy_name, code_hash)

			# Save to strategies_local.py
			result = save_strategy(code, strategy_name, STRATEGIES_LOCAL_PATH)
			if not result.get("ok"):
				logger.warning("Failed to save strategy: %s", result.get("error"))
				continue

			# Reload module to pick up new strategy
			try:
				mod = importlib.import_module(STRATEGIES_LOCAL_MODULE)
				importlib.reload(mod)
			except Exception as exc:
				logger.warning("Failed to reload strategies_local: %s", exc)

			# Verify strategy registered after reload
			available = list_strategies(STRATEGIES_LOCAL_PATH)
			available_names = [s["name"] for s in available]
			if strategy_name not in available_names:
				logger.warning(
					"Strategy '%s' not found in strategy map after reload. Available: %s",
					strategy_name, available_names,
				)
				continue

			# Create backtest hypothesis for the target series
			hypotheses.append(Hypothesis(
				strategy=strategy_name,
				data_sources=make_ds(db=config["db"], series=config["series"]),
				start_date=self.start_date,
				end_date=self.end_date,
				fee_pct=self.fee_pct,
				tags=["source:hypothesis_driven"],
				notes=config.get("rationale", ""),
			))

		logger.info(
			"Generated %d strategies from validated hypotheses",
			len(hypotheses),
		)

		# ── Submit backtests ─────────────────────────────────────────
		if hypotheses:
			# Persist hypotheses so they can be resumed
			for h in hypotheses:
				self.tracker.save_hypothesis(h)

			batch = hypotheses[:budget]
			remaining_time = self._remaining_time(start_time)
			bt_results = queue.submit(
				batch, phase="hypothesis_driven", max_time_seconds=remaining_time,
			)
			results.extend(bt_results)

		return results, llm_calls_used

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
					data_sources=make_ds(db=Path(p["db_path"]).name, series=p["series"]),
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
		from .novel_generator import NovelStrategyGenerator
		novel_gen = NovelStrategyGenerator(
			tracker=self.tracker, audit=self.audit,
			start_date=self.start_date, end_date=self.end_date,
			fee_pct=self.fee_pct,
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
					novel_gen.generate(
						proposal, client, strategies, series_map,
					)
				)
				llm_calls_used += 1
			except Exception as exc:
				logger.warning("Novel strategy generation failed: %s", exc)

		# Add hypotheses for target_series specified in novel proposals,
		# but only for strategies that were successfully generated and registered.
		from edge_catcher.runner.strategy_parser import (
			list_strategies, STRATEGIES_LOCAL_PATH,
		)
		available = {s["name"] for s in list_strategies(STRATEGIES_LOCAL_PATH)}
		from .hypothesis import Hypothesis as _H
		for proposal in novel_proposals:
			if proposal["name"] not in available:
				continue
			target_series = proposal.get("target_series", [])
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
					data_sources=make_ds(db=Path(db_path).name, series=series),
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
			try:
				self.on_progress(phase, completed, total)
			except Exception:
				logger.warning("on_progress callback failed", exc_info=True)
