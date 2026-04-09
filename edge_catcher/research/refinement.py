# edge_catcher/research/refinement.py
"""RefinementExecutor: iterative LLM-driven strategy refinement extracted from LoopOrchestrator."""

from __future__ import annotations

import hashlib
import importlib
import json
import logging
import math
from collections import defaultdict
from pathlib import Path
from typing import Callable

from .audit import AuditLog
from .hypothesis import HypothesisResult
from .tracker import Tracker

logger = logging.getLogger(__name__)


class RefinementExecutor:
	"""Execute the refinement phase: iteratively refine 'explore'-verdict strategies."""

	def __init__(
		self,
		tracker: Tracker,
		audit: AuditLog,
		start_date: str | None,
		end_date: str | None,
		fee_pct: float,
		max_refinements: int,
		remaining_time_fn: Callable[[], float | None],
	) -> None:
		self.tracker = tracker
		self.audit = audit
		self.start_date = start_date
		self.end_date = end_date
		self.fee_pct = fee_pct
		self.max_refinements = max_refinements
		self.remaining_time_fn = remaining_time_fn

	def run(
		self,
		agent: "ResearchAgent",
		queue: "RunQueue",
		series_map: dict[str, list[str]],
		budget: int,
		llm_budget: int,
	) -> list[HypothesisResult]:
		"""Phase 3: iteratively refine 'explore'-verdict LLM-generated strategies."""
		from edge_catcher.ai.strategizer import _parse_strategy_response
		from edge_catcher.runner.strategy_parser import (
			validate_strategy_code, save_strategy, list_strategies,
			STRATEGIES_LOCAL_PATH, STRATEGIES_LOCAL_MODULE,
		)
		from .data_source_config import make_ds
		from .hypothesis import Hypothesis

		results: list[HypothesisResult] = []
		llm_calls_used = 0

		# Find refinement candidates: strategies with explore but no promote
		cached = self.tracker.list_results()
		candidates = self.find_candidates(cached)
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
			remaining_time = self.remaining_time_fn()
			if remaining_time is not None and remaining_time <= 0:
				break

			# Determine starting version: find latest refinement with actual code
			existing_version = self.count_existing_refinements(strategy_name, cached)
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
				remaining_time = self.remaining_time_fn()
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
				user_prompt = self.build_refinement_prompt(
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
						data_sources=make_ds(db=Path(db_path).name, series=series),
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
				remaining_time = self.remaining_time_fn()
				refine_results = queue.submit(
					batch, phase="refinement", max_time_seconds=remaining_time,
				)
				results.extend(refine_results)
				budget -= len(refine_results)

				# Keep/discard decision — compare against both current and original baseline
				base_results = self.tracker.list_results_for_strategy(strategy_name)
				if self.should_keep_refinement(strat_results, refine_results, baseline_results=base_results):
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

	def find_candidates(self, all_results: list[dict] | None = None) -> list[str]:
		"""Find strategies with 'explore' verdicts worth refining.

		Includes both LLM-generated and grid strategies.
		Excludes strategies that already have refinement children.
		"""
		if all_results is None:
			all_results = self.tracker.list_results()

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

	def count_existing_refinements(self, base_strategy: str, all_results: list[dict] | None = None) -> int:
		"""Count how many refinement iterations already exist for a base strategy."""
		if all_results is None:
			all_results = self.tracker.list_results()
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
	def should_keep_refinement(
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
	def build_refinement_prompt(
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
				f"PnL={r['net_pnl_cents']:.0f}\u00a2, "
				f"Drawdown={r['max_drawdown_pct']:.1f}%, "
				f"WinRate={r['win_rate']:.1%}, "
				f"Verdict={r['verdict']} ({r['verdict_reason']})"
			)

		# Diagnose issues
		issues: list[str] = []
		for r in results:
			if r["verdict"] == "kill" and r["sharpe"] < 1.0:
				issues.append(
					f"Low Sharpe ({r['sharpe']:.2f}) on {r['series']} \u2014 "
					f"consider tightening entry conditions or adding filters"
				)
			if r["total_trades"] < 50:
				issues.append(
					f"Too few trades ({r['total_trades']}) on {r['series']} \u2014 "
					f"consider loosening entry thresholds"
				)
			if r["max_drawdown_pct"] > 20:
				issues.append(
					f"High drawdown ({r['max_drawdown_pct']:.1f}%) on {r['series']} \u2014 "
					f"consider adding a stop-loss or position size limit"
				)
			if r["verdict"] == "kill" and r["net_pnl_cents"] <= 0:
				issues.append(
					f"Net loss ({r['net_pnl_cents']:.0f}\u00a2) on {r['series']} \u2014 "
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
