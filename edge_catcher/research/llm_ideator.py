# edge_catcher/research/llm_ideator.py
"""LLM-driven hypothesis ideation from aggregate research results."""

from __future__ import annotations

import hashlib
import json
import logging
import re
from collections import Counter
from pathlib import Path

from .audit import AuditLog
from .hypothesis import Hypothesis
from .tracker import Tracker

logger = logging.getLogger(__name__)

_PROMPTS_DIR = Path(__file__).parent.parent / "ai" / "prompts"
_MIN_RESULTS_FOR_IDEATION = 10


class LLMIdeator:
	def __init__(
		self,
		tracker: Tracker,
		audit: AuditLog,
		client,  # LLMClient instance
		journal=None,  # ResearchJournal | None
	) -> None:
		self.tracker = tracker
		self.audit = audit
		self.client = client
		self.journal = journal

	def ideate(
		self,
		available_strategies: list[str],
		series_map: dict[str, list[str]],
		start_date: str,
		end_date: str,
		fee_pct: float = 1.0,
	) -> tuple[list[Hypothesis], list[dict]]:
		"""Run one LLM ideation call and return (hypotheses, novel_proposals).

		Raises ValueError if tracker has fewer than _MIN_RESULTS_FOR_IDEATION results.
		"""
		results = self.tracker.list_results()
		if len(results) < _MIN_RESULTS_FOR_IDEATION:
			raise ValueError(
				f"Not enough data for LLM ideation "
				f"({len(results)} results, need ≥{_MIN_RESULTS_FOR_IDEATION}). "
				f"Run grid phase first."
			)

		system_prompt = self._load_system_prompt()
		user_prompt = self.build_ideation_prompt(available_strategies, series_map)

		prompt_hash = hashlib.sha256(
			(system_prompt + user_prompt).encode()
		).hexdigest()

		response = self.client.complete(system_prompt, user_prompt, task="ideator")
		logger.info("LLM ideation response:\n%s", response[:2000])

		existing, novel = self.parse_response(response)

		# Filter out strategies that don't actually exist
		strategy_set = set(available_strategies)
		valid_existing = [e for e in existing if e.get("strategy") in strategy_set]
		if len(valid_existing) < len(existing):
			rejected = [e["strategy"] for e in existing if e.get("strategy") not in strategy_set]
			logger.warning(
				"Rejected %d existing_strategy_hypotheses with unknown strategies: %s",
				len(rejected), rejected,
			)

		model = self.client._resolve_model("ideator") or ""
		usage = self.client.last_usage
		token_count = usage.get("input_tokens", 0) + usage.get("output_tokens", 0)

		self.audit.record_decision(
			prompt_hash=prompt_hash,
			prompt_text=user_prompt,
			response_text=response,
			parsed_output={"existing": valid_existing, "novel": novel},
			model=model,
			token_count=token_count,
		)

		# Convert existing strategy hypotheses to Hypothesis objects
		hypotheses: list[Hypothesis] = []
		for entry in valid_existing:
			hypotheses.append(Hypothesis(
				strategy=entry["strategy"],
				series=entry["series"],
				db_path=entry["db_path"],
				start_date=start_date,
				end_date=end_date,
				fee_pct=fee_pct,
				tags=["source:llm_ideated"],
				notes=entry.get("rationale", ""),
			))

		return hypotheses, novel

	def build_ideation_prompt(
		self,
		available_strategies: list[str],
		series_map: dict[str, list[str]],
	) -> str:
		"""Build the user prompt from current Tracker state."""
		results = self.tracker.list_results()

		promoted = [r for r in results if r["verdict"] == "promote"]
		explored = [r for r in results if r["verdict"] == "explore"]
		killed = [r for r in results if r["verdict"] == "kill"]

		# Kill patterns: aggregate by strategy (store raw verdict_reason for grouping)
		kill_by_strategy: dict[str, list[str]] = {}
		for r in killed:
			kill_by_strategy.setdefault(r["strategy"], []).append(r["verdict_reason"])

		# Coverage: what series/strategy combos exist
		tested_combos = {(r["strategy"], r["series"]) for r in results}
		all_combos = set()
		for db_path, series_list in series_map.items():
			for series in series_list:
				for strat in available_strategies:
					all_combos.add((strat, series))
		untested = all_combos - tested_combos

		parts: list[str] = []
		parts.append("## Summary")
		parts.append(f"Total backtests: {len(results)}")
		parts.append(f"Promoted: {len(promoted)}, Explore: {len(explored)}, Killed: {len(killed)}")

		if promoted:
			parts.append("\n## Promoted Strategies (strong edge)")
			for r in promoted:
				parts.append(
					f"- {r['strategy']} × {r['series']} (db: {r['db_path']}): "
					f"Sharpe={r['sharpe']:.2f}, WinRate={r['win_rate']:.1%}, "
					f"PnL={r['net_pnl_cents']:.0f}¢, Trades={r['total_trades']}"
				)

		if explored:
			explore_by_strategy: dict[str, list[dict]] = {}
			for r in explored:
				explore_by_strategy.setdefault(r["strategy"], []).append(r)
			parts.append("\n## Explored Strategies (inconclusive)")
			for strat, strat_results in explore_by_strategy.items():
				parts.append(f"### {strat} ({len(strat_results)} explores)")
				for r in strat_results[:5]:
					parts.append(
						f"  - {r['series']}: {r['verdict_reason']}"
					)
				if len(strat_results) > 5:
					parts.append(f"  - ... and {len(strat_results) - 5} more")

		if kill_by_strategy:
			parts.append("\n## Kill Patterns")
			sorted_kills = sorted(
				kill_by_strategy.items(), key=lambda x: len(x[1]), reverse=True
			)
			shown = sorted_kills[:10]
			omitted = sorted_kills[10:]
			for strat, reasons in shown:
				reason_counts = Counter(reasons)
				parts.append(f"### {strat} ({len(reasons)} kills)")
				for reason, count in reason_counts.most_common():
					parts.append(f"  - {reason}: {count}x")
			if omitted:
				omitted_kills = sum(len(r) for _, r in omitted)
				parts.append(
					f"\n{len(omitted)} strategies with {omitted_kills} total kills omitted"
				)

		parts.append(f"\n## Available Strategies: {', '.join(available_strategies)}")

		parts.append(f"\n## Available Data")
		for db_path, series_list in series_map.items():
			parts.append(f"- {db_path}: {', '.join(series_list)}")

		if untested:
			parts.append(f"\n## Untested Combinations ({len(untested)} remaining)")
			for strat, series in sorted(untested)[:20]:
				parts.append(f"  - {strat} × {series}")
			if len(untested) > 20:
				parts.append(f"  - ... and {len(untested) - 20} more")

		# Research Journal context
		if self.journal:
			journal_context = self.journal.build_context_for_prompt()
			if journal_context:
				parts.append(f"\n## Research Journal (prior session learnings)\n{journal_context}")

		# Self-performance feedback
		self_performance = self._build_self_performance_summary()
		if self_performance:
			parts.append(f"\n{self_performance}")

		return "\n".join(parts)

	def parse_response(self, response: str) -> tuple[list[dict], list[dict]]:
		"""Parse LLM JSON response into (existing_hypotheses, novel_proposals).

		Returns empty lists on parse failure.
		"""
		# Strip markdown fencing if present
		cleaned = response.strip()
		fence_match = re.search(r"```(?:json)?\n(.*?)```", cleaned, re.DOTALL)
		if fence_match:
			cleaned = fence_match.group(1).strip()

		try:
			data = json.loads(cleaned)
		except json.JSONDecodeError:
			logger.warning("LLMIdeator: failed to parse response as JSON")
			return [], []

		existing = data.get("existing_strategy_hypotheses", [])
		novel = data.get("novel_strategy_proposals", [])
		return existing, novel

	def _build_self_performance_summary(self) -> str:
		"""Build a summary of how previous LLM suggestions performed."""
		results = self.tracker.list_results()
		if not results:
			return ""

		# Categorize by source tag (filter out adjacent-* expansions)
		novel = [r for r in results
		         if self._has_tag(r, "source:llm_novel_strategy")
		         and not self._has_any_adjacent_tag(r)]
		ideated = [r for r in results if self._has_tag(r, "source:llm_ideated")]
		refined = [r for r in results if self._has_tag(r, "source:llm_refinement")]

		if not novel and not ideated:
			return ""  # no LLM history yet

		parts = ["## Your Track Record (from previous sessions)"]

		if novel:
			parts.append(self._summarize_group("Novel Strategy Proposals", novel))

		if ideated:
			parts.append(self._summarize_group("Existing Combo Suggestions", ideated))

		if refined:
			parts.append(self._summarize_refinements(refined))

		# Validation gate analysis from validation_details
		candidates = [r for r in results if r.get("validation_details")]
		if candidates:
			parts.append(self._summarize_validation_gates(candidates))

		# Steering directives — the self-improvement payload
		gate_stats = self._compute_gate_stats(candidates) if candidates else {}
		steering = self._build_steering_directives(novel, ideated, gate_stats)
		if steering:
			parts.append(steering)

		return "\n\n".join(parts)

	@staticmethod
	def _has_tag(result: dict, tag: str) -> bool:
		tags = json.loads(result["tags"]) if isinstance(result["tags"], str) else (result["tags"] or [])
		return tag in tags

	@staticmethod
	def _has_any_adjacent_tag(result: dict) -> bool:
		tags = json.loads(result["tags"]) if isinstance(result["tags"], str) else (result["tags"] or [])
		return any(t.startswith("adjacent-") for t in tags)

	def _summarize_group(self, title: str, results: list[dict]) -> str:
		"""Summarize a group of LLM-sourced results."""
		total = len(results)
		verdict_counts: Counter = Counter(r["verdict"] for r in results)
		promoted = verdict_counts.get("promote", 0)
		explore = verdict_counts.get("explore", 0)
		killed = verdict_counts.get("kill", 0)
		error = verdict_counts.get("error", 0)
		hit_rate = self._hit_rate(results)

		lines = [
			f"### {title}",
			f"- Total proposed: {total}",
			f"- Outcomes: {promoted} promoted, {explore} explore, {killed} killed, {error} error",
			f"- Hit rate (non-kill): {hit_rate:.0%}",
		]

		# Best performer
		ok_results = [r for r in results if r["verdict"] in ("promote", "explore") and r["sharpe"] is not None]
		if ok_results:
			best = max(ok_results, key=lambda r: r["sharpe"])
			lines.append(f"- Best performer: \"{best['strategy']}\" (Sharpe {best['sharpe']:.2f} on {best['series']})")

		# Common failure: low trade count
		low_trade = [r for r in results if (r["total_trades"] or 0) < 50 and r["verdict"] == "kill"]
		if len(low_trade) > total * 0.5:
			lines.append(f"- Common failure: strategies with <50 trades ({len(low_trade)}/{total} proposals)")

		return "\n".join(lines)

	def _summarize_refinements(self, results: list[dict]) -> str:
		"""Summarize refinement trajectories grouped by parent strategy tag."""
		# Group by parent_strategy tag
		parent_groups: dict[str, list[dict]] = {}
		for r in results:
			tags = json.loads(r["tags"]) if isinstance(r["tags"], str) else (r["tags"] or [])
			parent_tag = next((t for t in tags if t.startswith("parent_strategy:")), None)
			parent = parent_tag.split(":", 1)[1] if parent_tag else r["strategy"]
			parent_groups.setdefault(parent, []).append(r)

		improved = 0
		regressed = 0
		for parent, group in parent_groups.items():
			# Results are DESC by completed_at, so [0] is newest, [-1] is oldest
			sharpes = [r["sharpe"] for r in group if r["sharpe"] is not None]
			if len(sharpes) >= 2:
				if sharpes[0] > sharpes[-1]:  # newest > oldest = improved
					improved += 1
				else:
					regressed += 1
			else:
				improved += 1  # only one sample, treat as neutral/improved

		total_parents = len(parent_groups)
		lines = [
			"### Refinement Trajectories",
			f"- Strategies refined: {total_parents}",
			f"- Improved: {improved}/{total_parents}",
			f"- Regressed: {regressed}/{total_parents}",
		]
		return "\n".join(lines)

	def _summarize_validation_gates(self, candidates: list[dict]) -> str:
		"""Summarize validation gate pass/fail rates."""
		gate_stats = self._compute_gate_stats(candidates)
		if not gate_stats:
			return ""

		total = len(candidates)
		# Count how many passed all gates (verdict == promote among candidates)
		passed_all = sum(1 for r in candidates if r["verdict"] == "promote")

		lines = [
			"### Validation Gate Performance",
			f"- Candidates reaching validation: {total}",
			f"- Passed all gates: {passed_all} ({passed_all / total:.0%})",
		]

		# Most common failure gate
		failure_counts = {g: s["failed"] for g, s in gate_stats.items() if s["failed"] > 0}
		if failure_counts:
			worst_gate = max(failure_counts, key=lambda g: failure_counts[g])
			worst_count = failure_counts[worst_gate]
			lines.append(f"- Most common failure gate: {worst_gate} ({worst_count}/{total} failed here)")

		# Per-gate pass rates
		for gate_name, stats in sorted(gate_stats.items()):
			lines.append(f"- {gate_name} pass rate: {stats['pass_rate']:.0%}")

		return "\n".join(lines)

	@staticmethod
	def _compute_gate_stats(candidates: list[dict]) -> dict:
		"""Return {gate_name: {"passed": N, "failed": N, "pass_rate": float}}."""
		aggregated: dict[str, dict] = {}
		for r in candidates:
			raw = r.get("validation_details")
			if not raw:
				continue
			gates = json.loads(raw) if isinstance(raw, str) else raw
			for gate in gates:
				name = gate.get("gate_name", "unknown")
				if name not in aggregated:
					aggregated[name] = {"passed": 0, "failed": 0, "pass_rate": 0.0}
				if gate.get("passed"):
					aggregated[name]["passed"] += 1
				else:
					aggregated[name]["failed"] += 1
		# Compute pass_rate
		for name, stats in aggregated.items():
			total = stats["passed"] + stats["failed"]
			stats["pass_rate"] = stats["passed"] / total if total > 0 else 0.0
		return aggregated

	def _build_steering_directives(
		self,
		novel: list[dict],
		ideated: list[dict],
		gate_stats: dict,
	) -> str:
		"""Build actionable steering directives based on performance history."""
		directives = []

		# Trade count bottleneck
		if novel:
			low_trade_kills = [r for r in novel if (r["total_trades"] or 0) < 50 and r["verdict"] == "kill"]
			if len(low_trade_kills) > len(novel) * 0.5:
				directives.append(
					f"Trade frequency bottleneck: {len(low_trade_kills)}/{len(novel)} "
					f"novel proposals killed for <50 trades. "
					f"Propose strategies with wider entry conditions."
				)

		# Hardest validation gate
		if gate_stats:
			worst_gate = min(gate_stats, key=lambda g: gate_stats[g]["pass_rate"])
			rate = gate_stats[worst_gate]["pass_rate"]
			if rate < 0.5:
				directives.append(
					f"{worst_gate} is your hardest gate ({rate:.0%} pass rate). "
					f"Design strategies that are robust to this check."
				)

		# Best-performing category
		if novel and ideated:
			novel_hit = self._hit_rate(novel)
			ideated_hit = self._hit_rate(ideated)
			if ideated_hit > novel_hit + 0.1:
				directives.append(
					f"Existing combo suggestions outperform novel proposals "
					f"({ideated_hit:.0%} vs {novel_hit:.0%} non-kill rate). "
					f"Prioritize untested combinations."
				)

		# Winning patterns
		promotes = [r for r in (novel + (ideated or [])) if r["verdict"] == "promote"]
		if promotes:
			directives.append(self._describe_winning_pattern(promotes))

		if not directives:
			return ""

		parts = ["### What To Try Next"]
		for d in directives:
			parts.append(f"- {d}")
		return "\n".join(parts)

	@staticmethod
	def _hit_rate(results: list[dict]) -> float:
		if not results:
			return 0.0
		non_kill = sum(1 for r in results if r["verdict"] in ("promote", "explore"))
		return non_kill / len(results)

	@staticmethod
	def _describe_winning_pattern(promotes: list[dict]) -> str:
		descriptions = []
		for r in promotes[:3]:  # limit to 3 for brevity
			descriptions.append(f"{r['strategy']} (Sharpe {r['sharpe']:.2f} on {r['series']})")
		return f"Winning patterns to expand: {', '.join(descriptions)}. Explore variants of these."

	def _load_system_prompt(self) -> str:
		return (_PROMPTS_DIR / "ideator_system.txt").read_text()
