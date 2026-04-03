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
	) -> None:
		self.tracker = tracker
		self.audit = audit
		self.client = client

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

	def _load_system_prompt(self) -> str:
		return (_PROMPTS_DIR / "ideator_system.txt").read_text()
