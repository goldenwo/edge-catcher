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

		existing, novel = self.parse_response(response)

		model = self.client.model
		model_str = model if isinstance(model, str) else ""

		self.audit.record_decision(
			prompt_hash=prompt_hash,
			prompt_text=user_prompt,
			response_text=response,
			parsed_output={"existing": existing, "novel": novel},
			model=model_str,
		)

		# Convert existing strategy hypotheses to Hypothesis objects
		hypotheses: list[Hypothesis] = []
		for entry in existing:
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

		# Kill patterns: aggregate by strategy
		kill_by_strategy: dict[str, list[str]] = {}
		for r in killed:
			kill_by_strategy.setdefault(r["strategy"], []).append(
				f"{r['series']}: {r['verdict_reason']}"
			)

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
			for strat, reasons in kill_by_strategy.items():
				parts.append(f"### {strat} ({len(reasons)} kills)")
				for reason in reasons[:5]:
					parts.append(f"  - {reason}")
				if len(reasons) > 5:
					parts.append(f"  - ... and {len(reasons) - 5} more")

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
