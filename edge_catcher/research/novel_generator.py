# edge_catcher/research/novel_generator.py
"""NovelStrategyGenerator: generates strategy code from LLM proposals and returns hypotheses."""

from __future__ import annotations

import hashlib
import importlib
import logging
from pathlib import Path

from .audit import AuditLog
from .tracker import Tracker

logger = logging.getLogger(__name__)


class NovelStrategyGenerator:
	"""Generate strategy code from a novel proposal and return hypotheses to test."""

	def __init__(
		self,
		tracker: Tracker,
		audit: AuditLog,
		start_date: str | None,
		end_date: str | None,
		fee_pct: float,
	) -> None:
		self.tracker = tracker
		self.audit = audit
		self.start_date = start_date
		self.end_date = end_date
		self.fee_pct = fee_pct

	def generate(
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
		from .data_source_config import make_ds
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
					data_sources=make_ds(db=Path(db_path).name, series=series),
					start_date=self.start_date,
					end_date=self.end_date,
					fee_pct=self.fee_pct,
					tags=["source:llm_novel_strategy"],
					notes=proposal.get("description", ""),
				))

		logger.info("Generated novel strategy '%s' with %d hypotheses",
					strategy_name, len(hypotheses))
		return hypotheses
