"""AI Strategy Generator: hypothesis + analysis results → Python strategy class."""
from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Optional

import yaml

from .client import LLMClient

logger = logging.getLogger(__name__)

_PROMPTS_DIR = Path(__file__).parent / "prompts"


def _load_system_prompt() -> str:
    return (_PROMPTS_DIR / "strategizer_system.txt").read_text(encoding="utf-8")


def _get_hypothesis_config(hypothesis_id: str, config_path: Path = Path("config")) -> Optional[dict]:
    """Load hypothesis config from config/ and config.local/ YAML files."""
    merged: dict = {}
    for cfg_dir in [config_path, Path("config.local")]:
        cfg_file = cfg_dir / "hypotheses.yaml"
        if cfg_file.exists():
            with open(cfg_file) as f:
                data = yaml.safe_load(f) or {}
            merged.update(data.get("hypotheses", {}))
    return merged.get(hypothesis_id)


def _get_tracker_result(
    hypothesis_id: str,
    run_id: Optional[str] = None,
    db_path: str = "data/research.db",
) -> Optional[dict]:
    """Fetch result from Tracker (research.db).

    Checks research loop results first, then statistical hypothesis_results.
    Uses hypothesis_id as lookup key unless run_id is provided.
    """
    from edge_catcher.research.tracker import Tracker
    tracker = Tracker(db_path)
    lookup = run_id if run_id else hypothesis_id
    # Try research loop results (hypotheses + results tables)
    result = tracker.get_result_by_id(lookup)
    if not result:
        # Fall back to statistical hypothesis_results table
        result = tracker.get_hypothesis_result_by_id(lookup)
    return result


def _build_user_prompt(hypothesis_config: dict, analysis_result: Optional[dict]) -> str:
    """Build the user prompt with hypothesis + analysis context."""
    parts = ["Generate a trading strategy based on this hypothesis:\n"]
    parts.append(f"**Hypothesis config:**\n```yaml\n{yaml.dump(hypothesis_config, default_flow_style=False)}```\n")
    if analysis_result:
        # Include key fields, not the entire raw data
        summary = {
            k: analysis_result[k]
            for k in ("verdict", "fee_adjusted_edge", "naive_edge", "clustered_edge",
                       "naive_n", "clustered_n", "naive_z_stat", "clustered_z_stat")
            if k in analysis_result and analysis_result[k] is not None
        }
        parts.append(f"**Analysis results:**\n```json\n{json.dumps(summary, indent=2)}```\n")
    parts.append("Generate a strategy class that trades this edge. Include sensible default parameters.")
    return "\n".join(parts)


def _parse_strategy_response(response: str) -> tuple[str, str]:
    """Extract Python code block and strategy name from LLM response.

    Returns (code, strategy_name).
    Raises ValueError if no Python code block found.
    """
    match = re.search(r"```python\n(.*?)```", response, re.DOTALL)
    if not match:
        raise ValueError(f"No Python code block found in LLM response:\n{response[:200]}")

    code = match.group(1).strip()

    # Extract strategy name from the code
    import ast
    try:
        tree = ast.parse(code)
        for node in tree.body:
            if isinstance(node, ast.ClassDef):
                for item in node.body:
                    if isinstance(item, ast.Assign):
                        for target in item.targets:
                            if (isinstance(target, ast.Name) and target.id == 'name'
                                    and isinstance(item.value, ast.Constant)):
                                return code, item.value.value
                # Fallback to snake_case class name
                snake = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', node.name)
                snake = re.sub(r'([a-z\d])([A-Z])', r'\1_\2', snake).lower()
                return code, snake
    except SyntaxError:
        pass

    return code, "unnamed-strategy"


def _build_hypothesis_prompt(hypothesis_config: dict, test_result, profiles: list) -> str:
    """Build a user prompt for strategy generation from a validated hypothesis."""
    parts = [
        "## Validated Hypothesis",
        f"Test type: {hypothesis_config.get('test_type', 'unknown')}",
        f"Series: {hypothesis_config.get('series', 'unknown')}",
        f"Database: {hypothesis_config.get('db', 'unknown')}",
        f"Rationale: {hypothesis_config.get('rationale', '')}",
        "",
        "## Statistical Evidence",
        f"Verdict: {test_result.verdict}",
        f"Z-statistic: {test_result.z_stat:.2f}",
        f"Fee-adjusted edge: {test_result.fee_adjusted_edge:.4f}",
        f"Detail: {json.dumps(test_result.detail, indent=2)}",
        "",
        "## Parameters",
        f"{json.dumps(hypothesis_config.get('params', {}), indent=2)}",
    ]

    # Add series profile if available
    if profiles:
        parts.append("")
        parts.append("## Series Context")
        for p in profiles:
            if hasattr(p, 'series_ticker') and p.series_ticker == hypothesis_config.get('series'):
                parts.append(f"Settlement: {getattr(p, 'settlement_frequency', 'unknown')}")
                parts.append(f"Markets: {getattr(p, 'market_count', 'unknown')}")
                break

    parts.append("")
    parts.append("Generate a Python strategy class that exploits this validated edge.")
    parts.append("The strategy should target the specific conditions where the edge was found.")

    return "\n".join(parts)


def generate_from_hypothesis(
    hypothesis_config: dict,
    test_result,
    profiles: list,
    client=None,
) -> tuple[str, str]:
    """Generate strategy code from a validated statistical hypothesis.

    Returns (code, strategy_name).
    """
    if client is None:
        client = LLMClient()

    system_prompt = _load_system_prompt()
    user_prompt = _build_hypothesis_prompt(hypothesis_config, test_result, profiles)

    response = client.complete(system_prompt, user_prompt, task="strategizer")
    return _parse_strategy_response(response)


def strategize(
    hypothesis_id: str,
    run_id: Optional[str],
    client: LLMClient,
    config_path: Path = Path("config"),
    research_db: str = "data/research.db",
) -> dict:
    """Generate a strategy class from a hypothesis + analysis results.

    Returns {"code": str, "strategy_name": str, "error": str | None}.
    """
    hyp_config = _get_hypothesis_config(hypothesis_id, config_path)
    if not hyp_config:
        return {"code": "", "strategy_name": "", "error": f"Hypothesis {hypothesis_id!r} not found"}

    analysis = _get_tracker_result(hypothesis_id, run_id, research_db)

    system_prompt = _load_system_prompt()
    user_prompt = _build_user_prompt(hyp_config, analysis)

    try:
        response = client.complete(system_prompt, user_prompt, task="strategizer")
    except Exception as e:
        return {"code": "", "strategy_name": "", "error": str(e)}

    try:
        code, name = _parse_strategy_response(response)
    except ValueError as e:
        return {"code": "", "strategy_name": "", "error": str(e)}

    return {"code": code, "strategy_name": name, "error": None}
