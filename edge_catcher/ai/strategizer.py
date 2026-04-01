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
    return (_PROMPTS_DIR / "strategizer_system.txt").read_text()


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


def _get_analysis_result(hypothesis_id: str, run_id: Optional[str], db_path: Path) -> Optional[dict]:
    """Fetch analysis result from DB. Uses latest run if run_id is None."""
    if not db_path.exists():
        return None
    from edge_catcher.storage.db import get_connection
    conn = get_connection(db_path)
    try:
        if run_id:
            row = conn.execute(
                "SELECT * FROM analysis_results WHERE run_id = ?", (run_id,)
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT * FROM analysis_results WHERE hypothesis_id = ? ORDER BY run_timestamp DESC LIMIT 1",
                (hypothesis_id,),
            ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


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
        for node in ast.walk(tree):
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


def strategize(
    hypothesis_id: str,
    run_id: Optional[str],
    client: LLMClient,
    db_path: Path,
    config_path: Path = Path("config"),
) -> dict:
    """Generate a strategy class from a hypothesis + analysis results.

    Returns {"code": str, "strategy_name": str, "error": str | None}.
    """
    hyp_config = _get_hypothesis_config(hypothesis_id, config_path)
    if not hyp_config:
        return {"code": "", "strategy_name": "", "error": f"Hypothesis {hypothesis_id!r} not found"}

    analysis = _get_analysis_result(hypothesis_id, run_id, db_path)

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
