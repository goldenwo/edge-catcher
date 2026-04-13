"""Edge Report Interpreter: analysis JSON → plain-English summary."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Union

from .client import LLMClient

_PROMPTS_DIR = Path(__file__).parent / "prompts"


# ── prompt builders ───────────────────────────────────────────────────────────

def _load_system_prompt() -> str:
    return (_PROMPTS_DIR / "interpreter_system.txt").read_text(encoding="utf-8")


def _build_user_prompt(report: dict) -> str:
    return f"Interpret this analysis report:\n\n{json.dumps(report, indent=2, default=str)}"


# ── public API ────────────────────────────────────────────────────────────────

def interpret(report_path: Union[str, Path], client: LLMClient) -> str:
    """
    Load an analysis JSON report and return a plain-English summary.

    Args:
        report_path: Path to the JSON analysis report.
        client: LLMClient instance.

    Returns:
        Plain-English summary string from the LLM.

    Raises:
        FileNotFoundError: If ``report_path`` does not exist.
        LLMError: If the LLM call fails (propagated from client).
    """
    report_path = Path(report_path)
    if not report_path.exists():
        raise FileNotFoundError(f"Report not found: {report_path}")

    with open(report_path) as f:
        report = json.load(f)

    system_prompt = _load_system_prompt()
    user_prompt = _build_user_prompt(report)
    return client.complete(system_prompt, user_prompt, task="interpreter")
