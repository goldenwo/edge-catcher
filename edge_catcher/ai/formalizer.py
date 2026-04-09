"""Hypothesis Formalizer: natural language → YAML config + stub module."""
from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

import yaml

from .client import LLMClient

_PROMPTS_DIR = Path(__file__).parent / "prompts"


# ── prompt builders ───────────────────────────────────────────────────────────

def _load_system_prompt() -> str:
    return (_PROMPTS_DIR / "formalizer_system.txt").read_text()


def _build_user_prompt(description: str) -> str:
    return f"Formalize this market hypothesis:\n\n{description}"


# ── response parsing ──────────────────────────────────────────────────────────

def _parse_llm_response(response: str) -> tuple[str, dict]:
    """
    Extract hypothesis_id and config dict from an LLM response.

    Expects a ```yaml ... ``` fenced block whose top-level structure is either::

        hypotheses:
          <hypothesis_id>:
            ...

    or (bare dict)::

        <hypothesis_id>:
          ...

    Returns: (hypothesis_id, hyp_config_dict)
    Raises: ValueError if no YAML block is found or the structure is unexpected.
    """
    yaml_match = re.search(r"```yaml\n(.*?)```", response, re.DOTALL)
    if not yaml_match:
        raise ValueError(f"No YAML block found in LLM response:\n{response}")

    raw_yaml = yaml_match.group(1)
    parsed = yaml.safe_load(raw_yaml)

    # Normalize: accept both {"hypotheses": {"id": {...}}} and {"id": {...}}
    if isinstance(parsed, dict) and "hypotheses" in parsed:
        entries = parsed["hypotheses"]
    else:
        entries = parsed

    if not isinstance(entries, dict) or len(entries) == 0:
        raise ValueError(f"Expected a non-empty hypothesis dict, got: {parsed!r}")

    hypothesis_id = next(iter(entries))
    hyp_config = entries[hypothesis_id]
    return hypothesis_id, hyp_config


# ── stub generation ───────────────────────────────────────────────────────────

def _stub_content(hypothesis_id: str, name: str) -> str:
    return f'''"""
{name} — generated stub.

Edit run() to implement your statistical test.
See edge_catcher/hypotheses/examples/example_hypothesis.py for a reference.
"""
from __future__ import annotations

from pathlib import Path

from edge_catcher.storage.models import HypothesisResult

HYPOTHESIS_ID = "{hypothesis_id}"


def run(db_conn, config_path: Path = Path("config")) -> HypothesisResult:
    """Run the {name} hypothesis against the local database."""
    raise NotImplementedError(
        f"Hypothesis {{HYPOTHESIS_ID!r}} is not yet implemented. "
        f"Edit {{__file__}} and implement the run() function. "
        f"See edge_catcher/hypotheses/examples/example_hypothesis.py for guidance."
    )
'''


# ── path helpers ──────────────────────────────────────────────────────────────

def _module_str_to_file(module_str: str) -> Path:
    """``edge_catcher.hypotheses.custom.foo`` → ``edge_catcher/hypotheses/custom/foo.py``"""
    return Path(module_str.replace(".", "/") + ".py")


def _module_str_to_relative_file(module_str: str, hypotheses_base: Path) -> Path:
    """Map a module string to a path relative to *hypotheses_base*."""
    parts = module_str.split(".")
    try:
        idx = parts.index("hypotheses")
        sub_parts = parts[idx + 1:]
    except ValueError:
        sub_parts = parts[-2:]  # fallback: last two segments
    return hypotheses_base / Path("/".join(sub_parts) + ".py")


# ── public API ────────────────────────────────────────────────────────────────

def formalize(
    description: str,
    client: LLMClient,
    config_path: Optional[Path] = None,
    hypotheses_base: Optional[Path] = None,
) -> dict:
    """
    Convert a plain-English hypothesis description into config + stub module.

    Args:
        description: Natural-language hypothesis.
        client: LLMClient instance (caller is responsible for authentication).
        config_path: Path to ``hypotheses.yaml`` (default: ``config.local/hypotheses.yaml``).
        hypotheses_base: Override base directory for stub file creation.
            When ``None`` the module string is mapped to a project-root path.
            Pass ``tmp_path / "hypotheses"`` in tests to keep them hermetic.

    Returns:
        On success: ``{"hypothesis_id", "config_path", "module_path", "message"}``.
        On parse failure: ``{"error": True, "raw_response": str}``.
    """
    config_path = config_path or Path("config.local/hypotheses.yaml")

    system_prompt = _load_system_prompt()
    user_prompt = _build_user_prompt(description)
    response = client.complete(system_prompt, user_prompt, task="formalizer")

    try:
        hypothesis_id, hyp_config = _parse_llm_response(response)
    except ValueError:
        print("Could not parse LLM response. Raw output:\n")
        print(response)
        print("\nPlease manually create the config entry and module.")
        return {"error": True, "raw_response": response}

    # ── append to hypotheses.yaml ─────────────────────────────────────────────
    config_path.parent.mkdir(parents=True, exist_ok=True)
    existing = {}
    if config_path.exists():
        with open(config_path) as f:
            existing = yaml.safe_load(f) or {}
    existing.setdefault("hypotheses", {})[hypothesis_id] = hyp_config
    with open(config_path, "w") as f:
        yaml.dump(existing, f, default_flow_style=False, allow_unicode=True)

    # ── resolve stub file path ────────────────────────────────────────────────
    module_str = hyp_config.get(
        "module", f"edge_catcher.hypotheses.local.{hypothesis_id}"
    )
    if hypotheses_base:
        module_file = _module_str_to_relative_file(module_str, hypotheses_base)
    else:
        module_file = _module_str_to_file(module_str)

    # ── create parent dirs + __init__.py sentinels ────────────────────────────
    module_file.parent.mkdir(parents=True, exist_ok=True)
    for parent in reversed(list(module_file.parents)):
        init_file = parent / "__init__.py"
        if parent.exists() and not init_file.exists():
            try:
                init_file.touch()
            except OSError:
                pass

    # ── write stub ────────────────────────────────────────────────────────────
    name = hyp_config.get("name", hypothesis_id)
    module_file.write_text(_stub_content(hypothesis_id, name))

    message = (
        f"Created hypothesis '{hypothesis_id}'.\n"
        f"  Config: {config_path}\n"
        f"  Module: {module_file}\n\n"
        f"Edit the module, then run:\n"
        f"  python -m edge_catcher analyze --hypothesis {hypothesis_id}"
    )
    return {
        "hypothesis_id": hypothesis_id,
        "config_path": config_path,
        "module_path": module_file,
        "message": message,
    }
