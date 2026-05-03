"""Provider-agnostic LLM client (Anthropic, OpenAI, OpenRouter, Claude Code CLI)."""
from __future__ import annotations

import logging
import os
import shutil
from typing import Optional

logger = logging.getLogger(__name__)


def detect_active_provider(include_cli: bool = True) -> Optional[str]:
	"""Detect the active LLM provider from environment.

	Resolution order:
	1. EDGE_CATCHER_LLM_PROVIDER env var
	2. First API key found (ANTHROPIC > OPENAI > OPENROUTER)
	3. claude-code CLI on PATH (only when include_cli=True)
	"""
	env = os.getenv("EDGE_CATCHER_LLM_PROVIDER")
	if env:
		return env
	if os.getenv("ANTHROPIC_API_KEY"):
		return "anthropic"
	if os.getenv("OPENAI_API_KEY"):
		return "openai"
	if os.getenv("OPENROUTER_API_KEY"):
		return "openrouter"
	if include_cli and (shutil.which("claude") or shutil.which("npx")):
		return "claude-code"
	return None


class LLMError(Exception):
	"""Raised when an LLM operation fails."""


class LLMClient:
    """
    Provider-agnostic LLM client.

    Provider detection order:
    1. ``provider`` constructor argument
    2. ``EDGE_CATCHER_LLM_PROVIDER`` env var
    3. Auto-detect from which API key env var is set
       (ANTHROPIC_API_KEY first, then OPENAI_API_KEY, then OPENROUTER_API_KEY)
    4. Fall back to ``claude-code`` if the ``claude`` CLI is on PATH
    """

    _DEFAULT_MODELS: dict[str, dict[str, str]] = {
        "anthropic": {
            "formalizer": "claude-sonnet-4-20250514",
            "interpreter": "claude-haiku-4-5-20251001",
            "strategizer": "claude-sonnet-4-20250514",
            "ideator": "claude-sonnet-4-20250514",
            "refiner": "claude-sonnet-4-20250514",
        },
        "openai": {
            "formalizer": "gpt-4o",
            "interpreter": "gpt-4o-mini",
            "strategizer": "gpt-4o",
            "ideator": "gpt-4o",
            "refiner": "gpt-4o",
        },
        "claude-code": {
            "formalizer": "sonnet",
            "interpreter": "haiku",
            "strategizer": "sonnet",
            "ideator": "opus",
            "refiner": "sonnet",
        },
    }

    def __init__(
        self,
        provider: Optional[str] = None,
        model: Optional[str] = None,
        api_key: Optional[str] = None,
    ) -> None:
        self.provider = self._resolve_provider(provider)
        self.model = model
        self.api_key = api_key or self._resolve_api_key()
        self.last_usage: dict[str, int] = {}

    # -- provider / key resolution ---------------------------------------------

    def _resolve_provider(self, explicit: Optional[str]) -> Optional[str]:
        if explicit:
            return explicit
        return detect_active_provider()

    def _resolve_api_key(self) -> Optional[str]:
        _env_vars = {
            "anthropic": "ANTHROPIC_API_KEY",
            "openai": "OPENAI_API_KEY",
            "openrouter": "OPENROUTER_API_KEY",
        }
        var = _env_vars.get(self.provider or "")
        return os.getenv(var) if var else None

    def _resolve_model(self, task: str) -> Optional[str]:
        if self.model:
            return self.model
        return self._DEFAULT_MODELS.get(self.provider or "", {}).get(task)

    # -- public API ------------------------------------------------------------

    def complete(self, system_prompt: str, user_prompt: str, task: str = "formalizer") -> str:
        """Call the LLM and return the response text.

        After each call, ``self.last_usage`` is updated with token counts
        (keys vary by provider).
        """
        self.last_usage = {}
        if self.provider == "claude-code":
            model = self._resolve_model(task)
            return self._call_claude_code(system_prompt, user_prompt, model, task=task)
        if not self.provider or not self.api_key:
            raise LLMError(
                "AI features require an API key. Set ANTHROPIC_API_KEY, OPENAI_API_KEY, "
                "or OPENROUTER_API_KEY in your environment or .env file."
            )
        model = self._resolve_model(task)
        if model is None:
            raise LLMError(
                f"No model resolved for provider={self.provider!r} task={task!r}. "
                "Set 'model' in the config or add a default to _DEFAULT_MODELS."
            )
        if self.provider == "anthropic":
            return self._call_anthropic(system_prompt, user_prompt, model)
        if self.provider == "openai":
            return self._call_openai(system_prompt, user_prompt, model)
        if self.provider == "openrouter":
            return self._call_openrouter(system_prompt, user_prompt, model)
        raise LLMError(f"Unknown provider: {self.provider!r}")

    # -- provider implementations ----------------------------------------------

    # Effort level per task for claude-code provider.
    _CLAUDE_CODE_EFFORT: dict[str, str] = {
        "ideator": "high",
        "strategizer": "high",
        "refiner": "high",
    }

    def _call_claude_code(
        self, system_prompt: str, user_prompt: str, model: str | None,
        task: str = "",
    ) -> str:
        """Call Claude Code CLI in one-shot mode (no API key required).

        Uses ``claude`` directly if on PATH, otherwise falls back to
        ``npx @anthropic-ai/claude-code``.
        """
        import subprocess
        if shutil.which("claude"):
            cmd = ["claude", "-p"]
        else:
            cmd = ["npx", "--yes", "@anthropic-ai/claude-code", "-p"]
        if model:
            cmd += ["--model", model]
        effort = self._CLAUDE_CODE_EFFORT.get(task)
        if effort:
            cmd += ["--effort", effort]
        budget = os.getenv("EDGE_CATCHER_CC_BUDGET_USD")
        if budget:
            cmd += ["--max-budget-usd", budget]
        prompt = f"{system_prompt}\n\n---\n\n{user_prompt}"
        timeout = 3600
        try:
            proc = subprocess.run(
                cmd,
                input=prompt,
                capture_output=True,
                text=True,
                timeout=timeout,
                shell=True,
                encoding="utf-8",
            )
        except FileNotFoundError:
            raise LLMError(
                "Claude Code CLI not found. Install it: npm install -g @anthropic-ai/claude-code"
            )
        except subprocess.TimeoutExpired:
            raise LLMError("Claude Code CLI timed out after 5 minutes")
        stdout = proc.stdout.strip()
        stderr_detail = proc.stderr.strip()
        if proc.returncode != 0:
            # Salvage the LLM response when the CLI prints valid output but
            # exits non-zero due to a downstream-of-response failure (most
            # commonly a SessionEnd hook getting cancelled during shutdown
            # in `-p` mode). Real failures — auth, quota, model-not-found —
            # produce empty stdout because the response was never generated,
            # so they still surface as LLMError.
            if stdout:
                logger.warning(
                    "Claude Code CLI exited %d but stdout has content; "
                    "salvaging response. stderr: %s",
                    proc.returncode, stderr_detail or "(empty)",
                )
                return stdout
            detail = stderr_detail or stdout
            raise LLMError(f"Claude Code CLI failed (exit {proc.returncode}): {detail}")
        return stdout

    def _call_anthropic(self, system_prompt: str, user_prompt: str, model: str) -> str:
        try:
            import anthropic
        except ImportError:
            raise LLMError("Anthropic SDK not installed. Run: pip install -e '.[ai]'")
        client = anthropic.Anthropic(api_key=self.api_key)
        msg = client.messages.create(
            model=model,
            max_tokens=2048,
            system=[
                {
                    "type": "text",
                    "text": system_prompt,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[{"role": "user", "content": user_prompt}],
        )
        usage = msg.usage
        cache_created = getattr(usage, "cache_creation_input_tokens", 0) or 0
        cache_read = getattr(usage, "cache_read_input_tokens", 0) or 0
        self.last_usage = {
            "input_tokens": usage.input_tokens,
            "output_tokens": usage.output_tokens,
            "cache_creation_input_tokens": cache_created,
            "cache_read_input_tokens": cache_read,
        }
        if cache_created or cache_read:
            logger.info(
                "Prompt cache: created=%d read=%d input=%d output=%d",
                cache_created, cache_read, usage.input_tokens, usage.output_tokens,
            )
        # Anthropic ContentBlock is a 12-variant union (TextBlock, ThinkingBlock,
        # tool-use variants, etc.); only TextBlock has .text. We don't enable
        # tools or thinking for the formalizer/ideator paths so the first block
        # is always TextBlock — narrow explicitly so type-checking holds and
        # the failure mode is loud if someone changes the API call shape.
        first = msg.content[0]
        if not isinstance(first, anthropic.types.TextBlock):
            raise LLMError(
                f"Anthropic returned a non-text first content block "
                f"({type(first).__name__}); update _call_anthropic if tool-use "
                f"or thinking modes were enabled."
            )
        return first.text

    def _call_openai(self, system_prompt: str, user_prompt: str, model: str) -> str:
        try:
            import openai
        except ImportError:
            raise LLMError("OpenAI SDK not installed. Run: pip install -e '.[ai]'")
        client = openai.OpenAI(api_key=self.api_key)
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        openai_usage = resp.usage
        if openai_usage:
            self.last_usage = {
                "input_tokens": openai_usage.prompt_tokens,
                "output_tokens": openai_usage.completion_tokens,
            }
        # OpenAI's chat completion content is Optional[str] in the SDK type
        # signature (refusal/null cases). For our prompts none of those fire,
        # but raise loudly if it ever does so the caller doesn't get back "".
        content = resp.choices[0].message.content
        if content is None:
            raise LLMError(
                "OpenAI returned a null content block; check refusal field on the response."
            )
        return content

    def _call_openrouter(self, system_prompt: str, user_prompt: str, model: str) -> str:
        try:
            import httpx
        except ImportError:
            raise LLMError("httpx not installed. Run: pip install httpx")
        if not model:
            raise LLMError(
                "OpenRouter requires a model name. Use --model to specify one."
            )
        resp = httpx.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={"Authorization": f"Bearer {self.api_key}"},
            json={
                "model": model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            },
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        or_usage = data.get("usage")
        if or_usage:
            self.last_usage = {
                "input_tokens": or_usage.get("prompt_tokens", 0),
                "output_tokens": or_usage.get("completion_tokens", 0),
            }
        return data["choices"][0]["message"]["content"]
