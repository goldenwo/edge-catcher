"""Provider-agnostic LLM client (Anthropic, OpenAI, OpenRouter, Claude Code CLI)."""
from __future__ import annotations

import logging
import os
import shutil
from typing import Optional

logger = logging.getLogger(__name__)


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
        env = os.getenv("EDGE_CATCHER_LLM_PROVIDER")
        if env:
            return env
        if os.getenv("ANTHROPIC_API_KEY"):
            return "anthropic"
        if os.getenv("OPENAI_API_KEY"):
            return "openai"
        if os.getenv("OPENROUTER_API_KEY"):
            return "openrouter"
        if shutil.which("claude") or shutil.which("npx"):
            return "claude-code"
        return None

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
        if proc.returncode != 0:
            detail = proc.stderr.strip() or proc.stdout.strip()
            raise LLMError(f"Claude Code CLI failed (exit {proc.returncode}): {detail}")
        return proc.stdout.strip()

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
        return msg.content[0].text

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
        return resp.choices[0].message.content

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
