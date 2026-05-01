# LLM Providers

`edge_catcher.ai` is the in-tree wrapper for hypothesis formalization,
result interpretation, strategy code generation, and autonomous ideation.
It supports four providers and resolves the active one from environment
without requiring a config file. This guide covers how to pick one,
manage cost, and override defaults per task.

## Quickstart

The minimum viable setup is one environment variable. Pick the provider
you have credentials for; the rest auto-configure.

```bash
# Anthropic — preferred for the autonomous research loop
export ANTHROPIC_API_KEY=sk-ant-...

# OpenAI
export OPENAI_API_KEY=sk-...

# OpenRouter — single key, many models
export OPENROUTER_API_KEY=sk-or-...

# Claude Code CLI — no API key, runs against your local Claude Code
# install (free if you already use it). Auto-detected if `claude` is on PATH.
```

Test that resolution works:

```python
from edge_catcher.ai.client import LLMClient
print(LLMClient().provider)  # "anthropic" / "openai" / "openrouter" / "claude-code"
```

## Provider comparison

| Provider | Best for | Cost shape | Caveats |
|---|---|---|---|
| **Anthropic** (Claude Sonnet/Haiku) | Default for the research loop. Strong on structured-output tasks (formalizer, refiner) and multi-turn reasoning (ideator). | Pay per token; prompt caching cuts repeat-system-prompt costs ~90% on the second+ call. | Rate limits per workspace; check your tier before running long sweeps. |
| **OpenAI** (GPT-4o / GPT-4o-mini) | Cost-sensitive workloads. `4o-mini` interpreter is ~10× cheaper than Anthropic Haiku for similar quality on JSON-out tasks. | Pay per token; no automatic prompt caching at the API level. | Higher rate of JSON malformation on long structured outputs vs. Anthropic; the formalizer retries internally but factor that into iteration budgets. |
| **OpenRouter** | Provider-agnostic experimentation. Useful when you want to A/B GPT-5, Llama-3.x, or DeepSeek behind one key without changing client code. | Pay per token (a small markup over upstream); most models priced per the upstream's official rate. | Per-model availability fluctuates; pin a known-good model in `model=` when running production cron. |
| **Claude Code CLI** | Free local development. The `claude-code` provider invokes the Claude Code CLI as a subprocess and inherits your existing subscription / quota. | $0 incremental — uses your existing Claude Code budget. | Sequential subprocess overhead (~1-2s per call) makes it slower for high-throughput sweeps. Use Anthropic-direct for production loops. |

## Per-task model selection

Each provider has per-task defaults the client falls back to when no
explicit `model=` argument is passed. The defaults match the task's
reasoning-vs-throughput trade-off:

| Task | What it does | Reasoning needed? | Default tier |
|---|---|---|---|
| `formalizer` | Hypothesis JSON → strategy spec | high (structured output) | Sonnet / GPT-4o |
| `interpreter` | Backtest result → human-readable summary | low (paraphrase) | Haiku / 4o-mini |
| `strategizer` | Strategy spec → Python code | high | Sonnet / GPT-4o |
| `ideator` | Backlog of past results → novel hypothesis ideas | high (creative) | Sonnet (Opus on claude-code) |
| `refiner` | Candidate strategy + bug report → patch | high | Sonnet / GPT-4o |

**Look up the actual defaults** in
[`edge_catcher/ai/client.py`](../edge_catcher/ai/client.py) `_DEFAULT_MODELS`
— they evolve as model lineups change. Keeping the table here would
drift; the source is the canonical reference.

### Overriding the model per call

```python
from edge_catcher.ai.client import LLMClient

# Use a specific model for one call (overrides per-task default).
client = LLMClient(provider="anthropic", model="claude-opus-4-20250514")
response = client.complete(system_prompt, user_prompt, task="ideator")
```

### Overriding the model per task globally

Set `EDGE_CATCHER_LLM_PROVIDER=<provider>` to lock the provider, then
use the YAML config to pin per-task models:

```yaml
# config.local/research.yaml (gitignored — your local overrides go here)
ai:
  provider: anthropic
  models:
    interpreter: claude-haiku-4-5-20251001
    ideator: claude-opus-4-20250514  # bigger model for creative work
```

The autonomous research loop respects these overrides; ad-hoc CLI calls
can still pass `model=` explicitly.

## Cost management

### Prompt caching (Anthropic only)

The formalizer and ideator pass a multi-page system prompt every call.
Anthropic's API supports prompt caching with `cache_control` markers,
which the in-tree client sets automatically — second-and-subsequent
calls within a 5-minute window read the system prompt from cache for
~10% of the tokens-in cost. The savings are visible in
`client.last_usage["cache_read_input_tokens"]` after each call.

To verify caching is active in a sweep:

```python
client = LLMClient()
for hypothesis in batch:
    client.complete(system_prompt, hypothesis.spec, task="formalizer")
    print(client.last_usage)
# After call 1: cache_creation_input_tokens=N, cache_read=0
# After call 2+: cache_creation=0, cache_read=N (cache hit)
```

If `cache_read` stays 0 across calls, your sweep is taking more than
5 minutes per call (cache TTL expired). Reduce per-call work or run
batches concurrently.

### Tier selection

For sweeps that drive >1000 calls (e.g., autonomous ideation runs),
the cost-per-task tier matters more than absolute model quality:

```bash
# Production cron — Haiku for everything that doesn't need Sonnet
export EDGE_CATCHER_LLM_PROVIDER=anthropic
# Then in your loop config, set interpreter+formalizer to haiku:
```

Empirically Haiku is 80–90% as good as Sonnet on the formalizer task
and 5× cheaper. The autonomous research loop's `interpreter` task is
already Haiku by default precisely because the work is paraphrase-heavy.

### Model fallback

The client raises `LLMError` on missing model resolution. To survive
transient API failures gracefully:

```python
try:
    response = client.complete(system_prompt, user_prompt, task="formalizer")
except LLMError as e:
    if "rate limit" in str(e).lower():
        time.sleep(60)
        client = LLMClient(provider="openrouter", model="anthropic/claude-sonnet-4")
        response = client.complete(system_prompt, user_prompt, task="formalizer")
    else:
        raise
```

The autonomous research loop handles this internally — see
[`research/loop.py`](../edge_catcher/research/loop.py) `_run_with_retry`.
The above pattern is for one-off scripts.

## Provider resolution order

The client picks the first one of these that returns a value:

1. The `provider` constructor argument (explicit)
2. The `EDGE_CATCHER_LLM_PROVIDER` env var
3. Auto-detect from the first API key found, in this order:
   `ANTHROPIC_API_KEY` → `OPENAI_API_KEY` → `OPENROUTER_API_KEY`
4. Fall back to `claude-code` if the `claude` CLI is on PATH

This means setting `EDGE_CATCHER_LLM_PROVIDER` lets you switch
providers without touching code, even when multiple keys are set in
your environment. Useful for cost-comparison sweeps:

```bash
EDGE_CATCHER_LLM_PROVIDER=openai  python -m edge_catcher.research run ...
EDGE_CATCHER_LLM_PROVIDER=anthropic python -m edge_catcher.research run ...
# Compare results and per-call usage from the two runs.
```

## Claude Code CLI vs API

The `claude-code` provider is a great default for **interactive
development** — write a prompt, get a response, no token meter ticking.

For **production cron-driven loops**, switch to Anthropic-direct:

| | Claude Code CLI | Anthropic API |
|---|---|---|
| Cost | $0 incremental (your existing CC subscription) | Pay-per-token |
| Latency | Subprocess overhead ~1–2s per call | ~200ms (no subprocess) |
| Concurrency | Sequential (1 CLI at a time per system) | Many parallel (subject to rate limit) |
| Prompt caching | No | Yes |

A sweep of 500 hypotheses with the formalizer:
- CLI: 500 × 2s ≈ 17 minutes wall clock; $0
- API + caching: 500 × 0.3s ≈ 2.5 minutes wall clock; ~$2

Pick the right tool for the use case. The client makes this a
one-line switch in your environment.

## Troubleshooting

**`LLMError: No model resolved for provider='anthropic' task='ideator'`** —
Either set `model=` explicitly when constructing `LLMClient`, or extend
`_DEFAULT_MODELS["anthropic"]` with an entry for that task. This usually
means a new task name was added to the codebase but the defaults map
wasn't updated.

**`LLMError: Anthropic returned a non-text first content block`** —
You're getting back tool-use or thinking-mode content blocks from a
codepath that doesn't enable them. Check that the `messages.create`
call site doesn't include `tools=` or `thinking=` parameters.

**`LLMError: OpenAI returned a null content block`** — The model
returned a refusal. Inspect the request prompt for content that might
trigger OpenAI's safety classifier (typically the user prompt, not the
system prompt). The autonomous loop logs the prompt prefix on this
error so you can grep recent runs.

**No provider auto-detected** — Set `EDGE_CATCHER_LLM_PROVIDER`
explicitly. The client's auto-detect requires either (a) an API key env
var set in the current shell, or (b) the `claude` CLI on PATH.

## See also

- [`edge_catcher/ai/client.py`](../edge_catcher/ai/client.py) — `LLMClient` source + `_DEFAULT_MODELS`
- [`edge_catcher/ai/formalizer.py`](../edge_catcher/ai/formalizer.py) — typical client usage pattern
- [Research pipeline data flow](research-pipeline-data-flow.md) — where each task is invoked
- README §"AI (optional)" — short version of provider setup
