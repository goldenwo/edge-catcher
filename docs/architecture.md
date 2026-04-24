# Architecture

This document describes the high-level structure of edge-catcher: the
research pipeline, the live trading pipeline, and the boundaries
between framework and user research.

## Four-stage filter

A new trading idea passes through four progressively-rigorous stages
before any capital — paper or real — gets deployed:

1. **Hypothesis.** A statistical claim about the market. Either
   formalized from plain English by the research agent's `formalizer`
   (`edge_catcher/ai/formalizer.py`) or written by hand into
   `config/hypotheses.yaml` / `config.local/hypotheses.yaml`. The
   hypothesis registry runs the underlying statistical test
   (`proportions_ztest` for binary outcomes, clustered SEs by expiry
   date) and reports whether the signal is real before any strategy
   code gets written.

2. **Event backtest.** Fast, approximate. Replays historical trade
   events from the SQLite DB through your `Strategy` subclass via
   `edge_catcher.runner.event_backtest`. Cheap (seconds to minutes per
   run), great for parameter sweeps and the autonomous research loop's
   grid phase. Does **not** reproduce live execution fidelity — no
   orderbook state, no synthetic settlement model, no dispatch
   plumbing — so it answers "does this signal have directional edge?"
   not "what would the live P&L be?".

3. **Replay backtest.** Slow, high-fidelity verdict. Replays a captured
   daily bundle (compressed JSONL of every WS frame plus state
   snapshots) through the exact `dispatch_message` path the live paper
   trader uses, via `edge_catcher.monitors.replay.backtester`. Seeds
   `MarketState`, open trades, and strategy state from the prior day's
   bundle. Answers "would replay produce the same trades as live?".
   Slower than the event backtester because it walks every WS frame in
   order, but bit-exact reproducible.

4. **Paper trader.** P&L source of truth. The live paper trader
   (`edge_catcher.monitors.engine`) runs the same dispatch path against
   the live Kalshi WS feed, records trades to a `paper_trades` SQLite
   DB, and captures every event for replay.

Each stage is more expensive and more realistic than the last, so
ideas that survive the cheap stages get the expensive ones; ideas that
fail cheaply get killed before burning compute or attention. The four
stages are independent codepaths — a regression in one is not
automatically a regression in another.

## Research loop

The autonomous research agent (`edge_catcher/research/`) automates
stages 1–2:

```
formalize → grid plan → validate → journal → self-performance
```

- **Formalize.** LLM converts plain-English hypotheses into structured
  config (`ai/formalizer.py`).
- **Grid plan.** `research/grid_planner.py` enumerates every
  strategy×series combination available, deduplicates already-tested
  pairs, and queues runs (`research/run_queue.py`).
- **Validate.** Each run goes through statistical gates:
  Harvey-Liu-Zhu threshold (t > 3.0 instead of 1.96 to control multiple
  comparison bias), clustered standard errors by expiration date,
  Bonferroni correction. The `evaluator.py` produces a verdict —
  promote, explore, or kill.
- **Journal.** `research/tracker.py` persists every result to SQLite
  with the full audit trail (decisions, parameters, statistics) so
  later runs deduplicate against history.
- **Self-performance.** The agent reports its own track record (how
  often promoted strategies survive replay/paper, false-discovery
  rate) so its calibration can itself be audited.

The LLM ideator (`research/llm_ideator.py`) only activates after the
grid has produced enough results to seed it (default: 10+ runs). Grid
phase needs no LLM calls at all, which keeps research cheap when you
just want to sweep parameters.

## Capture / replay pipeline

The live paper trader writes every WS frame it sees to a
`RawFrameWriter` (`monitors/capture/writer.py`). At UTC midnight, a
rotation callback assembles a **daily bundle**:

- Compressed JSONL of every dispatched event for the day
- The `strategies_local.py` snapshot used to run that day
- The `config/` and `config.local/` snapshots
- `market_state.json`, `open_trades.json`, and `strategy_state.json`
  captured at the moment of rotation
- A "day slice" — the prior 24h of relevant context

The bundle is uploaded to Cloudflare R2 (`monitors/capture/transport.py`)
and the raw working file is deleted. The replay backtester
(`monitors/replay/backtester.py`) can take any bundle and reproduce
that day bit-exact: it instantiates a fresh engine, seeds it from the
state snapshots, and walks the JSONL through the same `dispatch_message`
function the live engine uses. This is what makes Stage 3 of the
four-stage filter possible — the same code answers "what did happen?"
and "what would happen?".

Bundles are self-contained: replay does not need access to the live
DB, the live engine, or even the live machine. Pull a bundle from R2,
run replay, get a deterministic answer.

## LLM abstraction

The framework supports four LLM providers via a single client at
`edge_catcher/ai/client.py`:

- **Claude Code CLI** — auto-detected if `claude` is on the PATH. No
  API key needed; reuses your existing Claude Code session.
- **Anthropic** — `ANTHROPIC_API_KEY`.
- **OpenAI** — `OPENAI_API_KEY`.
- **OpenRouter** — `OPENROUTER_API_KEY`.

Auto-detection picks the first available provider; explicit override
goes through the `--provider` CLI flag or the
`EDGE_CATCHER_LLM_PROVIDER` environment variable. Each task
(`formalizer`, `interpreter`, `strategizer`, `ideator`) has its own
default model and effort level — see the README "Task Models" table.
A per-call budget cap is enforced via `EDGE_CATCHER_CC_BUDGET_USD`.

The AI features are strictly optional. The grid phase of the research
loop runs without any LLM, the backtester runs without any LLM, and
the paper trader runs without any LLM. The LLM only enters when you
ask for plain-English ↔ hypothesis translation or autonomous
ideation.

## Adapter registry

Adding a new exchange is a strictly additive operation: drop a new
directory under `edge_catcher/adapters/<exchange>/`, concat your
`<EXCHANGE>_ADAPTERS` list into `api/adapter_registry.py`, register
your download + data-check handlers in `api/dispatchers.py`, done.
There is no central if/elif on exchange name in the core code.

Two registries cooperate:

- `edge_catcher.adapters.<exchange>.registry.<EXCHANGE>_ADAPTERS` — a
  list of `AdapterMeta` entries describing the metadata for each data
  source (id, db_file, fee_model, optional `extra` bag).
  `api/adapter_registry.py` aggregates these into a single `ADAPTERS`
  list that the rest of the system queries.
- `api.dispatchers.DOWNLOAD_DISPATCHERS` and
  `DATA_CHECK_DISPATCHERS` — keyed by `meta.exchange`, hold the
  callables that build download threads and report whether a DB
  contains data. Call sites in `api/main.py` and
  `api/download_service.py` route through these instead of branching
  on exchange string.

Full walkthrough in [`docs/adapter-guide.md`](adapter-guide.md). Design
rationale in [ADR 0001](adr/0001-adapter-registry.md).

## Gitignore split

The framework cleanly separates **public** (tracked, no edge revealed)
from **private** (gitignored, edge-revealing):

| Tracked | Gitignored |
|---------|------------|
| Framework code (`edge_catcher/`, `api/`, `ui/`) | `config.local/`, `edge_catcher/hypotheses/local/` |
| Example configs (`config/`) | `edge_catcher/monitors/strategies_local.py` |
| `strategies_example.py` (tutorial) | `scripts/`, `reports/` |
| Framework tests (`tests/`) | `tests/test_local_*.py`, `tests/test_flow_*.py` |
| Public docs (`docs/`) | `docs/superpowers/` |

At runtime the loaders merge the two. `config.local/hypotheses.yaml`
is layered over `config/hypotheses.yaml`. Private hypothesis modules
in `edge_catcher/hypotheses/local/` are auto-discovered by the same
registry that finds the public ones. The runner's strategy auto-discovery
walks both `strategies.py` and `strategies_local.py`.

This split is the design contract that makes edge-catcher publishable
as a framework: the public repo holds the *plumbing*, the private tree
holds the *positions*. Contributors only ever touch tracked paths;
researchers using the framework get a clean home for their proprietary
work without having to vendor or fork the framework.
