# edge-catcher

> Research-grade pipeline for finding pricing inefficiencies on prediction markets — from hypothesis to backtest to paper-trader, with the same code path at every step.

[![CI](https://github.com/goldenwo/edge-catcher/actions/workflows/ci.yml/badge.svg)](https://github.com/goldenwo/edge-catcher/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![Release](https://img.shields.io/github/v/release/goldenwo/edge-catcher)](https://github.com/goldenwo/edge-catcher/releases)

edge-catcher is a Python framework for building and validating systematic strategies on event markets like Kalshi. Take a hunch — *"NO trades at <5¢ rebound 70% of the time"* — and walk it through a four-stage filter ending in a live paper trader, with bit-exact reproducibility between every stage.

**Built for** quants, researchers, and developers who want rigorous tooling for prediction-market alpha without the overfitting trap.

---

## Highlights

- 🔬 **Anti-p-hacking by default** — Pre-registered hypotheses, Bonferroni + Harvey-Liu-Zhu (t > 3.0), clustered SEs, mandatory out-of-sample validation
- 🤖 **Autonomous research loop** — Exhaustive grid sweep + LLM ideator with kill/explore/promote verdicts
- 📈 **Event-driven backtester** — Fee-adjusted P&L, Sharpe, win rate, drawdown, streaming stats (O(1) memory)
- 🎯 **Capture / replay paper trader** — Daily JSONL bundles, replay backtester reproduces live trades bit-exactly
- 🔌 **Pluggable adapter registry** — Add a new exchange in ~50 LOC
- 🧠 **Optional AI (no API key required)** — Auto-detects Claude Code CLI; falls back to Anthropic / OpenAI / OpenRouter
- 🖥️ **React + FastAPI web UI** — 8 pages: Dashboard, Data Sources, Analyze, Strategize, Research, Hypotheses, Backtest, Settings
- ✅ **900+ tests** — Fully mocked, runs without live API keys

---

## Quickstart

```bash
git clone https://github.com/goldenwo/edge-catcher
cd edge-catcher
pip install -e ".[dev]"

# First backtest — bundled fixture, no data download, no API keys
edge-catcher backtest \
    --series DEMO_SERIES \
    --db-path edge_catcher/data/examples/demo_markets.db \
    --strategy longshot_fade_example --json
```

You should see a JSON blob with `"status": "ok"` and a per-strategy P&L breakdown. That's the toolchain end-to-end in 60 seconds.

**Next:** the [5-minute walkthrough](docs/quickstart.md) adds a P&L report against bundled paper-trade fixtures.

---

## How it works

edge-catcher gives you a **four-stage filter** for taking a hypothesis from idea to live paper-trading:

```
                ┌─→  1. Hypothesis      Does the signal exist?           (research agent)
                │
                ├─→  2. Event backtest  Does it have directional edge?   (fast, approximate)
                │
                ├─→  3. Replay backtest Does live execution match?       (high-fidelity)
                │
                └─→  4. Paper trader    Does P&L hold up live?           (source of truth)
```

Stages 3 and 4 share the same dispatch path. **A passing replay means your live trader will behave identically** — the same `dispatch_message` function processes both a captured JSONL frame and a real-time WebSocket message.

---

## Features

### Research

- **Autonomous research loop** — exhaustive strategy×series grid, then LLM-driven hypothesis ideation when the grid plateaus
- **Pre-registered hypothesis configs** — auto-discovered from `hypotheses/` modules; no inline statistical hacks
- **Clustered standard errors** — by expiration date, since contracts on the same date share a common shock
- **Multi-comparison correction** — Bonferroni + Harvey-Liu-Zhu (t > 3.0 instead of 1.96) drops false discovery rate to <0.3%
- **Validation gates** — out-of-sample windows enforce honest evaluation

### Backtesting

- **Event-driven** — replays historical trade events at original timestamps with fee-adjusted P&L
- **Per-strategy breakdowns** — win rate, Sharpe, deployed capital, drawdown, expectancy
- **Streaming stats** — closed-trade accumulators run in O(1) memory regardless of dataset size
- **List discovery** — `--list-strategies` and `--list-series` show what's available before you start

### Live trading + replay

- **Paper trader** — real-time WebSocket dispatch, orderbook-aware sizing, fresh-book gates, multi-strategy concurrency
- **Capture pipeline** — daily JSONL bundles assembled at midnight UTC with state snapshots; uploads to Cloudflare R2 with local fallback
- **Replay backtester** — re-runs a captured day through the exact `dispatch_message` path used live; verifies bit-exact reproducibility
- **Discord notifications** — trade entry/exit with per-strategy color/emoji + P&L

### Adapters

- **Kalshi** — settled markets across BTC, altcrypto, sports, politics, financials, esports, weather, entertainment
- **Coinbase** — 1-minute OHLC candles for any asset
- **Registry pattern** — see [docs/adapter-guide.md](docs/adapter-guide.md) for adding a new exchange

### Reporting + UI

- **Reporting CLI** — `python -m edge_catcher.reporting` produces daily P&L JSON with corrected `deployed = entry_price × fill_size` math
- **React + Vite dashboard** — Dashboard, Data Sources, Analyze, Strategize, Research, Hypotheses, Backtest, Settings
- **FastAPI backend** — REST endpoints, background download workers, live progress streams

### AI (optional)

- **Claude Code CLI** — auto-detected if `claude` is on PATH; no API key required
- **Anthropic / OpenAI / OpenRouter** — fallback providers via `ANTHROPIC_API_KEY` / `OPENAI_API_KEY` / `OPENROUTER_API_KEY`
- **Tasks**: hypothesis formalization, result interpretation, strategy code generation, autonomous ideation
- **Per-call budget cap** via `EDGE_CATCHER_CC_BUDGET_USD`
- **Deep dive**: provider trade-offs, cost management, model selection, prompt caching → [docs/llm-providers.md](docs/llm-providers.md)

---

## Documentation

| | |
|--|--|
| **Get started** | [quickstart.md](docs/quickstart.md) — 5-minute walkthrough |
| **Build your first strategy** | [tutorial.md](docs/tutorial.md) — 30-min end-to-end |
| **Write a strategy** | [strategy-guide.md](docs/strategy-guide.md) |
| **Add an exchange** | [adapter-guide.md](docs/adapter-guide.md) |
| **System design** | [architecture.md](docs/architecture.md) |
| **P&L reporting** | [reporting.md](docs/reporting.md) |
| **Deploy with Docker / VPS** | [deployment-docker.md](docs/deployment-docker.md) |
| **Deploy with systemd (no Docker)** | [deploy/README.md](deploy/README.md) |
| **Research data flow** | [research-pipeline-data-flow.md](docs/research-pipeline-data-flow.md) |
| **LLM provider trade-offs** | [llm-providers.md](docs/llm-providers.md) |
| **What's next** | [roadmap.md](docs/roadmap.md) |
| **ADRs** | [docs/adr/](docs/adr/) |

---

## Project structure

```
edge_catcher/
├── adapters/      Exchange data collectors (Kalshi, Coinbase)
├── ai/            Provider-agnostic LLM client + prompts
├── hypotheses/    Statistical hypothesis modules + registry
├── monitors/      Paper trader (engine, dispatch, capture, replay, sizing)
├── research/      Autonomous research agent + grid planner + ideator
├── runner/        Backtester + strategy framework
├── reporting/     P&L reporting CLI
└── storage/       SQLite persistence layer
api/               FastAPI backend
ui/                React + Vite frontend (8 pages)
config/            Public market configs (per category)
tests/             900+ tests across 75 files
```

### Privacy by design

The framework ships publicly; **your strategies stay yours**. Private zones are gitignored:

- `edge_catcher/runner/strategies_local.py` (and `monitors/strategies_local.py`)
- `edge_catcher/hypotheses/local/`
- `config.local/`
- `scripts/` (analysis)
- `reports/` (research outputs)

The runtime auto-discovers and merges these at load time. You never commit research.

---

## Use cases

- **Quant researcher** — Pipeline a hypothesis from `formalize → grid backtest → AI ideator → replay → paper trader` without re-implementing infrastructure each time
- **Strategy developer** — Drop a new `Strategy` subclass in `strategies_local.py`; the backtester, research loop, and paper trader auto-discover it
- **Exchange contributor** — Implement `AdapterMeta` + a download function and the new exchange shows up everywhere (CLI, UI, research loop)
- **Statistical purist** — Pre-registered hypotheses + clustered SEs + HLZ thresholds + OOS gates prevent the usual finance research traps

---

## Running tests

```bash
pytest tests/ -v
```

900+ tests run fully mocked — no live API keys needed. Tests that exercise the paper trader or FastAPI dashboard skip cleanly when the `[live]` or `[ui]` extras aren't installed; install `[dev,live,ui]` if you want full coverage.

---

## Contributing

PRs welcome on the framework. See [CONTRIBUTING.md](CONTRIBUTING.md) for the contribution flow and the [roadmap](docs/roadmap.md) for v1.1 candidates.

---

## License

[MIT](LICENSE)
