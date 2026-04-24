# edge-catcher

A production-grade hypothesis testing pipeline for detecting pricing inefficiencies in prediction markets (Kalshi), with automated backtesting, strategy research, and an optional AI-driven ideation loop.

Built for rigorous, anti-p-hacking statistical research: pre-registered hypotheses, out-of-sample validation, clustered standard errors, and multi-comparison correction.

---

## Features

- **Multi-market adapter pattern** — Kalshi (multiple categories) + Coinbase OHLC
- **Event-driven backtester** — Run strategies against historical trade data with fee-adjusted PnL, Sharpe ratio, win rate, and per-strategy breakdowns
- **Autonomous research loop** — Grid sweep across all strategy×series combinations, then LLM-driven hypothesis ideation for novel strategies
- **Incremental data pipeline** — SQLite with WAL mode, resumable downloads, 90-day rolling archive
- **Rigorous statistics** — `proportions_ztest` for binary outcomes, clustered SEs by expiration date, Harvey-Liu-Zhu threshold (t > 3.0), Bonferroni correction
- **AI-powered workflow (optional)** — Claude Code CLI (no API key needed), Anthropic, OpenAI, or OpenRouter for hypothesis formalization, result interpretation, and strategy ideation
- **Web UI** — React + FastAPI dashboard for data source management, backtesting, and strategy generation
- **No auto-trading** — Research and alerting only

---

## Architecture

```
edge-catcher/
├── edge_catcher/
│   ├── adapters/         # Market data collectors
│   │   ├── base.py       # AdapterMeta + PredictionMarketAdapter ABC (Kalshi-shaped)
│   │   ├── kalshi/       # Kalshi API adapter (markets + trades)
│   │   └── coinbase/     # Coinbase OHLC adapter (any product)
│   ├── hypotheses/       # Statistical hypothesis modules
│   │   ├── examples/     # Example hypothesis template
│   │   ├── kalshi/       # Kalshi-specific hypotheses
│   │   └── registry.py   # Auto-discovery + multi-comparison correction
│   ├── ai/               # LLM integration
│   │   ├── client.py     # Provider-agnostic client (Anthropic, OpenAI, OpenRouter, Claude Code CLI)
│   │   ├── formalizer.py # English → hypothesis config
│   │   ├── interpreter.py# Analysis JSON → English summary
│   │   ├── strategizer.py# Hypothesis → strategy code generation
│   │   └── prompts/      # Editable system prompts
│   ├── research/         # Autonomous research agent
│   │   ├── agent.py      # Hypothesis runner + adjacent generation
│   │   ├── loop.py       # Grid + LLM phase orchestrator
│   │   ├── llm_ideator.py# LLM-driven hypothesis proposals
│   │   ├── grid_planner.py# Exhaustive strategy×series grid
│   │   ├── evaluator.py  # Promote/explore/kill verdict engine
│   │   ├── tracker.py    # SQLite result persistence
│   │   ├── run_queue.py  # Parallel execution with audit logging
│   │   ├── audit.py      # Integrity checks + decision log
│   │   └── reporter.py   # Research report generation
│   ├── runner/           # Backtest engine
│   │   ├── backtest.py   # Core backtester
│   │   ├── event_backtest.py # Event-driven backtest
│   │   ├── strategies.py # Public strategy framework
│   │   ├── strategy_parser.py # Strategy discovery + validation
│   │   └── strategies_local.py.example # Template for private strategies
│   ├── storage/          # SQLite persistence layer
│   │   ├── db.py         # Connection management, WAL, OHLC tables
│   │   ├── models.py     # Market, Trade dataclasses
│   │   └── archiver.py   # 90-day rolling archive
│   ├── reports/          # Report formatting
│   ├── reporting/        # P&L reporting CLI (python -m edge_catcher.reporting)
│   ├── data/examples/    # Bundled fixtures: demo_markets.db + paper_trades_demo.db
│   └── fees.py           # Config-driven fee models
├── api/                  # FastAPI backend
│   ├── main.py           # REST endpoints
│   ├── adapter_registry.py # All data source definitions
│   ├── tasks.py          # Background download state
│   └── models.py         # API schemas
├── ui/                   # React + Vite frontend
├── config/
│   ├── fees.yaml               # Fee models per market
│   ├── hypotheses.yaml         # Hypothesis configs
│   ├── markets-btc.yaml        # Default market series (Kalshi BTC)
│   └── markets-*.yaml          # Category-specific series
└── tests/                # pytest suite (255+ tests)
```

---

## Quickstart

**Requirements:** Python 3.11+

```bash
# Clone and install
git clone https://github.com/goldenwo/edge-catcher.git
cd edge-catcher
pip install -e ".[dev]"

# Try the bundled example with no data download required
edge-catcher backtest \
    --series DEMO_SERIES \
    --db-path edge_catcher/data/examples/demo_markets.db \
    --strategy longshot_fade_example --json

# Generate a P&L report against the bundled paper-trades fixture
python -m edge_catcher.reporting --db edge_catcher/data/examples/paper_trades_demo.db

# Download real market data (default series)
python -m edge_catcher download

# Download OHLC data (no API key needed)
python -m edge_catcher download-btc

# Run a backtest on downloaded data
python -m edge_catcher backtest --series SERIES_A --strategy example --json

# Run all registered hypotheses
python -m edge_catcher analyze
```

For a longer walkthrough see [`docs/quickstart.md`](docs/quickstart.md).

---

## CLI Reference

```bash
# Data download
python -m edge_catcher download                      # Default market contracts
python -m edge_catcher download --markets config/markets-altcrypto.yaml  # Category-specific
python -m edge_catcher download-btc                  # OHLC candles (primary asset)
python -m edge_catcher download-altcoin-ohlc         # OHLC candles (additional assets)

# Backtesting
python -m edge_catcher backtest --series SERIES_A --strategy example --json
python -m edge_catcher backtest --list-strategies    # Show available strategies
python -m edge_catcher backtest --list-series        # Show available series
python -m edge_catcher list-dbs                      # Scan all databases

# Research loop
python -m edge_catcher research loop --grid-only --max-runs 50 --parallel 4
python -m edge_catcher research loop --max-runs 200 --parallel 4 --max-llm-calls 5
python -m edge_catcher research status               # Show progress
python -m edge_catcher research audit decisions       # Review LLM decisions

# Analysis
python -m edge_catcher analyze
python -m edge_catcher archive                       # Archive old trades

# AI tools (optional)
python -m edge_catcher formalize "your hypothesis in plain English"
python -m edge_catcher interpret
```

---

## Autonomous Research Loop

The research loop automates hypothesis generation and testing across all available data:

```
Grid Phase ──→ LLM Ideation ──→ Strategy Generation ──→ Backtest ──→ Evaluate
   (exhaustive)     (opus)          (sonnet)            (local)     (promote/explore/kill)
       │                                                                    │
       └────────────────────────── feedback ────────────────────────────────┘
```

**Grid phase:** Tests every strategy×series combination. No LLM calls, fast.

**LLM phase:** Analyzes results, proposes new hypotheses and novel strategies. Uses Claude Code CLI by default (no API key required if `claude` is on PATH).

```bash
# Warm up the grid first (need 10+ results before LLM activates)
python -m edge_catcher research loop --grid-only --max-runs 50 --parallel 4

# Full loop with LLM ideation
python -m edge_catcher research loop --max-runs 200 --parallel 4 --max-llm-calls 5

# Overnight unattended
export EDGE_CATCHER_CC_BUDGET_USD=1  # cap per-call spend
while true; do
    python -m edge_catcher research loop --max-runs 100 --parallel 4 --max-llm-calls 5
    EXIT=$?
    if [ $EXIT -ne 2 ]; then break; fi
    echo "Budget exhausted, continuing..."
done
```

Exit code 2 = more work to do. Exit code 0 = grid exhausted. The loop auto-deduplicates and resumes.

---

## Data Sources

### Kalshi Adapters

Kalshi adapters download settled contracts and trade history. No API key required for settled data.

| Adapter | Config | Database |
|---------|--------|----------|
| Default | `config/markets-btc.yaml` | `data/kalshi-btc.db` |
| Per-category | `config/markets-*.yaml` | `data/kalshi-*.db` |

Each category adapter reads its market series from a YAML config in `config/`. Database filenames are auto-derived from the config filename.

### Coinbase OHLC Adapters

Coinbase adapters download 1-minute OHLC candles. No API key required (public endpoints).

OHLC adapters download 1-minute candles for supported assets. Each writes to a table in `data/ohlc.db` (or `data/btc.db` for the primary asset). All adapters share a global rate limiter.

---

## Configuration

### Markets (`config/markets-btc.yaml`)

```yaml
adapters:
  kalshi:
    enabled: true
    series:
      - SERIES_A   # example series
      - SERIES_B   # example series
    statuses:
      - settled
    min_available_ram_pct: 10  # RAM guard for low-memory machines
```

### Fees (`config/fees.yaml`)

```yaml
kalshi:
  maker:
    formula: "0.0175 * P * (1 - P)"
  taker:
    formula: "0.07 * P * (1 - P)"
```

---

## AI Integration

AI features are optional. The core pipeline works without any API key.

### Provider Priority

1. **Claude Code CLI** — auto-detected if `claude` is on PATH (no API key needed)
2. **Anthropic** — `ANTHROPIC_API_KEY`
3. **OpenAI** — `OPENAI_API_KEY`
4. **OpenRouter** — `OPENROUTER_API_KEY`

Override with `--provider` or `EDGE_CATCHER_LLM_PROVIDER` env var.

### Task Models (Claude Code CLI defaults)

| Task | Model | Effort | Purpose |
|------|-------|--------|---------|
| Ideator | opus | high | Analyze results, propose new hypotheses |
| Strategizer | sonnet | high | Generate strategy code from proposals |
| Formalizer | sonnet | default | Plain English → structured hypothesis |
| Interpreter | haiku | default | Summarize results |

Per-call budget cap: `EDGE_CATCHER_CC_BUDGET_USD=1`

---

## Web UI

```bash
# Install UI dependencies
pip install -e ".[ui]"
cd ui && npm install

# Run backend + frontend
uvicorn api.main:app --reload &
npm run dev
```

The UI provides:
- Data source management (download, API key config, status monitoring)
- Strategy backtesting with real-time progress
- AI-powered hypothesis formalization and interpretation
- Model/provider configuration

---

## Writing a Strategy

Strategies live in `edge_catcher/runner/strategies.py` (public) or `edge_catcher/runner/strategies_local.py` (private, gitignored). A worked tutorial example, `longshot_fade_example`, ships in `edge_catcher/runner/strategies_example.py` — copy it to `strategies_local.py` and edit:

```python
from edge_catcher.runner.strategies import Strategy, Signal

class MyStrategy(Strategy):
    name = "my_strategy"

    def on_trade(self, trade, market, context) -> list[Signal]:
        # Return zero or more Signal objects (action='buy' or 'sell')
        ...
```

Strategies are auto-discovered by the backtester and research loop. See [`docs/strategy-guide.md`](docs/strategy-guide.md) for the full walkthrough.

---

## Statistical Methodology

### Why t > 3.0 instead of 1.96?

The Harvey-Liu-Zhu (2016) threshold corrects for multiple comparison bias in financial research. At t = 1.96, ~5% of random noise passes significance. Requiring t > 3.0 drops the false discovery rate to <0.3%.

### Why clustered standard errors?

Contracts expiring on the same date share a common shock. Treating them as independent inflates the effective sample size. We cluster by expiration date and use the within-cluster majority outcome as the unit of observation.

### Verdict logic

```
INSUFFICIENT_DATA    → n < 30 per bucket or < 80 independent observations
NO_EDGE              → no bucket clears t > 3.0
INCONCLUSIVE         → mixed signal (some buckets significant, some not)
EDGE_EXISTS          → signal clears HLZ threshold, edge survives fees
EDGE_NOT_TRADEABLE   → signal is real but fee-adjusted edge ≤ 0
```

---

## Running Tests

```bash
pytest tests/ -v
```

255+ tests run against mocked API responses — no live API key needed.

---

## License

MIT
