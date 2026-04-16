# CLAUDE.md — Project Instructions for AI Agents

## Private File Rules

**NEVER create files in these locations** — they contain private research and are gitignored:

| Pattern | What it covers |
|---------|---------------|
| `scripts/` | All analysis and research scripts |
| `run_*.py` | All sweep/test runner scripts |
| `TASK-*.md` | All task specs with strategy details |
| `tests/test_flow_*.py` | Strategy-specific tests that reveal logic |
| `tests/test_local_*.py` | Any local/private test files |
| `edge_catcher/runner/strategies_local.py` | Private strategy implementations |
| `edge_catcher/hypotheses/local/` | Private hypothesis analysis |
| `edge_catcher/monitors/` | Paper trader implementations |
| `config.local/` | Local config overrides |
| `reports/` | All backtest reports and findings |
| `data/` | All databases |

### Where to put things instead:
- **New strategies** → `edge_catcher/runner/strategies_local.py` (gitignored)
- **New analysis scripts** → `scripts/` (gitignored)
- **Test runner scripts** → `scripts/` (gitignored, prefix with `run_`)
- **Strategy-specific tests** → `tests/test_local_*.py` (gitignored)
- **Task specs** → `TASK-*.md` in repo root (gitignored)
- **Framework tests** (non-strategy-specific) → `tests/test_*.py` (tracked, OK)

### Rule of thumb:
If a file reveals **what** we trade, **how** we detect edges, or **specific thresholds/parameters**, it is PRIVATE and goes in a gitignored location. Framework/infrastructure code that doesn't reveal strategy logic is fine to track.

## Project Structure

- `edge_catcher/` — core Python package
  - `runner/` — backtester engine + strategies framework
  - `monitors/` — paper trader engine, dispatch, capture/replay pipeline
    - `capture/` — daily bundle assembly (`bundle.py`), R2 transport (`transport.py`), raw frame writer (`writer.py`)
    - `replay/` — replay backtester (`backtester.py`), JSONL loader (`loader.py`)
  - `research/` — automated hypothesis testing agent
  - `adapters/` — market data adapters (Kalshi, Coinbase)
  - `storage/` — database layer
  - `ai/` — LLM integration for hypothesis formalization
- `api/` — FastAPI backend for UI
- `ui/` — React + Vite frontend
- `config/` — market configs per category (tracked)
- `tests/` — pytest suite (framework tests only)

## Backtesters

Two backtester engines exist for different stages of the research pipeline:

**Event backtester** (`runner/event_backtest.py`) — fast hypothesis discovery. Replays historical trade events from the DB against strategy logic. Used for quick sweeps over parameter space. Does NOT reproduce live execution fidelity (no orderbook state, no synthetic settlement, no dispatch plumbing). Use for "does this signal have directional edge?" questions.

**Replay backtester** (`monitors/replay/backtester.py`) — execution fidelity verdict. Replays a captured daily bundle (JSONL + strategies + config + state snapshots) through the exact same `dispatch_message` path the live paper trader uses. Seeds MarketState, open trades, and strategy state from the prior day's bundle. Use for "does replay produce the same trades as live?" questions. Entry point: `replay_capture(bundle_path)`.

**Four-stage filter** for strategy validation:
1. **Hypothesis** → does the signal exist? (research agent)
2. **Event backtest** → does it have directional edge? (fast, approximate)
3. **Replay backtest** → does live execution match? (slow, high-fidelity)
4. **Live paper trader** → P&L source of truth

**Daily bundle pipeline:** the paper trader captures all events to a JSONL file via `RawFrameWriter`. At midnight UTC, `rotation_callback` assembles a daily bundle (compressed JSONL + strategies_local.py + config + market_state + open_trades + strategy_state + day slice), uploads to Cloudflare R2, and deletes the raw file. Bundles are self-contained — replay can run against any bundle without needing the live DB or engine.

## Coding Standards
- Python 3.10+
- Tabs, 4-space width
- Type hints on all function signatures
- Tests for all new framework code
