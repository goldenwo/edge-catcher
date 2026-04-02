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
  - `research/` — automated hypothesis testing agent
  - `adapters/` — market data adapters (Kalshi, Coinbase)
  - `storage/` — database layer
  - `ai/` — LLM integration for hypothesis formalization
- `api/` — FastAPI backend for UI
- `ui/` — React + Vite frontend
- `config/` — market configs per category (tracked)
- `tests/` — pytest suite (framework tests only)

## Coding Standards
- Python 3.10+
- Tabs, 4-space width
- Type hints on all function signatures
- Tests for all new framework code
