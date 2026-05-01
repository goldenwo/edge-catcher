# Contributing to edge-catcher

Thanks for your interest in improving edge-catcher. This guide covers the
basics of working in the codebase: layout, what is public vs private, how
to run tests and the linter, how to extend the framework, and the PR
workflow.

## Repo layout

For the high-level architecture diagram see [README.md](README.md#architecture).
The short version:

- `edge_catcher/` — the importable framework (adapters, runner, research,
  monitors, reporting, AI client, storage)
- `api/` — FastAPI backend that wraps the framework for the UI
- `ui/` — React + Vite frontend
- `config/` — example configs (markets, fees, hypotheses)
- `tests/` — framework-level pytest suite
- `docs/` — public docs (this guide, quickstart, architecture, ADRs)

## Public-vs-private split

edge-catcher's design separates **framework** (public, tracked) from
**user research** (private, gitignored). Anything that reveals strategy
edge — parameters, entry rules, ticker whitelists, alert thresholds — is
expected to live outside the public tree.

| Path | Status | Notes |
|------|--------|-------|
| `edge_catcher/runner/strategies.py` | tracked | base class + reusable mixins |
| `edge_catcher/runner/strategies_example.py` | tracked | tutorial strategy |
| `edge_catcher/runner/strategies_local.py` | tracked | example local file (deliberately empty/safe) |
| `edge_catcher/monitors/strategies_local.py` | **gitignored** | your paper-trader strategies |
| `config/` | tracked | example configs |
| `config.local/` | **gitignored** | your hypotheses, alert configs, secrets |
| `edge_catcher/hypotheses/local/` | **gitignored** | your private hypothesis modules |
| `scripts/` | **gitignored** | analysis and sweep scripts |
| `reports/` | **gitignored** | backtest output and notes |
| `docs/superpowers/` | **gitignored** | personal design docs / drafts |
| Auto-memory under `~/.claude/...` | not in repo | per-machine notes |

When you contribute to the framework, you only ever touch tracked paths.
If you need to demo a strategy in a test, prefer the bundled
`longshot_fade_example` over inventing a private one.

## Running tests

```bash
pip install -e ".[dev]"
pytest tests/ -v
```

The suite is fully mocked — no live API key or network access needed.
It runs ~900+ tests in well under a minute on a modern laptop.

If you add framework code, add tests. If you write strategy-specific or
research-specific tests, keep them in a gitignored location (e.g.
`tests/test_local_*.py`) so they do not leak edge into the public tree.

## Running the linter

```bash
ruff check .
```

Style is configured in [`ruff.toml`](ruff.toml): pycodestyle errors,
pyflakes, warnings (no full default rule set), with tab-indentation
exceptions because the codebase uses tabs.

## Type-checking

```bash
mypy edge_catcher api
```

The CI gate is **zero-tolerance** as of v1.3.0 — any new type error
fails the build. mypy config lives in `pyproject.toml` under
`[tool.mypy]`. A handful of legacy modules use targeted
`[[tool.mypy.overrides]]` entries when an upstream library's union
types vary across versions; prefer fixing types at the source over
adding new overrides.

If you cannot avoid an override, narrow it to the smallest possible
scope (one module, specific error code) and leave a comment explaining
the upstream constraint. Stale overrides eventually rot — adding a TODO
that names the version you'd like to upgrade to (e.g. `cryptography>=46`)
keeps cleanup possible later.

## How to add an exchange

See [`docs/adapter-guide.md`](docs/adapter-guide.md) for the full
walkthrough. Short version: drop a new directory under
`edge_catcher/adapters/<exchange>/` with `adapter.py`, `registry.py`,
optional `fees.py`; concat the registry list in
`api/adapter_registry.py`; register your download + data-check handlers
in `api/dispatchers.py`. Background and rationale in
[`docs/adr/0001-adapter-registry.md`](docs/adr/0001-adapter-registry.md).

## How to add a strategy

See [`docs/strategy-guide.md`](docs/strategy-guide.md). Most users start
with the backtester `Strategy` interface — copy
`edge_catcher/runner/strategies_example.py` to `strategies_local.py`,
rename, edit, and the auto-discovery picks it up.

The paper-trader uses a separate `PaperStrategy` base class with an
`on_tick` callback; it is more involved and most contributors will not
need to touch it.

## How to add a hypothesis

The tracked `config/hypotheses.yaml` holds the example hypothesis
templates. To add your own without leaking it into the public repo:

1. Create `config.local/hypotheses.yaml` (gitignored).
2. Copy a tracked example as a starting point.
3. Edit thresholds, buckets, and target series for your hypothesis.
4. Run `edge-catcher analyze` and the loader will merge `config.local/`
   over `config/`.

Private hypothesis Python modules go in
`edge_catcher/hypotheses/local/` (also gitignored), discovered by the
hypothesis registry the same way as the tracked ones.

## Pull request process

- Target `main`. Branch off `main`.
- Keep PRs focused. One concept per PR makes review tractable.
- Include tests for any new framework code. Strategy-specific tests stay
  in your gitignored fork.
- Update relevant docs (`README.md`, `docs/*.md`, ADRs) when behavior or
  surface area changes.
- Run `ruff check .`, `mypy edge_catcher api`, and `pytest tests/`
  locally before opening the PR. CI runs the same three checks against
  Python 3.11 and 3.12 — all three are required for merge.
- Do not commit anything from `data/`, `reports/`, `config.local/`,
  `scripts/`, or `edge_catcher/monitors/strategies_local.py` — these are
  gitignored for a reason.

## Code style

- Python 3.11+
- **Tabs** for indentation, 4-space visual width (per
  [`CLAUDE.md`](CLAUDE.md))
- Line length 120 (configured in `ruff.toml`)
- Type hints on all function signatures
- Prefer composition + explicit imports; no wildcard `import *`
- Docstrings on public APIs; concise `# comments` only when intent is
  not obvious from the code
