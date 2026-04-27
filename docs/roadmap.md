# edge-catcher Roadmap

> This roadmap describes what's likely to land if the project keeps moving. Items are not commitments. PRs and issues welcome on anything below.

## v1.0.0 — Initial public release (current)

**What ships:**
- Event-driven backtester with fee-adjusted P&L, Sharpe ratio, win rate
- Paper trader with orderbook-aware sizing and capture/replay parity
- Autonomous research agent — grid search + LLM-driven hypothesis ideation
- Adapters: Kalshi (multi-category) + Coinbase OHLC
- Bundled fixtures and one toy example strategy (`longshot_fade_example`)
- Reporting module (`edge_catcher.reporting`) with corrected per-contract math
- React + Vite UI for runs, hypotheses, and P&L
- Documentation: README, CONTRIBUTING, quickstart, adapter-guide, strategy-guide, architecture, reporting
- CI on Python 3.11 + 3.12 (pytest + ruff)

**Deliberately deferred (so v1 ships clean):**
- Notification delivery channels (users wire their own)
- Docker / cloud deployment tooling
- Tutorial walkthroughs beyond the quickstart
- Type-check CI (mypy / pyright)
- GitHub issue / PR templates beyond minimal defaults

## Short-term polish (v1.0.x)

All four v1.0.x items shipped between v1.0.0 and the next tag — see the **Unreleased** section of [CHANGELOG.md](../CHANGELOG.md) for the full list. New small polish items can be added back here as they come up.

## v1.1 candidates (when motivated)

Bigger pieces that need design + implementation time:

- **`edge_catcher/notifications/` abstraction** — pluggable delivery adapters: stdout (default), webhook, file-write, SMTP email. Wires the reporting module to "P&L in Discord/Slack/email" without users reinventing the integration.
- **Tutorial walkthrough docs** — "Build your first strategy end-to-end" covering: write a `Strategy`, run a backtest, examine results, iterate. Goes deeper than the quickstart.
- **Multi-provider LLM onboarding deep-dive** — current README has a short section; full guide would cover provider trade-offs, model-selection per task, cost management, claude-code CLI vs API.
- **Polymarket adapter** — concretely exercises the dispatch registry refactor with a second prediction-market exchange.
- **Docker / cloud deployment guide** — `docker-compose.yml` for local dev, plus a "deploy to a Linux VPS" walkthrough.
- **mypy / pyright type-check CI** — currently ruff-only; type checking would catch a meaningful class of bugs.
- **Public reporting-module delegation pattern** — example showing how to wire `python -m edge_catcher.reporting` into a daily cron + your delivery channel of choice.
- **Desktop-app UX** — install once, launch, and the UI opens automatically — no separate `uvicorn` + `npm run dev` dance. Three plausible shapes, ordered by effort:
  - `edge-catcher launch` CLI command — starts FastAPI + opens default browser to `localhost`. Optional `pystray` tray icon. Lowest friction; works for technical and casual users alike.
  - **Pywebview wrapper** — Python opens a native OS window pointing at the embedded FastAPI. No browser chrome. Feels like a real lightweight desktop app.
  - **Tauri shell + Python sidecar** — proper installable binary with native menus and signed builds. The React+Vite UI is already Tauri-friendly. Right answer when shipping to non-Python users matters.

## Beyond v1.1

Aspirational, no schedule:

- **More exchange adapters** — Manifold, others — likely community-contributed once the registry pattern is proven
- **More hypothesis examples** — beyond the `longshot_bias_tutorial`. Educational hypotheses for common patterns (mean reversion, expiration-week effects, etc.).
- **Strategy benchmarking suite** — standardized backtest setups so different strategies can be compared apples-to-apples.

## How to contribute or suggest changes

Open an issue or PR. See [CONTRIBUTING.md](../CONTRIBUTING.md).
