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

Small, low-risk improvements that didn't make the v1 cut:

- **Demo fixture with `fill_size > 1`** — makes the reporting regression test strict (the "deployed = entry_price × fill_size" assertion currently can't fully distinguish from the buggy `SUM(entry_price)` formula because all fixture rows have `fill_size = 1`).
- **GitHub issue + PR templates** — `.github/ISSUE_TEMPLATE/` + `.github/pull_request_template.md`.
- **`CODE_OF_CONDUCT.md`** — standard contributor agreement.
- **Resource limits in `deploy/paper-trader.service`** — match `edge-catcher-download.service`'s `MemoryMax` / `CPUQuota` / `Nice` settings, or add a comment explaining intentional omission.

## v1.1 candidates (when motivated)

Bigger pieces that need design + implementation time:

- **`edge_catcher/notifications/` abstraction** — pluggable delivery adapters: stdout (default), webhook, file-write, SMTP email. Wires the reporting module to "P&L in Discord/Slack/email" without users reinventing the integration.
- **Tutorial walkthrough docs** — "Build your first strategy end-to-end" covering: write a `Strategy`, run a backtest, examine results, iterate. Goes deeper than the quickstart.
- **Multi-provider LLM onboarding deep-dive** — current README has a short section; full guide would cover provider trade-offs, model-selection per task, cost management, claude-code CLI vs API.
- **Polymarket adapter** — concretely exercises the dispatch registry refactor with a second prediction-market exchange.
- **Docker / cloud deployment guide** — `docker-compose.yml` for local dev, plus a "deploy to a Linux VPS" walkthrough.
- **mypy / pyright type-check CI** — currently ruff-only; type checking would catch a meaningful class of bugs.
- **Public reporting-module delegation pattern** — example showing how to wire `python -m edge_catcher.reporting` into a daily cron + your delivery channel of choice.

## Beyond v1.1

Aspirational, no schedule:

- **More exchange adapters** — Manifold, others — likely community-contributed once the registry pattern is proven
- **More hypothesis examples** — beyond the `longshot_bias_tutorial`. Educational hypotheses for common patterns (mean reversion, expiration-week effects, etc.).
- **Strategy benchmarking suite** — standardized backtest setups so different strategies can be compared apples-to-apples.

## How to contribute or suggest changes

Open an issue or PR. See [CONTRIBUTING.md](../CONTRIBUTING.md).
