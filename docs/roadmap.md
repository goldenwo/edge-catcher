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

All four v1.0.x items shipped between v1.0.0 and the next tag — see the [CHANGELOG.md](../CHANGELOG.md) `[1.0.1]` section. New small polish items can be added back here as they come up.

## v1.1.0 — Released 2026-04-27

- ✅ **`edge_catcher/notifications/` abstraction** — pluggable delivery adapters: stdout, file (JSONL), webhook (discord/slack/generic styles), SMTP. YAML-configured at `config.local/notifications.yaml` with env-var interpolation and version handshake. Reporting CLI gains `--notify`, `--notify-config`, `--quiet` flags. Backward-compatible: invocations without `--notify` keep the v1.0.x JSON-to-stdout behavior byte-for-byte (locked by a golden-file test). See [docs/upgrade-1.1.md](upgrade-1.1.md) for Pi redeploy + cron-wiring instructions.

## v1.1 candidates (recommended order)

Bigger pieces that need design + implementation time. Numbered by recommended-execution order (highest leverage first; can be re-shuffled to fit your priorities):

1. **mypy / pyright type-check CI** — currently ruff-only; type checking would catch a meaningful class of bugs (would have caught several issues found via review-loops on the v1.1.0 work). Small scope (~1-2 sessions), no runtime behavior change. Land before subsequent feature work so new PRs have the type checker as a CI gate.
2. **Polymarket adapter** — concretely exercises the dispatch registry refactor with a second prediction-market exchange. Architectural validation: surfaces hidden coupling NOW with one new exchange, not later with five. ~1-2 sessions.
3. **Public reporting-module delegation pattern** — small doc + example showing how to wire `python -m edge_catcher.reporting --notify` into a daily cron + delivery channel. ~1-2 hours; complements [docs/upgrade-1.1.md](upgrade-1.1.md).
4. **Tutorial walkthrough docs** — "Build your first strategy end-to-end" covering: write a `Strategy`, run a backtest, examine results, iterate. Goes deeper than the quickstart. Right after Polymarket validates the architecture so the docs reflect the validated API.
5. **Multi-provider LLM onboarding deep-dive** — current README has a short section; full guide would cover provider trade-offs, model-selection per task, cost management, claude-code CLI vs API.
6. **Docker / cloud deployment guide** — `docker-compose.yml` for local dev, plus a "deploy to a Linux VPS" walkthrough. Substantive; touches infrastructure assumptions.
7. **Desktop-app UX** — install once, launch, and the UI opens automatically — no separate `uvicorn` + `npm run dev` dance. Largest commitment in the v1.1 list; defer unless a specific user is pulling for it. Three plausible shapes, ordered by effort:
  - `edge-catcher launch` CLI command — starts FastAPI + opens default browser to `localhost`. Optional `pystray` tray icon. Lowest friction; works for technical and casual users alike.
  - **Pywebview wrapper** — Python opens a native OS window pointing at the embedded FastAPI. No browser chrome. Feels like a real lightweight desktop app.
  - **Tauri shell + Python sidecar** — proper installable binary with native menus and signed builds. The React+Vite UI is already Tauri-friendly. Right answer when shipping to non-Python users matters.

## v1.2 candidates

- **Unified-layer async + middleware → migrate `monitors/notifications.py`** — adds `AsyncChannel` protocol, rate-limiting + bounded-concurrency middleware to `edge_catcher.notifications`. Enables migrating the paper-trader-internal Discord client (`edge_catcher/monitors/notifications.py`, deprecated in v1.1) onto the unified config story. Public users get one notification surface (YAML-config + multi-adapter for the reporting CLI AND the paper trader) instead of two. Spec §11 of the v1.1 notifications design lists this as deferred; the v1.2 cycle should pick it up.

## Beyond v1.1

Aspirational, no schedule:

- **More exchange adapters** — Manifold, others — likely community-contributed once the registry pattern is proven
- **More hypothesis examples** — beyond the `longshot_bias_tutorial`. Educational hypotheses for common patterns (mean reversion, expiration-week effects, etc.).
- **Strategy benchmarking suite** — standardized backtest setups so different strategies can be compared apples-to-apples.

## How to contribute or suggest changes

Open an issue or PR. See [CONTRIBUTING.md](../CONTRIBUTING.md).
