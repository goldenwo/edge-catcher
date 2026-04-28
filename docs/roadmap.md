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

## v1.2.0 — Released 2026-04-27

- ✅ **Rich multi-section daily P&L formatter** — `report_to_notification` now produces a 4-section body (Yesterday breakdown by strategy/series, All-time per-strategy summary, Portfolio stats, Open positions) information-equivalent to an LLM-formatted summary. `generate_report` adds `open_positions` + `all_time_by_strategy` fields (additive; v1.1.x consumers unaffected). Open-positions section caps at 30 rows with `…(N more)` overflow marker so the body stays under Discord's 4096-char embed-description limit. See [docs/upgrade-1.2.md](upgrade-1.2.md) for retiring an LLM-formatter daily P&L cron pattern.

## v1.3 candidates — UI parity for v1.1.0 + v1.2.0 features

The UI (React + Vite + FastAPI under `ui/` and `api/`) currently surfaces only research / backtest / hypothesis flows. None of the v1.1.0 / v1.2.0 operational surface (notifications layer, reporting CLI flags, rich P&L formatter, paper-trader state) has a UI today. Adding it is the natural next cycle — public users get a one-stop interface that matches what's now available on the CLI side.

Recommended order (highest leverage first; can be re-shuffled):

1. **Reports page** — UI surface for `python -m edge_catcher.reporting`. Pick a date, pick a DB, click Generate; see the rich-formatted body inline plus the raw JSON expandable. New API endpoint `GET /api/reporting/run?db=...&date=...` returning the full `generate_report` dict. ~1 session.
2. **Notifications config editor** — CRUD on `config.local/notifications.yaml` with form-based UI per channel type (stdout / file / webhook / smtp), schema validation via the loader at write time, "Test Channel" button that dispatches a sample notification and shows the per-channel `DeliveryResult` (success/error/latency_ms). New API endpoints `GET/PUT /api/notifications/config` + `POST /api/notifications/test`. ~1-2 sessions; needs careful secret handling (env-var references stay as `${VAR}`, never expanded into the response).
3. **"Send to channel" button on Reports page** — reuses the notifications-config endpoints; one click sends the currently-displayed report to a chosen channel. Trivial once #1 + #2 land.
4. **Paper-trader live monitor** — read-only view of the running paper trader's state (active strategies, open positions, recent fills, today's P&L). Requires a state-export endpoint on the paper trader (or shared SQLite read against `paper_trades.db`). ~1-2 sessions; the harder design question is "what do non-Pi users see if no paper trader is running locally?" — likely a "Not running" empty state + a link to docs.
5. **Settings → Notifications subsection** — extend the existing `Settings.tsx` page (currently AI-provider-only) to include the notifications config editor as a sub-section, instead of a standalone page. Cosmetic but better discoverability.

Out of scope for the v1.3 cycle (intentional):
- "Deploy to Pi" / one-click cron migration. Per-user infrastructure is too varied to template; docs/upgrade-1.x.md guides remain the canonical path.

## v1.4 candidates — Desktop-app UX (formerly v1.1 candidate #7)

Promoted from "v1.1 candidate" to a dedicated v1.4 cycle since v1.3 will already pull substantial UI work. Goal: install once, launch once, the UI opens automatically — no separate `uvicorn` + `npm run dev` dance for end users. Three plausible shapes, ordered by effort:

- `edge-catcher launch` CLI command — starts FastAPI + opens default browser to `localhost`. Optional `pystray` tray icon. Lowest friction; works for technical and casual users alike. ~1 session.
- **Pywebview wrapper** — Python opens a native OS window pointing at the embedded FastAPI. No browser chrome. Feels like a real lightweight desktop app. ~2 sessions.
- **Tauri shell + Python sidecar** — proper installable binary with native menus and signed builds. The React+Vite UI is already Tauri-friendly. Right answer when shipping to non-Python users matters. ~3-4 sessions.

Pick the shape that matches actual user demand. The CLI launcher (option 1) is the no-regret first step and unlocks the other two later.

## v1.1 candidates (still relevant — recommended order)

Bigger pieces that need design + implementation time. Numbered by recommended-execution order (highest leverage first; can be re-shuffled to fit your priorities):

1. **mypy / pyright type-check CI** — currently ruff-only; type checking would catch a meaningful class of bugs (would have caught several issues found via review-loops on the v1.1.0 / v1.2.0 work). Small scope (~1-2 sessions), no runtime behavior change. Land before subsequent feature work so new PRs have the type checker as a CI gate.
2. **Polymarket adapter** — concretely exercises the dispatch registry refactor with a second prediction-market exchange. Architectural validation: surfaces hidden coupling NOW with one new exchange, not later with five. ~1-2 sessions.
3. **Public reporting-module delegation pattern** — small doc + example showing how to wire `python -m edge_catcher.reporting --notify` into a daily cron + delivery channel. ~1-2 hours; complements [docs/upgrade-1.1.md](upgrade-1.1.md) and [docs/upgrade-1.2.md](upgrade-1.2.md).
4. **Tutorial walkthrough docs** — "Build your first strategy end-to-end" covering: write a `Strategy`, run a backtest, examine results, iterate. Goes deeper than the quickstart. Right after Polymarket validates the architecture so the docs reflect the validated API.
5. **Multi-provider LLM onboarding deep-dive** — current README has a short section; full guide would cover provider trade-offs, model-selection per task, cost management, claude-code CLI vs API.
6. **Docker / cloud deployment guide** — `docker-compose.yml` for local dev, plus a "deploy to a Linux VPS" walkthrough. Substantive; touches infrastructure assumptions.

## v1.5+ candidates

- **Unified-layer async + middleware → migrate `monitors/notifications.py`** — adds `AsyncChannel` protocol, rate-limiting + bounded-concurrency middleware to `edge_catcher.notifications`. Enables migrating the paper-trader-internal Discord client (`edge_catcher/monitors/notifications.py`, deprecated in v1.1) onto the unified config story. Public users get one notification surface (YAML-config + multi-adapter for the reporting CLI AND the paper trader) instead of two. Was tagged as v1.2 candidate but has been pushed back as v1.3+v1.4 take priority.

## Beyond

Aspirational, no schedule:

- **More exchange adapters** — Manifold, others — likely community-contributed once the registry pattern is proven
- **More hypothesis examples** — beyond the `longshot_bias_tutorial`. Educational hypotheses for common patterns (mean reversion, expiration-week effects, etc.).
- **Strategy benchmarking suite** — standardized backtest setups so different strategies can be compared apples-to-apples.

## How to contribute or suggest changes

Open an issue or PR. See [CONTRIBUTING.md](../CONTRIBUTING.md).
