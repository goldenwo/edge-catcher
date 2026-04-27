# Changelog

All notable changes to edge-catcher are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **`edge_catcher.notifications`** — pluggable notification delivery layer with `stdout`, `file`, `webhook` (discord/slack/generic styles), and `smtp` adapters. YAML-configured at `config.local/notifications.yaml` with env-var interpolation and a forward-compat `version:` field. Used by the reporting CLI's new `--notify`, `--notify-config`, `--quiet` flags.
- **`config/notifications.example.yaml`** — sanitized template with all four adapter types and discord/slack webhook examples.
- **Reporting CLI** — `--notify <name>` (repeatable), `--notify-config <path>`, `--quiet`. Backward-compatible: invocations without `--notify` keep the existing JSON-to-stdout behavior.

### Notes

- `edge_catcher/monitors/notifications.py` (paper-trader-internal Discord client) is intentionally **not** deprecated by the new layer. The two solve different problems: the new layer is sync + config-driven for the reporting CLI; the monitors helper is async + rate-limited + bounded-concurrency for the trading loop. Migrating one onto the other is not planned (would require adding async + rate-limiting to the user-facing surface, explicitly out of scope per the v1.1 design).

## [1.0.1] — 2026-04-26

### Added

- **`CODE_OF_CONDUCT.md`** — adopts Contributor Covenant 2.1 by reference; reporting goes through GitHub private security advisories or issues.
- **GitHub issue + PR templates** — `.github/ISSUE_TEMPLATE/{bug_report,feature_request}.md` and `.github/pull_request_template.md` give contributors a structured intake form and a reviewer-friendly PR checklist.
- **Resource limits on `deploy/paper-trader.service`** — `MemoryMax=512M`, `CPUQuota=50%`, `Nice=10`. Sized for a small VPS or 8GB SBC; comment calls out when to raise them.

### Changed

- **Demo fixture has a `fill_size > 1` row.** Row `A-01` now ships with `fill_size=4` so `SUM(entry_price * fill_size)` is arithmetically distinct from the buggy `SUM(entry_price)` formula. The reporting regression test now fails the bug instead of merely guarding the SQL shape. Fixture totals updated: net_pnl 821¢ → 1112¢, deployed 89¢ → 98¢ (counts and fees unchanged).
- **`CONTRIBUTING.md`** — drops the stale `public-release-v1` branch reference (merged into `main`); test count refreshed to "~900+" to match the public clone reality.
- **`README.md`** — leaner structure (badges, four-stage flow diagram, doc table). Test count corrected to match public-clone reality.
- **`docs/roadmap.md`** — desktop-app UX added as a v1.1 candidate; v1.0.x polish section trimmed as items shipped.

## [1.0.0] — 2026-04-25

Initial public release. The framework, adapter registry, research agent, paper trader, capture/replay pipeline, and UI are all open source under MIT.

### Added

- **Event-driven backtester** with fee-adjusted P&L, Sharpe ratio, win rate, and per-strategy breakdowns
- **Paper trader** with orderbook-aware sizing, capture/replay parity, and per-tick state
- **Autonomous research loop** — grid sweep across strategy×series combinations + LLM-driven hypothesis ideation
- **Adapters:** Kalshi (multi-category: BTC, altcrypto, sports, politics, financials, esports, weather, entertainment) + Coinbase OHLC
- **Adapter registry pattern** — `AdapterMeta` for plug-in exchange support
- **Exchange dispatch registry** (`api/dispatchers.py`) — eliminates if/elif chains for adding new exchanges
- **Capture/replay pipeline** — daily JSONL bundles + state snapshots, R2 transport with local fallback, bit-exact reproducibility between live paper trader and replay backtest
- **Reporting module** (`edge_catcher.reporting`) — P&L summary with corrected math (`deployed = entry_price × fill_size`), CLI via `python -m edge_catcher.reporting`
- **Provider-agnostic LLM client** — Anthropic, OpenAI, OpenRouter, claude-code CLI
- **Bundled fixtures** — `edge_catcher/data/examples/demo_markets.db` + `paper_trades_demo.db` for the quickstart
- **Toy example strategy** — `longshot_fade_example` mirroring the longshot bias tutorial pattern
- **React + Vite UI** — Dashboard, DataSources, Analyze, Strategize, Research, Hypotheses, Backtest, Settings pages
- **Documentation** — README, CONTRIBUTING, quickstart, adapter-guide, strategy-guide, architecture, reporting, roadmap
- **CI** — pytest + ruff on Python 3.11 and 3.12 (GitHub Actions)
- **MIT License**

### Strategy / hypothesis privacy

The framework ships publicly; the user's specific strategies, hypotheses, and analysis stay private via gitignore. Private zones:

- `edge_catcher/runner/strategies_local.py` (and `monitors/strategies_local.py`)
- `edge_catcher/hypotheses/local/`
- `config.local/`
- `scripts/` (analysis)
- `reports/` (research outputs)

The runtime auto-discovers and merges private modules at load time — users never commit their research.

### Known limitations (deliberately deferred to v1.1+)

- **No notification delivery channels.** The reporting module produces JSON; wire your own Slack/Discord/email integration. See the roadmap for the planned `edge_catcher/notifications/` abstraction.
- **No Docker / cloud deployment tooling.** A systemd service example is in `deploy/`; full deployment guides are v1.1.
- **Documentation is minimum viable.** README + 6 topic guides. Tutorial walkthroughs ("build your first strategy end-to-end") are v1.1.
- **Type-check CI (mypy/pyright) is not enabled** — ruff-only for now.

### Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). PRs welcome on framework code; private strategies stay yours.

### Acknowledgments

Built with the support of Anthropic Claude (Opus + Sonnet + Haiku) for code review, design guidance, and refactoring.

---

[1.0.1]: https://github.com/goldenwo/edge-catcher/releases/tag/v1.0.1
[1.0.0]: https://github.com/goldenwo/edge-catcher/releases/tag/v1.0.0
