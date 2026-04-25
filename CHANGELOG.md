# Changelog

All notable changes to edge-catcher are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] — 2026-04-22

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

[1.0.0]: https://github.com/goldenwo/edge-catcher/releases/tag/v1.0.0
