# Changelog

All notable changes to edge-catcher are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

The **live-execution cycle** (internally scoped as v1.6.0, sub-projects A–F) lands here: the paper-only engine grows a real-money order path — `LiveExecutor`, an order state machine with reconciliation, risk gates, and a live-only entry gate — all behind the same `Executor` Protocol the paper trader already used. Plus dual-slippage diagnostics, an opt-in honest paper fill model (Phase 1), and migration-safety + parsing hardening. `executor: live|paper` is the mode of record; paper behavior is byte-unchanged (G-parity 11/11) unless a feature is explicitly opted into. No version tag is cut yet — promoting this to `## [1.6.0] — <date>` is the operator's release step.

### Added

- **Live execution (sub-projects A–F, "v1.6.0" PRs 1–6).** `LiveExecutor` + order builders + `fill_math` + a dispatch pending-branch (#37); an order state machine over a `live_trades` store with reconciliation + ghost-reject handling (#39); risk gates + migration runner + dispatch wiring (#36); live engine wiring + cutover (#40); sizing-wire for live order placement — gate + builder + freshness (#41); additive `Signal`/`Executor`/`OpenPosition` fields preserving the protocol-growth invariant (#35).
- **Live-only spread-aware entry gate (#50)** — rejects entries whose book spread would cross the strategy's protective stop; paper/replay paths unaffected.
- **Dual-slippage diagnostics (#54)** — reporting-only `market_impact_cents` (vs top-of-book) and `limit_slippage_cents` (vs the order's limit), persisted on filled entries; additive `OrderResult` fields + migration `0004`.
- **Honest paper fill simulator — Phase 1 (#57)** — opt-in `paper_fill_model: "fixed"` wraps `PaperExecutor` with a `HonestPaperExecutor` + `FixedSlippageModel` applying a hand-tuned pessimistic per-strategy slippage penalty, narrowing the optimistic executor's over-promise vs live fills. Default `"optimistic"` is byte-unchanged (G-parity 11/11); fail-closed config validation at the boot gate. Phase 2 (`EmpiricalSlippageModel` fit to live data) is the empirical follow-up.
- **Python 3.13 added to the CI test matrix (#48).**

### Changed

- **Async engine refactor ("v1.6.0" PR 1/6, #30)** — `KalshiOrderClient` + `Executor` + dispatch became async so `LiveExecutor` can issue awaited HTTP calls inside `place()`; replay parity preserved (the async dispatch is still driven deterministically by the captured WS stream).
- **Venue-neutral live contract (#47)** — extracted a `LiveVenueClient` Protocol so the live path isn't hard-coupled to Kalshi.
- **Order audit-write off-loaded to the executor (#34)** with a platform-aware test threshold.

### Fixed

- **Kalshi fill/order parsing + replay/reconcile hardening (#43).**
- **Live entry price/stop derivation for real-strategy orders (#42).**
- **Absent-ticker open rows resolved by settlement result + expiration (C2, #45).**
- **Live exit close booked only on a venue-confirmed full fill (#51, #52)** — previously a 0-fill IOC could book a phantom exit at the WS bid.
- **Cutover beacon reports the active executor (#49)** so a real-money boot never mislabels itself.
- **Audit-write exceptions guarded in `_request` (#33)** — order responses preserved on a disk-write failure.
- **Migration safety (#55, #56)** — migration SQL split per statement for crash-window safety; paper `_migrate` narrowed to tolerate only duplicate-column errors instead of swallowing genuine SQL errors.
- **Flaky `test_rotation_callback` threading race (#58)** — poll the mock's `call_args` (assigned last) instead of `call_count` to avoid a background-thread race.

### Security

- **`client_order_id` charset + length validation on `OrderRequest` (#28).**
- **Discord `allowed_mentions` defanged (#29)** to neutralize strategy-name-driven mention injection in notifications.

### Docs

- **Honest-paper config documented (#59)** — `paper_fill_model` / `honest_paper` in the paper-trader config example, architecture, and README.
- **Replay parity gate (#31)** gained a skip-list + non-strict warning banner for legacy OLD_FAIL bundles.

## [1.5.0] — 2026-05-08

This release contains the foundation for the live-execution cycle: sub-project A (order placement primitive in PR #24), an auth-signing hardening (PR #25), and sub-project G (paper migration onto a unified engine in PR #26). Together they relocate kalshi auth out of the paper-trader namespace into a shared adapters location, fix a query-string-signing edge case before live trading depends on it, and stand up the `edge_catcher.engine` package + Executor Protocol that B/C/D/E/F will extend.

### Added

- **Sub-project A — Order placement primitive (PR #24).** Live-trader's order-place + cancel HTTP layer split out from paper-trader scaffolding. Moves `make_auth_headers` from `monitors/auth.py` to `edge_catcher/adapters/kalshi/auth.py` (auth is shared exchange code, not paper-specific) and extends it to sign arbitrary `(method, path)` pairs for POST/DELETE. New `LiveClient` with order placement primitives and a Kalshi-error class hierarchy. Foundation for sub-project D's LiveExecutor.
- **Sub-project G — Paper migration onto unified engine (PR #26).** Extracts a shared `engine/` package from `edge_catcher/monitors/`, defines a sync `Executor.place(req) -> OrderResult` Protocol, and ports the orderbook-walk fill simulation as `PaperExecutor`. Foundation for sub-projects B/C/D/E/F (live execution, order state machine, risk gates, daemon refactor, UI/audit) — all build on the new Executor contract.
- **`edge_catcher.engine` package** — relocated from `edge_catcher.monitors`: `dispatch`, `market_state`, `strategy_base`, `recovery`, `discovery`, `metrics`, `trade_store`, `notifications`, `capture/`, `replay/`, `engine`. The pre-G `monitors/` shape is preserved on disk through a deferred-retirement window (follow-up PR after ≥3 stable Pi days will delete `monitors/{__init__,market_state,strategy_base}.py`).
- **`edge_catcher.engine.executor`** — `Executor` Protocol + `OrderRequest` / `OrderResult` frozen-slotted dataclasses. Protocol-growth invariant documented: additive-only fields, no reordering/removal, new status literals require dispatch-side branch updates.
- **`PaperExecutor`** at `edge_catcher.engine.executors.paper.PaperExecutor` — wraps the existing `resolve_fill` / `walk_book_with_ceiling` / `compute_raw_size` pure functions into the Executor protocol shape. Byte-exact behavior preservation (proven by parity sweep — see Verification).
- **Bundle-compat shims** at `edge_catcher/monitors/{market_state,strategy_base}.py` — minimal re-exports (`Strategy as PaperStrategy` for backward compat) so pre-cutover R2 bundles' `strategies_local.py` files still resolve through the new replay path.
- **New tests:** `tests/test_engine_executor_protocol.py` (6 tests covering OrderRequest immutability, OrderResult shape variants, structural-typing); `tests/test_engine_paper_executor_wrap.py` (3 tests confirming the wrap preserves resolve_fill semantics); `tests/test_engine_dispatch_executor_wiring.py` (5 tests on dispatch's `Executor.place` integration with exact-kwargs assertion); `tests/test_engine_notifications_relocation.py` (3 tests pinning the Discord webhook wire-shape across the relocation); `tests/test_engine_replay_smoke.py` + `tests/fixtures/synthetic_bundle/` (end-to-end smoke through the new replay path with a public-safe synthetic strategy); `test_assemble_bundle_strategies_source_is_engine_not_monitors` (regression test pinning bundle.py's source path to `engine/`, not `monitors/`).

### Changed

- **kalshi auth path-signing hardened (PR #25).** `make_auth_headers` strips the query-string from the path before signing — Kalshi's signature spec covers the URL path, not the query. Pre-fix, any GET with query parameters (e.g. `/markets?status=active`) would produce a signature that included the `?status=active` substring, which the server rejected on its expected canonical-form recompute. Affects future live trading (paper trader didn't hit this codepath).
- **`PaperStrategy` → `Strategy`** (in `edge_catcher.engine.strategy_base`). Old name still importable through the `monitors/strategy_base.py` shim during the deferred-retirement window.
- **Cutover lever** at `cli/paper_trade.py:8` flipped from `from edge_catcher.monitors.engine import run_engine` to `from edge_catcher.engine.engine import run_engine`. systemd `ExecStart=` is unchanged; Pi cutover is `git pull` + scp of `engine/strategies_local.py` + `systemctl restart`.
- **`engine/capture/bundle.py`** — `strategies_src` path now reads from `edge_catcher/engine/strategies_local.py` (was `monitors/`) so post-cutover daily bundles encode the actual code the live engine runs. (Pre-merge spec-review-loop caught this latent path-drift bug; would have triggered a quiet replay-divergence incident 24-48h after Pi cutover.)
- **mypy:** added `'^edge_catcher/engine/strategies_local\.py$'` to exclude list (private-file rules apply equally to the engine-side strategies file).
- **Docs swept:** `README.md`, `CONTRIBUTING.md`, `CLAUDE.md`, `docs/strategy-guide.md`, `docs/tutorial.md`, `docs/architecture.md`, `docs/roadmap.md`, `docs/upgrade-1.2.md`, `.github/pull_request_template.md`, `.dockerignore` — all references to `edge_catcher.monitors.*` / `monitors/strategies_local.py` updated to `engine/`. Historical `CHANGELOG.md` entries (v1.0.0–v1.4.0) intentionally left at their release-time paths.

### Verification

- **Local pytest:** 1295 passed, 4 skipped (residuals are pre-existing in private/gitignored files).
- **mypy + ruff:** clean.
- **CI:** test (3.11) PASS · test (3.12) PASS.
- **Parity sweep CUTOVER GATE:** 11/11 R2 bundles byte-exact between pre-G and post-G replay paths (PASS=11, DIFF=0, OLD_FAIL=0, NEW_FAIL=0).
- **spec-review-loop on plan:** 3 rounds CONVERGED. Round 1 caught the bundle.py source-path bug; round 2 added the regression test; round 3 ten-invariant verification clean.
- **Independent code-review (Opus):** APPROVE-WITH-NOTES; 7 suggestions applied.
- **Independent security-review (Opus):** SAFE-WITH-FIXES; cheap defensive items applied (subprocess timeouts, bundle trust-model docstring); 3 medium findings documented as carry-forwards for sub-project D (LiveExecutor's pre-live hardening backlog).
- **Pi cutover:** `paper-trader.service` restarted at 2026-05-08 12:52:40 EDT. Cutover beacon `engine[G]: paper executor wired, package=edge_catcher.engine` present in `/var/log/edge-catcher/paper-trader.log`. NRestarts=0. Recovery completed (160 tickers / 7 series). Strategy state preserved (debut-fade dedup intact). Daily P&L cron manually fired post-cutover: `pnl_discord OK 246ms`.

### Notes

- **Rollback safety.** `monitors/strategies_local.py` stays untouched on the Pi disk through the deferred-retirement window; the cutover is a copy, never a rename. `git revert` of the merge commit restores the full `monitors/` codepath; the rollback file is then immediately loadable.
- **Sub-project G is foundation only.** B/C/D/E/F are explicitly out of scope for this release. LiveExecutor (D) lands separately and will inherit the security carry-forwards documented in PR #26 (client_order_id validation, bundle trust model, Discord content escaping).
- **Follow-up PR** retires `monitors/{__init__,market_state,strategy_base}.py` after ≥3 stable Pi days on the new engine.

## [1.4.0] — 2026-05-07

### Added

- **Reports page** (`/reports`) — UI surface for `python -m edge_catcher.reporting`. Pick a date, pick a DB from `data/*.db`, see today's P&L hero, today by strategy/series breakdown, open positions, all-time stats (4 KPI cards), all-time by strategy, and the raw JSON expandable. New API endpoints `GET /api/reporting/dbs` (auto-discovered list with row counts + relative mtime labels) and `GET /api/reporting/run?db=&date=` (returns the full `generate_report` dict). New "Operations" nav section between Research and Settings — anchors home for upcoming v1.4 follow-ups (Notifications config editor, Send-to-channel button, Paper-trader live monitor).
- **Service layer** at `api/reporting_service.py` — pure functions `list_dbs()` and `run_report()`. SQLite probes use read-only URI mode (`file:<path>?mode=ro`) so the list endpoint coexists with an actively-writing paper trader (Pi WAL-mode runtime). Cross-platform path handling via `Path.as_uri()`. Per-file `sqlite3.DatabaseError` and `FileNotFoundError` are caught and skipped with a logged warning — one bad file does not 500 the whole list. `_DATA_DIR` is anchored against repo root via `Path(__file__).resolve().parents[1] / "data"`, so the route works regardless of uvicorn's launch cwd.
- **Test coverage** — 25 unit tests in `tests/test_reporting_service.py` (path traversal, WAL coexistence, locked-DB graceful skip, corrupt-bytes skip, file-disappearing-mid-iteration, error-dict normalization, cwd-anchor lock) + 6 FastAPI TestClient integration tests in `tests/test_api_reporting.py` covering 200/400/404/500 paths.

### Notes

- **No DB schema migration.** Reads existing `paper_trades` table verbatim (schema established in v1.1.0).
- **Pi runtime: zero-impact.** Reports is a UI-only addition; existing CLI / cron P&L delivery on the Pi continues unchanged. No cutover required.
- **Auth posture inherited.** Reports endpoints use the project's existing `Depends(check_auth)` convention (Bearer-token check that's a no-op when `API_KEY` env var is unset). Frontend Bearer-attachment is a pre-existing project-wide gap (only `DataSources.tsx` attaches via `localStorage.getItem('ec_token')`); v1.4.0 inherits the as-is posture, deferring a cross-cutting fix to a future release.

## [1.3.1] — 2026-05-01

### Fixed

- **Polymarket adapter pagination + 404 robustness** (PR #16) — `collect_markets()` now honors the `dry_run` flag (was silently walking Gamma `/markets` until offset ~100K, hitting Gamma's undocumented offset ceiling with 422 Unprocessable Entity). Now `dry_run` breaks after one page (kalshi-adapter parity), and 422 at offset > 0 is treated as natural end-of-pagination. `collect_trades()` now treats 404 from CLOB (typical for closed/settled markets with no live trade endpoint) as no-trades rather than aborting the sweep.
- **CLI `download` multi-exchange dispatch** (PR #17) — `edge-catcher download --markets config/markets-polymarket.yaml` previously crashed with `KeyError: 'kalshi'` because `cli/download.py` always instantiated `KalshiAdapter` regardless of the markets file's exchange. The CLI now resolves the `AdapterMeta` from the markets-yaml filename and dispatches by `meta.exchange`, mirroring `api.dispatchers.DOWNLOAD_DISPATCHERS`. Polymarket joins Kalshi as a first-class CLI exchange. Unknown exchanges raise a structured `NotImplementedError` with a hint about Coinbase OHLC's separate subcommands.
- **`edge_catcher.adapters.polymarket.__init__` re-exports `PolymarketAdapter`** (PR #16) — kalshi-package parity; `from edge_catcher.adapters.polymarket import PolymarketAdapter` now works.

### Changed

- **`monitors/auth.py` narrows `load_pem_private_key`'s return to `RSAPrivateKey`** via `isinstance` (PR #16) — the cryptography library's union return type changes shape across versions (newer releases add post-quantum types like `MLDSAxxPrivateKey`, `MLKEMxxxPrivateKey`); the explicit narrow makes the subsequent `.sign(...)` call type-check on every supported release. The per-module `[[tool.mypy.overrides]] ignore_errors = true` is removed; mypy is now clean here without suppression.
- **`TradeStoreProtocol` extracted in `monitors/trade_store.py`** (PR #16) — structural protocol that both `TradeStore` (SQLite) and `InMemoryTradeStore` (replay) satisfy nominally without inheritance. `monitors/dispatch.py` now annotates every `store: …` parameter as `TradeStoreProtocol`; the `# type: ignore[arg-type]` at `monitors/replay/backtester.py:197` is removed. No behavior change — protocols are erased at runtime.

### Added

- **`CONTRIBUTING.md` "Type-checking" section** (PR #16) — formalizes the v1.3.0 zero-tolerance mypy gate with override-discipline guidance; the PR-process bullet now requires running `mypy edge_catcher api` alongside ruff and pytest.
- **`CLAUDE.md` "Adding a new exchange" step 7** (PR #17) — reminds future contributors to wire CLI dispatch alongside the API one.
- **6 dispatch regression tests in `tests/test_download_resolve.py`** (PR #17) — parametrized `_resolve_meta` cases, unknown-raises, Polymarket dispatch, Kalshi dispatch regression guard.

## [1.3.0] — 2026-05-01

### Added

- **Polymarket adapter** (`edge_catcher.adapters.polymarket`) — second exchange, parallel structure to Kalshi. Uses Gamma's public market-metadata API + CLOB's public trades API (no auth required). 24-test suite plus a live-API smoke verified 5 markets fetched and 1 trade projected end-to-end. Wires through `api/adapter_registry.py` so existing download/backtest/UI flows work transparently. New config: `config/markets-polymarket.yaml`.
- **Docker deployment path** — `Dockerfile`, `docker-compose.yml`, `.dockerignore`, and `docs/deployment-docker.md` (~280-line full VPS walkthrough). Image build verified. Adds a turnkey path for users who'd rather not hand-provision a Pi.
- **`docs/tutorial.md`** — 30-minute build-your-first-strategy walkthrough that takes the reader from hypothesis through real-data graduation; complements the existing 5-minute `quickstart.md`.
- **`docs/llm-providers.md`** — ~225-line deep-dive on the multi-provider LLM layer (Anthropic / OpenAI / OpenRouter / Together). Covers env-var setup, model selection, and observed cost/latency tradeoffs.
- **`docs/reporting.md` "Wiring up delivery" section** — formalizes the `--notify` delegation pattern shipped in v1.1; replaces older prose that pre-dated the turnkey flag.

### Changed

- **mypy zero-tolerance CI gate** — `Type-check (mypy)` step now runs between Lint and Test in `.github/workflows/ci.yml`. Type checking is enforced strictly: the baseline is empty and any new error fails the build. Adds `mypy` + `types-PyYAML` + `types-requests` as dev deps.
- **63 pre-existing mypy errors cleared at the source** rather than baselined. Real defects surfaced and fixed in the process: dead `Hypothesis(series=, db_path=)` API call in `research/loop.py` (now `data_sources=`), `pnl_cents` float→int silent precision loss in event backtester, ast `end_lineno` Optional handling in `strategy_parser.py`, tighter narrowing in `ai/client.py` and `monitors/dispatch.py`.

### Fixed

- **websockets `InvalidStatus` rename compat** — `monitors/engine.py` reconnect path now catches both `InvalidStatus` (websockets ≥ 13) and `InvalidStatusCode` (older) via a `getattr` fallback. Earlier pin compatibility hot-path didn't survive the rename; this restores it.
- **Tutorial CLI references** — `docs/tutorial.md` was published in v1.2.1 with four references that didn't match the actual CLI surface: Part 4 sample JSON used `win_rate_pct`/`per_strategy: [...]` (actual: `win_rate` fraction + `per_strategy: {}` dict); Part 5 referenced a non-existent `--show-trades` flag; Part 6 referenced a non-existent `--param key=v1,v2` sweep syntax; Part 7 referenced `download --series` (actual: `download --markets FILE`). All four corrected to match the live CLI; smoke-tested against the bundled `demo_markets.db` fixture.

## [1.2.1] — 2026-04-30

### Fixed

- **Replay parity: `MarketState._first_seen` now persisted in bundles** — the live engine accumulated this set across days/weeks (the set of tickers ever observed), but bundles never serialized it, so replay reported `is_first_observation=True` for every ticker on its first replay-time event. Strategies keying off `is_first_observation` for entry decisions entered spurious replay-only trades that never fired live. Validated bit-exact across 7 days of post-fix paper-trader bundles (2026-04-21 .. 2026-04-27). Older pre-fix bundles hit the legacy fallback's documented mid-day WS-reconnect+`clear()` reconstruction limitation; their parity will improve as new bundles capture under the v2 writer.

### Changed

- **Bundle `market_state_at_start.json` bumps to `schema_version: 2`** — adds a sorted `first_seen` array. Adopts `json.dumps(sort_keys=True)` for byte-stable output (useful for debug-by-diff and the determinism assertion in the new bundle-write test). Pre-v2 bundles remain readable: `_seed_market_state` falls back to deriving `_first_seen` from the union of orderbooks ∪ metadata keys with an info-level log line. The bundle's manifest `schema_version` is intentionally NOT bumped — per-file snapshots are independently versioned (matches the existing convention in `strategy_state_at_start.json`).
- **`_write_market_state_snapshot` accepts an injectable `captured_at` keyword param** — defaults to `datetime.now(UTC)` so production callers are unaffected, but tests can pass a frozen ISO string for determinism assertions.

### Added

- **`tests/test_replay_parity_first_seen.py`** — 7-day strict-parity harness behind a `requires_bundles` pytest marker (opt-in via `-m requires_bundles`). Verifies `replay_capture(bundle).trades` matches the bundle's `paper_trades_v2_<day>.sqlite` exactly, modulo a per-day allowlist (`[]` for all 7 days at present). Freshness gate fails loud if the cached bundle's `engine_commit` drifts from the fixture's recorded commit.
- **`tests/fixtures/replay_parity/regenerate.py`** — reproducible fixture-regen script with explicit Goal-table cross-check (`EXPECTED_TRADE_COUNTS`); refuses to write fixtures on parity violation. Exit codes: 0 success / 2 mismatch / 3 bundle missing.
- **`MARKET_STATE_SCHEMA_VERSION = 2`** module constant in `edge_catcher/monitors/capture/bundle.py` for consumers that need to programmatically version-gate.
- **Regression tests for `MarketState.clear()`** locking in the WS-reconnect contract (`_first_seen` resets cleanly + the next dispatched tick surfaces `is_first_observation=True` through the strategy's `on_tick`).

## [1.2.0] — 2026-04-27

### Added

- **`generate_report` adds `open_positions` and `all_time_by_strategy` fields** — additive; existing keys/values unchanged. Surfaces the data the daily P&L formatter needs to produce a rich Discord-friendly summary without an LLM in the loop.
- **`report_to_notification` produces a multi-section body** — Yesterday / All-time-by-strategy / Portfolio / Open-positions sections in plain text + simple markdown. Empty-day handling explicit ("No settled trades."). Renders well across all four adapters (Discord embed, Slack mrkdwn, SMTP, stdout). Replaces the v1.1.0 single-line body, which was minimum-viable for a generic helper.
- **`docs/upgrade-1.2.md`** — full migration guide for retiring the LLM-formatter daily P&L cron pattern. Covers both crontab and OpenClaw paths, troubleshooting (including the Vixie cron `%`-escape footgun), Slack rendering caveat, and channel-privacy reminder.

### Changed

- **`tests/fixtures/reporting_cli_no_notify_golden.json` rebaselined** — locks the v1.2.0 JSON shape (adds `open_positions` + `all_time_by_strategy`). Existing v1.1.x consumers reading specific keys continue to work; consumers doing strict byte-equality against the v1.1.0 output need to refresh their baseline.
- **`docs/upgrade-1.1.md`** — escaped `%` in cron-line examples (Vixie/cronie treats unescaped `%` as command-stdin terminator; copy-pasted entries silently broke daily P&L delivery). Added a troubleshooting row.

### Fixed

- **`_section_yesterday` no longer emits dangling header** when `today_by_strategy` has rows whose status is outside `(won, lost)` (e.g., a hypothetical future `pending` status). Falls back to the same "No settled trades." message as the empty-input case.
- **Open-positions section caps at 30 rows** with a `…(N more)` overflow marker — prevents Discord from rejecting embed descriptions longer than 4096 chars on busy days with many concurrent positions.

## [1.1.0] — 2026-04-27

### Added

- **`edge_catcher.notifications`** — pluggable notification delivery layer with `stdout`, `file`, `webhook` (discord/slack/generic styles), and `smtp` adapters. YAML-configured at `config.local/notifications.yaml` with env-var interpolation and a forward-compat `version:` field. Used by the reporting CLI's new `--notify`, `--notify-config`, `--quiet` flags. See [docs/upgrade-1.1.md](docs/upgrade-1.1.md) for upgrade + cron-wiring instructions.
- **`config/notifications.example.yaml`** — sanitized template with all four adapter types and discord/slack webhook examples.
- **Reporting CLI** — `--notify <name>` (repeatable), `--notify-config <path>`, `--quiet`. Backward-compatible: invocations without `--notify` keep the existing JSON-to-stdout behavior byte-for-byte (locked by a golden-file test).

### Deprecated

- `edge_catcher/monitors/notifications.py` — paper-trader-internal Discord webhook helper. Will be migrated onto `edge_catcher.notifications` in a future release (see roadmap v1.2 candidate); existing callers continue to work in the meantime.

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

[1.2.0]: https://github.com/goldenwo/edge-catcher/releases/tag/v1.2.0
[1.1.0]: https://github.com/goldenwo/edge-catcher/releases/tag/v1.1.0
[1.0.1]: https://github.com/goldenwo/edge-catcher/releases/tag/v1.0.1
[1.0.0]: https://github.com/goldenwo/edge-catcher/releases/tag/v1.0.0
