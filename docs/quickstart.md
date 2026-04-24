# Quickstart (5 minutes)

This walkthrough takes you from a fresh clone to a successful backtest
and a P&L report against the bundled fixtures, with no API keys or
network access required.

## 1. Clone and install

```bash
git clone https://github.com/goldenwo/edge-catcher.git
cd edge-catcher

# Minimal install (just the framework + dev tooling for running tests)
pip install -e ".[dev]"

# Full install (adds the AI client + UI dependencies)
pip install -e ".[dev,ai,ui]"
```

Edge Catcher requires Python 3.11+. The install registers an
`edge-catcher` console script and also exposes the same CLI as
`python -m edge_catcher`.

## 2. Run the example backtest

```bash
edge-catcher backtest \
    --series DEMO_SERIES \
    --db-path edge_catcher/data/examples/demo_markets.db \
    --strategy longshot_fade_example \
    --json
```

This runs `LongshotFadeExample` — a tutorial strategy that buys the NO
leg whenever the YES leg trades at <=5c (a "longshot fade") and exits
when the NO leg recovers — over the bundled `demo_markets.db` fixture.
The fixture contains a single series called `DEMO_SERIES` with two days
of synthetic trade data, so the backtest is deterministic.

You should see a JSON blob ending with `"status": "ok"` and a small
`per_strategy` block. The bundled fixture is intentionally tiny; the
goal is to confirm the toolchain works, not to find edge.

## 3. Run the example reporter

```bash
python -m edge_catcher.reporting --db edge_catcher/data/examples/paper_trades_demo.db
```

This prints a JSON report against `paper_trades_demo.db`, which contains
20 settled trades across two synthetic strategies. You will see
`all_time` aggregates (win rate, P&L, deployed capital) and a `today`
bucket that is empty because the fixture's `exit_time` values are not
"today" relative to your wall clock.

To see the `today` bucket populated, pin the date to a day that has
settled trades in the fixture (e.g. `2026-04-03`):

```bash
python -m edge_catcher.reporting \
    --db edge_catcher/data/examples/paper_trades_demo.db \
    --date 2026-04-03
```

## 4. Run the test suite

```bash
pytest tests/ -v
```

The suite runs fully mocked — no live API access, no API keys. Useful
for confirming your install is healthy.

## 5. Next steps

- Write your own strategy: see [`docs/strategy-guide.md`](strategy-guide.md).
- Add a new exchange: see [`docs/adapter-guide.md`](adapter-guide.md).
- Understand the four-stage research filter: see
  [`docs/architecture.md`](architecture.md).
- Customize the P&L report and pipe it to a notification channel: see
  [`docs/reporting.md`](reporting.md).
- Download real Kalshi data: `edge-catcher download` (settled markets,
  no API key required) or read the README's CLI Reference section.
- Try the autonomous research loop:
  `edge-catcher research loop --grid-only --max-runs 50 --parallel 4`
  (no LLM required for the grid phase).
