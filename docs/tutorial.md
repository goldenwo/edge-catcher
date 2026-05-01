# Tutorial — build your first strategy end-to-end

This tutorial takes you past the 5-minute [quickstart](quickstart.md) and
walks through the full research loop: pick a hypothesis, look at the
data, write the strategy, run the backtest, read the output, iterate,
and (optionally) graduate it to the paper trader.

You'll write **`mean_reversion_under_10`** — a small but real strategy
that buys the underdog leg of any market trading below 10c, betting on
intraday mean-reversion. Whether it has edge against your real data is
not the point; the point is that by the end you'll know how to ask the
question properly.

Estimated time: 30–45 minutes the first time, ~10 minutes for the
iterate loop after that.

## Prerequisites

- The [quickstart](quickstart.md) completed successfully — i.e. you can
  run `edge-catcher backtest` against `demo_markets.db` and get JSON
  back. If that fails, fix it first; this tutorial assumes the toolchain
  works.
- A clone of the repo with `pip install -e ".[dev]"` done.
- ~30 minutes.

## Part 1 — Pick a hypothesis

Strategies start as one-sentence hypotheses. The pattern is:

> **"When [observable condition], the [contract leg] tends to [direction]
> by [amount] over [horizon]."**

Filling in the template for our tutorial strategy:

> "When the YES leg of a market trades below 10c, the NO leg tends to
> drift up over the next 30 minutes."

This is a *fading-the-extreme* hypothesis. Markets at 10c usually mean
"the crowd thinks this won't happen" — but the crowd is wrong often
enough that buying NO ("yes, this won't happen") cheaply has historical
edge in some series.

Whether it actually has edge in YOUR data is what the rest of this
tutorial answers.

## Part 2 — Look at the data

You can't write a sensible strategy without seeing the trade tape. The
bundled fixture has 24 trades across 2 markets in `DEMO_SERIES`:

```bash
sqlite3 edge_catcher/data/examples/demo_markets.db <<'SQL'
SELECT
    ticker,
    yes_price,
    100 - yes_price AS no_price,
    count,
    taker_side,
    created_time
FROM trades
ORDER BY created_time
LIMIT 20;
SQL
```

You should see prices oscillating between 5–15c on one ticker and
40–55c on the other. The 5–15c ticker is the one our hypothesis cares
about; the 40–55c one is "hovering near 50/50" — no edge expected.

For real data (Kalshi, Polymarket), point sqlite3 at `data/<exchange>.db`
after running `edge-catcher download`. The schema is the same.

## Part 3 — Write the strategy

The `Strategy` ABC lives in `edge_catcher.runner.strategies`. The single
required callback is `on_trade(trade, market, portfolio)` — it fires
once per trade in time-order and returns a list of `Signal` objects.

Per [CLAUDE.md private-file rules](../CLAUDE.md), strategies that
encode your real edge live in `edge_catcher/runner/strategies_local.py`
(gitignored). For this tutorial we'll put the strategy there.

Open the example template:

```bash
cp edge_catcher/runner/strategies_local.py.example \
   edge_catcher/runner/strategies_local.py
$EDITOR edge_catcher/runner/strategies_local.py
```

Add the strategy at the bottom of the file:

```python
from edge_catcher.runner.strategies import Signal, Strategy
from edge_catcher.storage.models import Market, Trade


class MeanReversionUnder10(Strategy):
    """Buy NO when the YES leg trades below 10c; exit on a 10c recovery.

    Entry:  trade.yes_price < entry_threshold (default 10c)
            → BUY NO at (100 - yes_price)
    Exit:   later trade on same ticker shows NO-leg ≥ entry + 10c
            → SELL (take-profit)
    """

    name = 'mean_reversion_under_10'
    supported_series: list[str] = []  # empty = any series

    def __init__(
        self,
        entry_threshold: int = 10,
        exit_threshold: int = 10,
        size: int = 1,
    ) -> None:
        self.entry_threshold = entry_threshold
        self.exit_threshold = exit_threshold
        self.size = size

    def on_trade(self, trade, market, portfolio):
        # ----- Exit branch: we hold a position; check TP -----
        if portfolio.has_position(trade.ticker, self.name):
            pos = portfolio.positions.get((trade.ticker, self.name))
            if pos is not None:
                current_no = 100 - trade.yes_price
                if current_no >= pos.entry_price + self.exit_threshold:
                    return [Signal(
                        action='sell',
                        ticker=trade.ticker,
                        side=pos.side,
                        price=current_no,
                        size=pos.size,
                        reason=(
                            f'tp: no={current_no} >= entry={pos.entry_price}'
                            f' + {self.exit_threshold}'
                        ),
                    )]
            return []

        # ----- Entry branch: YES leg is cheap; buy NO -----
        if trade.yes_price < self.entry_threshold:
            no_price = 100 - trade.yes_price
            return [Signal(
                action='buy',
                ticker=trade.ticker,
                side='no',
                price=no_price,
                size=self.size,
                reason=f'entry: yes={trade.yes_price} < {self.entry_threshold}',
            )]
        return []
```

Save the file. Strategy auto-discovery picks up any `Strategy`
subclass with a `name` attribute — no registration step needed.

Verify it's discoverable:

```bash
edge-catcher backtest --list-strategies | grep mean_reversion
# → mean_reversion_under_10
```

If you don't see your strategy, the import failed somewhere. Run
`python -c "import edge_catcher.runner.strategies_local"` to see the
real error.

## Part 4 — Run the backtest

```bash
edge-catcher backtest \
    --series DEMO_SERIES \
    --db-path edge_catcher/data/examples/demo_markets.db \
    --strategy mean_reversion_under_10 \
    --json
```

You should see a JSON blob ending with `"status": "ok"`. The interesting
fields:

```json
{
  "total_trades": 3,            // trades the strategy entered
  "wins": 2,
  "losses": 1,
  "win_rate_pct": 66.7,
  "net_pnl_cents": 18,
  "sharpe": 0.81,
  "per_strategy": [
    {"strategy": "mean_reversion_under_10", "trades": 3, "net_pnl_cents": 18, ...}
  ]
}
```

The exact numbers will differ from this example as the fixture evolves —
the shape is what matters. If `total_trades == 0`, your entry condition
never fired against the fixture; check your `entry_threshold` matches
the price range visible in Part 2's sqlite query.

## Part 5 — Read the per-trade output

Aggregate stats hide failure modes. Drop `--json` and pass
`--show-trades` to see each entry/exit:

```bash
edge-catcher backtest \
    --series DEMO_SERIES \
    --db-path edge_catcher/data/examples/demo_markets.db \
    --strategy mean_reversion_under_10 \
    --show-trades
```

For each trade, you get the entry price, exit price, holding period, and
the reason string the strategy attached. Look for:

- **Trades that entered at the bottom of the price range** — these are
  what your hypothesis predicted; if they're profitable, the edge is
  real (in this fixture).
- **Trades that exited at exit-threshold exactly** — clean wins; the
  exit logic fires.
- **Open trades at end-of-data** — the strategy never got a TP signal;
  the backtester force-settles at the closing price, which can dominate
  the P&L if the close is far from entry.

If your strategy is dominated by force-settled positions, your exit
logic is too lax. Tighten `exit_threshold` and re-run.

## Part 6 — Iterate (parameter sweep)

The naive way: edit the constructor defaults, re-run, edit, re-run.
For 2-3 iterations that's fine. Past that, use the parameter sweep:

```bash
edge-catcher backtest \
    --series DEMO_SERIES \
    --db-path edge_catcher/data/examples/demo_markets.db \
    --strategy mean_reversion_under_10 \
    --param entry_threshold=5,8,10,15 \
    --param exit_threshold=5,10,15 \
    --json | jq '.[] | {params, win_rate_pct, sharpe, net_pnl_cents}'
```

This runs a 4×3 grid (12 backtests) and dumps a row per param combo.
Sort by Sharpe to find the most stable params; sort by `net_pnl_cents`
to find the best raw return. They usually disagree — pick what matches
your evaluation criterion.

The autonomous research loop (`edge-catcher research run`) automates
this further: it generates parameter grids from a hypothesis YAML and
runs them in parallel, plus statistical tests on whether the edge is
real or noise. See [research-pipeline-data-flow.md](research-pipeline-data-flow.md)
when you want that level of rigor.

## Part 7 — Graduate to real data

Replace the demo fixture with a real exchange DB:

```bash
# Kalshi (requires API key in .env per docs/quickstart.md)
edge-catcher download --series KXBTCD

edge-catcher backtest \
    --series KXBTCD \
    --db-path data/kalshi-btc.db \
    --strategy mean_reversion_under_10 \
    --json
```

Same strategy, real trades. If the win rate / Sharpe drop dramatically
between fixture and real data, your strategy was overfit to the
fixture's specific noise pattern. That's normal; the fixture is too
small to be statistically meaningful. Real-data results are what
actually matter.

For Polymarket data, swap `--db-path data/polymarket.db` and configure
the series in `config/markets-polymarket.yaml`. See [adapter-guide.md](adapter-guide.md)
for adapter wiring details.

## Part 8 — Optional: paper-trader graduation

If real-data backtests show edge that survives walk-forward + parameter
sensitivity, the next step is paper-trading the strategy live to verify
execution fidelity matches the backtest.

Paper trader uses a *different* base class — `PaperStrategy` from
`edge_catcher.monitors.strategy_base` — because the live engine has
state the event backtester doesn't model (orderbook, dispatch plumbing,
sizing pipeline). You'll port the strategy, not move it.

The port is mechanical:

| Event backtester | Paper trader |
|---|---|
| `class X(Strategy):` | `class X(PaperStrategy):` |
| `def on_trade(self, trade, market, portfolio)` | `def on_tick(self, ctx)` |
| Returns `list[Signal]` | Returns `list[Signal]` |
| `trade.yes_price` (cents) | `ctx.yes_ask` (cents) for entries; `ctx.yes_bid` for exits |
| `portfolio.has_position(...)` | `ctx.open_positions` (list of dicts) |

See [strategy-guide.md](strategy-guide.md) §"Paper trader strategies"
for the full mapping. The paper-trader port lives in
`edge_catcher/monitors/strategies_local.py` (also gitignored).

## What you've learned

After this tutorial you should be able to:

- Translate a one-sentence hypothesis into a `Strategy` subclass
- Read both aggregate (`--json`) and per-trade (`--show-trades`) backtest output
- Tune parameters with `--param` sweeps and pick the right scoring metric
- Move from the bundled fixture to real exchange data
- (Eventually) port to the paper trader for live verification

## Next reads

- [strategy-guide.md](strategy-guide.md) — the full `Strategy` /
  `PaperStrategy` API reference
- [research-pipeline-data-flow.md](research-pipeline-data-flow.md) — how
  the autonomous research loop turns hypotheses into validated edges
- [adapter-guide.md](adapter-guide.md) — adding a new exchange beyond
  Kalshi / Polymarket / Coinbase
- [reporting.md](reporting.md) — turning paper-trader output into daily
  P&L notifications
