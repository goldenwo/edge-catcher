# Strategy Guide

edge-catcher has two strategy interfaces, one for each backtester
engine. They share a common spirit but are **independent base classes
with different callbacks** — strategies are not portable between them.

| Engine | Base class | Callback | Module |
|--------|-----------|----------|--------|
| Event backtester (research / parameter sweeps) | `Strategy` | `on_trade(trade, market, portfolio)` | `edge_catcher.runner.strategies` |
| Paper trader (live + replay backtester) | `PaperStrategy` | `on_tick(ctx)` | `edge_catcher.monitors.strategy_base` |

Most users start with the event backtester. The paper trader has more
moving parts (live state, orderbook handling, dispatch plumbing) and is
covered briefly at the bottom.

## Event backtester strategies

### File layout

```
edge_catcher/runner/
├── strategies.py              # tracked: Strategy ABC + Signal + reusable mixins
├── strategies_example.py      # tracked: LongshotFadeExample tutorial strategy
├── strategies_local.py        # tracked but example-only — your local strategies
├── strategies_local.py.example# tracked: minimal copy-paste template
└── strategy_parser.py         # tracked: auto-discovery
```

When you write a strategy, you typically copy
`strategies_example.py` (or `strategies_local.py.example`) into
`strategies_local.py` and edit it in place. `strategies_local.py` is
where the auto-discovery looks for user strategies; tracking the file
ensures the import path always exists, but in practice contributors
overwrite it locally.

The runner-side `strategies_local.py` is *tracked* (and intentionally
inert in the public repo). Only the paper-trader copy at
`edge_catcher/monitors/strategies_local.py` is gitignored — see
`CONTRIBUTING.md` for the full public/private split.

### Auto-discovery

The backtester scans these modules for `Strategy` subclasses on every
run:

- `edge_catcher.runner.strategies`
- `edge_catcher.runner.strategies_local`
- `edge_catcher.runner.strategies_example`

Any concrete `Strategy` subclass with a unique `name` attribute is
picked up automatically. No manual registration, no decorator. The
research loop uses the same discovery, so any strategy you write is
immediately available everywhere.

### The `Strategy` interface

```python
from edge_catcher.runner.strategies import Strategy, Signal
from edge_catcher.storage.models import Market, Trade


class MyStrategy(Strategy):
    name = "my_strategy"
    supported_series: list[str] = []  # empty = any series; else whitelist

    def __init__(self, threshold: int = 50, size: int = 1) -> None:
        self.threshold = threshold
        self.size = size

    def on_trade(self, trade: Trade, market: Market, portfolio) -> list[Signal]:
        return []  # zero or more buy/sell signals
```

`Signal` is a dataclass:

```python
Signal(
    action="buy",        # "buy" or "sell"
    ticker="...",
    side="yes",          # "yes" or "no"
    price=42,            # cents
    size=1,              # contracts
    reason="...",        # logged for analysis
)
```

The backtester hands each strategy a `Trade` (the latest tick on a
given ticker), the `Market` metadata, and the current `Portfolio` view.
Your job is to decide whether to enter, exit, or do nothing — return a
list of `Signal` objects (often empty, sometimes one, occasionally two
if you exit and immediately re-enter). Position management,
settlement, slippage, and fee accounting all happen inside the engine
based on the signals you return.

### Walked-through example: `LongshotFadeExample`

The full source is in
[`edge_catcher/runner/strategies_example.py`](../edge_catcher/runner/strategies_example.py).
The relevant logic:

```python
class LongshotFadeExample(Strategy):
    name = 'longshot_fade_example'
    supported_series: list[str] = []  # any series

    def __init__(self, entry_threshold=5, exit_threshold=10, size=1):
        self.entry_threshold = entry_threshold
        self.exit_threshold = exit_threshold
        self.size = size

    def on_trade(self, trade, market, portfolio):
        # Exit branch
        if portfolio.has_position(trade.ticker, self.name):
            pos = portfolio.positions.get((trade.ticker, self.name))
            if pos is not None:
                current_no = 100 - trade.yes_price
                if current_no >= pos.entry_price + self.exit_threshold:
                    return [Signal(
                        action='sell', ticker=trade.ticker, side=pos.side,
                        price=current_no, size=pos.size,
                        reason='take_profit',
                    )]
            return []

        # Entry branch
        if trade.yes_price <= self.entry_threshold:
            no_price = 100 - trade.yes_price
            return [Signal(
                action='buy', ticker=trade.ticker, side='no',
                price=no_price, size=self.size,
                reason='longshot fade',
            )]
        return []
```

Step by step:

1. **Engine streams trades in time order.** For each trade on each
   ticker the engine calls `on_trade(trade, market, portfolio)`.
2. **Exit branch first.** If we already have a position on this ticker
   under our strategy name, look at the current NO-leg price (the
   complement of `trade.yes_price`) and check the take-profit
   condition. If hit, return a sell `Signal`.
3. **Entry branch.** If we have no position and the YES leg is at or
   below the entry threshold (5c by default), the NO leg is cheap →
   buy NO at `100 - trade.yes_price`.
4. **Otherwise return `[]`.** The engine moves on.
5. **Settlement.** Any open position at market close is auto-settled
   by the engine's settlement sweep — your strategy's
   `on_market_close` hook can override (default: no-op).

### `supported_series` semantics

`supported_series: list[str]` is a soft whitelist: an empty list means
"the strategy is willing to run on any series" and a populated list
means "only run on these series". The backtester's `--series` CLI flag
is the hard filter that selects which DB to read from; your strategy's
`supported_series` is what the auto-discovery uses to decide whether
the strategy is eligible for that DB at all.

### Running your strategy

```bash
# List discovered strategies (sanity check)
edge-catcher backtest --list-strategies

# Run on the bundled fixture
edge-catcher backtest \
    --series DEMO_SERIES \
    --db-path edge_catcher/data/examples/demo_markets.db \
    --strategy my_strategy \
    --json

# Run on real downloaded data
edge-catcher backtest --series KXBTCD --strategy my_strategy --json
```

Pass strategy parameters via the matching CLI flags (the engine
introspects each strategy's `__init__` signature and exposes
`--<param>` flags automatically — see `--help` for the full list).

### Reusable mixins

`strategies.py` ships with two reusable mixins:

- `VolumeMixin` — skip entry once a ticker has had more than
  `max_trades` observed trades, useful for "early signal only"
  strategies.
- `MomentumMixin` — skip entry when an external price (e.g. BTC OHLC)
  moved more than `max_move_pct` in the recent window, useful for
  filtering out regime shifts.

Compose with multiple inheritance:

```python
class MyFiltered(VolumeMixin, MyStrategy):
    name = "my_filtered"
    def __init__(self, max_trades=20, **kwargs):
        MyStrategy.__init__(self, **kwargs)
        self._init_volume_filter(max_trades)
    def on_trade(self, trade, market, portfolio):
        if self._increment_and_check(trade.ticker):
            return []
        return MyStrategy.on_trade(self, trade, market, portfolio)
```

## Paper-trader strategies

The paper trader (`edge_catcher.monitors`) runs a live WS feed and
calls strategies on every tick. Most users will not need to write one
of these — the event backtester is the right place to iterate on
hypotheses.

If you do, the shape is:

```python
from edge_catcher.monitors.strategy_base import PaperStrategy, Signal


class MyPaper(PaperStrategy):
    name = "my_paper"
    supported_series = ["MY_SERIES"]
    default_params = {"threshold": 50}
    emoji = "🔵"

    def on_tick(self, ctx) -> list[Signal]:
        # ctx is a TickContext (orderbook + recent trades + state)
        ...
        return []  # entry / exit signals
```

Paper-trader strategies live in
`edge_catcher/monitors/strategies_local.py` (gitignored — your live
strategies stay private). The paper trader handles sizing, dispatch,
position state, and capture/replay; you implement only the decision
logic in `on_tick`.

For the broader paper-trader pipeline (live engine, capture/replay,
daily bundles) see [`docs/architecture.md`](architecture.md).
