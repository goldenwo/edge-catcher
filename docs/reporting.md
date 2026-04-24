# Reporting

`edge_catcher.reporting` is a small, dependency-free P&L reporter for
the paper trader's `paper_trades` SQLite DB. It emits JSON: aggregate
all-time stats, a "settled today" bucket, and a per-strategy /
per-series breakdown for the day. Pipe it wherever you want — Discord,
Slack, email, a webhook, a static dashboard — the module produces JSON
and stops there.

## Python API

```python
from pathlib import Path
from edge_catcher.reporting import generate_report

report = generate_report(Path("data/paper_trades.db"))
# Or pin a specific day's "settled today" bucket:
report = generate_report(Path("data/paper_trades.db"), date="2026-04-03")
```

### Return shape

```python
{
    "timestamp": "2026-04-24T22:37:08.493157+00:00",   # report generation time, UTC ISO8601
    "date": "2026-04-03",                              # the "today" bucket date

    "all_time": {
        "total_trades":       int,
        "open_trades":        int,
        "closed_trades":      int,
        "wins":               int,
        "losses":             int,
        "win_rate_pct":       float,   # 0–100
        "net_pnl_cents":      int,
        "net_pnl_usd":        float,
        "avg_pnl_cents":      float,
        "fees_cents":         int,
        "deployed_cents":     int,     # SUM(entry_price * fill_size)
        "deployed_usd":       float,
        "roi_deployed_pct":   float,   # net_pnl / deployed * 100
    },

    "today": {
        "settled_count": int,
        "pnl_cents":     int,
        "pnl_usd":       float,
    },

    "today_by_strategy": [
        {
            "strategy":      str,
            "series_ticker": str,
            "status":        "won" | "lost",
            "count":         int,
            "pnl_cents":     int,
        },
        # one row per (strategy, series, status) triple
    ],
}
```

If the DB doesn't exist, the report is `{"error": "DB not found at <path>"}`.
The CLI exits 1 in that case.

## CLI

```bash
python -m edge_catcher.reporting --db <path> [--date YYYY-MM-DD]
```

`--date` pins the "settled today" bucket to a specific day. Default is
UTC today. The `today` filter is **EDT-adjusted** (UTC minus 4 hours)
because that matches Kalshi's settlement day convention; trades are
attributed to the date where their `exit_time` falls in EDT.

## Settled-today semantics

Trades are filtered into the `today` bucket by their `exit_time`, not
their `entry_time`. A trade entered at 23:00 UTC on day N and exited at
01:00 UTC on day N+1 belongs to day N+1. The EDT shift ensures the
bucket aligns with Kalshi-day boundaries (most contracts settle in the
NYC afternoon, so EDT is the operationally correct day cutoff).

The filter also requires `status IN ('won', 'lost')` so open trades
never appear in the today bucket. All-time aggregates use the same
status filter for closed trades but include open trades in
`open_trades` for visibility.

## Deployed capital formula

```
deployed_cents = SUM(entry_price * fill_size)
```

Where `entry_price` is per-contract cents and `fill_size` is the
number of contracts filled. So deployed capital is "the total cents
committed to entries", not the notional max payout. ROI is then
`net_pnl_cents / deployed_cents * 100`. This corrects an earlier bug
where deployed was double-counted via `entry_price * fill_size /
some_factor` — the comment block at the top of
`edge_catcher/reporting/__init__.py` records the math fixes.

## Example output

Run against the bundled fixture, pinned to a day with settled trades:

```bash
python -m edge_catcher.reporting \
    --db edge_catcher/data/examples/paper_trades_demo.db \
    --date 2026-04-03
```

```json
{
  "timestamp": "2026-04-24T22:37:08.493157+00:00",
  "date": "2026-04-03",
  "all_time": {
    "total_trades": 20,
    "open_trades": 0,
    "closed_trades": 20,
    "wins": 12,
    "losses": 8,
    "win_rate_pct": 60.0,
    "net_pnl_cents": 821,
    "net_pnl_usd": 8.21,
    "avg_pnl_cents": 41.0,
    "fees_cents": 20,
    "deployed_cents": 89,
    "deployed_usd": 0.89,
    "roi_deployed_pct": 922.47
  },
  "today": {
    "settled_count": 8,
    "pnl_cents": 267,
    "pnl_usd": 2.67
  },
  "today_by_strategy": [
    {"strategy": "longshot_fade_example", "series_ticker": "DEMO_A15M",
     "status": "lost", "count": 1, "pnl_cents": -7},
    {"strategy": "longshot_fade_example", "series_ticker": "DEMO_A15M",
     "status": "won",  "count": 3, "pnl_cents": 195},
    {"strategy": "longshot_fade_example", "series_ticker": "DEMO_B15M",
     "status": "lost", "count": 3, "pnl_cents": -18},
    {"strategy": "longshot_fade_example", "series_ticker": "DEMO_B15M",
     "status": "won",  "count": 1, "pnl_cents": 97}
  ]
}
```

(The `roi_deployed_pct` looks gigantic because the demo fixture uses
deliberately tiny entry prices — the math is right, the data is just
toy.)

## Piping to jq

The output is plain JSON, so jq composes naturally:

```bash
# Just today's net P&L in cents
python -m edge_catcher.reporting --db data/paper_trades.db | jq '.today.pnl_cents'

# All-time win rate
python -m edge_catcher.reporting --db data/paper_trades.db | jq '.all_time.win_rate_pct'

# Per-strategy net P&L for today
python -m edge_catcher.reporting --db data/paper_trades.db \
  | jq '.today_by_strategy
         | group_by(.strategy)
         | map({strategy: .[0].strategy,
                pnl_cents: (map(.pnl_cents) | add)})'
```

## Wiring up delivery

The framework intentionally stops at JSON. Most users wire their own
delivery channel — a small wrapper script that calls
`generate_report` (or the CLI), formats the relevant fields into a
message, and ships it via Discord webhook / Slack incoming webhook /
email / push notification. Keep that wrapper in your gitignored
`scripts/` directory; it tends to bake in API tokens and channel IDs
that don't belong in the public tree.

A typical cron entry on a research machine:

```cron
# 07:45 ET daily: post settled-yesterday report to Discord
45 7 * * * cd /opt/edge-catcher && python scripts/post_daily_pnl.py
```
