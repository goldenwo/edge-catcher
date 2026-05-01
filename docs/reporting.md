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

Two patterns, pick whichever matches your setup.

### Direct delegation via `--notify` (recommended, v1.1+)

The reporting CLI accepts `--notify <channel>` and `--notify-config <path>`,
which delegate delivery to the [`edge_catcher.notifications`](../README.md#notifications)
adapter layer — Discord webhook, Slack incoming webhook, SMTP, or
file-JSONL audit log. No wrapper script needed; channels live in YAML.

**Setup:**

1. Create `config.local/notifications.yaml` (gitignored — it carries
   webhook URLs and other secrets). Start from
   [`config/notifications.example.yaml`](../config/notifications.example.yaml).

   ```yaml
   version: 1
   channels:
     daily-pnl:
       type: webhook
       style: discord
       url: ${DISCORD_PNL_WEBHOOK_URL}
       title_prefix: "[paper-trader]"
   ```

2. Set the corresponding env var in `.env` (also gitignored):

   ```
   DISCORD_PNL_WEBHOOK_URL=https://discord.com/api/webhooks/.../...
   ```

3. Verify the channel resolves before scheduling cron:

   ```bash
   python -m edge_catcher.reporting \
     --db data/paper_trades.db \
     --notify daily-pnl \
     --notify-config config.local/notifications.yaml
   ```

   You should see a delivery line like
   `daily-pnl  ok  204  117ms` and the message in your Discord.
   `--quiet` suppresses the JSON dump on stdout if you only want the
   delivery to happen (cron-friendly).

**Cron entry** — single command, no wrapper script to maintain:

```cron
# Daily P&L at 07:45 local time. Note: any literal `%` in the cron line
# must be escaped as `\%` per Vixie cron spec — see docs/upgrade-1.1.md
# §"Vixie cron escapes" if your daily P&L silently stops delivering.
45 7 * * * cd /opt/edge-catcher && \
    /usr/bin/python3 -m edge_catcher.reporting \
        --db data/paper_trades.db \
        --notify daily-pnl \
        --notify-config config.local/notifications.yaml \
        --quiet \
        >> /var/log/edge-catcher/daily-pnl.log 2>&1
```

**Multi-channel delivery** — pass `--notify` more than once to fan out
the same report to multiple destinations on a single run:

```bash
python -m edge_catcher.reporting \
    --db data/paper_trades.db \
    --notify daily-pnl \
    --notify ops-jsonl \
    --notify-config config.local/notifications.yaml
```

`ops-jsonl` here is a `type: file` channel (append a JSONL audit log
that ops scrapes); `daily-pnl` is the Discord webhook above. Both fire
on the same run; the per-channel `DeliveryResult` rows summarize success,
HTTP status, and latency in the table the CLI prints (or in the log file
when redirected from cron).

**Rich body (v1.2+)** — the CLI produces a 4-section message body
(Yesterday by strategy/series, All-time per-strategy, Portfolio stats,
Open positions) information-equivalent to an LLM-formatted summary.
Renders cleanly across all four channel styles. See
[upgrade-1.2.md](upgrade-1.2.md) for migrating off an LLM-formatter cron
pattern, and the channel-privacy reminder for routing strategy/series
names to the right audience.

**Error reporting** — if `generate_report` returns an error dict (DB
missing, schema drift, etc.), the CLI dispatches an error-severity
notification on the same channel set BEFORE exiting non-zero. You'll
see the failure in your Discord/Slack instead of having to scrape the
cron log.

### Custom wrapper script (still supported)

If you need formatting beyond what `report_to_notification` produces, or
you're delivering to a destination the notifications layer doesn't yet
adapt (e.g. a custom internal API), keep a small wrapper script:

```cron
# 07:45 ET daily: custom-formatted report
45 7 * * * cd /opt/edge-catcher && python scripts/post_daily_pnl.py
```

Put the script in `scripts/` (gitignored per the project's private-file
convention) — it tends to bake in API tokens and channel IDs that don't
belong in the public tree.
