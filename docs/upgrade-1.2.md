# Upgrading from v1.1.x to v1.2.0

v1.2.0 makes `python -m edge_catcher.reporting --notify` produce a multi-section daily-P&L message that's information-equivalent to the existing LLM-formatter cron pattern. This unlocks retiring the LLM-in-the-loop daily report and standardising on the unified notifications layer.

## What's new

- **`generate_report` now surfaces `open_positions` and `all_time_by_strategy` fields** — additive; existing v1.1.0 consumers are unaffected.
- **`report_to_notification` produces a rich multi-section body** with: Yesterday breakdown by strategy/series, All-time per-strategy summary, Portfolio stats, Open positions.
- **Discord embeds, Slack blocks, SMTP plain-text, file-JSONL audit logs** — all render the new body correctly without per-channel formatter changes.

## Migration: retire LLM-formatter daily P&L cron

If your daily P&L is delivered through an LLM-formatter agent (e.g., the `paper-trader-daily-pnl` job in `~/.openclaw/cron/jobs.json` calling Claude Haiku to format), you can now replace it with a direct shell-cron invocation.

### Option A — Plain user crontab (simplest)

Replace the OpenClaw agent job with a crontab entry:

```cron
# Daily P&L at 07:45 ET
45 7 * * * cd /home/openclaw/edge-catcher && \
    DISCORD_PNL_WEBHOOK_URL=https://discord.com/api/webhooks/.../... \
    /usr/bin/python3 -m edge_catcher.reporting \
        --db data/paper_trades_v2.db \
        --date "$(TZ=America/New_York date -d 'yesterday' +%Y-%m-%d)" \
        --notify-config config.local/notifications.yaml \
        --notify pnl_discord \
        --quiet
```

Notes:
- The `$(TZ=America/New_York date -d 'yesterday' ...)` substitution gives you the EDT yesterday, matching the existing Haiku query's "yesterday in EDT" logic.
- `--quiet` suppresses the JSON dump on stdout (cron mail would otherwise mail you the report).
- `config.local/notifications.yaml` should already exist from your v1.1.x setup (per `docs/upgrade-1.1.md`); if not, copy `config/notifications.example.yaml` and edit.

### Option B — Keep OpenClaw scheduling, swap the agent for a shell-exec

If you prefer keeping the schedule in `~/.openclaw/cron/jobs.json` (e.g., for visibility / unified scheduling), replace the `agentTurn` payload with a trivial shell-exec payload (depends on what your OpenClaw scheduler supports — check the docs). The body of the agent's `message` was previously the SQL-query-and-format prompt; the new agent message can simply be:

```
Run this command and report the exit code:
exec `cd /home/openclaw/edge-catcher && python3 -m edge_catcher.reporting --db data/paper_trades_v2.db --date $(TZ=America/New_York date -d 'yesterday' +%Y-%m-%d) --notify-config config.local/notifications.yaml --notify pnl_discord --quiet`
```

The agent becomes a thin shim. Lower value than Option A unless OpenClaw's scheduler integration is critical.

### Backing out

If the new formatter's output isn't what you want, the old `paper-trader-daily-pnl` job can be re-enabled by setting `enabled: true` in jobs.json. Keep it as a fallback for one cycle before removing it entirely.

## Updating `config.local/notifications.yaml`

If you want to add channels alongside Discord (e.g., file audit log), the same config now produces the rich body for ALL of them:

```yaml
version: 1
channels:
  pnl_discord:
    type: webhook
    url: ${DISCORD_PNL_WEBHOOK_URL}
    style: discord

  pnl_audit_log:
    type: file
    path: /var/log/edge-catcher/pnl.jsonl
```

Then dispatch to both:

```bash
python -m edge_catcher.reporting ... --notify pnl_discord --notify pnl_audit_log
```

The Discord embed gets the rich body in its description; the JSONL file gets one record per delivery with the full report dict in `payload` for downstream `jq` queries.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Discord embed shows raw asterisks (`**Section:**`) instead of bold | Style not set to `discord` (defaulted to `generic`) | Set `style: discord` on the webhook channel. |
| Yesterday section says "No settled trades." but you know there were trades | `--date` argument is wrong (UTC vs EDT mismatch) | Confirm the cron date computation matches the EDT-bucket convention used by `generate_report`. |
| Test suite fails on byte-for-byte golden after upgrade | You're running v1.1.x tests against v1.2.x code (or vice versa) | Pull the latest tests; the golden file is rebaselined per release that changes report shape. |

## Reverting to v1.1.x

```bash
git checkout v1.1.0
sudo systemctl restart paper-trader
# Re-enable the OpenClaw agent job in ~/.openclaw/cron/jobs.json (set enabled: true)
```

The notifications layer's PUBLIC API is backward compatible — v1.1.x callers using `report_to_notification(report)` continue to work; the body just becomes longer. Only consumers doing strict byte-equality against the v1.1.0 single-line format need to update.
