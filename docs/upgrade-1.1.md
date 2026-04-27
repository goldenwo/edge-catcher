# Upgrading from v1.0.x to v1.1.0

v1.1.0 adds the `edge_catcher.notifications` package and three CLI flags (`--notify`, `--notify-config`, `--quiet`) to `python -m edge_catcher.reporting`. **Existing cron invocations without `--notify` keep behaving byte-for-byte the same** (locked by a golden-file test) — so you can upgrade without touching cron, then add `--notify` opt-in when you're ready.

## What's new

- **`edge_catcher.notifications`** — pluggable delivery layer with four adapters: `stdout`, `file` (JSONL), `webhook` (discord / slack / generic styles), `smtp`. YAML-configured with `${ENV_VAR}` interpolation.
- **Reporting CLI flags:**
  - `--notify <name>` — repeat to dispatch to multiple channels.
  - `--notify-config <path>` — defaults to `config.local/notifications.yaml`.
  - `--quiet` — suppress the stdout JSON dump when `--notify` is in use (keeps the stderr results table + exit code).
- **`config/notifications.example.yaml`** — sanitized template. Copy to `config.local/notifications.yaml` and fill in your real values.

## Upgrade steps (machine running the paper trader / cron)

```bash
cd /path/to/edge-catcher    # adjust to your checkout
git fetch --tags
git checkout main           # or stay on whichever branch you deploy from
git pull
git tag -l v1.1.0           # confirm the tag landed locally
```

If your paper trader runs as a systemd service, restart it after the pull:

```bash
sudo systemctl restart paper-trader
sudo journalctl -u paper-trader -n 50    # sanity-check the restart
```

The paper trader's per-trade Discord notifications (`edge_catcher/monitors/notifications.py`) are unchanged — they continue to read `DISCORD_PAPER_TRADE_LOGS_WEBHOOK_URL` as before. A `DeprecationWarning` now fires at module import to signal the planned v1.2 migration onto the unified layer; this is silenced for internal callers via a `pyproject.toml` filter.

## Wiring the reporting CLI to your delivery channel

### 1. Create your local notifications config

```bash
cp config/notifications.example.yaml config.local/notifications.yaml
```

Edit `config.local/notifications.yaml` to keep only the channels you actually use, and replace `${VAR}` placeholders with real env-var names.

Example minimal config (Discord only):

```yaml
version: 1

channels:
  pnl_discord:
    type: webhook
    url: ${DISCORD_PNL_WEBHOOK_URL}
    style: discord
```

Set the env var the config references in your shell or systemd unit:

```bash
export DISCORD_PNL_WEBHOOK_URL="https://discord.com/api/webhooks/.../..."
```

### 2. Test the wiring (one-shot)

```bash
DISCORD_PNL_WEBHOOK_URL=... python -m edge_catcher.reporting \
    --db data/paper_trades_v2.db \
    --notify pnl_discord
```

Expected output:

- **stdout:** the same JSON report v1.0.x produced.
- **stderr:** a per-channel results table, e.g.

  ```
  channel              status  latency
  -------------------- ------- -------
  pnl_discord          OK      143ms
  ```

- **Discord:** an embed in the channel.

If the channel name is unknown to the config, the CLI exits **2** with a stderr error. If all configured channels fail at delivery, it exits **1**. If at least one succeeds, exit **0**. Same exit-code semantics for cron consumers.

### 3. Update your cron job

Add `--notify <name>` (and optionally `--quiet` to suppress the stdout JSON):

```cron
# Daily P&L at 07:45 ET, Discord-only
45 7 * * * cd /home/openclaw/edge-catcher && \
    DISCORD_PNL_WEBHOOK_URL=... \
    /usr/bin/python -m edge_catcher.reporting \
        --db data/paper_trades_v2.db \
        --notify-config config.local/notifications.yaml \
        --notify pnl_discord \
        --quiet
```

If you'd rather keep the JSON for log archival, drop `--quiet`. The JSON still goes to stdout; the per-channel results table goes to stderr.

## Multi-channel example (Discord + email + JSONL audit log)

```yaml
version: 1

channels:
  pnl_discord:
    type: webhook
    url: ${DISCORD_PNL_WEBHOOK_URL}
    style: discord

  pnl_email:
    type: smtp
    host: smtp.gmail.com
    port: 587
    user: ${SMTP_USER}
    password: ${SMTP_PASSWORD}
    from: alerts@example.com
    to: [me@example.com]
    use_tls: true

  daily_log:
    type: file
    path: /var/log/edge-catcher/notifications.jsonl
```

Then dispatch to all three:

```bash
python -m edge_catcher.reporting --db ... \
    --notify pnl_discord \
    --notify pnl_email \
    --notify daily_log
```

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Exit code 2, "config not found" on stderr | `config.local/notifications.yaml` missing or `--notify-config` path wrong | Confirm the path; for cron, prefer an absolute path. |
| Exit code 2, "unknown channel(s)" on stderr | `--notify <name>` doesn't match any key under `channels:` in the YAML | Check spelling; channel names are case-sensitive. |
| Exit code 2, "env var X is not set" | YAML references a `${VAR}` not in the environment | Set the env var in the systemd unit / cron's `Environment=` / shell profile. |
| Discord embed shows no color or wrong color | `style:` not set to `discord` | The `webhook` type accepts `style: discord | slack | generic`; default is `generic`. |
| SMTP delivery fails with "Username and Password not accepted" | Gmail rejects regular passwords for SMTP | Use an app password (Google → Security → App passwords). |
| `${VAR}` shows up literally in the dispatched payload | The env var IS set but the YAML quoted it as a string | YAML doesn't interpolate inside single-quoted scalars; rewrite `'${VAR}'` as `${VAR}` (unquoted) or `"${VAR}"` (double-quoted). |

## What's NOT in v1.1.0 (deferred)

Per the v1.1 design and roadmap, intentionally deferred:

- Async delivery (`asend`)
- Retry / exponential backoff
- HTML email
- SMTP OAuth2 / XOAUTH2
- Concurrent dispatch
- Migrating `edge_catcher/monitors/notifications.py` (paper-trader-internal Discord client) onto the unified layer — see [roadmap.md](roadmap.md) v1.2 candidate.

If your use case needs any of these now, open an issue with the specific need and we can re-prioritize.
