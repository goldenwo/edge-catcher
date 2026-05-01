# Docker / cloud deployment

This guide ships you from "git clone" to "paper trader writing v2 bundles
on a $5/month VPS" in ~20 minutes. Two paths:

- **Local Docker dev** — `docker compose up` for a full stack on your
  laptop. Fast iteration with bind-mounted source.
- **VPS production** — same image, hardened compose, persistent volumes,
  reverse proxy for the API/UI, basic monitoring.

For the systemd-on-bare-metal alternative (no containers, manage Python
yourself), see [`deploy/README.md`](../deploy/README.md). Pick one or
the other — don't run both against the same `data/` directory.

---

## Local Docker dev

### Prerequisites

- Docker Engine ≥24 + Docker Compose v2 (the `docker compose` plugin,
  not the legacy `docker-compose` script).
- A clone of this repo with your `config.local/`, `.env`, and
  `data/` directories already in place. The compose file bind-mounts
  these into the containers — they are NOT baked into the image.
- ~2 GB free disk for the build cache + image layers.

### Bring it up

```bash
# Build the image and start all three services in the background.
docker compose up -d --build

# Tail logs; ctrl-C exits the tail without stopping the services.
docker compose logs -f paper-trader

# Open the UI: http://127.0.0.1:5173
# API health:  http://127.0.0.1:8000/health
```

### What's running

| Service | Image | Purpose |
|---|---|---|
| `paper-trader` | `edge-catcher:local` (built from `Dockerfile`) | Long-running market loop. Writes `data/paper_trades.db` + `data/bundles/` |
| `api` | same image | FastAPI backend (`uvicorn api.main:app`). Bound to `127.0.0.1:8000` |
| `ui` | `node:22-slim` (Vite dev server) | React frontend on `127.0.0.1:5173`, proxies to `api:8000` |

The three services share the same Docker network (compose default), so
the UI can reach the API via the `api` hostname; the host can only
reach `127.0.0.1` because the ports are bind-localhost-only by default.

### Source iteration

The paper-trader container has the source baked in (no bind-mount on
`/app/edge_catcher`), so Python code changes need a rebuild:

```bash
docker compose build paper-trader && docker compose up -d paper-trader
```

The UI service bind-mounts `./ui` into the container and runs the Vite
dev server — frontend changes hot-reload without a rebuild.

### Tear down

```bash
docker compose down            # stop services, keep volumes/data
docker compose down -v         # also remove anonymous volumes (UI node_modules)
```

`./data`, `./logs`, and `./config.local` are bind-mounted, so they
survive `down -v` — only the in-container ephemeral state goes.

---

## VPS production walkthrough

Tested on Ubuntu 24.04 LTS on a $5–10/month VPS (1 vCPU, 1–2GB RAM).
Should also work on Debian 12, Hetzner Cloud, DigitalOcean, etc. —
anywhere Docker Engine runs.

### 1. Provision

```bash
# As root on the fresh VPS:
apt-get update && apt-get install -y docker.io docker-compose-v2 git curl
systemctl enable --now docker

# Create a non-root deploy user that's in the docker group.
useradd --create-home --shell /bin/bash --groups docker edge
mkdir -p /opt/edge-catcher
chown edge:edge /opt/edge-catcher
```

Switch to the `edge` user for everything below.

### 2. Clone + configure

```bash
sudo -iu edge
cd /opt/edge-catcher
git clone https://github.com/goldenwo/edge-catcher.git .

# .env carries secrets — restrictive perms.
cp .env.example .env
chmod 600 .env
$EDITOR .env
# Set:
#   KALSHI_API_KEY=...                       (if you collect from Kalshi)
#   DISCORD_PNL_WEBHOOK_URL=...              (if you ship reports to Discord)
#   ANTHROPIC_API_KEY=...                    (if you use the research agent)

# config.local/ holds non-secret runtime config (paper-trader.yaml,
# notifications.yaml). Set up from the examples.
mkdir -p config.local
cp config/notifications.example.yaml config.local/notifications.yaml
cp config/paper-trader.example.yaml config.local/paper-trader.yaml
$EDITOR config.local/paper-trader.yaml      # set strategies, series, etc.

# data/ holds the live SQLite DB + bundles. Make sure it exists.
mkdir -p data logs
```

### 3. Build + start

```bash
docker compose up -d --build paper-trader api
# The UI dev server isn't typically run on production — see "UI in production"
# below for the static-build alternative.

# Verify both started cleanly (look for "running" status).
docker compose ps
docker compose logs --tail=50 paper-trader
docker compose logs --tail=50 api
```

### 4. Reverse proxy + TLS for the API

The API binds to `127.0.0.1:8000` by default — not exposed to the
internet. Front it with caddy or nginx so you can reach it from a
browser with HTTPS.

#### Caddy (one-line config, auto-TLS):

```bash
sudo apt-get install -y caddy
cat <<EOF | sudo tee /etc/caddy/Caddyfile
edge-catcher.example.com {
    reverse_proxy 127.0.0.1:8000
}
EOF
sudo systemctl reload caddy
```

Caddy auto-provisions a Let's Encrypt cert for `edge-catcher.example.com`
and renews it. Make sure your DNS A record points at the VPS first.

#### nginx alternative — see https://nginx.org/en/docs/http/configuring_https_servers.html

### 5. UI in production

Two options:

**Option A — same compose, dev server.** Add the `ui` service back to
your prod compose if you don't mind running a Vite dev server in
production (small surface, but not the canonical pattern):

```yaml
ui:
  # … same as docker-compose.yml …
  ports:
    - "127.0.0.1:5173:5173"
```

Then add a Caddy block:
```
edge-catcher.example.com {
    reverse_proxy /api/* 127.0.0.1:8000
    reverse_proxy /* 127.0.0.1:5173
}
```

**Option B — static build, served by Caddy.** Cleaner for production:

```bash
cd /opt/edge-catcher/ui
npm ci && npm run build
# `npm run build` outputs to ui/dist/
```

Caddy block:
```
edge-catcher.example.com {
    handle /api/* {
        reverse_proxy 127.0.0.1:8000
    }
    handle {
        root * /opt/edge-catcher/ui/dist
        file_server
        try_files {path} /index.html
    }
}
```

Re-run `npm run build` whenever you pull UI changes.

### 6. Daily P&L cron from Docker

Two patterns:

**Pattern A — host crontab + `docker compose exec`:**

```cron
# 07:45 ET daily — runs reporting CLI inside the api container
45 7 * * * docker compose -f /opt/edge-catcher/docker-compose.yml exec -T api \
    python -m edge_catcher.reporting \
        --db data/paper_trades.db \
        --notify daily-pnl \
        --notify-config config.local/notifications.yaml \
        --quiet \
        >> /var/log/edge-catcher/daily-pnl.log 2>&1
```

**Pattern B — sidecar service with built-in scheduler.** Add a `cron`
service to the compose file using e.g. `mcuadros/ofelia`. Slightly more
moving parts but keeps the schedule in version control.

See [docs/reporting.md](reporting.md) §"Direct delegation via `--notify`"
for the underlying CLI invocation and YAML setup.

### 7. Log rotation

`docker compose logs` keeps logs in JSON files under
`/var/lib/docker/containers/`. Without rotation they grow unbounded.

Add to `/etc/docker/daemon.json` (create the file if missing) and restart
docker:

```json
{
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "10m",
    "max-file": "3"
  }
}
```

```bash
sudo systemctl restart docker
docker compose down && docker compose up -d
```

### 8. Backups

The state that matters lives in `data/`:
- `data/paper_trades.db` — every trade decision
- `data/bundles/` — daily replay artifacts (already uploaded to R2 if
  capture is enabled, but local copies are useful)

Daily backup snippet (host crontab):

```cron
0 4 * * * tar czf /backups/edge-catcher-$(date +\%F).tar.gz \
    -C /opt/edge-catcher data/ && \
    find /backups -name 'edge-catcher-*.tar.gz' -mtime +14 -delete
```

For multi-host disaster recovery, set up R2 replication of the
`data/bundles/` directory (the replay backtester can run against any
historical bundle from R2 — no live DB needed for backtest reproduction).

### 9. Updating

```bash
sudo -iu edge
cd /opt/edge-catcher
git pull origin main
docker compose build paper-trader api
docker compose up -d paper-trader api
# Wait for healthcheck → check logs:
docker compose logs --tail=30 -f paper-trader
```

The image rebuild is the slow step (~2-3 min on a 1 vCPU box).
The container restart is sub-second.

For a zero-downtime update, run a second compose project with a
different name (`docker compose -p edge2 up -d`), wait for it to be
healthy, swap the Caddy upstream, then bring the old project down.
Out of scope for this guide.

---

## Troubleshooting

**`docker compose up` fails with "no space left on device"** — old image
layers fill `/var/lib/docker`. `docker system prune -af` reclaims them.

**API container restart-loops with "address already in use"** — something
else is bound to port 8000 on the host. `sudo ss -tlnp 'sport = :8000'`
to find it. Either stop the conflicting process or change the port
mapping in `docker-compose.yml`.

**Paper-trader can't write to `data/paper_trades.db`** — bind-mount
permission issue. The container runs as UID 999 (the `edge` user inside
the image); the host directory needs write access for that UID. Fix:

```bash
sudo chown -R 999:999 data/ logs/
```

Or change the compose file's `user:` directive to match your host user.

**UI shows "Network Error" for every API call** — Vite dev server's CORS
proxy isn't reaching the API container. Check that the `api` service
healthcheck is passing (`docker compose ps`); if it isn't, the API
itself crashed at startup — check `docker compose logs api`.

**`KalshiAdapter` raises "API key required"** — the container can't see
your `.env` file. The compose file uses `env_file: .env` which expects
the file at the compose-file's directory. Verify with
`docker compose config | grep KALSHI`.

---

## See also

- [`deploy/README.md`](../deploy/README.md) — systemd-on-bare-metal alternative (no Docker)
- [`docs/architecture.md`](architecture.md) — what each service does
- [`docs/reporting.md`](reporting.md) — daily P&L cron setup (cron pattern works inside Docker too)
- [`docs/upgrade-1.1.md`](upgrade-1.1.md), [`docs/upgrade-1.2.md`](upgrade-1.2.md) — Vixie cron `%` escape footgun + rich body migration
