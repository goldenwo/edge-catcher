# Edge Catcher — Systemd Service Deployment

This directory contains files for running the Kalshi data downloader as a
systemd service on any Linux host.

## Prerequisites

1. **Python venv configured** at `/opt/edge-catcher/.venv` with all
   dependencies installed:
   ```bash
   cd /opt/edge-catcher
   python -m venv .venv
   .venv/bin/pip install -e ".[ai,ui]"
   ```

2. **API key present** in `/opt/edge-catcher/.env`:
   ```
   KALSHI_API_KEY=your_key_here
   ```

3. Run the install script as root (once):
   ```bash
   sudo bash deploy/install-service.sh
   ```

## Start / Stop / Status

```bash
# Start the download
sudo systemctl start edge-catcher-download

# Stop it
sudo systemctl stop edge-catcher-download

# Check status
sudo systemctl status edge-catcher-download

# Disable autostart on boot
sudo systemctl disable edge-catcher-download
```

## Viewing Logs

```bash
# Follow the live log
tail -f /var/log/edge-catcher/download.log

# Or via journald
sudo journalctl -u edge-catcher-download -f
```

## How It Differs From `download_loop.sh`

| Feature | `download_loop.sh` | Systemd service |
|---|---|---|
| Process management | Shell loop in terminal | OS-level, survives logouts |
| Restart on failure | Shell `while true` loop | `Restart=on-failure` |
| Clean-exit behaviour | Always restarts | Does NOT restart on exit 0 |
| Logging | `/tmp/edge-catcher-download.log` | `/var/log/edge-catcher/download.log` |
| Resource limits | None | `MemoryMax=2G`, `CPUQuota=80%` |
| Boot autostart | No | Yes (`systemctl enable`) |

The key difference: the service uses `Restart=on-failure`, meaning it only
restarts when the process crashes (non-zero exit). When the download finishes
successfully (`exit 0`), the service stops and stays stopped. This is the
correct production behavior — no accidental re-download loops.

`download_loop.sh` is kept for development and one-off testing use.

## Service Exit Behaviour

- **exit 0** (`download_complete`): service stops, does **not** restart.
- **non-zero exit** (crash, OOM, network failure): service waits 30 s then
  restarts. After 5 failures within 10 minutes, systemd stops retrying.

To reset the failure counter after investigating:
```bash
sudo systemctl reset-failed edge-catcher-download
```

---

## OHLC Refresher Service

The OHLC refresher (`deploy/ohlc-refresher.service`) is a lightweight companion
daemon that polls Coinbase and upserts one-minute candles into `data/ohlc.db`,
keeping the tables ≤90s fresh for `on_tick` spot-pricing strategies. It runs as
a **separate** service alongside the paper-trader.

### When to enable

Enable it when you have at least one `on_tick` strategy that reads `self.ohlc`
(e.g. `strat-13`). Without it, the paper-trader's staleness gate will
reject every tick and your strategy will be silent.

### Setup

1. **Config** — in your `config.local/paper-trader-spotfair.yaml` (or whichever
   config you use), ensure both the `ohlc:` block and the `ohlc_refresh:` block
   are present and enabled:

   ```yaml
   ohlc:
     enabled: true
     assets:
       eth: [data/ohlc.db, eth_ohlc]
       sol: [data/ohlc.db, sol_ohlc]
       doge: [data/ohlc.db, doge_ohlc]

   ohlc_refresh:
     enabled: true
     db_path: data/ohlc.db
     products: [ETH-USD, SOL-USD, DOGE-USD]
     poll_interval_s: 20
   ```

2. **Install and start** — copy the unit and enable it:

   ```bash
   sudo cp deploy/ohlc-refresher.service /etc/systemd/system/
   sudo systemctl daemon-reload
   sudo systemctl enable --now ohlc-refresher
   ```

3. **NTP time sync is required** — the staleness gate compares candle timestamps
   to local wall-clock. If the host clock drifts, the gate rejects otherwise-fresh
   candles, silently raising tick rejection counts. Verify NTP is active:

   ```bash
   timedatectl status   # look for "NTP service: active"
   # Or, if you use chrony:
   chronyc tracking
   ```

   Most cloud VMs (Ubuntu, Debian) have `systemd-timesyncd` enabled by default.
   Raspberry Pi OS does too. If it's off: `sudo systemctl enable --now systemd-timesyncd`.

### Logs

```bash
# Follow per-cycle freshness + STALE warnings
tail -f /var/log/edge-catcher/ohlc-refresher.log

# Or via journald
sudo journalctl -u ohlc-refresher -f
```

Look for `STALE` WARN lines — these mean a product's newest candle is older than
`staleness_warn_s` (default 75s) and the paper-trader's staleness gate is likely
rejecting ticks for that asset.

### Relationship to the paper-trader

Both services read the same config file and write/read the same `data/ohlc.db`.
The refresher only ever upserts (idempotent); a hard kill is safe at any point.
Restart order doesn't matter — the paper-trader will tolerate a momentarily
empty or stale `ohlc.db` until the refresher catches up after boot.
