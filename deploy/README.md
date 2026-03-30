# Edge Catcher — Systemd Service Deployment

This directory contains files for running the Kalshi data downloader as a
systemd service on the Raspberry Pi 5 (or any Linux host).

## Prerequisites

1. **Python venv configured** at `/home/private-infra/edge-catcher/.venv` with all
   dependencies installed:
   ```bash
   cd /home/private-infra/edge-catcher
   python -m venv .venv
   .venv/bin/pip install -e ".[ai,ui]"
   ```

2. **API key present** in `/home/private-infra/edge-catcher/.env`:
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
