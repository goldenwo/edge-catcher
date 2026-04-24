#!/bin/bash
# Install the paper trader as a systemd service.
# Run as root: sudo bash deploy/install-paper-trader.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="/var/log/edge-catcher"

echo "Installing edge-catcher-paper-trader systemd service..."

mkdir -p "$LOG_DIR"
chown "${SERVICE_USER:-edge-catcher}:${SERVICE_USER:-edge-catcher}" "$LOG_DIR"

cp "$SCRIPT_DIR/paper-trader.service" /etc/systemd/system/edge-catcher-paper-trader.service
systemctl daemon-reload
systemctl enable edge-catcher-paper-trader.service

echo "Done."
echo "Start:  sudo systemctl start edge-catcher-paper-trader"
echo "Status: sudo systemctl status edge-catcher-paper-trader"
echo "Logs:   tail -f /var/log/edge-catcher/paper-trader.log"
