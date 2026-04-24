#!/bin/bash
# Run as root (sudo) to install the systemd service.
# Usage: sudo bash deploy/install-service.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_FILE="$SCRIPT_DIR/edge-catcher-download.service"
LOG_DIR="/var/log/edge-catcher"

echo "Installing edge-catcher-download systemd service..."

# Create log directory
mkdir -p "$LOG_DIR"
chown "${SERVICE_USER:-edge-catcher}:${SERVICE_USER:-edge-catcher}" "$LOG_DIR"

# Install unit file
cp "$SERVICE_FILE" /etc/systemd/system/edge-catcher-download.service
systemctl daemon-reload
systemctl enable edge-catcher-download.service

echo "Done. To start: sudo systemctl start edge-catcher-download"
echo "Logs: tail -f /var/log/edge-catcher/download.log"
echo "Status: sudo systemctl status edge-catcher-download"
