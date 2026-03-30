#!/bin/bash
# DEPRECATED: Use the systemd service for production deployments.
# See deploy/README.md for setup instructions.
# This script is kept for development/testing use only.

cd ~/edge-catcher
source .venv/bin/activate

FIRST_RUN=true

while true; do
    echo "$(date): Starting download..." >> /tmp/edge-catcher-download.log

    if [ "$FIRST_RUN" = true ]; then
        python -m edge_catcher download >> /tmp/edge-catcher-download.log 2>&1
        FIRST_RUN=false
    else
        echo "$(date): Restarting with --skip-market-scan" >> /tmp/edge-catcher-download.log
        python -m edge_catcher download --skip-market-scan >> /tmp/edge-catcher-download.log 2>&1
    fi

    EXIT_CODE=$?
    echo "$(date): Download exited with code $EXIT_CODE" >> /tmp/edge-catcher-download.log

    # Check completion: markets with trades vs markets with volume
    MARKETS_WITH_TRADES=$(python -c "
import sqlite3
conn = sqlite3.connect('data/kalshi.db')
with_trades = conn.execute('SELECT COUNT(DISTINCT ticker) FROM trades').fetchone()[0]
total_vol = conn.execute('SELECT COUNT(*) FROM markets WHERE volume > 0').fetchone()[0]
print(with_trades)
conn.close()
" 2>/dev/null)

    TOTAL_MARKETS_VOL=$(python -c "
import sqlite3
conn = sqlite3.connect('data/kalshi.db')
total_vol = conn.execute('SELECT COUNT(*) FROM markets WHERE volume > 0').fetchone()[0]
print(total_vol)
conn.close()
" 2>/dev/null)

    echo "$(date): Markets with trades: $MARKETS_WITH_TRADES / $TOTAL_MARKETS_VOL" >> /tmp/edge-catcher-download.log

    if [ "$MARKETS_WITH_TRADES" -ge "$TOTAL_MARKETS_VOL" ] 2>/dev/null && [ "$TOTAL_MARKETS_VOL" -gt 0 ]; then
        echo "$(date): Download complete!" >> /tmp/edge-catcher-download.log
        break
    fi

    echo "$(date): Restarting in 10 seconds..." >> /tmp/edge-catcher-download.log
    sleep 10
done
