#!/bin/bash
# Wrapper used by systemd service.
# Runs full market scan on first run (empty DB), skip scan on restarts.
cd /home/private-infra/edge-catcher
source .venv/bin/activate

MARKET_COUNT=$(python -c "
import sqlite3, os
db = 'data/kalshi-btc.db'
if not os.path.exists(db):
    print(0)
else:
    conn = sqlite3.connect(db)
    print(conn.execute('SELECT COUNT(*) FROM markets').fetchone()[0])
    conn.close()
" 2>/dev/null || echo 0)

if [ "$MARKET_COUNT" -gt 100000 ]; then
    echo "$(date): Markets already in DB ($MARKET_COUNT) — skipping market scan"
    exec python -m edge_catcher download --skip-market-scan
else
    echo "$(date): Fresh DB or small market count ($MARKET_COUNT) — running full scan"
    exec python -m edge_catcher download
fi
