-- Migration 0002: risk_state table
-- Key-value store for risk module persistent state.
-- Phase 1 single row: key='closed_equity_peak', value='{"cents": <int>}'.
-- PeakTracker uses INSERT OR IGNORE on first boot to seed; on_trade_close
-- UPDATEs when closed equity exceeds the stored peak.

CREATE TABLE IF NOT EXISTS risk_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,     -- JSON-encoded; consumers parse with json.loads()
    updated_at TEXT NOT NULL -- ISO-8601 UTC
);
