-- Migration 0001: kill_switch table
-- Persists auto-tripped kill-switch rows across process restarts.
-- reason: one of KILL_AUTO_PANIC | KILL_AUTO_DRAWDOWN | KILL_AUTO_DAILY
-- detail: structured snapshot at the moment of trip
-- tripped_at: ISO-8601 UTC with microsecond resolution
-- cleared_at: NULL while active; set on manual or auto-midnight clear
-- cleared_by: 'auto_midnight' | 'human:<note>' | 'operator-cli'

CREATE TABLE IF NOT EXISTS kill_switch (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    reason TEXT NOT NULL,
    detail TEXT NOT NULL,
    tripped_at TEXT NOT NULL,
    cleared_at TEXT,
    cleared_by TEXT,
    UNIQUE(reason, tripped_at)
);

-- Partial index for the active-kill fast-path in KillSwitch.active_auto_kill().
-- SQLite evaluates the WHERE clause at index creation time; this index covers
-- only rows where cleared_at IS NULL (i.e. active rows).
CREATE INDEX IF NOT EXISTS kill_switch_active_idx
    ON kill_switch(cleared_at)
    WHERE cleared_at IS NULL;
