-- Migration 0003: live_trades table
-- The single table for live-money order state (sub-project B / v1.6.0 PR 5).
-- Mirrors paper_trades_v2.db:paper_trades shape (battle-tested 4+ months,
-- queried by the reporting CLI + Reports page) and extends additively with
-- live-only columns (kalshi_order_id, client_order_id, placed_at_utc,
-- reconciled_at_utc, the 10-value status enum, allocated-fee accounting).
--
-- 0001 (kill_switch) + 0002 (risk_state) already shipped by PR #36.
--
-- Idempotency: CREATE TABLE/INDEX IF NOT EXISTS — safe to re-run. Future
-- schema changes MUST be additive-only (ALTER TABLE ADD COLUMN); never DROP
-- or rename (Risk #1: once Phase 1 has live data, old rows must stay readable).

CREATE TABLE IF NOT EXISTS live_trades (
    -- Identity
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Trade definition (mirrors paper_trades)
    ticker TEXT NOT NULL,
    series TEXT NOT NULL,
    strategy TEXT NOT NULL,
    side TEXT NOT NULL CHECK(side IN ('yes', 'no')),
    intended_size INTEGER NOT NULL,            -- size_contracts requested at place; DECREMENTS on partial-exit splits
    original_intended_size INTEGER NOT NULL,   -- IMMUTABLE after INSERT — used for allocated-fee math across splits
    fill_size INTEGER NOT NULL DEFAULT 0,      -- actual filled (current; decrements on partial-exit splits)
    entry_price_cents INTEGER NOT NULL,        -- Signal's entry_price intent
    blended_entry_cents INTEGER,               -- VWAP of fills; NULL until fill
    slippage_cents INTEGER,                    -- signed: positive = worse than limit (any side)
    fill_pct REAL,                             -- fill_size / intended_size
    stop_loss_distance_cents INTEGER,          -- from Signal; for reporting

    -- Lifecycle (NEW for live).  These 10 values are live_trades.status
    -- DB-state values, DISTINCT from engine.executor.OrderResult.status
    -- (which remains the 3-value Literal {filled, rejected, pending}).
    status TEXT NOT NULL CHECK(status IN (
        'pending', 'open', 'exit_pending',
        'won', 'lost', 'scratch',
        'rejected', 'rejected_post_hoc', 'cancelled', 'lost_truth'
    )),
    -- D's idempotency key; UNIQUE at the DB layer.  Charset+length is enforced
    -- at the Python layer (engine.execution._make_client_order_id) and at the
    -- wire layer (live.client _CLIENT_ORDER_ID_PATTERN per PR #28).  A DB-layer
    -- CHECK regex is INTENTIONALLY OMITTED: split-ids (record_partial_exit) can
    -- exceed 80 chars (parent-id + "-split-N"), so a CHECK here would either
    -- permit arbitrary length (defeating its purpose) or special-case
    -- split-ids (complexity for no defensive gain).  The Python-layer guard
    -- is the binding source of truth.
    client_order_id TEXT NOT NULL,         -- D's idempotency key; UNIQUE
    kalshi_order_id TEXT,                  -- Kalshi's id; NULL until known

    -- Timestamps
    placed_at_utc TEXT NOT NULL,           -- ISO-8601, when D POSTed
    entry_time TEXT,                       -- when fill confirmed (from WS or reconcile)
    exit_time TEXT,                        -- when close confirmed
    reconciled_at_utc TEXT,                -- last successful reconcile against Kalshi

    -- Closed-trade fields (mirrors paper_trades; populated on close)
    exit_price_cents INTEGER,
    pnl_cents INTEGER,                         -- exit - entry - entry_fee - exit_fee
    entry_fee_cents INTEGER,                   -- total entry fee paid for the entry; immutable after entry-fill
    entry_fee_remaining_cents INTEGER,         -- decrements on partial-exit splits; final close consumes the remainder
    exit_fee_cents INTEGER,
    exit_reason TEXT,                          -- 'take_profit' | 'stop_loss' | 'time_exit' | 'settlement'

    -- Audit (NEW for live)
    rejection_reason TEXT,                 -- when status='rejected' or 'rejected_post_hoc'
    notes TEXT,                            -- operator notes (e.g., manual cancel)

    UNIQUE(client_order_id)
);

-- Index for C's read API (frequent COUNT(*) WHERE status='open' queries).
CREATE INDEX IF NOT EXISTS live_trades_status_idx ON live_trades(status);

-- Partial index for reconciliation queries (pending / exit_pending scans).
CREATE INDEX IF NOT EXISTS live_trades_pending_idx ON live_trades(status, placed_at_utc)
    WHERE status IN ('pending', 'exit_pending');

-- Partial index for daily P&L computation (closed-trade rows only).
CREATE INDEX IF NOT EXISTS live_trades_exit_idx ON live_trades(exit_time)
    WHERE status IN ('won', 'lost', 'scratch');
