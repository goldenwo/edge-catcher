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
    slippage_cents INTEGER,                    -- DEPRECATED (spec §4.2 — use market_impact_cents + limit_slippage_cents); signed: positive = worse than limit (any side)
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

-- Compatibility VIEW: the daily-P&L reporting CLI
-- (edge_catcher.reporting) hardcodes `FROM paper_trades` and reads the
-- paper-DB column names. This VIEW lets `python -m edge_catcher.reporting
-- --db <live_trades.db>` run UNMODIFIED against the live DB (Phase H1;
-- spec §7 / R2-Gap2).
--
--   * entry_price_cents -> entry_price, series -> series_ticker: the
--     reporting CLI reads the paper names; pnl_cents, entry_fee_cents,
--     fill_size, exit_time, strategy pass through unchanged.
--   * status is NOT a dumb pass-through: `exit_pending` is a STILL-HELD
--     position (exit order in flight; no paper analog) so it MUST project
--     as `open` or the operator UNDER-sees live exposure. The other
--     live-only statuses (pending/rejected/rejected_post_hoc/cancelled/
--     lost_truth) pass through RAW and are naturally excluded because
--     reporting only matches open/won/lost/scratch. Per-row column
--     transform, NOT aggregation — no GROUP BY, no row collapsing (a
--     split-row partial-exit residual stays `open` -> excluded from
--     closed sums; children carry allocated fee+pnl).
--   * SQLite has no CREATE OR REPLACE VIEW. Bare CREATE VIEW errors on a
--     0003 re-run (see runner "Atomicity warning"); CREATE VIEW IF NOT
--     EXISTS would silently retain a STALE definition after any future
--     projection change. So DROP VIEW IF EXISTS then CREATE VIEW —
--     re-run-safe AND always picks up the latest projection. This does
--     NOT violate this file's data-preservation contract (lines 10-12
--     forbid DROP/rename of TABLES/COLUMNS = live data; a VIEW is
--     derived, stateless, holds ZERO rows — dropping/recreating it
--     loses nothing).
DROP VIEW IF EXISTS paper_trades;
CREATE VIEW paper_trades AS
SELECT
  id, ticker,
  series            AS series_ticker,
  strategy, side, intended_size, fill_size,
  entry_price_cents AS entry_price,
  blended_entry_cents, slippage_cents, fill_pct,
  stop_loss_distance_cents,
  CASE WHEN status = 'exit_pending' THEN 'open' ELSE status END AS status,
  client_order_id, kalshi_order_id, placed_at_utc, entry_time, exit_time,
  reconciled_at_utc, exit_price_cents, pnl_cents,
  entry_fee_cents, exit_fee_cents, exit_reason, rejection_reason, notes
FROM live_trades;
