-- Migration 0004: dual-slippage diagnostic columns on live_trades
-- Reporting-only (never feed cost basis / size / fees / pnl). See
-- docs/superpowers/specs/2026-05-24-dual-slippage-metrics-design.md.
--
--   market_impact_cents    : signed, blended vs top-of-book best (positive = worse)
--   limit_slippage_cents   : signed, blended vs the order limit  (positive = worse)
--   entry_best_price_cents : reconcile-support ref — top-of-book best snapshot at INSERT
--   entry_limit_price_cents: reconcile-support ref — the order limit at INSERT
--
-- Idempotency: SQLite ADD COLUMN is not natively idempotent (no IF NOT EXISTS).
-- The runner (apply_migrations) catches a re-applied "duplicate column name"
-- and records the version, so a crash-window re-run is safe. Additive only —
-- never DROP/rename (old live rows must stay readable, per 0003's Risk #1).

ALTER TABLE live_trades ADD COLUMN market_impact_cents INTEGER;
ALTER TABLE live_trades ADD COLUMN limit_slippage_cents INTEGER;
ALTER TABLE live_trades ADD COLUMN entry_best_price_cents INTEGER;
ALTER TABLE live_trades ADD COLUMN entry_limit_price_cents INTEGER;
