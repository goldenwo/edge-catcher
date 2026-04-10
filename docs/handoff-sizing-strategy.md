# Paper Trader Sizing & Orderbook Validation — Handoff Doc

## Problem Statement
The paper trader currently assumes every trade fills instantly at the observed price with 1 contract. This means:
- PnL is overstated — no slippage, no partial fills, no sizing
- Can't answer "would this strategy work at $X real capital?"
- Thin markets like KXXRP (3-14 trades/contract lifetime) may show paper profits that evaporate with real order placement
- No way to compare series by deployable capital

## Current State
`paper_trader_v2.py` entry flow:
```
WS tick arrives → first price check → _apply_debut_fade() → _record_buy(conn, ticker, price, strategy, side, series_ticker)
```

`_record_buy` writes to SQLite with:
- `entry_price` (the WS tick price, taken at face value)
- `entry_fee_cents` (calculated from price, size hardcoded to 1)
- No orderbook check, no size column, no fill validation

PnL calculation:
```python
pnl = exit_price - entry_price - entry_fee_cents  # always 1 contract
```

Fee model already supports size:
```python
KALSHI_FEE.calculate(price=50, size=10)  # returns 18¢ — works correctly
```

DB schema (current):
```sql
paper_trades (
    id, ticker, entry_price, entry_time, exit_price, exit_time,
    pnl_cents, status, strategy, side, series_ticker, entry_fee_cents
)
```

## What Needs to Change

### 1. DB Migration — Add sizing columns
```sql
ALTER TABLE paper_trades ADD COLUMN intended_size INTEGER NOT NULL DEFAULT 1;
ALTER TABLE paper_trades ADD COLUMN fill_size INTEGER NOT NULL DEFAULT 1;
ALTER TABLE paper_trades ADD COLUMN blended_entry INTEGER;  -- actual fill price after walking book
ALTER TABLE paper_trades ADD COLUMN book_depth INTEGER;      -- total contracts available at signal time
ALTER TABLE paper_trades ADD COLUMN fill_pct REAL;           -- fill_size / intended_size
ALTER TABLE paper_trades ADD COLUMN slippage_cents REAL;     -- blended_entry - intended_price
```

### 2. Target Size Config
```python
TARGET_SIZE: dict[str, int] = {
    "KXXRP": 20,
    "KXBTC15M": 10,
    "KXNBAMENTION": 50,
}
DEFAULT_SIZE = 10
```
These are starting values — tune based on orderbook data after a few days.

### 3. Orderbook Snapshot Function
On every signal, before recording the trade:
```python
async def _check_orderbook(client, ticker, side, intended_size) -> dict:
    resp = await client.get(f"{KALSHI_REST_BASE}/markets/{ticker}/orderbook")
    book = resp.json().get("orderbook_fp", {})
    
    # Walk the relevant side of the book
    if side == "no":
        levels = book.get("no_dollars", [])  # [[price, quantity], ...]
    else:
        levels = book.get("yes_dollars", [])
    
    # Walk levels until intended_size is filled
    filled = 0
    total_cost = 0
    for price, qty in levels:
        take = min(qty, intended_size - filled)
        total_cost += take * price
        filled += take
        if filled >= intended_size:
            break
    
    blended_price = (total_cost / filled * 100) if filled > 0 else None
    
    return {
        "book_depth": sum(qty for _, qty in levels),
        "fill_size": filled,
        "fill_pct": filled / intended_size if intended_size > 0 else 0,
        "blended_price_cents": round(blended_price) if blended_price else None,
        "slippage_cents": round(blended_price - levels[0][0] * 100) if blended_price and levels else None,
    }
```

### 4. Modified Entry Flow
```
WS tick → first price check → _apply_debut_fade():
  1. Determine intended_size from TARGET_SIZE[series]
  2. Call _check_orderbook(client, ticker, side, intended_size)
  3. If fill_size == 0: skip, log "no liquidity"
  4. If fill_size < intended_size: record partial fill
  5. Record trade with: fill_size, blended_entry, book_depth, fill_pct, slippage
```

### 5. Modified PnL Calculation
```python
# Entry
entry_fee = KALSHI_FEE.calculate(blended_entry, fill_size)

# Settlement
pnl = fill_size * (exit_price - blended_entry) - entry_fee - exit_fee
```

### 6. Daily Cron Report Additions
```
Portfolio Stats:
• Total PnL: +$X (at realistic sizing)
• Total PnL (1-contract baseline): +$Y
• Avg fill rate: Z% across all trades
• Avg slippage: W¢

Per-Series Fill Quality:
• KXNBAMENTION: 95% avg fill, 0.3¢ avg slippage
• KXXRP: 22% avg fill, 2.1¢ avg slippage  ← would flag this as undeployable
• KXBTC15M: 78% avg fill, 0.8¢ avg slippage
```

## Key Design Decisions to Make

1. **Orderbook check is async REST call** — adds ~100-200ms latency to paper entry. Acceptable? Or snapshot in background and use last-known book state?

2. **Should partial fills create a trade?** e.g. wanted 50, book has 8 — record an 8-contract trade or skip entirely? Recommend: record partial, let the data tell you fill rates.

3. **Should the REST poller (30s fallback) also do orderbook checks?** It already hits REST — could combine the calls. WS path would need a separate async orderbook fetch.

4. **Backward compatibility** — existing 1,773 closed trades have no sizing data. Set all historical to intended_size=1, fill_size=1, fill_pct=1.0, slippage=0. Keep them as the baseline.

5. **Rate limits** — Kalshi REST has rate limits. Each orderbook check is 1 call. At peak you might fire 20+ signals in a burst (new hour, many contracts). Need to queue or rate-limit the orderbook checks.

## Files to Modify
- `edge_catcher/monitors/paper_trader_v2.py` — main changes
- `tests/test_event_backtest.py` — update Portfolio tests for sizing
- Daily PnL cron job payload (OpenClaw cron 24f910d2)

## Estimated Scope
~150-200 lines of modification + DB migration. No new files needed.
