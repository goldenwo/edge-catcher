# ADR 0001: Adapter Registry Architecture

**Date:** 2026-04-16
**Status:** Accepted
**Supersedes:** N/A
**Superseded by:** N/A

## Context

Edge Catcher started with a Kalshi-only adapter and grew to include Coinbase. The original registry design baked Kalshi assumptions into the core (`_db_file_from_markets_yaml` prefixed "kalshi", `coinbase_product_id` polluted the generic `AdapterMeta`, single monolithic `ADAPTERS` list). As the framework is meant to be reusable across prediction markets and other exchanges, the registry became a blocker to portability.

## Decision

Adopt a per-exchange directory layout under `edge_catcher/adapters/<exchange>/` with `adapter.py`, `registry.py`, and optional `fees.py`. A thin central aggregator at `api/adapter_registry.py` imports and concatenates per-exchange `<EXCHANGE>_ADAPTERS` lists.

`AdapterMeta` lives in `edge_catcher/adapters/base.py`, requires `exchange`, `db_file`, and `fee_model` explicitly, and provides a generic `extra: dict[str, Any]` bag for exchange-specific data. No magic defaults, no implicit path derivation, no typed exchange-specific fields on the base dataclass.

## Consequences

### Positive
- Adding a new exchange: create subdir + add 2 lines to aggregator.
- Adapter metadata co-located with adapter implementation.
- No Kalshi-specific logic in core code.
- Type-safe required fields; missing `exchange`/`db_file`/`fee_model` raises at construction.

### Negative
- `extra: dict[str, Any]` escape hatch loses static type safety for exchange-specific fields. Acceptable at current scale (13 adapters, 2 exchanges). Revisit at 5+ exchanges with 3+ specialized fields each.
- Renaming `kalshi` → `kalshi_btc` is a one-time breaking change for any stored adapter-id references.

### Neutral
- One extra directory layer of indirection.
- Fees for new exchanges live in `adapters/<exchange>/fees.py`, not a central file. Generic fee types stay in `edge_catcher/fees.py`.

## Adapter implementation contract

The `PredictionMarketAdapter` ABC in `edge_catcher/adapters/base.py` is a Kalshi-shaped contract: `collect_markets` + `collect_trades` + `validate_response`. Prediction markets (Kalshi, Polymarket, etc.) should inherit from it. Exchanges with a different shape — Coinbase does OHLC candles, not markets+trades — intentionally do NOT inherit; they duck-type their own interface. The `AdapterMeta` entry in the registry still tags them with `exchange=`, which is what the download dispatch in `api/main.py` switches on.

## Contract for future adapter additions

See `CLAUDE.md` → "Adding a new exchange" for the step-by-step.
