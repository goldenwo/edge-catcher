# Adapter Guide

This guide walks through adding a new exchange to edge-catcher. The
plug-in shape is intentionally small: drop a directory, register a
metadata list, wire two dispatch handlers, done.

For the design rationale see
[ADR 0001 — Adapter Registry Architecture](adr/0001-adapter-registry.md).

## When to inherit from `PredictionMarketAdapter`

The `PredictionMarketAdapter` ABC in
`edge_catcher/adapters/base.py` describes a Kalshi-shaped contract:
markets + per-market trade history + a `validate_response` hook. If
your exchange has the same shape (Polymarket, Manifold, etc.) inherit
from it.

If your exchange has a different shape — Coinbase, for instance, exposes
OHLC candles and has no notion of per-market trade tape — duck-type
freely. The `AdapterMeta` registry only cares about the metadata and
the `exchange` tag; the `api/dispatchers.py` registry cares about the
download + data-check callables. Coinbase is the in-tree precedent for
this style.

## Steps

### 1. Create the adapter directory

```
edge_catcher/adapters/<exchange>/
├── __init__.py     # empty or re-exports
├── adapter.py      # collector class
├── registry.py     # AdapterMeta entries
└── fees.py         # optional, only if your exchange has specialized fees
```

### 2. Implement `adapter.py`

For prediction markets:

```python
# edge_catcher/adapters/myexchange/adapter.py
from typing import List, Optional
from edge_catcher.adapters.base import PredictionMarketAdapter
from edge_catcher.storage.models import Market, Trade


class MyExchangeAdapter(PredictionMarketAdapter):
    def __init__(self, db_path: str, api_key: Optional[str] = None) -> None:
        self.db_path = db_path
        self.api_key = api_key

    def collect_markets(self, series_tickers: Optional[List[str]] = None) -> List[Market]:
        # Hit the exchange API, return Market objects.
        ...

    def collect_trades(self, ticker: str, since: Optional[str] = None) -> List[Trade]:
        # Page through trade history for `ticker` since the optional cursor.
        ...

    def validate_response(self, data: dict, schema_key: str) -> bool:
        # Schema check; raise ValueError on failure.
        ...
```

For OHLC-style exchanges, skip the ABC:

```python
class MyOhlcAdapter:
    def __init__(self, db_path: str, product_id: str) -> None: ...
    def collect_candles(self, since: Optional[int] = None) -> int: ...
```

### 3. Add `fees.py` if needed

Most exchanges fit one of the existing fee shapes — flat, formula, or
zero. If yours does, import from `edge_catcher.fees`:

```python
from edge_catcher.fees import ZERO_FEE  # commission-free
```

If your exchange has its own fee structure, define it in
`fees.py` next to the adapter:

```python
# edge_catcher/adapters/myexchange/fees.py
from edge_catcher.fees import FeeModel

MYEX_STANDARD_FEE = FeeModel(
    maker_formula="0.01 * P * (1 - P)",
    taker_formula="0.05 * P * (1 - P)",
)
```

### 4. Register `AdapterMeta` entries

```python
# edge_catcher/adapters/myexchange/registry.py
from edge_catcher.adapters.base import AdapterMeta
from edge_catcher.adapters.myexchange.fees import MYEX_STANDARD_FEE


MYEXCHANGE_ADAPTERS: list[AdapterMeta] = [
    AdapterMeta(
        id="myex_main",
        exchange="myexchange",
        name="MyExchange Main",
        description="Settled contracts on the main MyExchange book.",
        db_file="data/myex-main.db",
        fee_model=MYEX_STANDARD_FEE,
        requires_api_key=False,
        markets_yaml="config/markets-myex-main.yaml",
    ),
]
```

`exchange`, `db_file`, and `fee_model` are required. Anything
exchange-specific (product IDs, region codes, asset symbols) goes in
the optional `extra: dict[str, Any]` bag — see
[ADR 0001](adr/0001-adapter-registry.md) for the rationale.

### 5. Wire into `api/adapter_registry.py`

```python
# api/adapter_registry.py
from edge_catcher.adapters.myexchange.registry import MYEXCHANGE_ADAPTERS

ADAPTERS: list[AdapterMeta] = [
    *KALSHI_ADAPTERS,
    *COINBASE_ADAPTERS,
    *MYEXCHANGE_ADAPTERS,
]
```

That single concat is the only edit required to `api/adapter_registry.py`.

### 6. Register dispatch handlers in `api/dispatchers.py`

The download + data-check call sites in `api/main.py` and
`api/download_service.py` look up your handlers by
`meta.exchange`. Register them inside `_register_builtins()`:

```python
# api/dispatchers.py (inside _register_builtins())
from api.download_service import _myexchange_has_data
from api.main import _myexchange_download_target

DOWNLOAD_DISPATCHERS["myexchange"] = _myexchange_download_target
DATA_CHECK_DISPATCHERS["myexchange"] = _myexchange_has_data
```

`_myexchange_download_target(adapter_id, meta, req, state)` returns a
`(target_callable, args_tuple)` pair that
`threading.Thread(target=..., args=...)` consumes. `_myexchange_has_data(meta, conn)`
returns `True` if the DB contains any rows that "count as data" for
your exchange (used by the UI to gray out empty data sources).

Use `_kalshi_download_target` / `_kalshi_has_data` as the closest
reference for prediction-market shape, and `_coinbase_download_target`
/ `_coinbase_has_data` for OHLC shape.

### 7. Run the tests

```bash
pytest tests/ -v
```

If you add framework-level behavior (new `AdapterMeta` fields, dispatch
helper changes), add corresponding tests under `tests/`. Keep
exchange-specific integration tests against live endpoints out of the
public tree — they belong in your local `scripts/` or a private fork.

## Minimal end-to-end Kalshi example

To see all of this on a real exchange, the in-tree Kalshi adapter is
the reference:

- `edge_catcher/adapters/kalshi/adapter.py` — `KalshiAdapter`
  inheriting `PredictionMarketAdapter`
- `edge_catcher/adapters/kalshi/fees.py` — `STANDARD_FEE`,
  `INDEX_FEE`
- `edge_catcher/adapters/kalshi/registry.py` — `KALSHI_ADAPTERS`
  with one `AdapterMeta` per Kalshi category
- `api/adapter_registry.py` — `*KALSHI_ADAPTERS` concatenated into
  the central `ADAPTERS` list
- `api/dispatchers.py` — `_kalshi_download_target` and
  `_kalshi_has_data` registered under the `"kalshi"` key

The Coinbase OHLC adapter is the reference for the duck-typed,
non-`PredictionMarketAdapter` style.

## Further reading

- [ADR 0001 — Adapter Registry Architecture](adr/0001-adapter-registry.md):
  why the per-exchange directory layout, why `extra: dict[str, Any]`
  instead of typed exchange-specific fields, what the breaking-change
  considerations look like.
- `CONTRIBUTING.md` for the PR process when contributing a new adapter
  upstream.
