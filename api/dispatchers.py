"""Exchange download dispatch registries.

Mirrors the AdapterMeta pattern: each exchange registers its download
thread-target builder and its data-presence checker here, and the call
sites in api/main.py + api/download_service.py route through these
registries instead of using if/elif on the exchange string.

Two registries exist because the call sites have different shapes:

- DOWNLOAD_DISPATCHERS: (adapter_id, meta, req, state) -> (target, args)
  Builds the (thread-target, argv) tuple consumed by
  threading.Thread(target=..., args=...).

- DATA_CHECK_DISPATCHERS: (meta, conn) -> bool
  Returns True if the adapter's DB actually contains data for this
  exchange. Callers own the connection lifecycle.

Adding a new exchange:
    from api.download_service import polymarket_has_data
    from api.main import _polymarket_download_target  # or equivalent
    DOWNLOAD_DISPATCHERS["polymarket"] = _polymarket_download_target
    DATA_CHECK_DISPATCHERS["polymarket"] = polymarket_has_data
"""
from __future__ import annotations

from typing import Any, Callable

# Handler signatures
DownloadDispatcher = Callable[[str, Any, Any, Any], tuple[Callable[..., Any], tuple]]
DataCheckDispatcher = Callable[[Any, Any], bool]

DOWNLOAD_DISPATCHERS: dict[str, DownloadDispatcher] = {}
DATA_CHECK_DISPATCHERS: dict[str, DataCheckDispatcher] = {}


def dispatch_download(adapter_id: str, meta: Any, req: Any, state: Any) -> tuple[Callable[..., Any], tuple]:
	"""Look up the download thread-target builder for meta.exchange."""
	handler = DOWNLOAD_DISPATCHERS.get(meta.exchange)
	if handler is None:
		raise ValueError(f"No download dispatcher for exchange {meta.exchange!r}")
	return handler(adapter_id, meta, req, state)


def dispatch_data_check(meta: Any, conn: Any) -> bool:
	"""Look up the data-presence checker for meta.exchange.

	Returns False if the exchange is not registered (unknown exchange ==
	no data), matching the fall-through behavior of the original
	adapter_has_data() if/elif.
	"""
	handler = DATA_CHECK_DISPATCHERS.get(meta.exchange)
	if handler is None:
		return False
	return handler(meta, conn)


def _register_builtins() -> None:
	"""Register built-in handlers.

	Imported lazily at module bottom. Call sites must import this module
	at function scope (not module scope) to avoid circular imports — the
	api.main and api.download_service modules transitively depend on each
	other during startup.
	"""
	from api.download_service import _kalshi_has_data, _coinbase_has_data, _polymarket_has_data
	from api.main import _kalshi_download_target, _coinbase_download_target, _polymarket_download_target

	DOWNLOAD_DISPATCHERS["kalshi"] = _kalshi_download_target
	DOWNLOAD_DISPATCHERS["coinbase"] = _coinbase_download_target
	DOWNLOAD_DISPATCHERS["polymarket"] = _polymarket_download_target
	DATA_CHECK_DISPATCHERS["kalshi"] = _kalshi_has_data
	DATA_CHECK_DISPATCHERS["coinbase"] = _coinbase_has_data
	DATA_CHECK_DISPATCHERS["polymarket"] = _polymarket_has_data


_register_builtins()
