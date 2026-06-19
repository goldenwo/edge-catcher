"""Config-gated construction of a shared OHLCProvider for the live + replay engine.

This is the SINGLE seam that decides whether on_tick strategies get external
spot data at runtime. It mirrors the proven offline path in ``cli/backtest.py``
(``OHLCProvider({asset: (db, table)})`` then ``for s in strategies: s.ohlc = provider``),
but is config-gated and default-OFF: a config with no ``ohlc`` block (or
``enabled: false``) returns ``None`` so ``strategy.ohlc`` is left untouched. That
default-OFF behaviour is the parity guarantee — the G-parity bundles carry no
``ohlc`` block, so the wiring is a no-op on the parity path.
"""
from __future__ import annotations

from edge_catcher.research.ohlc_provider import OHLCProvider


def build_ohlc_provider(config: dict | None) -> OHLCProvider | None:
	"""Return one OHLCProvider from ``config['ohlc']``, or None when absent/disabled.

	Config shape: ``ohlc: {enabled: bool, assets: {asset: [db_path, table]}}``.

	Returns ``None`` (no provider built) when the config is absent, has no
	``ohlc`` block, has ``enabled`` falsy, or carries no assets. Otherwise builds
	an :class:`OHLCProvider` with the same ``{asset: (db_path, table)}`` mapping
	the offline backtest CLI uses. The caller owns the returned provider's
	lifecycle and must ``.close()`` it.
	"""
	block = (config or {}).get("ohlc") or {}
	if not block.get("enabled"):
		return None
	assets = block.get("assets") or {}
	if not assets:
		return None
	return OHLCProvider({a: (paths[0], paths[1]) for a, paths in assets.items()})
