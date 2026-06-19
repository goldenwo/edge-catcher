"""`edge_catcher ohlc-refresh --config <yaml>` — run the OHLC refresher loop."""
from __future__ import annotations

import yaml

from edge_catcher.live.ohlc_refresher import RefreshConfig, run_refresher


def _load_config(path: str) -> RefreshConfig | None:
	with open(path, encoding="utf-8") as f:
		return RefreshConfig.from_yaml(yaml.safe_load(f) or {})


def _run_ohlc_refresh(args) -> None:
	cfg = _load_config(args.config)
	if cfg is None:
		print("ohlc_refresh: disabled or missing in config; nothing to do")
		return
	run_refresher(cfg)


def register(subparsers) -> None:
	p = subparsers.add_parser("ohlc-refresh", help="Run the Coinbase OHLC refresher loop")
	p.add_argument("--config", required=True)
	p.set_defaults(func=_run_ohlc_refresh)
