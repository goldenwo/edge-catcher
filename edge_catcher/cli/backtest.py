"""Backtest CLI command — event-driven backtest on historical trade data."""

import sys
from pathlib import Path


def build_strategy_map():
	"""Build the strategy name → class mapping. Returns (strategy_map, has_local)."""
	import importlib
	from edge_catcher.runner.strategy_parser import (
		STRATEGIES_PUBLIC_MODULE, STRATEGIES_LOCAL_MODULE, STRATEGIES_LOCAL_PATH,
	)

	strategy_map: dict = {}

	# Auto-discover public strategies
	pub_mod = importlib.import_module(STRATEGIES_PUBLIC_MODULE)
	for attr_name in dir(pub_mod):
		obj = getattr(pub_mod, attr_name)
		if isinstance(obj, type) and hasattr(obj, 'on_trade'):
			name_attr = getattr(obj, 'name', None)
			if isinstance(name_attr, str):
				strategy_map[name_attr] = obj

	# Auto-discover local strategies (override public if same name)
	local_mod = None
	if STRATEGIES_LOCAL_PATH.exists():
		try:
			local_mod = importlib.import_module(STRATEGIES_LOCAL_MODULE)
			for attr_name in dir(local_mod):
				obj = getattr(local_mod, attr_name)
				if isinstance(obj, type) and hasattr(obj, 'on_trade'):
					name_attr = getattr(obj, 'name', None)
					if isinstance(name_attr, str):
						strategy_map[name_attr] = obj
		except ImportError:
			pass

	return strategy_map, local_mod is not None


def _auto_strategy_args(parser) -> None:
	"""Auto-generate CLI args from strategy __init__ signatures.

	Inspects all discovered strategies and creates --param-name args
	for each unique parameter. No manual arg definitions needed.
	"""
	import inspect

	strategy_map, _ = build_strategy_map()

	# Params that are internal or handled specially (not CLI-configurable)
	SKIP = {'self', 'size', 'btc_closes', 'ohlc_provider'}

	seen: dict[str, tuple[type, object]] = {}
	for cls in strategy_map.values():
		sig = inspect.signature(cls.__init__)
		for name, param in sig.parameters.items():
			if name in SKIP or name in seen:
				continue
			if param.kind in (inspect.Parameter.VAR_KEYWORD, inspect.Parameter.VAR_POSITIONAL):
				continue
			default = param.default if param.default != inspect.Parameter.empty else None
			param_type = type(default) if default is not None else int
			seen[name] = (param_type, default)

	for name, (ptype, default) in sorted(seen.items()):
		cli_flag = f'--{name.replace("_", "-")}'
		help_text = f'(default: {default})' if default is not None else None
		try:
			parser.add_argument(cli_flag, type=ptype, default=None,
			                    dest=name, help=help_text)
		except Exception:
			pass  # Already defined (e.g., by framework args)


def run(args) -> None:
	import json
	from datetime import date

	json_mode = getattr(args, 'json', False)
	ohlc_provider = None

	# --- --list-strategies: output unique strategy names and exit ---
	if getattr(args, 'list_strategies', False):
		strategy_map, _ = build_strategy_map()
		# Deduplicate: keep first name per class (preserves logical ordering)
		seen_classes: set = set()
		unique_names: list = []
		for name, cls in strategy_map.items():
			if cls not in seen_classes:
				seen_classes.add(cls)
				unique_names.append(name)
		print(json.dumps({"strategies": sorted(unique_names)}))
		return

	# --- --list-series: query DB for distinct series and exit ---
	if getattr(args, 'list_series', False):
		import sqlite3
		db_path = args.db_path
		try:
			conn = sqlite3.connect(db_path)
			rows = conn.execute(
				"SELECT DISTINCT series_ticker FROM markets ORDER BY series_ticker"
			).fetchall()
			total = conn.execute("SELECT COUNT(*) FROM markets").fetchone()[0]
			conn.close()
			series = [r[0] for r in rows]
			print(json.dumps({"series": series, "db_path": db_path, "total_markets": total}))
		except Exception as exc:
			print(json.dumps({"status": "error", "message": str(exc)}))
			sys.exit(1)
		return

	strategy_map, _has_local = build_strategy_map()

	try:
		if not args.series:
			msg = "--series is required for backtest (e.g. --series TICKER)"
			if json_mode:
				print(json.dumps({"status": "error", "message": msg}))
			else:
				print(f"error: {msg}", file=sys.stderr)
			sys.exit(1)

		strategy_names = [s.strip() for s in args.strategy.split(',')]

		import inspect

		strategies = []
		for name in strategy_names:
			cls = strategy_map.get(name)
			if cls is None:
				msg = f"Unknown strategy: {name}. Available: {', '.join(sorted(strategy_map))}"
				if json_mode:
					print(json.dumps({"status": "error", "message": msg}))
				else:
					print(msg, file=sys.stderr)
				sys.exit(1)

			# Build kwargs via introspection — only pass params the class accepts
			sig = inspect.signature(cls.__init__)
			available: dict = {}
			for param_name, param in sig.parameters.items():
				if param_name == 'self' or param.kind in (
					inspect.Parameter.VAR_KEYWORD, inspect.Parameter.VAR_POSITIONAL,
				):
					continue
				val = getattr(args, param_name, None)
				if val is not None:
					available[param_name] = val

			strategies.append(cls(**available))

		# Inject OHLC provider if --ohlc-config is provided
		ohlc_provider = None
		if getattr(args, 'ohlc_config', None):
			from edge_catcher.research.ohlc_provider import OHLCProvider
			ohlc_map = json.loads(args.ohlc_config)
			ohlc_provider = OHLCProvider({
				asset: (paths[0], paths[1]) for asset, paths in ohlc_map.items()
			})
			for s in strategies:
				s.ohlc = ohlc_provider

		start = date.fromisoformat(args.start) if args.start else None
		end = date.fromisoformat(args.end) if args.end else None

		from edge_catcher.runner.event_backtest import EventBacktester
		from api.adapter_registry import get_fee_model_for_db
		_base_model = get_fee_model_for_db(args.db_path, args.series)
		_fee_pct = args.fee_pct
		fee_fn = lambda p, s: _fee_pct * _base_model.calculate(p, s)
		backtester = EventBacktester()
		result = backtester.run(
			series=args.series,
			strategies=strategies,
			start=start,
			end=end,
			initial_cash=args.cash,
			slippage_cents=args.slippage,
			db_path=Path(args.db_path),
			fee_fn=fee_fn,
		)

		output_path = Path(args.output)
		output_path.parent.mkdir(parents=True, exist_ok=True)
		with open(output_path, 'w') as f:
			json.dump(result.to_dict(), f, indent=2)

		if json_mode:
			payload = result.to_dict()
			payload['status'] = 'ok'
			print(json.dumps(payload))
		else:
			print(result.summary())
			print(f"\nJSON saved to {args.output}")

	except SystemExit:
		raise
	except Exception as exc:
		if json_mode:
			print(json.dumps({"status": "error", "message": str(exc)}))
			sys.exit(1)
		raise
	finally:
		if ohlc_provider is not None:
			ohlc_provider.close()


def register(subparsers) -> None:
	from edge_catcher.reports import BACKTEST_OUTPUT

	bt = subparsers.add_parser("backtest", help="Run event-driven backtest on historical trade data")
	bt.add_argument("--series", default=None, help="Series ticker (e.g. TICKER)")
	bt.add_argument("--strategy", default="example", help="Comma-separated strategy names (use --list-strategies)")
	bt.add_argument("--start", default=None, help="Start date ISO format (e.g. 2025-06-01)")
	bt.add_argument("--end", default=None, help="End date ISO format (e.g. 2026-03-30)")
	bt.add_argument("--cash", type=float, default=10000.0, help="Initial capital (default: 10000)")
	bt.add_argument("--slippage", type=int, default=1, help="Slippage in cents (default: 1)")
	# Strategy-specific params auto-generated from __init__ signatures
	_auto_strategy_args(bt)
	bt.add_argument("--db-path", default="data/kalshi.db", dest="db_path")
	bt.add_argument("--output", default=str(BACKTEST_OUTPUT))
	bt.add_argument("--fee-pct", type=float, default=1.0, dest="fee_pct",
	                help="Multiplier on entry fee formula (default: 1.0 = full taker fee; 0.25 = maker fee; 0.0 = no fee)")
	bt.add_argument("--json", action="store_true", default=False,
	                help="Output only valid JSON to stdout; progress goes to stderr")
	bt.add_argument("--list-strategies", action="store_true", default=False, dest="list_strategies",
	                help="Print available strategy names as JSON and exit")
	bt.add_argument("--list-series", action="store_true", default=False, dest="list_series",
	                help="Print distinct series_ticker values from the DB as JSON and exit")
	bt.add_argument("--btc-db", default="data/btc.db", dest="btc_db",
	                help="Path to BTC OHLC database (default: data/btc.db)")
	bt.add_argument("--altcoin-ohlc-db", default="data/ohlc.db", dest="altcoin_ohlc_db",
	                help="Path to altcoin OHLC database (default: data/ohlc.db)")
	bt.add_argument("--ohlc-config", default=None, dest="ohlc_config",
	                help='JSON mapping asset names to [db_path, table] pairs '
	                     '(e.g. \'{"btc": ["data/ohlc.db", "btc_ohlc"]}\')')
	bt.set_defaults(func=run)
