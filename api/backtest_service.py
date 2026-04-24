"""Backtest service: background task logic and history queries."""
from __future__ import annotations

import logging

from api.models import BacktestRequest
from api.tasks import backtest_states

logger = logging.getLogger(__name__)


def run_backtest_task(task_id: str, body: BacktestRequest) -> None:
	"""Background task: load strategies, run EventBacktester, persist results."""
	import importlib
	import inspect
	import json
	from datetime import date

	from edge_catcher.runner.event_backtest import EventBacktester
	from edge_catcher.runner.strategy_parser import (
		STRATEGIES_PUBLIC_MODULE, STRATEGIES_LOCAL_MODULE, STRATEGIES_LOCAL_PATH,
	)
	from api.adapter_registry import get_fee_model_for_db

	state = backtest_states[task_id]
	state.running = True
	state.progress = "Loading strategies..."

	try:
		# Resolve which DB contains the requested series
		from api.adapter_registry import resolve_db_for_series
		db_path = resolve_db_for_series(body.series)
		if db_path is None:
			state.error = f"Series '{body.series}' not found in any database"
			state.running = False
			return

		# Build strategy map from public + local strategies
		strategy_map: dict[str, type] = {}

		# Import public strategies
		pub_mod = importlib.import_module(STRATEGIES_PUBLIC_MODULE)
		for attr_name in dir(pub_mod):
			obj = getattr(pub_mod, attr_name)
			if isinstance(obj, type) and hasattr(obj, 'name') and hasattr(obj, 'on_trade'):
				if hasattr(obj, 'name') and isinstance(getattr(obj, 'name', None), str):
					strategy_map[obj.name] = obj

		# Import local strategies (if file exists)
		if STRATEGIES_LOCAL_PATH.exists():
			try:
				local_mod = importlib.import_module(STRATEGIES_LOCAL_MODULE)
				importlib.reload(local_mod)  # Pick up recent saves
				for attr_name in dir(local_mod):
					obj = getattr(local_mod, attr_name)
					if isinstance(obj, type) and hasattr(obj, 'on_trade'):
						name_attr = getattr(obj, 'name', None)
						if isinstance(name_attr, str):
							strategy_map[name_attr] = obj
			except Exception as e:
				logger.warning("Failed to import strategies_local: %s", e)

		# Instantiate requested strategies
		strategies = []
		optional_kwargs = {}
		if body.tp is not None:
			optional_kwargs['take_profit'] = body.tp
		if body.sl is not None:
			optional_kwargs['stop_loss'] = body.sl
		if body.min_price is not None:
			optional_kwargs['min_price'] = body.min_price
		if body.max_price is not None:
			optional_kwargs['max_price'] = body.max_price

		for name in body.strategies:
			cls = strategy_map.get(name)
			if cls is None:
				state.error = f"Unknown strategy: {name}. Available: {list(strategy_map.keys())}"
				state.running = False
				return
			# Filter kwargs to only those the class accepts
			sig = inspect.signature(cls.__init__)
			valid_kwargs = {k: v for k, v in optional_kwargs.items() if k in sig.parameters}
			strategies.append(cls(**valid_kwargs))

		state.progress = f"Running backtest on {body.series}..."

		start = date.fromisoformat(body.start) if body.start else None
		end = date.fromisoformat(body.end) if body.end else None

		def on_progress(info: dict) -> None:
			if state.cancel_requested:
				return
			state.trades_processed = info["trades_processed"]
			state.trades_estimated = info["trades_estimated"]
			state.net_pnl_cents = int(info["net_pnl_cents"])
			pct = (
				info["trades_processed"] / info["trades_estimated"] * 100
				if info["trades_estimated"]
				else 0
			)
			state.progress = (
				f"Processed {info['trades_processed']:,} / ~{info['trades_estimated']:,} trades "
				f"({pct:.0f}%) \u2014 P&L: {info['net_pnl_cents']:+}\u00a2"
			)

		fee_model = get_fee_model_for_db(str(db_path), body.series)

		backtester = EventBacktester()
		result = backtester.run(
			series=body.series,
			strategies=strategies,
			start=start,
			end=end,
			initial_cash=body.cash,
			slippage_cents=body.slippage,
			db_path=db_path,
			fee_fn=fee_model.calculate,
			on_progress=on_progress,
			is_cancelled=lambda: state.cancel_requested,
		)

		if state.cancel_requested:
			state.error = "Backtest stopped by user"
			return

		result_dict = result.to_dict()
		state.result = result_dict

		# Save to JSON file
		from edge_catcher.reports import BACKTEST_DIR
		result_path = BACKTEST_DIR / f"backtest_{task_id}.json"
		result_path.parent.mkdir(parents=True, exist_ok=True)
		with open(result_path, "w") as f:
			json.dump(result_dict, f, indent=2, default=str)

		# Persist to research.db via Tracker
		from edge_catcher.research.tracker import Tracker
		from api.config_helpers import research_db_path as _research_db_path
		tracker = Tracker(str(_research_db_path()))
		tracker.save_ui_backtest(
			task_id=task_id,
			series=body.series,
			strategies=json.dumps(body.strategies),
			db_path=str(db_path),
			start_date=body.start,
			end_date=body.end,
			total_trades=result_dict["total_trades"],
			wins=result_dict["wins"],
			losses=result_dict["losses"],
			net_pnl_cents=result_dict["net_pnl_cents"],
			sharpe=result_dict["sharpe"],
			max_drawdown_pct=result_dict["max_drawdown_pct"],
			win_rate=result_dict["win_rate"],
			result_path=str(result_path),
			hypothesis_id=body.hypothesis_id,
		)

		state.progress = "Complete"
	except Exception as e:
		logger.error("Backtest failed: %s", e)
		state.error = str(e)
		state.progress = "Error"
	finally:
		state.running = False


def query_backtest_history(limit: int = 25, offset: int = 0) -> tuple[list[dict], int]:
	"""Query UI backtest history from research.db via Tracker."""
	import json
	from edge_catcher.research.tracker import Tracker
	from api.config_helpers import research_db_path as _research_db_path

	tracker = Tracker(str(_research_db_path()))
	rows, total = tracker.list_ui_backtests(limit=limit, offset=offset)
	results = [
		dict(
			task_id=r["task_id"],
			series=r["series"],
			strategies=json.loads(r["strategies"]) if isinstance(r["strategies"], str) else r["strategies"],
			hypothesis_id=r.get("hypothesis_id"),
			timestamp=r["run_timestamp"],
			total_trades=r["total_trades"] or 0,
			net_pnl_cents=int(r["net_pnl_cents"] or 0),
			sharpe=r["sharpe"] or 0.0,
			win_rate=r["win_rate"] or 0.0,
		)
		for r in rows
	]
	return results, total
