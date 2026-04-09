"""Pipeline status service for Edge Catcher API.

Aggregates status from DB, config, and strategy files.
Extracted from api/main.py.
"""
from __future__ import annotations

from pathlib import Path

from api.config_helpers import validate_db, load_merged_hypotheses
from api.models import (
	PipelineStatusResponse, PipelineDataStatus, PipelineHypothesesStatus,
	PipelineAnalysisStatus, PipelineStrategiesStatus, PipelineBacktestStatus,
)


def get_pipeline_status() -> PipelineStatusResponse:
	"""Aggregate pipeline status from DB, config, and strategy files."""
	from edge_catcher.runner.strategy_parser import (
		list_strategies, STRATEGIES_LOCAL_PATH, STRATEGIES_PUBLIC_PATH,
	)

	db = validate_db("kalshi.db")

	# Data + Analysis + Backtest — single DB connection
	data_status = PipelineDataStatus(has_data=False, markets=0, trades=0)
	analysis_count = 0
	latest_verdict = None
	bt_count = 0
	latest_sharpe = None
	if db.exists():
		from edge_catcher.storage.db import get_connection
		conn = get_connection(db)
		try:
			m = conn.execute("SELECT COUNT(*) FROM markets").fetchone()[0]
			t = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
			data_status = PipelineDataStatus(has_data=t > 0, markets=m, trades=t)

			analysis_count = conn.execute("SELECT COUNT(*) FROM analysis_results").fetchone()[0]
			row = conn.execute(
				"SELECT verdict FROM analysis_results ORDER BY run_timestamp DESC LIMIT 1"
			).fetchone()
			if row:
				latest_verdict = row["verdict"]

			bt_exists = conn.execute(
				"SELECT name FROM sqlite_master WHERE type='table' AND name='backtest_results'"
			).fetchone()
			if bt_exists:
				bt_count = conn.execute("SELECT COUNT(*) FROM backtest_results").fetchone()[0]
				bt_row = conn.execute(
					"SELECT sharpe FROM backtest_results ORDER BY run_timestamp DESC LIMIT 1"
				).fetchone()
				if bt_row:
					latest_sharpe = bt_row["sharpe"]
		finally:
			conn.close()

	# Hypotheses — merge config/ and config.local/, using dict to deduplicate
	hyp_count = len(load_merged_hypotheses())

	# Strategies
	strats = list_strategies(file_path=STRATEGIES_LOCAL_PATH)
	pub_strats = list_strategies(file_path=STRATEGIES_PUBLIC_PATH)
	all_strats = pub_strats + strats

	return PipelineStatusResponse(
		data=data_status,
		hypotheses=PipelineHypothesesStatus(count=hyp_count),
		analysis=PipelineAnalysisStatus(count=analysis_count, latest_verdict=latest_verdict),
		strategies=PipelineStrategiesStatus(count=len(all_strats), names=[s["name"] for s in all_strats]),
		backtest=PipelineBacktestStatus(count=bt_count, latest_sharpe=latest_sharpe),
	)


def build_adapter_info_list() -> list[dict]:
	"""Build adapter info list with download status, DB sizes, etc."""
	from api.adapter_registry import ADAPTERS, is_api_key_set
	from api.tasks import get_adapter_state, save_adapter_history
	from api.download_service import adapter_has_data

	result = []
	for meta in ADAPTERS:
		state = get_adapter_state(meta.id)
		# Seed history from existing DB data if no recorded download
		if not state.last_run and not state.running and adapter_has_data(meta):
			state.last_run = "detected"
			state.progress = "Previously downloaded"
			save_adapter_history(meta.id, state.last_run)
		if state.running:
			dl_status = "running"
		elif state.error:
			dl_status = "error"
		elif state.last_run:
			dl_status = "complete"
		else:
			dl_status = "idle"
		db_file = Path(meta.db_file)
		db_size_mb = round(db_file.stat().st_size / (1024 * 1024), 1) if db_file.exists() else None
		result.append(dict(
			id=meta.id,
			name=meta.name,
			description=meta.description,
			requires_api_key=meta.requires_api_key,
			api_key_env_var=meta.api_key_env_var,
			api_key_set=is_api_key_set(meta),
			download_status=dl_status,
			default_start_date=meta.default_start_date,
			db_size_mb=db_size_mb,
		))
	return result
