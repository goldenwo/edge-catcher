"""FastAPI backend for Edge Catcher UI."""
from __future__ import annotations

import json
import logging
import os
import tempfile
import uuid
from pathlib import Path
from typing import Optional

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from api.auth import check_auth
from api.adapter_registry import get_adapter
from api.config_helpers import (
	validate_db as _validate_db,
	config_path as _config_path,
	markets_yaml as _markets_yaml,
	research_db_path as _research_db_path,
	load_merged_hypotheses,
)
from edge_catcher.ai.client import detect_active_provider as _detect_active_provider_full


def _detect_active_provider() -> str | None:
	"""Detect provider for API settings (excludes CLI auto-detection)."""
	return _detect_active_provider_full(include_cli=False)
from api.models import (
    AdapterDownloadRequest,
    AdapterDownloadStatus,
    AdapterInfo,
    AdapterKeyRequest,
    AIKeyRequest,
    AISettingsResponse,
    AnalyzeRequest,
    DownloadStatusResponse,
    FormalizeRequest,
    FormalizeResponse,
    HypothesisItem,
    InterpretRequest,
    InterpretResponse,
    ResultDetail,
    ResultSummary,
    StatusResponse,
    PipelineStatusResponse,
    StrategyInfo, StrategizeRequest, StrategizeResponse,
    StrategySaveRequest, StrategySaveResponse,
    BacktestRequest, BacktestStatusResponse, BacktestHistoryItem,
    FeeInfoResponse,
    ModelOption, ModelSettingsResponse, ModelOverrideRequest,
    ResearchLoopStartRequest, ReviewRejectRequest,
)
from api.tasks import download_state, get_adapter_state, backtest_states, get_backtest_state, is_backtest_running, BacktestTaskState, analyze_states, get_analyze_state, AnalyzeTaskState
from api.download_service import (
	run_kalshi_download as _run_kalshi_download,
	run_coinbase_download as _run_coinbase_download,
	run_legacy_download as _run_legacy_download,
	save_api_key as _save_api_key,
	clear_api_key as _clear_api_key,
)
from api.research_tasks import (
    ResearchLoopState, research_loop_state,
    get_research_loop_state, is_research_loop_running,
)

logger = logging.getLogger(__name__)

app = FastAPI(title="Edge Catcher API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── status ────────────────────────────────────────────────────────────────────

@app.get("/api/status", response_model=StatusResponse)
def get_status(_: None = Depends(check_auth)) -> StatusResponse:
    from edge_catcher.storage.db import get_connection, get_db_stats

    db = _validate_db("kalshi.db")
    if not db.exists():
        return StatusResponse(
            markets=0,
            trades=0,
            results=0,
            db_size_mb=0.0,
            last_download=download_state.last_run,
        )
    conn = get_connection(db)
    try:
        stats = get_db_stats(conn)
    finally:
        conn.close()
    return StatusResponse(**stats, last_download=download_state.last_run)


# ── download ──────────────────────────────────────────────────────────────────

@app.post("/api/download")
async def start_download(
    background_tasks: BackgroundTasks,
    _: None = Depends(check_auth),
) -> dict:
    if download_state.running:
        raise HTTPException(status_code=409, detail="Download already in progress")
    background_tasks.add_task(_run_legacy_download, download_state)
    return {"task_id": str(uuid.uuid4())}


@app.get("/api/download/status", response_model=DownloadStatusResponse)
async def get_download_status(_: None = Depends(check_auth)) -> DownloadStatusResponse:
    return DownloadStatusResponse(
        running=download_state.running,
        progress=download_state.progress,
        markets_fetched=download_state.markets_fetched,
        trades_fetched=download_state.trades_fetched,
    )


# ── hypotheses ────────────────────────────────────────────────────────────────

@app.get("/api/hypotheses", response_model=list[HypothesisItem])
def get_hypotheses(_: None = Depends(check_auth)) -> list[HypothesisItem]:
    from api.config_helpers import config_path as _cfg_path
    import yaml

    public_ids: set[str] = set()
    pub_file = _cfg_path() / "hypotheses.yaml"
    if pub_file.exists():
        with open(pub_file) as f:
            data = yaml.safe_load(f) or {}
        public_ids = set(data.get("hypotheses", {}).keys())

    merged = load_merged_hypotheses()
    return [
        HypothesisItem(
            id=hyp_id,
            name=cfg.get("name", hyp_id),
            market=cfg.get("market", "unknown"),
            status=cfg.get("status", "exploratory"),
            source="public" if hyp_id in public_ids else "local",
        )
        for hyp_id, cfg in merged.items()
    ]


@app.delete("/api/hypotheses/{hypothesis_id}")
def delete_hypothesis(
    hypothesis_id: str,
    _: None = Depends(check_auth),
) -> dict:
    import yaml

    local_file = Path("config.local") / "hypotheses.yaml"
    if not local_file.exists():
        raise HTTPException(status_code=404, detail="No local hypotheses file")
    with open(local_file) as f:
        data = yaml.safe_load(f) or {}
    hyps = data.get("hypotheses", {})
    if hypothesis_id not in hyps:
        raise HTTPException(
            status_code=400,
            detail=f"'{hypothesis_id}' is not in config.local — cannot delete public hypotheses",
        )
    del hyps[hypothesis_id]
    data["hypotheses"] = hyps
    with open(local_file, "w") as f:
        yaml.dump(data, f, default_flow_style=False, allow_unicode=True)
    return {"ok": True, "deleted": hypothesis_id}


def _run_analyze_task(task_id: str, hypothesis_id: str | None) -> None:
    """Background thread target for analysis."""
    from edge_catcher.runner.backtest import run_backtest

    state = analyze_states[task_id]
    state.running = True
    state.progress = "Running analysis..."
    try:
        result = run_backtest(
            hypothesis_id=hypothesis_id,
            db_path=_validate_db("kalshi.db"),
            config_path=_config_path(),
            output_path=None,
        )
        state.result = result
        state.progress = "Complete"
    except Exception as exc:
        state.error = str(exc)
        state.progress = "Failed"
    finally:
        state.running = False


@app.post("/api/analyze")
async def analyze(
    body: AnalyzeRequest,
    _: None = Depends(check_auth),
) -> dict:
    import threading

    task_id = str(uuid.uuid4())
    analyze_states[task_id] = AnalyzeTaskState(
        task_id=task_id,
        hypothesis_id=body.hypothesis_id,
    )
    threading.Thread(
        target=_run_analyze_task,
        args=(task_id, body.hypothesis_id),
        daemon=True,
    ).start()
    return {"task_id": task_id}


@app.get("/api/analyze/{task_id}/status")
async def analyze_status(task_id: str) -> dict:
    state = get_analyze_state(task_id)
    if not state:
        raise HTTPException(status_code=404, detail=f"Task {task_id!r} not found")
    return {
        "running": state.running,
        "progress": state.progress,
        "error": state.error,
    }


@app.get("/api/analyze/{task_id}/result")
async def analyze_result(task_id: str) -> dict:
    state = get_analyze_state(task_id)
    if not state:
        raise HTTPException(status_code=404, detail=f"Task {task_id!r} not found")
    if state.running:
        raise HTTPException(status_code=409, detail="Analysis still running")
    if state.error:
        raise HTTPException(status_code=500, detail=state.error)
    if not state.result:
        raise HTTPException(status_code=404, detail="No result available")
    return state.result


# ── results ───────────────────────────────────────────────────────────────────

@app.get("/api/results")
def get_results(
    limit: int = Query(25, ge=1, le=100),
    offset: int = Query(0, ge=0),
    hypothesis_id: Optional[str] = None,
    verdict: Optional[str] = None,
    _: None = Depends(check_auth),
) -> dict:
    from edge_catcher.storage.db import get_connection

    db = _validate_db("kalshi.db")
    if not db.exists():
        return {"results": [], "total": 0}
    conn = get_connection(db)
    try:
        exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='analysis_results'"
        ).fetchone()
        if not exists:
            return {"results": [], "total": 0}

        where_clauses = []
        params: list = []
        if hypothesis_id:
            where_clauses.append("hypothesis_id = ?")
            params.append(hypothesis_id)
        if verdict:
            where_clauses.append("verdict = ?")
            params.append(verdict)
        where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        total = conn.execute(
            f"SELECT COUNT(*) FROM analysis_results{where_sql}", params
        ).fetchone()[0]
        rows = conn.execute(
            f"""
            SELECT run_id, hypothesis_id, verdict, run_timestamp
            FROM analysis_results{where_sql}
            ORDER BY run_timestamp DESC
            LIMIT ? OFFSET ?
            """,
            params + [limit, offset],
        ).fetchall()
        return {
            "results": [ResultSummary(**dict(r)).model_dump() for r in rows],
            "total": total,
        }
    finally:
        conn.close()


@app.get("/api/results/hypothesis-ids")
def get_result_hypothesis_ids(_: None = Depends(check_auth)) -> list[str]:
    from edge_catcher.storage.db import get_connection

    db = _validate_db("kalshi.db")
    if not db.exists():
        return []
    conn = get_connection(db)
    try:
        exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='analysis_results'"
        ).fetchone()
        if not exists:
            return []
        rows = conn.execute(
            "SELECT DISTINCT hypothesis_id FROM analysis_results ORDER BY hypothesis_id"
        ).fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()


@app.get("/api/results/{run_id}", response_model=ResultDetail)
def get_result(
    run_id: str,
    _: None = Depends(check_auth),
) -> ResultDetail:
    from edge_catcher.storage.db import get_connection

    db = _validate_db("kalshi.db")
    if not db.exists():
        raise HTTPException(status_code=404, detail="Database not found")
    conn = get_connection(db)
    try:
        row = conn.execute(
            "SELECT * FROM analysis_results WHERE run_id = ?",
            (run_id,),
        ).fetchone()
    finally:
        conn.close()
    if not row:
        raise HTTPException(status_code=404, detail=f"Run {run_id!r} not found")

    d = dict(row)
    for field in ("raw_bucket_data", "warnings"):
        if d.get(field) and isinstance(d[field], str):
            try:
                d[field] = json.loads(d[field])
            except (json.JSONDecodeError, TypeError):
                pass
    return ResultDetail(**d)


@app.delete("/api/results/{run_id}")
def delete_result(
    run_id: str,
    _: None = Depends(check_auth),
) -> dict:
    from edge_catcher.storage.db import get_connection

    db = _validate_db("kalshi.db")
    if not db.exists():
        raise HTTPException(status_code=404, detail="Database not found")
    conn = get_connection(db)
    try:
        cur = conn.execute("DELETE FROM analysis_results WHERE run_id = ?", (run_id,))
        try:
            conn.execute("DELETE FROM hypothesis_runs WHERE run_id = ?", (run_id,))
        except Exception:
            pass  # table may not exist
        conn.commit()
    finally:
        conn.close()
    if cur.rowcount == 0:
        raise HTTPException(status_code=404, detail=f"Run {run_id!r} not found")
    return {"ok": True}


# ── AI ────────────────────────────────────────────────────────────────────────

@app.post("/api/formalize", response_model=FormalizeResponse)
def formalize_hypothesis(
    body: FormalizeRequest,
    _: None = Depends(check_auth),
) -> FormalizeResponse:
    try:
        from edge_catcher.ai.client import LLMClient
        from edge_catcher.ai.formalizer import formalize
    except ImportError:
        raise HTTPException(
            status_code=501,
            detail="AI deps missing. Run: pip install -e '.[ai]'",
        )

    try:
        model_override = os.getenv("EDGE_CATCHER_LLM_MODEL") or None
        client = LLMClient(provider=body.provider, model=model_override)
        result = formalize(body.description, client)
    except Exception as exc:
        return FormalizeResponse(message="", error=str(exc))

    if result.get("error"):
        return FormalizeResponse(
            message=result.get("raw_response", ""),
            error="Could not parse LLM response",
        )
    return FormalizeResponse(
        message=result["message"],
        error=None,
        hypothesis_id=result.get("hypothesis_id"),
    )


@app.post("/api/interpret", response_model=InterpretResponse)
def interpret_result(
    body: InterpretRequest,
    _: None = Depends(check_auth),
) -> InterpretResponse:
    try:
        from edge_catcher.ai.client import LLMClient
        from edge_catcher.ai.interpreter import interpret
    except ImportError:
        raise HTTPException(
            status_code=501,
            detail="AI deps missing. Run: pip install -e '.[ai]'",
        )

    db = _validate_db("kalshi.db")
    if not db.exists():
        raise HTTPException(status_code=404, detail="Database not found")

    from edge_catcher.storage.db import get_connection

    conn = get_connection(db)
    try:
        row = conn.execute(
            "SELECT * FROM analysis_results WHERE run_id = ?",
            (body.run_id,),
        ).fetchone()
    finally:
        conn.close()

    if not row:
        raise HTTPException(
            status_code=404, detail=f"Run {body.run_id!r} not found"
        )

    report_data = {body.run_id: dict(row)}
    temp_path: Optional[Path] = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump(report_data, f, default=str)
            temp_path = Path(f.name)
        model_override = os.getenv("EDGE_CATCHER_LLM_MODEL") or None
        client = LLMClient(provider=body.provider, model=model_override)
        summary = interpret(temp_path, client)
        return InterpretResponse(summary=summary)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        if temp_path and temp_path.exists():
            temp_path.unlink()


# ── adapter registry ─────────────────────────────────────────────────────────

@app.get("/adapters", response_model=list[AdapterInfo])
def list_adapters() -> list[AdapterInfo]:
    from api.pipeline_service import build_adapter_info_list
    return [AdapterInfo(**a) for a in build_adapter_info_list()]


@app.get("/adapters/{adapter_id}/status", response_model=AdapterDownloadStatus)
async def adapter_status(adapter_id: str) -> AdapterDownloadStatus:
    meta = get_adapter(adapter_id)
    if not meta:
        raise HTTPException(status_code=404, detail=f"Adapter {adapter_id!r} not found")
    state = get_adapter_state(adapter_id)
    return AdapterDownloadStatus(
        adapter_id=adapter_id,
        running=state.running,
        progress=state.progress,
        rows_fetched=state.rows_fetched,
        error=state.error,
    )


@app.put("/adapters/{adapter_id}/key", status_code=200)
async def save_adapter_key(
    adapter_id: str, req: AdapterKeyRequest, _=Depends(check_auth)
) -> dict:
    meta = get_adapter(adapter_id)
    if not meta:
        raise HTTPException(status_code=404, detail=f"Adapter {adapter_id!r} not found")
    if not meta.api_key_env_var:
        raise HTTPException(status_code=400, detail="Adapter has no API key")
    _save_api_key(meta.api_key_env_var, req.api_key)
    return {"ok": True}


@app.delete("/adapters/{adapter_id}/key", status_code=200)
async def clear_adapter_key(adapter_id: str, _=Depends(check_auth)) -> dict:
    meta = get_adapter(adapter_id)
    if not meta:
        raise HTTPException(status_code=404, detail=f"Adapter {adapter_id!r} not found")
    if not meta.api_key_env_var:
        raise HTTPException(status_code=400, detail="Adapter has no API key")
    _clear_api_key(meta.api_key_env_var)
    return {"ok": True}


@app.post("/adapters/{adapter_id}/download", status_code=202)
async def start_adapter_download(
    adapter_id: str,
    req: AdapterDownloadRequest,
    _=Depends(check_auth),
) -> dict:
    import threading

    meta = get_adapter(adapter_id)
    if not meta:
        raise HTTPException(status_code=404, detail=f"Adapter {adapter_id!r} not found")
    state = get_adapter_state(adapter_id)
    if state.running:
        raise HTTPException(status_code=409, detail="Download already in progress")
    if req.api_key and meta.api_key_env_var:
        _save_api_key(meta.api_key_env_var, req.api_key)
    if meta.markets_yaml:
        target = _run_kalshi_download
        args = (adapter_id, state, req.start_date, meta.markets_yaml, meta.db_file)
    elif meta.coinbase_product_id:
        target = _run_coinbase_download
        args = (adapter_id, state, req.start_date, meta.coinbase_product_id, meta.db_file)
    else:
        raise HTTPException(status_code=400, detail=f"No download handler for {adapter_id!r}")
    threading.Thread(target=target, args=args, daemon=True).start()
    return {"adapter_id": adapter_id, "task_id": str(uuid.uuid4())}


# ── AI settings ──────────────────────────────────────────────────────────────

_AI_PROVIDERS = {
    "anthropic": "ANTHROPIC_API_KEY",
    "openai": "OPENAI_API_KEY",
    "openrouter": "OPENROUTER_API_KEY",
}

_AI_MODEL_OPTIONS: dict[str, list[dict[str, str]]] = {
    "anthropic": [
        {"id": "claude-haiku-4-5-20251001", "label": "Claude Haiku 4.5 (fast, cheap)"},
        {"id": "claude-sonnet-4-20250514", "label": "Claude Sonnet 4 (balanced)"},
        {"id": "claude-opus-4-20250514", "label": "Claude Opus 4 (most capable)"},
    ],
    "openai": [
        {"id": "gpt-4o-mini", "label": "GPT-4o Mini (fast, cheap)"},
        {"id": "gpt-4o", "label": "GPT-4o (balanced)"},
        {"id": "o3-mini", "label": "o3-mini (reasoning)"},
    ],
    "openrouter": [
        {"id": "anthropic/claude-haiku-4-5-20251001", "label": "Claude Haiku 4.5"},
        {"id": "anthropic/claude-sonnet-4-20250514", "label": "Claude Sonnet 4"},
        {"id": "openai/gpt-4o", "label": "GPT-4o"},
    ],
}


@app.get("/api/settings/ai", response_model=AISettingsResponse)
async def get_ai_settings() -> AISettingsResponse:
    return AISettingsResponse(
        anthropic=bool(os.getenv("ANTHROPIC_API_KEY")),
        openai=bool(os.getenv("OPENAI_API_KEY")),
        openrouter=bool(os.getenv("OPENROUTER_API_KEY")),
    )


@app.post("/api/settings/ai")
async def save_ai_key(body: AIKeyRequest) -> dict:
    env_var = _AI_PROVIDERS.get(body.provider)
    if not env_var:
        raise HTTPException(status_code=400, detail=f"Unknown provider: {body.provider!r}")
    _save_api_key(env_var, body.api_key)
    return {"ok": True}


@app.get("/api/settings/ai/models", response_model=ModelSettingsResponse)
async def get_ai_models() -> ModelSettingsResponse:
    provider = _detect_active_provider()
    current = os.getenv("EDGE_CATCHER_LLM_MODEL") or None
    options = _AI_MODEL_OPTIONS.get(provider or "", [])
    return ModelSettingsResponse(
        provider=provider,
        current_model=current,
        models=[ModelOption(**m) for m in options],
    )


@app.post("/api/settings/ai/model")
async def save_ai_model(body: ModelOverrideRequest) -> dict:
    if body.model:
        provider = _detect_active_provider()
        valid_ids = {m["id"] for m in _AI_MODEL_OPTIONS.get(provider or "", [])}
        if body.model not in valid_ids:
            raise HTTPException(
                status_code=400,
                detail=f"Model {body.model!r} is not available for provider {provider!r}",
            )
        _save_api_key("EDGE_CATCHER_LLM_MODEL", body.model)
    else:
        _clear_api_key("EDGE_CATCHER_LLM_MODEL")
    return {"ok": True}


# ── pipeline status ───────────────────────────────────────────────────────────

@app.get("/api/pipeline/status", response_model=PipelineStatusResponse)
def pipeline_status(_: None = Depends(check_auth)) -> PipelineStatusResponse:
    from api.pipeline_service import get_pipeline_status
    return get_pipeline_status()


# ── series list ───────────────────────────────────────────────────────────────

@app.get("/api/series")
def get_series(_: None = Depends(check_auth)) -> list[str]:
    from api.adapter_registry import ADAPTERS
    from edge_catcher.storage.db import get_connection

    all_series: set[str] = set()
    seen_dbs: set[str] = set()
    for adapter in ADAPTERS:
        db_path = Path(adapter.db_file)
        db_key = str(db_path)
        if db_key in seen_dbs or not db_path.exists():
            continue
        seen_dbs.add(db_key)
        try:
            conn = get_connection(db_path)
            try:
                rows = conn.execute("SELECT DISTINCT series_ticker FROM markets ORDER BY series_ticker").fetchall()
                all_series.update(r[0] for r in rows if r[0])
            finally:
                conn.close()
        except Exception:
            continue
    return sorted(all_series)


@app.get("/api/series/{series}/fee-info", response_model=FeeInfoResponse)
def series_fee_info(series: str, _: None = Depends(check_auth)) -> FeeInfoResponse:
    from api.adapter_registry import get_fee_model_for_db, resolve_db_for_series
    db = resolve_db_for_series(series)
    if db is None:
        raise HTTPException(status_code=404, detail=f"Series '{series}' not found")
    fee_model = get_fee_model_for_db(str(db), series)
    return FeeInfoResponse(
        id=fee_model.id,
        name=fee_model.name,
        description=fee_model.description,
        formula=fee_model.formula,
    )


# ── strategies ────────────────────────────────────────────────────────────────

@app.get("/api/strategies", response_model=list[StrategyInfo])
def get_strategies(_: None = Depends(check_auth)) -> list[StrategyInfo]:
    from edge_catcher.runner.strategy_parser import list_strategies, STRATEGIES_PUBLIC_PATH, STRATEGIES_LOCAL_PATH
    pub = list_strategies(file_path=STRATEGIES_PUBLIC_PATH)
    local = list_strategies(file_path=STRATEGIES_LOCAL_PATH)
    return [StrategyInfo(**s) for s in pub + local]


# ── strategize (AI) ───────────────────────────────────────────────────────────

@app.post("/api/strategize", response_model=StrategizeResponse)
def strategize_endpoint(
    body: StrategizeRequest,
    _: None = Depends(check_auth),
) -> StrategizeResponse:
    try:
        from edge_catcher.ai.client import LLMClient
        from edge_catcher.ai.strategizer import strategize
    except ImportError:
        raise HTTPException(status_code=501, detail="AI deps missing. Run: pip install -e '.[ai]'")

    model_override = os.getenv("EDGE_CATCHER_LLM_MODEL") or None
    client = LLMClient(provider=body.provider, model=model_override)
    result = strategize(body.hypothesis_id, body.run_id, client, _validate_db("kalshi.db"), _config_path())
    return StrategizeResponse(**result)


# ── strategy save ─────────────────────────────────────────────────────────────

@app.post("/api/strategies/save", response_model=StrategySaveResponse)
def save_strategy_endpoint(
    body: StrategySaveRequest,
    _: None = Depends(check_auth),
) -> StrategySaveResponse:
    from edge_catcher.runner.strategy_parser import save_strategy, STRATEGIES_LOCAL_PATH
    result = save_strategy(
        body.code,
        body.strategy_name,
        STRATEGIES_LOCAL_PATH,
    )
    return StrategySaveResponse(**result)


# ── backtest ──────────────────────────────────────────────────────────────────

@app.post("/api/backtest")
async def start_backtest(
    body: BacktestRequest,
    _: None = Depends(check_auth),
) -> dict:
    import threading

    if is_backtest_running():
        raise HTTPException(status_code=409, detail="A backtest is already running")

    from api.backtest_service import run_backtest_task
    task_id = str(uuid.uuid4())
    backtest_states[task_id] = BacktestTaskState(task_id=task_id)
    threading.Thread(target=run_backtest_task, args=(task_id, body), daemon=True).start()
    return {"task_id": task_id}


@app.post("/api/backtest/{task_id}/stop")
async def stop_backtest(task_id: str, _: None = Depends(check_auth)) -> dict:
    state = get_backtest_state(task_id)
    if state is None:
        raise HTTPException(status_code=404, detail="Task not found")
    if not state.running:
        raise HTTPException(status_code=409, detail="Task is not running")
    state.cancel_requested = True
    state.progress = "Stopping..."
    return {"ok": True}


@app.get("/api/backtest/history")
def backtest_history(
    limit: int = Query(25, ge=1, le=100),
    offset: int = Query(0, ge=0),
    _: None = Depends(check_auth),
) -> dict:
    from api.backtest_service import query_backtest_history
    db = _validate_db("kalshi.db")
    if not db.exists():
        return {"results": [], "total": 0}
    rows, total = query_backtest_history(db, limit=limit, offset=offset)
    return {
        "results": [BacktestHistoryItem(**r).model_dump() for r in rows],
        "total": total,
    }


@app.delete("/api/backtest/history/{task_id}")
def delete_backtest(
    task_id: str,
    _: None = Depends(check_auth),
) -> dict:
    from edge_catcher.storage.db import get_connection

    db = _validate_db("kalshi.db")
    if not db.exists():
        raise HTTPException(status_code=404, detail="Database not found")
    conn = get_connection(db)
    try:
        cur = conn.execute("DELETE FROM backtest_results WHERE task_id = ?", (task_id,))
        conn.commit()
    finally:
        conn.close()
    if cur.rowcount == 0:
        raise HTTPException(status_code=404, detail=f"Backtest {task_id!r} not found")
    return {"ok": True}


@app.get("/api/backtest/active")
async def backtest_active() -> dict:
    """Return the task_id of the currently running backtest, if any."""
    for tid, state in backtest_states.items():
        if state.running:
            return {"task_id": tid}
    return {"task_id": None}


@app.get("/api/backtest/{task_id}/status", response_model=BacktestStatusResponse)
async def backtest_status(task_id: str) -> BacktestStatusResponse:
    state = get_backtest_state(task_id)
    if not state:
        raise HTTPException(status_code=404, detail=f"Backtest {task_id!r} not found")
    return BacktestStatusResponse(
        running=state.running,
        progress=state.progress,
        error=state.error,
        trades_processed=state.trades_processed if state.trades_estimated else None,
        trades_estimated=state.trades_estimated or None,
        net_pnl_cents=int(state.net_pnl_cents) if state.trades_estimated else None,
    )


@app.get("/api/backtest/{task_id}/result")
async def backtest_result(task_id: str) -> dict:
    state = get_backtest_state(task_id)
    if not state:
        raise HTTPException(status_code=404, detail=f"Backtest {task_id!r} not found")
    if state.running:
        raise HTTPException(status_code=409, detail="Backtest still running")
    if state.error:
        raise HTTPException(status_code=500, detail=state.error)
    if not state.result:
        raise HTTPException(status_code=404, detail="No result available")
    return state.result


# ── Research Dashboard ───────────────────────────────────────────────────────


def _run_research_loop(task_id: str, body: ResearchLoopStartRequest) -> None:
    """Background thread: runs the research loop."""
    import threading
    import time as _time
    from edge_catcher.research.loop import LoopOrchestrator

    state = research_loop_state[task_id]
    state.running = True
    state.phase = "starting"
    state.runs_total = body.max_runs

    cancel_event = threading.Event()
    start = _time.monotonic()

    def on_progress(phase: str, completed: int, total: int) -> None:
        state.phase = phase
        state.runs_completed = completed
        state.runs_total = total
        state.elapsed_seconds = _time.monotonic() - start
        if state.cancel_requested:
            cancel_event.set()

    try:
        orch = LoopOrchestrator(
            research_db=str(_research_db_path()),
            max_runs=body.max_runs,
            max_time_minutes=float(body.max_time),
            parallel=body.parallel,
            fee_pct=body.fee_pct if body.fee_pct is not None else 1.0,
            max_llm_calls=body.max_llm_calls if body.max_llm_calls is not None else 10,
            grid_only=(body.mode == "grid_only"),
            llm_only=(body.mode == "llm_only"),
            refine_only=(body.mode == "refine_only"),
            start_date=body.start,
            end_date=body.end,
            force=body.force,
            cancel_event=cancel_event,
            on_progress=on_progress,
        )
        exit_code, results = orch.run()
        state.phase = "completed" if exit_code == 0 else "stopped"
    except Exception as exc:
        state.error = str(exc)
        state.phase = "error"
    finally:
        state.running = False
        state.elapsed_seconds = _time.monotonic() - start


@app.get("/api/research/profiles")
def research_profiles(_: None = Depends(check_auth)):
    """Return series profiles from the Context Engine."""
    from edge_catcher.research.context_engine import ContextEngine
    import dataclasses

    data_dir = Path(os.getenv("DATA_DIR", "data"))
    db_paths = [str(p) for p in sorted(data_dir.glob("*.db")) if p.name != "research.db"]

    engine = ContextEngine(data_dir=str(data_dir))
    profiles = engine.profile_all(db_paths)

    return {
        "profiles": [dataclasses.asdict(p) for p in profiles],
        "count": len(profiles),
    }


# ── research loop control ────────────────────────────────────────────────────

@app.post("/api/research/loop/start")
def research_loop_start(
    body: ResearchLoopStartRequest,
    _: None = Depends(check_auth),
) -> dict:
    import threading

    if is_research_loop_running():
        raise HTTPException(status_code=409, detail="A research loop is already running")
    # Clear completed/errored state entries so status returns to idle
    stale = [k for k, v in research_loop_state.items() if not v.running]
    for k in stale:
        del research_loop_state[k]
    task_id = str(uuid.uuid4())
    research_loop_state[task_id] = ResearchLoopState(task_id=task_id)
    threading.Thread(target=_run_research_loop, args=(task_id, body), daemon=True).start()
    return {"task_id": task_id}


@app.post("/api/research/loop/stop")
def research_loop_stop(_: None = Depends(check_auth)) -> dict:
    for state in research_loop_state.values():
        if state.running:
            state.cancel_requested = True
            return {"ok": True}
    raise HTTPException(status_code=409, detail="No research loop is running")


@app.get("/api/research/loop/status")
def research_loop_status_endpoint(_: None = Depends(check_auth)) -> dict:
    for state in research_loop_state.values():
        if state.running or state.phase != "idle":
            return {
                "running": state.running,
                "phase": state.phase,
                "runs_completed": state.runs_completed,
                "runs_total": state.runs_total,
                "elapsed_seconds": state.elapsed_seconds,
                "task_id": state.task_id,
                "error": state.error,
            }
    return {
        "running": False, "phase": "idle",
        "runs_completed": 0, "runs_total": 0,
        "elapsed_seconds": 0, "task_id": None, "error": None,
    }


@app.get("/api/research/review-queue")
def research_review_queue(_: None = Depends(check_auth)):
    """Return strategies with promote/review verdicts for human review."""
    from edge_catcher.research.tracker import Tracker

    research_db = str(_research_db_path())
    if not Path(research_db).exists():
        return {"strategies": [], "count": 0}

    tracker = Tracker(research_db)
    results = tracker.list_results()

    review_queue = [
        r for r in results
        if r.get("verdict") in ("promote", "review")
    ]

    # Sort by Sharpe descending
    review_queue.sort(key=lambda r: r.get("sharpe", 0), reverse=True)

    for r in review_queue:
        if r.get("validation_details") and isinstance(r["validation_details"], str):
            try:
                r["validation_details"] = json.loads(r["validation_details"])
            except (json.JSONDecodeError, TypeError):
                pass

    return {
        "strategies": review_queue,
        "count": len(review_queue),
    }


# ── research results ─────────────────────────────────────────────────────────

@app.get("/api/research/results")
def research_results(
    limit: int = 50, offset: int = 0, sort: str = "completed_at",
    verdict: str | None = None,
    _: None = Depends(check_auth),
) -> dict:
    from edge_catcher.research.tracker import Tracker
    research_db = str(_research_db_path())
    if not Path(research_db).exists():
        return {"results": [], "total": 0}
    tracker = Tracker(research_db)
    results = tracker.list_results(limit=limit, offset=offset, sort=sort, verdict=verdict)
    counts = tracker.count_by_verdict()
    total = sum(counts.values())
    for r in results:
        if r.get("validation_details") and isinstance(r["validation_details"], str):
            try:
                r["validation_details"] = json.loads(r["validation_details"])
            except (json.JSONDecodeError, TypeError):
                pass
    return {"results": results, "total": total}


@app.get("/api/research/verdict-counts")
def research_verdict_counts(_: None = Depends(check_auth)) -> dict:
    from edge_catcher.research.tracker import Tracker
    research_db = str(_research_db_path())
    if not Path(research_db).exists():
        return {"promote": 0, "review": 0, "explore": 0, "kill": 0}
    tracker = Tracker(research_db)
    counts = tracker.count_by_verdict()
    return {
        "promote": counts.get("promote", 0),
        "review": counts.get("review", 0),
        "explore": counts.get("explore", 0),
        "kill": counts.get("kill", 0),
    }


# ── research review actions ──────────────────────────────────────────────────

@app.post("/api/research/review/{hypothesis_id}/approve")
def research_review_approve(
    hypothesis_id: str,
    _: None = Depends(check_auth),
) -> dict:
    from edge_catcher.research.tracker import Tracker
    research_db = str(_research_db_path())
    if not Path(research_db).exists():
        raise HTTPException(status_code=404, detail="Research database not found")
    tracker = Tracker(research_db)
    result = tracker.get_result_by_id(hypothesis_id)
    if not result:
        raise HTTPException(status_code=404, detail=f"Hypothesis {hypothesis_id!r} not found")
    if result.get("verdict") == "accepted":
        return {"ok": True}
    tracker.update_verdict(hypothesis_id, "accepted")
    return {"ok": True}


@app.post("/api/research/review/{hypothesis_id}/reject")
def research_review_reject(
    hypothesis_id: str,
    body: ReviewRejectRequest,
    _: None = Depends(check_auth),
) -> dict:
    from edge_catcher.research.tracker import Tracker
    research_db = str(_research_db_path())
    if not Path(research_db).exists():
        raise HTTPException(status_code=404, detail="Research database not found")
    tracker = Tracker(research_db)
    result = tracker.get_result_by_id(hypothesis_id)
    if not result:
        raise HTTPException(status_code=404, detail=f"Hypothesis {hypothesis_id!r} not found")
    if result.get("verdict") == "kill":
        return {"ok": True}
    tracker.reject_and_update_kill_registry(
        hypothesis_id,
        body.reason or "Manually rejected from dashboard",
    )
    return {"ok": True}


# ── research audit ───────────────────────────────────────────────────────────

@app.get("/api/research/audit/executions")
def research_audit_executions(
    limit: int = 100,
    _: None = Depends(check_auth),
) -> list:
    from edge_catcher.research.audit import AuditLog
    research_db = str(_research_db_path())
    if not Path(research_db).exists():
        return []
    audit = AuditLog(research_db)
    return audit.list_executions(limit=limit)


@app.get("/api/research/audit/decisions")
def research_audit_decisions(
    limit: int = 100,
    _: None = Depends(check_auth),
) -> list:
    from edge_catcher.research.audit import AuditLog
    research_db = str(_research_db_path())
    if not Path(research_db).exists():
        return []
    audit = AuditLog(research_db)
    return audit.list_decisions(limit=limit)


# ── storage management ──────────────────────────────────────────────────────

@app.get("/api/storage/report")
def storage_report(_: None = Depends(check_auth)) -> dict:
    from edge_catcher.storage.archiver import get_size_report

    archive_dir = Path("data/archive")
    dbs = {}
    for db_name in ("kalshi.db", "research.db"):
        db_path = Path("data") / db_name
        if db_path.exists():
            dbs[db_name] = get_size_report(db_path, archive_dir)
        else:
            dbs[db_name] = {"db_size_mb": 0, "archive_size_mb": 0, "total_mb": 0}

    # Row counts
    counts: dict = {}
    kalshi_db = Path("data/kalshi.db")
    if kalshi_db.exists():
        from edge_catcher.storage.db import get_connection
        conn = get_connection(kalshi_db)
        try:
            for table in ("trades", "markets"):
                try:
                    counts[table] = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                except Exception:
                    counts[table] = 0
        finally:
            conn.close()

    return {"databases": dbs, "row_counts": counts}


@app.post("/api/storage/archive")
def storage_archive(
    days: int = Query(90, ge=1),
    _: None = Depends(check_auth),
) -> dict:
    from edge_catcher.storage.db import get_connection
    from edge_catcher.storage.archiver import archive_old_trades, archive_old_markets

    archive_dir = Path("data/archive")
    kalshi_db = _validate_db("kalshi.db")
    if not kalshi_db.exists():
        raise HTTPException(status_code=404, detail="Database not found")

    conn = get_connection(kalshi_db)
    try:
        results = {
            "trades": archive_old_trades(conn, archive_dir, days),
            "markets": archive_old_markets(conn, archive_dir, days),
        }
    finally:
        conn.close()
    return results


@app.post("/api/storage/vacuum")
def storage_vacuum(_: None = Depends(check_auth)) -> dict:
    from edge_catcher.storage.db import get_connection
    from edge_catcher.storage.archiver import vacuum_db, get_size_report

    kalshi_db = _validate_db("kalshi.db")
    if not kalshi_db.exists():
        raise HTTPException(status_code=404, detail="Database not found")

    before = get_size_report(kalshi_db)
    conn = get_connection(kalshi_db)
    try:
        vacuum_db(conn)
    finally:
        conn.close()
    after = get_size_report(kalshi_db)
    return {
        "before_mb": before["db_size_mb"],
        "after_mb": after["db_size_mb"],
        "saved_mb": round(before["db_size_mb"] - after["db_size_mb"], 4),
    }


# ── static UI (production) ────────────────────────────────────────────────────

_ui_dist = Path(__file__).parent.parent / "ui" / "dist"
if _ui_dist.exists():
    # Serve static assets directly; fall back to index.html for SPA routes
    app.mount("/assets", StaticFiles(directory=str(_ui_dist / "assets")), name="assets")

    @app.get("/{full_path:path}", include_in_schema=False)
    async def serve_spa(full_path: str) -> FileResponse:
        candidate = _ui_dist / full_path
        if candidate.exists() and candidate.is_file():
            return FileResponse(str(candidate))
        return FileResponse(str(_ui_dist / "index.html"))
