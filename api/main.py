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

from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from api.auth import check_auth
from api.adapter_registry import ADAPTERS, get_adapter, is_api_key_set
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
    PipelineStatusResponse, PipelineDataStatus, PipelineHypothesesStatus,
    PipelineAnalysisStatus, PipelineStrategiesStatus, PipelineBacktestStatus,
    StrategyInfo, StrategizeRequest, StrategizeResponse,
    StrategySaveRequest, StrategySaveResponse,
    BacktestRequest, BacktestStatusResponse, BacktestHistoryItem,
)
from api.tasks import download_state, get_adapter_state, save_adapter_history, backtest_states, get_backtest_state, is_backtest_running, BacktestTaskState

logger = logging.getLogger(__name__)

app = FastAPI(title="Edge Catcher API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── path helpers ──────────────────────────────────────────────────────────────

def _db_path() -> Path:
    return Path(os.getenv("DB_PATH", "data/kalshi.db"))


def _config_path() -> Path:
    """Return config path for hypotheses. Always use config/ for shared config like markets.yaml."""
    explicit = os.getenv("CONFIG_PATH")
    if explicit:
        return Path(explicit)
    return Path("config")


def _markets_yaml() -> Path:
    """Always return config/markets.yaml — not overridable locally."""
    return Path("config") / "markets.yaml"


# ── status ────────────────────────────────────────────────────────────────────

@app.get("/api/status", response_model=StatusResponse)
async def get_status(_: None = Depends(check_auth)) -> StatusResponse:
    from edge_catcher.storage.db import get_connection, get_db_stats

    db = _db_path()
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

def _run_download() -> None:
    """Background task: download markets then trades."""
    from datetime import datetime, timezone

    from edge_catcher.adapters.kalshi import KalshiAdapter
    from edge_catcher.storage.db import (
        get_connection,
        get_markets_by_series,
        init_db,
        upsert_market,
        upsert_trades_batch,
    )

    db = _db_path()
    cfg = _config_path()

    download_state.running = True
    download_state.progress = "Initializing..."
    download_state.markets_fetched = 0
    download_state.trades_fetched = 0
    download_state.error = None

    try:
        init_db(db)
        adapter = KalshiAdapter(config_path=_markets_yaml())
        conn = get_connection(db)
        try:
            # Phase 1: markets
            for _series, page_markets in adapter.iter_market_pages():
                for m in page_markets:
                    upsert_market(conn, m)
                conn.commit()
                download_state.markets_fetched += len(page_markets)
                download_state.progress = (
                    f"Markets: {download_state.markets_fetched} fetched"
                )

            # Phase 2: trades (skip tickers already in DB)
            existing_tickers = {
                r[0] for r in conn.execute("SELECT DISTINCT ticker FROM trades")
            }
            markets_with_vol: list = []
            for series in adapter.get_configured_series():
                for m in get_markets_by_series(conn, series):
                    if (m.volume is None or (m.volume or 0) > 0) and m.ticker not in existing_tickers:
                        markets_with_vol.append(m)
            markets_with_vol.sort(key=lambda m: m.volume or 0, reverse=True)

            total = len(markets_with_vol)
            for i, market in enumerate(markets_with_vol, 1):
                download_state.progress = f"Trades: {i}/{total} markets"
                trades = adapter.collect_trades(market.ticker)
                if trades:
                    upsert_trades_batch(conn, trades)
                    conn.commit()
                    download_state.trades_fetched += len(trades)
        finally:
            conn.close()

        download_state.last_run = datetime.now(timezone.utc).isoformat()
        download_state.progress = "Complete"
    except Exception as exc:
        logger.error("Download failed: %s", exc)
        download_state.error = str(exc)
        download_state.progress = f"Error: {exc}"
    finally:
        download_state.running = False


@app.post("/api/download")
async def start_download(
    background_tasks: BackgroundTasks,
    _: None = Depends(check_auth),
) -> dict:
    if download_state.running:
        raise HTTPException(status_code=409, detail="Download already in progress")
    background_tasks.add_task(_run_download)
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
async def get_hypotheses(_: None = Depends(check_auth)) -> list[HypothesisItem]:
    import yaml

    # Merge hypotheses from config/ and config.local/ (local overrides public)
    merged: dict = {}
    for cfg_dir in [_config_path(), Path("config.local")]:
        cfg_file = cfg_dir / "hypotheses.yaml"
        if cfg_file.exists():
            with open(cfg_file) as f:
                data = yaml.safe_load(f) or {}
            merged.update(data.get("hypotheses", {}))

    return [
        HypothesisItem(
            id=hyp_id,
            name=cfg.get("name", hyp_id),
            market=cfg.get("market", "unknown"),
            status=cfg.get("status", "exploratory"),
        )
        for hyp_id, cfg in merged.items()
    ]


@app.post("/api/analyze")
async def analyze(
    body: AnalyzeRequest,
    _: None = Depends(check_auth),
) -> dict:
    from edge_catcher.runner.backtest import run_backtest

    try:
        return run_backtest(
            hypothesis_id=body.hypothesis_id,
            db_path=_db_path(),
            config_path=_config_path(),
            output_path=Path("reports/latest_analysis.json"),
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ── results ───────────────────────────────────────────────────────────────────

@app.get("/api/results", response_model=list[ResultSummary])
async def get_results(_: None = Depends(check_auth)) -> list[ResultSummary]:
    from edge_catcher.storage.db import get_connection

    db = _db_path()
    if not db.exists():
        return []
    conn = get_connection(db)
    try:
        rows = conn.execute(
            """
            SELECT run_id, hypothesis_id, verdict, run_timestamp
            FROM analysis_results
            WHERE run_id IN (
                SELECT run_id FROM (
                    SELECT run_id, ROW_NUMBER() OVER (
                        PARTITION BY hypothesis_id ORDER BY run_timestamp DESC
                    ) AS rn
                    FROM analysis_results
                )
                WHERE rn = 1
            )
            ORDER BY run_timestamp DESC
            LIMIT 100
            """
        ).fetchall()
        return [ResultSummary(**dict(r)) for r in rows]
    finally:
        conn.close()


@app.get("/api/results/{run_id}", response_model=ResultDetail)
async def get_result(
    run_id: str,
    _: None = Depends(check_auth),
) -> ResultDetail:
    from edge_catcher.storage.db import get_connection

    db = _db_path()
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


# ── AI ────────────────────────────────────────────────────────────────────────

@app.post("/api/formalize", response_model=FormalizeResponse)
async def formalize_hypothesis(
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

    cfg_file = _config_path() / "hypotheses.yaml"
    try:
        client = LLMClient(provider=body.provider)
        result = formalize(body.description, client, config_path=cfg_file)
    except Exception as exc:
        return FormalizeResponse(message="", error=str(exc))

    if result.get("error"):
        return FormalizeResponse(
            message=result.get("raw_response", ""),
            error="Could not parse LLM response",
        )
    return FormalizeResponse(message=result["message"], error=None)


@app.post("/api/interpret", response_model=InterpretResponse)
async def interpret_result(
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

    db = _db_path()
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
        client = LLMClient(provider=body.provider)
        summary = interpret(temp_path, client)
        return InterpretResponse(summary=summary)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        if temp_path and temp_path.exists():
            temp_path.unlink()


# ── adapter registry ─────────────────────────────────────────────────────────

def _save_api_key(env_var: str, value: str) -> None:
    """Append or update KEY=value in .env file."""
    env_path = Path(".env")
    lines = env_path.read_text().splitlines() if env_path.exists() else []
    updated = False
    for i, line in enumerate(lines):
        if line.startswith(f"{env_var}="):
            lines[i] = f"{env_var}={value}"
            updated = True
            break
    if not updated:
        lines.append(f"{env_var}={value}")
    env_path.write_text("\n".join(lines) + "\n")
    os.environ[env_var] = value  # also update current process


def _clear_api_key(env_var: str) -> None:
    """Remove KEY=value from .env file and unset from current process."""
    env_path = Path(".env")
    if env_path.exists():
        lines = [l for l in env_path.read_text().splitlines() if not l.startswith(f"{env_var}=")]
        env_path.write_text("\n".join(lines) + "\n" if lines else "")
    os.environ.pop(env_var, None)


def _run_coinbase_download(adapter_id: str, state, start_date: str | None = None) -> None:
    from datetime import datetime, timezone

    from edge_catcher.adapters.coinbase import CoinbaseAdapter
    from edge_catcher.storage.db import get_connection, init_btc_ohlc_table

    state.running = True
    state.progress = "Initializing..."
    state.error = None
    state.rows_fetched = 0
    try:
        db_path = Path(os.getenv("BTC_DB_PATH", "data/btc.db"))
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = get_connection(db_path)
        init_btc_ohlc_table(conn)
        adapter = CoinbaseAdapter()
        if start_date:
            start_dt = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)
        else:
            start_dt = datetime(2025, 3, 21, tzinfo=timezone.utc)
        start_ts = int(start_dt.timestamp())
        end_ts = int(datetime.now(timezone.utc).timestamp())

        def _on_progress(pct, page, total_pages, rows):
            state.progress = f"{pct}% — {rows:,} candles ({page}/{total_pages} pages)"

        n = adapter.download_range(start_ts, end_ts, conn, progress_callback=_on_progress)
        conn.close()
        state.rows_fetched = n
        state.progress = f"Complete — {n:,} new candles"
        state.last_run = datetime.now(timezone.utc).isoformat()
        save_adapter_history(adapter_id, state.last_run)
    except Exception as e:
        state.error = str(e)
        state.progress = "Error"
    finally:
        state.running = False


def _run_kalshi_adapter_download(
    adapter_id: str, state, start_date: str | None = None, markets_yaml: str | None = None
) -> None:
    """Run Kalshi download, updating the per-adapter state."""
    from datetime import datetime, timezone

    from edge_catcher.adapters.kalshi import KalshiAdapter
    from edge_catcher.storage.db import (
        get_connection,
        get_markets_by_series,
        init_db,
        upsert_market,
        upsert_trades_batch,
    )

    db = _db_path()

    state.running = True
    state.progress = "Initializing..."
    state.rows_fetched = 0
    state.error = None

    try:
        init_db(db)
        config_file = Path(markets_yaml) if markets_yaml else _markets_yaml()
        adapter = KalshiAdapter(config_path=config_file)
        conn = get_connection(db)
        try:
            markets_count = 0
            for _series, page_markets in adapter.iter_market_pages():
                for m in page_markets:
                    upsert_market(conn, m)
                conn.commit()
                markets_count += len(page_markets)
                state.progress = f"Markets: {markets_count} fetched"

            existing_tickers = {
                r[0] for r in conn.execute("SELECT DISTINCT ticker FROM trades")
            }
            markets_with_vol: list = []
            for series in adapter.get_configured_series():
                for m in get_markets_by_series(conn, series):
                    if (m.volume is None or (m.volume or 0) > 0) and m.ticker not in existing_tickers:
                        markets_with_vol.append(m)
            markets_with_vol.sort(key=lambda m: m.volume or 0, reverse=True)

            total = len(markets_with_vol)
            trades_count = 0
            for i, market in enumerate(markets_with_vol, 1):
                state.progress = f"Trades: {i}/{total} markets"
                trades = adapter.collect_trades(market.ticker, since=start_date)
                if trades:
                    upsert_trades_batch(conn, trades)
                    conn.commit()
                    trades_count += len(trades)
            state.rows_fetched = trades_count
        finally:
            conn.close()

        state.last_run = datetime.now(timezone.utc).isoformat()
        state.progress = "Complete"
        save_adapter_history(adapter_id, state.last_run)
    except Exception as exc:
        logger.error("Kalshi adapter download failed: %s", exc)
        state.error = str(exc)
        state.progress = "Error"
    finally:
        state.running = False


def _adapter_has_data(meta) -> bool:
    """Check whether an adapter's DB actually contains data for it."""
    import sqlite3, yaml
    db_file = Path(meta.db_file)
    if not db_file.exists():
        return False
    try:
        conn = sqlite3.connect(str(db_file), timeout=5)
        if meta.markets_yaml:
            cfg = yaml.safe_load(Path(meta.markets_yaml).read_text())
            series = cfg.get("adapters", {}).get("kalshi", {}).get("series", [])
            if not series:
                conn.close()
                return False
            placeholders = ",".join("?" for _ in series)
            count = conn.execute(
                f"SELECT COUNT(*) FROM markets WHERE series_ticker IN ({placeholders})", series
            ).fetchone()[0]
            conn.close()
            return count > 0
        else:
            # coinbase_btc — check btc_ohlc table
            count = conn.execute("SELECT COUNT(*) FROM btc_ohlc").fetchone()[0]
            conn.close()
            return count > 0
    except Exception:
        return False


@app.get("/adapters", response_model=list[AdapterInfo])
async def list_adapters() -> list[AdapterInfo]:
    result = []
    for meta in ADAPTERS:
        state = get_adapter_state(meta.id)
        # Seed history from existing DB data if no recorded download
        if not state.last_run and not state.running and _adapter_has_data(meta):
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
        result.append(
            AdapterInfo(
                id=meta.id,
                name=meta.name,
                description=meta.description,
                requires_api_key=meta.requires_api_key,
                api_key_env_var=meta.api_key_env_var,
                api_key_set=is_api_key_set(meta),
                download_status=dl_status,
                default_start_date=meta.default_start_date,
                db_size_mb=db_size_mb,
            )
        )
    return result


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
        target = _run_kalshi_adapter_download
        args = (adapter_id, state, req.start_date, meta.markets_yaml)
    elif adapter_id == "coinbase_btc":
        target, args = _run_coinbase_download, (adapter_id, state, req.start_date)
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


# ── pipeline status ───────────────────────────────────────────────────────────

@app.get("/api/pipeline/status", response_model=PipelineStatusResponse)
async def pipeline_status(_: None = Depends(check_auth)) -> PipelineStatusResponse:
    import yaml as _yaml
    from edge_catcher.runner.strategy_parser import list_strategies

    db = _db_path()

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
    merged_hyps: dict = {}
    for cfg_dir in [_config_path(), Path("config.local")]:
        cfg_file = cfg_dir / "hypotheses.yaml"
        if cfg_file.exists():
            with open(cfg_file) as f:
                data = _yaml.safe_load(f) or {}
            merged_hyps.update(data.get("hypotheses", {}))
    hyp_count = len(merged_hyps)

    # Strategies
    strategies_path = Path("edge_catcher/runner/strategies_local.py")
    strats = list_strategies(file_path=strategies_path)
    pub_strats = list_strategies(file_path=Path("edge_catcher/runner/strategies.py"))
    all_strats = pub_strats + strats

    return PipelineStatusResponse(
        data=data_status,
        hypotheses=PipelineHypothesesStatus(count=hyp_count),
        analysis=PipelineAnalysisStatus(count=analysis_count, latest_verdict=latest_verdict),
        strategies=PipelineStrategiesStatus(count=len(all_strats), names=[s["name"] for s in all_strats]),
        backtest=PipelineBacktestStatus(count=bt_count, latest_sharpe=latest_sharpe),
    )


# ── series list ───────────────────────────────────────────────────────────────

@app.get("/api/series")
async def get_series(_: None = Depends(check_auth)) -> list[str]:
    db = _db_path()
    if not db.exists():
        return []
    from edge_catcher.storage.db import get_connection
    conn = get_connection(db)
    try:
        rows = conn.execute("SELECT DISTINCT series_ticker FROM markets ORDER BY series_ticker").fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()


# ── strategies ────────────────────────────────────────────────────────────────

@app.get("/api/strategies", response_model=list[StrategyInfo])
async def get_strategies(_: None = Depends(check_auth)) -> list[StrategyInfo]:
    from edge_catcher.runner.strategy_parser import list_strategies
    pub = list_strategies(file_path=Path("edge_catcher/runner/strategies.py"))
    local = list_strategies(file_path=Path("edge_catcher/runner/strategies_local.py"))
    return [StrategyInfo(**s) for s in pub + local]


# ── strategize (AI) ───────────────────────────────────────────────────────────

@app.post("/api/strategize", response_model=StrategizeResponse)
async def strategize_endpoint(
    body: StrategizeRequest,
    _: None = Depends(check_auth),
) -> StrategizeResponse:
    try:
        from edge_catcher.ai.client import LLMClient
        from edge_catcher.ai.strategizer import strategize
    except ImportError:
        raise HTTPException(status_code=501, detail="AI deps missing. Run: pip install -e '.[ai]'")

    client = LLMClient(provider=body.provider)
    result = strategize(body.hypothesis_id, body.run_id, client, _db_path(), _config_path())
    return StrategizeResponse(**result)


# ── strategy save ─────────────────────────────────────────────────────────────

@app.post("/api/strategies/save", response_model=StrategySaveResponse)
async def save_strategy_endpoint(
    body: StrategySaveRequest,
    _: None = Depends(check_auth),
) -> StrategySaveResponse:
    from edge_catcher.runner.strategy_parser import save_strategy
    result = save_strategy(
        body.code,
        body.strategy_name,
        Path("edge_catcher/runner/strategies_local.py"),
    )
    return StrategySaveResponse(**result)


# ── backtest ──────────────────────────────────────────────────────────────────

def _run_backtest_task(task_id: str, body: BacktestRequest) -> None:
    """Background task: run backtest and store results."""
    import inspect
    import importlib
    from datetime import date, datetime, timezone

    from edge_catcher.runner.event_backtest import EventBacktester
    from edge_catcher.runner.strategy_parser import list_strategies

    state = backtest_states[task_id]
    state.running = True
    state.progress = "Loading strategies..."

    try:
        # Build strategy map from public + local strategies
        strategy_map: dict[str, type] = {}

        # Import public strategies
        from edge_catcher.runner import strategies as pub_mod
        for attr_name in dir(pub_mod):
            obj = getattr(pub_mod, attr_name)
            if isinstance(obj, type) and hasattr(obj, 'name') and hasattr(obj, 'on_trade'):
                if hasattr(obj, 'name') and isinstance(getattr(obj, 'name', None), str):
                    strategy_map[obj.name] = obj

        # Import local strategies (if file exists)
        local_path = Path("edge_catcher/runner/strategies_local.py")
        if local_path.exists():
            try:
                import edge_catcher.runner.strategies_local as local_mod
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

        backtester = EventBacktester()
        result = backtester.run(
            series=body.series,
            strategies=strategies,
            start=start,
            end=end,
            initial_cash=body.cash,
            slippage_cents=body.slippage,
            db_path=_db_path(),
        )

        result_dict = result.to_dict()
        state.result = result_dict

        # Save to JSON file
        result_path = Path(f"reports/backtest_{task_id}.json")
        result_path.parent.mkdir(parents=True, exist_ok=True)
        import json
        with open(result_path, "w") as f:
            json.dump(result_dict, f, indent=2, default=str)

        # Index in DB
        from edge_catcher.storage.db import get_connection, init_db
        init_db(_db_path())
        conn = get_connection(_db_path())
        try:
            conn.execute(
                """INSERT OR REPLACE INTO backtest_results
                   (task_id, series, strategies, start_date, end_date, run_timestamp,
                    total_trades, wins, losses, net_pnl_cents, sharpe, max_drawdown_pct,
                    win_rate, result_path)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (task_id, body.series, json.dumps(body.strategies),
                 body.start, body.end, datetime.now(timezone.utc).isoformat(),
                 result_dict["total_trades"], result_dict["wins"], result_dict["losses"],
                 result_dict["net_pnl_cents"], result_dict["sharpe"],
                 result_dict["max_drawdown_pct"], result_dict["win_rate"],
                 str(result_path)),
            )
            conn.commit()
        finally:
            conn.close()

        state.progress = "Complete"
    except Exception as e:
        logger.error("Backtest failed: %s", e)
        state.error = str(e)
        state.progress = "Error"
    finally:
        state.running = False


@app.post("/api/backtest")
async def start_backtest(
    body: BacktestRequest,
    _: None = Depends(check_auth),
) -> dict:
    import threading

    if is_backtest_running():
        raise HTTPException(status_code=409, detail="A backtest is already running")

    task_id = str(uuid.uuid4())
    backtest_states[task_id] = BacktestTaskState(task_id=task_id)
    threading.Thread(target=_run_backtest_task, args=(task_id, body), daemon=True).start()
    return {"task_id": task_id}


@app.get("/api/backtest/history", response_model=list[BacktestHistoryItem])
async def backtest_history(_: None = Depends(check_auth)) -> list[BacktestHistoryItem]:
    import json
    db = _db_path()
    if not db.exists():
        return []
    from edge_catcher.storage.db import get_connection
    conn = get_connection(db)
    try:
        # Check table exists
        exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='backtest_results'"
        ).fetchone()
        if not exists:
            return []
        rows = conn.execute(
            "SELECT * FROM backtest_results ORDER BY run_timestamp DESC LIMIT 50"
        ).fetchall()
        return [
            BacktestHistoryItem(
                task_id=r["task_id"],
                series=r["series"],
                strategies=json.loads(r["strategies"]),
                timestamp=r["run_timestamp"],
                total_trades=r["total_trades"] or 0,
                net_pnl_cents=r["net_pnl_cents"] or 0,
                sharpe=r["sharpe"] or 0.0,
                win_rate=r["win_rate"] or 0.0,
            )
            for r in rows
        ]
    finally:
        conn.close()


@app.get("/api/backtest/{task_id}/status", response_model=BacktestStatusResponse)
async def backtest_status(task_id: str) -> BacktestStatusResponse:
    state = get_backtest_state(task_id)
    if not state:
        raise HTTPException(status_code=404, detail=f"Backtest {task_id!r} not found")
    return BacktestStatusResponse(running=state.running, progress=state.progress, error=state.error)


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
