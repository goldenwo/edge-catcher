"""Pydantic request/response models for the Edge Catcher API."""
from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel


class StatusResponse(BaseModel):
    markets: int
    trades: int
    results: int
    db_size_mb: float
    last_download: Optional[str]


class DownloadStatusResponse(BaseModel):
    running: bool
    progress: str
    markets_fetched: int
    trades_fetched: int


class HypothesisItem(BaseModel):
    id: str
    name: str
    market: str
    status: str


class AnalyzeRequest(BaseModel):
    hypothesis_id: Optional[str] = None


class ResultSummary(BaseModel):
    run_id: str
    hypothesis_id: str
    verdict: Optional[str]
    run_timestamp: str


class ResultDetail(BaseModel):
    run_id: str
    hypothesis_id: str
    run_timestamp: str
    market: str
    status: str
    naive_n: Optional[int] = None
    naive_z_stat: Optional[float] = None
    naive_p_value: Optional[float] = None
    naive_edge: Optional[float] = None
    clustered_n: Optional[int] = None
    clustered_z_stat: Optional[float] = None
    clustered_p_value: Optional[float] = None
    clustered_edge: Optional[float] = None
    fee_adjusted_edge: Optional[float] = None
    confidence_interval_low: Optional[float] = None
    confidence_interval_high: Optional[float] = None
    verdict: Optional[str] = None
    warnings: Optional[Any] = None
    total_markets_seen: Optional[int] = None
    delisted_or_cancelled: Optional[int] = None
    raw_bucket_data: Optional[Any] = None


class FormalizeRequest(BaseModel):
    description: str
    provider: Optional[str] = None


class FormalizeResponse(BaseModel):
    message: str
    error: Optional[str] = None


class InterpretRequest(BaseModel):
    run_id: str
    provider: Optional[str] = None


class InterpretResponse(BaseModel):
    summary: str


class AdapterInfo(BaseModel):
    id: str
    name: str
    description: str
    requires_api_key: bool
    api_key_env_var: Optional[str] = None
    api_key_set: bool
    download_status: str
    default_start_date: Optional[str] = None
    db_size_mb: Optional[float] = None


class AdapterDownloadRequest(BaseModel):
    adapter_id: str
    api_key: Optional[str] = None
    start_date: Optional[str] = None  # ISO date string, e.g. "2024-01-01"


class AdapterDownloadStatus(BaseModel):
    adapter_id: str
    running: bool
    progress: str
    rows_fetched: int
    error: Optional[str] = None


class AIKeyRequest(BaseModel):
    provider: str
    api_key: str


class AdapterKeyRequest(BaseModel):
    api_key: str


class AISettingsResponse(BaseModel):
    anthropic: bool
    openai: bool
    openrouter: bool


# ── Pipeline Status ──────────────────────────────────────────────────────────

class PipelineDataStatus(BaseModel):
    has_data: bool
    markets: int
    trades: int

class PipelineHypothesesStatus(BaseModel):
    count: int

class PipelineAnalysisStatus(BaseModel):
    count: int
    latest_verdict: Optional[str] = None

class PipelineStrategiesStatus(BaseModel):
    count: int
    names: list[str]

class PipelineBacktestStatus(BaseModel):
    count: int
    latest_sharpe: Optional[float] = None

class PipelineStatusResponse(BaseModel):
    data: PipelineDataStatus
    hypotheses: PipelineHypothesesStatus
    analysis: PipelineAnalysisStatus
    strategies: PipelineStrategiesStatus
    backtest: PipelineBacktestStatus


# ── Strategies ───────────────────────────────────────────────────────────────

class StrategyInfo(BaseModel):
    name: str
    class_name: str

class StrategizeRequest(BaseModel):
    hypothesis_id: str
    run_id: Optional[str] = None
    provider: Optional[str] = None

class StrategizeResponse(BaseModel):
    code: str
    strategy_name: str
    error: Optional[str] = None

class StrategySaveRequest(BaseModel):
    code: str
    strategy_name: str

class StrategySaveResponse(BaseModel):
    ok: bool
    path: Optional[str] = None
    error: Optional[str] = None


# ── Backtest ─────────────────────────────────────────────────────────────────

class BacktestRequest(BaseModel):
    series: str
    strategies: list[str]
    start: Optional[str] = None
    end: Optional[str] = None
    cash: float = 10000.0
    slippage: int = 1
    tp: Optional[int] = None
    sl: Optional[int] = None
    min_price: Optional[int] = None
    max_price: Optional[int] = None

class BacktestStatusResponse(BaseModel):
    running: bool
    progress: str
    error: Optional[str] = None
    trades_processed: Optional[int] = None
    trades_estimated: Optional[int] = None
    net_pnl_cents: Optional[int] = None

class BacktestHistoryItem(BaseModel):
    task_id: str
    series: str
    strategies: list[str]
    timestamp: str
    total_trades: int
    net_pnl_cents: int
    sharpe: float
    win_rate: float
