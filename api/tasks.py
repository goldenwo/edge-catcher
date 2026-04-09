"""In-process download state singleton."""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)

_HISTORY_FILE = Path("data/.adapter_history.json")


def _load_history() -> Dict[str, str]:
    """Load persisted per-adapter last_run timestamps."""
    if _HISTORY_FILE.exists():
        try:
            return json.loads(_HISTORY_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def save_adapter_history(adapter_id: str, last_run: str) -> None:
    """Persist a successful download timestamp for an adapter."""
    history = _load_history()
    history[adapter_id] = last_run
    _HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    _HISTORY_FILE.write_text(json.dumps(history))


@dataclass
class DownloadState:
    running: bool = False
    progress: str = ""
    markets_fetched: int = 0
    trades_fetched: int = 0
    last_run: Optional[str] = None
    error: Optional[str] = None


download_state = DownloadState()


@dataclass
class AdapterDownloadState:
    running: bool = False
    progress: str = "Idle"
    rows_fetched: int = 0
    error: Optional[str] = None
    last_run: Optional[str] = None


# Keep existing download_state for backward compat
adapter_states: Dict[str, AdapterDownloadState] = {}


def get_adapter_state(adapter_id: str) -> AdapterDownloadState:
    if adapter_id not in adapter_states:
        history = _load_history()
        last_run = history.get(adapter_id)
        adapter_states[adapter_id] = AdapterDownloadState(
            last_run=last_run,
            progress="Idle" if not last_run else f"Last run: {last_run}",
        )
    return adapter_states[adapter_id]


@dataclass
class BacktestTaskState:
    task_id: str = ""
    running: bool = False
    progress: str = ""
    error: Optional[str] = None
    result: Optional[dict] = None  # BacktestResult.to_dict() when complete
    trades_processed: int = 0
    trades_estimated: int = 0
    net_pnl_cents: int = 0
    cancel_requested: bool = False


backtest_states: Dict[str, BacktestTaskState] = {}


def get_backtest_state(task_id: str) -> Optional[BacktestTaskState]:
    return backtest_states.get(task_id)


def is_backtest_running() -> bool:
    return any(s.running for s in backtest_states.values())


@dataclass
class AnalyzeTaskState:
    task_id: str = ""
    hypothesis_id: Optional[str] = None
    running: bool = False
    progress: str = ""
    error: Optional[str] = None
    result: Optional[dict] = None


analyze_states: Dict[str, AnalyzeTaskState] = {}


def get_analyze_state(task_id: str) -> Optional[AnalyzeTaskState]:
    return analyze_states.get(task_id)
