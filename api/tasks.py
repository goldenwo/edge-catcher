"""In-process download state singleton."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional


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
        adapter_states[adapter_id] = AdapterDownloadState()
    return adapter_states[adapter_id]
