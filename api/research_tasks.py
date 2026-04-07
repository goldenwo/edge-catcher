"""Research loop task state management.

Mirrors the backtest pattern in api/tasks.py.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class ResearchLoopState:
	task_id: str = ""
	running: bool = False
	phase: str = "idle"
	runs_completed: int = 0
	runs_total: int = 0
	elapsed_seconds: float = 0.0
	error: Optional[str] = None
	cancel_requested: bool = False


research_loop_state: Dict[str, ResearchLoopState] = {}


def get_research_loop_state(task_id: str) -> Optional[ResearchLoopState]:
	return research_loop_state.get(task_id)


def is_research_loop_running() -> bool:
	return any(s.running for s in research_loop_state.values())
