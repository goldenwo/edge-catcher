"""Research module: automated hypothesis testing across market categories."""

from .hypothesis import Hypothesis, HypothesisResult
from .evaluator import Evaluator, Thresholds
from .tracker import Tracker
from .reporter import Reporter
from .agent import ResearchAgent
from .audit import AuditLog
from .grid_planner import GridPlanner
from .run_queue import RunQueue
from .llm_ideator import LLMIdeator
from .loop import LoopOrchestrator
from .validation import ValidationPipeline, GateContext, GateResult, Gate
from .journal import ResearchJournal

__all__ = [
    "Hypothesis",
    "HypothesisResult",
    "Evaluator",
    "Thresholds",
    "Tracker",
    "Reporter",
    "ResearchAgent",
    "AuditLog",
    "GridPlanner",
    "RunQueue",
    "LLMIdeator",
    "LoopOrchestrator",
    "ValidationPipeline",
    "GateContext",
    "GateResult",
    "Gate",
    "ResearchJournal",
]
