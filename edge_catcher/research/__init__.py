"""Research module: automated hypothesis testing across market categories."""

from .hypothesis import Hypothesis, HypothesisResult
from .evaluator import Evaluator, Thresholds
from .tracker import Tracker
from .reporter import Reporter
from .agent import ResearchAgent

__all__ = [
    "Hypothesis",
    "HypothesisResult",
    "Evaluator",
    "Thresholds",
    "Tracker",
    "Reporter",
    "ResearchAgent",
]
