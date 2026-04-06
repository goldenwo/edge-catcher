"""Gate interface and shared types for the validation pipeline."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
	from edge_catcher.research.agent import ResearchAgent
	from edge_catcher.research.tracker import Tracker

from edge_catcher.research.hypothesis import Hypothesis, HypothesisResult


@dataclass
class GateResult:
	"""Result of a single validation gate check."""
	passed: bool
	gate_name: str
	reason: str
	details: dict = field(default_factory=dict)


@dataclass
class GateContext:
	"""Shared context passed to all gates in the pipeline."""
	tracker: "Tracker | None"
	pnl_values: list[float]
	hypothesis: Hypothesis
	agent: "ResearchAgent | None" = None


class Gate(ABC):
	"""Abstract base for validation gates."""
	name: str

	@abstractmethod
	def check(self, result: HypothesisResult, context: GateContext) -> GateResult:
		"""Run this gate's validation. Return GateResult with pass/fail."""
		...
