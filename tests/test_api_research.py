"""Tests for research dashboard API."""
import pytest
from api.research_tasks import (
	ResearchLoopState,
	research_loop_state,
	get_research_loop_state,
	is_research_loop_running,
)


class TestResearchLoopState:
	def setup_method(self):
		research_loop_state.clear()

	def test_default_state(self):
		state = ResearchLoopState(task_id="t1")
		assert state.running is False
		assert state.phase == "idle"
		assert state.runs_completed == 0
		assert state.cancel_requested is False

	def test_get_state(self):
		state = ResearchLoopState(task_id="t1")
		research_loop_state["t1"] = state
		assert get_research_loop_state("t1") is state
		assert get_research_loop_state("missing") is None

	def test_is_running(self):
		assert is_research_loop_running() is False
		state = ResearchLoopState(task_id="t1", running=True)
		research_loop_state["t1"] = state
		assert is_research_loop_running() is True
