"""Tests for research dashboard API."""
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
from api.main import app
from api.research_tasks import (
	ResearchLoopState,
	research_loop_state,
	get_research_loop_state,
	is_research_loop_running,
)

client = TestClient(app)

_nonexistent_db = Path("data/_test_nonexistent_research.db")


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


class TestLoopControlEndpoints:
	def setup_method(self):
		research_loop_state.clear()

	def test_start_loop(self):
		with patch("threading.Thread") as mock_thread:
			resp = client.post("/api/research/loop/start", json={
				"mode": "grid_only", "max_runs": 50, "max_time": 30, "parallel": 2,
			})
		assert resp.status_code == 200
		data = resp.json()
		assert "task_id" in data
		assert mock_thread.called

	def test_start_loop_conflict(self):
		state = ResearchLoopState(task_id="t1", running=True)
		research_loop_state["t1"] = state
		resp = client.post("/api/research/loop/start", json={
			"mode": "full", "max_runs": 100, "max_time": 60, "parallel": 4,
		})
		assert resp.status_code == 409

	def test_stop_loop(self):
		state = ResearchLoopState(task_id="t1", running=True)
		research_loop_state["t1"] = state
		resp = client.post("/api/research/loop/stop")
		assert resp.status_code == 200
		assert state.cancel_requested is True

	def test_stop_loop_not_running(self):
		resp = client.post("/api/research/loop/stop")
		assert resp.status_code == 409

	def test_loop_status_idle(self):
		resp = client.get("/api/research/loop/status")
		assert resp.status_code == 200
		data = resp.json()
		assert data["running"] is False
		assert data["phase"] == "idle"

	def test_loop_status_running(self):
		state = ResearchLoopState(
			task_id="t1", running=True, phase="ideate",
			runs_completed=10, runs_total=100, elapsed_seconds=45.0,
		)
		research_loop_state["t1"] = state
		resp = client.get("/api/research/loop/status")
		data = resp.json()
		assert data["running"] is True
		assert data["phase"] == "ideate"
		assert data["runs_completed"] == 10


class TestResultsEndpoints:
	@patch("api.main._research_db_path", return_value=_nonexistent_db)
	def test_verdict_counts_empty(self, _mock):
		resp = client.get("/api/research/verdict-counts")
		assert resp.status_code == 200
		data = resp.json()
		assert data == {"promote": 0, "review": 0, "explore": 0, "kill": 0}

	@patch("api.main._research_db_path", return_value=_nonexistent_db)
	def test_results_empty(self, _mock):
		resp = client.get("/api/research/results")
		assert resp.status_code == 200
		data = resp.json()
		assert data["results"] == []
		assert data["total"] == 0


class TestAuditEndpoints:
	@patch("api.main._research_db_path", return_value=_nonexistent_db)
	def test_executions_empty(self, _mock):
		resp = client.get("/api/research/audit/executions")
		assert resp.status_code == 200
		assert resp.json() == []

	@patch("api.main._research_db_path", return_value=_nonexistent_db)
	def test_decisions_empty(self, _mock):
		resp = client.get("/api/research/audit/decisions")
		assert resp.status_code == 200
		assert resp.json() == []
