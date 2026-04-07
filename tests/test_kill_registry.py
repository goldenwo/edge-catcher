"""Tests for the persistent kill registry in Tracker."""

import sqlite3
import tempfile
from pathlib import Path

import pytest

from edge_catcher.research.tracker import Tracker


@pytest.fixture
def tracker(tmp_path):
	db_path = str(tmp_path / "test_research.db")
	return Tracker(db_path)


class TestKillRegistry:
	def test_upsert_and_list(self, tracker):
		tracker.upsert_kill_registry(
			strategy="BadStrat",
			kill_count=8,
			series_tested=10,
			kill_rate=0.8,
			reason_summary='["low sharpe: 5x", "negative pnl: 3x"]',
		)
		entries = tracker.list_kill_registry()
		assert len(entries) == 1
		assert entries[0]["strategy"] == "BadStrat"
		assert entries[0]["kill_rate"] == 0.8
		assert entries[0]["permanent"] == 1  # SQLite stores bool as int

	def test_upsert_updates_existing(self, tracker):
		tracker.upsert_kill_registry("BadStrat", 8, 10, 0.8, "[]")
		tracker.upsert_kill_registry("BadStrat", 12, 14, 0.857, '["new reason"]')
		entries = tracker.list_kill_registry()
		assert len(entries) == 1
		assert entries[0]["kill_count"] == 12
		assert entries[0]["reason_summary"] == '["new reason"]'

	def test_reset_sets_permanent_false(self, tracker):
		tracker.upsert_kill_registry("BadStrat", 8, 10, 0.8, "[]")
		tracker.reset_kill_registry("BadStrat")
		entries = tracker.list_kill_registry()
		assert entries[0]["permanent"] == 0

	def test_re_kill_after_reset(self, tracker):
		"""A reset strategy that gets killed again becomes permanent again."""
		tracker.upsert_kill_registry("BadStrat", 8, 10, 0.8, "[]")
		tracker.reset_kill_registry("BadStrat")
		# Re-killed: upsert should set permanent back to TRUE
		tracker.upsert_kill_registry("BadStrat", 12, 14, 0.857, "[]")
		entries = tracker.list_kill_registry()
		assert entries[0]["permanent"] == 1

	def test_list_permanent_only(self, tracker):
		tracker.upsert_kill_registry("Dead1", 8, 10, 0.8, "[]")
		tracker.upsert_kill_registry("Dead2", 9, 10, 0.9, "[]")
		tracker.reset_kill_registry("Dead1")
		permanent = tracker.list_kill_registry(permanent_only=True)
		assert len(permanent) == 1
		assert permanent[0]["strategy"] == "Dead2"

	def test_empty_registry(self, tracker):
		assert tracker.list_kill_registry() == []
