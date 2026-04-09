"""Tests for hypothesis results storage and kill registry."""

import pytest
from edge_catcher.research.tracker import Tracker


@pytest.fixture
def tracker(tmp_path):
    return Tracker(db_path=str(tmp_path / "test_research.db"))


class TestHypothesisResultsStorage:
    def test_save_and_list(self, tracker):
        tracker.save_hypothesis_result(
            test_type="price_bucket_bias", series="SER_A", db="kalshi.db",
            params={"buckets": [[0.01, 0.10]]}, thresholds={"clustered_z_stat": 3.0},
            verdict="EDGE_EXISTS", z_stat=3.5, fee_adjusted_edge=0.02,
            detail={"buckets": [{"range": [0.01, 0.10], "z": 3.5}]}, rationale="test",
        )
        results = tracker.list_hypothesis_results()
        assert len(results) == 1
        assert results[0]["verdict"] == "EDGE_EXISTS"
        assert results[0]["series"] == "SER_A"

    def test_filter_by_verdict(self, tracker):
        tracker.save_hypothesis_result(
            test_type="t1", series="S", db="d.db", params={}, thresholds={},
            verdict="EDGE_EXISTS", z_stat=3.0, fee_adjusted_edge=0.01, detail={},
        )
        tracker.save_hypothesis_result(
            test_type="t2", series="S", db="d.db", params={}, thresholds={},
            verdict="NO_EDGE", z_stat=1.0, fee_adjusted_edge=0.0, detail={},
        )
        edges = tracker.list_hypothesis_results(verdict="EDGE_EXISTS")
        assert len(edges) == 1
        assert edges[0]["test_type"] == "t1"


class TestHypothesisKillRegistry:
    def test_record_no_edge_increments(self, tracker):
        tracker.record_hypothesis_kill("price_bucket_bias", "SER_A", "kalshi.db",
            verdict="NO_EDGE", params={}, z_stat=1.2)
        kills = tracker.list_hypothesis_kills()
        assert len(kills) == 1
        assert kills[0]["kill_count"] == 1
        assert kills[0]["permanent"] == 0

    def test_three_no_edge_makes_permanent(self, tracker):
        for _ in range(3):
            tracker.record_hypothesis_kill("price_bucket_bias", "SER_A", "kalshi.db",
                verdict="NO_EDGE", params={}, z_stat=1.0)
        kills = tracker.list_hypothesis_kills()
        assert kills[0]["kill_count"] == 3
        assert kills[0]["permanent"] == 1

    def test_insufficient_data_does_not_count(self, tracker):
        tracker.record_hypothesis_kill("price_bucket_bias", "SER_A", "kalshi.db",
            verdict="INSUFFICIENT_DATA", params={}, z_stat=0.0)
        kills = tracker.list_hypothesis_kills()
        assert len(kills) == 0

    def test_edge_not_tradeable_slow_kill(self, tracker):
        for i in range(4):
            tracker.record_hypothesis_kill("momentum", "SER_B", "alt.db",
                verdict="EDGE_NOT_TRADEABLE", params={}, z_stat=3.0)
        kills = tracker.list_hypothesis_kills()
        assert kills[0]["kill_count"] == 4
        assert kills[0]["permanent"] == 0

        tracker.record_hypothesis_kill("momentum", "SER_B", "alt.db",
            verdict="EDGE_NOT_TRADEABLE", params={}, z_stat=3.0)
        kills = tracker.list_hypothesis_kills()
        assert kills[0]["kill_count"] == 5
        assert kills[0]["permanent"] == 1

    def test_is_hypothesis_killed(self, tracker):
        for _ in range(3):
            tracker.record_hypothesis_kill("price_bucket_bias", "SER_A", "k.db",
                verdict="NO_EDGE", params={}, z_stat=1.0)
        assert tracker.is_hypothesis_killed("price_bucket_bias", "SER_A", "k.db") is True
        assert tracker.is_hypothesis_killed("price_bucket_bias", "SER_B", "k.db") is False
