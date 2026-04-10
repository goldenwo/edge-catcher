"""Tests for edge_catcher.research.export."""

from __future__ import annotations

from pathlib import Path

from edge_catcher.research.export import ExportCollector
from edge_catcher.research.tracker import Tracker
from edge_catcher.research.data_source_config import make_ds
from edge_catcher.research.hypothesis import Hypothesis, HypothesisResult


def _make_collector(tmp_path: Path) -> ExportCollector:
	db_path = str(tmp_path / "research.db")
	return ExportCollector(db_path=db_path)


def _insert_result(tracker: Tracker, strategy: str = "test-strat", series: str = "SERIES_A",
                   verdict: str = "promote", sharpe: float = 5.0) -> str:
	"""Insert a hypothesis + result into tracker, return hypothesis ID."""
	h = Hypothesis(
		strategy=strategy,
		data_sources=make_ds(db="test.db", series=series),
		start_date="2025-01-01",
		end_date="2025-12-31",
		fee_pct=1.0,
	)
	result = HypothesisResult(
		hypothesis=h, status="ok", total_trades=100, wins=60, losses=40,
		win_rate=0.6, net_pnl_cents=500.0, sharpe=sharpe, max_drawdown_pct=10.0,
		fees_paid_cents=50.0, avg_win_cents=15.0, avg_loss_cents=-10.0,
		per_strategy={strategy: {"sharpe": sharpe}},
		verdict=verdict, verdict_reason="test", raw_json={},
	)
	tracker.save_result(result)
	return h.id


class TestCollectResults:
	def test_empty_db_returns_empty_strategies(self, tmp_path):
		collector = _make_collector(tmp_path)
		bundle = collector.collect()
		assert bundle["version"] == 1
		assert bundle["strategies"] == {}
		assert "exported_at" in bundle
		assert bundle["filter"]["verdicts"] == ["promote", "review"]

	def test_collects_promotes_grouped_by_strategy(self, tmp_path):
		collector = _make_collector(tmp_path)
		_insert_result(collector.tracker, strategy="alpha", series="S1", verdict="promote")
		_insert_result(collector.tracker, strategy="alpha", series="S2", verdict="promote")
		_insert_result(collector.tracker, strategy="beta", series="S1", verdict="review")
		_insert_result(collector.tracker, strategy="gamma", series="S1", verdict="kill")

		bundle = collector.collect()
		assert set(bundle["strategies"].keys()) == {"alpha", "beta"}
		assert len(bundle["strategies"]["alpha"]["results"]) == 2
		assert len(bundle["strategies"]["beta"]["results"]) == 1
		assert bundle["strategies"]["beta"]["results"][0]["verdict"] == "review"

	def test_result_fields_complete(self, tmp_path):
		collector = _make_collector(tmp_path)
		_insert_result(collector.tracker, strategy="alpha", series="S1",
		               verdict="promote", sharpe=5.5)
		bundle = collector.collect()
		result = bundle["strategies"]["alpha"]["results"][0]
		required_fields = [
			"hypothesis_id", "strategy", "series", "db_path", "start_date",
			"end_date", "fee_pct", "verdict", "status", "sharpe", "win_rate",
			"net_pnl_cents", "max_drawdown_pct", "total_trades", "wins",
			"losses", "fees_paid_cents", "avg_win_cents", "avg_loss_cents",
			"verdict_reason", "validation_details", "completed_at", "audit",
		]
		for field in required_fields:
			assert field in result, f"Missing field: {field}"
		assert result["sharpe"] == 5.5

	def test_custom_verdicts_filter(self, tmp_path):
		collector = _make_collector(tmp_path)
		_insert_result(collector.tracker, strategy="alpha", series="S1", verdict="promote")
		_insert_result(collector.tracker, strategy="beta", series="S1", verdict="kill")

		bundle = collector.collect(verdicts=["kill"])
		assert set(bundle["strategies"].keys()) == {"beta"}
		assert bundle["filter"]["verdicts"] == ["kill"]
