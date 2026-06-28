"""Reporter surfaces the execution archetype for each candidate."""

from edge_catcher.research.data_source_config import make_ds
from edge_catcher.research.hypothesis import Hypothesis, HypothesisResult
from edge_catcher.research.reporter import Reporter


def _result(strategy="s1", verdict="promote"):
	h = Hypothesis(strategy=strategy, data_sources=make_ds(db="x.db", series="S1"))
	return HypothesisResult(
		hypothesis=h, status="ok", total_trades=100, wins=60, losses=40,
		win_rate=0.6, net_pnl_cents=500.0, sharpe=2.0, max_drawdown_pct=5.0,
		fees_paid_cents=10.0, avg_win_cents=15.0, avg_loss_cents=-8.0,
		per_strategy={}, verdict=verdict, verdict_reason="ok", raw_json={},
	)


def test_report_includes_execution_archetype(monkeypatch):
	monkeypatch.setattr(
		"edge_catcher.research.reporter.resolve_execution_archetype",
		lambda name: "taker_synthetic",
	)
	report = Reporter().generate_report([_result()])
	assert report["promoted"][0]["execution_archetype"] == "taker_synthetic"
