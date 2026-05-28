"""run_engine paper branch honors paper_fill_model (Phase 1 wiring)."""
from __future__ import annotations

from edge_catcher.engine.executors.honest_paper import HonestPaperExecutor
from edge_catcher.engine.executors.paper import PaperExecutor


def test_paper_fill_model_selection(tmp_path, monkeypatch):
	# Build a minimal paper config and call the composition helper directly.
	from edge_catcher.engine.engine import _build_paper_executor
	from edge_catcher.engine.market_state import MarketState

	ms = MarketState()
	base_cfg = {"sizing": {"risk_per_trade_cents": 200, "min_fill": 1, "require_fresh_book": True}}

	# default (key absent) → bare PaperExecutor
	ex_default = _build_paper_executor({**base_cfg}, ms)
	assert type(ex_default) is PaperExecutor

	# explicit optimistic → bare PaperExecutor
	ex_opt = _build_paper_executor({**base_cfg, "paper_fill_model": "optimistic"}, ms)
	assert type(ex_opt) is PaperExecutor

	# fixed → HonestPaperExecutor wrapping a PaperExecutor
	ex_fixed = _build_paper_executor(
		{
			**base_cfg,
			"paper_fill_model": "fixed",
			"honest_paper": {"default_slippage_cents": 2, "per_strategy": {}},
		},
		ms,
	)
	assert isinstance(ex_fixed, HonestPaperExecutor)
	assert isinstance(ex_fixed.base, PaperExecutor)
