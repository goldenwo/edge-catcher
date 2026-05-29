import subprocess
import sys

from edge_catcher.cross_check import CrossCheckReport, Finding, Outcome, _num, categorize_exit, true_pnl_cents


def test_num_coerces_and_defaults():
	assert _num("1.5") == 1.5
	assert _num(None) == 0.0
	assert _num("garbage") == 0.0


def test_true_pnl_yes_win():
	# 3 YES contracts, market resolved YES, value $1, cost $1.50 total, $0.03 fee
	s = {
		"market_result": "yes", "yes_count_fp": 3, "no_count_fp": 0, "value": 100,
		"yes_total_cost_dollars": 1.50, "no_total_cost_dollars": 0.0, "fee_cost": 0.03,
	}
	# payout 3*100=300c; cost 150c; fee 3c -> 147c
	assert true_pnl_cents(s) == 147


def test_true_pnl_empty_result_is_not_a_loss_signal():
	# Expired-but-pending settlement (market_result == "") -> payout 0; caller treats
	# the ABSENCE of a settled result as UNSETTLED, not a loss (see reconcile, Task 3).
	s = {"market_result": "", "yes_count_fp": 2, "no_count_fp": 0, "value": 100,
	     "yes_total_cost_dollars": 1.0, "no_total_cost_dollars": 0.0, "fee_cost": 0.0}
	assert true_pnl_cents(s) == -100  # 0 payout - 100c cost; reconcile must gate on settled-ness


def test_categorize_exit():
	assert categorize_exit({"initial_count_fp": 3, "fill_count_fp": 3, "status": "executed"}) == "filled"
	assert categorize_exit({"initial_count_fp": 3, "fill_count_fp": 1, "status": "canceled"}) == "partial"
	assert categorize_exit({"initial_count_fp": 3, "fill_count_fp": 0, "status": "canceled"}) == "canceled"
	assert categorize_exit({"initial_count_fp": 3, "fill_count_fp": 0, "status": "expired"}) == "zero_fill"


def test_core_imports_no_order_client():
	"""§8.3: importing the pure core must NOT pull in the order-placing client."""
	code = (
		"import edge_catcher.cross_check\n"
		"import sys\n"
		"bad = [m for m in sys.modules if m.startswith('edge_catcher.live')]\n"
		"assert not bad, f'live package leaked: {bad}'\n"
	)
	r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
	assert r.returncode == 0, r.stderr


def test_true_pnl_no_win_and_multi_leg_sum():
	# NO-side win: 2 NO contracts, resolved NO, $0.80 cost, no fee -> 2*100 - 80 = 120c
	no_win = {"market_result": "no", "yes_count_fp": 0, "no_count_fp": 2, "value": 100,
	          "yes_total_cost_dollars": 0.0, "no_total_cost_dollars": 0.80, "fee_cost": 0.0}
	assert true_pnl_cents(no_win) == 120
	# Caller sums across a ticker's settlements (both round-trip legs settle).
	leg = {"market_result": "yes", "yes_count_fp": 1, "no_count_fp": 0, "value": 100,
	       "yes_total_cost_dollars": 0.40, "no_total_cost_dollars": 0.0, "fee_cost": 0.0}
	assert sum(true_pnl_cents(s) for s in [leg, leg]) == 120  # (100-40)*2


def test_sqlite_readonly_mode_blocks_writes(tmp_path):
	"""Pins the read-only open pattern the wrapper relies on (spec §8.1): a
	mode=ro connection raises on any write."""
	import sqlite3

	import pytest
	db = tmp_path / "t.db"
	con = sqlite3.connect(str(db))
	con.execute("CREATE TABLE t(x)")
	con.commit()
	con.close()
	ro = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
	with pytest.raises(sqlite3.OperationalError):
		ro.execute("INSERT INTO t(x) VALUES (1)")
	ro.close()


def _f(outcome, material, ticker="KXTEST15M-A", detail="x"):
	return Finding(ticker=ticker, outcome=outcome, material=material, detail=detail)


def test_report_is_clean_and_exit_code():
	clean = CrossCheckReport(findings=[_f(Outcome.UNATTRIBUTED, False), _f(Outcome.UNSETTLED, False)], n_tickers=2)
	assert clean.is_clean is True
	assert clean.exit_code == 0

	dirty = CrossCheckReport(findings=[_f(Outcome.PHANTOM, True)], n_tickers=1)
	assert dirty.is_clean is False
	assert dirty.exit_code == 1


def test_report_counts_by_outcome():
	rep = CrossCheckReport(
		findings=[_f(Outcome.MATCHED, False), _f(Outcome.MATCHED, False), _f(Outcome.PHANTOM, True)],
		n_tickers=3,
	)
	assert rep.counts() == {"matched": 2, "phantom": 1}


# ---------------------------------------------------------------------------
# Task 3 — reconcile() structural outcome tests
# ---------------------------------------------------------------------------
from edge_catcher.cross_check import reconcile  # noqa: E402

SERIES = frozenset({"KXTEST15M"})


def _buy(ticker, fill=3, initial=3, coid="bot-1"):
	return {"ticker": ticker, "action": "buy", "side": "yes", "fill_count_fp": fill,
	        "initial_count_fp": initial, "taker_fill_cost_dollars": 1.50,
	        "taker_fees_dollars": 0.03, "client_order_id": coid, "status": "executed"}


def _settle(ticker, result="yes"):
	return {"ticker": ticker, "market_result": result, "yes_count_fp": 3, "no_count_fp": 0,
	        "value": 100, "yes_total_cost_dollars": 1.50, "no_total_cost_dollars": 0.0, "fee_cost": 0.03}


def _row(ticker, **kw):
	base = {"ticker": ticker, "series": "KXTEST15M", "strategy": "s", "side": "yes",
	        "fill_size": 3, "blended_entry_cents": 50, "status": "won", "pnl_cents": 147,
	        "exit_reason": "settlement", "client_order_id": "bot-1"}
	base.update(kw)
	return base


def _outcomes(rep):
	return {f.ticker: f.outcome for f in rep.findings}


def test_matched_clean():
	t = "KXTEST15M-A"
	rep = reconcile([_row(t)], [_buy(t)], [_settle(t)], in_scope_series=SERIES, expected_strategy="s")
	assert _outcomes(rep)[t] == Outcome.MATCHED


def test_phantom_db_row_no_kalshi_buy():
	t = "KXTEST15M-A"
	rep = reconcile([_row(t)], [], [], in_scope_series=SERIES, expected_strategy="s")
	f = {x.ticker: x for x in rep.findings}[t]
	assert f.outcome == Outcome.PHANTOM and f.material is True


def test_missing_kalshi_buy_no_db_row_when_strategy_scoped():
	t = "KXTEST15M-A"
	rep = reconcile([], [_buy(t)], [_settle(t)], in_scope_series=SERIES, expected_strategy="s")
	f = {x.ticker: x for x in rep.findings}[t]
	assert f.outcome == Outcome.MISSING and f.material is True


def test_unattributed_when_no_strategy_scope():
	t = "KXTEST15M-A"
	rep = reconcile([], [_buy(t)], [_settle(t)], in_scope_series=SERIES, expected_strategy=None)
	f = {x.ticker: x for x in rep.findings}[t]
	assert f.outcome == Outcome.UNATTRIBUTED and f.material is False


def test_out_of_scope_ticker_excluded():
	rep = reconcile([], [_buy("KXOTHER-A")], [], in_scope_series=SERIES, expected_strategy="s")
	assert rep.findings == []


def test_multi_entry_flagged():
	t = "KXTEST15M-A"
	rep = reconcile([_row(t)], [_buy(t, coid="bot-1"), _buy(t, coid="bot-2")],
	                [_settle(t)], in_scope_series=SERIES, expected_strategy="s")
	f = {x.ticker: x for x in rep.findings}[t]
	assert f.outcome == Outcome.MULTI_ENTRY and f.material is True


def test_unsettled_when_no_settlement():
	t = "KXTEST15M-A"
	rep = reconcile([_row(t, status="open", pnl_cents=None)], [_buy(t)], [],
	                in_scope_series=SERIES, expected_strategy="s")
	f = {x.ticker: x for x in rep.findings}[t]
	assert f.outcome == Outcome.UNSETTLED and f.material is False


def test_empty_inputs_are_clean():
	rep = reconcile([], [], [], in_scope_series=SERIES, expected_strategy="s")
	assert rep.findings == [] and rep.is_clean is True and rep.exit_code == 0


def test_stray_settlement_without_order_or_row_is_ignored():
	# A settlement for an in-scope ticker with no filled BUY and no db row contributes
	# no finding (tickers are built from rows ∪ filled-BUYs only — intentional, no
	# PHANTOM_SETTLEMENT class). Documents the §5.2 window-edge behavior.
	t = "KXTEST15M-Z"
	rep = reconcile([], [], [_settle(t)], in_scope_series=SERIES, expected_strategy="s")
	assert rep.findings == []
