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
	# Defensive fallback: a 0-fill with no cancel signal. Real REST 0-fill IOCs come back
	# status="canceled" (the case above); "zero_fill" guards an unexpected/absent status.
	assert categorize_exit({"initial_count_fp": 3, "fill_count_fp": 0, "status": ""}) == "zero_fill"


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


def test_in_scope_excludes_prefix_sharing_longer_series():
	# Regression: a DISTINCT longer series that shares the in-scope series' leading
	# chars (the real KXXRP vs KXXRPD/KXXRP15M collision) must be excluded — a bare
	# startswith would over-scope it and emit a spurious MISSING. spec §5.2.
	t_in = "KXTEST15M-26MAY241600-T1"
	t_out = "KXTEST15MAX-26MAY241600-T1"  # different series, shares the KXTEST15M prefix
	rep = reconcile([], [_buy(t_in), _buy(t_out)], [], in_scope_series=SERIES, expected_strategy="s")
	tickers = {f.ticker for f in rep.findings}
	assert t_in in tickers and t_out not in tickers


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


# ---------------------------------------------------------------------------
# Task 4 — _compare_fields: field disagreements + material classification
# ---------------------------------------------------------------------------

def _finding_for(rep, ticker):
	return {f.ticker: f for f in rep.findings}[ticker]


def test_blended_entry_disagreement_is_material():
	t = "KXTEST15M-A"
	# Kalshi blended = 150c cost / 3 fills = 50c; db says 60c -> 10c diff > 1c threshold.
	rep = reconcile([_row(t, blended_entry_cents=60)], [_buy(t)], [_settle(t)],
	                in_scope_series=SERIES, expected_strategy="s")
	f = _finding_for(rep, t)
	assert f.material is True and "blended_entry_cents" in f.fields


def test_blended_within_threshold_is_noise():
	t = "KXTEST15M-A"
	rep = reconcile([_row(t, blended_entry_cents=51)], [_buy(t)], [_settle(t)],
	                in_scope_series=SERIES, expected_strategy="s")
	f = _finding_for(rep, t)
	assert f.material is False  # 1c diff is not > 1c


def test_pnl_disagreement_is_material():
	t = "KXTEST15M-A"
	# true pnl = 147c; db says 0 -> phantom-exit class.
	rep = reconcile([_row(t, pnl_cents=0)], [_buy(t)], [_settle(t)],
	                in_scope_series=SERIES, expected_strategy="s")
	f = _finding_for(rep, t)
	assert f.material is True and "pnl_cents" in f.fields


def test_fill_size_any_mismatch_material():
	t = "KXTEST15M-A"
	rep = reconcile([_row(t, fill_size=2)], [_buy(t, fill=3)], [_settle(t)],
	                in_scope_series=SERIES, expected_strategy="s")
	assert _finding_for(rep, t).material is True


def _settle_partial(ticker):
	"""Settlement for a 1-contract YES win: true_pnl = 100 - 50 cost - 3 fee = 47c."""
	return {"ticker": ticker, "market_result": "yes", "yes_count_fp": 1, "no_count_fp": 0,
	        "value": 100, "yes_total_cost_dollars": 0.50, "no_total_cost_dollars": 0.0, "fee_cost": 0.03}


def test_partial_entry_reconciles_against_partial_fill():
	t = "KXTEST15M-A"
	# Partial BUY: 1 of 3 filled, cost 0.50 -> blended 50c, fill_size 1; 1-contract
	# settlement -> true_pnl 47c. db records the partial correctly -> clean, tagged.
	buy = _buy(t, fill=1, initial=3)
	buy["taker_fill_cost_dollars"] = 0.50
	rep = reconcile([_row(t, fill_size=1, blended_entry_cents=50, pnl_cents=47)],
	                [buy], [_settle_partial(t)], in_scope_series=SERIES, expected_strategy="s")
	f = _finding_for(rep, t)
	assert f.material is False and "partial_entry" in f.detail


def test_db_rejected_with_filled_buy_is_material():
	t = "KXTEST15M-A"
	rep = reconcile([_row(t, status="rejected", pnl_cents=None)], [_buy(t)], [_settle(t)],
	                in_scope_series=SERIES, expected_strategy="s")
	f = _finding_for(rep, t)
	assert f.material is True and "status" in f.fields


def test_expected_no_settlement_status_skipped():
	t = "KXTEST15M-A"
	# lost_truth is an expected operational state; pnl not compared -> clean.
	rep = reconcile([_row(t, status="lost_truth", pnl_cents=-25)], [_buy(t)], [_settle(t)],
	                in_scope_series=SERIES, expected_strategy="s")
	assert _finding_for(rep, t).material is False


# ---------------------------------------------------------------------------
# Rejected / 0-fill rows must not be mistaken for PHANTOM (spec §5.3).
# debut-fade is an IOC taker; an IOC that fills 0 is correctly recorded
# status='rejected', fill_size=0 — it asserts NO position, so with no filled
# Kalshi BUY the db and exchange AGREE. Flagging it PHANTOM is a false positive.
# ---------------------------------------------------------------------------

def test_rejected_zero_fill_row_no_buy_is_not_phantom():
	t = "KXTEST15M-A"
	rep = reconcile([_row(t, status="rejected", fill_size=0, pnl_cents=None)], [], [],
	                in_scope_series=SERIES, expected_strategy="s")
	assert rep.findings == []
	assert rep.is_clean is True


def test_fill_claiming_row_no_buy_is_still_phantom():
	# Regression guard: a row that CLAIMS a position (fill_size>0) with no filled
	# Kalshi BUY is still a real PHANTOM (a recorded position that doesn't exist).
	t = "KXTEST15M-A"
	rep = reconcile([_row(t, fill_size=3)], [], [], in_scope_series=SERIES, expected_strategy="s")
	f = _finding_for(rep, t)
	assert f.outcome == Outcome.PHANTOM and f.material is True


def test_rejected_zero_fill_with_filled_buy_still_material():
	# The fill-parse #43 signature (status='rejected', fill_size=0) but Kalshi DID
	# fill must still be flagged — it routes to the matched path (buy present), not
	# the phantom branch, so the fix above must not swallow it.
	t = "KXTEST15M-A"
	rep = reconcile([_row(t, status="rejected", fill_size=0, pnl_cents=None)], [_buy(t)], [_settle(t)],
	                in_scope_series=SERIES, expected_strategy="s")
	f = _finding_for(rep, t)
	assert f.material is True and "status" in f.fields


def test_dual_slippage_skipped_when_absent():
	t = "KXTEST15M-A"
	row = _row(t)  # no market_impact_cents key (pre-#54 db)
	rep = reconcile([row], [_buy(t)], [_settle(t)], in_scope_series=SERIES,
	                expected_strategy="s", has_dual_slippage=False)
	assert _finding_for(rep, t).material is False


def test_misbooked_partial_is_material_and_still_tagged():
	t = "KXTEST15M-A"
	# 1-of-3 partial, but the db mis-books fill_size=3 -> material AND still tagged
	# partial_entry (the #52 re-certification path; the tag must survive a disagreement).
	buy = _buy(t, fill=1, initial=3)
	buy["taker_fill_cost_dollars"] = 0.50
	rep = reconcile([_row(t, fill_size=3, blended_entry_cents=50, pnl_cents=47)],
	                [buy], [_settle_partial(t)], in_scope_series=SERIES, expected_strategy="s")
	f = _finding_for(rep, t)
	assert f.material is True and "partial_entry" in f.detail and "fill_size" in f.fields


def test_dual_slippage_present_is_noop_today():
	t = "KXTEST15M-A"
	# Row carries the Phase-2 columns + has_dual_slippage=True. Per spec §10 the
	# present-path recompute is deferred, so this is a no-op gate -> clean.
	row = _row(t, market_impact_cents=2, limit_slippage_cents=3)
	rep = reconcile([row], [_buy(t)], [_settle(t)], in_scope_series=SERIES,
	                expected_strategy="s", has_dual_slippage=True)
	assert _finding_for(rep, t).material is False


def test_blended_none_skips_entry_comparison():
	t = "KXTEST15M-A"
	# A settled matched row with blended_entry_cents=None must skip the blended leg
	# cleanly (the is-not-None guard is load-bearing) -> clean.
	rep = reconcile([_row(t, blended_entry_cents=None)], [_buy(t)], [_settle(t)],
	                in_scope_series=SERIES, expected_strategy="s")
	assert _finding_for(rep, t).material is False


def test_non_terminal_status_on_settled_market_is_material():
	t = "KXTEST15M-A"
	# A settled+MATCHED row stuck at a non-terminal status (open/pending/exit_pending)
	# with pnl never written is a recording failure (#51/#52 class) the tool must surface
	# — not a silent CLEAN. (review C6)
	rep = reconcile([_row(t, status="open", pnl_cents=None)], [_buy(t)], [_settle(t)],
	                in_scope_series=SERIES, expected_strategy="s")
	f = _finding_for(rep, t)
	assert f.material is True and "status" in f.fields


def _settle_void(ticker):
	"""A void (refunded) settlement leg: market_result='void', cost present, no payout."""
	return {"ticker": ticker, "market_result": "void", "yes_count_fp": 0, "no_count_fp": 0,
	        "value": 100, "yes_total_cost_dollars": 1.50, "no_total_cost_dollars": 0.0, "fee_cost": 0.0}


def test_void_leg_excluded_from_true_pnl():
	t = "KXTEST15M-A"
	# A winning YES leg (true_pnl 147) PLUS a void leg (a refund, not a -150 loss) must
	# reconcile to 147, not 147-150 — only YES/NO legs contribute realized P&L. (review C10/void)
	rep = reconcile([_row(t, pnl_cents=147)], [_buy(t)], [_settle(t), _settle_void(t)],
	                in_scope_series=SERIES, expected_strategy="s")
	assert _finding_for(rep, t).material is False


def test_exit_phantom_annotated_with_categorized_sell():
	t = "KXTEST15M-A"
	# pnl disagreement (db 0 vs true 147) on a row that booked an exit whose SELL
	# actually 0-filled -> material, annotated with the exit cause (#51/#52 signature).
	# Real REST 0-fill IOC SELLs come back status="canceled" (not the FIX-only "expired").
	sell = {"ticker": t, "action": "sell", "side": "yes", "initial_count_fp": 3,
	        "fill_count_fp": 0, "status": "canceled", "taker_fill_cost_dollars": 0.0}
	rep = reconcile([_row(t, pnl_cents=0, exit_reason="ws_exit_fill")],
	                [_buy(t), sell], [_settle(t)], in_scope_series=SERIES, expected_strategy="s")
	f = _finding_for(rep, t)
	assert f.material is True and "canceled" in f.exit_quality and "exit IOC" in f.detail
	# NOTE: the "Exit-fill quality in to_markdown()" rendering assertion is deferred to
	# Task 5 (to_markdown is implemented there).


def test_zero_fill_buy_creates_no_entry():
	t = "KXTEST15M-A"
	# A 0-fill BUY (IOC that never entered) must NOT become MISSING/MATCHED (spec §5.1;
	# _filled_buy requires fill>0). No db row + no filled BUY -> empty universe -> no finding.
	rep = reconcile([], [_buy(t, fill=0)], [], in_scope_series=SERIES, expected_strategy="s")
	assert rep.findings == []


def test_multi_entry_via_multiple_db_rows():
	t = "KXTEST15M-A"
	# MULTI_ENTRY precedence via the >1-db-row leg of the guard (Task 3's committed test
	# only exercised >1 BUY). spec §5.1.
	rep = reconcile([_row(t), _row(t)], [_buy(t)], [_settle(t)],
	                in_scope_series=SERIES, expected_strategy="s")
	f = _finding_for(rep, t)
	assert f.outcome == Outcome.MULTI_ENTRY and f.material is True


# ---------------------------------------------------------------------------
# Task 5 — to_markdown: report rendering
# ---------------------------------------------------------------------------

def test_to_markdown_contains_verdict_and_counts():
	t = "KXTEST15M-A"
	rep = reconcile([_row(t, pnl_cents=0)], [_buy(t)], [_settle(t)],
	                in_scope_series=SERIES, expected_strategy="s")
	md = rep.to_markdown()
	assert "# Live-Execution Cross-Check" in md
	assert "NEEDS-REVISION" in md  # material finding present
	assert "pnl_cents" in md       # the disagreeing field is shown
	assert "matched" in md         # outcome counts table


def test_to_markdown_clean_verdict():
	t = "KXTEST15M-A"
	rep = reconcile([_row(t)], [_buy(t)], [_settle(t)], in_scope_series=SERIES, expected_strategy="s")
	assert "CLEAN" in rep.to_markdown()


def test_to_markdown_renders_exit_fill_quality_section():
	t = "KXTEST15M-A"
	# Re-homed from Task 4: an exit-phantom (pnl disagree + zero-fill SELL) must render
	# the §7 "Exit-fill quality" section once to_markdown exists.
	sell = {"ticker": t, "action": "sell", "side": "yes", "initial_count_fp": 3,
	        "fill_count_fp": 0, "status": "canceled", "taker_fill_cost_dollars": 0.0}
	rep = reconcile([_row(t, pnl_cents=0, exit_reason="ws_exit_fill")],
	                [_buy(t), sell], [_settle(t)], in_scope_series=SERIES, expected_strategy="s")
	md = rep.to_markdown()
	assert "Exit-fill quality" in md
	assert "canceled" in md
