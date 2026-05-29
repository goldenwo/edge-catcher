"""Live-execution cross-check — reconcile live_trades.db against Kalshi ground truth.

Read-only validation tool (Phase 1.5 §5.2.1). Compares recorded live_trades rows
against Kalshi /portfolio/orders + /portfolio/settlements to surface recording
discrepancies (phantom rows, missing rows, field disagreements) BEFORE Phase 2 fits
an empirical slippage model to the data — three live-recording bugs (#43/#51/#52)
landed in six weeks, so the data must be proven against the exchange first.

PURE: no I/O (db, filesystem, network) and NO imports from edge_catcher.live — that
package's __init__ eagerly imports the order-placing KalshiOrderClient, and this tool
must have no order path. The wrapper (scripts/cross_check_live.py) does all I/O and
passes plain dict/row lists in.

DISTINCT from edge_catcher.live.reconciliation (real-time pending-order reconcile
during trading). This is OFFLINE db-vs-exchange validation; its report type is
CrossCheckReport (cf. live.reconciliation.StartupReconcileReport).

Generalizes the proven P1 prototype (analyze_debut_fade_verdict.py).
"""
from __future__ import annotations

from enum import Enum
from typing import Any, Mapping


def _num(x: Any) -> float:
	"""Coerce a Kalshi numeric-ish field to float; non-numeric -> 0.0."""
	try:
		return float(x)
	except (TypeError, ValueError):
		return 0.0


def true_pnl_cents(settlement: Mapping[str, Any]) -> int:
	"""P&L (cents) from one Kalshi settlement record. Validated in P1 vs db id39 = -29c.

	pnl = winning_count*value - (yes_total_cost + no_total_cost) - fee_cost.
	NOTE: an empty/absent market_result yields a 0-payout figure; callers must gate
	terminal comparison on settled-ness (see reconcile UNSETTLED handling), NOT treat
	this as a real loss.
	"""
	res = settlement.get("market_result")
	yc = _num(settlement.get("yes_count_fp"))
	nc = _num(settlement.get("no_count_fp"))
	val = _num(settlement.get("value")) or 100.0
	win = yc if res == "yes" else (nc if res == "no" else 0)
	payout = win * val
	cost = (_num(settlement.get("yes_total_cost_dollars")) + _num(settlement.get("no_total_cost_dollars"))) * 100
	fee = _num(settlement.get("fee_cost")) * 100
	return round(payout - cost - fee)


def categorize_exit(order: Mapping[str, Any]) -> str:
	"""Classify an exit (SELL) IOC outcome: filled / partial / zero_fill / canceled / other."""
	initial = _num(order.get("initial_count_fp"))
	filled = _num(order.get("fill_count_fp"))
	status = (order.get("status") or "").lower()
	if filled >= initial > 0:
		return "filled"
	if 0 < filled < initial:
		return "partial"
	if filled == 0 and "cancel" in status:
		return "canceled"
	if filled == 0:
		return "zero_fill"
	return "other"


class Outcome(str, Enum):
	"""Per-ticker reconciliation outcome (spec §5.3)."""

	MATCHED = "matched"
	PHANTOM = "phantom"            # db row, no filled Kalshi BUY
	MISSING = "missing"           # expected bot fill, no db row
	UNATTRIBUTED = "unattributed"  # in-series fill, not attributable to the bot strategy
	MULTI_ENTRY = "multi_entry"    # >1 BUY or >1 db row for a ticker (assumption broke)
	UNSETTLED = "unsettled"        # matched, no settlement yet


# Material thresholds (spec §5.4). price/blended: > N cents; pnl: >= N cents.
# fill_size + structural status mismatches are material with NO numeric threshold.
DEFAULT_THRESHOLDS: Mapping[str, int] = {
	"blended_entry_cents": 1,
	"pnl_cents": 5,
}
