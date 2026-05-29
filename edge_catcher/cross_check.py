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

from collections import Counter, defaultdict
from dataclasses import dataclass, field
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


@dataclass(frozen=True)
class Finding:
	"""One reconciliation finding for a ticker.

	``fields`` maps a field name to (db_value, kalshi_value) for field-disagreement
	findings; empty for structural outcomes. ``material`` drives is_clean (spec §5.4).
	"""

	ticker: str
	outcome: Outcome
	material: bool
	detail: str
	fields: Mapping[str, tuple[Any, Any]] = field(default_factory=dict)
	exit_quality: tuple[str, ...] = ()  # categorize_exit() per SELL IOC (§7 report + §5.3 annotation)


@dataclass(frozen=True)
class CrossCheckReport:
	findings: list[Finding]
	n_tickers: int

	@property
	def is_clean(self) -> bool:
		"""True iff no MATERIAL finding (spec §5.4). UNATTRIBUTED/UNSETTLED/noise are not material."""
		return not any(f.material for f in self.findings)

	@property
	def exit_code(self) -> int:
		"""Derived (single source of truth): 0 iff clean."""
		return 0 if self.is_clean else 1

	def counts(self) -> dict[str, int]:
		"""Count of findings per outcome value (for the report summary)."""
		return dict(Counter(f.outcome.value for f in self.findings))


def _in_scope(ticker: str, series: frozenset[str]) -> bool:
	return any(ticker.startswith(s) for s in series)


def _filled_buy(order: Mapping[str, Any]) -> bool:
	return order.get("action") == "buy" and _num(order.get("fill_count_fp")) > 0


def reconcile(
	live_rows: list[Mapping[str, Any]],
	orders: list[Mapping[str, Any]],
	settlements: list[Mapping[str, Any]],
	*,
	in_scope_series: frozenset[str],
	expected_strategy: str | None = None,
	has_dual_slippage: bool = False,
	thresholds: Mapping[str, int] | None = None,
) -> CrossCheckReport:
	"""Reconcile live_trades rows against Kalshi orders + settlements (spec §5).

	All inputs are plain dict/row lists (no I/O here). ``in_scope_series`` defines the
	ticker universe (e.g. {"KXETH15M"}); out-of-scope tickers are ignored. When
	``expected_strategy`` is set, an in-scope filled BUY with no db row is MISSING;
	otherwise UNATTRIBUTED (can't assert bot ownership). ``has_dual_slippage`` is
	decided once by the caller via PRAGMA table_info (Task 4 uses it).
	"""
	thresholds = thresholds or DEFAULT_THRESHOLDS

	# Bucket db rows + filled BUYs by ticker, scoped.
	rows_by_ticker: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
	for r in live_rows:
		if _in_scope(r["ticker"], in_scope_series):
			rows_by_ticker[r["ticker"]].append(r)
	buys_by_ticker: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
	for o in orders:
		if _filled_buy(o) and _in_scope(o.get("ticker", ""), in_scope_series):
			buys_by_ticker[o["ticker"]].append(o)
	setts_by_ticker: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
	for s in settlements:
		if _in_scope(s.get("ticker", ""), in_scope_series):
			setts_by_ticker[s["ticker"]].append(s)
	sells_by_ticker: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
	for o in orders:
		if o.get("action") == "sell" and _in_scope(o.get("ticker", ""), in_scope_series):
			sells_by_ticker[o["ticker"]].append(o)

	findings: list[Finding] = []
	tickers = sorted(set(rows_by_ticker) | set(buys_by_ticker))
	for t in tickers:
		rows = rows_by_ticker.get(t, [])
		buys = buys_by_ticker.get(t, [])
		setts = setts_by_ticker.get(t, [])

		if len(rows) > 1 or len(buys) > 1:
			findings.append(Finding(t, Outcome.MULTI_ENTRY, True,
				f"{len(rows)} db rows / {len(buys)} filled BUYs — one-entry-per-ticker assumption broke"))
			continue
		if rows and not buys:
			findings.append(Finding(t, Outcome.PHANTOM, True, "db row asserts a fill with no filled Kalshi BUY"))
			continue
		if buys and not rows:
			if expected_strategy is not None:
				findings.append(Finding(t, Outcome.MISSING, True,
					"filled Kalshi BUY with no live_trades row (eyeball for a manual trade on a bot series)"))
			else:
				findings.append(Finding(t, Outcome.UNATTRIBUTED, False,
					"in-series filled BUY, no db row, no --strategy to attribute it to the bot"))
			continue
		# Matched: one row + one filled BUY.
		settled = any((s.get("market_result") not in ("", None)) for s in setts)
		if not settled:
			findings.append(Finding(t, Outcome.UNSETTLED, False,
				"filled BUY present, no settled Kalshi result yet — terminal fields not compared"))
			continue
		# MATCHED + settled: field comparison is added in Task 4.
		findings.append(_compare_fields(
			rows[0], buys[0], setts, sells_by_ticker.get(t, []), has_dual_slippage, thresholds,
		))

	return CrossCheckReport(findings=findings, n_tickers=len(tickers))


def _compare_fields(
	row: Mapping[str, Any],
	buy: Mapping[str, Any],
	setts: list[Mapping[str, Any]],
	exits: list[Mapping[str, Any]],
	has_dual_slippage: bool,
	thresholds: Mapping[str, int],
) -> Finding:
	# Task 4 fills this in; for now a matched row is clean.
	return Finding(row["ticker"], Outcome.MATCHED, False, "matched")
