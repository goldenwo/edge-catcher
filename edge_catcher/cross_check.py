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

Generalizes the proven P1 verdict prototype.
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

	def to_markdown(self) -> str:
		verdict = "CLEAN" if self.is_clean else "NEEDS-REVISION"
		lines = [
			"# Live-Execution Cross-Check",
			"",
			f"Verdict: **{verdict}** (exit code {self.exit_code}) — {self.n_tickers} in-scope tickers.",
			"Ground truth: Kalshi `/portfolio/orders` + `/portfolio/settlements` (read-only).",
			"",
			"## Outcome counts",
			"",
			"| Outcome | Count |",
			"|---|---|",
		]
		for outcome, n in sorted(self.counts().items()):
			lines.append(f"| {outcome} | {n} |")
		material = [f for f in self.findings if f.material]
		lines += ["", f"## Material findings ({len(material)})", ""]
		if not material:
			lines.append("_None — db reconciles with Kalshi ground truth._")
		else:
			lines += ["| Ticker | Outcome | Detail | Fields (db → kalshi) |", "|---|---|---|---|"]
			for f in material:
				flds = "; ".join(f"{k}: {v[0]}→{v[1]}" for k, v in f.fields.items()) or "—"
				lines.append(f"| {f.ticker} | {f.outcome.value} | {f.detail} | {flds} |")
		informational = [f for f in self.findings if not f.material and f.outcome is not Outcome.MATCHED]
		if informational:
			lines += ["", f"## Informational ({len(informational)})", ""]
			for f in informational:
				lines.append(f"- `{f.ticker}` **{f.outcome.value}** — {f.detail}")
		exit_cats = Counter(c for f in self.findings for c in f.exit_quality)
		if exit_cats:
			lines += ["", "## Exit-fill quality (IOC SELLs)", "", "| Outcome | Count |", "|---|---|"]
			for cat, n in sorted(exit_cats.items()):
				lines.append(f"| {cat} | {n} |")
		return "\n".join(lines)


def _in_scope(ticker: str, series: frozenset[str]) -> bool:
	# Match the series as a COMPLETE leading segment, not a bare char-prefix. Kalshi
	# tickers are "<SERIES>-<EVENT>-<OUTCOME>", and distinct series share leading chars
	# (e.g. KXXRP vs KXXRPD vs KXXRP15M, or KXBTC vs KXBTC15M), so a plain startswith
	# would over-scope a different series and manufacture spurious MISSING/UNATTRIBUTED.
	return any(ticker == s or ticker.startswith(f"{s}-") for s in series)


def _filled_buy(order: Mapping[str, Any]) -> bool:
	return order.get("action") == "buy" and _num(order.get("fill_count_fp")) > 0


def _asserts_fill(row: Mapping[str, Any]) -> bool:
	"""A db row claims a real position iff it recorded a positive fill. A rejected /
	0-fill row (an IOC that didn't fill, correctly recorded status='rejected',
	fill_size=0) asserts NO position and must not be mistaken for a PHANTOM."""
	return _num(row.get("fill_size")) > 0


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
	ticker universe (e.g. {"KXTEST15M"}); out-of-scope tickers are ignored. When
	``expected_strategy`` is set, an in-scope filled BUY with no db row is MISSING;
	otherwise UNATTRIBUTED (can't assert bot ownership). ``has_dual_slippage`` is
	decided once by the caller via PRAGMA table_info; consumed by the dual-slippage gate.
	"""
	thresholds = thresholds or DEFAULT_THRESHOLDS

	# Bucket db rows + filled BUYs by ticker, scoped.
	rows_by_ticker: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
	for r in live_rows:
		if _in_scope(r["ticker"], in_scope_series):
			rows_by_ticker[r["ticker"]].append(r)
	# Bucket filled BUYs + SELLs in ONE pass over orders (one _in_scope check per order).
	buys_by_ticker: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
	sells_by_ticker: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
	for o in orders:
		ot = o.get("ticker", "")
		if not _in_scope(ot, in_scope_series):
			continue
		if _filled_buy(o):
			buys_by_ticker[ot].append(o)
		elif o.get("action") == "sell":
			sells_by_ticker[ot].append(o)
	setts_by_ticker: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
	for s in settlements:
		if _in_scope(s.get("ticker", ""), in_scope_series):
			setts_by_ticker[s["ticker"]].append(s)

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
			# PHANTOM only when a db row CLAIMS a position (fill_size>0) yet Kalshi shows
			# no filled BUY — a recorded position that doesn't exist on the exchange. A
			# rejected/0-fill row asserts no position, so with no Kalshi BUY the db and
			# exchange AGREE: a correctly recorded rejection, not a discrepancy (spec §5.3).
			# strat-34 is an IOC taker whose entries often 0-fill, so this must not
			# false-positive into a material PHANTOM on every rejection.
			if any(_asserts_fill(r) for r in rows):
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
		# MATCHED + settled: reconcile entry/terminal fields against Kalshi ground truth.
		findings.append(_compare_fields(
			rows[0], buys[0], setts, sells_by_ticker.get(t, []), has_dual_slippage, thresholds,
		))

	return CrossCheckReport(findings=findings, n_tickers=len(tickers))


# Terminal db statuses with NO Kalshi settlement counterpart (0003 DDL enum) —
# expected operational states, never treated as won/lost disagreements (spec §5.3).
_EXPECTED_NO_SETTLEMENT = frozenset({"cancelled", "rejected_post_hoc", "lost_truth"})

# Non-terminal db statuses (0003 DDL): on a SETTLED market a row still in one of these
# never advanced — a recording failure the cross-check must surface (the #51/#52 class).
_NON_TERMINAL_STATUSES = frozenset({"pending", "open", "exit_pending"})

# IOC exit outcomes that indicate a problem worth surfacing (parallel to the sets above).
_BAD_IOC_EXIT_OUTCOMES = frozenset({"zero_fill", "canceled", "partial"})


def _kalshi_blended_cents(buy: Mapping[str, Any]) -> int:
	fills = _num(buy.get("fill_count_fp"))
	if fills <= 0:
		return 0
	return round((_num(buy.get("taker_fill_cost_dollars")) / fills) * 100)


def _compare_fields(
	row: Mapping[str, Any],
	buy: Mapping[str, Any],
	setts: list[Mapping[str, Any]],
	exits: list[Mapping[str, Any]],
	has_dual_slippage: bool,
	thresholds: Mapping[str, int],
) -> Finding:
	"""Compare a MATCHED+settled db row against Kalshi-derived ground truth (spec §5.3/§5.4).

	``exits`` = the ticker's SELL orders, categorized for the §7 exit-fill-quality report
	and to annotate the #51/#52 exit-phantom cause onto a terminal disagreement.
	"""
	t = row["ticker"]
	diffs: dict[str, tuple[Any, Any]] = {}
	material = False
	partial = 0 < _num(buy.get("fill_count_fp")) < _num(buy.get("initial_count_fp"))

	# Entry: blended (threshold) + fill_size (any mismatch). Partial entries compare
	# against the PARTIAL fill (taker_fill_cost / fill_count already reflects it).
	k_blended = _kalshi_blended_cents(buy)
	db_blended = row.get("blended_entry_cents")
	if db_blended is not None and abs(int(db_blended) - k_blended) > thresholds["blended_entry_cents"]:
		diffs["blended_entry_cents"] = (db_blended, k_blended)
		material = True
	k_fill = int(round(_num(buy.get("fill_count_fp"))))
	if row.get("fill_size") is not None and int(row["fill_size"]) != k_fill:
		diffs["fill_size"] = (row.get("fill_size"), k_fill)
		material = True

	# Terminal: pnl is the truth-signal; status is structural only (spec §5.3).
	status = row.get("status")
	if status in _NON_TERMINAL_STATUSES:
		# Settled market but the db row never reached a terminal state — a recording
		# failure (the #51/#52 class) the tool exists to surface.
		diffs["status"] = (status, "settled-market-but-row-non-terminal")
		material = True
	elif status not in _EXPECTED_NO_SETTLEMENT:
		if status == "rejected":
			diffs["status"] = ("rejected", "filled-BUY-exists")
			material = True
		else:
			# Sum only YES/NO legs: a void/scalar settlement is a refund, not a realized
			# loss, so it must not contribute its cost as negative P&L (else a false diff).
			true_pnl = sum(true_pnl_cents(s) for s in setts if s.get("market_result") in ("yes", "no"))
			db_pnl = row.get("pnl_cents")
			if db_pnl is not None and abs(int(db_pnl) - true_pnl) >= thresholds["pnl_cents"]:
				diffs["pnl_cents"] = (db_pnl, true_pnl)
				material = True

	# Dual-slippage (Phase-2 model-input fields): present only when the column exists
	# (PRAGMA decided once by the caller). Per spec §10 today's db lacks these columns,
	# so the numeric recompute-vs-Kalshi-refs is deferred until live produces them —
	# intentional no-op gate, pinned by test_dual_slippage_present_is_noop_today.
	# FAIL-LOUD CONTRACT: when the columns DO appear, this MUST be implemented before
	# is_clean can be trusted for them — do NOT leave as `pass`. The wrapper warns the
	# operator (stderr) whenever has_dual_slippage is True, so a present-but-unchecked
	# run is never silently certified clean.
	if has_dual_slippage:
		pass

	# Exit-fill quality (spec §7) + exit-phantom annotation (§5.3): categorize the
	# ticker's SELL IOCs. The #51/#52 phantom shows as a pnl disagreement on a row that
	# booked an exit whose SELL actually zero-filled / canceled — surface that cause.
	exit_quality = tuple(categorize_exit(o) for o in exits)

	# Tag persists whether or not the partial was mis-booked (#52 re-cert path).
	tag = " (partial_entry)" if partial else ""
	if diffs:
		detail = "field disagreement" + tag + ": " + ", ".join(diffs)
	else:
		detail = "matched" + tag
	bad_exits = sorted(set(exit_quality) & _BAD_IOC_EXIT_OUTCOMES)
	if diffs and bad_exits:
		detail += f" [exit IOC: {', '.join(bad_exits)}]"
	return Finding(t, Outcome.MATCHED, material, detail, fields=diffs, exit_quality=exit_quality)
