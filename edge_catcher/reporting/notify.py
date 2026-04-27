"""Build a Notification envelope from the reporting module's report dict."""
from __future__ import annotations

from collections import defaultdict

from edge_catcher.notifications import Notification


def error_report_to_notification(report: dict) -> Notification:
	"""Build an error-severity Notification when generate_report returned an error.

	Title carries the date if present; body shows the error message; severity is 'error';
	payload is the full error report.
	"""
	date = report.get("date", "unknown")
	err = report.get("error", "unknown error")
	return Notification(
		title=f"Daily P&L FAILED — {date}",
		body=f"Error: {err}",
		severity="error",
		payload=report,
	)


def report_to_notification(report: dict) -> Notification:
	"""Convert generate_report() output into a multi-section Notification.

	Title is the date; body has Yesterday / All-time-by-strategy / Portfolio /
	Open-positions sections in plain text with simple markdown headers; severity
	flips to 'warn' when today's pnl is negative; payload is the full report
	dict so JSON-aware adapters (file, generic webhook) can consume the
	structured data.

	If `report` is missing the expected `all_time` or `today` keys, returns an
	error-severity Notification rather than raising — the CLI's notify path
	treats this as a delivery-able failure signal.
	"""
	at = report.get("all_time")
	today = report.get("today")
	date = report.get("date", "unknown")
	if not isinstance(at, dict) or not isinstance(today, dict):
		return Notification(
			title=f"Daily P&L MALFORMED — {date}",
			body=f"Report missing expected keys (all_time / today). Got: {sorted(report)}",
			severity="error",
			payload=report,
		)

	body_parts = [
		_section_yesterday(date, report.get("today_by_strategy") or []),
		_section_all_time_by_strategy(report.get("all_time_by_strategy") or []),
		_section_portfolio(at),
		_section_open_positions(report.get("open_positions") or []),
	]
	body = "\n\n".join(part for part in body_parts if part)

	severity = "info" if today.get("pnl_cents", 0) >= 0 else "warn"
	return Notification(
		title=f"Daily P&L — {date}",
		body=body,
		severity=severity,
		payload=report,
	)


def _section_yesterday(date: str, by_strategy: list) -> str:
	"""Aggregate (strategy, series) across won/lost statuses; one line per pair."""
	if not by_strategy:
		return f"**Yesterday ({date}):** No settled trades."
	# Aggregate: {(strategy, series): {"won": count_won, "lost": count_lost, "pnl_cents": net}}
	agg: dict = defaultdict(lambda: {"won": 0, "lost": 0, "pnl_cents": 0})
	for row in by_strategy:
		key = (row["strategy"], row["series_ticker"])
		status = row["status"]
		if status in ("won", "lost"):
			agg[key][status] += row["count"]
			agg[key]["pnl_cents"] += row["pnl_cents"]
	if not agg:
		# All rows had a status outside ("won", "lost") — degrade to the same
		# message as the empty-input case rather than emit a dangling header.
		return f"**Yesterday ({date}):** No settled trades."
	lines = [f"**Yesterday ({date}):**"]
	for (strategy, series), stats in sorted(agg.items()):
		w, lost = stats["won"], stats["lost"]
		total = w + lost
		wr = (w / total * 100) if total else 0
		pnl_usd = stats["pnl_cents"] / 100
		sign = "+" if stats["pnl_cents"] >= 0 else ""
		lines.append(
			f"  • {strategy} / {series}: {w}W / {lost}L | Net: {sign}${pnl_usd:.2f} | WR: {wr:.0f}%"
		)
	return "\n".join(lines)


def _section_all_time_by_strategy(rows: list) -> str:
	if not rows:
		return ""  # omit section entirely if no settled trades
	lines = ["**All-time by strategy:**"]
	for r in rows:
		strat = r["strategy"]
		total = r["closed_trades"]
		wins = r["wins"]
		pnl_usd = r["net_pnl_usd"]
		wr = r["win_rate_pct"]
		sign = "+" if r["net_pnl_cents"] >= 0 else ""
		lines.append(
			f"  • {strat}: {total} trades | Net: {sign}${pnl_usd:.2f} | WR: {wins}/{total} = {wr}%"
		)
	return "\n".join(lines)


def _section_portfolio(at: dict) -> str:
	pnl_usd = at.get("net_pnl_usd", 0)
	pnl_cents = at.get("net_pnl_cents", 0)
	deployed_usd = at.get("deployed_usd", 0)
	roi = at.get("roi_deployed_pct", 0)
	avg = at.get("avg_pnl_cents", 0)
	wr = at.get("win_rate_pct", 0)
	closed = at.get("closed_trades", 0)
	wins = at.get("wins", 0)
	sign = "+" if pnl_cents >= 0 else ""
	return (
		"**Portfolio:**\n"
		f"  • PnL: {sign}${pnl_usd:.2f} ({pnl_cents}¢)\n"
		f"  • Deployed: ${deployed_usd:.2f}\n"
		f"  • ROI: {roi}% (deployed)\n"
		f"  • Avg PnL/trade: {avg}¢\n"
		f"  • Overall WR: {wr}% ({wins}/{closed})"
	)


def _section_open_positions(rows: list) -> str:
	if not rows:
		return "**Open positions:** None."
	parts = [f"{r['strategy']}/{r['series_ticker']} ({r['count']})" for r in rows]
	return "**Open positions:** " + ", ".join(parts)
