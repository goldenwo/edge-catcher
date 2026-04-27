"""Build a Notification envelope from the reporting module's report dict."""
from __future__ import annotations

from edge_catcher.notifications import Notification


def report_to_notification(report: dict) -> Notification:
	"""Convert generate_report() output into a Notification.

	Title is the date; body is a one-line P&L summary; severity flips
	to 'warn' when today's pnl is negative; payload is the full report
	dict so JSON-aware adapters (file, generic webhook) can consume the
	structured data.
	"""
	at = report["all_time"]
	body = (
		f"Net: ${at['net_pnl_usd']:.2f} · "
		f"WR: {at['win_rate_pct']}% · "
		f"Trades: {at['closed_trades']} · "
		f"ROI: {at['roi_deployed_pct']}%"
	)
	severity = "info" if report["today"]["pnl_cents"] >= 0 else "warn"
	return Notification(
		title=f"Daily P&L — {report['date']}",
		body=body,
		severity=severity,
		payload=report,
	)
