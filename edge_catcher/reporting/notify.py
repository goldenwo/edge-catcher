"""Build a Notification envelope from the reporting module's report dict."""
from __future__ import annotations

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
	"""Convert generate_report() output into a Notification.

	Title is the date; body is a one-line P&L summary; severity flips
	to 'warn' when today's pnl is negative; payload is the full report
	dict so JSON-aware adapters (file, generic webhook) can consume the
	structured data.

	If `report` is missing the expected `all_time` or `today` keys,
	returns an error-severity Notification rather than raising — the
	CLI's notify path treats this as a delivery-able failure signal.
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
	body = (
		f"Net: ${at.get('net_pnl_usd', 0):.2f} · "
		f"WR: {at.get('win_rate_pct', 0)}% · "
		f"Trades: {at.get('closed_trades', 0)} · "
		f"ROI: {at.get('roi_deployed_pct', 0)}%"
	)
	severity = "info" if today.get("pnl_cents", 0) >= 0 else "warn"
	return Notification(
		title=f"Daily P&L — {date}",
		body=body,
		severity=severity,
		payload=report,
	)
