"""Unit tests for the startup-reconcile -> Discord notification wiring.

`startup_reconcile` returns a `StartupReconcileReport`; the live boot path
(engine.py) turns operator-relevant outcomes into a `Notification` and fans it
out to the dedicated risk channel. Two pure-ish units under test:

* `_reconcile_alert_notification(report) -> Notification | None` — the DECISION
  (when to notify, what severity, what content). Pure; no I/O.
* `_emit_reconcile_report(report, channels)` — the GLUE (call the decision, send
  via the unified layer, swallow a delivery failure so a notify error can never
  crash a successfully-reconciled live engine).

The notification layer's `send` is the real seam: tests monkeypatch
`edge_catcher.notifications.send` (the source the glue imports at call time) and
assert on the captured `Notification` — never a hand-rolled mock of the engine.
"""
from __future__ import annotations

import logging

from edge_catcher.engine.engine import (
	_emit_reconcile_report,
	_reconcile_alert_notification,
)
from edge_catcher.live.reconciliation import StartupReconcileReport


def _report(**overrides: int) -> StartupReconcileReport:
	"""A StartupReconcileReport with the given non-zero fields (rest default 0)."""
	return StartupReconcileReport(**overrides)


# ---------------------------------------------------------------------------
# _reconcile_alert_notification — the decision
# ---------------------------------------------------------------------------


def test_clean_report_yields_no_notification() -> None:
	"""A fully clean reconcile (every count 0) returns None — no Discord noise
	on every boot. The trigger is operator-relevant outcomes only."""
	assert _reconcile_alert_notification(_report()) is None


def test_lost_truth_yields_error_severity() -> None:
	"""lost_truth (we believe we hold a position Kalshi has no record of — real
	money, manual investigation) escalates the notification to 'error'."""
	note = _reconcile_alert_notification(_report(lost_truth=1, alerts=1))
	assert note is not None
	assert note.severity == "error"


def test_orphans_only_yields_warn_severity() -> None:
	"""Orphan recoveries with NO lost_truth are 'warn' — Kalshi held a position
	we had no row for; auto-recovered, operator should confirm which strategy."""
	note = _reconcile_alert_notification(
		_report(orphan_positions_recovered=2, alerts=2)
	)
	assert note is not None
	assert note.severity == "warn"


def test_settled_only_fires_at_info_even_with_zero_alerts() -> None:
	"""settled_recovered>0 with alerts==0 is benign (positions settled while the
	daemon was down, now handed to the settlement poller) — but still surfaced,
	at 'info'. Proves the trigger is alerts>0 OR settled_recovered>0, not just
	alerts (the 'tackle it, don't omit settled' requirement)."""
	note = _reconcile_alert_notification(_report(settled_recovered=3))
	assert note is not None
	assert note.severity == "info"


def test_notification_body_includes_all_counts() -> None:
	"""The body must carry the full breakdown so the operator can act without
	opening the logs — every count, including the ones that did not trigger."""
	note = _reconcile_alert_notification(
		_report(
			pending_resolved=4,
			pending_post_hoc_rejected=1,
			orphan_positions_recovered=2,
			lost_truth=1,
			settled_recovered=3,
			alerts=3,
		)
	)
	assert note is not None
	for fragment in (
		"orphan_positions_recovered=2",
		"lost_truth=1",
		"settled_recovered=3",
		"pending_resolved=4",
		"pending_post_hoc_rejected=1",
	):
		assert fragment in note.body, f"missing {fragment!r} in body"


# ---------------------------------------------------------------------------
# _emit_reconcile_report — the glue (send + swallow)
# ---------------------------------------------------------------------------


def test_emit_does_not_send_on_clean_report(monkeypatch) -> None:
	"""A clean report produces no Notification, so the glue must NOT call send
	(no empty pings)."""
	calls: list = []
	monkeypatch.setattr(
		"edge_catcher.notifications.send",
		lambda note, channels: calls.append((note, channels)),
	)
	_emit_reconcile_report(_report(), ["live_risk_discord"])
	assert calls == []


def test_emit_sends_notification_to_channels_when_alerts(monkeypatch) -> None:
	"""An alerting report sends exactly one Notification to the channels it was
	handed (the boot-resolved risk channels)."""
	calls: list = []
	monkeypatch.setattr(
		"edge_catcher.notifications.send",
		lambda note, channels: calls.append((note, channels)),
	)
	channels = ["live_risk_discord"]
	_emit_reconcile_report(_report(lost_truth=1, alerts=1), channels)
	assert len(calls) == 1
	sent_note, sent_channels = calls[0]
	assert sent_note.severity == "error"
	assert sent_channels is channels


def test_emit_swallows_send_failure(monkeypatch, caplog) -> None:
	"""A delivery failure must NEVER propagate — reconcile already succeeded and
	the engine must not crash on a notification error (real-money post-boot
	safety; send() is documented never-raises, this is belt-and-suspenders)."""
	def _boom(note, channels):
		raise RuntimeError("discord down")

	monkeypatch.setattr("edge_catcher.notifications.send", _boom)
	with caplog.at_level(logging.ERROR):
		# Must not raise.
		_emit_reconcile_report(_report(lost_truth=1, alerts=1), ["live_risk_discord"])
	assert any(
		"notification" in r.message.lower() for r in caplog.records
	), "expected an ERROR log when the reconcile notification send fails"
