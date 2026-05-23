"""Operational metrics counter for the paper trader's summary log.

Counters reset each interval and answer "what happened this interval?".
Gauges persist across intervals — they're set once (e.g., at startup) and
stay visible in every summary line. The split prevents a caller from
accidentally using inc() on a gauge and erasing its persistence semantics.
"""

from __future__ import annotations

from dataclasses import dataclass, field

_COUNTER_KEYS = (
	"entries_attempted",
	"entries_filled",
	"entries_skipped_stale",
	"entries_skipped_other",
	"trades_settled_won",
	"trades_settled_lost",
	# Risk-gate counters (C) — incremented by Gate.gate_entry on every call.
	# `risk_gate_decisions_total` is split into two counters: one for Allow,
	# one for each Reject reason (label emulation via key suffix).
	"risk_gate_allowed",           # decision=allow
	"risk_gate_rejected_operator", # reason=KILL_OPERATOR
	"risk_gate_rejected_panic",    # reason=KILL_AUTO_PANIC
	"risk_gate_rejected_drawdown", # reason=KILL_AUTO_DRAWDOWN
	"risk_gate_rejected_daily",    # reason=KILL_AUTO_DAILY
	"risk_gate_rejected_invalid",  # reason=INVALID_SIGNAL
	"risk_gate_rejected_max_open",         # reason=MAX_OPEN
	"risk_gate_rejected_min_fill",         # reason=BELOW_MIN_FILL
	"risk_gate_rejected_stale_bankroll",   # reason=STALE_BANKROLL
	# Bankroll cache refresh failure counter.
	"risk_bankroll_refresh_failures_total",
	# Pending + unhandled-status counters for the dispatch pending branch.
	"entries_pending",          # executor returned pending (NetworkError / timeout / malformed-fills)
	"entries_unhandled_status", # defensive: OrderResult.status outside the known Literal set
	# Lost-CAS fill counter (filled IOC but durable row already left pending).
	"entries_filled_lost_cas",
)
_GAUGE_KEYS = (
	"entries_skipped_unsupported",
	# Risk gauges (C) — polled lazily by E; set after each gate call / refresh.
	"risk_kill_active_operator",    # 0/1 — operator kill active
	"risk_kill_active_auto_panic",  # 0/1 — auto-panic kill active
	"risk_kill_active_auto_drawdown", # 0/1 — auto-drawdown kill active
	"risk_kill_active_auto_daily",  # 0/1 — auto-daily kill active
	"risk_equity_cents",            # current computed equity in cents
	"risk_peak_cents",              # closed-equity peak in cents
	"risk_daily_pnl_cents",         # daily P&L in cents (may be negative)
	"risk_bankroll_age_seconds",    # seconds since last bankroll cache refresh
)


# Mapping from GateRejectReason literals to their counter key.
# Gate uses this to inc() the right counter without a large if/elif chain.
_GATE_REJECT_COUNTER: dict[str, str] = {
	"KILL_OPERATOR": "risk_gate_rejected_operator",
	"KILL_AUTO_PANIC": "risk_gate_rejected_panic",
	"KILL_AUTO_DRAWDOWN": "risk_gate_rejected_drawdown",
	"KILL_AUTO_DAILY": "risk_gate_rejected_daily",
	"INVALID_SIGNAL": "risk_gate_rejected_invalid",
	"MAX_OPEN": "risk_gate_rejected_max_open",
	"BELOW_MIN_FILL": "risk_gate_rejected_min_fill",
	"STALE_BANKROLL": "risk_gate_rejected_stale_bankroll",
}


@dataclass
class Metrics:
	"""Per-interval counters plus persistent gauges for the paper trader."""

	_counters: dict[str, int] = field(
		default_factory=lambda: {k: 0 for k in _COUNTER_KEYS}
	)
	_gauges: dict[str, int] = field(
		default_factory=lambda: {k: 0 for k in _GAUGE_KEYS}
	)

	def inc(self, key: str) -> None:
		if key not in self._counters:
			raise KeyError(f"{key!r} is not a counter (gauges use set_gauge)")
		self._counters[key] += 1

	def set_gauge(self, key: str, value: int) -> None:
		if key not in self._gauges:
			raise KeyError(f"{key!r} is not a gauge")
		self._gauges[key] = value

	def snapshot(self) -> dict[str, int]:
		return {**self._counters, **self._gauges}

	def reset_and_snapshot(self) -> dict[str, int]:
		snap = self.snapshot()
		for k in self._counters:
			self._counters[k] = 0
		return snap
