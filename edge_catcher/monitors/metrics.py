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
)
_GAUGE_KEYS = ("entries_skipped_unsupported",)


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
