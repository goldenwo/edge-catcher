"""Temporal Consistency gate — tests performance across non-overlapping time windows."""

from __future__ import annotations

import logging
import math
import sqlite3
import time
from datetime import datetime, timedelta

from edge_catcher.research.hypothesis import Hypothesis, HypothesisResult

from .gate import Gate, GateContext, GateResult

logger = logging.getLogger(__name__)


class TemporalConsistencyGate(Gate):
	"""Fail strategies that don't hold up across different time regimes."""

	name = "temporal_consistency"

	def __init__(
		self,
		n_windows: int = 5,
		min_profitable_windows: float = 0.6,
		worst_window_sharpe_floor: float = -0.5,
		timeout_seconds: float = 1800,
	) -> None:
		self.n_windows = n_windows
		self.min_profitable_windows = min_profitable_windows
		self.worst_window_sharpe_floor = worst_window_sharpe_floor
		self.timeout_seconds = timeout_seconds

	def check(self, result: HypothesisResult, context: GateContext) -> GateResult:
		if context.agent is None:
			return GateResult(
				passed=False, gate_name=self.name,
				reason="no agent available for temporal consistency backtests",
				details={},
			)

		h = context.hypothesis

		start, end = self._resolve_dates(h)
		if start is None or end is None:
			return GateResult(
				passed=False, gate_name=self.name,
				reason="cannot determine date range for temporal consistency",
				details={},
			)

		windows = self._make_windows(start, end)
		if len(windows) < 3:
			# Pass as a review-tier soft pass rather than hard-fail. A
			# strategy running against a series with <15 days of history
			# shouldn't be demoted just because we can't meaningfully
			# partition time yet — let it through for downstream gates
			# and human review. Discovered during Task 5 sweep v2 analysis:
			# many crypto 15m series have 9-22 days of data, which were
			# being silently failed even with strong per-trade Sharpes.
			from datetime import datetime
			try:
				total_days = (datetime.fromisoformat(end) - datetime.fromisoformat(start)).days
			except Exception:
				total_days = 0
			return GateResult(
				passed=True, gate_name=self.name, tier="review",
				reason=(
					f"insufficient data for temporal consistency "
					f"({total_days} days, need >= 15) — passed as review"
				),
				details={"total_days": total_days, "windows_possible": len(windows)},
			)

		sharpes: list[float] = []
		profitable: list[bool] = []
		deadline = time.monotonic() + self.timeout_seconds

		for w_start, w_end in windows:
			if time.monotonic() > deadline:
				return GateResult(
					passed=False, gate_name=self.name,
					reason="temporal consistency timed out",
					details={"windows_completed": len(sharpes)},
				)

			w_h = Hypothesis(
				strategy=h.strategy, data_sources=h.data_sources,
				start_date=w_start, end_date=w_end, fee_pct=h.fee_pct,
			)
			data = context.agent.run_backtest_only(w_h)

			if data is None:
				continue
			trades = data.get("total_trades", 0)
			if trades < 10:
				continue

			# Normalize to per-trade Sharpe
			sr = data.get("sharpe", 0.0) / math.sqrt(trades) if trades >= 1 else 0.0
			sharpes.append(sr)
			profitable.append(data.get("net_pnl_cents", 0) > 0)

		if len(sharpes) < 3:
			return GateResult(
				passed=False, gate_name=self.name,
				reason=f"only {len(sharpes)} valid windows, need >= 3",
				details={"valid_windows": len(sharpes)},
			)

		profitable_pct = sum(profitable) / len(profitable)
		worst_sharpe = min(sharpes)

		details = {
			"sharpes": [round(s, 3) for s in sharpes],
			"profitable": profitable,
			"profitable_pct": round(profitable_pct, 3),
			"worst_sharpe": round(worst_sharpe, 3),
			"valid_windows": len(sharpes),
		}

		passed = (
			profitable_pct >= self.min_profitable_windows
			and worst_sharpe >= self.worst_window_sharpe_floor
		)

		gte = ">="
		lt = "<"
		reason = (
			f"profitable {profitable_pct:.0%} "
			f"({gte if profitable_pct >= self.min_profitable_windows else lt} {self.min_profitable_windows:.0%}), "
			f"worst Sharpe {worst_sharpe:.2f} "
			f"({gte if worst_sharpe >= self.worst_window_sharpe_floor else lt} {self.worst_window_sharpe_floor})"
		)

		return GateResult(passed=passed, gate_name=self.name, reason=reason, details=details)

	def _resolve_dates(self, h: Hypothesis) -> tuple[str | None, str | None]:
		"""Resolve start/end dates, querying DB if needed."""
		from pathlib import Path

		start = h.start_date
		end = h.end_date

		if start and end:
			return start, end

		try:
			db_path = str(Path("data") / h.data_sources.primaries[0].db)
			with sqlite3.connect(db_path) as conn:
				row = conn.execute(
					"SELECT MIN(open_time) as min_t, MAX(close_time) as max_t "
					"FROM markets WHERE series_ticker = ?",
					(h.series,),
				).fetchone()
			if row and row[0] and row[1]:
				db_start = row[0][:10]
				db_end = row[1][:10]
				return start or db_start, end or db_end
		except Exception as exc:
			logger.warning("temporal consistency: failed to query DB for dates: %s", exc)

		return None, None

	def _make_windows(
		self, start_str: str, end_str: str,
	) -> list[tuple[str, str]]:
		"""Split date range into non-overlapping (start, end) windows.

		Aims for ``self.n_windows`` windows of ``>= 7`` days each, but
		scales down to fewer, shorter windows on series with limited
		history so the gate can still meaningfully partition time.
		Requires at least 15 days total and ``>= 3`` windows of
		``>= 5`` days each — below that, returns an empty list and the
		caller short-circuits to a review-tier soft pass.
		"""
		start = datetime.fromisoformat(start_str)
		end = datetime.fromisoformat(end_str)
		total_days = (end - start).days

		MIN_TOTAL_DAYS = 15
		MIN_WINDOW_DAYS = 5
		MIN_WINDOWS = 3

		if total_days < MIN_TOTAL_DAYS:
			return []

		# Pick the largest window count that still gives each window
		# at least MIN_WINDOW_DAYS. Preserves the default n_windows=5
		# for year-long ranges while allowing n=3 on 15-34 day ranges.
		target_n = self.n_windows
		while target_n > MIN_WINDOWS and total_days / target_n < MIN_WINDOW_DAYS:
			target_n -= 1
		if total_days / target_n < MIN_WINDOW_DAYS:
			return []

		window_days = total_days / target_n
		windows: list[tuple[str, str]] = []

		for i in range(target_n):
			w_start = start + timedelta(days=i * window_days)
			w_end = start + timedelta(days=(i + 1) * window_days)
			if w_start >= w_end:
				continue
			windows.append((
				w_start.strftime("%Y-%m-%d"),
				w_end.strftime("%Y-%m-%d"),
			))

		return windows
