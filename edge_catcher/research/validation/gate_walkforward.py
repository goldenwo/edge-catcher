"""Walk-Forward Analysis gate — tests out-of-sample performance."""

from __future__ import annotations

import logging
import sqlite3
import time
from datetime import datetime, timedelta

from edge_catcher.research.hypothesis import Hypothesis, HypothesisResult

from .gate import Gate, GateContext, GateResult

logger = logging.getLogger(__name__)


class WalkForwardGate(Gate):
	"""Fail strategies that don't hold up out-of-sample in rolling windows."""

	name = "walk_forward"

	def __init__(
		self,
		n_windows: int = 5,
		oos_ratio: float = 0.3,
		min_oos_sharpe_ratio: float = 0.5,
		min_profitable_windows: float = 0.6,
		timeout_seconds: float = 1800,  # 30 minutes
	) -> None:
		self.n_windows = n_windows
		self.oos_ratio = oos_ratio
		self.min_oos_sharpe_ratio = min_oos_sharpe_ratio
		self.min_profitable_windows = min_profitable_windows
		self.timeout_seconds = timeout_seconds

	def check(self, result: HypothesisResult, context: GateContext) -> GateResult:
		if context.agent is None:
			return GateResult(
				passed=False, gate_name=self.name,
				reason="no agent available for walk-forward backtests",
				details={},
			)

		h = context.hypothesis

		# Determine date range
		start, end = self._resolve_dates(h)
		if start is None or end is None:
			return GateResult(
				passed=False, gate_name=self.name,
				reason="cannot determine date range for walk-forward",
				details={},
			)

		# Split into windows
		windows = self._make_windows(start, end)
		if len(windows) < 3:
			return GateResult(
				passed=False, gate_name=self.name,
				reason=f"only {len(windows)} windows possible, need ≥3",
				details={},
			)

		is_sharpes: list[float] = []
		oos_sharpes: list[float] = []
		oos_profitable: list[bool] = []
		deadline = time.monotonic() + self.timeout_seconds

		for is_start, is_end, oos_start, oos_end in windows:
			if time.monotonic() > deadline:
				return GateResult(
					passed=False, gate_name=self.name,
					reason="walk-forward timed out",
					details={"windows_completed": len(is_sharpes)},
				)

			# Run IS backtest
			is_h = Hypothesis(
				strategy=h.strategy, series=h.series, db_path=h.db_path,
				start_date=is_start, end_date=is_end, fee_pct=h.fee_pct,
			)
			is_data = context.agent.run_backtest_only(is_h)

			# Run OOS backtest
			oos_h = Hypothesis(
				strategy=h.strategy, series=h.series, db_path=h.db_path,
				start_date=oos_start, end_date=oos_end, fee_pct=h.fee_pct,
			)
			oos_data = context.agent.run_backtest_only(oos_h)

			# Skip window if either segment has insufficient trades
			if is_data is None or oos_data is None:
				continue
			if is_data.get("total_trades", 0) < 10 or oos_data.get("total_trades", 0) < 10:
				continue

			is_sharpes.append(is_data.get("sharpe", 0.0))
			oos_sharpes.append(oos_data.get("sharpe", 0.0))
			oos_profitable.append(oos_data.get("net_pnl_cents", 0) > 0)

		# Need at least 3 valid windows
		if len(is_sharpes) < 3:
			return GateResult(
				passed=False, gate_name=self.name,
				reason=f"only {len(is_sharpes)} valid windows, need ≥3",
				details={"valid_windows": len(is_sharpes)},
			)

		# Compute aggregate metrics
		mean_is = sum(is_sharpes) / len(is_sharpes)
		mean_oos = sum(oos_sharpes) / len(oos_sharpes)
		sharpe_ratio = mean_oos / mean_is if mean_is > 0 else 0.0
		profitable_pct = sum(oos_profitable) / len(oos_profitable)

		details = {
			"is_sharpes": [round(s, 3) for s in is_sharpes],
			"oos_sharpes": [round(s, 3) for s in oos_sharpes],
			"oos_profitable": oos_profitable,
			"sharpe_ratio": round(sharpe_ratio, 3),
			"profitable_pct": round(profitable_pct, 3),
			"valid_windows": len(is_sharpes),
		}

		passed = (
			sharpe_ratio >= self.min_oos_sharpe_ratio
			and profitable_pct >= self.min_profitable_windows
		)

		reason = (
			f"OOS/IS Sharpe ratio {sharpe_ratio:.2f} "
			f"({'≥' if sharpe_ratio >= self.min_oos_sharpe_ratio else '<'} {self.min_oos_sharpe_ratio}), "
			f"profitable {profitable_pct:.0%} "
			f"({'≥' if profitable_pct >= self.min_profitable_windows else '<'} {self.min_profitable_windows:.0%})"
		)

		return GateResult(passed=passed, gate_name=self.name, reason=reason, details=details)

	def _resolve_dates(self, h: Hypothesis) -> tuple[str | None, str | None]:
		"""Resolve start/end dates, querying DB if needed."""
		start = h.start_date
		end = h.end_date

		if start and end:
			return start, end

		# Query DB for actual data range
		try:
			with sqlite3.connect(h.db_path) as conn:
				row = conn.execute(
					"SELECT MIN(open_time) as min_t, MAX(close_time) as max_t "
					"FROM markets WHERE series_ticker = ?",
					(h.series,),
				).fetchone()
			if row and row[0] and row[1]:
				db_start = row[0][:10]  # ISO date portion
				db_end = row[1][:10]
				return start or db_start, end or db_end
		except Exception as exc:
			logger.warning("walk-forward: failed to query DB for dates: %s", exc)

		return None, None

	def _make_windows(
		self, start_str: str, end_str: str,
	) -> list[tuple[str, str, str, str]]:
		"""Split date range into (is_start, is_end, oos_start, oos_end) tuples."""
		start = datetime.fromisoformat(start_str)
		end = datetime.fromisoformat(end_str)
		total_days = (end - start).days

		if total_days < self.n_windows * 7:  # need at least a week per window
			return []

		window_days = total_days / self.n_windows
		oos_days = window_days * self.oos_ratio
		is_days = window_days - oos_days

		windows: list[tuple[str, str, str, str]] = []
		for i in range(self.n_windows):
			w_start = start + timedelta(days=i * window_days)
			is_end_dt = w_start + timedelta(days=is_days)
			oos_start_dt = is_end_dt + timedelta(days=1)
			oos_end_dt = w_start + timedelta(days=window_days)

			if oos_start_dt >= oos_end_dt:
				continue  # window too small for a gap

			windows.append((
				w_start.strftime("%Y-%m-%d"),
				is_end_dt.strftime("%Y-%m-%d"),
				oos_start_dt.strftime("%Y-%m-%d"),
				oos_end_dt.strftime("%Y-%m-%d"),
			))

		return windows
