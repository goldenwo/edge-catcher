"""Tail-risk gate — catches strategies with a selling-deep-OTM payoff
signature (high win rate + asymmetrically large average loss).

Monte Carlo sign-flip and Deflated Sharpe gates both assume symmetric or
near-normal returns. A strategy that wins 88% of trades for a small premium
but occasionally takes a devastating loss (classic vol-seller shape) passes
those tests while being structurally fragile. This gate flags that pattern
directly from the realized ``pnl_values`` distribution.
"""

from __future__ import annotations

import logging
import statistics

from edge_catcher.research.hypothesis import HypothesisResult

from .gate import Gate, GateContext, GateResult

logger = logging.getLogger(__name__)


class TailRiskGate(Gate):
	"""Fail strategies with high win rate + asymmetrically large avg loss."""

	name = "tail_risk"

	def __init__(
		self,
		max_win_rate: float = 0.75,
		max_loss_win_ratio: float = 3.0,
		review_worst_to_median_ratio: float = 10.0,
		min_trades: int = 50,
	) -> None:
		self.max_win_rate = max_win_rate
		self.max_loss_win_ratio = max_loss_win_ratio
		self.review_worst_to_median_ratio = review_worst_to_median_ratio
		self.min_trades = min_trades

	def check(self, result: HypothesisResult, context: GateContext) -> GateResult:
		pnl = context.pnl_values
		T = len(pnl)

		if T < self.min_trades:
			return GateResult(
				passed=True, gate_name=self.name,
				reason=f"only {T} trades (< {self.min_trades}), tail-risk skipped",
				details={"T": T, "skipped": True},
			)

		wins = [p for p in pnl if p > 0]
		losses = [p for p in pnl if p < 0]
		n_wins = len(wins)
		n_losses = len(losses)
		win_rate = n_wins / T

		# Empty-wins edge case: strategy never wins. Leave judgment to other
		# gates (expected-value / DSR). Don't flag as tail-risk.
		if n_wins == 0:
			return GateResult(
				passed=True, gate_name=self.name,
				reason="no wins — not a vol-seller pattern",
				details={"T": T, "n_wins": 0, "win_rate": 0.0},
			)

		# Perfect win rate: no losses to be asymmetric with respect to.
		if n_losses == 0:
			return GateResult(
				passed=True, gate_name=self.name,
				reason="no realized losses in sample",
				details={"T": T, "n_wins": n_wins, "n_losses": 0,
				         "win_rate": round(win_rate, 4)},
			)

		# After the ``n_wins == 0`` and ``n_losses == 0`` early exits above,
		# ``wins`` and ``losses`` are both non-empty and strictly positive /
		# strictly negative respectively, so avg_win and median_win are
		# guaranteed > 0 — no zero-division guards needed.
		avg_win = statistics.mean(wins)
		avg_loss_mag = abs(statistics.mean(losses))
		loss_win_ratio = avg_loss_mag / avg_win

		median_win = statistics.median(wins)
		worst_loss_mag = abs(min(pnl))
		worst_to_median_win = worst_loss_mag / median_win

		details = {
			"T": T,
			"win_rate": round(win_rate, 4),
			"avg_win": round(avg_win, 4),
			"avg_loss": round(-avg_loss_mag, 4),
			"loss_win_ratio": round(loss_win_ratio, 4),
			"worst_loss": round(-worst_loss_mag, 4),
			"median_win": round(median_win, 4),
			"worst_loss_to_median_win": round(worst_to_median_win, 4),
		}

		# Vol-seller pattern: high win rate AND asymmetric avg loss
		if win_rate >= self.max_win_rate and loss_win_ratio >= self.max_loss_win_ratio:
			return GateResult(
				passed=False, gate_name=self.name,
				reason=(
					f"vol-seller pattern: win_rate {win_rate:.0%} >= {self.max_win_rate:.0%} "
					f"AND avg_loss/avg_win {loss_win_ratio:.1f}x >= {self.max_loss_win_ratio:.1f}x"
				),
				details=details,
			)

		# Catastrophic single loss relative to median win → review tier
		if worst_to_median_win >= self.review_worst_to_median_ratio:
			return GateResult(
				passed=True, gate_name=self.name,
				reason=(
					f"worst loss {worst_to_median_win:.1f}x the median win "
					f"(>= {self.review_worst_to_median_ratio:.1f}x) — flagged for review"
				),
				details=details,
				tier="review",
			)

		return GateResult(
			passed=True, gate_name=self.name,
			reason=(
				f"win_rate {win_rate:.0%}, avg_loss/avg_win {loss_win_ratio:.1f}x, "
				f"worst/median_win {worst_to_median_win:.1f}x — within tail-risk bounds"
			),
			details=details,
		)
