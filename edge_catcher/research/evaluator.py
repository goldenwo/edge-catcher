"""Threshold-based verdict evaluation: kill / promote / explore."""

from __future__ import annotations

from dataclasses import dataclass

from .hypothesis import HypothesisResult


@dataclass
class Thresholds:
    min_sharpe: float = 1.0          # below this → kill
    min_trades: int = 50             # too few trades → inconclusive (explore, not kill)
    min_net_pnl_cents: float = 0.0   # must be positive after fees → kill if not
    promote_sharpe: float = 2.0      # above this → promote (requires all promote thresholds)


class Evaluator:
    def evaluate(
        self,
        result: HypothesisResult,
        thresholds: Thresholds | None = None,
    ) -> tuple[str, str]:
        """Return (verdict, reason). verdict is 'kill', 'promote', or 'explore'."""
        if thresholds is None:
            thresholds = Thresholds()

        if result.status == "error":
            return "kill", f"backtest error: {result.verdict_reason}"

        # Too few trades → inconclusive, promote for more exploration
        if result.total_trades < thresholds.min_trades:
            return (
                "explore",
                f"only {result.total_trades} trades (need ≥{thresholds.min_trades}) — inconclusive",
            )

        # Kill conditions — any single failure kills the hypothesis
        if result.net_pnl_cents <= thresholds.min_net_pnl_cents:
            return (
                "kill",
                f"net PnL {result.net_pnl_cents:.0f}¢ ≤ {thresholds.min_net_pnl_cents:.0f}¢",
            )
        if result.sharpe < thresholds.min_sharpe:
            return (
                "kill",
                f"Sharpe {result.sharpe:.2f} < {thresholds.min_sharpe:.2f}",
            )
        # Promote conditions
        if result.sharpe >= thresholds.promote_sharpe:
            return (
                "promote",
                f"Sharpe {result.sharpe:.2f} ≥ {thresholds.promote_sharpe:.2f}",
            )

        # Between kill and promote → explore further
        return (
            "explore",
            f"Sharpe {result.sharpe:.2f}, win rate {result.win_rate:.1%}, "
            f"PnL {result.net_pnl_cents:.0f}¢ — worth investigating",
        )
