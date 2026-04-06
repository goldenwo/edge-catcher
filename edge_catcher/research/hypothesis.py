"""Hypothesis and HypothesisResult dataclasses."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field


@dataclass
class Hypothesis:
    strategy: str               # strategy name (e.g. 'Cvol', 'D', 'Cstack')
    series: str                 # series ticker (e.g. 'KXBTCD')
    db_path: str                # path to database
    start_date: str | None = None   # ISO date (e.g. '2025-01-01'), None = all data
    end_date: str | None = None     # ISO date (e.g. '2025-12-31'), None = all data
    fee_pct: float = 1.0        # fee multiplier (1.0 = full taker, 0.25 = maker)
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    parent_id: str | None = None
    tags: list[str] = field(default_factory=list)
    notes: str = ""

    def dedup_key(self) -> tuple:
        """Unique key for deduplication (strategy, series, db, start, end, fee)."""
        return (self.strategy, self.series, self.db_path,
                self.start_date, self.end_date, self.fee_pct)


@dataclass
class HypothesisResult:
    hypothesis: Hypothesis
    status: str                 # 'ok' or 'error'
    total_trades: int
    wins: int
    losses: int
    win_rate: float
    net_pnl_cents: float
    sharpe: float
    max_drawdown_pct: float
    fees_paid_cents: float
    avg_win_cents: float
    avg_loss_cents: float
    per_strategy: dict
    verdict: str                # 'kill', 'promote', 'explore'
    verdict_reason: str
    raw_json: dict              # full backtester output

    @classmethod
    def error(cls, hypothesis: Hypothesis, message: str) -> "HypothesisResult":
        """Create a failed result."""
        return cls(
            hypothesis=hypothesis,
            status="error",
            total_trades=0,
            wins=0,
            losses=0,
            win_rate=0.0,
            net_pnl_cents=0.0,
            sharpe=0.0,
            max_drawdown_pct=0.0,
            fees_paid_cents=0.0,
            avg_win_cents=0.0,
            avg_loss_cents=0.0,
            per_strategy={},
            verdict="kill",
            verdict_reason=f"error: {message}",
            raw_json={"status": "error", "message": message},
        )
