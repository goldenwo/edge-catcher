"""Findings reporter: generates JSON + markdown reports from hypothesis results."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from .hypothesis import HypothesisResult

logger = logging.getLogger(__name__)


class Reporter:
    def generate_report(self, results: list[HypothesisResult]) -> dict:
        """Generate structured findings report grouped by verdict."""
        promoted = sorted(
            [r for r in results if r.verdict == "promote"],
            key=lambda r: r.sharpe,
            reverse=True,
        )
        explore = sorted(
            [r for r in results if r.verdict == "explore"],
            key=lambda r: r.sharpe,
            reverse=True,
        )
        killed = [r for r in results if r.verdict == "kill"]
        errors = [r for r in results if r.status == "error"]

        total = len(results)
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "summary": {
                "total": total,
                "promoted": len(promoted),
                "explore": len(explore),
                "killed": len(killed),
                "errors": len(errors),
                "best_sharpe": promoted[0].sharpe if promoted else None,
                "best_win_rate": max((r.win_rate for r in promoted), default=None),
                "total_pnl_cents": sum(r.net_pnl_cents for r in results),
            },
            "promoted": [self._result_to_dict(r) for r in promoted],
            "explore": [self._result_to_dict(r) for r in explore],
            "killed": [self._result_to_dict(r) for r in killed],
        }

    def to_markdown(self, report: dict) -> str:
        """Format report as markdown for human review."""
        lines: list[str] = []
        s = report["summary"]
        generated = report.get("generated_at", "")

        lines.append("# Research Findings Report")
        lines.append(f"\n_Generated: {generated}_\n")
        lines.append("## Summary\n")
        lines.append(f"| Metric | Value |")
        lines.append(f"|--------|-------|")
        lines.append(f"| Total hypotheses tested | {s['total']} |")
        lines.append(f"| Promoted | {s['promoted']} |")
        lines.append(f"| Explore | {s['explore']} |")
        lines.append(f"| Killed | {s['killed']} |")
        lines.append(f"| Errors | {s['errors']} |")
        if s.get("best_sharpe") is not None:
            lines.append(f"| Best Sharpe | {s['best_sharpe']:.2f} |")
        if s.get("best_win_rate") is not None:
            lines.append(f"| Best Win Rate | {s['best_win_rate']:.1%} |")
        lines.append(f"| Total PnL | {s['total_pnl_cents']:.0f}¢ |")

        if report["promoted"]:
            lines.append("\n## Promoted (ranked by Sharpe)\n")
            lines.append("| Strategy | Series | DB | Trades | Win Rate | Sharpe | PnL (¢) | Fees (¢) |")
            lines.append("|----------|--------|----|--------|----------|--------|---------|----------|")
            for r in report["promoted"]:
                lines.append(
                    f"| {r['strategy']} | {r['series']} | {Path(r['db_path']).name} "
                    f"| {r['total_trades']} | {r['win_rate']:.1%} | {r['sharpe']:.2f} "
                    f"| {r['net_pnl_cents']:.0f} | {r['fees_paid_cents']:.0f} |"
                )
            lines.append("")
            lines.append("### Promoted Details\n")
            for r in report["promoted"]:
                lines.append(f"#### {r['strategy']} × {r['series']}")
                lines.append(f"- **DB:** `{r['db_path']}`")
                lines.append(f"- **Period:** {r['start_date']} → {r['end_date']}")
                lines.append(f"- **Fee pct:** {r['fee_pct']}")
                lines.append(f"- **Trades:** {r['total_trades']} (W:{r['wins']} L:{r['losses']})")
                lines.append(f"- **Win Rate:** {r['win_rate']:.1%}")
                lines.append(f"- **Sharpe:** {r['sharpe']:.2f}")
                lines.append(f"- **Max DD:** {r['max_drawdown_pct']:.1f}%")
                lines.append(f"- **Net PnL:** {r['net_pnl_cents']:.0f}¢")
                lines.append(f"- **Fees Paid:** {r['fees_paid_cents']:.0f}¢")
                lines.append(f"- **Verdict Reason:** {r['verdict_reason']}")
                if r.get("parent_id"):
                    lines.append(f"- **Parent Hypothesis:** `{r['parent_id']}`")
                lines.append("")

        if report["explore"]:
            lines.append("\n## Explore (worth investigating further)\n")
            lines.append("| Strategy | Series | DB | Trades | Win Rate | Sharpe | PnL (¢) | Reason |")
            lines.append("|----------|--------|----|--------|----------|--------|---------|--------|")
            for r in report["explore"]:
                reason_short = r["verdict_reason"][:60] + "…" if len(r["verdict_reason"]) > 60 else r["verdict_reason"]
                lines.append(
                    f"| {r['strategy']} | {r['series']} | {Path(r['db_path']).name} "
                    f"| {r['total_trades']} | {r['win_rate']:.1%} | {r['sharpe']:.2f} "
                    f"| {r['net_pnl_cents']:.0f} | {reason_short} |"
                )

        if report["killed"]:
            lines.append("\n\n## Killed\n")
            lines.append("| Strategy | Series | DB | Trades | Win Rate | Sharpe | PnL (¢) | Kill Reason |")
            lines.append("|----------|--------|----|--------|----------|--------|---------|-------------|")
            for r in report["killed"]:
                reason_short = r["verdict_reason"][:60] + "…" if len(r["verdict_reason"]) > 60 else r["verdict_reason"]
                lines.append(
                    f"| {r['strategy']} | {r['series']} | {Path(r['db_path']).name} "
                    f"| {r['total_trades']} | {r['win_rate']:.1%} | {r['sharpe']:.2f} "
                    f"| {r['net_pnl_cents']:.0f} | {reason_short} |"
                )

        return "\n".join(lines) + "\n"

    def save(self, report: dict, path: str) -> None:
        """Save JSON + markdown to disk."""
        base = Path(path)
        base.parent.mkdir(parents=True, exist_ok=True)

        json_path = base.with_suffix(".json")
        md_path = base.with_suffix(".md")

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        logger.info("Saved JSON report to %s", json_path)

        with open(md_path, "w", encoding="utf-8") as f:
            f.write(self.to_markdown(report))
        logger.info("Saved markdown report to %s", md_path)

    @staticmethod
    def _result_to_dict(r: HypothesisResult) -> dict:
        h = r.hypothesis
        return {
            "id": h.id,
            "strategy": h.strategy,
            "series": h.series,
            "db_path": h.db_path,
            "start_date": h.start_date,
            "end_date": h.end_date,
            "fee_pct": h.fee_pct,
            "parent_id": h.parent_id,
            "tags": h.tags,
            "notes": h.notes,
            "status": r.status,
            "total_trades": r.total_trades,
            "wins": r.wins,
            "losses": r.losses,
            "win_rate": r.win_rate,
            "net_pnl_cents": r.net_pnl_cents,
            "sharpe": r.sharpe,
            "max_drawdown_pct": r.max_drawdown_pct,
            "fees_paid_cents": r.fees_paid_cents,
            "avg_win_cents": r.avg_win_cents,
            "avg_loss_cents": r.avg_loss_cents,
            "verdict": r.verdict,
            "verdict_reason": r.verdict_reason,
        }
