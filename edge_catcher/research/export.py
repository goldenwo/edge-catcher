"""Export promoted research results into a portable bundle."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from .agent import ResearchAgent
from .audit import AuditLog
from .journal import ResearchJournal
from .tracker import Tracker

logger = logging.getLogger(__name__)


class ExportCollector:
	"""Collects research artifacts and builds an export bundle."""

	def __init__(self, db_path: str = "data/research.db") -> None:
		self.db_path = db_path
		self.tracker = Tracker(db_path)

	def collect(self, verdicts: list[str] | None = None) -> dict:
		"""Collect all artifacts matching the given verdict filter.

		Returns a bundle dict ready for serialization to manifest.json.
		"""
		if verdicts is None:
			verdicts = ["promote", "review"]

		results = self._collect_results(verdicts)
		strategies = self._group_by_strategy(results)
		self._attach_source(strategies)
		journal = ResearchJournal(db_path=self.db_path)
		audit = AuditLog(self.db_path)
		self._attach_journal(strategies, journal)
		self._attach_audit(strategies, audit)

		return {
			"version": 1,
			"exported_at": datetime.now(timezone.utc).isoformat(),
			"filter": {"verdicts": verdicts},
			"strategies": strategies,
			"series_mapping": {},
		}

	def _collect_results(self, verdicts: list[str]) -> list[dict]:
		"""Query tracker for results matching any of the given verdicts."""
		all_results: list[dict] = []
		for verdict in verdicts:
			all_results.extend(self.tracker.list_results(verdict=verdict))
		return all_results

	def _attach_source(self, strategies: dict) -> None:
		"""Read strategy source code from strategies_local.py."""
		for name in strategies:
			strategies[name]["source"] = ResearchAgent.read_strategy_code(name)

	def _group_by_strategy(self, results: list[dict]) -> dict:
		"""Group results by strategy name into the bundle format."""
		strategies: dict[str, dict] = {}
		for row in results:
			name = row["strategy"]
			if name not in strategies:
				strategies[name] = {
					"source": None,
					"results": [],
					"journal_entries": [],
					"hypothesis_config": None,
				}
			strategies[name]["results"].append({
				"hypothesis_id": row["id"],
				"strategy": row["strategy"],
				"series": row["series"],
				"db_path": row["db_path"],
				"start_date": row["start_date"],
				"end_date": row["end_date"],
				"fee_pct": row["fee_pct"],
				"verdict": row["verdict"],
				"status": row["status"],
				"sharpe": row["sharpe"],
				"win_rate": row["win_rate"],
				"net_pnl_cents": row["net_pnl_cents"],
				"max_drawdown_pct": row["max_drawdown_pct"],
				"total_trades": row["total_trades"],
				"wins": row["wins"],
				"losses": row["losses"],
				"fees_paid_cents": row["fees_paid_cents"],
				"avg_win_cents": row["avg_win_cents"],
				"avg_loss_cents": row["avg_loss_cents"],
				"verdict_reason": row["verdict_reason"],
				"validation_details": json.loads(row["validation_details"]) if row.get("validation_details") else [],
				"completed_at": row["completed_at"],
				"audit": [],
			})
		return strategies

	def _attach_journal(self, strategies: dict, journal: ResearchJournal) -> None:
		"""Attach relevant journal entries for each strategy."""
		all_entries = journal.read_recent(limit=10000)
		for entry in all_entries:
			content = entry["content"]
			strategy_name = content.get("strategy")
			if strategy_name and strategy_name in strategies:
				strategies[strategy_name]["journal_entries"].append({
					"entry_type": entry["entry_type"],
					"run_id": entry["run_id"],
					"content": content,
					"created_at": entry["created_at"],
				})

	def _attach_audit(self, strategies: dict, audit: AuditLog) -> None:
		"""Attach audit execution records to their corresponding results."""
		executions = audit.list_executions()
		exec_by_hid: dict[str, list[dict]] = {}
		for ex in executions:
			hid = ex["hypothesis_id"]
			exec_by_hid.setdefault(hid, []).append({
				"phase": ex["phase"],
				"verdict": ex["verdict"],
				"status": ex["status"],
				"queue_position": ex["queue_position"],
				"started_at": ex["started_at"],
				"completed_at": ex["completed_at"],
			})

		for strat_data in strategies.values():
			for result in strat_data["results"]:
				hid = result["hypothesis_id"]
				result["audit"] = exec_by_hid.get(hid, [])
