"""Append-only experiment log for the research loop.

Records outcomes, trajectory status, and observations after each loop run
so future LLM prompts can build on prior reasoning.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS research_journal (
	id          INTEGER PRIMARY KEY AUTOINCREMENT,
	run_id      TEXT NOT NULL,
	entry_type  TEXT NOT NULL,
	content     TEXT NOT NULL,
	created_at  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_journal_created ON research_journal(created_at DESC);
"""

_VALID_ENTRY_TYPES = {"outcome", "trajectory", "observation"}


class ResearchJournal:
	def __init__(self, db_path: str | Path = "data/research.db") -> None:
		self.db_path = Path(db_path)
		self.db_path.parent.mkdir(parents=True, exist_ok=True)
		self._init()

	def _connect(self, timeout: float = 30.0) -> sqlite3.Connection:
		conn = sqlite3.connect(str(self.db_path), timeout=timeout)
		conn.row_factory = sqlite3.Row
		return conn

	def _init(self) -> None:
		conn = self._connect()
		try:
			conn.executescript(_SCHEMA)
			conn.commit()
		finally:
			conn.close()

	def write_entry(self, run_id: str, entry_type: str, content: dict) -> None:
		"""Append a new journal entry."""
		if entry_type not in _VALID_ENTRY_TYPES:
			raise ValueError(f"entry_type must be one of {_VALID_ENTRY_TYPES}, got {entry_type!r}")
		conn = self._connect()
		try:
			conn.execute(
				"""INSERT INTO research_journal (run_id, entry_type, content, created_at)
				   VALUES (?, ?, ?, ?)""",
				(
					run_id,
					entry_type,
					json.dumps(content),
					datetime.now(timezone.utc).isoformat(),
				),
			)
			conn.commit()
		finally:
			conn.close()

	def read_recent(self, limit: int = 50) -> list[dict]:
		"""Return the most recent journal entries as plain dicts, newest first."""
		conn = self._connect()
		try:
			rows = conn.execute(
				"""SELECT id, run_id, entry_type, content, created_at
				   FROM research_journal
				   ORDER BY created_at DESC, id DESC
				   LIMIT ?""",
				(limit,),
			).fetchall()
			result = []
			for row in rows:
				d = dict(row)
				d["content"] = json.loads(d["content"])
				result.append(d)
			return result
		finally:
			conn.close()

	def get_latest_trajectory(self) -> dict | None:
		"""Return the most recent trajectory entry's content, or None."""
		conn = self._connect()
		try:
			row = conn.execute(
				"""SELECT content FROM research_journal
				   WHERE entry_type = 'trajectory'
				   ORDER BY created_at DESC, id DESC
				   LIMIT 1""",
			).fetchone()
			if row is None:
				return None
			return json.loads(row["content"])
		finally:
			conn.close()

	def build_context_for_prompt(self, max_chars: int = 8000) -> str:
		"""Format recent journal entries as text for LLM prompt inclusion.

		Returns a markdown-formatted summary, newest first. The latest trajectory
		entry is always included first (even if it alone exceeds max_chars, since
		trajectory entries are small and most actionable). Remaining entries are
		added newest-first until the character budget is exhausted.
		"""
		entries = self.read_recent(limit=200)

		# Separate trajectory entries from others
		trajectories = [e for e in entries if e["entry_type"] == "trajectory"]
		others = [e for e in entries if e["entry_type"] != "trajectory"]

		parts: list[str] = []
		chars_used = 0

		# Always include the latest trajectory first
		if trajectories:
			latest_traj = trajectories[0]
			traj_text = _format_trajectory(latest_traj["content"])
			parts.append(traj_text)
			chars_used += len(traj_text)

		# Then fill remaining budget with other entries (newest first)
		for entry in others:
			et = entry["entry_type"]
			c = entry["content"]
			if et == "outcome":
				text = _format_outcome(c)
			elif et == "observation":
				text = _format_observation(c)
			else:
				continue

			if chars_used + len(text) + 1 > max_chars:
				break
			parts.append(text)
			chars_used += len(text) + 1

		return "\n".join(parts)

	@staticmethod
	def classify_trajectory(
		run_id: str,
		results: list[dict],
		prev_trajectory: dict | None,
	) -> str:
		"""Classify research trajectory as improving / plateauing / stuck.

		Parameters
		----------
		run_id:
			The UUID of the current loop invocation.
		results:
			List of result dicts built by the loop, each with keys 'run_id'
			(loop invocation UUID), 'verdict', and 'sharpe'. These are NOT
			raw tracker rows — the loop constructs them from HypothesisResult
			objects.
		prev_trajectory:
			The previous trajectory entry content dict, or None if first run.
		"""
		if not results:
			return "stuck"

		this_run = [r for r in results if r.get("run_id") == run_id]
		if not this_run:
			return "stuck"

		promote_rate = sum(1 for r in this_run if r.get("verdict") in ("promote", "review")) / len(this_run)
		best_this_run = max((r.get("sharpe", 0) for r in this_run), default=0)

		prev_best_sharpe: float = 0.0
		if prev_trajectory is not None:
			prev_best_sharpe = prev_trajectory.get("best_sharpe_overall", 0.0)

		# Require meaningful promote rate (> 5%), not just any single promote
		if promote_rate > 0.05:
			return "improving"
		# New best Sharpe must actually exceed previous (not just within 5%)
		if prev_best_sharpe > 0 and best_this_run > prev_best_sharpe:
			return "improving"
		# Any promotes/explores at all = plateauing
		if promote_rate > 0 or any(r.get("verdict") == "explore" for r in this_run):
			return "plateauing"
		return "stuck"


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _format_trajectory(c: dict) -> str:
	status = c.get("status", "unknown").upper()
	lines = [f"## Research Trajectory: {status}"]
	total = c.get("total_sessions")
	if total is not None:
		lines.append(f"- Total sessions: {total}")
	promote_rate = c.get("promote_rate")
	if promote_rate is not None:
		prev = c.get("promote_rate_prev")
		prev_str = f" (prev: {prev:.1%})" if prev is not None else ""
		lines.append(f"- Promote rate: {promote_rate:.1%}{prev_str}")
	best_this = c.get("best_sharpe_this_run")
	best_all = c.get("best_sharpe_overall")
	if best_this is not None:
		lines.append(f"- Best Sharpe this run: {best_this:.2f}")
	if best_all is not None:
		lines.append(f"- Best Sharpe overall: {best_all:.2f}")
	new_p = c.get("new_promotes")
	new_e = c.get("new_explores")
	new_k = c.get("new_kills")
	if any(v is not None for v in (new_p, new_e, new_k)):
		lines.append(
			f"- This run: {new_p or 0} promotes, {new_e or 0} explores, {new_k or 0} kills"
		)
	return "\n".join(lines)


def _format_outcome(c: dict) -> str:
	phase = c.get("phase", "?")
	strategy = c.get("strategy", "?")
	verdicts = c.get("verdicts", {})
	best_sharpe = c.get("best_sharpe")
	sharpe_str = f", best Sharpe={best_sharpe:.2f}" if best_sharpe is not None else ""
	verdicts_str = ", ".join(f"{k}={v}" for k, v in verdicts.items()) if verdicts else ""
	verdicts_part = f" ({verdicts_str})" if verdicts_str else ""
	return f"- **[{phase}]** {strategy}{verdicts_part}{sharpe_str}"


def _format_observation(c: dict) -> str:
	pattern = c.get("pattern", "")
	evidence = c.get("evidence", "")
	lines = [f"- **Pattern:** {pattern}"]
	if evidence:
		lines.append(f"  Evidence: {evidence}")
	return "\n".join(lines)
