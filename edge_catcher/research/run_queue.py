# edge_catcher/research/run_queue.py
"""Run queue: executes backtests with concurrency limit and audit logging."""

from __future__ import annotations

import logging
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from .audit import AuditLog
from .hypothesis import Hypothesis, HypothesisResult

logger = logging.getLogger(__name__)


class RunQueue:
	def __init__(
		self,
		agent,  # ResearchAgent — no type import to avoid circular deps
		audit: AuditLog,
		parallel: int = 1,
	) -> None:
		self.agent = agent
		self.audit = audit
		self.parallel = parallel
		if parallel > 1:
			self._enable_wal(audit.db_path)

	@staticmethod
	def _enable_wal(db_path) -> None:
		"""Enable WAL journal mode for concurrent writes."""
		conn = sqlite3.connect(str(db_path))
		conn.execute("PRAGMA journal_mode=WAL")
		conn.close()
		logger.info("Enabled WAL mode on %s", db_path)

	def submit(
		self,
		hypotheses: list[Hypothesis],
		phase: str,
		max_time_seconds: float | None = None,
		sweep_N_override: int | None = None,
	) -> list[HypothesisResult]:
		"""Execute hypotheses through the agent, recording each in the audit log.

		Args:
			hypotheses: Ordered list of hypotheses to run.
			phase: 'grid' or 'llm' — recorded in audit log.
			max_time_seconds: Wall-clock timeout. Finishes in-flight runs, skips rest.
			sweep_N_override: If set, forwarded to each hypothesis run so DSR's
				multiple-testing N is a sweep-start snapshot (same for every
				hypothesis) rather than the monotonically growing tracker count.

		Returns:
			List of results for completed runs.
		"""
		if not hypotheses:
			return []

		if self.parallel <= 1:
			return self._run_sequential(hypotheses, phase, max_time_seconds, sweep_N_override)
		return self._run_parallel(hypotheses, phase, max_time_seconds, sweep_N_override)

	def _run_sequential(
		self,
		hypotheses: list[Hypothesis],
		phase: str,
		max_time_seconds: float | None,
		sweep_N_override: int | None = None,
	) -> list[HypothesisResult]:
		results: list[HypothesisResult] = []
		start_time = time.monotonic()

		for i, h in enumerate(hypotheses):
			if max_time_seconds is not None:
				elapsed = time.monotonic() - start_time
				if elapsed >= max_time_seconds:
					logger.info(
						"RunQueue: time limit reached after %d/%d runs (%.1fs)",
						i, len(hypotheses), elapsed,
					)
					break

			result = self._run_one(h, phase, i, sweep_N_override)
			results.append(result)

		return results

	def _run_parallel(
		self,
		hypotheses: list[Hypothesis],
		phase: str,
		max_time_seconds: float | None,
		sweep_N_override: int | None = None,
	) -> list[HypothesisResult]:
		results: list[HypothesisResult] = []
		start_time = time.monotonic()
		queue = list(enumerate(hypotheses))

		with ThreadPoolExecutor(max_workers=self.parallel) as executor:
			# Submit in batches to respect timeout
			while queue:
				if max_time_seconds is not None:
					elapsed = time.monotonic() - start_time
					if elapsed >= max_time_seconds:
						break

				# Submit up to `parallel` tasks at a time
				batch = queue[:self.parallel]
				queue = queue[self.parallel:]

				futures = {
					executor.submit(self._run_one, h, phase, i, sweep_N_override): i
					for i, h in batch
				}

				for future in as_completed(futures):
					try:
						results.append(future.result())
					except Exception as exc:
						logger.error("Parallel run failed: %s", exc)

		logger.info("RunQueue: completed %d/%d runs", len(results), len(hypotheses))
		return results

	def _run_one(
		self,
		h: Hypothesis,
		phase: str,
		queue_position: int,
		sweep_N_override: int | None = None,
	) -> HypothesisResult:
		started_at = datetime.now(timezone.utc).isoformat()
		result = self.agent.run_hypothesis(h, sweep_N_override=sweep_N_override)
		self.audit.record_execution(
			hypothesis_id=h.id,
			phase=phase,
			queue_position=queue_position,
			verdict=result.verdict,
			status=result.status,
			started_at=started_at,
		)
		return result
