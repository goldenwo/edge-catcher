# edge_catcher/research/audit.py
"""Append-only audit log for research loop decisions, executions, and integrity checks."""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

_AUDIT_SCHEMA = """
CREATE TABLE IF NOT EXISTS audit_decisions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    prompt_hash     TEXT NOT NULL,
    prompt_text     TEXT NOT NULL,
    response_text   TEXT NOT NULL,
    parsed_output   TEXT,
    model           TEXT NOT NULL,
    token_count     INTEGER,
    created_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_executions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    hypothesis_id   TEXT NOT NULL,
    phase           TEXT NOT NULL,
    queue_position  INTEGER NOT NULL,
    verdict         TEXT,
    status          TEXT NOT NULL,
    data_checksum   TEXT,
    started_at      TEXT,
    completed_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_integrity (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    checkpoint      TEXT NOT NULL,
    result_hash     TEXT NOT NULL,
    result_count    INTEGER NOT NULL,
    created_at      TEXT NOT NULL
);
"""


class AuditLog:
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
			conn.executescript(_AUDIT_SCHEMA)
			conn.commit()
		finally:
			conn.close()

	# ── Decisions ─────────────────────────────────────────────────────────────

	def record_decision(
		self,
		prompt_hash: str,
		prompt_text: str,
		response_text: str,
		parsed_output: dict | list | None = None,
		model: str = "",
		token_count: int = 0,
	) -> None:
		conn = self._connect()
		try:
			conn.execute(
				"""INSERT INTO audit_decisions
				   (prompt_hash, prompt_text, response_text, parsed_output,
				    model, token_count, created_at)
				   VALUES (?,?,?,?,?,?,?)""",
				(
					prompt_hash,
					prompt_text,
					response_text,
					json.dumps(parsed_output) if parsed_output else None,
					model,
					token_count,
					datetime.now(timezone.utc).isoformat(),
				),
			)
			conn.commit()
		finally:
			conn.close()

	def list_decisions(self, limit: int | None = None) -> list[dict]:
		conn = self._connect()
		try:
			query = "SELECT * FROM audit_decisions ORDER BY created_at DESC"
			if limit is not None:
				query += f" LIMIT {int(limit)}"
			rows = conn.execute(query).fetchall()
			return [dict(r) for r in rows]
		finally:
			conn.close()

	# ── Executions ────────────────────────────────────────────────────────────

	def record_execution(
		self,
		hypothesis_id: str,
		phase: str,
		queue_position: int,
		verdict: str,
		status: str,
		started_at: str | None = None,
		data_checksum: str | None = None,
	) -> None:
		conn = self._connect()
		try:
			conn.execute(
				"""INSERT INTO audit_executions
				   (hypothesis_id, phase, queue_position, verdict, status,
				    data_checksum, started_at, completed_at)
				   VALUES (?,?,?,?,?,?,?,?)""",
				(
					hypothesis_id,
					phase,
					queue_position,
					verdict,
					status,
					data_checksum,
					started_at or datetime.now(timezone.utc).isoformat(),
					datetime.now(timezone.utc).isoformat(),
				),
			)
			conn.commit()
		finally:
			conn.close()

	def list_executions(self, limit: int | None = None) -> list[dict]:
		conn = self._connect()
		try:
			query = "SELECT * FROM audit_executions ORDER BY completed_at DESC"
			if limit is not None:
				query += f" LIMIT {int(limit)}"
			rows = conn.execute(query).fetchall()
			return [dict(r) for r in rows]
		finally:
			conn.close()

	# ── Integrity ─────────────────────────────────────────────────────────────

	def record_integrity(
		self,
		checkpoint: str,
		result_hash: str,
		result_count: int,
	) -> None:
		conn = self._connect()
		try:
			conn.execute(
				"""INSERT INTO audit_integrity
				   (checkpoint, result_hash, result_count, created_at)
				   VALUES (?,?,?,?)""",
				(checkpoint, result_hash, result_count,
				 datetime.now(timezone.utc).isoformat()),
			)
			conn.commit()
		finally:
			conn.close()

	def list_integrity_checks(self) -> list[dict]:
		conn = self._connect()
		try:
			rows = conn.execute(
				"SELECT * FROM audit_integrity ORDER BY created_at DESC"
			).fetchall()
			return [dict(r) for r in rows]
		finally:
			conn.close()

	@staticmethod
	def compute_result_hash(rows: list[dict]) -> str:
		"""SHA-256 of result rows sorted by hypothesis_id, serialized as JSON."""
		sorted_rows = sorted(rows, key=lambda r: r.get("hypothesis_id", ""))
		payload = json.dumps(sorted_rows, sort_keys=True, default=str)
		return hashlib.sha256(payload.encode()).hexdigest()

	# ── Structural Validation ─────────────────────────────────────────────────

	@staticmethod
	def validate_hypothesis(
		strategy: str,
		series: str,
		db_path: str,
		start_date: str,
		end_date: str,
	) -> tuple[bool, str | None]:
		"""Validate hypothesis fields are present and db file exists."""
		if not strategy:
			return False, "strategy is empty"
		if not series:
			return False, "series is empty"
		if not start_date or not end_date:
			return False, "start_date or end_date is empty"
		if not Path(db_path).exists():
			return False, f"db_path not found: {db_path}"
		return True, None

	@staticmethod
	def validate_result_consistency(
		total_trades: int,
		wins: int,
		losses: int,
		status: str,
	) -> tuple[bool, str | None]:
		"""Check that wins + losses == total_trades for non-error results."""
		if status == "error":
			return True, None
		if wins + losses != total_trades:
			return False, (
				f"Inconsistent: wins ({wins}) + losses ({losses}) "
				f"!= total_trades ({total_trades})"
			)
		return True, None
