"""Append-only JSONL audit logger for the live order placement layer.

Writes one JSON object per API request/response cycle. The format is
forward-compatible with sub-project F's tamper-evident audit chain — F
will read this file and add a hash chain on top, not rewrite the format.
"""

from __future__ import annotations
import json
import threading
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class AuditEvent:
	"""One row in the audit log."""

	ts: str  # ISO-8601 UTC, microsecond precision
	op: str  # 'place' / 'cancel' / 'status' / 'balance' / 'positions'
	method: str  # 'POST' / 'GET' / 'DELETE'
	path: str  # e.g. '/trade-api/v2/portfolio/orders'
	client_order_id: str | None  # uuid4 set by client.place(); None for reads
	request: dict[str, Any]  # request body or query params
	response_status: int | None  # HTTP status; None if network error
	response_body: dict[str, Any] | None  # parsed JSON; None on non-JSON
	duration_ms: float
	outcome: str  # 'success' / 'http_error' / 'network_error' / 'cap_exceeded'
	error: str | None = None  # exception message; None on success
	retries: int = 0  # number of retries before this final outcome


class AuditLogger:
	"""Thread-safe append-only JSONL writer.

	One process writes (the engine in E or the CLI in cli.py); locking is
	cheap insurance against concurrent CLI invocations stomping each other.
	"""

	def __init__(self, log_path: Path) -> None:
		self._log_path = log_path
		self._lock = threading.Lock()
		log_path.parent.mkdir(parents=True, exist_ok=True)

	def write(self, event: AuditEvent) -> None:
		line = json.dumps(asdict(event), default=str, separators=(",", ":"))
		with self._lock, self._log_path.open("a", encoding="utf-8") as fh:
			fh.write(line + "\n")

	@staticmethod
	def now_iso() -> str:
		return datetime.now(timezone.utc).isoformat()
