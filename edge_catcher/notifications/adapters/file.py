"""File adapter — appends one JSON object per line to a file path."""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

from edge_catcher.notifications.envelope import DeliveryResult, Notification


class FileChannel:
	"""Append-only JSONL notification log.

	Single-writer assumption: this adapter does NOT take a file lock.
	POSIX guarantees append-atomicity only for writes smaller than
	PIPE_BUF (4096 bytes on Linux). Concurrent writers from multiple
	processes (e.g. two cron jobs targeting the same path) MAY interleave
	if a record exceeds that limit. Typical notification records are
	~500 bytes — well under the limit — but if you embed large payloads,
	either serialize callers or write to per-process paths.
	"""

	def __init__(self, name: str, path: str) -> None:
		self.name = name
		self.path = Path(path)

	def send(self, notification: Notification) -> DeliveryResult:
		t0 = time.perf_counter()
		try:
			self.path.parent.mkdir(parents=True, exist_ok=True)
			record = {
				"ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
				"channel": self.name,
				"title": notification.title,
				"body": notification.body,
				"severity": notification.severity,
				"payload": notification.payload,
			}
			with open(self.path, "a", encoding="utf-8") as fh:
				fh.write(json.dumps(record, ensure_ascii=False) + "\n")
				fh.flush()
		except OSError as exc:
			return DeliveryResult(
				channel_name=self.name,
				success=False,
				error=repr(exc),
				latency_ms=(time.perf_counter() - t0) * 1000,
			)
		return DeliveryResult(
			channel_name=self.name,
			success=True,
			latency_ms=(time.perf_counter() - t0) * 1000,
		)
