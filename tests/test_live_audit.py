"""Tests for edge_catcher.live.audit."""
from __future__ import annotations

import json
import threading

from edge_catcher.live.audit import AuditEvent, AuditLogger


def test_audit_event_serializable():
	ev = AuditEvent(
		ts="2026-05-07T12:00:00+00:00",
		op="place",
		method="POST",
		path="/trade-api/v2/portfolio/orders",
		client_order_id="abc-123",
		request={"ticker": "X", "count": 1},
		response_status=201,
		response_body={"order": {"order_id": "x"}},
		duration_ms=100.0,
		outcome="success",
	)
	# Must be JSON-encodable
	from dataclasses import asdict
	encoded = json.dumps(asdict(ev), default=str)
	decoded = json.loads(encoded)
	assert decoded["op"] == "place"
	assert decoded["response_status"] == 201


def test_audit_logger_writes_jsonl(tmp_path):
	log = tmp_path / "audit.jsonl"
	logger = AuditLogger(log)
	ev = AuditEvent(
		ts=AuditLogger.now_iso(),
		op="place",
		method="POST",
		path="/x",
		client_order_id=None,
		request={},
		response_status=200,
		response_body={},
		duration_ms=1.0,
		outcome="success",
	)
	logger.write(ev)
	logger.write(ev)
	lines = log.read_text(encoding="utf-8").strip().split("\n")
	assert len(lines) == 2
	for line in lines:
		json.loads(line)  # must parse


def test_audit_logger_creates_parent_dir(tmp_path):
	log = tmp_path / "nested" / "deeper" / "audit.jsonl"
	logger = AuditLogger(log)
	ev = AuditEvent(
		ts=AuditLogger.now_iso(), op="balance", method="GET",
		path="/x", client_order_id=None, request={}, response_status=200,
		response_body={}, duration_ms=1.0, outcome="success",
	)
	logger.write(ev)
	assert log.exists()


def test_audit_logger_is_thread_safe(tmp_path):
	"""Concurrent writes from multiple threads produce well-formed JSONL."""
	log = tmp_path / "concurrent.jsonl"
	logger = AuditLogger(log)

	def writer(n: int) -> None:
		for i in range(50):
			logger.write(AuditEvent(
				ts=AuditLogger.now_iso(),
				op=f"op-{n}-{i}",
				method="GET",
				path="/x",
				client_order_id=None,
				request={"i": i, "n": n},
				response_status=200,
				response_body={},
				duration_ms=0.1,
				outcome="success",
			))

	threads = [threading.Thread(target=writer, args=(n,)) for n in range(4)]
	for t in threads:
		t.start()
	for t in threads:
		t.join()

	lines = log.read_text(encoding="utf-8").strip().split("\n")
	assert len(lines) == 200
	# Every line is valid JSON
	for line in lines:
		obj = json.loads(line)
		assert obj["outcome"] == "success"
