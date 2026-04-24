"""Tests for replay/loader.py — JSONL streaming, decompression, and sorting.

The loader is the entry point for the replay backtester. It must:
  * Transparently read raw .jsonl or .jsonl.zst files
  * Skip the schema header line
  * Skip malformed lines (log warning, don't crash)
  * Skip lines exceeding MAX_LINE_BYTES (defensive cap)
  * Yield events sorted by recv_seq for deterministic replay
  * Optionally filter by ticker set
"""
from __future__ import annotations

import json
from pathlib import Path

import zstandard as zstd


# ---------------------------------------------------------------------------
# Raw JSONL round-trip
# ---------------------------------------------------------------------------


def test_loader_yields_events_in_recv_seq_order(tmp_path: Path) -> None:
	"""Events must be sorted by recv_seq, not by on-disk order."""
	from edge_catcher.monitors.replay.loader import read_jsonl_window

	path = tmp_path / "kalshi_engine_2026-04-14.jsonl"
	path.write_text(
		json.dumps({"schema_version": 1, "exchange": "kalshi", "header": True}) + "\n"
		+ json.dumps({"recv_seq": 3, "recv_ts": "2026-04-14T00:00:03+00:00", "source": "ws", "payload": {}}) + "\n"
		+ json.dumps({"recv_seq": 1, "recv_ts": "2026-04-14T00:00:01+00:00", "source": "ws", "payload": {}}) + "\n"
		+ json.dumps({"recv_seq": 2, "recv_ts": "2026-04-14T00:00:02+00:00", "source": "ws", "payload": {}}) + "\n",
		encoding="utf-8",
	)

	events = list(read_jsonl_window(path))
	assert [e["recv_seq"] for e in events] == [1, 2, 3]


def test_loader_skips_header_line(tmp_path: Path) -> None:
	"""The header line (with ``header: true``) must not appear in the output."""
	from edge_catcher.monitors.replay.loader import read_jsonl_window

	path = tmp_path / "kalshi_engine_2026-04-14.jsonl"
	path.write_text(
		json.dumps({"schema_version": 1, "exchange": "kalshi", "header": True}) + "\n"
		+ json.dumps({"recv_seq": 1, "recv_ts": "2026-04-14T00:00:01+00:00", "source": "ws", "payload": {}}) + "\n",
		encoding="utf-8",
	)
	events = list(read_jsonl_window(path))
	assert len(events) == 1
	assert events[0]["recv_seq"] == 1


def test_loader_skips_malformed_lines(tmp_path: Path) -> None:
	"""Malformed JSON lines must be silently skipped, not raise."""
	from edge_catcher.monitors.replay.loader import read_jsonl_window

	path = tmp_path / "kalshi_engine_2026-04-14.jsonl"
	path.write_text(
		json.dumps({"schema_version": 1, "header": True}) + "\n"
		+ json.dumps({"recv_seq": 1, "source": "ws", "payload": {}}) + "\n"
		+ "this is not json at all\n"
		+ '{"broken": ' + "\n"  # truncated JSON
		+ json.dumps({"recv_seq": 2, "source": "ws", "payload": {}}) + "\n",
		encoding="utf-8",
	)

	events = list(read_jsonl_window(path))
	assert len(events) == 2
	assert [e["recv_seq"] for e in events] == [1, 2]


def test_loader_skips_blank_lines(tmp_path: Path) -> None:
	"""Empty / whitespace-only lines must be silently skipped."""
	from edge_catcher.monitors.replay.loader import read_jsonl_window

	path = tmp_path / "kalshi_engine_2026-04-14.jsonl"
	path.write_text(
		json.dumps({"schema_version": 1, "header": True}) + "\n"
		+ "\n"
		+ "   \n"
		+ json.dumps({"recv_seq": 1, "source": "ws", "payload": {}}) + "\n",
		encoding="utf-8",
	)
	events = list(read_jsonl_window(path))
	assert len(events) == 1


# ---------------------------------------------------------------------------
# zstd decompression
# ---------------------------------------------------------------------------


def test_loader_decompresses_zstd(tmp_path: Path) -> None:
	"""A .jsonl.zst file should decompress transparently and yield the
	same events as the uncompressed version would."""
	from edge_catcher.monitors.replay.loader import read_jsonl_window

	content = (
		json.dumps({"schema_version": 1, "header": True}) + "\n"
		+ json.dumps({"recv_seq": 1, "source": "ws", "payload": {"a": 1}}) + "\n"
		+ json.dumps({"recv_seq": 2, "source": "synthetic.rest_orderbook", "payload": {"b": 2}}) + "\n"
	)
	compressed = zstd.ZstdCompressor().compress(content.encode("utf-8"))

	path = tmp_path / "kalshi_engine_2026-04-14.jsonl.zst"
	path.write_bytes(compressed)

	events = list(read_jsonl_window(path))
	assert len(events) == 2
	assert events[0]["recv_seq"] == 1
	assert events[1]["recv_seq"] == 2
	assert events[1]["source"] == "synthetic.rest_orderbook"


# ---------------------------------------------------------------------------
# Oversized line defense
# ---------------------------------------------------------------------------


def test_loader_skips_oversized_lines(tmp_path: Path) -> None:
	"""A single line exceeding MAX_LINE_BYTES must be skipped (log-and-continue),
	not cause a crash or memory blowup."""
	from edge_catcher.monitors.replay.loader import MAX_LINE_BYTES, read_jsonl_window

	# Craft a payload that exceeds MAX_LINE_BYTES when serialized
	oversized_payload = {"blob": "x" * (MAX_LINE_BYTES + 1000)}
	path = tmp_path / "kalshi_engine_2026-04-14.jsonl"
	path.write_text(
		json.dumps({"schema_version": 1, "header": True}) + "\n"
		+ json.dumps({"recv_seq": 1, "source": "ws", "payload": {}}) + "\n"
		+ json.dumps({"recv_seq": 2, "source": "ws", "payload": oversized_payload}) + "\n"
		+ json.dumps({"recv_seq": 3, "source": "ws", "payload": {}}) + "\n",
		encoding="utf-8",
	)
	events = list(read_jsonl_window(path))
	# The oversized line is dropped; 1 and 3 remain
	seqs = [e["recv_seq"] for e in events]
	assert 2 not in seqs
	assert seqs == [1, 3]


# ---------------------------------------------------------------------------
# Ticker filter
# ---------------------------------------------------------------------------


def test_loader_ticker_filter_keeps_matching_ws_events(tmp_path: Path) -> None:
	"""With a ticker filter set, ws events for other tickers should be dropped."""
	from edge_catcher.monitors.replay.loader import read_jsonl_window

	path = tmp_path / "kalshi_engine_2026-04-14.jsonl"
	path.write_text(
		json.dumps({"schema_version": 1, "header": True}) + "\n"
		+ json.dumps({"recv_seq": 1, "source": "ws", "payload": {"type": "ticker", "msg": {"market_ticker": "KXETH-1"}}}) + "\n"  # noqa: E501
		+ json.dumps({"recv_seq": 2, "source": "ws", "payload": {"type": "ticker", "msg": {"market_ticker": "KXSOL-1"}}}) + "\n"  # noqa: E501
		+ json.dumps({"recv_seq": 3, "source": "ws", "payload": {"type": "ticker", "msg": {"market_ticker": "KXETH-1"}}}) + "\n",  # noqa: E501
		encoding="utf-8",
	)

	events = list(read_jsonl_window(path, ticker_filter={"KXETH-1"}))
	assert [e["recv_seq"] for e in events] == [1, 3]


def test_loader_ticker_filter_keeps_matching_synthetic_events(tmp_path: Path) -> None:
	"""With a ticker filter set, synthetic events whose payload has a
	matching `ticker` field should be kept. Unknown shapes always pass
	so heartbeat/metadata events aren't accidentally dropped."""
	from edge_catcher.monitors.replay.loader import read_jsonl_window

	path = tmp_path / "kalshi_engine_2026-04-14.jsonl"
	path.write_text(
		json.dumps({"schema_version": 1, "header": True}) + "\n"
		+ json.dumps({"recv_seq": 1, "source": "synthetic.rest_orderbook", "payload": {"ticker": "KXETH-1"}}) + "\n"
		+ json.dumps({"recv_seq": 2, "source": "synthetic.rest_orderbook", "payload": {"ticker": "KXSOL-1"}}) + "\n"
		+ json.dumps({"recv_seq": 3, "source": "synthetic.settlement", "payload": {"strategy": "s1", "ticker": "KXETH-1", "side": "yes", "entry_time": "2026-04-14T00:00:00+00:00", "result": "yes"}}) + "\n",  # noqa: E501
		encoding="utf-8",
	)

	events = list(read_jsonl_window(path, ticker_filter={"KXETH-1"}))
	seqs = [e["recv_seq"] for e in events]
	assert 1 in seqs  # matching
	assert 2 not in seqs  # non-matching
	assert 3 in seqs  # matching settlement


def test_loader_no_filter_passes_everything(tmp_path: Path) -> None:
	"""ticker_filter=None yields every event."""
	from edge_catcher.monitors.replay.loader import read_jsonl_window

	path = tmp_path / "kalshi_engine_2026-04-14.jsonl"
	path.write_text(
		json.dumps({"schema_version": 1, "header": True}) + "\n"
		+ json.dumps({"recv_seq": 1, "source": "ws", "payload": {"type": "ticker", "msg": {"market_ticker": "KXA"}}}) + "\n"  # noqa: E501
		+ json.dumps({"recv_seq": 2, "source": "ws", "payload": {"type": "trade", "msg": {"market_ticker": "KXB"}}}) + "\n",  # noqa: E501
		encoding="utf-8",
	)
	events = list(read_jsonl_window(path, ticker_filter=None))
	assert len(events) == 2
