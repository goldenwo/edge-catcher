"""Tests for replay/loader.py — JSONL streaming, decompression, ordering.

The loader is the entry point for the replay backtester. It must:
  * Transparently read raw .jsonl or .jsonl.zst files
  * Skip the schema header line
  * Skip malformed lines (log warning, don't crash)
  * Skip lines exceeding MAX_LINE_BYTES (defensive cap)
  * Stream events in on-disk (== recv_seq) order WITHOUT buffering the whole
    file. A real day is ~8M events / multiple GB; the previous materialise-
    then-sort loader OOM'd the CR-5 sweep. On-disk order IS recv_seq order by
    the capture writer's monotonic-append invariant (writer.py); a backwards
    recv_seq means a corrupt/reordered bundle and raises.
  * Optionally filter by ticker set
"""
from __future__ import annotations

import json
import tracemalloc
from pathlib import Path

import pytest
import zstandard as zstd


# ---------------------------------------------------------------------------
# Raw JSONL round-trip
# ---------------------------------------------------------------------------


def test_loader_yields_events_in_file_order(tmp_path: Path) -> None:
	"""Events stream in on-disk order, which is recv_seq order by the capture
	writer's monotonic-append invariant (writer.py)."""
	from edge_catcher.engine.replay.loader import read_jsonl_window

	path = tmp_path / "kalshi_engine_2026-04-14.jsonl"
	path.write_text(
		json.dumps({"schema_version": 1, "exchange": "kalshi", "header": True}) + "\n"
		+ json.dumps({"recv_seq": 1, "recv_ts": "2026-04-14T00:00:01+00:00", "source": "ws", "payload": {}}) + "\n"
		+ json.dumps({"recv_seq": 2, "recv_ts": "2026-04-14T00:00:02+00:00", "source": "ws", "payload": {}}) + "\n"
		+ json.dumps({"recv_seq": 3, "recv_ts": "2026-04-14T00:00:03+00:00", "source": "ws", "payload": {}}) + "\n",
		encoding="utf-8",
	)

	events = list(read_jsonl_window(path))
	assert [e["recv_seq"] for e in events] == [1, 2, 3]


def test_loader_raises_on_recv_seq_inversion(tmp_path: Path) -> None:
	"""A backwards recv_seq violates the writer's monotonic-append invariant
	(writer.py) — the bundle is corrupt or reordered. The loader raises rather
	than silently replaying out of order (which would invalidate a CR-5 parity
	verdict before live cutover). The previous loader silently re-sorted, which
	required buffering the whole multi-GB file and OOM'd the sweep."""
	from edge_catcher.engine.replay.loader import read_jsonl_window

	path = tmp_path / "kalshi_engine_2026-04-14.jsonl"
	path.write_text(
		json.dumps({"schema_version": 1, "exchange": "kalshi", "header": True}) + "\n"
		+ json.dumps({"recv_seq": 1, "recv_ts": "2026-04-14T00:00:01+00:00", "source": "ws", "payload": {}}) + "\n"
		+ json.dumps({"recv_seq": 3, "recv_ts": "2026-04-14T00:00:03+00:00", "source": "ws", "payload": {}}) + "\n"
		+ json.dumps({"recv_seq": 2, "recv_ts": "2026-04-14T00:00:02+00:00", "source": "ws", "payload": {}}) + "\n",
		encoding="utf-8",
	)

	with pytest.raises(ValueError, match="recv_seq"):
		list(read_jsonl_window(path))


def test_loader_raises_on_non_int_recv_seq(tmp_path: Path) -> None:
	"""Every real event carries an int recv_seq (writer.py); the only seq-less
	line — the header — is filtered before the guard. A yielded event whose
	recv_seq is missing or non-int therefore means the bundle is corrupt or
	tampered. The loader must fail loud, not silently pass it: a non-int seq
	can't advance the monotonic check, so silently skipping it would also blind
	the ordering guard to a reorder straddling that event."""
	from edge_catcher.engine.replay.loader import read_jsonl_window

	# String recv_seq (e.g. a botched schema rev or a partial-write garble).
	str_seq = tmp_path / "kalshi_engine_2026-04-14.jsonl"
	str_seq.write_text(
		json.dumps({"schema_version": 1, "exchange": "kalshi", "header": True}) + "\n"
		+ json.dumps({"recv_seq": 1, "source": "ws", "payload": {}}) + "\n"
		+ json.dumps({"recv_seq": "2", "source": "ws", "payload": {}}) + "\n"
		+ json.dumps({"recv_seq": 3, "source": "ws", "payload": {}}) + "\n",
		encoding="utf-8",
	)
	with pytest.raises(ValueError, match="recv_seq"):
		list(read_jsonl_window(str_seq))

	# Bool recv_seq — isinstance(True, int) is True in Python, so it must be
	# rejected explicitly rather than silently treated as 1.
	bool_seq = tmp_path / "kalshi_engine_2026-04-15.jsonl"
	bool_seq.write_text(
		json.dumps({"schema_version": 1, "exchange": "kalshi", "header": True}) + "\n"
		+ json.dumps({"recv_seq": 1, "source": "ws", "payload": {}}) + "\n"
		+ json.dumps({"recv_seq": True, "source": "ws", "payload": {}}) + "\n",
		encoding="utf-8",
	)
	with pytest.raises(ValueError, match="recv_seq"):
		list(read_jsonl_window(bool_seq))


def test_loader_raises_on_duplicate_recv_seq(tmp_path: Path) -> None:
	"""recv_seq is a strictly-increasing monotonic counter — one per event
	(writer.py). A repeated recv_seq means a duplicated/re-emitted event
	(corruption or a botched crash-recovery), so the loader rejects a
	non-strictly-increasing sequence, not just a backwards one."""
	from edge_catcher.engine.replay.loader import read_jsonl_window

	path = tmp_path / "kalshi_engine_2026-04-14.jsonl"
	path.write_text(
		json.dumps({"schema_version": 1, "exchange": "kalshi", "header": True}) + "\n"
		+ json.dumps({"recv_seq": 1, "source": "ws", "payload": {}}) + "\n"
		+ json.dumps({"recv_seq": 2, "source": "ws", "payload": {}}) + "\n"
		+ json.dumps({"recv_seq": 2, "source": "ws", "payload": {}}) + "\n",
		encoding="utf-8",
	)
	with pytest.raises(ValueError, match="recv_seq"):
		list(read_jsonl_window(path))


def test_loader_streams_without_materializing_whole_file(tmp_path: Path) -> None:
	"""Peak memory while iterating must NOT scale with file size. The loader
	streams one event at a time; it must never buffer the whole file. A real
	day is ~8M events / multiple GB — the previous list()+sort() loader OOM'd
	the CR-5 sweep at ~12 GB for a single bundle.

	Builds a ~40 MB JSONL and asserts the tracemalloc peak stays a small
	fraction of that while consuming every event. The old materialise-then-sort
	loader peaked at ~the whole file; streaming keeps peak well under 1 MB."""
	from edge_catcher.engine.replay.loader import read_jsonl_window

	path = tmp_path / "kalshi_engine_2026-04-14.jsonl"
	n_events = 20_000
	blob = "x" * 2000  # ~2 KB payload per event
	with path.open("w", encoding="utf-8") as fh:
		fh.write(json.dumps({"schema_version": 1, "header": True}) + "\n")
		for seq in range(1, n_events + 1):
			fh.write(json.dumps({
				"recv_seq": seq,
				"recv_ts": "2026-04-14T00:00:00+00:00",
				"source": "ws",
				"payload": {"blob": blob},
			}) + "\n")

	file_bytes = path.stat().st_size
	assert file_bytes > 30_000_000, f"test file too small to be meaningful: {file_bytes}"

	tracemalloc.start()
	try:
		count = 0
		last_seq = 0
		for ev in read_jsonl_window(path):
			count += 1
			last_seq = ev["recv_seq"]
		_current, peak = tracemalloc.get_traced_memory()
	finally:
		tracemalloc.stop()

	# Correctness: every event streamed, in order.
	assert count == n_events
	assert last_seq == n_events
	# Bounded memory: peak must be a small FRACTION of the file, not O(file).
	# Streaming holds one event (~2 KB) + a 64 KiB read chunk (well under 1 MB
	# here); full materialisation (the old bug) peaks at ~the whole file. A
	# file/8 ceiling stays many x above the real streaming peak yet ~8x below a
	# full-file blowup, and scales with the file so it can't silently loosen.
	ceiling = file_bytes // 8
	assert peak < ceiling, (
		f"loader peak memory {peak/1e6:.1f} MB streaming a {file_bytes/1e6:.1f} MB "
		f"file (ceiling {ceiling/1e6:.1f} MB = file/8) — it is buffering the file "
		f"instead of streaming (regression of the CR-5 OOM fix)"
	)


def test_loader_skips_header_line(tmp_path: Path) -> None:
	"""The header line (with ``header: true``) must not appear in the output."""
	from edge_catcher.engine.replay.loader import read_jsonl_window

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
	from edge_catcher.engine.replay.loader import read_jsonl_window

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
	from edge_catcher.engine.replay.loader import read_jsonl_window

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
	from edge_catcher.engine.replay.loader import read_jsonl_window

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
	from edge_catcher.engine.replay.loader import MAX_LINE_BYTES, read_jsonl_window

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
	from edge_catcher.engine.replay.loader import read_jsonl_window

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
	from edge_catcher.engine.replay.loader import read_jsonl_window

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
	from edge_catcher.engine.replay.loader import read_jsonl_window

	path = tmp_path / "kalshi_engine_2026-04-14.jsonl"
	path.write_text(
		json.dumps({"schema_version": 1, "header": True}) + "\n"
		+ json.dumps({"recv_seq": 1, "source": "ws", "payload": {"type": "ticker", "msg": {"market_ticker": "KXA"}}}) + "\n"  # noqa: E501
		+ json.dumps({"recv_seq": 2, "source": "ws", "payload": {"type": "trade", "msg": {"market_ticker": "KXB"}}}) + "\n",  # noqa: E501
		encoding="utf-8",
	)
	events = list(read_jsonl_window(path, ticker_filter=None))
	assert len(events) == 2
