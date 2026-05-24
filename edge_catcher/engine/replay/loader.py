"""Replay JSONL loader.

Streams captured engine-input events from a bundle's JSONL (or ``.jsonl.zst``)
file, transparently decompressing zstd, in on-disk order, optionally filtering
by a ticker set.

On-disk order IS ``recv_seq`` order: the capture writer assigns ``recv_seq``
from a monotonic counter and appends each event synchronously on the single
engine thread (a flock bars a second writer) — see ``capture/writer.py``. So
the loader streams in file order WITHOUT buffering the whole file. This matters:
a real trading day is ~8M events / multiple GB decompressed, and the previous
"materialise the whole file into a list, then ``sort`` by ``recv_seq``"
approach made replay (and the CR-5 parity sweep) OOM at ~12 GB for a single
bundle. The sort was redundant — it never reordered real captures (file order
already equals ``recv_seq`` order) — so it's removed in favour of streaming.

Malformed lines, oversized lines, blank lines, and the schema header are all
silently skipped (the oversized + malformed cases log at WARNING / DEBUG
respectively). The one integrity violation the loader does NOT tolerate is a
``recv_seq`` that goes backwards: the bundle file is external input (read from
disk, possibly fetched from R2), and a backwards ``recv_seq`` means it is
corrupt or was reordered, breaking the writer invariant. Replaying it out of
order would silently produce wrong trades and invalidate a parity verdict
before live cutover, so that raises ``ValueError`` rather than failing quietly.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Iterator, Optional

import zstandard as zstd

log = logging.getLogger(__name__)

# Defensive per-line cap — an 8 MiB line in a JSONL is almost certainly a
# bug or attack. The real-world capture writer emits events well under
# 100 KB. Set conservatively to catch pathological cases without rejecting
# genuine large orderbooks.
MAX_LINE_BYTES = 8 * 1024 * 1024


def read_jsonl_window(
	path: Path | str,
	ticker_filter: Optional[set[str]] = None,
) -> Iterator[dict]:
	"""Stream parsed events from a captured JSONL or ``.jsonl.zst`` file.

	Yields in on-disk order, which is ``recv_seq`` order by the capture
	writer's monotonic-append invariant (see module docstring). Skips the
	schema header, malformed lines, blank lines, and any line that would
	exceed ``MAX_LINE_BYTES`` when read. Memory is bounded to one event at a
	time — the file is never materialised.

	Raises ``ValueError`` if ``recv_seq`` ever decreases: that breaks the
	writer invariant and means the bundle is corrupt or was reordered.

	Args:
		path:          Path to a ``.jsonl`` or ``.jsonl.zst`` file.
		ticker_filter: If set, only events matching one of these tickers
		               are yielded. Events without a recoverable ticker
		               field always pass through (to preserve heartbeats
		               and metadata-only events).
	"""
	path = Path(path)
	last_seq: Optional[int] = None
	for event in _stream_raw(path, ticker_filter):
		seq = event.get("recv_seq")
		if isinstance(seq, int):
			# A ticker filter yields a subsequence, which stays monotonic, so
			# this never false-trips on filtered streams.
			if last_seq is not None and seq < last_seq:
				raise ValueError(
					f"replay loader: recv_seq went backwards in {path} "
					f"({seq} after {last_seq}). The capture writer appends "
					f"monotonically (capture/writer.py), so on-disk order must "
					f"be recv_seq order — this bundle is corrupt or reordered."
				)
			last_seq = seq
		yield event


# ---------------------------------------------------------------------------
# Internal streaming
# ---------------------------------------------------------------------------


def _stream_raw(path: Path, ticker_filter: Optional[set[str]]) -> Iterator[dict]:
	"""Yield parsed events from a raw JSONL or zstd-compressed JSONL file."""
	if path.suffix == ".zst":
		with open(path, "rb") as f:
			dctx = zstd.ZstdDecompressor()
			with dctx.stream_reader(f) as reader:
				yield from _iter_lines(reader, ticker_filter)
	else:
		with open(path, "rb") as f:
			yield from _iter_lines(f, ticker_filter)


def _iter_lines(reader, ticker_filter: Optional[set[str]]) -> Iterator[dict]:
	"""Read bytes from ``reader`` a chunk at a time and yield parsed events.

	Handles oversized lines by buffering until a newline is found OR the
	buffer exceeds MAX_LINE_BYTES, in which case the buffer is dropped
	and scanning continues from the next newline.
	"""
	buffer = b""
	oversized = False
	while True:
		chunk = reader.read(64 * 1024)
		if not chunk:
			# Flush the tail (last line may lack a trailing newline)
			if buffer and not oversized:
				event = _parse_line(buffer)
				if event is not None and _passes_filter(event, ticker_filter):
					yield event
			break
		buffer += chunk

		while b"\n" in buffer:
			line, buffer = buffer.split(b"\n", 1)
			if oversized:
				# We were mid-skip of an oversized line; the newline we just
				# hit marks its end. Reset and keep going.
				oversized = False
				continue
			if len(line) > MAX_LINE_BYTES:
				log.warning(
					"replay loader: skipping oversized line (%d bytes > %d)",
					len(line), MAX_LINE_BYTES,
				)
				continue
			event = _parse_line(line)
			if event is not None and _passes_filter(event, ticker_filter):
				yield event

		# The remaining buffer may itself be an oversized line in progress.
		# If so, drop its content and mark the "skip until newline" state.
		if len(buffer) > MAX_LINE_BYTES:
			log.warning(
				"replay loader: buffer exceeded %d bytes without a newline — "
				"skipping oversized line",
				MAX_LINE_BYTES,
			)
			buffer = b""
			oversized = True


def _parse_line(line: bytes) -> Optional[dict]:
	"""Parse a single JSONL line. Returns None for blank/whitespace/header/malformed."""
	stripped = line.strip()
	if not stripped:
		return None
	try:
		obj = json.loads(stripped)
	except (json.JSONDecodeError, UnicodeDecodeError):
		log.debug("replay loader: skipping malformed line")
		return None
	if not isinstance(obj, dict):
		return None
	if obj.get("header"):
		return None
	return obj


# ---------------------------------------------------------------------------
# Ticker filter
# ---------------------------------------------------------------------------


def _passes_filter(event: dict, ticker_filter: Optional[set[str]]) -> bool:
	"""Return True if ``event`` should be yielded under ``ticker_filter``.

	Events without a recoverable ticker field always pass through so
	heartbeats, subscription acknowledgements, and metadata-only messages
	aren't accidentally filtered out.
	"""
	if ticker_filter is None:
		return True
	ticker = _extract_ticker(event)
	if ticker is None:
		return True
	return ticker in ticker_filter


def _extract_ticker(event: dict) -> Optional[str]:
	"""Best-effort extraction of a ticker from the event payload."""
	payload = event.get("payload")
	if not isinstance(payload, dict):
		return None
	# Synthetic shape: payload.ticker
	ticker = payload.get("ticker")
	if isinstance(ticker, str):
		return ticker
	# WS shape: payload.msg.market_ticker
	msg = payload.get("msg")
	if isinstance(msg, dict):
		mt = msg.get("market_ticker")
		if isinstance(mt, str):
			return mt
	return None
