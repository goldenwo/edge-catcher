"""Raw frame writer for the orderbook capture pipeline.

Tees parsed engine input events to JSONL files for later replay. The writer
runs on the same thread as the engine message loop and MUST NEVER raise into
the engine — any internal failure is logged and the write is dropped. This
invariant is enforced by test_capture_writer.test_write_ws_never_raises_on_*.

POSIX flock prevents two writer processes from corrupting the same file.
On Windows (dev workstation) the lock is a no-op because fcntl is unavailable;
real contention only happens on the Pi.

See docs/superpowers/specs/2026-04-14-orderbook-capture-replay-design.md §4.2.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

log = logging.getLogger(__name__)

# POSIX-only file locking. Windows dev machines run tests without contention
# protection; the Pi is the only production writer.
try:
	import fcntl  # type: ignore[import-not-found]
	_HAS_FCNTL = True
except ImportError:
	_HAS_FCNTL = False


# How often to check free disk space (in writes).
_DISK_CHECK_INTERVAL = 1000

# How many bytes of the active JSONL file to scan when recovering recv_seq
# from a corrupted sidecar. 4 MiB is enough to contain the last flush interval
# many times over at typical payload sizes.
_TAIL_SCAN_BYTES = 4 * 1024 * 1024


class CaptureLockError(Exception):
	"""Raised at writer construction time when another paper-trader process
	already holds the writer lock on the output directory. The caller should
	refuse to start; two writers would corrupt the JSONL."""


class RawFrameWriter:
	"""Append-only JSONL writer for engine input events.

	Rotates by UTC date. Maintains a `recv_seq` counter that is monotonic
	across process restarts (via the `.recv_seq` sidecar + tail scan).
	Silently stops writing on disk pressure so the engine never blocks on I/O.
	"""

	def __init__(
		self,
		output_dir: Path,
		enabled: bool = True,
		min_free_gb: int = 10,
		seq_flush_every: int = 100,
		rotation_callback: Optional[Callable[[date], None]] = None,
	) -> None:
		self.output_dir = Path(output_dir)
		self.enabled = enabled
		self.min_free_gb = min_free_gb
		self.seq_flush_every = seq_flush_every
		self._rotation_callback = rotation_callback
		self._stopped = False  # set True permanently on disk pressure
		self._active_file: Optional[Any] = None
		self._active_date: Optional[date] = None
		self._recv_seq = 0
		self._writes_since_seq_flush = 0
		self._writes_since_disk_check = 0
		self._lock_fd: Optional[Any] = None

		if not self.enabled:
			return

		self.output_dir.mkdir(parents=True, exist_ok=True)
		self._acquire_lock()
		self._init_recv_seq()

	# ------------------------------------------------------------------
	# Lifecycle
	# ------------------------------------------------------------------

	def _acquire_lock(self) -> None:
		"""Acquire an exclusive flock on output_dir/.writer.lock.

		Raises CaptureLockError if another process already holds it. On
		Windows (no fcntl), the lock is skipped — contention is a Pi-only
		concern and dev-workstation tests don't exercise it.
		"""
		if not _HAS_FCNTL:
			log.info("fcntl unavailable (Windows?); skipping writer lock")
			return
		lock_path = self.output_dir / ".writer.lock"
		lock_fd = open(lock_path, "w")
		try:
			fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
		except OSError as e:
			lock_fd.close()
			raise CaptureLockError(
				f"Another paper-trader process already holds {lock_path}. "
				f"Refusing to start — two writers would corrupt the JSONL."
			) from e
		self._lock_fd = lock_fd

	def _init_recv_seq(self) -> None:
		"""Initialize the recv_seq counter from disk state.

		Reads both the `.recv_seq` sidecar AND scans the tail of today's
		active JSONL, then uses max(sidecar, scanned) + 1 as the starting
		counter. We read both because the sidecar lags the JSONL by up to
		`seq_flush_every` writes — trusting the sidecar alone after a crash
		between flushes would reuse recv_seq values that already exist in
		the file.
		"""
		sidecar = self.output_dir / ".recv_seq"
		sidecar_value = 0
		if sidecar.exists():
			try:
				sidecar_value = int(sidecar.read_text().strip())
			except (ValueError, OSError):
				log.warning("recv_seq sidecar corrupted; falling back to JSONL tail scan")
				sidecar_value = 0
		scan_value = self._scan_active_for_max_seq()
		self._recv_seq = max(sidecar_value, scan_value)

	def _scan_active_for_max_seq(self) -> int:
		"""Scan the last _TAIL_SCAN_BYTES of today's active JSONL for the
		highest recv_seq. Returns 0 if the file doesn't exist or no valid
		lines are found."""
		active = self.output_dir / f"kalshi_engine_{datetime.now(timezone.utc).date().isoformat()}.jsonl"
		if not active.exists():
			return 0
		size = active.stat().st_size
		offset = max(0, size - _TAIL_SCAN_BYTES)
		max_seq = 0
		with open(active, "rb") as f:
			f.seek(offset)
			if offset > 0:
				f.readline()  # discard partial line at start of window
			for line_bytes in f:
				try:
					obj = json.loads(line_bytes)
				except (json.JSONDecodeError, UnicodeDecodeError):
					continue
				seq = obj.get("recv_seq")
				if isinstance(seq, int) and seq > max_seq:
					max_seq = seq
		return max_seq

	# ------------------------------------------------------------------
	# Public write API — must never raise
	# ------------------------------------------------------------------

	def write_ws(self, msg: dict, recv_ts: Optional[datetime] = None) -> None:
		"""Append one parsed WS message. Best-effort; never raises.

		``recv_ts`` — when provided, used as the event's on-disk timestamp.
		The engine MUST pass the same ``now`` it threads to dispatch_message
		so that replay's entry_time matches live's entry_time exactly.
		When None, the writer reads its own clock (test-only fallback).
		"""
		self._write_event(source="ws", payload=msg, recv_ts=recv_ts)

	def write_synthetic(self, kind: str, payload: dict, recv_ts: Optional[datetime] = None) -> None:
		"""Append one synthetic event from a non-WS source. Best-effort; never raises.

		``kind`` is one of: 'rest_orderbook', 'ticker_discovered', 'settlement'.
		The on-disk ``source`` field becomes ``f"synthetic.{kind}"``.
		``recv_ts`` — same contract as ``write_ws``: pass the ``now`` the
		live engine used for the corresponding store call so replay produces
		byte-identical timestamps.
		"""
		self._write_event(source=f"synthetic.{kind}", payload=payload, recv_ts=recv_ts)

	def _write_event(
		self,
		source: str,
		payload: dict,
		recv_ts: Optional[datetime] = None,
	) -> None:
		"""Core write path. Wraps everything in try/except so the writer
		never leaks an exception into the engine message loop."""
		if self._stopped or not self.enabled:
			return
		try:
			self._maybe_rotate()
			self._maybe_check_disk()
			if self._stopped:
				return
			self._recv_seq += 1
			ts = recv_ts if recv_ts is not None else datetime.now(timezone.utc)
			line = json.dumps({
				"recv_seq": self._recv_seq,
				"recv_ts": ts.isoformat(),
				"source": source,
				"payload": payload,
			})
			assert self._active_file is not None  # _maybe_rotate guarantees this
			self._active_file.write(line + "\n")
			self._writes_since_seq_flush += 1
			if self._writes_since_seq_flush >= self.seq_flush_every:
				self._flush_seq()
		except Exception as e:  # noqa: BLE001 — best-effort capture
			log.warning("capture write failed: %s", e)

	# ------------------------------------------------------------------
	# Rotation
	# ------------------------------------------------------------------

	def _maybe_rotate(self) -> None:
		"""Open today's file if no active file, or rotate if the UTC date has crossed."""
		today = datetime.now(timezone.utc).date()
		if self._active_file is None:
			self._open_active(today)
			return
		if self._active_date != today:
			self._rotate(today)

	def _open_active(self, day: date) -> None:
		"""Open (or reopen) the JSONL file for `day` in append mode. Emits
		a schema header line if the file is new."""
		path = self.output_dir / f"kalshi_engine_{day.isoformat()}.jsonl"
		is_new = not path.exists()
		self._active_file = open(path, "a", buffering=1, encoding="utf-8")  # line-buffered
		self._active_date = day
		if is_new:
			header = json.dumps({
				"schema_version": 1,
				"exchange": "kalshi",
				"captured_at": datetime.now(timezone.utc).isoformat(),
				"header": True,
			})
			self._active_file.write(header + "\n")

	def _rotate(self, new_day: date) -> None:
		"""Close yesterday's file and open a new one for ``new_day``.

		If a ``rotation_callback`` was registered at construction time, it
		is invoked SYNCHRONOUSLY on the engine thread after the new file
		is opened. This is a deliberate design choice: the callback is
		expected to deep-copy any live engine state it needs (e.g.
		market_state) immediately, which must happen on the engine thread
		to avoid a race with ongoing mutations. The callback is then
		responsible for spawning its own background thread for the slow
		bundle-assembly + upload work.

		Exceptions in the callback are caught and logged so a bundle
		assembly failure never propagates into the engine loop.
		"""
		old_day = self._active_date
		try:
			self._active_file.close()
		except Exception as e:  # pragma: no cover
			log.warning("error closing rotated file: %s", e)
		self._active_file = None
		log.info("orderbook capture: rotated %s → %s", old_day, new_day)
		self._open_active(new_day)

		if self._rotation_callback is not None and old_day is not None:
			try:
				self._rotation_callback(old_day)
			except Exception:
				log.exception("rotation_callback failed for %s", old_day)

	# ------------------------------------------------------------------
	# Disk pressure
	# ------------------------------------------------------------------

	def _maybe_check_disk(self) -> None:
		"""Every _DISK_CHECK_INTERVAL writes, sample free disk space. If
		free space has dropped below `min_free_gb`, permanently stop writing
		and log a warning. The engine continues running; capture just goes
		dark until the operator frees space and restarts the trader."""
		self._writes_since_disk_check += 1
		if self._writes_since_disk_check < _DISK_CHECK_INTERVAL:
			return
		self._writes_since_disk_check = 0
		free_bytes = self._free_disk_bytes()
		free_gb = free_bytes / (1024 ** 3)
		if free_gb < self.min_free_gb:
			log.warning(
				"orderbook capture stopped: free disk dropped to %.1f GB "
				"(below threshold of %d GB). Restart paper-trader after freeing space.",
				free_gb, self.min_free_gb,
			)
			self._stopped = True

	def _free_disk_bytes(self) -> int:
		"""Return free bytes on output_dir's filesystem. os.statvfs is POSIX-only;
		on Windows (dev tests) shutil.disk_usage is used."""
		statvfs = getattr(os, "statvfs", None)
		if statvfs is not None:
			stat = statvfs(self.output_dir)
			return stat.f_bavail * stat.f_frsize
		import shutil
		return shutil.disk_usage(self.output_dir).free

	# ------------------------------------------------------------------
	# recv_seq sidecar
	# ------------------------------------------------------------------

	def _flush_seq(self) -> None:
		"""Atomically update the `.recv_seq` sidecar. Failure is warned-and-ignored
		— a crash between flushes is handled by the tail-scan recovery path."""
		sidecar = self.output_dir / ".recv_seq"
		try:
			sidecar.write_text(str(self._recv_seq))
		except OSError as e:
			log.warning("recv_seq sidecar flush failed: %s", e)
		self._writes_since_seq_flush = 0

	# ------------------------------------------------------------------
	# Shutdown
	# ------------------------------------------------------------------

	def close(self) -> None:
		"""Close the active file, flush the sidecar, release the lock.
		Idempotent."""
		if self._active_file is not None:
			try:
				self._active_file.close()
			except Exception:  # pragma: no cover
				pass
			self._active_file = None
		if self.enabled:
			self._flush_seq()
		if self._lock_fd is not None:
			try:
				if _HAS_FCNTL:
					fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
				self._lock_fd.close()
			except Exception:  # pragma: no cover
				pass
			self._lock_fd = None
