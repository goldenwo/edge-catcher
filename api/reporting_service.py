"""Service layer for the Reports page (v1.4.0).

Pure functions that wrap edge_catcher.reporting.generate_report with
file-discovery, validation, and read-only sqlite probing. The route
handlers in api/main.py are thin shims over these.
"""
from __future__ import annotations

import logging
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from edge_catcher.reporting import generate_report

logger = logging.getLogger(__name__)

# Note: Path(__file__).resolve() follows symlinks, which is the desired
# behavior for `pip install -e .` (editable installs resolve to source repo).
# Bind-mounted Docker installs that mount the repo at the same path
# work transparently. For unusual layouts (vendored copy, system-package
# install with data/ elsewhere), tests and route handlers can pass an
# explicit `data_dir=` argument to override; no env-var override is
# defined in v1.4.0 (YAGNI until requested).
_REPO_ROOT = Path(__file__).resolve().parents[1]   # api/ → repo root
_DATA_DIR = _REPO_ROOT / "data"
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dataclass
class DbInfo:
	name: str       # basename only, e.g. "paper_trades_v2.db"
	size_mb: float
	mtime: str      # iso8601
	row_count: int  # paper_trades row count, 0 if table missing


def list_dbs(data_dir: Path = _DATA_DIR) -> list[DbInfo]:
	"""Discover *.db files in data_dir. Sorted by mtime desc (most-recent first).

	Skips files that don't have a paper_trades table — they're not reportable.
	Returns empty list if data_dir doesn't exist.

	Each candidate file is opened READ-ONLY via SQLite URI mode
	(`file:<path>?mode=ro`) to coexist with an actively-writing paper trader.
	Per-file `sqlite3.OperationalError` (locked / corrupt / not-a-sqlite-file
	ending in .db) is caught and the file is skipped with a logged warning —
	one bad file does not 500 the whole list.
	"""
	if not data_dir.exists():
		return []
	out: list[DbInfo] = []
	for path in sorted(data_dir.glob("*.db"), key=lambda p: p.stat().st_mtime, reverse=True):
		try:
			# Use Path.as_uri() for cross-platform safety: produces
			# `file:///E:/Projects/...` on Windows (forward slashes,
			# URI-legal) and `file:///home/...` on Linux. f-string
			# interpolation of a raw Path on Windows yields backslashes
			# which sqlite's URI parser does NOT handle.
			uri = path.as_uri() + "?mode=ro"
			con = sqlite3.connect(uri, uri=True)
			try:
				# Probe paper_trades existence + row count in two queries.
				# The LIMIT 1 confirms reportability cheaply (sqlite catalog
				# lookup, O(1)); the COUNT(*) only runs on confirmed-
				# paper_trades files.
				con.execute("SELECT 1 FROM paper_trades LIMIT 1").fetchone()
				row = con.execute("SELECT COUNT(*) FROM paper_trades").fetchone()
				row_count = int(row[0])
			finally:
				con.close()
			stat = path.stat()
		except (sqlite3.DatabaseError, FileNotFoundError) as exc:
			# Catches the DatabaseError hierarchy (OperationalError for
			# missing-table/locked, plain DatabaseError for "file is not
			# a database" / corrupt bytes) AND FileNotFoundError for
			# the paper-trader-rotation TOCTOU race. One bad file does
			# not 500 the whole list.
			logger.warning("list_dbs: skipping %s: %s", path.name, exc)
			continue
		out.append(DbInfo(
			name=path.name,
			size_mb=round(stat.st_size / (1024 * 1024), 2),
			mtime=datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
			row_count=row_count,
		))
	return out


def run_report(db_name: str, date: str | None, data_dir: Path = _DATA_DIR) -> dict:
	"""Run generate_report against {data_dir}/{db_name}.

	Validates db_name is a basename only (no slashes, no traversal),
	validates date is YYYY-MM-DD if provided. Raises ValueError on bad
	inputs, FileNotFoundError if the resolved path doesn't exist or if
	generate_report returns an error-shaped dict.
	"""
	# Reject empty string + path-traversal characters. Empty string is
	# the load-bearing check: `data_dir / ""` resolves to `data_dir`,
	# which exists, which would silently bypass the FileNotFoundError
	# branch.
	if not db_name or "/" in db_name or "\\" in db_name or db_name.startswith(".."):
		raise ValueError(f"invalid db name: {db_name!r}")
	if date is not None and not _DATE_RE.fullmatch(date):
		raise ValueError(f"invalid date format: {date!r}")
	db_path = data_dir / db_name
	if not db_path.exists():
		raise FileNotFoundError(db_name)
	result = generate_report(db_path, date=date)
	# generate_report's only documented error path is missing-DB → {"error": str}.
	# Defensive: if any other error-shaped result appears (corrupt DB, future
	# changes), normalize to FileNotFoundError so the route layer maps it to
	# 404 consistently and the frontend Report type stays honest.
	if isinstance(result, dict) and "error" in result:
		raise FileNotFoundError(result["error"])
	return result
