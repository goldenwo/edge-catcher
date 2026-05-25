"""SQL migration runner for live_trades.db (and any other edge-catcher SQLite DB).

Usage::

    from pathlib import Path
    from edge_catcher.storage.migrations import apply_migrations

    conn = sqlite3.connect("data/live_trades.db")
    applied = apply_migrations(conn, Path(__file__).parent)

Design constraints (YAGNI):
- No rollback / down-migrations.
- No cross-file transactions — each file is applied in its own implicit
  transaction (executescript commits before running).
- No version comparison beyond integer ordering of the filename prefix.
- Idempotent: already-applied versions are silently skipped.

Migration files must be named ``NNNN_<slug>.sql`` where NNNN is a
zero-padded integer (e.g. ``0001_create_kill_switch.sql``). Files are
sorted by the numeric prefix in ascending order so filesystem ordering
is irrelevant.
"""
from __future__ import annotations

import logging
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)

_MIGRATION_RE = re.compile(r"^(\d+)_.*\.sql$")

# DDL for the migrations-tracking table.  Created once on first call.
_INIT_SQL = """
CREATE TABLE IF NOT EXISTS live_schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);
"""

# Default migrations directory: the package directory itself (alongside this
# __init__.py).  Callers may pass an explicit migrations_dir to override.
_DEFAULT_MIGRATIONS_DIR = Path(__file__).parent


def _extract_version(filename: str) -> int | None:
	"""Return the integer version prefix from a migration filename, or None."""
	m = _MIGRATION_RE.match(filename)
	if m is None:
		return None
	return int(m.group(1))


def apply_migrations(
	conn: sqlite3.Connection,
	migrations_dir: Path | None = None,
) -> list[int]:
	"""Apply all pending migrations from *migrations_dir* to *conn*.

	Args:
		conn: An open SQLite connection.  The caller owns the connection
			lifecycle; this function does not close it.
		migrations_dir: Directory containing ``NNNN_*.sql`` migration files.
			Defaults to the package directory (alongside this ``__init__.py``),
			which is ``edge_catcher/storage/migrations/``.
			Raises ``FileNotFoundError`` with a clear message if the directory
			does not exist.

	Returns:
		A list of version integers that were applied during this call (empty
		when all migrations were already present in ``live_schema_migrations``).

	Raises:
		FileNotFoundError: If *migrations_dir* does not exist.
		sqlite3.DatabaseError: On any SQL error during migration execution.

	**Atomicity warning for future DML migrations:** ``executescript`` issues
	its own implicit COMMIT for the migration body, then the
	``live_schema_migrations`` INSERT is committed by a second ``conn.commit()``.
	If the process dies between those two commits, the migration body is
	persisted but the tracking row is missing — the migration re-runs on
	next startup. **This is safe today** because every Phase 1 migration is
	idempotent ``CREATE TABLE IF NOT EXISTS`` DDL. **It is NOT safe for any
	future DML migration** (``INSERT`` / ``UPDATE`` / ``DELETE``), which
	would silently double-apply and corrupt data. Any such migration must
	wrap its body in an explicit ``BEGIN; ... COMMIT;`` paired with the
	tracking-row INSERT inside the same transaction. Until that's needed,
	the YAGNI two-commit pattern stays.
	"""
	if migrations_dir is None:
		migrations_dir = _DEFAULT_MIGRATIONS_DIR

	if not migrations_dir.is_dir():
		raise FileNotFoundError(
			f"migrations_dir does not exist or is not a directory: {migrations_dir}"
		)

	# Ensure the tracking table exists.
	conn.executescript(_INIT_SQL)

	# Read already-applied versions.
	applied_versions: set[int] = {
		row[0]
		for row in conn.execute("SELECT version FROM live_schema_migrations").fetchall()
	}

	# Collect and sort migration files by numeric prefix.
	migration_files: list[tuple[int, Path]] = []
	for path in migrations_dir.iterdir():
		if not path.is_file() or path.suffix != ".sql":
			continue
		version = _extract_version(path.name)
		if version is None:
			log.debug("Skipping non-migration file in migrations_dir: %s", path.name)
			continue
		migration_files.append((version, path))

	migration_files.sort(key=lambda pair: pair[0])

	newly_applied: list[int] = []
	for version, path in migration_files:
		if version in applied_versions:
			log.debug("Migration %04d already applied — skipping", version)
			continue

		sql = path.read_text(encoding="utf-8")
		log.info("Applying migration %04d: %s", version, path.name)

		# executescript() issues an implicit COMMIT before executing and
		# commits after — same pattern as storage/db.py:init_db.
		#
		# Crash-window idempotency for additive ADD COLUMN migrations: the body
		# commit (above) and the tracking-row commit (below) are separate, so a
		# crash between them re-runs the body on the next boot. SQLite ADD COLUMN
		# is not idempotent — a re-run raises "duplicate column name". Tolerate
		# exactly that (the columns already exist from the crashed prior run) and
		# fall through to record the version so it never re-runs again. Any OTHER
		# OperationalError (a genuine SQL error) still propagates. NOTE: this
		# tolerance assumes an all-or-nothing additive body (independent nullable
		# ADD COLUMNs, as 0004 is). Keep future ADD COLUMN migrations independent
		# + nullable so a crash-window re-run is always safe.
		try:
			conn.executescript(sql)
		except sqlite3.OperationalError as exc:
			if "duplicate column name" not in str(exc).lower():
				raise
			log.warning(
				"Migration %04d: columns already present (crash-window re-run) "
				"— recording version without re-applying: %s",
				version,
				exc,
			)

		# Record the applied version.
		conn.execute(
			"INSERT INTO live_schema_migrations (version, applied_at) VALUES (?, ?)",
			(version, datetime.now(timezone.utc).isoformat()),
		)
		conn.commit()

		newly_applied.append(version)
		log.info("Migration %04d applied successfully", version)

	if not newly_applied:
		log.debug("All migrations already applied — nothing to do")

	return newly_applied
