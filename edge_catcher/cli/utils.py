"""CLI: utility commands — list-dbs and archive."""

import json
import sqlite3
from pathlib import Path


def _run_list_dbs(args) -> None:
	data_dir = Path("data")
	databases = []
	for db_file in sorted(data_dir.glob("*.db")):
		size_mb = round(db_file.stat().st_size / (1024 * 1024), 1)
		try:
			conn = sqlite3.connect(str(db_file))
			rows = conn.execute(
				"SELECT DISTINCT series_ticker FROM markets ORDER BY series_ticker"
			).fetchall()
			conn.close()
			series = [r[0] for r in rows]
		except Exception:
			series = []
		databases.append({"path": str(db_file), "size_mb": size_mb, "series": series})
	print(json.dumps({"databases": databases}))


def _run_archive(args) -> None:
	from edge_catcher.storage.db import get_connection
	from edge_catcher.storage.archiver import archive_old_trades

	conn = get_connection(Path(args.db_path))
	try:
		result = archive_old_trades(conn, Path(args.archive_dir), days_to_keep=90)
		if result["rows_archived"]:
			print(
				f"Archived {result['rows_archived']} trades → {result['archive_file']}"
			)
		else:
			print("No trades old enough to archive.")
	finally:
		conn.close()


def register(subparsers) -> None:
	ldbs = subparsers.add_parser(
		"list-dbs",
		help="Scan data/ for *.db files and list their series as JSON",
	)
	ldbs.set_defaults(func=_run_list_dbs)

	ar = subparsers.add_parser("archive", help="Archive trades older than 90 days")
	ar.add_argument("--db-path", default="data/kalshi.db")
	ar.add_argument("--archive-dir", default="data/archive")
	ar.set_defaults(func=_run_archive)
