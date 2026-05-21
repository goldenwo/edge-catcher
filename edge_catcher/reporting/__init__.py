"""P&L reporting for paper_trades sqlite DBs.

Ported and corrected from the historical scripts/daily_pnl_report.py.
Math fixes:
  - deployed = SUM(entry_price * fill_size)  (entry_price is per-contract cents)
  - "today" filter uses exit_time, not entry_time (matches the "settled today" label)
  - status IN ('won','lost','scratch') is safer than `!= 'open'`
  - NULL-safe aggregates via COALESCE
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import pathname2url


def _db_ro_uri(db_path: Path) -> str:
	"""Build a strictly read-only SQLite ``file:`` URI for ``db_path``.

	Spec §5/§7: reporting runs against the LIVE money DB and must NEVER be
	able to write it — it opens with ``mode=ro``.

	Why not a naive ``f"file:{db_path}?mode=ro"``: a SQLite file URI's PATH
	portion must be URI-encoded while ``?mode=ro`` stays a real query. An
	un-encoded path silently breaks when it contains a space, ``#`` or
	``?`` (``#`` starts a URI fragment / ``?`` a query — SQLite then opens
	a *different* (often fresh, empty) DB), and a Windows ``C:\\...`` drive
	path is not a valid file-URI body. ``pathname2url`` over the *resolved*
	(absolute) path yields the correct cross-platform body — ``/C:/...`` on
	Windows, ``/abs/path`` on POSIX — with spaces/special chars
	percent-encoded; the literal ``?mode=ro`` is appended AFTER encoding so
	it remains a live query parameter (not percent-encoded away).
	"""
	encoded = pathname2url(os.fspath(Path(db_path).resolve()))
	return f"file:{encoded}?mode=ro"


def generate_report(db_path: Path, date: str | None = None) -> dict:
	"""Generate a P&L report from a paper_trades DB.

	Args:
		db_path: Path to the sqlite DB with a `paper_trades` table.
		date: 'YYYY-MM-DD' for a specific day's settlement bucket (EDT day).
			If None, defaults to UTC today.

	Returns:
		dict with keys: timestamp, date, all_time, today, today_by_strategy,
		open_positions, all_time_by_strategy, or {'error': str} if the DB doesn't exist.
	"""
	if not Path(db_path).exists():
		return {"error": f"DB not found at {db_path}"}
	date_str = date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
	# §5/§7: read-only URI connect — reporting can never write the money DB.
	con = sqlite3.connect(_db_ro_uri(db_path), uri=True)
	try:
		all_time = _all_time_stats(con)
		today = _today_stats(con, date_str)
		today_by_strategy = _today_by_strategy(con, date_str)
		open_positions = _open_positions(con)
		all_time_by_strategy = _all_time_by_strategy(con)
	finally:
		con.close()
	return {
		"timestamp": datetime.now(timezone.utc).isoformat(),
		"date": date_str,
		"all_time": all_time,
		"today": today,
		"today_by_strategy": today_by_strategy,
		"open_positions": open_positions,
		"all_time_by_strategy": all_time_by_strategy,
	}


def _all_time_stats(con: sqlite3.Connection) -> dict:
	row = con.execute(
		"""SELECT
			COUNT(*) AS total_trades,
			SUM(CASE WHEN status='open' THEN 1 ELSE 0 END) AS open_trades,
			SUM(CASE WHEN status='won' THEN 1 ELSE 0 END) AS wins,
			SUM(CASE WHEN status='lost' THEN 1 ELSE 0 END) AS losses,
			SUM(CASE WHEN status='scratch' THEN 1 ELSE 0 END) AS scratches,
			COALESCE(SUM(CASE WHEN status IN ('won','lost','scratch') THEN pnl_cents END), 0) AS net_pnl_cents,
			COALESCE(SUM(CASE WHEN status IN ('won','lost','scratch') THEN entry_fee_cents END), 0) AS fees_cents,
			COALESCE(SUM(CASE WHEN status IN ('won','lost','scratch')
					THEN entry_price * fill_size END), 0) AS deployed_cents
		FROM paper_trades"""
	).fetchone()
	total, open_, wins, losses, scratches, net_pnl, fees, deployed = row
	# §7:147: a scratch (~0 pnl) is a CLOSED trade — it counts toward the
	# closed-trade count and the win-rate/avg-pnl DENOMINATOR (it is not a
	# win, so it correctly dilutes win-rate), and its actual pnl_cents
	# flows into net_pnl above (never hardcoded 0). It is never
	# reclassified as a win or loss (the wins/losses CASE sums are
	# unchanged). Reported as its own line via the by-strategy GROUP BY.
	closed = (wins or 0) + (losses or 0) + (scratches or 0)
	win_rate = (wins / closed * 100) if closed else 0.0
	avg_pnl = (net_pnl / closed) if closed else 0.0
	roi = (net_pnl / deployed * 100) if deployed else 0.0
	return {
		"total_trades": total,
		"open_trades": open_ or 0,
		"closed_trades": closed,
		"wins": wins or 0,
		"losses": losses or 0,
		"win_rate_pct": round(win_rate, 1),
		"net_pnl_cents": net_pnl,
		"net_pnl_usd": round(net_pnl / 100, 2),
		"avg_pnl_cents": round(avg_pnl, 1),
		"fees_cents": fees,
		"deployed_cents": deployed,
		"deployed_usd": round(deployed / 100, 2),
		"roi_deployed_pct": round(roi, 2),
	}


def _today_stats(con: sqlite3.Connection, date_str: str) -> dict:
	row = con.execute(
		"""SELECT
			COUNT(*) AS n,
			COALESCE(SUM(pnl_cents), 0) AS pnl_cents
		FROM paper_trades
		WHERE status IN ('won','lost','scratch')
		  AND date(datetime(exit_time, '-4 hours')) = ?""",
		(date_str,),
	).fetchone()
	n, pnl = row
	return {"settled_count": n, "pnl_cents": pnl, "pnl_usd": round(pnl / 100, 2)}


def _today_by_strategy(con: sqlite3.Connection, date_str: str) -> list[dict]:
	rows = con.execute(
		"""SELECT
			strategy,
			series_ticker,
			status,
			COUNT(*) AS n,
			COALESCE(SUM(pnl_cents), 0) AS pnl_cents
		FROM paper_trades
		WHERE status IN ('won','lost','scratch')
		  AND date(datetime(exit_time, '-4 hours')) = ?
		GROUP BY strategy, series_ticker, status
		ORDER BY strategy, series_ticker, status""",
		(date_str,),
	).fetchall()
	return [
		{"strategy": r[0], "series_ticker": r[1], "status": r[2], "count": r[3], "pnl_cents": r[4]}
		for r in rows
	]


def _open_positions(con: sqlite3.Connection) -> list[dict]:
	rows = con.execute(
		"""SELECT
			strategy,
			series_ticker,
			COUNT(*) AS n
		FROM paper_trades
		WHERE status = 'open'
		GROUP BY strategy, series_ticker
		ORDER BY strategy, series_ticker"""
	).fetchall()
	return [
		{"strategy": r[0], "series_ticker": r[1], "count": r[2]}
		for r in rows
	]


def _all_time_by_strategy(con: sqlite3.Connection) -> list[dict]:
	rows = con.execute(
		"""SELECT
			strategy,
			COUNT(*) AS closed_trades,
			SUM(CASE WHEN status = 'won' THEN 1 ELSE 0 END) AS wins,
			COALESCE(SUM(pnl_cents), 0) AS net_pnl_cents
		FROM paper_trades
		WHERE status IN ('won','lost','scratch')
		GROUP BY strategy
		ORDER BY strategy"""
	).fetchall()
	out = []
	for strategy, closed, wins, net_pnl in rows:
		win_rate = (wins / closed * 100) if closed else 0.0
		out.append({
			"strategy": strategy,
			"closed_trades": closed,
			"wins": wins or 0,
			"net_pnl_cents": net_pnl,
			"net_pnl_usd": round(net_pnl / 100, 2),
			"win_rate_pct": round(win_rate, 1),
		})
	return out
