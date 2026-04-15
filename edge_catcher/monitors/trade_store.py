"""SQLite storage for paper trades and strategy state."""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from edge_catcher.fees import STANDARD_FEE


class DuplicateOpenTradeError(Exception):
	"""Raised when record_trade is called with a composite key that matches
	an existing open trade.

	The composite key is (strategy, ticker, side, entry_time). Per spec §4.1,
	this key must be unique across open rows so the capture/replay pipeline's
	synthetic.settlement event can resolve the target row unambiguously.

	In the live engine this indicates either a strategy that scales in at the
	same millisecond (shouldn't happen today) or a serious bug. Treat it as
	fatal — do not catch and retry.
	"""


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS paper_trades (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	ticker TEXT NOT NULL,
	entry_price INTEGER NOT NULL,
	entry_time TEXT NOT NULL,
	exit_price INTEGER,
	exit_time TEXT,
	pnl_cents INTEGER,
	status TEXT NOT NULL DEFAULT 'open',
	strategy TEXT NOT NULL DEFAULT 'unknown',
	side TEXT NOT NULL DEFAULT 'yes',
	series_ticker TEXT,
	entry_fee_cents INTEGER NOT NULL DEFAULT 0,
	intended_size INTEGER NOT NULL DEFAULT 1,
	fill_size INTEGER NOT NULL DEFAULT 1,
	blended_entry INTEGER,
	book_depth INTEGER,
	fill_pct REAL,
	slippage_cents REAL,
	book_snapshot TEXT
);
CREATE INDEX IF NOT EXISTS idx_paper_trades_ticker ON paper_trades (ticker);
CREATE INDEX IF NOT EXISTS idx_paper_trades_status ON paper_trades (status);
CREATE INDEX IF NOT EXISTS idx_paper_trades_strategy ON paper_trades (strategy);

CREATE TABLE IF NOT EXISTS strategy_state (
	strategy TEXT NOT NULL,
	key TEXT NOT NULL,
	value TEXT NOT NULL,
	updated_at TEXT NOT NULL,
	PRIMARY KEY (strategy, key)
);
"""

# Columns added by migration (safe ALTER TABLE path for existing DBs)
_MIGRATION_COLUMNS: list[tuple[str, str]] = [
	("strategy", "TEXT NOT NULL DEFAULT 'unknown'"),
	("side", "TEXT NOT NULL DEFAULT 'yes'"),
	("series_ticker", "TEXT"),
	("entry_fee_cents", "INTEGER NOT NULL DEFAULT 0"),
	("intended_size", "INTEGER NOT NULL DEFAULT 1"),
	("fill_size", "INTEGER NOT NULL DEFAULT 1"),
	("blended_entry", "INTEGER"),
	("book_depth", "INTEGER"),
	("fill_pct", "REAL"),
	("slippage_cents", "REAL"),
	("book_snapshot", "TEXT"),
]


class TradeStore:
	"""Manages paper trade records and strategy state in a local SQLite database."""

	def __init__(self, db_path: Path) -> None:
		db_path.parent.mkdir(parents=True, exist_ok=True)
		self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
		self._conn.execute("PRAGMA journal_mode=WAL")
		self._conn.executescript(_SCHEMA)
		self._migrate()

	# -------------------------------------------------------------------------
	# Migration
	# -------------------------------------------------------------------------

	def _migrate(self) -> None:
		"""Safe ALTER TABLE migrations for pre-existing databases."""
		for col_name, col_def in _MIGRATION_COLUMNS:
			try:
				self._conn.execute(
					f"ALTER TABLE paper_trades ADD COLUMN {col_name} {col_def}"
				)
				self._conn.commit()
			except sqlite3.OperationalError:
				pass  # Column already exists

		# Backfill NULLs introduced by the migration
		self._conn.execute(
			"UPDATE paper_trades SET fill_pct = 1.0 WHERE fill_pct IS NULL"
		)
		self._conn.execute(
			"UPDATE paper_trades SET slippage_cents = 0.0 WHERE slippage_cents IS NULL"
		)
		self._conn.commit()

	# -------------------------------------------------------------------------
	# Trades
	# -------------------------------------------------------------------------

	def record_trade(
		self,
		ticker: str,
		entry_price: int,
		strategy: str,
		side: str,
		series_ticker: str,
		intended_size: int = 1,
		fill_size: int = 1,
		blended_entry: Optional[int] = None,
		book_depth: Optional[int] = None,
		fill_pct: Optional[float] = None,
		slippage_cents: Optional[float] = None,
		book_snapshot: Optional[str] = None,
		*,
		now: datetime,
	) -> int:
		"""Insert a new open trade and return its row id.

		`now` is written as `entry_time`. The caller passes
		`datetime.now(timezone.utc)` (live engine) or the captured `recv_ts`
		(replay backtester). This is required for parity between the two paths.
		"""
		if now.tzinfo is None:
			raise ValueError("now must be timezone-aware")
		# Treat blended_entry=0 as None — sub-cent book levels round to 0¢ and must
		# not be used as cost basis; fall back to entry_price instead.
		blended_entry = blended_entry if blended_entry else None
		effective_price = blended_entry if blended_entry is not None else entry_price
		entry_fee_cents = int(STANDARD_FEE.calculate(effective_price, fill_size))
		entry_time_iso = now.isoformat()

		# Composite-key uniqueness check (spec §4.1) — see DuplicateOpenTradeError.
		existing = self._conn.execute(
			"SELECT id FROM paper_trades "
			"WHERE strategy=? AND ticker=? AND side=? AND entry_time=? AND status='open'",
			(strategy, ticker, side, entry_time_iso),
		).fetchone()
		if existing is not None:
			raise DuplicateOpenTradeError(
				f"open trade already exists: strategy={strategy} ticker={ticker} "
				f"side={side} entry_time={entry_time_iso} (existing id={existing[0]})"
			)

		cur = self._conn.execute(
			"""
			INSERT INTO paper_trades (
				ticker, entry_price, entry_time, status,
				strategy, side, series_ticker, entry_fee_cents,
				intended_size, fill_size, blended_entry, book_depth,
				fill_pct, slippage_cents, book_snapshot
			) VALUES (?, ?, ?, 'open', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			""",
			(
				ticker, entry_price, entry_time_iso,
				strategy, side, series_ticker, entry_fee_cents,
				intended_size, fill_size, blended_entry, book_depth,  # blended_entry already sanitised above
				fill_pct, slippage_cents, book_snapshot,
			),
		)
		self._conn.commit()
		return cur.lastrowid  # type: ignore[return-value]

	def settle_trade(self, trade_id: int, result: str, *, now: datetime) -> None:
		"""Settle a trade by market resolution result ('yes' or 'no').

		exit_price = 100 when side wins, 0 otherwise.
		pnl = fill_size * (exit_price - effective_entry) - entry_fee
		Uses blended_entry (post-slippage fill price) when available.
		No exit fee at settlement since P*(1-P)=0 at prices 0 and 100.

		`now` is written as `exit_time`. Required for live/replay parity.
		"""
		if now.tzinfo is None:
			raise ValueError("now must be timezone-aware")
		row = self._conn.execute(
			"SELECT entry_price, side, fill_size, entry_fee_cents, blended_entry, status "
			"FROM paper_trades WHERE id=?",
			(trade_id,),
		).fetchone()
		if row is None:
			return
		entry_price, side, fill_size, entry_fee_cents, blended_entry, current_status = row
		if current_status != "open":
			return  # already exited or settled — avoid race with exit_trade
		# blended_entry=0 is a corrupt book walk — fall back to entry_price
		effective_entry = blended_entry if blended_entry else entry_price
		if side == "yes":
			exit_price = 100 if result == "yes" else 0
			status = "won" if result == "yes" else "lost"
		else:  # side == "no"
			exit_price = 100 if result == "no" else 0
			status = "won" if result == "no" else "lost"

		pnl = fill_size * (exit_price - effective_entry) - entry_fee_cents
		exit_time_iso = now.isoformat()
		self._conn.execute(
			"UPDATE paper_trades SET exit_price=?, exit_time=?, pnl_cents=?, status=? WHERE id=? AND status='open'",
			(exit_price, exit_time_iso, pnl, status, trade_id),
		)
		self._conn.commit()

	def exit_trade(self, trade_id: int, exit_price: int, *, now: datetime) -> None:
		"""Exit a trade at a specific price (TP/SL).

		pnl = fill_size * (exit_price - effective_entry) - entry_fee - exit_fee
		Uses blended_entry (post-slippage fill price) when available.
		Exit fee applies because TP/SL exits sell at a mid-price.

		`now` is written as `exit_time`. Required for live/replay parity.
		"""
		if now.tzinfo is None:
			raise ValueError("now must be timezone-aware")
		row = self._conn.execute(
			"SELECT entry_price, fill_size, entry_fee_cents, blended_entry, side, status "
			"FROM paper_trades WHERE id=?",
			(trade_id,),
		).fetchone()
		if row is None:
			return
		entry_price, fill_size, entry_fee_cents, blended_entry, side, current_status = row
		if current_status != "open":
			return  # already settled or exited — avoid race with settle_trade
		# blended_entry=0 is a corrupt book walk — fall back to entry_price
		effective_entry = blended_entry if blended_entry else entry_price
		exit_fee_cents = int(STANDARD_FEE.calculate(exit_price, fill_size))
		pnl = fill_size * (exit_price - effective_entry) - entry_fee_cents - exit_fee_cents
		status = "won" if pnl > 0 else ("lost" if pnl < 0 else "scratch")
		exit_time_iso = now.isoformat()
		self._conn.execute(
			"UPDATE paper_trades SET exit_price=?, exit_time=?, pnl_cents=?, status=? WHERE id=? AND status='open'",
			(exit_price, exit_time_iso, pnl, status, trade_id),
		)
		self._conn.commit()

	def get_open_trades(self) -> list[dict[str, Any]]:
		"""Return all open trades as dicts."""
		rows = self._conn.execute(
			"""
			SELECT id, ticker, entry_price, strategy, side, series_ticker,
			       entry_fee_cents, intended_size, fill_size, blended_entry,
			       book_depth, fill_pct, slippage_cents, status, entry_time
			FROM paper_trades WHERE status='open'
			"""
		).fetchall()
		return [_row_to_dict(r) for r in rows]

	def get_trade_by_id(self, trade_id: int) -> dict[str, Any] | None:
		"""Return a single trade by id, or None if not found."""
		row = self._conn.execute(
			"""
			SELECT id, ticker, entry_price, strategy, side, series_ticker,
			       entry_fee_cents, intended_size, fill_size, blended_entry,
			       book_depth, fill_pct, slippage_cents, status, entry_time,
			       exit_price, exit_time, pnl_cents
			FROM paper_trades WHERE id=?
			""",
			(trade_id,),
		).fetchone()
		return _row_to_dict(row) if row else None

	def get_open_trades_for(self, strategy: str, ticker: str) -> list[dict[str, Any]]:
		"""Return open trades filtered by strategy and ticker."""
		rows = self._conn.execute(
			"""
			SELECT id, ticker, entry_price, strategy, side, series_ticker,
			       entry_fee_cents, intended_size, fill_size, blended_entry,
			       book_depth, fill_pct, slippage_cents, status, entry_time
			FROM paper_trades WHERE status='open' AND strategy=? AND ticker=?
			""",
			(strategy, ticker),
		).fetchall()
		return [_row_to_dict(r) for r in rows]

	# -------------------------------------------------------------------------
	# Strategy state
	# -------------------------------------------------------------------------

	def save_state(self, strategy: str, state_dict: dict[str, Any]) -> None:
		"""Persist strategy state (full replace — deletes old keys first)."""
		now = datetime.now(timezone.utc).isoformat()
		self._conn.execute(
			"DELETE FROM strategy_state WHERE strategy=?", (strategy,)
		)
		for key, value in state_dict.items():
			self._conn.execute(
				"INSERT INTO strategy_state (strategy, key, value, updated_at) VALUES (?, ?, ?, ?)",
				(strategy, key, json.dumps(value), now),
			)
		self._conn.commit()

	def load_state(self, strategy: str) -> dict[str, Any]:
		"""Load strategy state; returns {} if no state exists."""
		rows = self._conn.execute(
			"SELECT key, value FROM strategy_state WHERE strategy=?",
			(strategy,),
		).fetchall()
		return {key: json.loads(value) for key, value in rows}

	def load_all_states(self) -> dict[str, dict[str, Any]]:
		"""Load all strategy states keyed by strategy name."""
		rows = self._conn.execute(
			"SELECT strategy, key, value FROM strategy_state"
		).fetchall()
		result: dict[str, dict[str, Any]] = {}
		for strategy, key, value in rows:
			if strategy not in result:
				result[strategy] = {}
			result[strategy][key] = json.loads(value)
		return result

	# -------------------------------------------------------------------------
	# Lifecycle
	# -------------------------------------------------------------------------

	def close(self) -> None:
		"""Close the database connection."""
		self._conn.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _row_to_dict(row: tuple[Any, ...]) -> dict[str, Any]:
	"""Map a SELECT row to a dict.

	Supports both the 15-column (open trades) and 18-column (full trade)
	variants. `entry_time` was added at position 15 in Task 8 (capture/replay)
	so the settlement poller can include it in the synthetic.settlement payload.
	"""
	keys = (
		"id", "ticker", "entry_price", "strategy", "side", "series_ticker",
		"entry_fee_cents", "intended_size", "fill_size", "blended_entry",
		"book_depth", "fill_pct", "slippage_cents", "status", "entry_time",
		"exit_price", "exit_time", "pnl_cents",
	)
	return dict(zip(keys, row))
