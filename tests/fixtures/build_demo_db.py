"""Deterministic builder for the paper_trades demo fixture DB.

Emits ``edge_catcher/data/examples/paper_trades_demo.db`` with a small,
hand-verifiable set of 20 paper_trades rows. Intended as the input fixture
for the reporting module's tests (Task 8 in the public-release v1 plan).

The schema matches ``paper_trades`` as defined by ``TradeStore`` in
``edge_catcher/monitors/trade_store.py``. No ``strategy_state`` rows are
written — the reporting module only reads trade rows.

Mix:
  * 20 rows total, split across 2 series (``DEMO_A15M`` / ``DEMO_B15M``)
    and 1 strategy (``longshot_fade_example``).
  * 12 wins / 8 losses → 60% win rate.
  * Entry prices 2–7c; winning exits 8–12c; losing exits 0c (settlement
    NO-side).
  * entry_time → exit_time is within 15 minutes per row.

Totals are deterministic so downstream reporting tests can assert exact
numbers. See the module footer for the computed totals.

Run from the repo root:

    python tests/fixtures/build_demo_db.py
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from edge_catcher.adapters.kalshi.fees import STANDARD_FEE


# Anchor time — fixed so the fixture is reproducible across runs/machines.
_BASE = datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)


# Each tuple: (series, ticker_suffix, entry_price_c, exit_price_c, minute_offset, won)
# exit_price=100 → win (settled YES on the NO side pays 100 - no, but these are
# modeled as a "NO"-side fade so entry_price is the NO-leg price; exit_price is
# the settlement value on that leg: 100 if the market resolved the fade-side).
#
# We keep the shape simple — all trades are side='no' (longshot fades), and
# pnl = fill_size * (exit_price - entry_price) - entry_fee - optional exit_fee.
# For settled trades (won/lost), no exit fee is charged (matches TradeStore.settle_trade).
# For TP exits (still side='no'), an exit fee IS charged.
_ROWS: list[tuple[str, str, int, int, int, str]] = [
	# --- DEMO_A15M wins (6) — settlement wins ---
	("DEMO_A15M", "A-01", 3, 100, 5,   "won"),
	("DEMO_A15M", "A-02", 4, 100, 7,   "won"),
	("DEMO_A15M", "A-03", 5, 100, 9,   "won"),
	("DEMO_A15M", "A-04", 2, 100, 4,   "won"),
	("DEMO_A15M", "A-05", 6, 100, 11,  "won"),
	("DEMO_A15M", "A-06", 3, 100, 6,   "won"),
	# --- DEMO_A15M wins (2) — TP exits at 10-12c ---
	("DEMO_A15M", "A-07", 4, 12,  8,   "won_tp"),
	("DEMO_A15M", "A-08", 5, 10,  10,  "won_tp"),
	# --- DEMO_A15M losses (2) — settlement losses ---
	("DEMO_A15M", "A-09", 7, 0,   13,  "lost"),
	("DEMO_A15M", "A-10", 6, 0,   14,  "lost"),

	# --- DEMO_B15M wins (3) — settlement wins ---
	("DEMO_B15M", "B-01", 3, 100, 3,   "won"),
	("DEMO_B15M", "B-02", 4, 100, 6,   "won"),
	("DEMO_B15M", "B-03", 2, 100, 5,   "won"),
	# --- DEMO_B15M wins (1) — TP exit ---
	("DEMO_B15M", "B-04", 5, 11,  9,   "won_tp"),
	# --- DEMO_B15M losses (6) — settlement losses ---
	("DEMO_B15M", "B-05", 5, 0,   11,  "lost"),
	("DEMO_B15M", "B-06", 6, 0,   12,  "lost"),
	("DEMO_B15M", "B-07", 7, 0,   14,  "lost"),
	("DEMO_B15M", "B-08", 4, 0,   7,   "lost"),
	("DEMO_B15M", "B-09", 3, 0,   8,   "lost"),
	("DEMO_B15M", "B-10", 5, 0,   10,  "lost"),
]


def _row_values(
	series: str, ticker_suffix: str, entry_price: int, exit_price: int,
	minute_offset: int, outcome: str,
) -> dict:
	"""Compute the full paper_trades row for one test case."""
	entry_time = _BASE + timedelta(days=minute_offset % 3, minutes=minute_offset)
	exit_time = entry_time + timedelta(minutes=12)  # within 15 minutes
	# One row uses fill_size > 1 so SUM(entry_price * fill_size) is arithmetically
	# distinct from SUM(entry_price). This keeps the deployed-math regression test
	# (test_deployed_uses_entry_price_times_fill_size_not_just_entry_price) honest
	# instead of merely guarding the SQL shape.
	fill_size = 4 if ticker_suffix == "A-01" else 1

	# Entry fee is the Kalshi standard taker fee on the entry leg. The live
	# engine always charges this — see TradeStore.record_trade.
	entry_fee_cents = int(STANDARD_FEE.calculate(entry_price, fill_size))

	# P&L reconstruction — mirrors TradeStore.settle_trade / exit_trade exactly.
	# settle_trade path: pnl = fill_size*(exit - entry) - entry_fee   (no exit fee)
	# exit_trade path:   pnl = fill_size*(exit - entry) - entry_fee - exit_fee
	if outcome in ("won", "lost"):
		# Settlement — no exit fee.
		pnl_cents = fill_size * (exit_price - entry_price) - entry_fee_cents
		status = "won" if outcome == "won" else "lost"
	else:  # won_tp — TP exit, exit fee applies.
		exit_fee_cents = int(STANDARD_FEE.calculate(exit_price, fill_size))
		pnl_cents = fill_size * (exit_price - entry_price) - entry_fee_cents - exit_fee_cents
		status = "won" if pnl_cents > 0 else ("lost" if pnl_cents < 0 else "scratch")

	return {
		"ticker": f"{series}-{ticker_suffix}",
		"entry_price": entry_price,
		"entry_time": entry_time.isoformat(),
		"exit_price": exit_price,
		"exit_time": exit_time.isoformat(),
		"pnl_cents": pnl_cents,
		"status": status,
		"strategy": "longshot_fade_example",
		"side": "no",
		"series_ticker": series,
		"entry_fee_cents": entry_fee_cents,
		"intended_size": fill_size,
		"fill_size": fill_size,
		"blended_entry": None,
		"book_depth": None,
		"fill_pct": 1.0,
		"slippage_cents": 0.0,
		"book_snapshot": None,
	}


def build(db_path: Path) -> int:
	"""Build the fixture DB at ``db_path``. Returns the sum of pnl_cents."""
	db_path = Path(db_path)
	db_path.parent.mkdir(parents=True, exist_ok=True)
	# Clear prior fixture (incl. WAL/SHM sidecars) for deterministic rebuild.
	for suffix in ("", "-wal", "-shm"):
		target = db_path.with_name(db_path.name + suffix)
		if target.exists():
			target.unlink()

	# Schema mirrors TradeStore._SCHEMA exactly. Kept inline to avoid invoking
	# TradeStore, which would also auto-open a WAL connection and muddy the
	# "same bytes in, same bytes out" contract.
	schema = """
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

	conn = sqlite3.connect(str(db_path))
	try:
		conn.executescript(schema)
		total_pnl = 0
		for row_spec in _ROWS:
			row = _row_values(*row_spec)
			conn.execute(
				"""
				INSERT INTO paper_trades (
					ticker, entry_price, entry_time, exit_price, exit_time,
					pnl_cents, status, strategy, side, series_ticker,
					entry_fee_cents, intended_size, fill_size, blended_entry,
					book_depth, fill_pct, slippage_cents, book_snapshot
				) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
				""",
				(
					row["ticker"], row["entry_price"], row["entry_time"],
					row["exit_price"], row["exit_time"], row["pnl_cents"],
					row["status"], row["strategy"], row["side"], row["series_ticker"],
					row["entry_fee_cents"], row["intended_size"], row["fill_size"],
					row["blended_entry"], row["book_depth"], row["fill_pct"],
					row["slippage_cents"], row["book_snapshot"],
				),
			)
			total_pnl += row["pnl_cents"]
		conn.commit()
	finally:
		conn.close()

	print(
		f"Wrote {db_path} with {len(_ROWS)} paper_trades rows, "
		f"total pnl_cents={total_pnl}."
	)
	return total_pnl


if __name__ == "__main__":
	repo_root = Path(__file__).resolve().parents[2]
	target = repo_root / "edge_catcher" / "data" / "examples" / "paper_trades_demo.db"
	build(target)
