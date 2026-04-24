"""Deterministic builder for the demo markets+trades fixture DB.

Emits ``edge_catcher/data/examples/demo_markets.db`` with a tiny, hand-picked
set of markets + trades on the synthetic series ``DEMO_SERIES``. The shape
matches the schema defined in ``edge_catcher/storage/db.py`` (see
``_SCHEMA_SQL``) so that the ``edge-catcher backtest`` CLI can read this DB
out of the box, both for --list-strategies smoke-tests and for running the
``longshot_fade_example`` example strategy.

Design goals:
  * Fully deterministic: re-running the script always produces byte-identical
    output (modulo SQLite's internal WAL state). The target DB is deleted
    before writing so the builder is idempotent.
  * Small: 2 markets, ~16 trades. The fixture exists to demonstrate the
    pipeline, not to exercise edge cases.
  * Exercises the longshot path: some trades are priced at <=5c so the
    example ``LongshotFadeExample`` strategy will fire.

Run from the repo root:

    python tests/fixtures/build_demo_markets_db.py
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from edge_catcher.storage.db import get_db, init_db, upsert_market, upsert_trades_batch
from edge_catcher.storage.models import Market, Trade


# Anchor time — fixed so the fixture is reproducible across runs/machines.
_BASE = datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)


def _market(
	ticker: str,
	*,
	open_offset_hours: float,
	close_offset_hours: float,
	result: str,
) -> Market:
	"""Build a settled Market row anchored on ``_BASE``."""
	open_time = _BASE + timedelta(hours=open_offset_hours)
	close_time = _BASE + timedelta(hours=close_offset_hours)
	return Market(
		ticker=ticker,
		event_ticker=f"{ticker}-EVT",
		series_ticker="DEMO_SERIES",
		title=f"Demo market {ticker}",
		status="settled",
		result=result,
		yes_bid=None,
		yes_ask=None,
		last_price=None,
		open_interest=100,
		volume=100,
		expiration_time=close_time,
		close_time=close_time,
		created_time=open_time,
		settled_time=close_time,
		open_time=open_time,
		notional_value=100.0,
		floor_strike=None,
		cap_strike=None,
		raw_data=None,
	)


def _trade(
	trade_id: str,
	ticker: str,
	yes_price: int,
	*,
	time_offset_hours: float,
	count: int = 1,
	taker_side: str = "yes",
) -> Trade:
	"""Build a Trade row. no_price is derived as 100 - yes_price."""
	return Trade(
		trade_id=trade_id,
		ticker=ticker,
		yes_price=yes_price,
		no_price=100 - yes_price,
		count=count,
		taker_side=taker_side,
		created_time=_BASE + timedelta(hours=time_offset_hours),
		raw_data=None,
	)


def build(db_path: Path) -> None:
	"""Build the fixture DB at ``db_path``. Re-runnable: deletes any pre-existing file."""
	db_path = Path(db_path)
	# Clear prior fixture (incl. WAL/SHM sidecars) for a clean, deterministic rebuild.
	for suffix in ("", "-wal", "-shm"):
		target = db_path.with_name(db_path.name + suffix)
		if target.exists():
			target.unlink()

	init_db(db_path)

	markets = [
		# Market A settles YES — longshot NO fades will lose here.
		_market("DEMO_SERIES-26APR01-A",
			open_offset_hours=0.0, close_offset_hours=6.0, result="yes"),
		# Market B settles NO — longshot NO fades will win here.
		_market("DEMO_SERIES-26APR02-A",
			open_offset_hours=24.0, close_offset_hours=30.0, result="no"),
	]

	# ~8 trades per market. Each market has a mix of longshot-range (<=5c)
	# and mid-range trades so LongshotFadeExample has both entry signals
	# and price movement to react to.
	trades: list[Trade] = [
		# Market A — settles YES. Longshot entries at 3c, 5c will LOSE.
		_trade("T-A-01", "DEMO_SERIES-26APR01-A", yes_price=50, time_offset_hours=0.1),
		_trade("T-A-02", "DEMO_SERIES-26APR01-A", yes_price=20, time_offset_hours=0.5),
		_trade("T-A-03", "DEMO_SERIES-26APR01-A", yes_price=10, time_offset_hours=1.0),
		_trade("T-A-04", "DEMO_SERIES-26APR01-A", yes_price=5,  time_offset_hours=1.5),  # entry
		_trade("T-A-05", "DEMO_SERIES-26APR01-A", yes_price=3,  time_offset_hours=2.0),  # deeper longshot
		_trade("T-A-06", "DEMO_SERIES-26APR01-A", yes_price=15, time_offset_hours=3.0),
		_trade("T-A-07", "DEMO_SERIES-26APR01-A", yes_price=40, time_offset_hours=4.0),
		_trade("T-A-08", "DEMO_SERIES-26APR01-A", yes_price=80, time_offset_hours=5.5),  # YES wave before settle

		# Market B — settles NO. Longshot entries here WIN.
		_trade("T-B-01", "DEMO_SERIES-26APR02-A", yes_price=55, time_offset_hours=24.1),
		_trade("T-B-02", "DEMO_SERIES-26APR02-A", yes_price=30, time_offset_hours=24.5),
		_trade("T-B-03", "DEMO_SERIES-26APR02-A", yes_price=12, time_offset_hours=25.0),
		_trade("T-B-04", "DEMO_SERIES-26APR02-A", yes_price=4,  time_offset_hours=25.5),  # entry
		_trade("T-B-05", "DEMO_SERIES-26APR02-A", yes_price=8,  time_offset_hours=26.0),
		_trade("T-B-06", "DEMO_SERIES-26APR02-A", yes_price=11, time_offset_hours=26.5),  # triggers TP at 89c NO
		_trade("T-B-07", "DEMO_SERIES-26APR02-A", yes_price=6,  time_offset_hours=27.0),
		_trade("T-B-08", "DEMO_SERIES-26APR02-A", yes_price=2,  time_offset_hours=29.0),
	]

	with get_db(db_path) as conn:
		for m in markets:
			upsert_market(conn, m)
		upsert_trades_batch(conn, trades)

	print(f"Wrote {db_path} with {len(markets)} markets, {len(trades)} trades.")


if __name__ == "__main__":
	repo_root = Path(__file__).resolve().parents[2]
	target = repo_root / "edge_catcher" / "data" / "examples" / "demo_markets.db"
	build(target)
