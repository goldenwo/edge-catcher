"""``SQLiteTradeStore`` — the live-only ``TradeStoreProtocol`` adapter.

Owned by sub-project B (v1.6.0 PR 5). This is the thin, stateful
connection-holding bridge between the engine's *structural*
``engine.trade_store.TradeStoreProtocol`` (what ``engine/dispatch.py`` calls)
and B's *pure-function* ``live.state`` writers over ``live_trades.db``.

``live.state`` is deliberately stateless free functions
(``record_pending(conn, *, ...)``) so the reconciler and WS handlers can call
them over whatever connection sub-project E owns. Dispatch, however, is written
against an *object* with methods. This class is the seam: it holds ONE
``sqlite3.Connection`` to ``live_trades.db`` (WAL + the ``0003`` migration
applied at construction via 4.A's ``connect_live_trades_db`` helper) and
delegates each Protocol method dispatch reaches on the LIVE path to the
matching ``live.state`` / ``engine.live_db`` function.

**Live-path surface (only what ``engine/dispatch.py`` invokes when the
executor is ``LiveExecutor`` — statuses filled / rejected / pending):**

* ``record_trade`` → :func:`live.state.record_open` (filled branch — the
  Kalshi-confirmed entry; ``dispatch.py`` ``_handle_enter`` filled arm).
* ``record_rejected`` → :func:`live.state.record_rejected` (rejected branch,
  non-``stale_book`` only; ``stale_book`` is the paper-side reject path that
  ``dispatch.py`` short-circuits before the store).
* ``record_pending`` → :func:`live.state.record_pending` (pending branch —
  NetworkError / malformed-fills / engine-timeout).
* ``get_open_trades`` / ``get_open_trades_for`` → ``live_trades`` open-row
  reads (dispatch builds ``TickContext.open_positions`` so live strategies
  can see their positions and emit exit Signals).
* ``close`` → close the held connection (idempotent).

**Paper-path methods deliberately NOT implemented** (``settle_trade``,
``exit_trade``, ``get_trade_by_id``, ``save_state``, ``load_state``,
``load_all_states``): they raise :class:`NotImplementedError` with an
explanatory message rather than silently no-op into a wrong real-money
result. Rationale — on the live path the post-placement lifecycle
(entry-fill → exit → partial-exit → settlement / close) is driven by 4.C's
**WS handlers** and 4.B's **reconciliation** calling ``live.state``'s
CAS-guarded ``transition_* / record_close / record_partial_exit`` functions
*directly against ``live_trades.db``*, NOT through this store. The paper
``exit_trade`` / ``settle_trade`` compute P&L in-store with a single
``status='open' → won/lost`` UPDATE and a ``paper_trades`` schema shape;
the live equivalent is a CAS ``won/lost/scratch`` close with entry-fee
remainder consumption keyed off the Kalshi exit fill — there is no faithful
1:1 mapping, so a silent no-op here would be a real-money correctness hole.
Strategy state likewise lives in ``live_trades.db`` (and is rehydrated by the
reconciler), not in a store-owned ``strategy_state`` table. E wires the live
engine so these are unreachable; the loud ``NotImplementedError`` is a
fail-loud guard if a future wiring change ever routes one here.

🚨 **Real-money invariant — ``RecordPendingFailed`` MUST propagate.**
``record_pending`` and ``record_trade`` (→ ``record_open``) are the two
funds-at-risk INSERTs: a failed INSERT means a Kalshi-side order is stranded
with no local row for B's reconciler to find. ``live.state`` raises
:class:`RecordPendingFailed` in exactly those two cases; this adapter does
**pure delegation with no try/except around the call**, so the exception
propagates uncaught — which is what the three
``except RecordPendingFailed: raise`` ghost-reject clauses in
``dispatch.process_tick`` / ``engine._ws_loop`` / ``engine`` outer reconnect
depend on to halt the engine. The ``record_rejected`` audit-write
best-effort carve-out (a failed rejected-row INSERT strands only an audit
row, no money) is *inherited* from ``live.state.record_rejected`` (it catches
its own ``sqlite3.Error``, logs ``rejected_audit_write_failed``, returns 0);
this adapter neither re-raises nor adds its own swallow.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from edge_catcher.live.state import (
	connect_live_trades_db,
	record_open,
	record_pending,
	record_rejected,
)

log = logging.getLogger(__name__)

# Columns selected for the paper-shaped open-trade dicts dispatch +
# strategies consume. The keys mirror engine.trade_store._row_to_dict's
# 15-column open-trade shape so a live strategy reading
# TickContext.open_positions sees the same dict keys it would under paper
# (id / ticker / entry_price / strategy / side / series_ticker /
# entry_fee_cents / intended_size / fill_size / blended_entry / fill_pct /
# slippage_cents / status / entry_time). live_trades' cent-suffixed columns
# (entry_price_cents / blended_entry_cents) are aliased to the paper names so
# strategy code stays venue/store agnostic; book_depth is absent in the live
# schema (live entries are IOC fills, not book walks) so it is reported as
# None to keep the shape stable.
_OPEN_ROW_SQL = (
	"SELECT id, ticker, entry_price_cents, strategy, side, series, "
	"entry_fee_cents, intended_size, fill_size, blended_entry_cents, "
	"fill_pct, slippage_cents, status, entry_time "
	"FROM live_trades WHERE status = 'open'"
)


def _open_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
	"""Map a live_trades open row to the paper open-trade dict shape.

	Keeps live strategy code shape-compatible with paper (see
	``engine.trade_store._row_to_dict``). ``book_depth`` is always ``None``
	(no book-walk concept for live IOC fills) — present so the key set is
	stable across stores.
	"""
	return {
		"id": row["id"],
		"ticker": row["ticker"],
		"entry_price": row["entry_price_cents"],
		"strategy": row["strategy"],
		"side": row["side"],
		"series_ticker": row["series"],
		"entry_fee_cents": row["entry_fee_cents"],
		"intended_size": row["intended_size"],
		"fill_size": row["fill_size"],
		"blended_entry": row["blended_entry_cents"],
		"book_depth": None,
		"fill_pct": row["fill_pct"],
		"slippage_cents": row["slippage_cents"],
		"status": row["status"],
		"entry_time": row["entry_time"],
	}


class SQLiteTradeStore:
	"""Live-only ``TradeStoreProtocol`` adapter backed by ``live_trades.db``.

	Structurally satisfies ``engine.trade_store.TradeStoreProtocol`` (nominal
	Protocol — no inheritance). Construction mirrors the paper ``TradeStore``
	(``__init__(db_path)``): it opens / migrates ``live_trades.db`` via 4.A's
	``connect_live_trades_db`` (WAL + ``0003``) and owns that single
	connection for its lifetime. ``check_same_thread=False`` is inherited from
	``connect_live_trades_db`` (matches paper ``TradeStore``'s choice). The
	caller (sub-project E) is responsible for calling :meth:`close` on
	shutdown.
	"""

	def __init__(self, db_path: Path) -> None:
		"""Open + migrate ``live_trades.db`` and hold the connection.

		Mirrors paper ``TradeStore.__init__(db_path)`` so E constructs the
		live store the same way it constructs the paper store, swapping only
		the class + db path. The WAL pragma + ``0003`` migration are applied
		by ``connect_live_trades_db`` (idempotent — re-running against an
		already-migrated DB is a no-op beyond the pragma).
		"""
		self._conn: sqlite3.Connection = connect_live_trades_db(db_path)
		self._closed = False

	# -------------------------------------------------------------------------
	# Live-path WRITE surface — delegate to live.state free functions
	# -------------------------------------------------------------------------

	def record_pending(
		self,
		*,
		ticker: str,
		series: str,
		strategy: str,
		side: str,
		intended_size: int,
		entry_price_cents: Optional[int],
		stop_loss_distance_cents: Optional[int],
		client_order_id: str,
		kalshi_order_id: Optional[str],
		placed_at_utc: str,
		rejection_reason: Optional[str],
	) -> None:
		"""Persist a ``pending`` row (dispatch's pending branch — D's
		NetworkError / malformed-fills / engine-timeout OrderResult).

		Pure delegation to :func:`live.state.record_pending` over the held
		connection. The locked 11-kwarg signature matches
		``TradeStoreProtocol.record_pending`` verbatim (pinned by
		``tests/test_engine_dispatch_pending_branch.py``).

		🚨 ``RecordPendingFailed`` (raised by ``live.state.record_pending`` on
		INSERT failure — a stranded funds-at-risk Kalshi order with no local
		row) propagates UNCAUGHT: there is intentionally **no** try/except
		around this call. The engine's ghost-reject clauses depend on it.
		"""
		record_pending(
			self._conn,
			ticker=ticker,
			series=series,
			strategy=strategy,
			side=side,  # type: ignore[arg-type]  # Protocol widens to str; live.state narrows to Literal["yes","no"] — value is validated upstream (OrderRequest.side cast in dispatch); the side CHECK constraint is the runtime backstop
			intended_size=intended_size,
			entry_price_cents=entry_price_cents,
			stop_loss_distance_cents=stop_loss_distance_cents,
			client_order_id=client_order_id,
			kalshi_order_id=kalshi_order_id,
			placed_at_utc=placed_at_utc,
			rejection_reason=rejection_reason,
		)

	def record_rejected(
		self,
		*,
		ticker: str,
		series: str,
		strategy: str,
		side: str,
		intended_size: int,
		entry_price_cents: Optional[int],
		stop_loss_distance_cents: Optional[int],
		client_order_id: str,
		placed_at_utc: str,
		rejection_reason: str,
	) -> None:
		"""Persist a ``rejected`` audit row (dispatch's rejected branch,
		non-``stale_book`` only).

		Pure delegation to :func:`live.state.record_rejected`. The locked
		10-kwarg signature (no ``kalshi_order_id``; ``rejection_reason``
		REQUIRED) matches ``TradeStoreProtocol.record_rejected`` verbatim
		(pinned by ``tests/test_engine_dispatch_pending_branch.py``).

		The audit-write best-effort **carve-out is inherited, not
		re-implemented**: ``live.state.record_rejected`` catches its own
		``sqlite3.Error``, logs ``rejected_audit_write_failed``, and returns 0
		(a failed rejected-row INSERT strands only an audit row — no Kalshi
		position, no money). This adapter adds no swallow and no re-raise, so
		it never raises ``RecordPendingFailed`` here (ghost-reject scope is
		funds-at-risk INSERTs only) and never masks a different error.
		"""
		record_rejected(
			self._conn,
			ticker=ticker,
			series=series,
			strategy=strategy,
			side=side,
			intended_size=intended_size,
			entry_price_cents=entry_price_cents,
			stop_loss_distance_cents=stop_loss_distance_cents,
			client_order_id=client_order_id,
			placed_at_utc=placed_at_utc,
			rejection_reason=rejection_reason,
		)

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
		"""Persist an ``open`` row for a Kalshi-confirmed entry (dispatch's
		filled branch — ``_handle_enter`` filled arm calls the paper-shaped
		``record_trade``).

		Maps the paper ``record_trade`` kwargs onto :func:`live.state.record_open`
		(B's live open-row writer) — the same mapping the ``_LiveBackedStore``
		orchestrator shim in ``tests/test_live_state_integration.py`` uses, so
		dispatch's filled branch persists a real live ``open`` row.

		Mapping notes:
		* ``series_ticker`` → ``series`` (the live schema's column name);
		* ``entry_price`` → ``entry_price_cents`` AND ``blended_entry_cents``
		  (falling back to ``entry_price`` when ``blended_entry`` is absent —
		  the live open row needs a non-NULL cost basis; mirrors paper's
		  ``effective_price``);
		* ``slippage_cents`` is int-coerced at the boundary (the live schema
		  column is ``INTEGER``; D's ``OrderResult.slippage_cents`` is already
		  an int from ``signed_slippage_cents`` — the float is the paper
		  Protocol's wider type, never a fractional live value);
		* ``client_order_id`` / ``kalshi_order_id`` / ``placed_at_utc`` /
		  ``entry_time`` / ``entry_fee_cents`` are NOT in the paper
		  ``record_trade`` signature. The live engine's filled-branch wiring
		  (E / PR 6) is responsible for threading D's real OrderResult values
		  through; until that wiring lands, ``record_trade`` is exercised only
		  via the same paper-shaped surface the orchestrator shim uses, where
		  these are synthesized from the call's identity + ``now``. This keeps
		  the adapter Protocol-faithful (the dispatch filled call site is the
		  paper shape) without inventing a divergent signature.

		🚨 ``record_open`` raises ``RecordPendingFailed`` on INSERT failure
		(Kalshi already filled the entry → a missing local row strands a live
		position). Pure delegation, no try/except — it propagates uncaught,
		same ghost-reject contract as ``record_pending``.

		``now`` must be timezone-aware (parity contract with paper
		``record_trade`` / dispatch's threaded clock); a naive datetime is a
		caller bug and is rejected loudly.
		"""
		if now.tzinfo is None:
			raise ValueError("now must be timezone-aware")
		# Live open rows need a non-NULL cost basis. Mirror paper's
		# effective_price: blended when present (and non-zero — a 0 blended is
		# a sub-cent book artefact, not a real price), else the entry price.
		effective_blended = blended_entry if blended_entry else entry_price
		now_iso = now.isoformat()
		return record_open(
			self._conn,
			ticker=ticker,
			series=series_ticker,
			strategy=strategy,
			side=side,
			intended_size=intended_size,
			fill_size=fill_size,
			entry_price_cents=entry_price,
			blended_entry_cents=effective_blended,
			# D's OrderResult.slippage_cents is an int (signed_slippage_cents);
			# the float here is only the paper Protocol's wider static type.
			slippage_cents=int(slippage_cents or 0),
			fill_pct=fill_pct if fill_pct is not None else 1.0,
			# stop_loss_distance is a Signal field not carried on the paper
			# record_trade surface; 0 = "not tracked on this row" (reporting
			# only — never feeds live P&L or the CAS preconditions).
			stop_loss_distance_cents=0,
			# Identity/fee fields absent from the paper record_trade signature
			# (see docstring). Synthesized deterministically from the call so
			# the open-row INSERT satisfies its NOT-NULL columns; E's filled-
			# branch wiring threads D's real values when PR 6 lands.
			client_order_id=f"{strategy}-{ticker}-{int(now.timestamp() * 1000)}-live",
			kalshi_order_id=f"kx-{strategy}-{int(now.timestamp())}",
			placed_at_utc=now_iso,
			entry_time=now_iso,
			entry_fee_cents=0,
		)

	# -------------------------------------------------------------------------
	# Live-path READ surface — open-position reads for TickContext
	# -------------------------------------------------------------------------

	def get_open_trades(self) -> list[dict[str, Any]]:
		"""All ``open`` rows, mapped to the paper open-trade dict shape.

		Dispatch / strategy code consume this to build
		``TickContext.open_positions``. Ordered by ``id`` ASC for determinism
		(matches ``engine.live_db.read_open_positions``' ordering contract).
		"""
		self._conn.row_factory = sqlite3.Row
		try:
			rows = self._conn.execute(
				f"{_OPEN_ROW_SQL} ORDER BY id ASC"
			).fetchall()
		finally:
			self._conn.row_factory = None
		return [_open_row_to_dict(r) for r in rows]

	def get_open_trades_for(
		self, strategy: str, ticker: str
	) -> list[dict[str, Any]]:
		"""``open`` rows filtered by ``strategy`` + ``ticker``.

		Parameter name is ``strategy`` (NOT ``strat_name``) — matches the
		Protocol + paper ``TradeStore.get_open_trades_for`` so dispatch's
		``store.get_open_trades_for(strat.name, ticker)`` call binds.
		"""
		self._conn.row_factory = sqlite3.Row
		try:
			rows = self._conn.execute(
				f"{_OPEN_ROW_SQL} AND strategy = ? AND ticker = ? "
				"ORDER BY id ASC",
				(strategy, ticker),
			).fetchall()
		finally:
			self._conn.row_factory = None
		return [_open_row_to_dict(r) for r in rows]

	# -------------------------------------------------------------------------
	# Paper-path Protocol methods — NOT reachable on the live dispatch path
	# -------------------------------------------------------------------------
	#
	# These complete the structural Protocol surface but raise loudly: the
	# live post-placement lifecycle is WS-handler / reconciliation driven via
	# live.state's CAS functions against live_trades.db directly, NOT through
	# the store. A silent no-op would be a real-money correctness hole (e.g. a
	# strategy exit Signal silently not closing a live position). E wires the
	# live engine so none of these are reached; this is the fail-loud backstop.

	def _live_only(self, method: str) -> NotImplementedError:
		return NotImplementedError(
			f"SQLiteTradeStore is live-only; {method} is a paper-path method "
			f"not reachable on the live dispatch path. The live post-placement "
			f"lifecycle (exit / settlement / partial-exit / strategy state) is "
			f"driven by live.state's CAS-guarded transition functions via the "
			f"WS handlers + reconciliation against live_trades.db directly — "
			f"see edge_catcher/live/store.py module docstring."
		)

	def settle_trade(self, trade_id: int, result: str, *, now: datetime) -> None:
		raise self._live_only("settle_trade")

	def exit_trade(self, trade_id: int, exit_price: int, *, now: datetime) -> None:
		raise self._live_only("exit_trade")

	def get_trade_by_id(self, trade_id: int) -> dict[str, Any] | None:
		raise self._live_only("get_trade_by_id")

	def save_state(self, strategy: str, state_dict: dict[str, Any]) -> None:
		raise self._live_only("save_state")

	def load_state(self, strategy: str) -> dict[str, Any]:
		raise self._live_only("load_state")

	def load_all_states(self) -> dict[str, dict[str, Any]]:
		raise self._live_only("load_all_states")

	# -------------------------------------------------------------------------
	# Lifecycle
	# -------------------------------------------------------------------------

	def close(self) -> None:
		"""Close the held connection. Idempotent — E's shutdown path (SIGTERM
		handler + a finally block) may call it more than once; a second call
		is a no-op rather than a ``ProgrammingError``."""
		if self._closed:
			return
		self._conn.close()
		self._closed = True
