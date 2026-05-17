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
executor is ``LiveExecutor`` — statuses rejected / pending):**

* ``record_rejected`` → :func:`live.state.record_rejected` (rejected branch,
  non-``stale_book`` only; ``stale_book`` is the paper-side reject path that
  ``dispatch.py`` short-circuits before the store).
* ``record_pending`` → :func:`live.state.record_pending` (pending branch —
  NetworkError / malformed-fills / engine-timeout).
* ``get_open_trades`` / ``get_open_trades_for`` → ``live_trades`` open-row
  reads (dispatch builds ``TickContext.open_positions`` so live strategies
  can see their positions and emit exit Signals).
* ``close`` → close the held connection (idempotent).

So this adapter's live responsibility is **pending/rejected persistence +
open-row reads ONLY**. The filled-entry write and the whole post-placement
lifecycle are NOT this adapter's job (see ``PR-5 → PR-6 (E) CONTRACT``).

**Paper-path methods deliberately NOT implemented** (``record_trade``,
``settle_trade``, ``exit_trade``, ``get_trade_by_id``, ``save_state``,
``load_state``, ``load_all_states``): they raise :class:`NotImplementedError`
with an explanatory message rather than silently no-op into a wrong
real-money result. Rationale — on the live path the entry-fill + the
post-placement lifecycle (entry-fill → exit → partial-exit → settlement /
close) is driven by ``live.state``'s ``record_open`` (filled entry, with D's
real ``OrderResult.order_id``/``client_order_id``) and 4.C's **WS handlers**
+ 4.B's **reconciliation** calling ``live.state``'s CAS-guarded
``transition_* / record_close / record_partial_exit`` functions *directly
against ``live_trades.db``*, NOT through this store. Specifically:

* ``record_trade`` — the paper-shaped ``TradeStoreProtocol.record_trade``
  signature structurally cannot carry D's real ``OrderResult.order_id``
  (→ ``kalshi_order_id``) or ``client_order_id``. Persisting a *synthesized*
  ``kalshi_order_id`` into a funds-at-risk ``open`` row would create a row
  4.B's reconciler / ``on_fill_event`` / phantom-pending poller can never
  reconcile (they key off the real Kalshi/client ids). It MUST go through
  :func:`live.state.record_open` with D's real values — E's wiring (PR 6).
* ``exit_trade`` / ``settle_trade`` — paper computes P&L in-store with a
  single ``status='open' → won/lost`` UPDATE on a ``paper_trades`` schema;
  the live equivalent is a CAS ``won/lost/scratch`` close with entry-fee
  remainder consumption keyed off the Kalshi exit fill — no faithful 1:1
  mapping, so a silent no-op here would be a real-money correctness hole
  (e.g. a strategy exit Signal silently not closing a live position).
* strategy state likewise lives in ``live_trades.db`` (rehydrated by the
  reconciler), not in a store-owned ``strategy_state`` table.

E wires the live engine so these are unreachable; the loud
``NotImplementedError`` is the fail-loud guard if a wiring change ever
routes one here before E's rewire lands.

----------------------------------------------------------------------------
**PR-5 → PR-6 (E) CONTRACT — read before wiring this store into a live run.**
----------------------------------------------------------------------------

``SQLiteTradeStore`` is the live **pending/rejected persistence + open-row
read** boundary ONLY. As shipped in PR 5 the merged ``engine/dispatch.py``
has **no live-vs-paper branching**: ``_handle_signal`` routes every exit
Signal to ``_handle_exit``, which unconditionally calls
``store.exit_trade(...)`` then ``store.get_trade_by_id(...)``; the filled
branch unconditionally calls the paper-shaped ``store.record_trade(...)``.
Against this adapter those hit the fail-loud ``NotImplementedError`` above.

Therefore, **before any live run, E (PR 6) MUST rewire dispatch** so that,
when the executor is ``LiveExecutor``:

(a) **filled-entry branch** → call :func:`live.state.record_open` directly
    with D's real ``OrderResult.order_id`` (renamed to ``kalshi_order_id``
    at the write boundary, spec §769) and ``client_order_id`` — NOT the
    paper-shaped ``store.record_trade``. (``LiveExecutor.place`` returns
    ``status="filled"`` synchronously for Kalshi IOC, so this is the common
    live entry path, not an edge case.)
(b) **exit Signal path** → route through D's executor → B's
    ``exit_pending`` / ``record_close`` / ``record_partial_exit`` against
    ``live_trades.db`` — NOT paper ``store.exit_trade`` /
    ``store.get_trade_by_id``.
(c) **settlement path** → B's settlement handler (CAS ``won/lost/scratch``
    close with entry-fee-remainder consumption) — NOT paper
    ``store.settle_trade``.

Until (a)/(b)/(c) land, ``record_trade`` / ``exit_trade`` /
``get_trade_by_id`` / ``settle_trade`` are **deliberately fail-loud** so
wiring this adapter into a live engine without the rewire fails immediately
and loudly rather than silently mis-persisting / not-closing a real-money
position. ``tests/test_live_store.py``'s ``strict=True`` xfail
``test_pr6_contract_live_lifecycle_methods_are_failloud_until_e_wires``
pins this: it asserts the *gap* (these four still raise), so the day E makes
them reachable/implemented the strict xfail XPASSes and **fails CI**,
forcing this contract back into review.

🚨 **Real-money invariant — ``RecordPendingFailed`` MUST propagate.**
``record_pending`` is the funds-at-risk INSERT this adapter performs on the
live path: a failed INSERT means a Kalshi-side order is stranded with no
local row for B's reconciler to find. ``live.state.record_pending`` raises
:class:`RecordPendingFailed` in that case; this adapter does **pure
delegation with no try/except around the call**, so the exception propagates
uncaught — which is what the three ``except RecordPendingFailed: raise``
ghost-reject clauses in ``dispatch.process_tick`` / ``engine._ws_loop`` /
``engine`` outer reconnect depend on to halt the engine.
(:func:`live.state.record_open` raises the SAME exception for the same
reason on the filled-entry INSERT, but that call is made by E's PR-6 wiring
directly against ``live.state`` — NOT through this adapter — so the
ghost-reject contract there is E's to preserve, not this store's.) The
``record_rejected`` audit-write best-effort carve-out (a failed
rejected-row INSERT strands only an audit row, no money) is *inherited*
from ``live.state.record_rejected`` (it catches its own ``sqlite3.Error``,
logs ``rejected_audit_write_failed``, returns 0); this adapter neither
re-raises nor adds its own swallow.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from edge_catcher.live.state import (
	connect_live_trades_db,
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
		"""Deliberately fail-loud — the live filled-entry path must NOT go
		through this paper-shaped Protocol method.

		``LiveExecutor.place`` returns ``status="filled"`` synchronously for
		Kalshi IOC fill-or-cancel, so the filled branch is the COMMON live
		entry path. The Kalshi-confirmed entry MUST be persisted via
		:func:`live.state.record_open`, which requires D's real
		``OrderResult.order_id`` (→ ``kalshi_order_id`` at the write boundary,
		spec §769) and ``client_order_id`` (D's idempotency key). The
		paper-shaped ``TradeStoreProtocol.record_trade(ticker, entry_price,
		strategy, side, series_ticker, ...)`` signature structurally CANNOT
		carry either — there is no parameter for them.

		Earlier this method synthesized a placeholder
		``client_order_id``/``kalshi_order_id`` to satisfy ``record_open``'s
		NOT-NULL columns. That is a real-money correctness hole (zero-error
		lens): a fabricated ``kalshi_order_id`` corresponds to NO real Kalshi
		order, so 4.B's reconciler / ``on_fill_event`` / phantom-pending
		poller — all of which key off ``client_order_id``/``kalshi_order_id``
		— can never reconcile that funds-at-risk ``open`` row. The synthesis
		is removed entirely; this method now fails loud exactly like the other
		paper-shaped lifecycle methods (``exit_trade`` / ``settle_trade`` /
		``save_state`` …).

		Per spec §769 / §"To E", wiring dispatch's filled branch to call
		``live.state.record_open`` directly with D's real ``OrderResult``
		values is **E's job (PR 6)**; see this module's
		``PR-5 → PR-6 (E) CONTRACT`` docstring section. Until E lands that
		rewire, reaching this method is a wiring bug and is rejected loudly
		rather than persisting an unreconcilable open row.
		"""
		base = self._live_only("record_trade")
		raise NotImplementedError(
			f"{base.args[0]} CONTRACT (spec §769 / §To-E): the live "
			f"filled-entry path must be wired by E (PR 6) to call "
			f"live.state.record_open directly with D's real "
			f"OrderResult.order_id (→ kalshi_order_id) and client_order_id; "
			f"the paper-shaped record_trade Protocol method cannot carry them "
			f"and is therefore not live-correct."
		) from None

	# -------------------------------------------------------------------------
	# Live-path READ surface — open-position reads for TickContext
	# -------------------------------------------------------------------------

	def get_open_trades(self) -> list[dict[str, Any]]:
		"""All ``open`` rows, mapped to the paper open-trade dict shape.

		Dispatch / strategy code consume this to build
		``TickContext.open_positions``. Ordered by ``id`` ASC for determinism
		(matches ``engine.live_db.read_open_positions``' ordering contract).
		"""
		# Save + restore the prior row_factory (NOT a hardcoded None): E's PR-6
		# wiring may share this connection and set a connection-level
		# row_factory; clobbering to None would silently reset it. This module
		# deliberately defends against cross-PR coupling — this closes the last
		# such gap. Behaviour-identical under every current caller (the factory
		# is None by default and no other code on this connection sets it);
		# purely forward-defensive.
		_prev_factory = self._conn.row_factory
		self._conn.row_factory = sqlite3.Row
		try:
			rows = self._conn.execute(
				f"{_OPEN_ROW_SQL} ORDER BY id ASC"
			).fetchall()
		finally:
			self._conn.row_factory = _prev_factory
		return [_open_row_to_dict(r) for r in rows]

	def get_open_trades_for(
		self, strategy: str, ticker: str
	) -> list[dict[str, Any]]:
		"""``open`` rows filtered by ``strategy`` + ``ticker``.

		Parameter name is ``strategy`` (NOT ``strat_name``) — matches the
		Protocol + paper ``TradeStore.get_open_trades_for`` so dispatch's
		``store.get_open_trades_for(strat.name, ticker)`` call binds.
		"""
		# Save + restore the prior row_factory (NOT a hardcoded None): same
		# cross-PR-coupling defense as get_open_trades — E's PR-6 wiring may
		# share this connection with its own connection-level row_factory.
		# Behaviour-identical under every current caller; forward-defensive.
		_prev_factory = self._conn.row_factory
		self._conn.row_factory = sqlite3.Row
		try:
			rows = self._conn.execute(
				f"{_OPEN_ROW_SQL} AND strategy = ? AND ticker = ? "
				"ORDER BY id ASC",
				(strategy, ticker),
			).fetchall()
		finally:
			self._conn.row_factory = _prev_factory
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
