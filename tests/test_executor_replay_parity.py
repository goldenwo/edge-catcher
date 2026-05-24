"""CR-5 — paper-vs-live EXECUTOR-translation parity harness (spec §9 / §10.8).

What this test IS
-----------------
The CR-5 question is narrow: *given the same market and the same sized
intent, does* :class:`LiveExecutor` *translate Kalshi's fills into the same
trade-row economics that* :class:`PaperExecutor`'s *book-walk produces?* This
module answers exactly that, by replaying the tracked synthetic bundle twice
through the SAME ``replay_capture`` → ``dispatch_message`` path — once with
``PaperExecutor`` (the book-walk), once with ``LiveExecutor`` against an
in-process :class:`MockKalshiServer` whose per-level fills are *derived from
the very book-walk PaperExecutor performed* — and diffing the resulting trade
rows on the existing parity column whitelist.

The four things the spec (SC-I2, spec §10 #8) requires this docstring to state
VERBATIM-in-intent:

  (i)   **Dispatch sizing is now wired (sizing-wire PR, spec §8.2 / SC-7).**
        ``_handle_enter`` calls ``build_entry_order(signal, allowed_size,
        exec_cfg, now)`` on the live path, producing a correctly-sized
        ``OrderRequest`` whose ``size_contracts`` equals the gate's
        ``Allow.size_contracts``.  The harness drives the REAL
        ``_handle_signal → _handle_enter → build_entry_order → LiveExecutor``
        path end-to-end — no shim substitutes for any step.

  (ii)  **The harness injects a book-derived mock gate so EXECUTOR parity
        is testable in CI.** A lightweight ``_BookSizeGate`` (test-only)
        returns ``Allow(size_contracts=N)`` for each entry signal in the SAME
        FIFO order the paper run's ``_RecordingPaperExecutor`` resolved sizes,
        where ``N`` is the contract count ``resolve_fill`` computed from the
        captured bundle's orderbook.  This is NOT a fiction — the gate returns
        the exact known book-derived size so the live path places an order with
        the identical size PaperExecutor would have, and the ``MockKalshiServer``
        returns the identical per-level fills, making the comparison isolate
        executor-translation (the real CR-5 question).  The previously-existing
        ``_BookSizedLiveExecutor`` shim (which patched ``size_contracts`` at the
        executor layer, bypassing ``build_entry_order``) is RETIRED as of the
        sizing-wire PR — spec §8.2 mandates it must not survive as a second,
        divergent account of "a live run."

  (iii) **CI runs harness-correctness on the tracked synthetic fixture.** The
        ``tests/fixtures/synthetic_bundle/2026-04-15`` fixture yields exactly
        ONE open, entry-only trade (no exit / settle / split). So the
        end-to-end fixture run asserts ENTRY parity only; the §10.3
        live-split-row → logical-trade collapse helper is proven non-vacuous
        by a SEPARATE direct unit test with a hand-constructed parent+children
        split-row input (the fixture cannot exercise it end-to-end).

  (iv)  **The AUTHORITATIVE real-money parity verdict is the spec's own
        ≥5-real-bundle Pi/local runbook gate**, run at Pi cutover (real bundles
        are gitignored — private-data scope). This harness — now full-fidelity
        since sizing is wired — proves the machinery is correct and ready; the
        runbook gate proves parity on real captured production data.

Deterministic ``client_order_id`` join key
------------------------------------------
``client_order_id`` is the join key between the paper run, the live run, and
the mock-response queue. ``_make_client_order_id`` is
``{strategy}-{ticker}-{ms_ts}-{uuid4_hex8}``: the ``ms_ts`` is deterministic
(it comes from the bundle's threaded ``recv_ts`` clock — identical across
paper↔live replay of the same bundle), but the ``uuid4`` suffix is
NON-deterministic by design (collision-safety, v1.6.0 round-4 review). The
function's OWN docstring (``execution.py:113-114``) mandates the standard
remedy: *"Tests that need deterministic IDs must mock ``uuid.uuid4``."* This
harness applies exactly that sanctioned, test-only monkeypatch (zero
``edge_catcher/`` change) for the duration of each replay so the FULL
``client_order_id`` is byte-identical across the paper run, the live run, and
the queued mock responses — making it the reliable join key the SC-I2 ruling
relies on.

Diff reuse + tolerance
----------------------
The comparison machinery is REUSED from ``tests/test_replay_parity.py``
(``PARITY_COLUMNS`` / ``_composite_key`` / ``_diff_rows`` are imported, never
edited). The CR-5 tolerance is applied on top: the money trio ``exit_price`` /
``blended_entry`` / ``fill_size`` must match EXACTLY (ENFORCED, never
excluded); ``slippage_cents`` may differ by ≤ 1¢ (ENFORCED within that band).

Two paper-only book-introspection columns are excluded from the comparison —
``book_depth`` AND ``book_snapshot`` — on the INDEPENDENT structural ground
that ``LiveExecutor`` cannot populate them (``_translate_order`` never sets
them ⇒ ``OrderResult`` defaults both ``None`` for live; Kalshi REST returns
only the fills array, no book depth/snapshot — orderbook-walk artifacts). To
be PRECISE about the reused whitelist: ``test_replay_parity.py``'s
``PARITY_COLUMNS`` itself excludes ONLY ``book_snapshot``; it INCLUDES /
ENFORCES ``book_depth`` (PARITY_COLUMNS line 55). CR-5 therefore excludes
``book_depth`` on the SAME structural-impossibility reason as
``book_snapshot`` — NOT on any ``test_replay_parity.py`` precedent (that
precedent exists for ``book_snapshot`` but NOT for ``book_depth``). See the
``_PAPER_ONLY_BOOK_COLUMNS`` block comment for the full rationale.
"""
from __future__ import annotations

import contextlib
import uuid as _uuid_mod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

import pytest

from edge_catcher.engine.execution import ExecCfg, validate_exec_cfg
from edge_catcher.engine.executor import OrderRequest, OrderResult
from edge_catcher.engine.executors.paper import (
	FillSkip,
	PaperExecutor,
	resolve_fill,
)
from edge_catcher.engine.executors.live import LiveExecutor
from edge_catcher.engine.market_state import MarketState, OrderbookSnapshot
from edge_catcher.engine.replay.backtester import replay_capture
from edge_catcher.engine.risk import Allow, RiskContext, SizingBreakdown

# REUSE the parity machinery — import, never edit (spec SC-I2 step 4).
from tests.test_replay_parity import (
	_composite_key,
	_diff_rows,
)
from tests.fixtures.mock_kalshi_server import MockKalshiServer

# The tracked synthetic fixture (NOT the gitignored replay_parity/* bundles).
_SYNTHETIC_BUNDLE = (
	Path(__file__).parent / "fixtures" / "synthetic_bundle" / "2026-04-15"
)


# ---------------------------------------------------------------------------
# CR-5 tolerance — applied on top of the reused _diff_rows whitelist diff.
#
# exit_price / blended_entry / fill_size : EXACT.
# slippage_cents                         : within ±1¢ (round-trip drift band).
#
# We post-filter _diff_rows' output rather than re-implementing the diff so
# the column whitelist + composite key stay single-sourced from
# test_replay_parity.py (the import contract in spec SC-I2 step 4).
#
# Paper-only book-introspection columns (book_depth / book_snapshot) are
# EXCLUDED from the CR-5 economic-parity comparison. Both are artifacts of
# PaperExecutor's orderbook WALK (it reads MarketState's captured book and
# records its depth + the consumed levels). LiveExecutor structurally CANNOT
# produce either: ``LiveExecutor._translate_order`` never sets them, so
# ``OrderResult`` defaults both to ``None`` for every live order, and Kalshi's
# REST fill response carries only the per-fill array — no orderbook depth, no
# book snapshot (executor.py: the ``OrderResult`` docstring documents these as
# paper-side fields D omits). The exclusion rests SOLELY on that independent
# structural-impossibility ground (orderbook-walk artifacts; Kalshi REST has
# no book depth/snapshot → ``None`` for live BY DESIGN) — NOT on a
# ``test_replay_parity.py`` precedent. PRECISELY: ``test_replay_parity.py``'s
# ``PARITY_COLUMNS`` excludes ONLY ``book_snapshot`` ("not material for P&L
# parity"); it INCLUDES / ENFORCES ``book_depth`` (PARITY_COLUMNS line 55).
# CR-5 excludes ``book_depth`` too, but on the SAME structural reason as
# ``book_snapshot`` (LiveExecutor cannot populate it), distinct from "the
# existing parity test already excludes it" — which is true of book_snapshot
# but NOT of book_depth. The ≥5-real-bundle runbook gate (the authoritative
# verdict) tolerates both for that same structural reason. The CR-5 money trio
# ``exit_price`` / ``blended_entry`` / ``fill_size`` (EXACT) and
# ``slippage_cents`` (±1¢) remain ENFORCED and are NOT excluded. CR-5 asks an
# ECONOMIC question (does live translate Kalshi's fills into the same size /
# cost basis / slippage / pnl?), NOT whether live can reconstruct paper's
# book-walk introspection.
# ---------------------------------------------------------------------------

_SLIPPAGE_TOLERANCE_CENTS = 1

# Paper-book-walk artifacts LiveExecutor cannot produce (see block comment).
_PAPER_ONLY_BOOK_COLUMNS = ("book_depth", "book_snapshot")


def _cr5_material_diffs(
	paper_row: dict[str, Any], live_row: dict[str, Any]
) -> dict[str, tuple[Any, Any]]:
	"""Reused ``_diff_rows``, then apply the CR-5 tolerance band.

	Two adjustments on top of the single-sourced ``_diff_rows`` whitelist diff:

	  * ``slippage_cents`` widened to ±1¢ (CR-5 names it the only tolerated-
	    band economic column; ``_diff_rows`` only approx-matches it to 1e-9).
	  * ``book_depth`` / ``book_snapshot`` dropped — paper-only book-walk
	    artifacts LiveExecutor structurally cannot emit (block comment above).

	Every other column — incl. the CR-5 EXACT trio ``exit_price`` /
	``blended_entry`` / ``fill_size`` — keeps ``_diff_rows``' exact semantics.
	"""
	diffs = _diff_rows(paper_row, live_row)
	slip = diffs.get("slippage_cents")
	if slip is not None:
		pv, lv = slip
		if (
			pv is not None
			and lv is not None
			and abs(float(pv) - float(lv)) <= _SLIPPAGE_TOLERANCE_CENTS
		):
			diffs.pop("slippage_cents")
	for col in _PAPER_ONLY_BOOK_COLUMNS:
		diffs.pop(col, None)
	return diffs


# ---------------------------------------------------------------------------
# §10.3 live-split-row → logical-trade collapse
#
# A LIVE partial exit (``live.state.record_partial_exit``) produces TWO rows
# from one logical trade:
#   * the PARENT residual — status stays 'open', fill_size/intended_size
#     decremented by the M closed contracts, cost basis + identity retained;
#   * one closed CHILD per partial — status won/lost/scratch, fill_size = M,
#     inheriting the parent's cost basis + identity, carrying an allocated
#     share of the parent's entry fee.
# PAPER closes a position as ONE row. To diff live vs paper we must first
# collapse each (parent residual + its children) family back into the single
# logical-trade view paper produces.
# ---------------------------------------------------------------------------


def _collapse_live_split_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
	"""Collapse live split-rows into one logical-trade row per trade (§10.3).

	Grouping key is the entry identity ``(strategy, ticker, side,
	entry_time)`` — the SAME tuple ``_composite_key`` diffs on, and the tuple
	``record_partial_exit`` preserves verbatim onto every child (it inherits
	the parent's ``entry_time`` / identity). For each family:

	  * ``fill_size`` / ``intended_size`` = sum across parent residual +
	    children (reconstructs the original pre-split position size paper
	    booked as one row);
	  * ``entry_fee_cents`` = sum (children carry allocated shares; the
	    parent residual carries its remaining share — together they re-sum to
	    the original total);
	  * ``pnl_cents`` = sum of the children's realized pnl (an unclosed parent
	    residual contributes ``None`` → treated as 0 for the sum; a fully
	    closed family has no residual);
	  * ``status`` / ``exit_price`` / ``exit_time`` taken from the LAST closed
	    child (the final partial that flattened the position) when the family
	    is fully closed; if a residual is still 'open' the logical row stays
	    'open' (mirrors paper, which only writes the closed row once flat);
	  * every other whitelist column (cost basis ``blended_entry`` /
	    ``entry_price``, identity, ``book_*``) is invariant across the family
	    — taken from the parent.

	A family with no split (a single row, no children) passes through
	unchanged. Input order is preserved for stable downstream diffing.

	This is a PURE function over row dicts — no DB, no I/O — so it is unit
	tested directly (``test_collapse_live_split_rows_*``) independent of any
	bundle.
	"""
	# Identify split families: a row is a "child" iff another row shares its
	# composite identity. Single rows (the common case, incl. the synthetic
	# fixture) are emitted untouched.
	by_key: dict[tuple, list[dict[str, Any]]] = {}
	order: list[tuple] = []
	for row in rows:
		k = _composite_key(row)
		if k not in by_key:
			by_key[k] = []
			order.append(k)
		by_key[k].append(row)

	collapsed: list[dict[str, Any]] = []
	for k in order:
		family = by_key[k]
		if len(family) == 1:
			collapsed.append(dict(family[0]))
			continue

		# Parent residual = the still-'open' row (record_partial_exit keeps
		# the parent 'open' until the final close). Children = the closed
		# rows. A fully-flattened family has no 'open' residual.
		residual = next((r for r in family if r.get("status") == "open"), None)
		children = [r for r in family if r.get("status") != "open"]
		basis_row = residual if residual is not None else family[0]

		logical = dict(basis_row)

		def _num(r: dict[str, Any], col: str) -> float:
			v = r.get(col)
			return 0.0 if v is None else float(v)

		total_fill = sum(_num(r, "fill_size") for r in family)
		total_intended = sum(_num(r, "intended_size") for r in family)
		total_entry_fee = sum(_num(r, "entry_fee_cents") for r in family)
		total_pnl = sum(_num(r, "pnl_cents") for r in children)

		logical["fill_size"] = int(total_fill)
		logical["intended_size"] = int(total_intended)
		logical["entry_fee_cents"] = int(total_entry_fee)

		if residual is None and children:
			# Family fully closed → present as one closed logical row, taking
			# the terminal fields from the final child that flattened it.
			last_child = children[-1]
			logical["status"] = last_child.get("status")
			logical["exit_price"] = last_child.get("exit_price")
			logical["exit_time"] = last_child.get("exit_time")
			logical["pnl_cents"] = int(total_pnl)
		else:
			# Residual still open → logical row stays open (paper writes the
			# closed row only once flat). pnl not yet realized.
			logical["status"] = "open"
			logical["exit_price"] = None
			logical["exit_time"] = None
			logical["pnl_cents"] = None

		collapsed.append(logical)

	return collapsed


# ---------------------------------------------------------------------------
# Book-derived sizing: the SC-F2-deferral sanctioned stand-in.
#
# PaperExecutor's resolve_fill book-walk is the single source of truth for the
# size + per-level fills. We re-run resolve_fill against the SAME MarketState
# book the executor walks (it is read-only — the synthetic bundle has no book
# mutation after the snapshot), keyed by the deterministic client_order_id, so
# the live run can be fed the IDENTICAL sized intent + fills.
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def _deterministic_client_order_ids():
	"""Patch ``execution.uuid.uuid4`` to a deterministic sequence.

	The sanctioned remedy from ``_make_client_order_id``'s own docstring
	(``execution.py:113-114``: "Tests that need deterministic IDs must mock
	``uuid.uuid4``"). Test-only — no ``edge_catcher/`` change. Both the paper
	and the live replay drive the SAME bundle through the SAME dispatch order
	with the SAME threaded clock, so an identical mocked uuid sequence makes
	every ``client_order_id`` byte-identical between the two runs (the
	deterministic ``ms_ts`` already matches; this pins the suffix too).

	Each ``uuid4()`` call returns a fresh deterministic value (so distinct
	orders WITHIN one run still get distinct ids — collision-safety is
	preserved in spirit), and the sequence RESETS per context entry so run N
	and run N+1 of the same bundle produce the identical id stream.
	"""
	import edge_catcher.engine.execution as _execmod

	counter = {"n": 0}

	def _fake_uuid4() -> _uuid_mod.UUID:
		counter["n"] += 1
		# Deterministic, well-formed UUID seeded by the call ordinal. .hex[:8]
		# (what _make_client_order_id consumes) is therefore stable per ordinal.
		return _uuid_mod.UUID(int=counter["n"], version=4)

	orig = _execmod.uuid.uuid4
	_execmod.uuid.uuid4 = _fake_uuid4
	try:
		yield
	finally:
		_execmod.uuid.uuid4 = orig


class _RecordingPaperExecutor:
	"""Wraps a real :class:`PaperExecutor`; records, per ``client_order_id``,
	the resolved size + per-level fills its ``resolve_fill`` book-walk produced.

	Delegates ``place`` to the real executor verbatim — the paper trade row is
	genuinely PaperExecutor's own output (zero behaviour change). The recorder
	re-runs ``resolve_fill`` against the SAME MarketState + config the wrapped
	executor uses (read-only; deterministic), so the recorded ``(size, fills)``
	is byte-identical to what the wrapped ``place`` walked. Keyed by
	``req.client_order_id`` — ``_make_client_order_id`` is deterministic given
	the bundle's threaded clock, so the key is stable across the paper run and
	the live run and is the reliable join key.
	"""

	def __init__(self, market_state: MarketState, config: dict) -> None:
		self._inner = PaperExecutor(market_state=market_state, config=config)
		self._ms = market_state
		self._config = config
		# client_order_id -> {"size": int, "fills": [{"price": c, "size": n}]}
		self.book_walk: dict[str, dict[str, Any]] = {}
		# Order in which entries were placed (deterministic == bundle order) —
		# the FIFO order the MockKalshiServer queue must mirror.
		self.entry_order: list[str] = []

	async def place(self, req: OrderRequest) -> OrderResult:
		# Mirror PaperExecutor.place's entry book-walk to recover the exact
		# size + per-level fills. We re-call resolve_fill (the SAME pure
		# function the inner executor calls) on the SAME snapshot.
		_rec: dict[str, Any] | None = None
		if req.action == "buy":
			snapshot = self._ms.get_orderbook(req.ticker) or OrderbookSnapshot([], [])
			fill_or_skip = resolve_fill(
				self._config, req.limit_price_cents, req.side, snapshot
			)
			if not isinstance(fill_or_skip, FillSkip) and fill_or_skip.fill_size > 0:
				fills = _reconstruct_fills(
					snapshot,
					req.side,
					fill_or_skip.fill_size,
					fill_or_skip.blended_price_cents,
				)
				_rec = {
					"size": fill_or_skip.fill_size,
					"fills": fills,
					"blended": fill_or_skip.blended_price_cents,
					# Recorded so the live run's queued response carries the REAL
					# bundle ticker (real bundles span many tickers; the synthetic
					# fixture happens to be SYN-TEST-T1). Keeps the entry response
					# internally consistent with the request LiveExecutor places.
					"ticker": req.ticker,
					# Entry intent the executor-level CR-5 path rebuilds the live
					# OrderRequest from (same intent in → isolate executor xlation).
					"series": req.series,
					"side": req.side,
					"strategy": req.strategy,
					"limit": req.limit_price_cents,
				}
		# Delegate verbatim — the recorded paper row is the REAL executor's.
		result = await self._inner.place(req)
		if _rec is not None:
			# Capture PaperExecutor's authoritative OrderResult so the
			# executor-level CR-5 path can diff live vs paper translation directly.
			_rec["paper_result"] = result
			self.book_walk[req.client_order_id] = _rec
			self.entry_order.append(req.client_order_id)
		return result


def _reconstruct_fills(
	snapshot: OrderbookSnapshot,
	side: str,
	fill_size: int,
	blended_cents: int,
) -> list[dict[str, int]]:
	"""Reproduce the per-level fills ``walk_book_with_ceiling`` consumed.

	Walks the same side's levels FIFO taking ``min(level_qty, remaining)``
	until ``fill_size`` is satisfied — the exact consumption order
	``walk_book_with_ceiling`` uses. Prices are in cents (``round(p*100)``),
	matching the FillEvent shape Kalshi returns and ``blended_price_cents``
	consumes. Asserts the reconstructed fills re-blend to the SAME blended
	price the book-walk produced (defence: a reconstruction that diverged
	would silently feed the live run a different market).
	"""
	levels = snapshot.yes_levels if side == "yes" else snapshot.no_levels
	fills: list[dict[str, int]] = []
	remaining = fill_size
	for price_dollars, qty in levels:
		if remaining <= 0:
			break
		take = min(qty, remaining)
		if take <= 0:
			continue
		fills.append({"price": round(price_dollars * 100), "size": take})
		remaining -= take
	from edge_catcher.engine.fill_math import blended_price_cents

	assert remaining == 0, (
		f"_reconstruct_fills could not satisfy fill_size={fill_size} from "
		f"{side} levels {levels} (remaining={remaining})"
	)
	assert blended_price_cents(fills) == blended_cents, (
		f"reconstructed fills {fills} re-blend to "
		f"{blended_price_cents(fills)}c but the book-walk reported "
		f"{blended_cents}c — reconstruction diverged from walk_book_with_ceiling"
	)
	return fills


class _BookSizeGate:
	"""Test-only mock gate — returns book-derived sizes in paper-run FIFO order.

	Used by the CR-5 live run to inject the real ``gate_entry → Allow`` path
	through ``_handle_signal → _handle_enter → build_entry_order`` without
	requiring a live DB or a real ``BankrollCache``.  The sizes it returns are
	the SAME contract counts ``resolve_fill`` computed from the captured
	bundle's orderbook (recorded by ``_RecordingPaperExecutor`` during the
	paper run) — not a fiction, but the exact known book-derived output.

	Implements only the ``gate_entry`` / ``gate_exit`` protocol that
	``_handle_signal`` calls — no other ``Gate`` surface is needed.

	``gate_entry`` pops sizes from a FIFO queue built from the recorder's
	``entry_order`` (the deterministic per-bundle dispatch sequence).  When the
	queue is exhausted (more signals than recorded entries — shouldn't happen
	on the synthetic fixture, which has exactly one entry) it returns
	``Allow(size_contracts=1)`` as a safe fallback so the executor's
	``size <= 0`` guard never fires unexpectedly.

	``gate_exit`` always returns ``Allow(size_contracts=0)`` (the proxy size;
	the real exit size comes from the trade row inside ``_handle_exit`` — the
	same convention as the production gate).
	"""

	def __init__(self, recorder: _RecordingPaperExecutor) -> None:
		# FIFO queue of book-derived sizes in the order paper placed entries.
		self._size_queue: list[int] = [
			recorder.book_walk[c]["size"] for c in recorder.entry_order
		]

	def gate_entry(self, sig: object, ctx: object) -> Allow:
		size = self._size_queue.pop(0) if self._size_queue else 1
		# SizingBreakdown is required by Allow but not inspected by _handle_signal.
		breakdown = SizingBreakdown(
			fixed_fraction_contracts=size,
			quarter_kelly_contracts=2**31,  # sentinel: Kelly arm inert
			absolute_max_contracts=size,
			bound_by="fixed_fraction",
		)
		return Allow(size_contracts=size, sizing_breakdown=breakdown)

	def gate_exit(self, sig: object, ctx: object) -> Allow:
		breakdown = SizingBreakdown(
			fixed_fraction_contracts=0,
			quarter_kelly_contracts=2**31,
			absolute_max_contracts=0,
			bound_by="fixed_fraction",
		)
		return Allow(size_contracts=0, sizing_breakdown=breakdown)


class _MinimalRiskContextProvider:
	"""Test-only RiskContextProvider that builds a safe, minimal RiskContext.

	The CR-5 harness has no live DB so it cannot use the real provider.  This
	stub satisfies ``_handle_signal``'s ``assert risk_ctx_provider is not None``
	invariant and returns a RiskContext with neutral values (0 open positions,
	0 P&L, no kill active) so ``_BookSizeGate.gate_entry`` is the only gate
	check that fires.

	``market_state`` is injected at construction (after the paper run has
	seeded it) so the context is structurally correct for any gate logic that
	inspects it.
	"""

	def __init__(self, market_state: MarketState) -> None:
		self._market_state = market_state

	def build(self, signal: object, now: object) -> RiskContext:
		now_dt = now if isinstance(now, datetime) else datetime.now(tz=timezone.utc)
		return RiskContext(
			now_utc=now_dt,
			market_state=self._market_state,
			open_positions=[],
			open_count=0,
			daily_pnl_cents=0,
			operator_kill_active=False,
		)


# ---------------------------------------------------------------------------
# Test 1 — end-to-end EXECUTOR-translation parity on the tracked synthetic
# fixture (entry-parity only; the fixture has no exit/split — that path is
# unit-tested separately below).
# ---------------------------------------------------------------------------


async def _run_paper(bundle: Path) -> tuple[list[dict], _RecordingPaperExecutor]:
	"""Paper run via the recording wrapper. Returns (trades, recorder)."""
	import edge_catcher.engine.replay.backtester as bt

	captured: dict[str, _RecordingPaperExecutor] = {}
	orig_paper = bt.PaperExecutor

	class _RecorderShim:
		# replay_capture does ``PaperExecutor(market_state=..., config=...)``
		# at :148 when no executor is injected. Swap that symbol for the
		# recording wrapper so the recorder sees the SAME MarketState replay
		# seeds + drives dispatch with.
		def __new__(cls, *, market_state: MarketState, config: dict):
			rec = _RecordingPaperExecutor(market_state=market_state, config=config)
			captured["rec"] = rec
			return rec

	bt.PaperExecutor = _RecorderShim
	try:
		with _deterministic_client_order_ids():
			result = await replay_capture(bundle)
	finally:
		bt.PaperExecutor = orig_paper
	return result.trades, captured["rec"]


async def _run_live(
	bundle: Path,
	recorder: _RecordingPaperExecutor,
	*,
	corrupt: bool = False,
	live_cfg: Any,
	live_audit: Any,
) -> list[dict]:
	"""Live run: REAL dispatch path (gate → build_entry_order → LiveExecutor).

	Drives ``replay_capture`` with:
	  * A ``_BookSizeGate`` mock gate that returns ``Allow(size_contracts=N)``
	    for each entry signal in the FIFO order the paper run resolved sizes
	    from the book.  This threads through the REAL ``_handle_signal →
	    _handle_enter → build_entry_order → LiveExecutor`` path end-to-end.
	  * A ``_MinimalRiskContextProvider`` that satisfies the
	    ``risk_ctx_provider is not None`` invariant without a live DB.
	  * A ``MockKalshiServer`` queued with the book-derived per-level fills
	    (same fills PaperExecutor walked), so ``LiveExecutor._translate_order``
	    sees the identical economics.
	  * ``ExecCfg(entry_slippage_cents=0, ...)`` injected into the config so
	    ``build_entry_order``'s limit price equals the tick ask price (byte-
	    equal to the paper path when slippage is zero).

	``corrupt=True`` deliberately offsets the blended price (+5¢ per fill) to
	prove the parity assertion CAN fail (non-vacuity probe).

	The ``_BookSizedLiveExecutor`` shim is RETIRED — spec §8.2 / sizing-wire PR.
	"""
	import edge_catcher.engine.replay.backtester as _bt

	# Coid-matched mode (CR-5): each entry response is keyed by its deterministic
	# client_order_id, so entries match regardless of dispatch order, and the
	# fresh exit-order coids _handle_exit generates (which we never queue) hit
	# the server's synthesised fully-filled echo — letting a real bundle's
	# exit places complete instead of stalling on A's retry backoff. The
	# previous FIFO/sticky-tail queue only worked because the synthetic fixture
	# has exactly one entry and no exit.
	server = MockKalshiServer(match_by_client_order_id=True)
	# Queue one filled response per placed entry, keyed by coid. Each response
	# reproduces the SAME per-level fills PaperExecutor walked, at the SAME
	# bundle ticker the entry placed.
	for coid in recorder.entry_order:
		walk = recorder.book_walk[coid]
		fills = [dict(f) for f in walk["fills"]]
		if corrupt:
			# Deliberately wrong: push every fill price +5¢ so the blended
			# entry diverges far beyond the ±1¢ slippage band and != EXACT
			# blended_entry. Proves the parity assertion CAN fail.
			fills = [{"price": f["price"] + 5, "size": f["size"]} for f in fills]
		server.queue_response(
			_kalshi_filled_body(
				order_id=f"ord-{coid}",
				fills=fills,
				client_order_id=coid,
				ticker=walk["ticker"],
			)
		)

	client = server.make_client(live_cfg, live_audit)
	live_exec = LiveExecutor(client)

	# Build ExecCfg with zero entry slippage so build_entry_order's limit price
	# equals the tick's yes_ask — byte-identical to the paper path's
	# limit_price_cents (entry_price = ctx.yes_ask; slippage = 0 → limit =
	# yes_ask + 0 = yes_ask). MockKalshiServer ignores the limit price; the
	# fills are book-derived regardless, so slippage here only affects the
	# OrderRequest.limit_price_cents field, not the fill economics.
	exec_cfg: ExecCfg = validate_exec_cfg({
		"entry_slippage_cents": 0,
		"exit_slippage_cents": {
			"take_profit": 0,
			"stop_loss": 0,
			"time_exit": 0,
		},
	})

	# Load the bundle config (same path replay_capture uses internally) so we
	# can inject _exec_cfg before passing it as the config override.
	_cfg_path = bundle / "paper-trader.yaml"
	_base_cfg: dict = yaml.safe_load(_cfg_path.read_text(encoding="utf-8")) or {}
	_base_cfg["_exec_cfg"] = exec_cfg

	# Build the mock gate + provider now that the paper run has finalised the
	# recorder.  Both are constructed after the paper run — the FIFO queue in
	# _BookSizeGate is built from recorder.entry_order (deterministic).
	mock_gate = _BookSizeGate(recorder)
	mock_provider = _MinimalRiskContextProvider(recorder._ms)

	# Patch dispatch_message to inject risk + risk_ctx_provider so the live
	# replay drives the REAL _handle_signal → _handle_enter → build_entry_order
	# path.  The patch wraps the real function for the duration of this call
	# only (restored in the finally block) — zero edge_catcher/ source change.
	_orig_dispatch = _bt.dispatch_message

	async def _patched_dispatch(**kwargs: Any) -> None:
		kwargs["risk"] = mock_gate
		kwargs["risk_ctx_provider"] = mock_provider
		return await _orig_dispatch(**kwargs)

	_bt.dispatch_message = _patched_dispatch  # type: ignore[assignment]
	try:
		with _deterministic_client_order_ids():
			result = await replay_capture(bundle, executor=live_exec, config=_base_cfg)
	finally:
		_bt.dispatch_message = _orig_dispatch

	await client.close()
	return result.trades


def _kalshi_filled_body(
	*, order_id: str, fills: list[dict[str, int]], client_order_id: str,
	ticker: str = "SYN-TEST-T1",
) -> dict[str, Any]:
	"""Kalshi 201 body for a fully-filled order whose fills are the
	book-derived per-level fills (sums to filled_count; price irrelevant to
	count). Mirrors the production ``{"order": {...}}`` wire shape
	``LiveExecutor._translate_order`` reads (``order.raw["fills"]``).

	``ticker`` defaults to the synthetic fixture's ticker but is supplied
	per-entry for real bundles (which span many tickers)."""
	filled = sum(f["size"] for f in fills)
	return {
		"order": {
			"order_id": order_id,
			"ticker": ticker,
			"side": "yes",
			"action": "buy",
			"count": filled,
			"yes_price": fills[0]["price"] if fills else 0,
			"time_in_force": "immediate_or_cancel",
			"status": "executed",
			"filled_count": filled,
			"fills": fills,
			"client_order_id": client_order_id,
		}
	}


def _compare_executor_results(
	paper: OrderResult, live: OrderResult
) -> dict[str, tuple[Any, Any]]:
	"""CR-5 economic diff: ``blended_entry_cents`` / ``filled_size`` EXACT, plus
	live must reach ``filled`` status (a live reject/pending on an entry paper
	FILLED is itself a divergence).

	These are the P&L-determining fields — equal cost basis + fill size ⇒ equal
	trade-row economics, because ``record_trade`` derives entry fee + pnl
	deterministically from them, mode-agnostically (so OrderResult parity on
	these two ⟹ trade-row parity).

	``slippage_cents`` is INTENTIONALLY NOT compared. It is a diagnostic field
	the two executors compute against DIFFERENT references — PaperExecutor vs the
	top-of-book best price (``executors/paper.py``: "market impact"), LiveExecutor
	vs the order's limit (``executors/live.py``: "vs-limit execution"). On a real
	book where the fill lands below the limit they legitimately differ (paper 0
	vs live the favorable delta) with ZERO P&L divergence. CR-5 proves economic
	(cost-basis) parity; whether to unify the slippage reporting reference for
	F-UI consistency is tracked as a separate follow-up — it is not a cutover
	concern (it never moves cost basis, size, fees, or pnl).
	"""
	d: dict[str, tuple[Any, Any]] = {}
	if live.status != "filled":
		d["status"] = (paper.status, live.status)
	if paper.blended_entry_cents != live.blended_entry_cents:
		d["blended_entry_cents"] = (paper.blended_entry_cents, live.blended_entry_cents)
	if paper.filled_size != live.filled_size:
		d["filled_size"] = (paper.filled_size, live.filled_size)
	return d


async def _run_executor_parity_diffs(
	bundle: Path,
	*,
	live_cfg: Any,
	live_audit: Any,
	corrupt: bool = False,
) -> tuple[list[tuple[str, dict]], int]:
	"""Executor-translation parity over a bundle's FILLED entries (real-bundle CR-5).

	Replays the bundle through PaperExecutor ONCE (the recording wrapper captures
	each filled entry's intent + book-derived fills + PaperExecutor's OrderResult),
	then for EACH filled entry feeds ``LiveExecutor`` the SAME OrderRequest intent
	+ the SAME book-derived fills via a coid-matched ``MockKalshiServer`` and diffs
	the two executors' OrderResults (``_compare_executor_results``).

	This isolates the only thing that varies live-vs-paper in production — the
	executor translation. There is NO gate / dispatch-replay / coid-join, so an
	entry PaperExecutor SKIPPED can never phantom-fill live (the full-replay
	harness's failure mode on real multi-entry bundles). ``record_trade`` is a
	deterministic function of the OrderResult, so OrderResult parity ⟹ trade-row
	parity. Only one replay (paper) is needed — ~2× faster than the dual replay.

	``corrupt=True`` offsets every live fill +5¢ to prove the diff CAN fail.
	Returns ``(diffs, n_filled_entries)`` where ``diffs`` is
	``[(client_order_id, diffdict), ...]`` (empty ⇒ parity holds).
	"""
	_, recorder = await _run_paper(bundle)
	diffs: list[tuple[str, dict]] = []
	for coid in recorder.entry_order:
		rec = recorder.book_walk[coid]
		fills = [dict(f) for f in rec["fills"]]
		if corrupt:
			fills = [{"price": f["price"] + 5, "size": f["size"]} for f in fills]
		server = MockKalshiServer(match_by_client_order_id=True)
		server.queue_response(
			_kalshi_filled_body(
				order_id=f"ord-{coid}", fills=fills,
				client_order_id=coid, ticker=rec["ticker"],
			)
		)
		client = server.make_client(live_cfg, live_audit)
		try:
			live_exec = LiveExecutor(client)
			# Same sized intent PaperExecutor walked: size = book-derived fill_size,
			# limit = the entry price paper booked at (so slippage is comparable).
			live_req = OrderRequest(
				ticker=rec["ticker"],
				series=rec["series"],
				side=rec["side"],
				size_contracts=rec["size"],
				limit_price_cents=rec["limit"],
				strategy=rec["strategy"],
				client_order_id=coid,
				action="buy",
			)
			live_result = await live_exec.place(live_req)
		finally:
			await client.close()
		d = _compare_executor_results(rec["paper_result"], live_result)
		if d:
			diffs.append((coid, d))
	return diffs, len(recorder.entry_order)


@pytest.mark.asyncio
async def test_executor_parity_synthetic_bundle_entry(
	live_cfg, live_audit, signing_env
):
	"""End-to-end: replay the tracked synthetic bundle through paper and
	through live (book-derived fills). The single entry-only trade must match
	on the parity whitelist under the CR-5 tolerance.

	This is harness-correctness on the tracked fixture (spec §9 CI scope).
	The fixture yields exactly ONE open entry-only trade — so this asserts
	ENTRY parity; exit/split parity is the runbook gate's job (real bundles)
	and the collapse logic is unit-tested below.
	"""
	paper_rows, recorder = await _run_paper(_SYNTHETIC_BUNDLE)

	# Sanity: the fixture is the known single open entry-only trade.
	assert len(paper_rows) == 1, f"fixture changed shape: {paper_rows!r}"
	assert paper_rows[0]["status"] == "open"
	assert recorder.entry_order, "paper run recorded no entry book-walk"

	live_rows = await _run_live(
		_SYNTHETIC_BUNDLE, recorder, live_cfg=live_cfg, live_audit=live_audit
	)

	paper_c = _collapse_live_split_rows(paper_rows)
	live_c = _collapse_live_split_rows(live_rows)

	paper_by_key = {_composite_key(r): r for r in paper_c}
	live_by_key = {_composite_key(r): r for r in live_c}

	assert set(paper_by_key) == set(live_by_key), (
		"key sets diverge — paper="
		f"{sorted(paper_by_key)} live={sorted(live_by_key)}"
	)

	for k in paper_by_key:
		diffs = _cr5_material_diffs(paper_by_key[k], live_by_key[k])
		assert not diffs, (
			f"CR-5 executor-parity violated for {k}: {diffs}\n"
			f"paper={paper_by_key[k]}\nlive={live_by_key[k]}"
		)

	# Spot-check the load-bearing economics explicitly (defence: prove the
	# pass is meaningful, not an empty key-set).
	pk = next(iter(paper_by_key))
	pr, lr = paper_by_key[pk], live_by_key[pk]
	assert pr["fill_size"] == lr["fill_size"]
	assert pr["blended_entry"] == lr["blended_entry"]
	assert abs((pr["slippage_cents"] or 0) - (lr["slippage_cents"] or 0)) <= 1


# ---------------------------------------------------------------------------
# Test 2 — NON-VACUITY (mandatory: this harness IS the cutover gate's
# machinery). Prove the parity assertion CAN fail: feed a deliberately-WRONG
# mock fill (blended price off by +5¢, well beyond the ±1¢ band) and show the
# diff RED-s; then the correctly book-derived fills GREEN.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_executor_parity_is_non_vacuous(
	live_cfg, live_audit, signing_env
):
	"""RED→GREEN: a corrupted live fill MUST trip the parity assertion; the
	correct book-derived fill MUST pass it. A parity test that cannot fail is
	worthless."""
	paper_rows, recorder = await _run_paper(_SYNTHETIC_BUNDLE)
	paper_c = _collapse_live_split_rows(paper_rows)
	paper_by_key = {_composite_key(r): r for r in paper_c}

	# --- RED: deliberately-wrong fills (blended +5¢ ⇒ != EXACT blended_entry
	# AND > ±1¢ slippage band). The parity diff MUST be non-empty.
	live_bad = await _run_live(
		_SYNTHETIC_BUNDLE,
		recorder,
		corrupt=True,
		live_cfg=live_cfg,
		live_audit=live_audit,
	)
	live_bad_by_key = {
		_composite_key(r): r for r in _collapse_live_split_rows(live_bad)
	}
	red_diffs: dict[tuple, dict] = {}
	for k in paper_by_key:
		d = _cr5_material_diffs(paper_by_key[k], live_bad_by_key[k])
		if d:
			red_diffs[k] = d
	assert red_diffs, (
		"NON-VACUITY FAILURE: a corrupted live fill (blended +5c) did NOT "
		"trip the parity diff — the parity assertion is vacuous and worthless"
	)
	# The corruption must surface as a blended_entry mismatch (EXACT column).
	assert any("blended_entry" in d for d in red_diffs.values()), (
		f"expected blended_entry divergence under corruption, got {red_diffs}"
	)

	# --- GREEN: the correctly book-derived fills pass cleanly.
	live_good = await _run_live(
		_SYNTHETIC_BUNDLE, recorder, live_cfg=live_cfg, live_audit=live_audit
	)
	live_good_by_key = {
		_composite_key(r): r for r in _collapse_live_split_rows(live_good)
	}
	for k in paper_by_key:
		d = _cr5_material_diffs(paper_by_key[k], live_good_by_key[k])
		assert not d, (
			f"GREEN leg failed — correct book-derived fills must match: {d}"
		)


# ---------------------------------------------------------------------------
# Test 3 — §10.3 collapse helper, unit-tested directly with a hand-constructed
# parent+children split-row input. NON-VACUOUS even though the tracked
# synthetic fixture has no exit/split (so the end-to-end run above can't
# exercise it).
# ---------------------------------------------------------------------------


def _split_family() -> list[dict[str, Any]]:
	"""A live split: 10-contract yes entry @50¢, two partial exits (4 then 6)
	fully flattening it. Mirrors ``record_partial_exit``'s output shape: a
	parent residual that ends 'open' with 0 remaining is impossible (the final
	partial is a record_close → no residual), so a FULLY-closed family is
	[child1, child2] with NO 'open' row. Identity tuple is shared verbatim."""
	ident = dict(
		strategy="syn",
		ticker="SYN-X-T1",
		side="yes",
		series_ticker="SYN",
		entry_price=50,
		blended_entry=50,
		entry_time="2026-04-15T12:00:00+00:00",
		book_depth=None,
		book_snapshot=None,
		fill_pct=1.0,
	)
	# child 1: 4 of 10 closed @ 60¢ (won), allocated entry fee 4/10 of 14 ≈ 6
	child1 = dict(
		ident,
		id=2,
		intended_size=4,
		fill_size=4,
		entry_fee_cents=6,
		status="won",
		exit_price=60,
		exit_time="2026-04-15T12:05:00+00:00",
		pnl_cents=34,
	)
	# child 2 (final close): 6 of 10 closed @ 60¢ (won), remaining entry fee 8
	child2 = dict(
		ident,
		id=3,
		intended_size=6,
		fill_size=6,
		entry_fee_cents=8,
		status="won",
		exit_price=60,
		exit_time="2026-04-15T12:06:00+00:00",
		pnl_cents=52,
	)
	return [child1, child2]


def test_collapse_live_split_rows_fully_closed_family():
	"""Two closed children (no residual) collapse to ONE closed logical row:
	sizes + fees + pnl summed, terminal fields from the final child."""
	collapsed = _collapse_live_split_rows(_split_family())

	assert len(collapsed) == 1, f"expected 1 logical row, got {collapsed!r}"
	row = collapsed[0]
	assert row["fill_size"] == 10           # 4 + 6
	assert row["intended_size"] == 10       # 4 + 6
	assert row["entry_fee_cents"] == 14     # 6 + 8 (re-sums to original total)
	assert row["pnl_cents"] == 86           # 34 + 52
	assert row["status"] == "won"           # from the final child
	assert row["exit_price"] == 60
	assert row["exit_time"] == "2026-04-15T12:06:00+00:00"
	# Identity + cost basis invariant across the family.
	assert row["blended_entry"] == 50
	assert _composite_key(row) == ("syn", "SYN-X-T1", "yes", "2026-04-15T12:00:00+00:00")


def test_collapse_live_split_rows_residual_still_open():
	"""Parent residual still 'open' + one closed child → logical row stays
	OPEN (paper writes the closed row only once flat); sizes still summed."""
	residual = dict(
		strategy="syn",
		ticker="SYN-X-T1",
		side="yes",
		series_ticker="SYN",
		entry_price=50,
		blended_entry=50,
		entry_time="2026-04-15T12:00:00+00:00",
		book_depth=None,
		book_snapshot=None,
		fill_pct=1.0,
		id=1,
		intended_size=6,
		fill_size=6,
		entry_fee_cents=8,
		status="open",
		exit_price=None,
		exit_time=None,
		pnl_cents=None,
	)
	child = dict(residual, id=2, intended_size=4, fill_size=4, entry_fee_cents=6,
	             status="won", exit_price=60, exit_time="2026-04-15T12:05:00+00:00",
	             pnl_cents=34)
	collapsed = _collapse_live_split_rows([residual, child])

	assert len(collapsed) == 1
	row = collapsed[0]
	assert row["status"] == "open"          # residual alive → logical open
	assert row["fill_size"] == 10           # 6 residual + 4 closed
	assert row["intended_size"] == 10
	assert row["entry_fee_cents"] == 14     # 8 + 6 (re-sums to original)
	assert row["exit_price"] is None
	assert row["exit_time"] is None
	assert row["pnl_cents"] is None


def test_collapse_live_split_rows_single_row_passthrough():
	"""A non-split family (one row, no children — the synthetic-fixture and
	common case) passes through UNCHANGED."""
	single = [dict(
		strategy="syn", ticker="SYN-X-T1", side="yes", series_ticker="SYN",
		entry_price=50, blended_entry=50, entry_time="2026-04-15T12:00:00+00:00",
		book_depth=None, book_snapshot=None, fill_pct=1.0, id=1,
		intended_size=4, fill_size=4, entry_fee_cents=6, status="open",
		exit_price=None, exit_time=None, pnl_cents=None,
	)]
	collapsed = _collapse_live_split_rows(single)
	assert collapsed == single
	assert collapsed is not single  # defensive copy, not the same list/dicts


# ---------------------------------------------------------------------------
# Test 4 — EXECUTOR-LEVEL parity (the real-bundle CR-5 path).
#
# CR-5's question is executor-translation parity: given the SAME sized intent
# and the SAME fills, does LiveExecutor produce the same OrderResult economics
# as PaperExecutor's book-walk? Dispatch is mode-agnostic (the executor is the
# only seam) and record_trade is a deterministic function of the OrderResult,
# so OrderResult parity ⟹ trade-row parity. This compares the two executors
# DIRECTLY per filled entry — no gate, no dispatch-replay, no coid-join, so it
# cannot phantom-fill an entry PaperExecutor skipped (the full-replay harness's
# failure mode on real multi-entry bundles). The gitignored real-bundle runner
# (scripts/run_cr5_parity.py) reuses _run_executor_parity_diffs over the local
# captured bundles. These tests prove the comparator on the tracked fixture.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_executor_level_parity_synthetic_entry(live_cfg, live_audit, signing_env):
	"""On the tracked fixture: feeding LiveExecutor the SAME intent + the SAME
	book-derived fills PaperExecutor walked yields the SAME P&L-determining
	economics (blended_entry + fill_size EXACT). slippage_cents is excluded —
	see _compare_executor_results (paper measures vs best, live vs limit)."""
	diffs, n_entries = await _run_executor_parity_diffs(
		_SYNTHETIC_BUNDLE, live_cfg=live_cfg, live_audit=live_audit
	)
	assert n_entries >= 1, "fixture must produce at least one filled entry"
	assert not diffs, f"executor-level parity violated: {diffs}"


@pytest.mark.asyncio
async def test_executor_level_parity_is_non_vacuous(live_cfg, live_audit, signing_env):
	"""A corrupted live fill (blended +5¢) MUST trip the comparator — proves the
	executor-level check can fail (a parity gate that can't fail is worthless)."""
	diffs, n_entries = await _run_executor_parity_diffs(
		_SYNTHETIC_BUNDLE, live_cfg=live_cfg, live_audit=live_audit, corrupt=True
	)
	assert n_entries >= 1
	assert diffs, (
		"NON-VACUITY FAILURE: corrupted live fills (+5¢) did not trip the "
		"executor-level comparator"
	)
	assert any("blended_entry_cents" in d for _, d in diffs), (
		f"expected blended_entry divergence under corruption, got {diffs}"
	)
