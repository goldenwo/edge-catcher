"""Test-only helpers for wiring a PaperExecutor into engine call sites.

Pre-G test call sites called ``process_tick`` / ``dispatch_message`` /
``_handle_ticker_msg`` / ``_handle_trade_msg`` without an executor argument.
Sub-project G adds ``executor: Executor`` as a required positional parameter
on each of those (engine/G's spec §Executor protocol contract).

These helpers let tests rewrite the call sites mechanically:

    process_tick(ctx, strategies, store, config, now=_now())
    # becomes
    process_tick(ctx, strategies, store, config,
                 _make_executor_for_ctx(ctx, config), now=_now())

For ticker/trade-msg tests that already build a real ``MarketState`` (``ms``),
pass ``PaperExecutor(market_state=ms, config=config)`` directly — no stub needed.
"""

from __future__ import annotations

from edge_catcher.engine.executors.paper import PaperExecutor


_DEFAULT_SIZING = {
	"sizing": {
		"risk_per_trade_cents": 200,
		"max_slippage_cents": 5,
		"min_fill": 1,
		"require_fresh_book": True,
	}
}


class _StubMarketState:
	"""MarketState stand-in that returns a single canned book for any ticker.

	PaperExecutor.place(req) calls ``market_state.get_orderbook(ticker)`` to walk
	the book; tests that hand-build a ``TickContext.orderbook`` without a real
	MarketState use this stub so the executor sees the same book the strategy did.
	"""

	def __init__(self, book) -> None:
		self._book = book

	def get_orderbook(self, ticker: str):  # noqa: ARG002 — same book for any ticker
		return self._book


def _make_executor_for_ctx(ctx, config: dict | None = None) -> PaperExecutor:
	"""Build a PaperExecutor backed by ``_StubMarketState(ctx.orderbook)``.

	Use in tests that pre-G called ``process_tick`` without an executor — the
	new signature requires one (engine/G's spec §Executor protocol contract).
	When ``config`` is None or missing a ``sizing`` block, falls back to the
	low-risk defaults that match the legacy paper-trader test config.
	"""
	cfg = config if config and "sizing" in config else _DEFAULT_SIZING
	return PaperExecutor(market_state=_StubMarketState(ctx.orderbook), config=cfg)
