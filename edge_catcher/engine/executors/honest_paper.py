"""Honest paper fill simulator — Phase 1 (fixed-slippage stub).

WHY: P1's debut-fade verdict found Gap-1 — the optimistic PaperExecutor reported
a 53.1% paper win-rate vs the strategy's true 22.2% live win-rate on the same
window (non-overlapping 95% CIs). The optimistic executor walks the book the bot
sees and assumes that liquidity is available at quoted prices; live IOCs slip,
partially fill, or miss. This wrapper applies a pessimistic per-strategy slippage
penalty on top of the optimistic fill so paper stops over-promising.

Phase plan (spec 2026-05-28-honest-paper-fill-simulator-design.md):
  Phase 1  — this file: HonestPaperExecutor wraps PaperExecutor, FixedSlippageModel
             applies a hand-tuned constant penalty. Opt-in via paper_fill_model.
  Phase 2  — EmpiricalSlippageModel fit to validated live data (drop-in SlippageModel).
  Phase 3  — collapse the wrapper into a single PaperExecutor; retire the optimistic path.

Opt-in: config key `paper_fill_model: "fixed"` (default "optimistic" = bare
PaperExecutor, byte-unchanged). `executor:` stays the mode-of-record, untouched.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Mapping, Protocol

from edge_catcher.engine.executor import OrderRequest, OrderResult
from edge_catcher.engine.market_state import OrderbookSnapshot


class SlippageModel(Protocol):
	"""Pessimistic post-processor for an optimistic paper fill.

	One method so Phase 2's EmpiricalSlippageModel is a drop-in replacement.
	``orderbook`` is supplied for Phase-2 conditional models; Phase 1 ignores it.
	``request`` exposes ``ticker`` + ``strategy`` but NOT a timestamp — Phase 2's
	deterministic seed will need an additive ``OrderRequest.signal_timestamp``.
	"""

	def adjust(
		self,
		result: OrderResult,
		request: OrderRequest,
		orderbook: OrderbookSnapshot,
	) -> OrderResult: ...


@dataclass(frozen=True)
class FixedSlippageModel:
	"""Apply a fixed per-strategy slippage penalty to a filled paper result.

	Phase 1 stub: ``default_cents`` is a hand-tuned operator guess (NOT a fitted
	value — the spec's conservative-bound win condition is only verifiable in
	Phase 2). ``per_strategy`` overrides it for named strategies.
	"""

	default_cents: int
	per_strategy: Mapping[str, int]

	def adjust(
		self,
		result: OrderResult,
		request: OrderRequest,
		orderbook: OrderbookSnapshot,
	) -> OrderResult:
		if result.status != "filled":
			return result  # rejected / pending pass through untouched
		# blended_entry_cents == 0 is PaperExecutor's legacy empty-book sentinel
		# for the *price* field — distinct from the dual-slippage *metric* fields,
		# which use None for "not measurable". On the sentinel, price is 0 and
		# metrics are already None; never add slippage to a non-fill.
		if result.blended_entry_cents == 0:
			return result
		x = self.per_strategy.get(request.strategy, self.default_cents)
		# PRICE moves side-signed: a buy pays MORE (+x), a sell receives LESS (-x).
		# Clamp to the 1..99c OrderRequest domain.
		price_signed = x if request.action == "buy" else -x
		new_blended = max(1, min(99, result.blended_entry_cents + price_signed))
		# SLIPPAGE metrics use the "positive = worse" convention on BOTH sides
		# (fill_math.signed_slippage_cents: buy = blended-ref, sell = ref-blended),
		# so the worsening is always ADDED regardless of side. Use the effective
		# post-clamp magnitude so a clamped fill never reports more slippage than
		# the price actually moved.
		worsening = abs(new_blended - result.blended_entry_cents)
		return replace(
			result,
			blended_entry_cents=new_blended,
			market_impact_cents=(
				result.market_impact_cents + worsening
				if result.market_impact_cents is not None
				else None
			),
			limit_slippage_cents=(
				result.limit_slippage_cents + worsening
				if result.limit_slippage_cents is not None
				else None
			),
		)
