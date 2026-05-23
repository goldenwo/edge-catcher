"""Synthetic strategy used by the engine/replay smoke test.

Public-safe fixture — does NOT reveal real strategy parameters or logic.
Fires a single deterministic enter signal on the first observation of the
synthetic ticker so the smoke test can verify the replay path produces
trade rows end-to-end through the new engine/dispatch path.
"""
from __future__ import annotations

from edge_catcher.engine.strategy_base import Signal, Strategy


class SyntheticTickStrategy(Strategy):
	name = "synthetic_tick"
	supported_series = ["SYN"]
	default_params: dict = {}
	emoji = "🧪"

	def on_tick(self, ctx) -> list[Signal]:
		if ctx.is_first_observation:
			return [
				Signal(
					action="enter",
					ticker=ctx.ticker,
					side="yes",
					series=ctx.series,
					strategy=self.name,
					reason="synthetic first-tick entry",
					# Required by build_entry_order on the live dispatch path.
					# entry_price_cents mirrors _handle_enter's own entry_price
					# computation (ctx.yes_ask for a yes-side entry) so the live
					# limit price is byte-equal to the paper limit when
					# ExecCfg.entry_slippage_cents=0.  stop_loss_distance_cents
					# is consumed by gate sizing (not by the order builder itself)
					# and must be a positive integer on the live path.
					entry_price_cents=ctx.yes_ask,
					stop_loss_distance_cents=5,
				)
			]
		return []
