from edge_catcher.engine.metrics import Metrics, _GATE_REJECT_COUNTER
from edge_catcher.engine.risk import GateRejectReason
import typing


def test_stale_bankroll_is_registered_reason_and_counter():
	# In the GateRejectReason Literal:
	assert "STALE_BANKROLL" in typing.get_args(GateRejectReason)
	# Mapped to a counter key:
	assert _GATE_REJECT_COUNTER["STALE_BANKROLL"] == "risk_gate_rejected_stale_bankroll"
	# And the key is registered (Metrics.inc raises KeyError on unknown keys):
	m = Metrics()
	m.inc("risk_gate_rejected_stale_bankroll")  # must NOT raise


def test_riskcontext_open_count_is_independent_field():
	from datetime import datetime, timezone
	from edge_catcher.engine.risk import RiskContext
	# open_count is a field that can differ from len(open_positions):
	ctx = RiskContext(
		now_utc=datetime(2026, 5, 22, tzinfo=timezone.utc),
		market_state=None,
		open_positions=[],          # zero status='open' rows
		open_count=3,               # but 3 pending rows hold MAX_OPEN slots
		daily_pnl_cents=0,
		operator_kill_active=False,
	)
	assert ctx.open_count == 3
	assert len(ctx.open_positions) == 0
