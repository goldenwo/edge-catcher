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
