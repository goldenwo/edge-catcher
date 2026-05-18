"""Cross-layer parity guard: the three ``_clamp_fill_pct`` implementations
must stay byte-for-byte semantically identical.

``_clamp_fill_pct`` is deliberately REPLICATED (not shared) in three places:

* ``edge_catcher/engine/executors/live.py``  — engine execution layer
* ``edge_catcher/live/reconciliation.py``    — live reconciler
* ``edge_catcher/live/ws_handlers.py``        — live WS handlers

The replication is an intentional layer-decoupling decision (see the
docstring on ``reconciliation._clamp_fill_pct``: importing the executor
helper would couple the live reconciler to the engine's execution layer).
Each copy's own docstring mandates the semantics match "byte-for-byte" so a
row's ``fill_pct`` is identical regardless of which path (executor /
reconcile / WS) booked the fill — F's slippage/partial-fill analytics read
the column as a probability and must not diverge by code path.

Three hand-maintained copies that MUST agree is a divergence hazard, and a
divergence here is exactly the path-divergence bug class this PR's review
rounds eliminated. This test is the ENFORCED invariant that makes the
deliberate replication safe: it fails CI the instant any copy diverges,
turning a silent future-incident into an immediate signal. If you
intentionally change the clamp semantics, change ALL THREE copies together
and extend the grid below.
"""
from __future__ import annotations

import pytest

from edge_catcher.engine.executors.live import _clamp_fill_pct as _clamp_exec
from edge_catcher.live.reconciliation import _clamp_fill_pct as _clamp_recon
from edge_catcher.live.ws_handlers import _clamp_fill_pct as _clamp_ws

# (numerator, denominator) spanning the full contract: non-positive
# denominator (→ 0.0 defence-in-depth), zero fill, proper fractions
# including a non-terminating one (exact float-division identity, NOT
# numeric closeness), exact full (→ 1.0), and overfill (→ clamp 1.0).
_GRID = [
	(0, 0),
	(5, 0),
	(3, -2),
	(0, 10),
	(3, 10),
	(1, 3),
	(7, 10),
	(9, 10),
	(10, 10),
	(11, 10),
	(250, 10),
	(123, 456),
	(1, 1_000_000),
]


@pytest.mark.parametrize(("num", "den"), _GRID)
def test_clamp_fill_pct_three_copies_are_byte_identical(
	num: int, den: int
) -> None:
	"""All three ``_clamp_fill_pct`` copies must return the EXACT same float
	for every input (exact ``==``, NOT ``approx`` — the contract is
	byte-for-byte semantic identity, not numeric closeness; a future
	rounding change in one copy is precisely the drift this guards)."""
	r_exec = _clamp_exec(num, den, None)
	r_recon = _clamp_recon(num, den)
	r_ws = _clamp_ws(num, den)
	assert r_exec == r_recon == r_ws, (
		f"_clamp_fill_pct diverged for (num={num}, den={den}): "
		f"executor={r_exec!r}, reconciliation={r_recon!r}, "
		f"ws_handlers={r_ws!r} — the three copies are deliberately "
		f"replicated but MUST stay byte-for-byte identical (see this "
		f"file's module docstring); change all three together"
	)
