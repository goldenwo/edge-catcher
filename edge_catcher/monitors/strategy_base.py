"""Bundle-compat shim — re-exports for ``edge_catcher.engine.strategy_base``.

Pre-cutover daily bundles capture a copy of the running ``strategies_local.py``,
which at the time inherited from ``PaperStrategy`` imported from
``edge_catcher.monitors.strategy_base``. Sub-project G renames the base class
to ``Strategy`` and moves it to ``edge_catcher.engine.strategy_base``; this
shim keeps the OLD import + class name resolvable so the replay backtester
can load those captured strategy files when sweeping historical R2 bundles
for the cutover-gate parity check.

Active code MUST import ``Strategy`` from ``edge_catcher.engine.strategy_base``.
This shim exists ONLY so old bundles + Pi-side rollback continue to resolve
their imports through the deferred-retirement window. After ``monitors/`` is
fully retired (follow-up PR after >=3 stable Pi days on the new engine), this
file goes away.
"""

from edge_catcher.engine.strategy_base import Signal
from edge_catcher.engine.strategy_base import Strategy as PaperStrategy

__all__ = ["PaperStrategy", "Signal"]
