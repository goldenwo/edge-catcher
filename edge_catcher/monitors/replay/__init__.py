"""Replay backtester for captured orderbook bundles.

Reads bundles produced by ``edge_catcher.monitors.capture.bundle.assemble_daily_bundle``
and dispatches their events through the same ``dispatch.py`` code path as the
live trader. The parity test in ``tests/test_replay_parity.py`` is the
correctness gate.

See docs/superpowers/specs/2026-04-14-orderbook-capture-replay-design.md §4.6.
"""
