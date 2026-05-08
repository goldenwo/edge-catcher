"""Backward-compatibility shim — sizing.py has moved to engine/executors/paper.py.

Re-exports all public names so existing monitors/ callers (dispatch.py, discovery.py)
and tests (test_sizing.py) continue to work without change.
"""

from edge_catcher.engine.executors.paper import (  # noqa: F401
	FillSkip,
	FillSkipReason,
	compute_raw_size,
	resolve_fill,
	validate_sizing_config,
	walk_book_with_ceiling,
)
