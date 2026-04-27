"""Verify the legacy monitors/notifications.py emits a DeprecationWarning at import."""
from __future__ import annotations

import importlib
import warnings


def test_import_emits_deprecation_warning():
	# Force a fresh import so the module-level warnings.warn fires.
	# (If the module was already imported by an earlier test, the warning
	# was already emitted and a re-import won't re-fire it. importlib.reload
	# re-runs module-level code.)
	import edge_catcher.monitors.notifications as mod
	with warnings.catch_warnings(record=True) as captured:
		warnings.simplefilter("always")
		importlib.reload(mod)
	deprecation = [w for w in captured if issubclass(w.category, DeprecationWarning)]
	assert len(deprecation) >= 1
	msg = str(deprecation[0].message)
	assert "edge_catcher.monitors.notifications" in msg
	assert "deprecated" in msg.lower()
	assert "edge_catcher.notifications" in msg  # points at the replacement


def test_pyproject_filter_actually_matches_deprecation_message():
	"""Regression: the prior filter `ignore::DeprecationWarning:edge_catcher.monitors.notifications`
	was a no-op because the warning is emitted with stacklevel=2, so its reported
	module is the CALLER (monitors.dispatch / monitors.engine), not
	monitors.notifications itself. A module-targeted filter never matched.

	The fix uses a message-regex filter, which IS robust against stacklevel
	attribution. This test introspects warnings.filters at runtime and confirms
	at least one registered 'ignore' entry would actually match the deprecation
	message — catching any future regression to a module-targeted filter.
	"""
	target_msg = (
		"edge_catcher.monitors.notifications is deprecated; "
		"use edge_catcher.notifications for new code. "
		"This module will be migrated onto the unified notifications layer "
		"in a future release."
	)
	matched = False
	for action, message_regex, category, _module_regex, _lineno in warnings.filters:
		if action != "ignore":
			continue
		# The filter's category must be DeprecationWarning or a parent (e.g. Warning).
		if not issubclass(DeprecationWarning, category):
			continue
		if message_regex is None:
			continue
		if message_regex.search(target_msg):
			matched = True
			break
	assert matched, (
		"No registered 'ignore' DeprecationWarning filter matches the "
		"monitors.notifications deprecation message. The pyproject.toml "
		"filterwarnings entry is either missing or targets the wrong field. "
		f"Current warnings.filters: {warnings.filters}"
	)
