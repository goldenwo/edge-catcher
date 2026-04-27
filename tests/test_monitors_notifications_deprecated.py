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
