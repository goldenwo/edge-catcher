"""Tests for edge_catcher.research.journal formatting helpers."""



def test_trajectory_uses_total_sessions_field():
	"""Trajectory entries should use 'total_sessions' not 'total_runs'."""
	from edge_catcher.research.journal import _format_trajectory

	content = {"status": "improving", "total_sessions": 5, "promote_rate": 0.1}
	text = _format_trajectory(content)
	assert "sessions" in text.lower()
	assert "Total runs" not in text
