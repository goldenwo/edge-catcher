"""Regression tests for LLMClient._call_claude_code error tolerance.

The Claude Code CLI's `-p` (print) mode exits non-zero when a SessionEnd
hook cancels during shutdown — even when the LLM response was already
written to stdout successfully. Without salvage, every refinement call
in the research loop fails despite the underlying call having succeeded.

These tests pin down: salvage stdout when non-empty on exit-non-zero;
still fail loudly when stdout is empty (real failures like quota or
auth).
"""

from __future__ import annotations

from unittest.mock import patch, MagicMock

import pytest

from edge_catcher.ai.client import LLMClient, LLMError


def _make_client_for_claude_code() -> LLMClient:
	"""Construct an LLMClient pinned to the claude-code provider for testing."""
	with patch("edge_catcher.ai.client.detect_active_provider", return_value="claude-code"):
		return LLMClient()


def _mock_proc(returncode: int, stdout: str = "", stderr: str = "") -> MagicMock:
	proc = MagicMock()
	proc.returncode = returncode
	proc.stdout = stdout
	proc.stderr = stderr
	return proc


def test_normal_success_returns_stdout():
	client = _make_client_for_claude_code()
	with patch("subprocess.run", return_value=_mock_proc(0, stdout='{"ok": true}')), \
	     patch("shutil.which", return_value="/usr/bin/claude"):
		result = client._call_claude_code("system", "user", model=None)
	assert result == '{"ok": true}'


def test_exit_nonzero_with_stdout_is_salvaged():
	"""Hook cancellation case: response on stdout, hook error on stderr, exit 1."""
	client = _make_client_for_claude_code()
	hook_err = "SessionEnd hook [...] failed: Hook cancelled"
	with patch("subprocess.run", return_value=_mock_proc(1, stdout='{"ok": true}', stderr=hook_err)), \
	     patch("shutil.which", return_value="/usr/bin/claude"):
		result = client._call_claude_code("system", "user", model=None)
	assert result == '{"ok": true}'


def test_exit_nonzero_empty_stdout_raises():
	"""Real failure case (quota, auth, model-not-found): empty stdout → LLMError."""
	client = _make_client_for_claude_code()
	with patch("subprocess.run", return_value=_mock_proc(1, stdout="", stderr="You're out of extra usage")), \
	     patch("shutil.which", return_value="/usr/bin/claude"):
		with pytest.raises(LLMError, match="out of extra usage"):
			client._call_claude_code("system", "user", model=None)


def test_exit_nonzero_whitespace_stdout_raises():
	"""Whitespace-only stdout should not be treated as substantive content."""
	client = _make_client_for_claude_code()
	with patch("subprocess.run", return_value=_mock_proc(1, stdout="   \n  ", stderr="real error")), \
	     patch("shutil.which", return_value="/usr/bin/claude"):
		with pytest.raises(LLMError, match="real error"):
			client._call_claude_code("system", "user", model=None)
