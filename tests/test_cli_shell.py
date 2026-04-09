"""Test that the CLI shell loads and dispatches commands."""


def test_main_function_exists():
	from edge_catcher.cli import main
	assert callable(main)


def test_help_exits_cleanly():
	"""--help should exit 0 (not crash)."""
	import sys
	from edge_catcher.cli import main
	old_argv = sys.argv
	sys.argv = ["edge_catcher", "--help"]
	try:
		main()
	except SystemExit as e:
		assert e.code == 0
	finally:
		sys.argv = old_argv
