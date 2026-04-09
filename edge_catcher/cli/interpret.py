"""CLI: interpret command — interpret analysis results in plain English using AI."""

import sys


def _run_interpret(args) -> None:
	from edge_catcher.ai.client import LLMClient, LLMError
	from edge_catcher.ai.interpreter import interpret

	client = LLMClient(provider=args.provider, model=args.model)
	try:
		summary = interpret(args.report, client)
	except LLMError as exc:
		print(f"Error: {exc}", file=sys.stderr)
		sys.exit(1)
	except FileNotFoundError as exc:
		print(f"Error: {exc}", file=sys.stderr)
		sys.exit(1)
	print(summary)


def register(subparsers) -> None:
	from edge_catcher.reports import ANALYSIS_OUTPUT

	p = subparsers.add_parser(
		"interpret",
		help="Interpret analysis results in plain English (requires AI)",
	)
	p.add_argument(
		"report",
		nargs="?",
		default=str(ANALYSIS_OUTPUT),
		help=f"Path to analysis JSON (default: {ANALYSIS_OUTPUT})",
	)
	p.add_argument(
		"--provider",
		choices=["anthropic", "openai", "openrouter"],
		default=None,
	)
	p.add_argument("--model", default=None, help="Override model name")
	p.set_defaults(func=_run_interpret)
