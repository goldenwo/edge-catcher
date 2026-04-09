"""CLI: formalize command — formalize a hypothesis from plain English using AI."""

import sys


def _run_formalize(args) -> None:
	from edge_catcher.ai.client import LLMClient, LLMError
	from edge_catcher.ai.formalizer import formalize

	client = LLMClient(provider=args.provider, model=args.model)
	try:
		result = formalize(args.description, client)
	except LLMError as exc:
		print(f"Error: {exc}", file=sys.stderr)
		sys.exit(1)

	if result.get("error"):
		sys.exit(1)
	print(result["message"])


def register(subparsers) -> None:
	p = subparsers.add_parser(
		"formalize",
		help="Formalize a hypothesis from plain English (requires AI)",
	)
	p.add_argument("description", help="Your hypothesis in plain English")
	p.add_argument(
		"--provider",
		choices=["anthropic", "openai", "openrouter"],
		default=None,
	)
	p.add_argument("--model", default=None, help="Override model name")
	p.set_defaults(func=_run_formalize)
