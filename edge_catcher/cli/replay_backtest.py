"""Replay backtest CLI command — replay a captured bundle through the live dispatch path."""

import json
import sys
from pathlib import Path


def run(args) -> None:
	from edge_catcher.monitors.replay.backtester import replay_capture

	bundle = Path(args.bundle)
	if not bundle.exists():
		print(json.dumps({"status": "error", "message": f"bundle not found: {bundle}"}))
		sys.exit(1)

	prior = Path(args.prior) if args.prior else None
	ticker_filter = set(args.series.split(",")) if args.series else None

	result = replay_capture(
		bundle_path=bundle,
		prior_bundle=prior,
		ticker_filter=ticker_filter,
	)

	output = {
		"status": "ok",
		"bundle": str(bundle),
		"events_processed": result.events_processed,
		"duration_seconds": round(result.duration_seconds, 2),
		"capture_window": {
			"start": result.capture_start_ts,
			"end": result.capture_end_ts,
		},
		"strategies": result.strategies_loaded,
		"trades": result.trades,
		"trade_count": len(result.trades),
	}

	if args.output:
		out_path = Path(args.output)
		out_path.parent.mkdir(parents=True, exist_ok=True)
		out_path.write_text(json.dumps(output, indent=2), encoding="utf-8")
		if not args.json:
			print(f"replay complete: {result.events_processed:,} events, "
				  f"{len(result.trades)} trades, {result.duration_seconds:.2f}s")
			print(f"output: {out_path}")
	elif args.json:
		print(json.dumps(output))
	else:
		print(f"replay complete: {result.events_processed:,} events, "
			  f"{len(result.trades)} trades, {result.duration_seconds:.2f}s")
		print(f"strategies: {result.strategies_loaded}")
		print(f"window: {result.capture_start_ts} .. {result.capture_end_ts}")
		for t in result.trades:
			print(f"  {t['strategy']:15s} {t['ticker']:40s} {t['side']:3s} "
				  f"entry={t.get('entry_price', '?'):>3} status={t.get('status', '?')}")


def register(subparsers) -> None:
	p = subparsers.add_parser(
		"replay-backtest",
		help="Replay a captured bundle through the live dispatch path",
	)
	p.add_argument("--bundle", required=True,
	               help="Path to bundle directory (contains manifest.json + jsonl.zst)")
	p.add_argument("--prior", default=None,
	               help="Path to prior-day bundle for state seeding (auto-resolves sibling if omitted)")
	p.add_argument("--series", default=None,
	               help="Comma-separated ticker filter (e.g. KXETH15M-26APR142330-30)")
	p.add_argument("--output", default=None,
	               help="Path to write JSON results (prints to stdout if omitted)")
	p.add_argument("--json", action="store_true", default=False,
	               help="Output only valid JSON to stdout")
	p.set_defaults(func=run)
