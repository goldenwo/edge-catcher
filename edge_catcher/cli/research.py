"""CLI module for the `research` command."""

import sys
from pathlib import Path


def register(subparsers) -> None:
	from edge_catcher.reports import RESEARCH_OUTPUT

	rs = subparsers.add_parser("research", help="Automated hypothesis research across market categories")
	rs.add_argument("--research-db", default="data/research.db", dest="research_db",
	                help="Path to research tracker SQLite DB (default: data/research.db)")
	rs_sub = rs.add_subparsers(dest="research_command")

	rs_run = rs_sub.add_parser("run", help="Run a single hypothesis")
	rs_run.add_argument("--strategy", required=True, help="Strategy name (use backtest --list-strategies)")
	rs_run.add_argument("--series", required=True, help="Series ticker (e.g. SERIES)")
	rs_run.add_argument("--db-path", required=True, dest="db_path", help="Path to database")
	rs_run.add_argument("--start", required=True, help="Start date ISO (e.g. 2025-01-01)")
	rs_run.add_argument("--end", required=True, help="End date ISO (e.g. 2025-12-31)")
	rs_run.add_argument("--fee-pct", type=float, default=1.0, dest="fee_pct",
	                    help="Fee multiplier (default: 1.0)")
	rs_run.add_argument("--force", action="store_true", default=False,
	                    help="Re-run even if already tested (overwrite existing result)")

	rs_sweep = rs_sub.add_parser("sweep", help="Sweep one strategy across all available series/DBs")
	rs_sweep.add_argument("--strategy", required=True, help="Strategy name")
	rs_sweep.add_argument("--fee-pct", type=float, default=1.0, dest="fee_pct")
	rs_sweep.add_argument("--start", default="2025-01-01", help="Start date (default: 2025-01-01)")
	rs_sweep.add_argument("--end", default="2025-12-31", help="End date (default: 2025-12-31)")
	rs_sweep.add_argument("--max-runs", type=int, default=50, dest="max_runs",
	                      help="Maximum hypotheses to run (default: 50)")
	rs_sweep.add_argument("--output", default=None, help="Save report to this base path")
	rs_sweep.add_argument("--force", action="store_true", default=False,
	                      help="Re-run even if already tested (overwrite existing results)")

	rs_sweepall = rs_sub.add_parser("sweep-all", help="Sweep ALL strategies across ALL available data")
	rs_sweepall.add_argument("--fee-pct", type=float, default=1.0, dest="fee_pct")
	rs_sweepall.add_argument("--start", default="2025-01-01")
	rs_sweepall.add_argument("--end", default="2025-12-31")
	rs_sweepall.add_argument("--max-runs", type=int, default=200, dest="max_runs",
	                         help="Total maximum hypotheses to run (default: 200)")
	rs_sweepall.add_argument("--output", default=str(RESEARCH_OUTPUT))
	rs_sweepall.add_argument("--force", action="store_true", default=False,
	                         help="Re-run even if already tested (overwrite existing results)")

	rs_sub.add_parser("status", help="Show what has been tested")

	rs_report = rs_sub.add_parser("report", help="Generate report from tracked results")
	rs_report.add_argument("--output", default=str(RESEARCH_OUTPUT),
	                       help="Output base path (suffixes .json and .md added)")

	rs_loop = rs_sub.add_parser("loop", help="Autonomous research loop: grid sweep + LLM ideation")
	rs_loop.add_argument("--max-runs", type=int, default=0, dest="max_runs",
	                     help="Max total backtests across both phases (default: 0 = unlimited)")
	rs_loop.add_argument("--max-time", type=float, default=None, dest="max_time",
	                     help="Wall-clock timeout in minutes")
	rs_loop.add_argument("--parallel", type=int, default=1,
	                     help="Concurrent backtests (default: 1)")
	rs_loop.add_argument("--fee-pct", type=float, default=1.0, dest="fee_pct")
	# Defaults match `sweep-all` so that TemporalConsistencyGate sees a
	# concrete date range on every hypothesis. Passing None propagates
	# through GridPlanner and causes the gate to query DB ranges per-series,
	# which silently fails with "0 windows possible" on series with < 35
	# days of data. Discovered during Task 5 sweep analysis.
	rs_loop.add_argument("--start", default="2025-01-01", help="Start date ISO (default: 2025-01-01)")
	rs_loop.add_argument("--end", default="2025-12-31", help="End date ISO (default: 2025-12-31)")
	rs_loop.add_argument("--max-llm-calls", type=int, default=10, dest="max_llm_calls",
	                     help="Cap on LLM API calls in ideation phase (default: 10)")
	rs_loop.add_argument("--grid-only", action="store_true", dest="grid_only",
	                     help="Skip LLM phase")
	rs_loop.add_argument("--llm-only", action="store_true", dest="llm_only",
	                     help="Skip grid/expansion, ideate from context + existing results only")
	rs_loop.add_argument("--output", default=None, help="Save report to this base path")
	rs_loop.add_argument("--force", action="store_true", default=False,
	                     help="Re-run even if already tested (overwrite existing results)")
	rs_loop.add_argument("--max-refinements", type=int, default=3, dest="max_refinements",
	                     help="Max refinement iterations per strategy (default: 3)")
	rs_loop.add_argument("--refine-only", action="store_true", dest="refine_only",
	                     help="Skip grid and LLM phases, only refine existing strategies")
	rs_loop.add_argument("--max-stuck-runs", type=int, default=3, dest="max_stuck_runs",
	                     help="Auto-terminate after N stuck runs post budget-shift (0=disable, default: 3)")

	rs_audit = rs_sub.add_parser("audit", help="Query the research audit log")
	rs_audit.add_argument("audit_type", choices=["decisions", "integrity", "trace"],
	                      help="What to query")
	rs_audit.add_argument("--id", default=None, dest="trace_id",
	                      help="Hypothesis ID for trace queries")

	rs_killreg = rs_sub.add_parser("kill-registry", help="Manage the persistent kill registry")
	rs_killreg.add_argument("kill_registry_action", choices=["list", "reset"],
	                        help="Action: list all entries or reset a strategy")
	rs_killreg.add_argument("--strategy", default=None, dest="kill_registry_strategy",
	                        help="Strategy name (required for reset)")

	rs_export = rs_sub.add_parser("export", help="Export promoted results as a zip bundle for Pi handoff")
	rs_export.add_argument("--output-dir", default="exports", dest="export_output_dir",
	                       help="Output directory for zip (default: exports)")

	rs.set_defaults(func=run)


def run(args) -> None:
	"""Dispatch to the appropriate research sub-subcommand."""
	subcmd = getattr(args, 'research_command', None)
	handlers = {
		'run': _run_single,
		'sweep': _run_sweep,
		'sweep-all': _run_sweep_all,
		'status': _run_status,
		'report': _run_report,
		'loop': _run_loop,
		'audit': _run_audit,
		'kill-registry': _run_kill_registry,
		'export': _run_export,
	}
	handler = handlers.get(subcmd)
	if handler:
		handler(args)
	else:
		print("Usage: python -m edge_catcher research "
		      "{run|sweep|sweep-all|status|report|loop|audit|kill-registry|export}")
		sys.exit(1)


def _make_tracker_and_agent(args):
	from edge_catcher.research import ResearchAgent, Tracker
	research_db = getattr(args, 'research_db', 'data/research.db')
	tracker = Tracker(research_db)
	force = getattr(args, 'force', False)
	agent = ResearchAgent(tracker=tracker, force=force)
	return tracker, agent


def _run_single(args) -> None:
	import json as _json
	from edge_catcher.research import Hypothesis, Reporter
	from edge_catcher.research.data_source_config import make_ds
	_, agent = _make_tracker_and_agent(args)
	h = Hypothesis(
		strategy=args.strategy,
		data_sources=make_ds(db=Path(args.db_path).name, series=args.series),
		start_date=args.start,
		end_date=args.end,
		fee_pct=args.fee_pct,
	)
	result = agent.run_hypothesis(h)
	output = Reporter._result_to_dict(result)
	print(_json.dumps(output, indent=2))


def _run_sweep(args) -> None:
	import json as _json
	from edge_catcher.research import Reporter
	_, agent = _make_tracker_and_agent(args)
	results = agent.sweep_all_series(
		strategy=args.strategy,
		fee_pct=args.fee_pct,
		start=args.start,
		end=args.end,
		max_runs=args.max_runs,
	)
	reporter = Reporter()
	report = reporter.generate_report(results)
	if getattr(args, 'output', None):
		reporter.save(report, args.output)
		print(f"Report saved to {args.output}", file=sys.stderr)
	print(_json.dumps(report, indent=2))


def _run_sweep_all(args) -> None:
	import json as _json
	from edge_catcher.research import Reporter
	from edge_catcher.cli.backtest import build_strategy_map
	_, agent = _make_tracker_and_agent(args)
	strategy_map, _ = build_strategy_map()
	all_strategies = [s for s in strategy_map if s not in ('example',)]
	all_results: list = []
	for strategy in all_strategies:
		results = agent.sweep_all_series(
			strategy=strategy,
			fee_pct=args.fee_pct,
			start=args.start,
			end=args.end,
			max_runs=max(1, args.max_runs // len(all_strategies)),
		)
		all_results.extend(results)
	reporter = Reporter()
	report = reporter.generate_report(all_results)
	if getattr(args, 'output', None):
		reporter.save(report, args.output)
		print(f"Report saved to {args.output}", file=sys.stderr)
	print(_json.dumps(report, indent=2))


def _run_status(args) -> None:
	research_db = getattr(args, 'research_db', 'data/research.db')
	from edge_catcher.research import Tracker
	tracker = Tracker(research_db)
	stats = tracker.stats()
	rows = tracker.list_results()
	print(f"\nResearch DB: {research_db}")
	print(f"Total results: {stats['total']}")
	for verdict, count in sorted(stats['by_verdict'].items()):
		print(f"  {verdict}: {count}")
	if rows:
		print(f"\nRecent results (last 10):")
		print(f"  {'Verdict':<10} {'Strategy':<12} {'Series':<20} {'Sharpe':>7} {'WinRate':>8} {'PnL(¢)':>9}")
		print(f"  {'-'*10} {'-'*12} {'-'*20} {'-'*7} {'-'*8} {'-'*9}")
		for row in rows[:10]:
			print(
				f"  {row['verdict']:<10} {row['strategy']:<12} {row['series']:<20} "
				f"{row['sharpe']:>7.2f} {row['win_rate']:>7.1%} {row['net_pnl_cents']:>9.0f}"
			)


def _run_report(args) -> None:
	from edge_catcher.research import Hypothesis, Reporter, Tracker
	from edge_catcher.research.hypothesis import HypothesisResult
	from edge_catcher.research.data_source_config import make_ds
	from edge_catcher.reports import RESEARCH_OUTPUT
	research_db = getattr(args, 'research_db', 'data/research.db')
	tracker = Tracker(research_db)
	rows = tracker.list_results()
	if not rows:
		print("No results in tracker yet. Run some hypotheses first.", file=sys.stderr)
		sys.exit(1)

	results = []
	for row in rows:
		h = Hypothesis(
			id=row['id'],
			strategy=row['strategy'],
			data_sources=make_ds(db=Path(row['db_path']).name, series=row['series']),
			start_date=row['start_date'],
			end_date=row['end_date'],
			fee_pct=row['fee_pct'],
		)
		results.append(HypothesisResult(
			hypothesis=h,
			status=row['status'],
			total_trades=row['total_trades'],
			wins=row['wins'],
			losses=row['losses'],
			win_rate=row['win_rate'],
			net_pnl_cents=row['net_pnl_cents'],
			sharpe=row['sharpe'],
			max_drawdown_pct=row['max_drawdown_pct'],
			fees_paid_cents=row['fees_paid_cents'],
			avg_win_cents=0.0,
			avg_loss_cents=0.0,
			per_strategy={},
			verdict=row['verdict'],
			verdict_reason=row['verdict_reason'],
			raw_json={},
		))

	reporter = Reporter()
	report = reporter.generate_report(results)
	output_path = getattr(args, 'output', None) or str(RESEARCH_OUTPUT)
	reporter.save(report, output_path)
	print(f"Report saved to {output_path}.json and {output_path}.md")


def _run_loop(args) -> None:
	from edge_catcher.research.loop import LoopOrchestrator
	research_db = getattr(args, 'research_db', 'data/research.db')
	force = getattr(args, 'force', False)
	orch = LoopOrchestrator(
		research_db=research_db,
		start_date=args.start,
		end_date=args.end,
		max_runs=args.max_runs,
		max_time_minutes=args.max_time,
		parallel=args.parallel,
		fee_pct=args.fee_pct,
		max_llm_calls=args.max_llm_calls,
		grid_only=args.grid_only,
		llm_only=args.llm_only,
		output_path=args.output,
		force=force,
		max_refinements=args.max_refinements,
		refine_only=args.refine_only,
		max_stuck_runs=args.max_stuck_runs,
	)
	exit_code, results = orch.run()

	verdicts = {}
	for r in results:
		verdicts[r.verdict] = verdicts.get(r.verdict, 0) + 1
	print(f"\nLoop complete: {len(results)} runs")
	for v, c in sorted(verdicts.items()):
		print(f"  {v}: {c}")
	if exit_code == 2:
		print("\nBudget exhausted — run again to continue.")
	if exit_code == 3:
		print("\nLoop terminated: stuck with no progress. Review kill-registry and data sources.")
	sys.exit(exit_code)


def _run_audit(args) -> None:
	from edge_catcher.research.audit import AuditLog
	research_db = getattr(args, 'research_db', 'data/research.db')
	audit_log = AuditLog(research_db)
	audit_type = getattr(args, 'audit_type', None)

	if audit_type == 'decisions':
		for d in audit_log.list_decisions()[:20]:
			print(f"  [{d['created_at']}] model={d['model']} hash={d['prompt_hash'][:12]}...")
	elif audit_type == 'integrity':
		for c in audit_log.list_integrity_checks():
			print(f"  [{c['created_at']}] {c['checkpoint']}: "
			      f"hash={c['result_hash'][:12]}... count={c['result_count']}")
	elif audit_type == 'trace':
		trace_id = getattr(args, 'trace_id', None)
		if not trace_id:
			print("Usage: research audit trace --id <hypothesis-id>")
			sys.exit(1)
		execs = [e for e in audit_log.list_executions() if e['hypothesis_id'] == trace_id]
		if execs:
			for e in execs:
				print(f"  Phase: {e['phase']}, Verdict: {e['verdict']}, "
				      f"Status: {e['status']}, At: {e['completed_at']}")
		else:
			print(f"  No audit records for hypothesis {trace_id}")


def _run_kill_registry(args) -> None:
	from edge_catcher.research import Tracker
	research_db = getattr(args, 'research_db', 'data/research.db')
	tracker = Tracker(research_db)
	action = getattr(args, 'kill_registry_action', None)
	if action == 'list':
		entries = tracker.list_kill_registry()
		if not entries:
			print("Kill registry is empty.")
		else:
			print(f"\nKill Registry ({len(entries)} entries):")
			for e in entries:
				perm = "PERMANENT" if e["permanent"] else "reset"
				print(f"  {e['strategy']:30s} kill_rate={e['kill_rate']:.0%} "
				      f"({e['kill_count']}/{e['series_tested']}) [{perm}] {e['reason_summary']}")
	elif action == 'reset':
		name = getattr(args, 'kill_registry_strategy', None)
		if not name:
			print("Usage: research kill-registry reset --strategy <name>")
			sys.exit(1)
		tracker.reset_kill_registry(name)
		print(f"Reset '{name}' — it can now be re-proposed by the ideator.")


def _run_export(args) -> None:
	from edge_catcher.research.export import ExportCollector
	research_db = getattr(args, 'research_db', 'data/research.db')
	output_dir = getattr(args, 'export_output_dir', 'exports')
	collector = ExportCollector(db_path=research_db)
	bundle = collector.collect()
	strat_count = len(bundle["strategies"])
	result_count = sum(len(s["results"]) for s in bundle["strategies"].values())
	if strat_count == 0:
		print("No promoted/reviewed results to export.")
		return
	zip_path = collector.write_zip(bundle, output_dir=output_dir)
	print(f"Exported {result_count} results across {strat_count} strategies to {zip_path}")
