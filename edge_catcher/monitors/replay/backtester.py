"""Replay backtester — drives a captured bundle through the live dispatch path.

``replay_capture(bundle_path)`` is the single entry point. It:

  1. Loads the bundle's ``manifest.json`` and verifies the schema version.
  2. Loads the bundle's ``strategies_local.py`` via ``importlib.util.spec_from_file_location``
     under a synthetic module name, so it does NOT shadow the dev workspace's
     cached copy in ``sys.modules``. Failing to load raises ``BundleStrategyLoadError``.
  3. Loads the bundle's ``paper-trader.yaml`` (optionally overridden by the caller).
  4. Constructs a fresh ``MarketState`` and seeds it from the PRIOR day's
     ``market_state_at_start.json`` (see ``_resolve_prior_file`` for the
     explicit → sibling → None fallback chain; R2 fetch is MVP-deferred).
  5. Constructs a fresh ``InMemoryTradeStore`` and seeds it from the PRIOR
     day's ``open_trades_at_start.sqlite`` (same resolution chain).
  6. Streams the bundle's JSONL through ``dispatch_message`` using the
     captured ``recv_ts`` as the ``now`` parameter, so every replayed trade
     row carries byte-identical timestamps to the live engine's writes.
  7. Returns a ``ReplayResult`` with the final trade list, processed event
     count, and duration.

See spec §4.6 for the full design.
"""
from __future__ import annotations

import importlib.util
import json
import logging
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import yaml

from edge_catcher.monitors.discovery import get_enabled_strategies
from edge_catcher.monitors.dispatch import dispatch_message
from edge_catcher.monitors.market_state import MarketState, OrderbookSnapshot
from edge_catcher.monitors.replay.loader import read_jsonl_window
from edge_catcher.monitors.strategy_base import PaperStrategy
from edge_catcher.monitors.trade_store import InMemoryTradeStore

log = logging.getLogger(__name__)


class BundleStrategyLoadError(Exception):
	"""Raised when the bundle's strategies_local.py fails to import or
	contains no PaperStrategy subclasses. Explicitly distinct from a missing
	file so the caller can tell "bundle incomplete" from "bundle code broken"."""


@dataclass
class ReplayResult:
	"""Return value of ``replay_capture``.

	``trades`` is the full list of rows produced by the InMemoryTradeStore —
	open, exited, and settled combined. The parity test projects them onto
	a column whitelist and compares against the bundle's day slice.
	"""
	trades: list[dict[str, Any]]
	final_market_state: MarketState
	events_processed: int
	duration_seconds: float
	capture_start_ts: Optional[str] = None
	capture_end_ts: Optional[str] = None
	strategies_loaded: list[str] = field(default_factory=list)
	store: Optional["InMemoryTradeStore"] = None  # NEW


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def replay_capture(
	bundle_path: Path | str,
	*,
	strategies: Optional[list[PaperStrategy]] = None,
	config: Optional[dict] = None,
	ticker_filter: Optional[set[str]] = None,
	prior_bundle: Optional[Path | str] = None,
) -> ReplayResult:
	"""Run the replay backtester against a captured bundle.

	Args:
		bundle_path:   Path to a bundle directory (contains manifest.json + jsonl.zst + etc).
		strategies:    Optional override — if None, loaded from the bundle's
		               strategies_local.py via importlib.
		config:        Optional config override — if None, loaded from bundle.
		ticker_filter: Optional set of tickers to restrict the replay to.
		prior_bundle:  Optional explicit prior-day bundle directory for
		               market_state / open_trades seeding. If None, the
		               sibling directory (``bundle_path.parent / <date-1>``)
		               is used; if that doesn't exist, seeding is skipped.
	"""
	bundle = Path(bundle_path)
	t0 = time.monotonic()

	# 1. Load manifest
	manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
	if manifest.get("schema_version") != 1:
		raise ValueError(f"unsupported manifest schema_version: {manifest.get('schema_version')}")
	capture_date_str = manifest["capture_date"]

	# 3. Load config first so strategy filtering can use it
	if config is None:
		config_path = bundle / "paper-trader.yaml"
		if config_path.exists():
			config = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
		else:
			log.warning("replay_capture: paper-trader.yaml missing from bundle %s", bundle)
			config = {}

	# 2. Load strategies (bundle-supplied unless overridden), then filter by config
	if strategies is None:
		all_bundle_strategies = _load_bundle_strategies(bundle, manifest)
		# Route through the engine's own filter so replay runs EXACTLY the
		# strategies the live trader was running — no more, no less. This
		# also deduplicates classes that share the same `.name`.
		enabled, _rejected_pairs = get_enabled_strategies(config, all_bundle_strategies)
		strategies = enabled
	strategy_names = [s.name for s in strategies]
	log.info("replay_capture: %d strategies active after config filter: %s", len(strategies), strategy_names)

	# 4. Verify engine version matches dev (warning only — doesn't block)
	_check_engine_version(bundle, manifest)

	# 5. Construct MarketState and seed from PRIOR day's bundle
	market_state = MarketState()
	_seed_market_state(market_state, bundle, prior_bundle)

	# 6. Construct InMemoryTradeStore and seed from PRIOR day's open_trades
	#    and strategy_state snapshot.
	store = InMemoryTradeStore()
	_seed_open_trades(store, bundle, prior_bundle)
	_seed_strategy_state(store, bundle, prior_bundle)

	# 7. Build strategy-by-series index (same shape as engine.py run_engine)
	strat_by_series: dict[str, list[PaperStrategy]] = {}
	for strat in strategies:
		series_for_strat = _series_for_strategy(config, strat.name)
		for s in series_for_strat:
			strat_by_series.setdefault(s, []).append(strat)

	# 8. Seed pending_states from strategies' persisted state via the store.
	# After Task 10 of the strategy-state-bundle-gap plan, this projects from
	# the store (which _seed_strategy_state may have populated) so replay
	# starts from the same scratchpad the live engine had at end-of-day.
	# Matches the live startup pattern at engine.py:541-544.
	pending_states: dict[str, dict] = {
		strat.name: store.load_state(strat.name) for strat in strategies
	}
	dirty: set[str] = set()

	# 9. Stream events and dispatch
	jsonl_path = bundle / f"kalshi_engine_{capture_date_str}.jsonl.zst"
	if not jsonl_path.exists():
		# Fallback to uncompressed
		jsonl_path = bundle / f"kalshi_engine_{capture_date_str}.jsonl"
		if not jsonl_path.exists():
			raise FileNotFoundError(f"no JSONL or JSONL.zst in bundle {bundle}")

	events_processed = 0
	first_ts: Optional[str] = None
	last_ts: Optional[str] = None
	for event in read_jsonl_window(jsonl_path, ticker_filter=ticker_filter):
		recv_ts = event.get("recv_ts")
		if not recv_ts:
			# Malformed capture: every non-header event must have recv_ts.
			# Don't silently fall back to datetime.now() — that would produce a
			# naive datetime AND make replay non-deterministic. Skip and warn.
			log.warning("replay_capture: skipping event with missing recv_ts: %r", event)
			continue
		now = datetime.fromisoformat(recv_ts)
		if now.tzinfo is None:
			# Capture should always write timezone-aware ISO strings. If one
			# slipped through, attach UTC explicitly — do NOT let a naive
			# datetime reach record_trade (which raises ValueError per Task 1).
			now = now.replace(tzinfo=timezone.utc)

		if first_ts is None:
			first_ts = recv_ts
		last_ts = recv_ts

		try:
			dispatch_message(
				event=event,
				config=config,
				market_state=market_state,
				store=store,
				strategies=strategies,
				strat_by_series=strat_by_series,
				pending_states=pending_states,
				dirty=dirty,
				now=now,
			)
		except Exception:
			log.exception(
				"replay_capture: dispatch error at recv_seq=%s source=%s",
				event.get("recv_seq"), event.get("source"),
			)
			# Don't halt on dispatch errors — the parity test will surface
			# the resulting row divergence if it matters.
		events_processed += 1

	# End-of-replay: flush pending_states to the store so final state is
	# observable via result.store.load_all_states() and so the strict
	# parity test can compare against the live DB's final strategy_state
	# table. Mirrors the live engine's shutdown behavior where the
	# _state_flusher writes dirty states to the DB on its last tick.
	for strat in strategies:
		state = pending_states.get(strat.name)
		if state:
			store.save_state(strat.name, state)

	return ReplayResult(
		trades=store.all_trades(),
		final_market_state=market_state,
		events_processed=events_processed,
		duration_seconds=time.monotonic() - t0,
		capture_start_ts=first_ts,
		capture_end_ts=last_ts,
		strategies_loaded=strategy_names,
		store=store,
	)


# ---------------------------------------------------------------------------
# Bundle strategy loading
# ---------------------------------------------------------------------------


def _load_bundle_strategies(bundle: Path, manifest: dict) -> list[PaperStrategy]:
	"""Load ``strategies_local.py`` from the bundle via spec_from_file_location.

	This explicitly avoids ``importlib.import_module("edge_catcher.monitors.strategies_local")``
	which would pick up the dev workspace's cached module from ``sys.modules``
	instead of the bundle's copy. Using a synthetic module name based on the
	bundle's engine_commit keeps multiple bundles loadable in the same process.
	"""
	strat_file = bundle / "strategies_local.py"
	if not strat_file.exists():
		raise BundleStrategyLoadError(
			f"strategies_local.py missing from bundle {bundle}"
		)

	commit = manifest.get("engine_commit", "unknown")
	synthetic_name = f"edge_catcher.replay.bundle_{commit}_strategies_local"
	spec = importlib.util.spec_from_file_location(synthetic_name, strat_file)
	if spec is None or spec.loader is None:
		raise BundleStrategyLoadError(f"could not build import spec for {strat_file}")
	module = importlib.util.module_from_spec(spec)
	sys.modules[synthetic_name] = module
	try:
		spec.loader.exec_module(module)
	except Exception as e:
		sys.modules.pop(synthetic_name, None)
		raise BundleStrategyLoadError(
			f"failed to exec bundle strategies from {strat_file}: {e}"
		) from e

	strategies: list[PaperStrategy] = []
	for name in dir(module):
		obj = getattr(module, name)
		if isinstance(obj, type) and issubclass(obj, PaperStrategy) and obj is not PaperStrategy:
			try:
				strategies.append(obj())
			except Exception as e:
				log.warning("replay_capture: could not instantiate %s: %s", name, e)

	if not strategies:
		raise BundleStrategyLoadError(
			f"strategies_local.py in {bundle} contains no PaperStrategy subclasses"
		)
	return strategies


def _series_for_strategy(config: dict, strategy_name: str) -> set[str]:
	"""Mirror of engine._series_for_strategy. Returns the configured series
	for a strategy as a set."""
	scfg = (config.get("strategies", {}) or {}).get(strategy_name, {}) or {}
	return set(scfg.get("series", []))


def _check_engine_version(bundle: Path, manifest: dict) -> None:
	"""Log a warning if the current dev HEAD doesn't match the bundle's
	engine_commit. Non-blocking — replay can proceed against drift, but the
	operator should know."""
	import subprocess
	try:
		dev_commit = subprocess.check_output(
			["git", "rev-parse", "HEAD"],
			text=True,
			stderr=subprocess.DEVNULL,
			cwd=str(Path(__file__).resolve().parent.parent.parent.parent),
		).strip()
	except (subprocess.CalledProcessError, FileNotFoundError):
		log.warning("replay_capture: could not determine dev engine commit")
		return
	bundle_commit = manifest.get("engine_commit", "unknown")
	if bundle_commit != "unknown" and bundle_commit != dev_commit:
		log.warning(
			"replay_capture: engine commit drift — bundle=%s dev=%s (replay may diverge)",
			bundle_commit, dev_commit,
		)


# ---------------------------------------------------------------------------
# Prior-bundle seeding
# ---------------------------------------------------------------------------


def _seed_market_state(market_state: MarketState, bundle: Path, prior_bundle: Optional[Path | str]) -> None:
	"""Seed from the PRIOR day's market_state_at_start.json. See spec §4.6 step 5."""
	snapshot_file = _resolve_prior_file(bundle, prior_bundle, "market_state_at_start.json")
	if snapshot_file is None:
		log.info("replay_capture: no prior bundle snapshot; starting MarketState empty")
		return
	state = json.loads(snapshot_file.read_text(encoding="utf-8"))
	for ticker, ob in state.get("orderbooks", {}).items():
		yes_levels = [(float(p), int(q)) for p, q in ob.get("yes_levels", [])]
		no_levels = [(float(p), int(q)) for p, q in ob.get("no_levels", [])]
		market_state.seed_orderbook(ticker, OrderbookSnapshot(yes_levels, no_levels))
	for ticker, meta in state.get("metadata", {}).items():
		market_state.register_ticker(ticker, meta=meta)


def _seed_open_trades(store: InMemoryTradeStore, bundle: Path, prior_bundle: Optional[Path | str]) -> None:
	"""Seed from the PRIOR day's open_trades_at_start.sqlite. See spec §4.6 step 6."""
	db_file = _resolve_prior_file(bundle, prior_bundle, "open_trades_at_start.sqlite")
	if db_file is None:
		return
	conn = sqlite3.connect(str(db_file))
	conn.row_factory = sqlite3.Row
	try:
		rows = conn.execute(
			"SELECT * FROM paper_trades WHERE status='open'"
		).fetchall()
	finally:
		conn.close()
	store.seed_from_rows([dict(r) for r in rows])


def _seed_strategy_state(
	store: InMemoryTradeStore,
	bundle: Path,
	prior_bundle: Optional[Path | str],
) -> None:
	"""Seed from the PRIOR day's strategy_state_at_start.json. See spec §4.2.

	For each entry in the envelope's ``states`` dict, calls
	``store.save_state(strategy_name, payload)`` which populates the
	InMemoryTradeStore's internal ``_strategy_state`` dict.

	Missing file is a no-op (logs info) — matches older bundles that
	predate this feature. Schema version mismatch raises loudly to prevent
	silent garbage-state replays.
	"""
	snapshot_file = _resolve_prior_file(bundle, prior_bundle, "strategy_state_at_start.json")
	if snapshot_file is None:
		log.info("replay_capture: no prior strategy_state snapshot; starting with empty state")
		return
	envelope = json.loads(snapshot_file.read_text(encoding="utf-8"))
	version = envelope.get("schema_version")
	if version != 1:
		raise ValueError(
			f"strategy_state_at_start.json: unsupported schema_version {version}"
		)
	states = envelope.get("states", {})
	for strategy_name, payload in states.items():
		store.save_state(strategy_name, payload)


def _resolve_prior_file(
	bundle: Path,
	prior_bundle: Optional[Path | str],
	filename: str,
) -> Optional[Path]:
	"""Resolution order (MVP): explicit prior_bundle → sibling dir → None.

	R2 fallback is MVP-deferred — see the plan's followup #13. When both
	local checks miss, log an info message pointing the operator at
	``rclone copy`` for manual fetch if they need a prior-day seed.
	"""
	# 1. Explicit prior bundle
	if prior_bundle is not None:
		p = Path(prior_bundle) / filename
		if p.exists():
			return p

	# 2. Sibling directory (e.g. ../2026-04-13/)
	bundle_date_str = bundle.name
	try:
		bundle_date = date.fromisoformat(bundle_date_str)
		prior_date = bundle_date - timedelta(days=1)
		sibling = bundle.parent / prior_date.isoformat() / filename
		if sibling.exists():
			return sibling
	except ValueError:
		pass

	# 3. R2 fallback — NOT IMPLEMENTED in MVP. See plan followup #13.
	log.info(
		"replay_capture: %s not found via explicit or sibling lookup; "
		"R2 fallback is MVP-deferred. If this causes replay divergence, "
		"fetch the prior bundle manually with: "
		"rclone copy r2:edge-catcher-captures/kalshi/<prior_date>/ <local>",
		filename,
	)
	return None
