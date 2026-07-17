"""Maker paper-vs-replay parity — two drivers, one engine path (SPEC §12.6a/b).

The SAME event stream is driven through (a) the paper engine's dispatch loop
fed in-process and (b) the replay backtester over an on-disk bundle built from
identical events + strategy source. The resting ledgers must be BYTE-IDENTICAL
(canonical JSON), and replaying the bundle twice must be identical
(determinism). This is real two-driver parity, not self-comparison: driver (a)
never touches the bundle machinery, driver (b) never touches the in-process
harness.

client_order_ids carry a uuid4 suffix by design; per the documented contract
(execution._make_client_order_id: "tests that need deterministic IDs mock
uuid.uuid4"), uuid4 is mocked with a counter reset per driver.

Strategy-neutral fixture: generic KXTEST tickers only (SPEC §11 privacy
boundary). Exercises: full fill (through-print), partial-then-expired,
pure expiry, and a would_cross pre-placement skip.
"""
from __future__ import annotations

import importlib.util
import itertools
import json
import uuid as _uuid_module
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml

from edge_catcher.engine.dispatch import dispatch_message, step_resting_orders
from edge_catcher.engine.executors.paper import PaperExecutor
from edge_catcher.engine.market_state import MarketState
from edge_catcher.engine.metrics import Metrics
from edge_catcher.engine.replay import backtester as backtester_mod
from edge_catcher.engine.resting import QueueFillModel, RestingOrderTracker
from edge_catcher.engine.trade_store import InMemoryTradeStore

_T0 = datetime(2026, 7, 16, 12, 0, 0, tzinfo=timezone.utc)
_DAY = "2026-07-16"

_STRATEGY_SOURCE = '''"""Strategy-neutral maker probe for the Phase 2a parity fixture (test-only)."""
from edge_catcher.engine.strategy_base import Signal, Strategy

_REST_PRICE = {"KXTEST-A": 15, "KXTEST-B": 15, "KXTEST-C": 15, "KXTEST-D": 25}
_TTL = {"KXTEST-A": 300, "KXTEST-B": 60, "KXTEST-C": 60, "KXTEST-D": 300}


class MakerProbe(Strategy):
	name = "maker_probe"
	supported_series = ["KXTEST"]
	default_params: dict = {}

	def __init__(self) -> None:
		self._emitted: set[str] = set()

	def on_tick(self, ctx):
		if ctx.ticker in self._emitted or ctx.ticker not in _REST_PRICE:
			return []
		self._emitted.add(ctx.ticker)
		return [Signal(
			action="enter", ticker=ctx.ticker, side="no", series="KXTEST",
			strategy=self.name, reason="parity-fixture",
			entry_price_cents=_REST_PRICE[ctx.ticker],
			stop_loss_distance_cents=5,
			exec_style="maker", rest_ttl_seconds=_TTL[ctx.ticker],
		)]
'''

_CONFIG: dict = {
	"sizing": {"risk_per_trade_cents": 200, "max_slippage_cents": 5,
	           "min_fill": 1, "require_fresh_book": True},
	"execution": {"max_resting_per_strategy": 4},
	"strategies": {"maker_probe": {"enabled": True, "series": ["KXTEST"]}},
}


def _iso(offset_s: float) -> str:
	return datetime.fromtimestamp(_T0.timestamp() + offset_s, tz=timezone.utc).isoformat()


def _trade(ticker: str, offset_s: float, yes_dollars: str, count: str,
           taker_side: str, seq: int) -> dict:
	return {
		"recv_seq": seq, "recv_ts": _iso(offset_s), "source": "ws",
		"payload": {"type": "trade", "msg": {
			"market_ticker": ticker, "yes_price_dollars": yes_dollars,
			"count_fp": count, "taker_side": taker_side,
		}},
	}


def _events() -> list[dict]:
	"""The one canonical event stream both drivers consume.

	Book per ticker: YES bid 0.80 (implied NO ask 20c), NO bid 0.15 depth 7
	(our level's queue). Orders placed on the trigger prints at t0+1
	(risk 200 // 15c = 13 contracts; B/C deadline = t0+61).
	"""
	events: list[dict] = []
	seq = itertools.count(1)
	for suffix in ("A", "B", "C", "D"):
		events.append({
			"recv_seq": next(seq), "recv_ts": _iso(0),
			"source": "synthetic.ticker_discovered",
			"payload": {
				"ticker": f"KXTEST-{suffix}",
				"yes_levels": [[0.80, 10]], "no_levels": [[0.15, 7]],
				"market_metadata": {},
			},
		})
	for suffix in ("A", "B", "C", "D"):   # trigger ticks — strategy emits here
		events.append(_trade(f"KXTEST-{suffix}", 1, "0.50", "1.0", "no", next(seq)))
	# A: through-level print sweeps the full 13.
	events.append(_trade("KXTEST-A", 10, "0.90", "50.0", "yes", next(seq)))
	# B: at-level print, 11 - 7 queue = 4 fill (partial).
	events.append(_trade("KXTEST-B", 12, "0.85", "11.0", "yes", next(seq)))
	# Book move at t0+50 (mid 82 -> 80): AFTER the +30s mark-out due times
	# (t0+40/42) but BEFORE the next event tick. Both drivers must sample the
	# mark-outs at the t0+120 EVENT tick — post-move mid — never at a paper
	# timer tick in between (§5.1 cadence independence).
	for suffix in ("A", "B"):
		events.append({
			"recv_seq": next(seq), "recv_ts": _iso(50),
			"source": "synthetic.rest_orderbook",
			"payload": {"ticker": f"KXTEST-{suffix}",
			            "yes_levels": [[0.70, 10]], "no_levels": [[0.10, 7]]},
		})
	# Final tick well past B/C deadlines -> backdated expiry cancels; also
	# samples the +30s mark-outs for A and B fills.
	events.append(_trade("KXTEST-A", 120, "0.90", "1.0", "yes", next(seq)))
	return events


def _write_bundle(tmp_path: Path) -> Path:
	bundle = tmp_path / _DAY
	bundle.mkdir()
	(bundle / "manifest.json").write_text(json.dumps({
		"schema_version": 2, "exchange": "kalshi", "capture_date": _DAY,
		"engine_commit": "unknown", "engine_dirty": False, "files": [],
	}), encoding="utf-8")
	(bundle / "paper-trader.yaml").write_text(yaml.safe_dump(_CONFIG), encoding="utf-8")
	(bundle / "strategies_local.py").write_text(_STRATEGY_SOURCE, encoding="utf-8")
	(bundle / "resting_orders.json").write_text("[]", encoding="utf-8")
	with open(bundle / f"kalshi_engine_{_DAY}.jsonl", "w", encoding="utf-8") as f:
		for event in _events():
			f.write(json.dumps(event) + "\n")
	return bundle


def _fresh_probe(tmp_path: Path):
	"""Instantiate MakerProbe from the SAME source file the bundle carries —
	one source of truth for both drivers."""
	src = tmp_path / "probe_module.py"
	src.parent.mkdir(parents=True, exist_ok=True)
	src.write_text(_STRATEGY_SOURCE, encoding="utf-8")
	spec = importlib.util.spec_from_file_location("parity_probe_module", src)
	module = importlib.util.module_from_spec(spec)
	spec.loader.exec_module(module)  # type: ignore[union-attr]
	return module.MakerProbe()


def _mock_uuid(monkeypatch) -> None:
	counter = itertools.count()

	class _FakeUUID:
		def __init__(self, n: int) -> None:
			self.hex = f"{n:032d}"

	monkeypatch.setattr(_uuid_module, "uuid4", lambda: _FakeUUID(next(counter)))


def _canonical(ledger) -> str:
	return json.dumps([asdict(r) for r in ledger], sort_keys=True, indent=1)


async def _drive_paper(
	tmp_path: Path, *, timer_ticks: bool = False,
) -> tuple[str, Metrics, InMemoryTradeStore]:
	"""Driver (a): the paper engine's dispatch loop, fed in-process.

	With ``timer_ticks=True``, clock-only steps are interleaved every 5 s
	between events — the same call shape as ``engine._resting_timer_loop`` —
	so the parity assertion also covers the production paper cadence
	(SPEC §5.1: the timer must never change the ledger)."""
	config = json.loads(json.dumps(_CONFIG))          # deep copy
	metrics = Metrics()
	config["_metrics"] = metrics
	market_state = MarketState()
	store = InMemoryTradeStore()
	executor = PaperExecutor(market_state=market_state, config=config)

	def _mid(ticker: str) -> int | None:
		bid = market_state.get_yes_bid(ticker)
		ask = market_state.get_yes_ask(ticker)
		if bid is None or ask is None:
			return None
		return round((bid + ask) / 2)

	tracker = RestingOrderTracker(QueueFillModel(), mid_provider=_mid)
	config["_tracker"] = tracker
	probe = _fresh_probe(tmp_path)
	strategies = [probe]
	strat_by_series = {"KXTEST": strategies}
	pending_states: dict[str, dict] = {probe.name: {}}
	dirty: set[str] = set()
	last_ts: float | None = None
	for event in _events():
		now = datetime.fromisoformat(event["recv_ts"])
		if timer_ticks and last_ts is not None:
			t = last_ts + 5.0
			while t < now.timestamp():
				step_resting_orders(
					config, store, "", [],
					datetime.fromtimestamp(t, tz=timezone.utc),
				)
				t += 5.0
		last_ts = now.timestamp()
		await dispatch_message(
			event=event, config=config, market_state=market_state, store=store,
			strategies=strategies, strat_by_series=strat_by_series,
			pending_states=pending_states, dirty=dirty, executor=executor, now=now,
		)
	assert last_ts is not None
	tracker.censor_open(ts=last_ts)
	return _canonical(tracker.ledger), metrics, store


@pytest.mark.asyncio
async def test_two_driver_parity_byte_identical_ledgers(tmp_path, monkeypatch):
	monkeypatch.setattr(backtester_mod, "_check_engine_version", lambda b, m: None)

	_mock_uuid(monkeypatch)
	paper_ledger, metrics, paper_store = await _drive_paper(tmp_path / "paper")

	bundle = _write_bundle(tmp_path)
	_mock_uuid(monkeypatch)                            # reset the counter
	result = await backtester_mod.replay_capture(bundle)
	replay_ledger = _canonical(result.resting_ledger)

	assert paper_ledger == replay_ledger               # BYTE-identical

	# Ledger content sanity (both drivers, since identical): the four
	# designed outcomes with §11 closure and errored == 0.
	rows = {r["ticker"]: r for r in json.loads(paper_ledger)}
	assert rows["KXTEST-A"]["disposition"] == "filled"
	assert rows["KXTEST-B"]["disposition"] == "partial"
	assert rows["KXTEST-B"]["end_cause"] == "expired"
	assert rows["KXTEST-C"]["disposition"] == "expired"
	assert "KXTEST-D" not in rows                      # would_cross: never placed
	dispositions = [r["disposition"] for r in rows.values()]
	assert len(dispositions) == 3 and "errored" not in dispositions

	# Driver (a) side effects: guard metric + booked trades.
	snap = metrics.snapshot()
	assert snap["maker_placed"] == 3
	assert snap["maker_skip_would_cross"] == 1
	assert snap["maker_filled"] == 1
	assert snap["maker_partial"] == 1
	assert snap["maker_expired"] == 2                  # B remainder + C
	booked = {t["ticker"]: t for t in paper_store.all_trades()}
	assert booked["KXTEST-A"]["fill_size"] == 13
	assert booked["KXTEST-A"]["blended_entry"] == 15
	assert booked["KXTEST-A"]["slippage_cents"] == 0
	assert booked["KXTEST-B"]["fill_size"] == 4        # partial residual stays open
	assert "KXTEST-C" not in booked                    # zero-fill expiry books nothing


@pytest.mark.asyncio
async def test_two_driver_parity_with_paper_timer_ticks(tmp_path, monkeypatch):
	"""SPEC §12.6b + §5.1: the paper engine's 5 s wall-clock timer must never
	change the ledger. The fixture's t0+50 book move sits between the +30 s
	mark-out due times (t0+40/42) and the next event tick (t0+120): a timer
	tick that sampled would record the PRE-move mid while timerless replay
	records the post-move mid — divergent mark_outs. Clock-only steps must
	sample nothing (event ticks only)."""
	monkeypatch.setattr(backtester_mod, "_check_engine_version", lambda b, m: None)

	_mock_uuid(monkeypatch)
	paper_ledger, _, _ = await _drive_paper(tmp_path / "paper", timer_ticks=True)

	bundle = _write_bundle(tmp_path)
	_mock_uuid(monkeypatch)                            # reset the counter
	result = await backtester_mod.replay_capture(bundle)
	assert paper_ledger == _canonical(result.resting_ledger)   # BYTE-identical


@pytest.mark.asyncio
async def test_replay_determinism_same_bundle_twice(tmp_path, monkeypatch):
	monkeypatch.setattr(backtester_mod, "_check_engine_version", lambda b, m: None)
	bundle = _write_bundle(tmp_path)

	_mock_uuid(monkeypatch)
	first = _canonical((await backtester_mod.replay_capture(bundle)).resting_ledger)
	_mock_uuid(monkeypatch)
	second = _canonical((await backtester_mod.replay_capture(bundle)).resting_ledger)
	assert first == second
	assert json.loads(first)                            # non-vacuous: rows exist
