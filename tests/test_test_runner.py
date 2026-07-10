"""Tests for the generic test runner framework."""

import sqlite3

import pytest
from edge_catcher.research.test_runner import (
	TestResult, TestRunner,
	EDGE_EXISTS, NO_EDGE, INSUFFICIENT_DATA, EDGE_NOT_TRADEABLE,
)


def _make_ohlc_db(tmp_path, candles: list[dict], table: str = "ohlc") -> str:
	"""Create a simple OHLC SQLite DB with timestamp, open, high, low, close, volume columns."""
	db_path = tmp_path / "ohlc.db"
	conn = sqlite3.connect(str(db_path))
	conn.execute(f"""
		CREATE TABLE {table} (
			timestamp TEXT,
			open REAL,
			high REAL,
			low REAL,
			close REAL,
			volume REAL
		)
	""")
	for c in candles:
		conn.execute(
			f"INSERT INTO {table} (timestamp, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?)",
			(c["timestamp"], c["open"], c["high"], c["low"], c["close"], c.get("volume", 0.0)),
		)
	conn.commit()
	conn.close()
	return str(db_path)


def _make_test_db(tmp_path, markets, trades):
	"""Create a test SQLite DB with markets and trades tables."""
	db_path = tmp_path / "test.db"
	conn = sqlite3.connect(str(db_path))
	conn.row_factory = sqlite3.Row
	conn.executescript("""
		CREATE TABLE markets (
			ticker TEXT PRIMARY KEY,
			series_ticker TEXT NOT NULL,
			title TEXT DEFAULT '',
			status TEXT DEFAULT 'settled',
			result TEXT,
			last_price INTEGER DEFAULT 0,
			volume INTEGER DEFAULT 0,
			close_time TEXT,
			open_time TEXT,
			raw_data TEXT DEFAULT '{}'
		);
		CREATE TABLE trades (
			trade_id TEXT PRIMARY KEY,
			ticker TEXT NOT NULL,
			yes_price INTEGER NOT NULL,
			no_price INTEGER NOT NULL,
			count INTEGER DEFAULT 1,
			taker_side TEXT DEFAULT 'yes',
			created_time TEXT NOT NULL,
			raw_data TEXT DEFAULT '{}'
		);
	""")
	for m in markets:
		cols = list(m.keys())
		conn.execute(
			f"INSERT INTO markets ({', '.join(cols)}) VALUES ({', '.join(['?'] * len(cols))})",
			[m[c] for c in cols],
		)
	for t in trades:
		cols = list(t.keys())
		conn.execute(
			f"INSERT INTO trades ({', '.join(cols)}) VALUES ({', '.join(['?'] * len(cols))})",
			[t[c] for c in cols],
		)
	conn.commit()
	return conn


class TestTestResult:
	def test_construction(self):
		r = TestResult(
			verdict=EDGE_EXISTS,
			z_stat=3.5,
			fee_adjusted_edge=0.02,
			detail={"buckets": []},
		)
		assert r.verdict == EDGE_EXISTS
		assert r.z_stat == 3.5

	def test_verdict_constants(self):
		assert EDGE_EXISTS == "EDGE_EXISTS"
		assert NO_EDGE == "NO_EDGE"
		assert INSUFFICIENT_DATA == "INSUFFICIENT_DATA"
		assert EDGE_NOT_TRADEABLE == "EDGE_NOT_TRADEABLE"


class TestTestRunner:
	def test_unknown_test_type_raises(self):
		runner = TestRunner()
		with pytest.raises(ValueError, match="Unknown test type"):
			runner.run("nonexistent_type", None, "SER", {}, {})


class TestPriceBucketBiasTest:
	def test_edge_exists_when_overpriced(self, tmp_path):
		"""Contracts at 50% that settle YES only 30% → significant negative z.

		Prints alternate taker sides so the genuine edge shows on the
		taker-replicable (NO) side too — one-sided prints are the class (b)
		artifact signature and correctly downgrade.
		"""
		markets = []
		trades = []
		for i in range(200):
			ticker = f"M-{i}"
			won = i < 60  # 30% win rate
			markets.append({
				"ticker": ticker, "series_ticker": "SER_A",
				"result": "yes" if won else "no",
				"last_price": 50, "volume": 10,
				"close_time": f"2026-01-{(i % 30) + 1:02d}T12:00:00Z",
				"open_time": f"2026-01-{(i % 30) + 1:02d}T00:00:00Z",
			})
			trades.append({
				"trade_id": f"t-{i}", "ticker": ticker,
				"yes_price": 50, "no_price": 50,
				"taker_side": "yes" if i % 2 == 0 else "no",
				"created_time": f"2026-01-{(i % 30) + 1:02d}T06:00:00Z",
			})
		conn = _make_test_db(tmp_path, markets, trades)
		from edge_catcher.research.test_runner import TestRunner, EDGE_EXISTS
		runner = TestRunner()
		result = runner.run("price_bucket_bias", conn, "SER_A",
			params={
				"buckets": [[0.40, 0.60]],
				"min_n_per_bucket": 10,
				"fee_model": "zero",
			},
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": -1.0},
		)
		conn.close()
		assert result.verdict == EDGE_EXISTS
		assert result.z_stat < -2.0

	def test_no_edge_when_fair(self, tmp_path):
		"""Contracts at 50% that settle YES ~50% → no edge."""
		markets = []
		trades = []
		for i in range(200):
			ticker = f"M-{i}"
			won = i < 100  # 50% win rate
			markets.append({
				"ticker": ticker, "series_ticker": "SER_A",
				"result": "yes" if won else "no",
				"last_price": 50, "volume": 10,
				"close_time": f"2026-01-{(i % 30) + 1:02d}T12:00:00Z",
				"open_time": f"2026-01-{(i % 30) + 1:02d}T00:00:00Z",
			})
			trades.append({
				"trade_id": f"t-{i}", "ticker": ticker,
				"yes_price": 50, "no_price": 50,
				"created_time": f"2026-01-{(i % 30) + 1:02d}T06:00:00Z",
			})
		conn = _make_test_db(tmp_path, markets, trades)
		from edge_catcher.research.test_runner import TestRunner, NO_EDGE
		runner = TestRunner()
		result = runner.run("price_bucket_bias", conn, "SER_A",
			params={
				"buckets": [[0.40, 0.60]],
				"min_n_per_bucket": 10,
				"fee_model": "zero",
			},
			thresholds={"clustered_z_stat": 3.0, "min_fee_adjusted_edge": 0.0},
		)
		conn.close()
		assert result.verdict == NO_EDGE

	def test_insufficient_data(self, tmp_path):
		"""Too few markets → INSUFFICIENT_DATA."""
		conn = _make_test_db(tmp_path,
			markets=[{"ticker": "M-1", "series_ticker": "SER_A", "result": "yes",
					  "last_price": 50, "close_time": "2026-01-01T12:00:00Z",
					  "open_time": "2026-01-01T00:00:00Z"}],
			trades=[{"trade_id": "t-1", "ticker": "M-1", "yes_price": 50, "no_price": 50,
					 "created_time": "2026-01-01T06:00:00Z"}],
		)
		from edge_catcher.research.test_runner import TestRunner, INSUFFICIENT_DATA
		runner = TestRunner()
		result = runner.run("price_bucket_bias", conn, "SER_A",
			params={"buckets": [[0.40, 0.60]], "min_n_per_bucket": 30, "fee_model": "zero"},
			thresholds={"clustered_z_stat": 3.0, "min_fee_adjusted_edge": 0.0},
		)
		conn.close()
		assert result.verdict == INSUFFICIENT_DATA

	def test_short_side_edge_survives_fee_after_magnitude_fix(self, tmp_path):
		"""FIX A3 (magnitude fee): a significant SHORT-side edge that clears the fee
		→ EDGE_EXISTS with a positive fee-adjusted edge.

		45% win rate at 50¢ implied = signed edge −0.05 (overpriced → profit by
		shorting). The tradeable edge is |−0.05| − fee(0.02 at 50¢) = +0.03 > 0.
		BEFORE the magnitude fix, the fee was charged on the SIGNED edge
		(−0.05 − 0.02 = −0.07 < 0), so this genuinely tradeable short-side edge could
		never grade EDGE_EXISTS — the exact bug FIX A3 repairs. Signed edge stays
		negative in the detail for direction. (A proper fees-eat-the-edge dataset,
		where |edge| < fee, is covered by test_edge_not_tradeable_small_edge below.)
		"""
		markets = []
		trades = []
		for i in range(200):
			ticker = f"M-{i}"
			# Short-side mispricing: 45% win rate at 50% implied = −0.05 signed edge
			won = i < 90
			markets.append({
				"ticker": ticker, "series_ticker": "SER_A",
				"result": "yes" if won else "no",
				"last_price": 50, "volume": 10,
				"close_time": f"2026-01-{(i % 30) + 1:02d}T12:00:00Z",
				"open_time": f"2026-01-{(i % 30) + 1:02d}T00:00:00Z",
			})
			trades.append({
				"trade_id": f"t-{i}", "ticker": ticker,
				"yes_price": 50, "no_price": 50,
				"taker_side": "yes" if i % 2 == 0 else "no",
				"created_time": f"2026-01-{(i % 30) + 1:02d}T06:00:00Z",
			})
		conn = _make_test_db(tmp_path, markets, trades)
		from edge_catcher.research.test_runner import TestRunner, EDGE_EXISTS
		runner = TestRunner()
		result = runner.run("price_bucket_bias", conn, "SER_A",
			params={
				"buckets": [[0.40, 0.60]],
				"min_n_per_bucket": 10,
				"fee_model": "standard",
			},
			thresholds={"clustered_z_stat": 1.5, "min_fee_adjusted_edge": 0.0},
		)
		conn.close()
		# |−0.05| − 0.02 = +0.03 clears the fee, and |z| is large → EDGE_EXISTS.
		assert result.verdict == EDGE_EXISTS
		assert result.fee_adjusted_edge == pytest.approx(0.03)
		# Signed edge is retained (negative) in the driver detail for direction.
		assert result.detail["driver_bucket"]["edge"] < 0


class TestLifecycleBiasTest:
	"""Tests for LifecycleBiasTest — market age mispricing detection."""

	def test_edge_exists_when_early_mispriced(self, tmp_path):
		"""Early trades (first 30 min) have 30% win rate at 50% implied; late ~50% → EDGE_EXISTS."""
		from edge_catcher.research.test_runner import TestRunner, EDGE_EXISTS

		markets = []
		trades = []
		# 200 markets, each with:
		#   - several early trades (within 30 min of open) at yes_price=50
		#   - several late trades (after 30 min) at yes_price=50
		# 30% of markets settle YES → strong early mispricing signal
		for i in range(200):
			ticker = f"LC-{i}"
			won = i < 60  # 30% win rate
			day = (i % 28) + 1
			open_time = f"2026-01-{day:02d}T08:00:00Z"
			markets.append({
				"ticker": ticker,
				"series_ticker": "SER_LC",
				"result": "yes" if won else "no",
				"last_price": 50,
				"volume": 20,
				"open_time": open_time,
				"close_time": f"2026-01-{day:02d}T20:00:00Z",
			})
			# 3 early trades within 15 minutes of open (mixed taker sides — the
			# genuine edge must show on the taker-replicable side, class (b)).
			for j in range(3):
				trades.append({
					"trade_id": f"lc-early-{i}-{j}",
					"ticker": ticker,
					"yes_price": 50,
					"no_price": 50,
					"count": 1,
					"taker_side": "yes" if j % 2 == 0 else "no",
					"created_time": f"2026-01-{day:02d}T08:{(j * 5):02d}:00Z",
				})
			# 3 late trades 2 hours after open
			for j in range(3):
				trades.append({
					"trade_id": f"lc-late-{i}-{j}",
					"ticker": ticker,
					"yes_price": 50,
					"no_price": 50,
					"count": 1,
					"created_time": f"2026-01-{day:02d}T10:{(j * 5):02d}:00Z",
				})

		conn = _make_test_db(tmp_path, markets, trades)
		runner = TestRunner()
		result = runner.run(
			"lifecycle_bias", conn, "SER_LC",
			params={
				"lifecycle_window_minutes": 30,
				"buckets": [[0.40, 0.60]],
				"min_n_per_bucket": 10,
				"fee_model": "zero",
			},
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": -1.0},
		)
		conn.close()
		assert result.verdict == EDGE_EXISTS
		assert result.z_stat < -2.0

	def test_no_edge_when_uniform(self, tmp_path):
		"""Both early and late segments have ~50% win rate → NO_EDGE."""
		from edge_catcher.research.test_runner import TestRunner, NO_EDGE

		markets = []
		trades = []
		for i in range(200):
			ticker = f"LC-{i}"
			won = i < 100  # 50% win rate
			day = (i % 28) + 1
			open_time = f"2026-01-{day:02d}T08:00:00Z"
			markets.append({
				"ticker": ticker,
				"series_ticker": "SER_LC2",
				"result": "yes" if won else "no",
				"last_price": 50,
				"volume": 20,
				"open_time": open_time,
				"close_time": f"2026-01-{day:02d}T20:00:00Z",
			})
			for j in range(3):
				trades.append({
					"trade_id": f"lc-early-{i}-{j}",
					"ticker": ticker,
					"yes_price": 50,
					"no_price": 50,
					"count": 1,
					"created_time": f"2026-01-{day:02d}T08:{(j * 5):02d}:00Z",
				})
			for j in range(3):
				trades.append({
					"trade_id": f"lc-late-{i}-{j}",
					"ticker": ticker,
					"yes_price": 50,
					"no_price": 50,
					"count": 1,
					"created_time": f"2026-01-{day:02d}T10:{(j * 5):02d}:00Z",
				})

		conn = _make_test_db(tmp_path, markets, trades)
		runner = TestRunner()
		result = runner.run(
			"lifecycle_bias", conn, "SER_LC2",
			params={
				"lifecycle_window_minutes": 30,
				"buckets": [[0.40, 0.60]],
				"min_n_per_bucket": 10,
				"fee_model": "zero",
			},
			thresholds={"clustered_z_stat": 3.0, "min_fee_adjusted_edge": 0.0},
		)
		conn.close()
		assert result.verdict == NO_EDGE

	def test_insufficient_data(self, tmp_path):
		"""Too few markets → INSUFFICIENT_DATA."""
		from edge_catcher.research.test_runner import TestRunner, INSUFFICIENT_DATA

		markets = [
			{
				"ticker": "LC-1",
				"series_ticker": "SER_LC3",
				"result": "yes",
				"last_price": 50,
				"open_time": "2026-01-01T08:00:00Z",
				"close_time": "2026-01-01T20:00:00Z",
			}
		]
		trades = [
			{
				"trade_id": "lc-t-1",
				"ticker": "LC-1",
				"yes_price": 50,
				"no_price": 50,
				"count": 1,
				"created_time": "2026-01-01T08:10:00Z",
			}
		]

		conn = _make_test_db(tmp_path, markets, trades)
		runner = TestRunner()
		result = runner.run(
			"lifecycle_bias", conn, "SER_LC3",
			params={
				"lifecycle_window_minutes": 30,
				"buckets": [[0.40, 0.60]],
				"min_n_per_bucket": 30,
				"fee_model": "zero",
			},
			thresholds={"clustered_z_stat": 3.0, "min_fee_adjusted_edge": 0.0},
		)
		conn.close()
		assert result.verdict == INSUFFICIENT_DATA


	def test_lifecycle_clusters_by_day_not_ticker(self, tmp_path):
		"""Lifecycle clusters by close_date (day), not ticker — the same fabrication
		vector FIX A1 removed from price_bucket_bias. 6 days × 30 markets → the
		early-segment cluster count is 6, not 180.
		"""
		from edge_catcher.research.test_runner import TestRunner

		markets = []
		trades = []
		wins_by_day = [11, 7, 10, 8, 10, 8]  # ~30% with per-day wobble (finite z)
		mkt = 0
		for d, wd in enumerate(wins_by_day, start=1):
			date = f"2026-04-{d:02d}"
			for k in range(30):
				won = k < wd
				ticker = f"LCD-{mkt}"
				mkt += 1
				markets.append({
					"ticker": ticker, "series_ticker": "SER_LCD",
					"result": "yes" if won else "no",
					"last_price": 50, "volume": 10,
					"open_time": f"{date}T08:00:00Z",
					"close_time": f"{date}T20:00:00Z",
				})
				trades.append({
					"trade_id": f"lcd-e-{ticker}", "ticker": ticker,
					"yes_price": 50, "no_price": 50, "count": 1,
					"created_time": f"{date}T08:10:00Z",
				})
				trades.append({
					"trade_id": f"lcd-l-{ticker}", "ticker": ticker,
					"yes_price": 50, "no_price": 50, "count": 1,
					"created_time": f"{date}T10:00:00Z",
				})
		conn = _make_test_db(tmp_path, markets, trades)
		runner = TestRunner()
		result = runner.run(
			"lifecycle_bias", conn, "SER_LCD",
			params={
				"lifecycle_window_minutes": 30,
				"buckets": [[0.40, 0.60]],
				"min_n_per_bucket": 10,
				"fee_model": "zero",
			},
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": -1.0},
		)
		conn.close()
		b = result.detail["buckets"][0]
		assert b["n_clusters"] == 6

	def test_lifecycle_early_late_split_boundary(self, tmp_path):
		"""Pin the early/late split, including the exact-cutoff boundary (<=).

		Guards the SQL rewrite against SQLite type affinity: an uncast
		strftime('%s', ...) TEXT value compares GREATER than any INTEGER, which would
		silently classify EVERY trade as late (early_n_trades == 0 → permanent
		INSUFFICIENT_DATA). Window = 30 min from open at 08:00 → trades at 08:10 and
		exactly 08:30 are EARLY; 08:50 is LATE.
		"""
		from edge_catcher.research.test_runner import TestRunner

		markets = []
		trades = []
		for i in range(40):
			ticker = f"LCB-{i}"
			won = i % 2 == 0
			day = (i % 5) + 1
			date = f"2026-04-{day:02d}"
			markets.append({
				"ticker": ticker, "series_ticker": "SER_LCB",
				"result": "yes" if won else "no",
				"last_price": 50, "volume": 10,
				"open_time": f"{date}T08:00:00Z",
				"close_time": f"{date}T20:00:00Z",
			})
			trades.append({
				"trade_id": f"lcb-e1-{i}", "ticker": ticker,
				"yes_price": 50, "no_price": 50, "count": 1,
				"created_time": f"{date}T08:10:00Z",
			})
			# Exactly at open + 30 min: inclusive boundary → EARLY.
			trades.append({
				"trade_id": f"lcb-e2-{i}", "ticker": ticker,
				"yes_price": 50, "no_price": 50, "count": 1,
				"created_time": f"{date}T08:30:00Z",
			})
			trades.append({
				"trade_id": f"lcb-l-{i}", "ticker": ticker,
				"yes_price": 50, "no_price": 50, "count": 1,
				"created_time": f"{date}T08:50:00Z",
			})
		conn = _make_test_db(tmp_path, markets, trades)
		runner = TestRunner()
		result = runner.run(
			"lifecycle_bias", conn, "SER_LCB",
			params={
				"lifecycle_window_minutes": 30,
				"buckets": [[0.40, 0.60]],
				"min_n_per_bucket": 10,
				"fee_model": "zero",
			},
			thresholds={"clustered_z_stat": 3.0, "min_fee_adjusted_edge": 0.0},
		)
		conn.close()
		b = result.detail["buckets"][0]
		assert b["early_n_trades"] == 80   # 2 early trades × 40 markets
		assert b["late_n_trades"] == 40    # 1 late trade × 40 markets

	def test_lifecycle_min_clusters_floor(self, tmp_path):
		"""A strong early signal spanning only 2 days must not drive a verdict when
		min_clusters = 3 (thin-day guard, previously missing from lifecycle).

		At min_clusters = 2 the bucket IS evaluated (clears the floor) but the MC
		null gate (class (d)) correctly refuses 2-cluster inference: under a
		discrete null two days tie with sizable probability, producing the
		zero-variance sentinel in the simulated statistic, so no finite z clears
		alpha → NO_EDGE with mc_gate_ok False, not EDGE_EXISTS.
		"""
		from edge_catcher.research.test_runner import TestRunner

		markets = []
		trades = []
		mkt = 0
		for d in (1, 2):
			date = f"2026-04-{d:02d}"
			wd = 3 if d == 1 else 4  # ~10-13% win at 50¢ — strong mispricing
			for k in range(30):
				won = k < wd
				ticker = f"LCF-{mkt}"
				mkt += 1
				markets.append({
					"ticker": ticker, "series_ticker": "SER_LCF",
					"result": "yes" if won else "no",
					"last_price": 50, "volume": 10,
					"open_time": f"{date}T08:00:00Z",
					"close_time": f"{date}T20:00:00Z",
				})
				# One early print per taker side: at k=2 days the side stat has
				# df=1 (Cauchy tails), so each side needs the market set's full
				# power to clear the class (b) gate — a genuine edge prints on both.
				for side in ("yes", "no"):
					trades.append({
						"trade_id": f"lcf-e-{side}-{ticker}", "ticker": ticker,
						"yes_price": 50, "no_price": 50, "count": 1,
						"taker_side": side,
						"created_time": f"{date}T08:10:00Z",
					})
				trades.append({
					"trade_id": f"lcf-l-{ticker}", "ticker": ticker,
					"yes_price": 50, "no_price": 50, "count": 1,
					"created_time": f"{date}T10:00:00Z",
				})
		conn = _make_test_db(tmp_path, markets, trades)
		runner = TestRunner()
		params = {
			"lifecycle_window_minutes": 30,
			"buckets": [[0.40, 0.60]],
			"min_n_per_bucket": 10,
			"fee_model": "zero",
		}
		ok = runner.run(
			"lifecycle_bias", conn, "SER_LCF", params=params,
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": -1.0, "min_clusters": 2},
		)
		floored = runner.run(
			"lifecycle_bias", conn, "SER_LCF", params=params,
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": -1.0, "min_clusters": 3},
		)
		conn.close()
		# min_clusters=2: the bucket clears the floor and is EVALUATED — it fails
		# only the (honest) MC null gate, not the floor.
		assert ok.verdict == NO_EDGE
		b = ok.detail["buckets"][0]
		assert b["n_clusters"] == 2
		assert b["significant"] is True
		assert b["mc_gate_ok"] is False
		# min_clusters=3: the same bucket never enters evaluation at all.
		assert floored.verdict == INSUFFICIENT_DATA

	def test_unknown_lifecycle_segment_raises(self, tmp_path):
		"""An unrecognized segment name must fail loud, not silently return the
		late segment (fail-loud stance, matching _resolve_fee_model)."""
		from edge_catcher.research.test_runner import _per_trade_band_day_stats

		conn = _make_test_db(tmp_path, markets=[], trades=[])
		with pytest.raises(ValueError, match="segment"):
			_per_trade_band_day_stats(
				conn.cursor(), "SER_X", [(40, 60)], lifecycle_segment=("earlly", 30),
			)
		conn.close()

	def test_lifecycle_differential_z_in_detail(self, tmp_path):
		"""The day-clustered differential (early_excess − late_excess over days
		present in BOTH segments) is reported per bucket, so a lifecycle-specific
		effect can be told apart from a static bias.

		Early trades at 50¢ on markets winning ~30% (mispriced −0.2); late trades at
		30¢ (calibrated). Wide bucket captures both. Two extra days carry early-only
		trades — they must be DROPPED from the differential (days in both = 20).
		"""
		from edge_catcher.research.test_runner import TestRunner

		markets = []
		trades = []
		mkt = 0
		wins_by_day = [2, 4] * 10  # 20 days, mean 30% of 10 markets/day
		for d, wd in enumerate(wins_by_day, start=1):
			date = f"2026-04-{d:02d}" if d <= 28 else f"2026-05-{d - 28:02d}"
			for k in range(10):
				won = k < wd
				ticker = f"LDF-{mkt}"
				mkt += 1
				markets.append({
					"ticker": ticker, "series_ticker": "SER_LDF",
					"result": "yes" if won else "no",
					"last_price": 30, "volume": 10,
					"open_time": f"{date}T08:00:00Z",
					"close_time": f"{date}T20:00:00Z",
				})
				trades.append({
					"trade_id": f"ldf-e-{ticker}", "ticker": ticker,
					"yes_price": 50, "no_price": 50, "count": 1,
					"created_time": f"{date}T08:10:00Z",
				})
				trades.append({
					"trade_id": f"ldf-l-{ticker}", "ticker": ticker,
					"yes_price": 30, "no_price": 70, "count": 1,
					"created_time": f"{date}T10:00:00Z",
				})
		# Two extra days with EARLY-only trades (no late segment on those days).
		for d in (29, 30):
			date = f"2026-05-{d - 28:02d}"
			for k in range(10):
				won = k < 3
				ticker = f"LDF-{mkt}"
				mkt += 1
				markets.append({
					"ticker": ticker, "series_ticker": "SER_LDF",
					"result": "yes" if won else "no",
					"last_price": 30, "volume": 10,
					"open_time": f"{date}T08:00:00Z",
					"close_time": f"{date}T20:00:00Z",
				})
				trades.append({
					"trade_id": f"ldf-e-{ticker}", "ticker": ticker,
					"yes_price": 50, "no_price": 50, "count": 1,
					"created_time": f"{date}T08:10:00Z",
				})
		conn = _make_test_db(tmp_path, markets, trades)
		runner = TestRunner()
		result = runner.run(
			"lifecycle_bias", conn, "SER_LDF",
			params={
				"lifecycle_window_minutes": 30,
				"buckets": [[0.20, 0.60]],
				"min_n_per_bucket": 10,
				"fee_model": "zero",
			},
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": -1.0},
		)
		conn.close()
		b = result.detail["buckets"][0]
		# Early mispriced, late calibrated → strongly negative differential.
		assert b["differential_z"] <= -3.0
		# Days present in BOTH segments only: 20, not 22.
		assert b["differential_n_clusters"] == 20
		assert b["n_clusters"] == 22  # early segment spans all 22 days


class TestVolumeMispricingTest:
	"""Tests for VolumeMispricingTest — liquidity-based edge detection.

	The EDGE_EXISTS positive control lives in TestCausalVolumeTerciles
	(test_thin_so_far_mispricing_edge_exists), which pins the causal
	at-trade-time tercile semantics that replaced final-volume membership.
	"""

	def test_no_edge_uniform_across_volumes(self, tmp_path):
		"""All volume levels show ~50% win rate → NO_EDGE."""
		from edge_catcher.research.test_runner import TestRunner, NO_EDGE

		markets = []
		trades = []
		volumes = [1, 10, 100]
		for vi, vol in enumerate(volumes):
			for i in range(90):
				ticker = f"VM-{vi}-{i}"
				won = i < 45  # 50% win rate
				day = (i % 28) + 1
				markets.append({
					"ticker": ticker,
					"series_ticker": "SER_VM2",
					"result": "yes" if won else "no",
					"last_price": 50,
					"volume": vol,
					"close_time": f"2026-01-{day:02d}T12:00:00Z",
					"open_time": f"2026-01-{day:02d}T00:00:00Z",
				})
				trades.append({
					"trade_id": f"vm-{vi}-{i}",
					"ticker": ticker,
					"yes_price": 50,
					"no_price": 50,
					"count": vol,
					"created_time": f"2026-01-{day:02d}T06:00:00Z",
				})

		conn = _make_test_db(tmp_path, markets, trades)
		runner = TestRunner()
		result = runner.run(
			"volume_mispricing", conn, "SER_VM2",
			params={
				"buckets": [[0.40, 0.60]],
				"min_n_per_bucket": 10,
				"fee_model": "zero",
			},
			thresholds={"clustered_z_stat": 3.0, "min_fee_adjusted_edge": 0.0},
		)
		conn.close()
		assert result.verdict == NO_EDGE

	def test_insufficient_data(self, tmp_path):
		"""Too few markets → INSUFFICIENT_DATA."""
		from edge_catcher.research.test_runner import TestRunner, INSUFFICIENT_DATA

		markets = [
			{
				"ticker": "VM-1",
				"series_ticker": "SER_VM3",
				"result": "yes",
				"last_price": 50,
				"volume": 5,
				"close_time": "2026-01-01T12:00:00Z",
				"open_time": "2026-01-01T00:00:00Z",
			}
		]
		trades = [
			{
				"trade_id": "vm-t-1",
				"ticker": "VM-1",
				"yes_price": 50,
				"no_price": 50,
				"count": 1,
				"created_time": "2026-01-01T06:00:00Z",
			}
		]

		conn = _make_test_db(tmp_path, markets, trades)
		runner = TestRunner()
		result = runner.run(
			"volume_mispricing", conn, "SER_VM3",
			params={
				"buckets": [[0.40, 0.60]],
				"min_n_per_bucket": 30,
				"fee_model": "zero",
			},
			thresholds={"clustered_z_stat": 3.0, "min_fee_adjusted_edge": 0.0},
		)
		conn.close()
		assert result.verdict == INSUFFICIENT_DATA

	def test_volume_dual_min_n_floor_requires_markets_too(self, tmp_path):
		"""The low-tercile floor requires BOTH n_trades >= min_n AND n_markets >=
		min_n — two heavily-traded thin markets must not clear a trades-only floor.
		"""
		from edge_catcher.research.test_runner import TestRunner

		markets = []
		trades = []
		# Two LOW-volume markets with 2,500 in-band trades each.
		for d, ticker in ((1, "VDF-A"), (2, "VDF-B")):
			date = f"2026-03-{d:02d}"
			markets.append({
				"ticker": ticker, "series_ticker": "SER_VDF",
				"result": "yes" if d == 1 else "no",
				"last_price": 50, "volume": 1,
				"close_time": f"{date}T12:00:00Z",
				"open_time": f"{date}T00:00:00Z",
			})
			for j in range(2500):
				trades.append({
					"trade_id": f"vdf-{ticker}-{j}", "ticker": ticker,
					"yes_price": 50, "no_price": 50, "count": 1,
					"created_time": f"{date}T06:00:00Z",
				})
		# Medium/high-volume markets (out of the low tercile; 1 in-band trade each).
		for i in range(8):
			ticker = f"VDF-H-{i}"
			day = (i % 4) + 1
			date = f"2026-03-{day:02d}"
			markets.append({
				"ticker": ticker, "series_ticker": "SER_VDF",
				"result": "yes" if i % 2 == 0 else "no",
				"last_price": 50, "volume": 100 + i,
				"close_time": f"{date}T12:00:00Z",
				"open_time": f"{date}T00:00:00Z",
			})
			trades.append({
				"trade_id": f"vdf-h-{i}", "ticker": ticker,
				"yes_price": 50, "no_price": 50, "count": 1,
				"created_time": f"{date}T06:00:00Z",
			})
		conn = _make_test_db(tmp_path, markets, trades)
		runner = TestRunner()
		result = runner.run(
			"volume_mispricing", conn, "SER_VDF",
			params={"buckets": [[0.40, 0.60]], "min_n_per_bucket": 30, "fee_model": "zero"},
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": -1.0},
		)
		conn.close()
		assert result.verdict == INSUFFICIENT_DATA

	def test_low_tercile_trade_price_calibrated_no_edge(self, tmp_path):
		"""#5 for VolumeMispricing: low-volume markets are calibrated at trade price
		even though lifetime VWAP would fabricate an edge (drift poison) → NO_EDGE.
		"""
		markets = []
		trades = []
		# 150 LOW-volume markets traded at 50¢ that win 50% (calibrated).
		for i in range(150):
			ticker = f"VMC-low-{i}"
			won = i < 75
			day = (i % 28) + 1
			markets.append({
				"ticker": ticker, "series_ticker": "SER_VMC",
				"result": "yes" if won else "no",
				"last_price": 99 if won else 1, "volume": 1,
				"close_time": f"2026-01-{day:02d}T12:00:00Z",
				"open_time": f"2026-01-{day:02d}T00:00:00Z",
			})
			trades.append({
				"trade_id": f"vmc-low-entry-{i}", "ticker": ticker,
				"yes_price": 50, "no_price": 50, "count": 1,
				"created_time": f"2026-01-{day:02d}T06:00:00Z",
			})
			# Lifetime-VWAP poison: large late trade drifting to the outcome,
			# outside the [0.40,0.60) band — trade-price method ignores it.
			drift = 99 if won else 1
			trades.append({
				"trade_id": f"vmc-low-drift-{i}", "ticker": ticker,
				"yes_price": drift, "no_price": 100 - drift, "count": 100,
				"created_time": f"2026-01-{day:02d}T11:00:00Z",
			})
		# 150 HIGH-volume markets, also calibrated at 50¢.
		for i in range(150):
			ticker = f"VMC-high-{i}"
			won = i < 75
			day = (i % 28) + 1
			markets.append({
				"ticker": ticker, "series_ticker": "SER_VMC",
				"result": "yes" if won else "no",
				"last_price": 50, "volume": 100,
				"close_time": f"2026-01-{day:02d}T12:00:00Z",
				"open_time": f"2026-01-{day:02d}T00:00:00Z",
			})
			trades.append({
				"trade_id": f"vmc-high-{i}", "ticker": ticker,
				"yes_price": 50, "no_price": 50, "count": 100,
				"created_time": f"2026-01-{day:02d}T06:00:00Z",
			})
		conn = _make_test_db(tmp_path, markets, trades)
		runner = TestRunner()
		result = runner.run(
			"volume_mispricing", conn, "SER_VMC",
			params={
				"buckets": [[0.40, 0.60]],
				"min_n_per_bucket": 10, "fee_model": "zero",
			},
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": -1.0},
		)
		conn.close()
		assert result.verdict == NO_EDGE


class TestCausalVolumeTerciles:
	"""Artifact class (a): final-volume look-ahead.

	VolumeMispricingTest's terciles used final settled m.volume, which is
	outcome-endogenous on in-play venues — winners/losers accumulate different
	volume BY settlement, so conditioning on final volume selects on outcome
	(proven by real adversarial refutations: rebuilding terciles causally
	collapsed verified kills' z-scores to noise, and final volume differed
	strongly by settlement outcome series-wide). Tercile membership must come
	from the volume traded BEFORE each print (running Σ of prior t.count per
	market by created_time) — capture is complete (ΣT.count matches m.volume),
	so this is exact.
	"""

	def test_final_volume_outcome_correlation_no_edge(self, tmp_path):
		"""THE class (a) regression control: a perfectly fair 50¢ series where
		final volume correlates with outcome (winners attract 100 extra late
		prints). Under FINAL-volume terciles the low tercile is exactly the
		losers → in-band win rate 0% at 50¢ → a fabricated edge that every other
		gate PASSES (the selection is real in the selected population — even the
		MC null can't see it). Under causal terciles every market's in-band print
		sits at cum-volume 0 → the thin-so-far cell holds winners AND losers →
		calibrated → NO_EDGE.
		"""
		markets = []
		trades = []
		for i in range(300):
			ticker = f"FV-{i}"
			won = i % 2 == 0  # fair: half win, interleaved across days
			day = (i % 28) + 1
			date = f"2026-01-{day:02d}"
			markets.append({
				"ticker": ticker, "series_ticker": "SER_FV",
				"result": "yes" if won else "no",
				"last_price": 99 if won else 1,
				"volume": 102 if won else 2,  # final volume ← outcome (in-play)
				"close_time": f"{date}T12:00:00Z",
				"open_time": f"{date}T00:00:00Z",
			})
			# The in-band prints: first prints of every market (cum volume 0/1).
			for si, side in enumerate(("yes", "no")):
				trades.append({
					"trade_id": f"fv-{side}-{i}", "ticker": ticker,
					"yes_price": 50, "no_price": 50, "count": 1,
					"taker_side": side,
					"created_time": f"{date}T06:00:{si:02d}Z",
				})
			# Winners then attract heavy in-play flow (out of the analysis band).
			if won:
				for j in range(100):
					trades.append({
						"trade_id": f"fv-drift-{i}-{j}", "ticker": ticker,
						"yes_price": 99, "no_price": 1, "count": 1,
						"created_time": f"{date}T08:{j // 60:02d}:{j % 60:02d}Z",
					})
		conn = _make_test_db(tmp_path, markets, trades)
		result = TestRunner().run(
			"volume_mispricing", conn, "SER_FV",
			params={"buckets": [[0.40, 0.60]], "min_n_per_bucket": 10, "fee_model": "zero"},
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": 0.0},
		)
		conn.close()
		assert result.verdict == NO_EDGE
		assert result.detail["volume_basis"] == "at_trade_cumulative"
		b = result.detail["buckets"][0]
		# The thin-so-far cell contains BOTH outcomes — calibrated, not selected.
		assert b["win_rate"] == pytest.approx(0.50, abs=0.02)

	def test_thin_so_far_mispricing_edge_exists(self, tmp_path):
		"""Positive control for the causal rebuild: prints placed while the market
		is still THIN (cum volume ≤ t1) are mispriced (30% win at 50¢); prints
		arriving after ~200 contracts have traded are calibrated → the low
		(thin-so-far) tercile drives EDGE_EXISTS.
		"""
		markets = []
		trades = []
		# Group T: 150 thin markets — only first prints, 30% win at 50¢.
		for i in range(150):
			ticker = f"TSF-T-{i}"
			won = i < 45
			day = (i % 28) + 1
			date = f"2026-01-{day:02d}"
			markets.append({
				"ticker": ticker, "series_ticker": "SER_TSF",
				"result": "yes" if won else "no",
				"last_price": 50, "volume": 2,
				"close_time": f"{date}T12:00:00Z",
				"open_time": f"{date}T00:00:00Z",
			})
			for si, side in enumerate(("yes", "no")):
				trades.append({
					"trade_id": f"tsf-t-{side}-{i}", "ticker": ticker,
					"yes_price": 50, "no_price": 50, "count": 1,
					"taker_side": side,
					"created_time": f"{date}T06:00:{si:02d}Z",
				})
		# Group D: 150 deep markets — 200 contracts trade first (out of band),
		# then calibrated 50¢ prints (50% win).
		for i in range(150):
			ticker = f"TSF-D-{i}"
			won = i < 75
			day = (i % 28) + 1
			date = f"2026-01-{day:02d}"
			markets.append({
				"ticker": ticker, "series_ticker": "SER_TSF",
				"result": "yes" if won else "no",
				"last_price": 50, "volume": 402,
				"close_time": f"{date}T12:00:00Z",
				"open_time": f"{date}T00:00:00Z",
			})
			for j in range(2):
				trades.append({
					"trade_id": f"tsf-d-pre-{i}-{j}", "ticker": ticker,
					"yes_price": 80, "no_price": 20, "count": 100,
					"created_time": f"{date}T05:0{j}:00Z",
				})
			for si, side in enumerate(("yes", "no")):
				trades.append({
					"trade_id": f"tsf-d-{side}-{i}", "ticker": ticker,
					"yes_price": 50, "no_price": 50, "count": 1,
					"taker_side": side,
					"created_time": f"{date}T06:00:{si:02d}Z",
				})
		conn = _make_test_db(tmp_path, markets, trades)
		result = TestRunner().run(
			"volume_mispricing", conn, "SER_TSF",
			params={"buckets": [[0.40, 0.60]], "min_n_per_bucket": 10, "fee_model": "zero"},
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": -1.0},
		)
		conn.close()
		assert result.verdict == EDGE_EXISTS
		assert result.z_stat < -2.0
		b = result.detail["driver_bucket"]
		# The driver's low tercile holds ONLY the thin-so-far prints.
		assert b["win_rate"] == pytest.approx(0.30, abs=0.02)
		assert b["n_markets"] == 150


class TestPerMarketSensitivity:
	"""Artifact class (f): print-count endogeneity.

	Per-trade weighting lets many-print markets dominate; when print count
	correlates with outcome, the per-trade edge can be fabricated while the
	one-obs-per-market view is calibrated or OPPOSITE (proven by a real
	adversarial refutation: a verified kill had a huge per-trade z while its
	per-market calibration was flat). Every bucket carries a per_market_edge
	sensitivity read; a sign flip (or zero) against the per-trade edge GATES
	the bucket out of EDGE_EXISTS → EDGE_NOT_TRADEABLE. Flag-only proved
	insufficient on real data: the (local) real-data control re-graded a
	hand-killed series EDGE_EXISTS through a sign-flipped bucket that passed
	every other gate.
	"""

	def test_sign_flip_gate_direct_verdict_logic(self):
		"""Direct helper guard: identical strong buckets, only the market-level
		outcome count differs. Sign-agreeing per-market view → EDGE_EXISTS;
		sign-flipped view → EDGE_NOT_TRADEABLE with the flag.
		"""
		from edge_catcher.research.test_runner import _bucket_bonferroni_verdict

		def bucket(market_wins: int) -> dict:
			# A strong per-trade signal over many markets and day-clusters whose
			# market-level outcome count either corroborates or contradicts it.
			return {
				"bucket_lo": 0.20, "bucket_hi": 0.35,
				"z": -5.0, "n_clusters": 90, "fee_adj": 0.05, "edge": -0.07,
				"n_markets": 2000, "mean_price": 0.27, "market_wins": market_wins,
			}

		agreeing = bucket(400)  # per-market win rate 0.20 → edge −0.07, same sign
		res_ok = _bucket_bonferroni_verdict([agreeing], 3.0, 0.0, True)
		assert res_ok[0] == EDGE_EXISTS
		assert agreeing["per_market_sign_flip"] is False

		flipped = bucket(640)  # per-market win rate 0.32 → edge +0.05, OPPOSITE
		res_flip = _bucket_bonferroni_verdict([flipped], 3.0, 0.0, True)
		assert res_flip[0] == EDGE_NOT_TRADEABLE
		assert flipped["per_market_sign_flip"] is True

	def test_print_count_endogeneity_flags_verdict(self, tmp_path):
		"""50 'loud' markets (20 prints each, all lose) + 150 'quiet' markets
		(1 print, 80% win) at 60¢: per-market win rate is exactly 0.60 —
		calibrated — while the per-trade view fabricates a −0.50 edge. The
		sign-flip gate downgrades the verdict and the flag is surfaced.
		"""
		markets = []
		trades = []
		mkt = 0
		for i in range(200):
			loud = i < 50
			won = not loud and (i - 50) < 120  # loud lose; 120/150 quiet win
			day = (i % 20) + 1
			date = f"2026-02-{day:02d}"
			ticker = f"PM-{mkt}"
			mkt += 1
			markets.append({
				"ticker": ticker, "series_ticker": "SER_PM",
				"result": "yes" if won else "no",
				"last_price": 60, "volume": 10,
				"close_time": f"{date}T12:00:00Z",
				"open_time": f"{date}T00:00:00Z",
			})
			n_prints = 20 if loud else 1
			for j in range(n_prints):
				trades.append({
					"trade_id": f"pm-{ticker}-{j}", "ticker": ticker,
					"yes_price": 60, "no_price": 40, "count": 1,
					"taker_side": "yes" if j % 2 == 0 else "no",
					"created_time": f"{date}T06:{j:02d}:00Z",
				})
		conn = _make_test_db(tmp_path, markets, trades)
		result = TestRunner().run(
			"price_bucket_bias", conn, "SER_PM",
			params={"buckets": [[0.50, 0.70]], "min_n_per_bucket": 10, "fee_model": "zero"},
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": -1.0},
		)
		conn.close()
		# The fabricated per-trade edge must not grade EDGE_EXISTS.
		assert result.verdict == EDGE_NOT_TRADEABLE
		b = result.detail["buckets"][0]
		# Per-trade view: fabricated large negative edge (loud losers dominate).
		assert b["edge"] < -0.3
		# Per-market view: calibrated (120 wins / 200 markets at 60¢).
		assert b["per_market_edge"] == pytest.approx(0.0, abs=0.01)
		assert b["per_market_sign_flip"] is True
		# The flag is surfaced at the top level for downstream verifiers.
		assert result.detail["per_market_sign_flip"] is True

	def test_gates_use_per_market_baseline_not_trade_weighted(self):
		"""The (c)/(f) gates compare MARKET-level outcome counts, so their null
		baseline must be the mean of per-market in-band prices — the
		trade-weighted band mean is wrong whenever print count correlates with
		price inside the band, and fabricates spurious sign flips that kill
		genuine edges. Entries carrying "per_market_mean_price" must use it.
		"""
		from edge_catcher.research.test_runner import _bucket_bonferroni_verdict

		def bucket(**extra) -> dict:
			# 100 loud low-priced markets (many prints) + 200 quiet high-priced
			# ones: trade-weighted mean 0.417, honest per-market mean 0.53,
			# uniform genuine -0.03 per-market edge (150/300 win).
			return {
				"bucket_lo": 0.40, "bucket_hi": 0.60,
				"z": -6.0, "n_clusters": 30, "fee_adj": 0.03, "edge": -0.03,
				"n_markets": 300, "mean_price": 0.417, "market_wins": 150,
				**extra,
			}

		honest = bucket(per_market_mean_price=0.53)
		res = _bucket_bonferroni_verdict([honest], 2.0, 0.0, True)
		assert res[0] == EDGE_EXISTS
		assert honest["per_market_edge"] == pytest.approx(-0.03)
		assert honest["per_market_sign_flip"] is False
		# The (c) expectations must use the same per-market baseline.
		assert honest["expected_market_wins"] == pytest.approx(300 * 0.53)

		# Without the per-market baseline the trade-weighted fallback fabricates
		# a +0.083 per-market edge -> spurious flip -> downgrade.
		fallback = bucket()
		res_fb = _bucket_bonferroni_verdict([fallback], 2.0, 0.0, True)
		assert res_fb[0] == EDGE_NOT_TRADEABLE
		assert fallback["per_market_sign_flip"] is True

	def test_price_print_count_correlation_does_not_fabricate_flip(self, tmp_path):
		"""End-to-end: the runner must populate per_market_mean_price from the
		batched per-market scan so a uniform genuine edge with price/print-count
		correlation is NOT flagged. 100 'loud' markets (10 prints each at 41¢,
		38% win) + 200 'quiet' markets (1 print at 59¢, 56% win): every market
		carries the same −0.03 edge; the trade-weighted baseline (0.417) would
		fabricate per_market_edge = +0.083 and a spurious flip.
		"""
		markets = []
		trades = []
		for i in range(100):
			ticker = f"LB-{i}"
			won = i % 50 < 19  # 38/100, spread across days
			day = (i % 25) + 1
			date = f"2026-01-{day:02d}"
			markets.append({
				"ticker": ticker, "series_ticker": "SER_LB",
				"result": "yes" if won else "no",
				"last_price": 41, "volume": 10,
				"close_time": f"{date}T12:00:00Z",
				"open_time": f"{date}T00:00:00Z",
			})
			for j in range(10):
				trades.append({
					"trade_id": f"lb-{ticker}-{j}", "ticker": ticker,
					"yes_price": 41, "no_price": 59, "count": 1,
					"taker_side": "yes" if j % 2 == 0 else "no",
					"created_time": f"{date}T06:{j:02d}:00Z",
				})
		for i in range(200):
			ticker = f"QB-{i}"
			won = i % 25 < 14  # 112/200, spread across days
			day = (i % 25) + 1
			date = f"2026-01-{day:02d}"
			markets.append({
				"ticker": ticker, "series_ticker": "SER_LB",
				"result": "yes" if won else "no",
				"last_price": 59, "volume": 10,
				"close_time": f"{date}T12:00:00Z",
				"open_time": f"{date}T00:00:00Z",
			})
			trades.append({
				"trade_id": f"qb-{i}", "ticker": ticker,
				"yes_price": 59, "no_price": 41, "count": 1,
				"taker_side": "yes" if i % 2 == 0 else "no",
				"created_time": f"{date}T06:00:00Z",
			})
		conn = _make_test_db(tmp_path, markets, trades)
		result = TestRunner().run(
			"price_bucket_bias", conn, "SER_LB",
			params={"buckets": [[0.40, 0.60]], "min_n_per_bucket": 10, "fee_model": "zero"},
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": -1.0},
		)
		conn.close()
		b = result.detail["buckets"][0]
		# The honest per-market baseline: (100·0.41 + 200·0.59) / 300 = 0.53.
		assert b["per_market_mean_price"] == pytest.approx(0.53, abs=0.001)
		# Per-market view agrees with the genuine per-trade direction: no flip.
		assert b["per_market_edge"] == pytest.approx(-0.03, abs=0.001)
		assert b["per_market_sign_flip"] is False

	def test_consistent_views_do_not_flag(self, tmp_path):
		"""When per-trade and per-market views agree (one print per market, a
		genuine +0.20 edge), the sensitivity read matches and nothing is flagged.
		"""
		markets = []
		trades = []
		for i in range(200):
			ticker = f"PC-{i}"
			won = i < 80  # 40% win at 20¢
			day = (i % 28) + 1
			date = f"2026-01-{day:02d}"
			markets.append({
				"ticker": ticker, "series_ticker": "SER_PC",
				"result": "yes" if won else "no",
				"last_price": 20, "volume": 10,
				"close_time": f"{date}T12:00:00Z",
				"open_time": f"{date}T00:00:00Z",
			})
			trades.append({
				"trade_id": f"pc-{i}", "ticker": ticker,
				"yes_price": 20, "no_price": 80, "count": 1,
				"taker_side": "yes" if i % 2 == 0 else "no",
				"created_time": f"{date}T06:00:00Z",
			})
		conn = _make_test_db(tmp_path, markets, trades)
		result = TestRunner().run(
			"price_bucket_bias", conn, "SER_PC",
			params={"buckets": [[0.10, 0.30]], "min_n_per_bucket": 10, "fee_model": "zero"},
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": -1.0},
		)
		conn.close()
		assert result.verdict == EDGE_EXISTS
		b = result.detail["buckets"][0]
		assert b["per_market_edge"] == pytest.approx(b["edge"], abs=0.001)
		assert b["per_market_sign_flip"] is False
		assert result.detail["per_market_sign_flip"] is False


class TestMomentumAlignmentTest:
	"""Tests for MomentumAlignmentTest — spot price momentum vs contract price lag."""

	def test_edge_exists_contracts_lag_spot(self, tmp_path):
		"""OHLC shows upward momentum. Contracts at 50% but settle YES 70% → EDGE_EXISTS."""
		from edge_catcher.research.test_runner import TestRunner, EDGE_EXISTS

		# Build OHLC candles showing strong upward momentum:
		# Each day has candles at 01:00–06:00 with steadily rising close prices.
		# lookback=5: compare candle at 05:xx to candle at 01:xx → positive momentum.
		n_days = 28
		candles = []
		for day in range(1, n_days + 1):
			for hour in range(1, 7):  # hours 01–06
				candles.append({
					"timestamp": f"2026-01-{day:02d}T0{hour}:00:00Z",
					"open": 100.0 + hour * 4,
					"high": 100.0 + hour * 5 + 1,
					"low": 100.0 + hour * 4 - 1,
					"close": 100.0 + hour * 5,
					"volume": 1000.0,
				})

		ohlc_db = _make_ohlc_db(tmp_path, candles, table="ohlc_btc")

		# Build 200 markets: contracts at 50% (VWAP from trades), 70% win rate
		markets = []
		trades = []
		for i in range(200):
			ticker = f"MA-{i}"
			won = i < 140  # 70% win rate
			day = (i % n_days) + 1
			markets.append({
				"ticker": ticker,
				"series_ticker": "SER_MA",
				"result": "yes" if won else "no",
				"last_price": 50,
				"volume": 10,
				"close_time": f"2026-01-{day:02d}T20:00:00Z",
				"open_time": f"2026-01-{day:02d}T00:00:00Z",
			})
			trades.append({
				"trade_id": f"ma-{i}",
				"ticker": ticker,
				"yes_price": 50,
				"no_price": 50,
				"count": 1,
				# Trade happens at 07:00 — after the rising candles at 01–06
				"created_time": f"2026-01-{day:02d}T07:00:00Z",
			})

		conn = _make_test_db(tmp_path, markets, trades)
		runner = TestRunner()
		result = runner.run(
			"momentum_alignment", conn, "SER_MA",
			params={
				"ohlc_config": {"db_path": ohlc_db, "table": "ohlc_btc", "asset": "BTC"},
				"lookback_candles": 5,
				"buckets": [[0.30, 0.70]],
				"min_n_per_bucket": 10,
				"fee_model": "zero",
			},
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": -1.0},
		)
		conn.close()
		assert result.verdict == EDGE_EXISTS
		assert result.z_stat > 2.0  # positive z: win rate > implied in up-momentum regime

	def test_no_edge_when_aligned(self, tmp_path):
		"""OHLC shows mixed momentum, contracts settle roughly at implied → NO_EDGE."""
		from edge_catcher.research.test_runner import TestRunner, NO_EDGE

		# Build OHLC candles with alternating up/down momentum (mixed)
		n_days = 28
		candles = []
		for day in range(1, n_days + 1):
			base = 100.0
			# Even days trend up, odd days trend down
			direction = 1 if day % 2 == 0 else -1
			for hour in range(1, 7):
				candles.append({
					"timestamp": f"2026-01-{day:02d}T0{hour}:00:00Z",
					"open": base + direction * hour * 4,
					"high": base + direction * hour * 5 + 1,
					"low": base + direction * hour * 4 - 1,
					"close": base + direction * hour * 5,
					"volume": 1000.0,
				})

		ohlc_db = _make_ohlc_db(tmp_path, candles, table="ohlc_mixed")

		# 200 markets: ~50% win rate at ~50% implied
		markets = []
		trades = []
		for i in range(200):
			ticker = f"MA2-{i}"
			won = i < 100  # 50% win rate
			day = (i % n_days) + 1
			markets.append({
				"ticker": ticker,
				"series_ticker": "SER_MA2",
				"result": "yes" if won else "no",
				"last_price": 50,
				"volume": 10,
				"close_time": f"2026-01-{day:02d}T20:00:00Z",
				"open_time": f"2026-01-{day:02d}T00:00:00Z",
			})
			trades.append({
				"trade_id": f"ma2-{i}",
				"ticker": ticker,
				"yes_price": 50,
				"no_price": 50,
				"count": 1,
				"created_time": f"2026-01-{day:02d}T07:00:00Z",
			})

		conn = _make_test_db(tmp_path, markets, trades)
		runner = TestRunner()
		result = runner.run(
			"momentum_alignment", conn, "SER_MA2",
			params={
				"ohlc_config": {"db_path": ohlc_db, "table": "ohlc_mixed", "asset": "BTC"},
				"lookback_candles": 5,
				"buckets": [[0.30, 0.70]],
				"min_n_per_bucket": 10,
				"fee_model": "zero",
			},
			thresholds={"clustered_z_stat": 3.0, "min_fee_adjusted_edge": 0.0},
		)
		conn.close()
		assert result.verdict == NO_EDGE

	def test_insufficient_data_no_ohlc(self, tmp_path):
		"""No ohlc_config provided, or DB/table missing → INSUFFICIENT_DATA, no crash."""
		from edge_catcher.research.test_runner import TestRunner, INSUFFICIENT_DATA

		markets = [
			{
				"ticker": "MA3-1",
				"series_ticker": "SER_MA3",
				"result": "yes",
				"last_price": 50,
				"close_time": "2026-01-01T12:00:00Z",
				"open_time": "2026-01-01T00:00:00Z",
			}
		]
		trades = [
			{
				"trade_id": "ma3-t-1",
				"ticker": "MA3-1",
				"yes_price": 50,
				"no_price": 50,
				"count": 1,
				"created_time": "2026-01-01T06:00:00Z",
			}
		]

		conn = _make_test_db(tmp_path, markets, trades)
		runner = TestRunner()

		# Case 1: no ohlc_config at all
		result = runner.run(
			"momentum_alignment", conn, "SER_MA3",
			params={"buckets": [[0.30, 0.70]], "min_n_per_bucket": 5, "fee_model": "zero"},
			thresholds={"clustered_z_stat": 3.0, "min_fee_adjusted_edge": 0.0},
		)
		assert result.verdict == INSUFFICIENT_DATA

		# Case 2: ohlc_config points to non-existent DB
		result2 = runner.run(
			"momentum_alignment", conn, "SER_MA3",
			params={
				"ohlc_config": {"db_path": str(tmp_path / "nonexistent.db"), "table": "ohlc", "asset": "BTC"},
				"buckets": [[0.30, 0.70]],
				"min_n_per_bucket": 5,
				"fee_model": "zero",
			},
			thresholds={"clustered_z_stat": 3.0, "min_fee_adjusted_edge": 0.0},
		)
		assert result2.verdict == INSUFFICIENT_DATA

		# Case 3: DB exists but table is missing
		empty_db = _make_ohlc_db(tmp_path, [], table="some_table")
		result3 = runner.run(
			"momentum_alignment", conn, "SER_MA3",
			params={
				"ohlc_config": {"db_path": empty_db, "table": "nonexistent_table", "asset": "BTC"},
				"buckets": [[0.30, 0.70]],
				"min_n_per_bucket": 5,
				"fee_model": "zero",
			},
			thresholds={"clustered_z_stat": 3.0, "min_fee_adjusted_edge": 0.0},
		)
		assert result3.verdict == INSUFFICIENT_DATA

		conn.close()

	def test_bare_db_path_resolves_under_data_dir(self, tmp_path, monkeypatch):
		"""#2: a bare ohlc db_path ("kalshi-altcrypto.db") that doesn't exist as-is
		resolves to data/<path>. With the file present there, the test proceeds past
		the existence check (no ohlc_db_not_found).
		"""
		# Build the OHLC db UNDER a data/ subdir, named exactly like the bare path.
		data_dir = tmp_path / "data"
		data_dir.mkdir()
		n_days = 28
		candles = []
		for day in range(1, n_days + 1):
			for hour in range(1, 7):
				candles.append({
					"timestamp": f"2026-01-{day:02d}T0{hour}:00:00Z",
					"open": 100.0 + hour * 4, "high": 100.0 + hour * 5 + 1,
					"low": 100.0 + hour * 4 - 1, "close": 100.0 + hour * 5,
					"volume": 1000.0,
				})
		# Write the db at data/kalshi-altcrypto.db (table "ohlc_alt").
		import sqlite3 as _sqlite3
		db_file = data_dir / "kalshi-altcrypto.db"
		oc = _sqlite3.connect(str(db_file))
		oc.execute("CREATE TABLE ohlc_alt (timestamp TEXT, open REAL, high REAL, low REAL, close REAL, volume REAL)")
		for c in candles:
			oc.execute(
				"INSERT INTO ohlc_alt VALUES (?,?,?,?,?,?)",
				(c["timestamp"], c["open"], c["high"], c["low"], c["close"], c["volume"]),
			)
		oc.commit()
		oc.close()

		markets = []
		trades = []
		for i in range(200):
			ticker = f"ALT-{i}"
			won = i < 140  # 70% win rate at 50¢ → up-momentum edge
			day = (i % n_days) + 1
			markets.append({
				"ticker": ticker, "series_ticker": "SER_ALT",
				"result": "yes" if won else "no",
				"last_price": 50, "volume": 10,
				"close_time": f"2026-01-{day:02d}T20:00:00Z",
				"open_time": f"2026-01-{day:02d}T00:00:00Z",
			})
			trades.append({
				"trade_id": f"alt-{i}", "ticker": ticker,
				"yes_price": 50, "no_price": 50, "count": 1,
				"created_time": f"2026-01-{day:02d}T07:00:00Z",
			})
		conn = _make_test_db(tmp_path, markets, trades)

		# Run from tmp_path so "data/kalshi-altcrypto.db" resolves there.
		monkeypatch.chdir(tmp_path)
		runner = TestRunner()
		result = runner.run(
			"momentum_alignment", conn, "SER_ALT",
			params={
				"ohlc_config": {"db_path": "kalshi-altcrypto.db", "table": "ohlc_alt", "asset": "ALT"},
				"lookback_candles": 5,
				"buckets": [[0.30, 0.70]],
				"min_n_per_bucket": 10, "fee_model": "zero",
			},
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": -1.0},
		)
		conn.close()
		# The bare path resolved under data/ — the test got past the existence check.
		assert result.detail.get("reason") != "ohlc_db_not_found"
		# Strengthened: the resolved OHLC data yields the up-momentum edge (70% win at
		# 50¢), proving the run proceeded end-to-end, not merely past the path check.
		assert result.verdict == EDGE_EXISTS

	def test_still_missing_path_reports_resolved_path(self, tmp_path, monkeypatch):
		"""#2: when neither the bare path nor data/<path> exists, stay graceful with
		ohlc_db_not_found and report the RESOLVED (data/-prefixed) path in detail.
		"""
		markets = [{
			"ticker": "ALT2-1", "series_ticker": "SER_ALT2", "result": "yes",
			"last_price": 50, "close_time": "2026-01-01T12:00:00Z",
			"open_time": "2026-01-01T00:00:00Z",
		}]
		trades = [{
			"trade_id": "alt2-t-1", "ticker": "ALT2-1", "yes_price": 50,
			"no_price": 50, "count": 1, "created_time": "2026-01-01T06:00:00Z",
		}]
		conn = _make_test_db(tmp_path, markets, trades)
		monkeypatch.chdir(tmp_path)
		runner = TestRunner()
		result = runner.run(
			"momentum_alignment", conn, "SER_ALT2",
			params={
				"ohlc_config": {"db_path": "no-such.db", "table": "ohlc", "asset": "ALT"},
				"buckets": [[0.30, 0.70]], "min_n_per_bucket": 5, "fee_model": "zero",
			},
			thresholds={"clustered_z_stat": 3.0, "min_fee_adjusted_edge": 0.0},
		)
		conn.close()
		assert result.verdict == INSUFFICIENT_DATA
		assert result.detail["reason"] == "ohlc_db_not_found"
		# Resolved path is the data/-prefixed candidate, not the bare name.
		import os
		assert result.detail["db_path"] == os.path.join("data", "no-such.db")


class TestFeeModelResolution:
	"""Regression tests for fee_model resolution (fail-loud on unknown names).

	Guards against the silent 0.0 fallback: a config passing an unrecognized
	fee_model (e.g. "kalshi") used to run the fee-adjusted edge gate with ZERO
	fees — a false-positive risk where a small raw edge passes a gate that real
	Kalshi fees would have killed.
	"""

	@pytest.mark.parametrize("test_type", [
		"price_bucket_bias",
		"lifecycle_bias",
		"volume_mispricing",
		"momentum_alignment",
	])
	def test_unknown_fee_model_raises(self, tmp_path, test_type):
		"""An unrecognized fee_model name must raise, not silently zero fees.

		Covers every registered test type: fee resolution happens before any DB
		query, so an empty DB still reaches (and trips) the resolver.
		"""
		conn = _make_test_db(tmp_path, markets=[], trades=[])
		runner = TestRunner()
		with pytest.raises(ValueError, match="fee_model"):
			runner.run(
				test_type, conn, "SER_X",
				params={"buckets": [[0.40, 0.60]], "fee_model": "not_a_real_fee_model"},
				thresholds={"clustered_z_stat": 3.0, "min_fee_adjusted_edge": 0.0},
			)
		conn.close()

	def test_kalshi_fee_model_is_applied(self, tmp_path):
		"""fee_model="kalshi" must apply the REAL Kalshi fee curve, not zero/flat.

		Runs the same dataset under "zero" vs "kalshi": kalshi must be lower by
		exactly the exchange's per-contract fee at the implied price
		(ceil(0.07*p*(1-p)*100) cents). A silent 0.0 fallback would make them
		equal; the old flat 0.0175*(1-p) rate would give the wrong magnitude.
		"""
		from edge_catcher.adapters.kalshi.fees import STANDARD_FEE

		markets = []
		trades = []
		for i in range(200):
			ticker = f"FM-{i}"
			won = i < 100  # 50% win rate at implied 0.50
			day = (i % 28) + 1
			markets.append({
				"ticker": ticker, "series_ticker": "SER_FM",
				"result": "yes" if won else "no",
				"last_price": 50, "volume": 10,
				"close_time": f"2026-01-{day:02d}T12:00:00Z",
				"open_time": f"2026-01-{day:02d}T00:00:00Z",
			})
			trades.append({
				"trade_id": f"fm-{i}", "ticker": ticker,
				"yes_price": 50, "no_price": 50,
				"created_time": f"2026-01-{day:02d}T06:00:00Z",
			})
		conn = _make_test_db(tmp_path, markets, trades)
		runner = TestRunner()
		params = {"buckets": [[0.40, 0.60]], "min_n_per_bucket": 10}
		thresholds = {"clustered_z_stat": 3.0, "min_fee_adjusted_edge": 0.0}
		zero = runner.run(
			"price_bucket_bias", conn, "SER_FM",
			params={**params, "fee_model": "zero"}, thresholds=thresholds,
		)
		kalshi = runner.run(
			"price_bucket_bias", conn, "SER_FM",
			params={**params, "fee_model": "kalshi"}, thresholds=thresholds,
		)
		conn.close()

		# Kalshi fees must actually bite (would be equal if "kalshi" → silent 0.0).
		assert kalshi.fee_adjusted_edge < zero.fee_adjusted_edge
		implied = zero.detail["overall_implied"]
		expected_fee = STANDARD_FEE.calculate(round(implied * 100), 1) / 100.0
		assert kalshi.fee_adjusted_edge == pytest.approx(zero.fee_adjusted_edge - expected_fee)

	def test_kalshi_aliases_standard(self):
		"""The "kalshi" rate aliases "standard" — same maker-fee approximation."""
		from edge_catcher.research.test_runner import FEE_MODELS

		assert FEE_MODELS["kalshi"] == FEE_MODELS["standard"]


class TestBucketScaleNormalization:
	"""#1 fail-safe: cents-scale bucket configs are auto-normalized to 0–1.

	The LLM ideator historically emitted cents-scale buckets (e.g. [[1,30]])
	against 0–1 implied data, so `1 <= implied` was unsatisfiable → every bucket
	n=0 → spurious INSUFFICIENT_DATA. The normalizer divides any >1 bound by 100
	and logs a warning, turning a silent total-drop into a corrected run.
	"""

	def _overpriced_30c_db(self, tmp_path, series):
		"""200 markets traded at 30¢ (implied 0.30) that settle YES only 10%.

		Lands in the 0.01–0.30 / 1–30¢ band; strongly overpriced → an edge so the
		verdict is not NO_EDGE, making the cents-vs-0–1 equivalence meaningful.
		"""
		markets = []
		trades = []
		for i in range(200):
			ticker = f"BS-{i}"
			won = i < 20  # 10% win rate at 30¢ implied
			day = (i % 28) + 1
			markets.append({
				"ticker": ticker, "series_ticker": series,
				"result": "yes" if won else "no",
				"last_price": 30, "volume": 10,
				"close_time": f"2026-01-{day:02d}T12:00:00Z",
				"open_time": f"2026-01-{day:02d}T00:00:00Z",
			})
			trades.append({
				"trade_id": f"bs-{i}", "ticker": ticker,
				"yes_price": 25, "no_price": 75, "count": 1,
				"created_time": f"2026-01-{day:02d}T06:00:00Z",
			})
		return _make_test_db(tmp_path, markets, trades)

	def test_cents_scale_buckets_populate_and_warn(self, tmp_path, caplog):
		"""A cents-scale [[1,30]] config populates (not INSUFFICIENT_DATA) + warns."""
		import logging
		conn = self._overpriced_30c_db(tmp_path, "SER_BS1")
		runner = TestRunner()
		with caplog.at_level(logging.WARNING):
			result = runner.run(
				"price_bucket_bias", conn, "SER_BS1",
				params={"buckets": [[1, 30]], "min_n_per_bucket": 10, "fee_model": "zero"},
				thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": -1.0},
			)
		conn.close()
		assert result.verdict != INSUFFICIENT_DATA
		assert any("cents" in rec.message.lower() or "normal" in rec.message.lower()
			for rec in caplog.records), "expected an auto-normalization warning"

	def test_cents_and_unit_configs_agree(self, tmp_path):
		"""[[1,30]] (cents) and [[0.01,0.30]] (0–1) give the same verdict + z."""
		conn = self._overpriced_30c_db(tmp_path, "SER_BS2")
		runner = TestRunner()
		thresholds = {"clustered_z_stat": 2.0, "min_fee_adjusted_edge": -1.0}
		cents = runner.run(
			"price_bucket_bias", conn, "SER_BS2",
			params={"buckets": [[1, 30]], "min_n_per_bucket": 10, "fee_model": "zero"},
			thresholds=thresholds,
		)
		unit = runner.run(
			"price_bucket_bias", conn, "SER_BS2",
			params={"buckets": [[0.01, 0.30]], "min_n_per_bucket": 10, "fee_model": "zero"},
			thresholds=thresholds,
		)
		conn.close()
		assert cents.verdict == unit.verdict
		assert cents.z_stat == pytest.approx(unit.z_stat)

	def test_per_bucket_normalize_leaves_siblings_intact(self):
		"""FIX B2: normalization is PER BUCKET. A mixed [[0.01,0.30],[1,30]] config
		rescales ONLY the cents-scale second bucket; the valid 0–1 first bucket is
		untouched (the old whole-list divide would have corrupted it to [0.0001,0.003]).
		"""
		from edge_catcher.research.test_runner import _normalize_buckets

		result = _normalize_buckets([[0.01, 0.30], [1, 30]])
		assert result == [(0.01, 0.30), (0.01, 0.30)]

	def test_c4_normalizer_divisor_is_100(self, tmp_path):
		"""C4: a cents [[1,30]] config resolves to bounds divided by 100 (not 10).

		Pins the divisor directly against the resolved bucket bounds. Uses two
		distinct trade prices (10¢ and 25¢) — a wrong /10 divisor would resolve the
		band to [0.1, 3.0] and bucket them differently than the correct [0.01, 0.30].
		"""
		markets = []
		trades = []
		for i in range(200):
			ticker = f"C4-{i}"
			# Alternate 10¢ and 25¢ entries; both must land inside [0.01, 0.30).
			price = 10 if i % 2 == 0 else 25
			won = i < 30  # 15% overall win rate → an edge (verdict is not NO_EDGE)
			day = (i % 28) + 1
			markets.append({
				"ticker": ticker, "series_ticker": "SER_C4",
				"result": "yes" if won else "no",
				"last_price": price, "volume": 10,
				"close_time": f"2026-01-{day:02d}T12:00:00Z",
				"open_time": f"2026-01-{day:02d}T00:00:00Z",
			})
			trades.append({
				"trade_id": f"c4-{i}", "ticker": ticker,
				"yes_price": price, "no_price": 100 - price, "count": 1,
				"created_time": f"2026-01-{day:02d}T06:00:00Z",
			})
		conn = _make_test_db(tmp_path, markets, trades)
		runner = TestRunner()
		result = runner.run(
			"price_bucket_bias", conn, "SER_C4",
			params={"buckets": [[1, 30]], "min_n_per_bucket": 10, "fee_model": "zero"},
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": -1.0},
		)
		conn.close()
		assert len(result.detail["buckets"]) == 1
		b = result.detail["buckets"][0]
		assert b["bucket_lo"] == pytest.approx(0.01)
		assert b["bucket_hi"] == pytest.approx(0.30)
		# Both 10¢ and 25¢ markets are inside [0.01,0.30) → all 200 counted.
		assert b["n_markets"] == 200

	def test_non_monotone_bucket_raises(self):
		"""FIX B2: a non-monotone bucket [[0.30, 0.10]] (lo >= hi) fails loud."""
		from edge_catcher.research.test_runner import _normalize_buckets

		with pytest.raises(ValueError, match=r"0.3.*0.1|Invalid bucket"):
			_normalize_buckets([[0.30, 0.10]])

	def test_negative_bucket_raises(self):
		"""FIX B2: a negative-bound bucket [[-0.1, 0.3]] (lo < 0) fails loud."""
		from edge_catcher.research.test_runner import _normalize_buckets

		with pytest.raises(ValueError, match=r"-0.1|Invalid bucket"):
			_normalize_buckets([[-0.1, 0.3]])

	def test_sub_cent_bucket_bound_raises(self):
		"""Kalshi prices are integer cents; a half-cent bound like 0.115 cannot be
		represented in the SQL band filter — float rounding silently empties or
		double-widens adjacent bands (round(0.115*100) == 12 == round(0.125*100)).
		Fail loud instead of silently corrupting the grid.
		"""
		from edge_catcher.research.test_runner import _normalize_buckets

		with pytest.raises(ValueError, match=r"integer.cent|0.115"):
			_normalize_buckets([[0.115, 0.125]])


class TestPriceBucketTradePriceCalibration:
	"""#5: condition on the price each trade was placed at, not lifetime VWAP.

	Lifetime VWAP overshoots toward 0/100 as 15-min markets resolve, fabricating a
	monotonic longshot/favorite edge. The new method buckets by per-trade price and
	clusters by ticker, so each market contributes one calibration residual.
	"""

	def test_calibrated_entry_prices_no_edge(self, tmp_path):
		"""Per-bucket win_rate ≈ entry price even though lifetime VWAP would fabricate
		a monotonic edge (YES drift to 99¢, NO drift to 1¢) → NO_EDGE.
		"""
		markets = []
		trades = []
		# Four analysis bands centered at 0.20/0.40/0.60/0.80; in each, a fraction
		# equal to the centre settles YES, so win_rate ≈ mean entry price.
		band_centres = [20, 40, 60, 80]
		mkt_id = 0
		for centre in band_centres:
			n = 120
			n_yes = round(n * centre / 100.0)
			for k in range(n):
				won = k < n_yes
				ticker = f"CAL-{mkt_id}"
				mkt_id += 1
				day = (k % 28) + 1
				markets.append({
					"ticker": ticker, "series_ticker": "SER_CAL",
					"result": "yes" if won else "no",
					"last_price": 99 if won else 1, "volume": 10,
					"close_time": f"2026-01-{day:02d}T12:00:00Z",
					"open_time": f"2026-01-{day:02d}T00:00:00Z",
				})
				# Calibrated entry trade at the band centre price.
				trades.append({
					"trade_id": f"cal-entry-{ticker}", "ticker": ticker,
					"yes_price": centre, "no_price": 100 - centre, "count": 1,
					"created_time": f"2026-01-{day:02d}T06:00:00Z",
				})
				# Lifetime-VWAP poison: a large late trade drifting to the outcome
				# (99¢ for YES, 1¢ for NO). Falls OUTSIDE every analysis band, so the
				# trade-price method ignores it; a lifetime-VWAP method would not.
				drift = 99 if won else 1
				trades.append({
					"trade_id": f"cal-drift-{ticker}", "ticker": ticker,
					"yes_price": drift, "no_price": 100 - drift, "count": 100,
					"created_time": f"2026-01-{day:02d}T11:00:00Z",
				})
		conn = _make_test_db(tmp_path, markets, trades)
		runner = TestRunner()
		result = runner.run(
			"price_bucket_bias", conn, "SER_CAL",
			params={
				"buckets": [[0.10, 0.30], [0.30, 0.50], [0.50, 0.70], [0.70, 0.90]],
				"min_n_per_bucket": 10, "fee_model": "zero",
			},
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": -1.0},
		)
		conn.close()
		assert result.verdict == NO_EDGE
		# Positively confirm the mechanism (not just an absent verdict): all four
		# bands populate and each is calibrated (win_rate ≈ entry price), proving the
		# count=100 drift trades at 99¢/1¢ were ignored rather than poisoning a VWAP.
		assert len(result.detail["buckets"]) == 4
		for b in result.detail["buckets"]:
			assert abs(b["edge"]) < 0.1

	def test_boundary_cents_included_integer_band_filter(self, tmp_path):
		"""FIX B1: the band filter binds INTEGER cent bounds, so 7¢ markets fall in
		[[0.07, 0.30]]. With float bounds, 0.07*100 == 7.000000000000001 > 7, so the
		`yes_price >= 7.000...1` comparison silently DROPPED every 7¢ market.
		"""
		markets = []
		trades = []
		# 200 markets all traded at exactly 7¢ (the lower boundary of the band).
		for i in range(200):
			ticker = f"BND-{i}"
			won = i < 14  # ~7% win rate → calibrated at 7¢, near-zero edge
			day = (i % 28) + 1
			markets.append({
				"ticker": ticker, "series_ticker": "SER_BND",
				"result": "yes" if won else "no",
				"last_price": 7, "volume": 10,
				"close_time": f"2026-01-{day:02d}T12:00:00Z",
				"open_time": f"2026-01-{day:02d}T00:00:00Z",
			})
			trades.append({
				"trade_id": f"bnd-{i}", "ticker": ticker,
				"yes_price": 7, "no_price": 93, "count": 1,
				"created_time": f"2026-01-{day:02d}T06:00:00Z",
			})
		conn = _make_test_db(tmp_path, markets, trades)
		runner = TestRunner()
		result = runner.run(
			"price_bucket_bias", conn, "SER_BND",
			params={
				"buckets": [[0.07, 0.30]],
				"min_n_per_bucket": 10, "fee_model": "zero",
			},
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": -1.0},
		)
		conn.close()
		# The bucket must populate — the 7¢ markets are in-band under integer bounds.
		# (Before FIX B1 the bucket was empty → INSUFFICIENT_DATA.)
		assert result.verdict != INSUFFICIENT_DATA
		assert len(result.detail["buckets"]) == 1
		assert result.detail["buckets"][0]["n_markets"] == 200

	def test_genuine_trade_price_mispricing_edge_exists(self, tmp_path):
		"""Markets traded at 20¢ that win 40% → +0.20 edge in the [0.10,0.30) bucket."""
		markets = []
		trades = []
		for i in range(200):
			ticker = f"MIS-{i}"
			won = i < 80  # 40% win rate at 20¢ entry
			day = (i % 28) + 1
			markets.append({
				"ticker": ticker, "series_ticker": "SER_MIS",
				"result": "yes" if won else "no",
				"last_price": 20, "volume": 10,
				"close_time": f"2026-01-{day:02d}T12:00:00Z",
				"open_time": f"2026-01-{day:02d}T00:00:00Z",
			})
			trades.append({
				"trade_id": f"mis-{i}", "ticker": ticker,
				"yes_price": 20, "no_price": 80, "count": 1,
				"created_time": f"2026-01-{day:02d}T06:00:00Z",
			})
		conn = _make_test_db(tmp_path, markets, trades)
		runner = TestRunner()
		result = runner.run(
			"price_bucket_bias", conn, "SER_MIS",
			params={
				"buckets": [[0.10, 0.30], [0.30, 0.50]],
				"min_n_per_bucket": 10, "fee_model": "zero",
			},
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": -1.0},
		)
		conn.close()
		assert result.verdict == EDGE_EXISTS
		# The driving bucket is the 0.10–0.30 band; positive edge (win > implied).
		assert result.z_stat > 2.0
		driver = result.detail["driver_bucket"]
		assert driver["bucket_lo"] == pytest.approx(0.10)
		assert driver["bucket_hi"] == pytest.approx(0.30)

	def test_unpooled_opposite_buckets_dont_cancel(self, tmp_path):
		"""A +edge longshot bucket and a −edge favorite bucket → EDGE_EXISTS, not
		cancelled to NO_EDGE by pooling. Verdict named to a single bucket.
		"""
		markets = []
		trades = []
		# Bucket A: trade at 20¢, win 45% → +0.25 edge.
		for i in range(150):
			ticker = f"UP-A-{i}"
			won = i < 68  # ~45%
			day = (i % 28) + 1
			markets.append({
				"ticker": ticker, "series_ticker": "SER_UP",
				"result": "yes" if won else "no",
				"last_price": 20, "volume": 10,
				"close_time": f"2026-01-{day:02d}T12:00:00Z",
				"open_time": f"2026-01-{day:02d}T00:00:00Z",
			})
			trades.append({
				"trade_id": f"up-a-{i}", "ticker": ticker,
				"yes_price": 20, "no_price": 80, "count": 1,
				"created_time": f"2026-01-{day:02d}T06:00:00Z",
			})
		# Bucket B: trade at 80¢, win 55% → −0.25 edge.
		for i in range(150):
			ticker = f"UP-B-{i}"
			won = i < 82  # ~55%
			day = (i % 28) + 1
			markets.append({
				"ticker": ticker, "series_ticker": "SER_UP",
				"result": "yes" if won else "no",
				"last_price": 80, "volume": 10,
				"close_time": f"2026-01-{day:02d}T12:00:00Z",
				"open_time": f"2026-01-{day:02d}T00:00:00Z",
			})
			trades.append({
				"trade_id": f"up-b-{i}", "ticker": ticker,
				"yes_price": 80, "no_price": 20, "count": 1,
				"created_time": f"2026-01-{day:02d}T06:00:00Z",
			})
		conn = _make_test_db(tmp_path, markets, trades)
		runner = TestRunner()
		result = runner.run(
			"price_bucket_bias", conn, "SER_UP",
			params={
				"buckets": [[0.10, 0.30], [0.70, 0.90]],
				"min_n_per_bucket": 10, "fee_model": "zero",
			},
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": -1.0},
		)
		conn.close()
		assert result.verdict == EDGE_EXISTS
		# A pooled aggregate would roughly cancel (+0.25 and −0.25), so EDGE_EXISTS
		# here proves the per-bucket (un-pooled) verdict drove it.
		driver = result.detail["driver_bucket"]
		assert (driver["bucket_lo"], driver["bucket_hi"]) in (
			(pytest.approx(0.10), pytest.approx(0.30)),
			(pytest.approx(0.70), pytest.approx(0.90)),
		)

	def test_clustering_by_day_not_ticker_or_trade(self, tmp_path):
		"""FIX A1: the cluster key is close_date (day) — NOT ticker, NOT trade.

		6 days × 50 markets/day at 50¢, with a real but per-day-noisy mispricing
		(mean win 63%). The effective N is the number of independent DAYS (6), so
		n_clusters == 6 — not 300 (per-ticker) and not 900 (per-trade). Clustering by
		ticker (300 clusters) would understate the SE and report |z| ≈ 4.66; the
		correct day-clustering reports |z| ≈ 3.04. Pinning n_clusters AND the z
		magnitude catches a revert to per-ticker clustering, which is the exact bug
		FIX A1 fixes (15-min markets → ~96 correlated markets/day fabricate edges).

		LOAD-BEARING invariance under per-trade calibration: with 3 trades/market all
		at the SAME price and outcome, each day's excess (wins/n − Σprice/n) is
		unchanged by the 3× row multiplication, so the pinned z survives the
		per-market → per-trade migration exactly. n_trades/n_markets pin the counting
		unit directly.
		"""
		markets = []
		trades = []
		# Per-day win counts vary around 63% so the day-clustered z is finite (not the
		# se==0 degenerate branch) yet distinguishable from the ticker-clustered z.
		wins_by_day = [38, 25, 36, 27, 34, 29]  # mean 31.5/50 = 63%
		mkt = 0
		for d, wd in enumerate(wins_by_day, start=1):
			date = f"2026-01-{d:02d}"
			for k in range(50):
				won = k < wd
				ticker = f"CL-{mkt}"
				mkt += 1
				markets.append({
					"ticker": ticker, "series_ticker": "SER_CL",
					"result": "yes" if won else "no",
					"last_price": 50, "volume": 100,
					"close_time": f"{date}T12:00:00Z",
					"open_time": f"{date}T00:00:00Z",
				})
				# Several trades per market — the cluster key must ignore trade count too.
				for j in range(3):
					trades.append({
						"trade_id": f"cl-{ticker}-{j}", "ticker": ticker,
						"yes_price": 50, "no_price": 50, "count": 1,
						"created_time": f"{date}T06:{j:02d}:00Z",
					})
		conn = _make_test_db(tmp_path, markets, trades)
		runner = TestRunner()
		result = runner.run(
			"price_bucket_bias", conn, "SER_CL",
			params={
				"buckets": [[0.40, 0.60]],
				"min_n_per_bucket": 10, "fee_model": "zero",
			},
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": -1.0},
		)
		conn.close()
		b = result.detail["buckets"][0]
		# Cluster key is the DAY: 6 distinct close_dates → 6 clusters (not 300, not 900).
		assert b["n_clusters"] == 6
		# Per-trade counting unit: 900 trade observations across 300 markets.
		assert b["n_trades"] == 900
		assert b["n_markets"] == 300
		# Day-clustered z ≈ 3.04; ticker-clustering would inflate it to ≈ 4.66.
		assert abs(result.z_stat) == pytest.approx(3.036, abs=0.05)

	def test_c1_edge_not_tradeable_small_edge(self, tmp_path):
		"""C1: a SIGNIFICANT, POSITIVE raw edge SMALLER than the fee → EDGE_NOT_TRADEABLE.

		~10k markets at 50¢ winning 51.6% across 40 days → raw edge +0.016 (positive
		and significant, |z| ≫ threshold), but the real Kalshi fee at 50¢ is 0.02, so
		fee_adj = 0.016 − 0.02 = −0.004 < 0. With min_fee_adjusted_edge = 0.0 this is
		the ONLY branch that yields EDGE_NOT_TRADEABLE (untested before — deleting the
		branch kept every other test green). Per-day win counts alternate 127/131 so
		the clustered z is finite (not the se==0 degenerate branch).
		"""
		markets = []
		trades = []
		n_days = 40
		per_day = 250
		mkt = 0
		for d in range(1, n_days + 1):
			# Distinct calendar dates across Feb + Mar for 40 independent day-clusters.
			date = f"2026-02-{d:02d}" if d <= 28 else f"2026-03-{d - 28:02d}"
			wins_per_day = 127 if d % 2 == 0 else 131  # mean 129/250 = 51.6%
			for k in range(per_day):
				won = k < wins_per_day
				ticker = f"NT-{mkt}"
				mkt += 1
				markets.append({
					"ticker": ticker, "series_ticker": "SER_NT",
					"result": "yes" if won else "no",
					"last_price": 50, "volume": 10,
					"close_time": f"{date}T12:00:00Z",
					"open_time": f"{date}T00:00:00Z",
				})
				trades.append({
					"trade_id": f"nt-{ticker}", "ticker": ticker,
					"yes_price": 50, "no_price": 50, "count": 1,
					"created_time": f"{date}T06:00:00Z",
				})
		conn = _make_test_db(tmp_path, markets, trades)
		runner = TestRunner()
		result = runner.run(
			"price_bucket_bias", conn, "SER_NT",
			params={"buckets": [[0.40, 0.60]], "min_n_per_bucket": 10, "fee_model": "standard"},
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": 0.0},
		)
		conn.close()
		assert result.verdict == EDGE_NOT_TRADEABLE
		assert result.fee_adjusted_edge < 0
		# Positive raw edge, significant z, fee-walled.
		driver = result.detail["driver_bucket"]
		assert driver["edge"] > 0
		assert abs(result.z_stat) >= 2.0

	def test_min_clusters_floor_excludes_thin_day_buckets(self, tmp_path):
		"""FIX A1 floor: a bucket that clears min_n but has fewer than min_clusters
		independent DAYS is not eligible for the verdict (it is noted in the detail).

		2 days × 30 markets at 50¢ that win ~12% → a significant, fee-clearing signal
		with n_clusters = 2. With min_clusters = 2 the bucket is evaluated (clears
		the floor); the class (d) MC null gate then refuses 2-cluster inference
		(discrete null → frequent day ties → sentinel sims), so the honest verdict
		is NO_EDGE with the bucket present and flagged. With min_clusters = 3 the
		same bucket (2 days < 3) is EXCLUDED from bucket_results →
		INSUFFICIENT_DATA, and it appears under cluster_floor_skipped. This is the
		guard against a thin-day bucket driving a verdict, which is exactly how
		intraday correlation fabricates edges.
		"""
		markets = []
		trades = []
		mkt = 0
		for d in (1, 2):
			date = f"2026-01-{d:02d}"
			wd = 3 if d == 1 else 4  # ~10–13% win at 50¢ (slight per-day variation)
			for k in range(30):
				won = k < wd
				ticker = f"MC-{mkt}"
				mkt += 1
				markets.append({
					"ticker": ticker, "series_ticker": "SER_MC",
					"result": "yes" if won else "no",
					"last_price": 50, "volume": 10,
					"close_time": f"{date}T12:00:00Z",
					"open_time": f"{date}T00:00:00Z",
				})
				# Both taker sides per market: at k=2 the side stat has df=1, so
				# each side needs the full market set to clear the class (b) gate.
				for side in ("yes", "no"):
					trades.append({
						"trade_id": f"mc-{side}-{ticker}", "ticker": ticker,
						"yes_price": 50, "no_price": 50, "count": 1,
						"taker_side": side,
						"created_time": f"{date}T06:00:00Z",
					})
		conn = _make_test_db(tmp_path, markets, trades)
		runner = TestRunner()
		# min_clusters = 2 (default): 2 days clears the floor → the bucket is evaluated.
		ok = runner.run(
			"price_bucket_bias", conn, "SER_MC",
			params={"buckets": [[0.40, 0.60]], "min_n_per_bucket": 10, "fee_model": "zero"},
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": -1.0, "min_clusters": 2},
		)
		# min_clusters = 3: 2 days < 3 → the bucket is excluded and noted, not scored.
		floored = runner.run(
			"price_bucket_bias", conn, "SER_MC",
			params={"buckets": [[0.40, 0.60]], "min_n_per_bucket": 10, "fee_model": "zero"},
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": -1.0, "min_clusters": 3},
		)
		conn.close()
		# min_clusters=2: evaluated (floor cleared); the MC null gate is what
		# (honestly) blocks EDGE_EXISTS at 2 clusters.
		assert ok.verdict == NO_EDGE
		assert ok.detail["buckets"][0]["n_clusters"] == 2
		assert ok.detail["buckets"][0]["significant"] is True
		assert ok.detail["buckets"][0]["mc_gate_ok"] is False
		assert floored.verdict == INSUFFICIENT_DATA
		assert floored.detail["reason"] == "no_bucket_met_min_n"
		# The thin-day bucket is recorded (not silently dropped) for transparency.
		assert len(floored.detail["cluster_floor_skipped"]) == 1
		assert floored.detail["cluster_floor_skipped"][0]["n_clusters"] == 2

	def test_fee_floor_clamped_at_zero(self, tmp_path):
		"""FIX A2: EDGE_EXISTS requires a net-positive fee-adjusted edge even when the
		config passes a NEGATIVE min_fee_adjusted_edge. Same dataset as C1 (edge +0.016
		at 50¢, real fee 0.02 → fee_adj −0.004, |z| ≫ threshold) but with
		min_fee_adjusted_edge = −1.0. The tradeability floor is clamped to 0, so the
		fee-walled bucket grades EDGE_NOT_TRADEABLE — NOT EDGE_EXISTS. Without the clamp
		the negative floor would let fee_adj = −0.004 qualify.
		"""
		markets = []
		trades = []
		n_days = 40
		per_day = 250
		mkt = 0
		for d in range(1, n_days + 1):
			date = f"2026-02-{d:02d}" if d <= 28 else f"2026-03-{d - 28:02d}"
			wins_per_day = 127 if d % 2 == 0 else 131
			for k in range(per_day):
				won = k < wins_per_day
				ticker = f"FF-{mkt}"
				mkt += 1
				markets.append({
					"ticker": ticker, "series_ticker": "SER_FF",
					"result": "yes" if won else "no",
					"last_price": 50, "volume": 10,
					"close_time": f"{date}T12:00:00Z",
					"open_time": f"{date}T00:00:00Z",
				})
				trades.append({
					"trade_id": f"ff-{ticker}", "ticker": ticker,
					"yes_price": 50, "no_price": 50, "count": 1,
					"created_time": f"{date}T06:00:00Z",
				})
		conn = _make_test_db(tmp_path, markets, trades)
		runner = TestRunner()
		result = runner.run(
			"price_bucket_bias", conn, "SER_FF",
			params={"buckets": [[0.40, 0.60]], "min_n_per_bucket": 10, "fee_model": "standard"},
			# Negative floor: the clamp must still require fee_adj > 0 for EDGE_EXISTS.
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": -1.0},
		)
		conn.close()
		assert result.verdict == EDGE_NOT_TRADEABLE
		assert result.fee_adjusted_edge < 0

	def test_c2_bonferroni_correction_flips_verdict(self, tmp_path):
		"""C2: pin the Bonferroni correction's real effect. The driver bucket's
		t(9) p-value (z = 2.642 over k = 10 days → p_t ≈ 0.027) sits BETWEEN the
		uncorrected alpha (0.0455 at threshold 2.0) and the K=4 corrected alpha
		(0.0114). Run with all 4 eligible buckets → NO_EDGE (correction bites);
		run that same driver bucket ALONE (K=1) → EDGE_EXISTS. Removing the K
		division would make both runs EDGE_EXISTS.
		"""
		per_day = 400
		driver_price = 25          # implied 0.25
		base = per_day * 25 // 100  # 100 fair wins/day at 25¢
		# Fixed zero-mean noise → real per-day variance so z is finite, not se==0.
		noise = [30, -30, 20, -20, 10, -10, 5, -5, 15, -15]
		surplus = 16               # tuned so driver clustered z == 2.642
		driver_wins = [base + surplus + x for x in noise]
		# 3 fair filler buckets (mean excess 0, same noise → small z, don't qualify).
		filler_specs = [
			(15, [round(per_day * 0.15) + x for x in noise]),
			(45, [round(per_day * 0.45) + x for x in noise]),
			(85, [round(per_day * 0.85) + x for x in noise]),
		]

		def _rows(specs):
			markets = []
			trades = []
			mkt = 0
			for price, wins_by_day in specs:
				for di, w in enumerate(wins_by_day):
					date = f"2026-02-{di + 1:02d}"
					for k in range(per_day):
						won = k < w
						ticker = f"C2-{price}-{mkt}"
						mkt += 1
						markets.append({
							"ticker": ticker, "series_ticker": "SER_C2",
							"result": "yes" if won else "no",
							"last_price": price, "volume": 10,
							"close_time": f"{date}T12:00:00Z",
							"open_time": f"{date}T00:00:00Z",
						})
						trades.append({
							"trade_id": f"c2-{ticker}", "ticker": ticker,
							"yes_price": price, "no_price": 100 - price, "count": 1,
							"created_time": f"{date}T06:00:00Z",
						})
			return markets, trades

		thresholds = {"clustered_z_stat": 2.0, "min_fee_adjusted_edge": -1.0}
		buckets_k4 = [[0.20, 0.30], [0.10, 0.20], [0.40, 0.50], [0.80, 0.90]]

		# Two independent DBs (distinct subdirs — _make_test_db uses a fixed filename).
		dir_k4 = tmp_path / "k4"
		dir_k1 = tmp_path / "k1"
		dir_k4.mkdir()
		dir_k1.mkdir()

		# K=4: all buckets together → corrected alpha 0.0114 < p_t 0.027 → NO_EDGE.
		m, t = _rows([(driver_price, driver_wins), *filler_specs])
		conn = _make_test_db(dir_k4, m, t)
		r4 = TestRunner().run("price_bucket_bias", conn, "SER_C2",
			params={"buckets": buckets_k4, "min_n_per_bucket": 10, "fee_model": "zero"},
			thresholds=thresholds)
		conn.close()

		# K=1: driver bucket alone → uncorrected alpha 0.0455 > p_t 0.027 → EDGE_EXISTS.
		m2, t2 = _rows([(driver_price, driver_wins)])
		conn2 = _make_test_db(dir_k1, m2, t2)
		r1 = TestRunner().run("price_bucket_bias", conn2, "SER_C2",
			params={"buckets": [[0.20, 0.30]], "min_n_per_bucket": 10, "fee_model": "zero"},
			thresholds=thresholds)
		conn2.close()

		assert r4.verdict == NO_EDGE
		assert r1.verdict == EDGE_EXISTS
		# Same driver z in both runs; only the corrected alpha changed.
		assert r1.z_stat == pytest.approx(2.642, abs=0.01)
		assert r4.detail["z_threshold_bonferroni"] == pytest.approx(2.531, abs=0.01)
		assert r4.detail["alpha_bonferroni"] == pytest.approx(0.0455 / 4, abs=0.001)

	def test_c3a_new_method_kills_in_band_vwap_artifact(self, tmp_path):
		"""C3(a): the trade-price method kills a lifetime-VWAP artifact IN A POPULATED
		bucket — it does not merely shuffle the artifact to INSUFFICIENT_DATA.

		Every market enters fair at 60¢ (in [0.50,0.70)). Each winner also drifts to
		80¢ and each loser to 40¢ with a small count, so lifetime VWAP stays inside
		[0.50,0.70) but is pulled toward the outcome (winners → higher implied, losers
		→ lower). The OLD lifetime-VWAP-per-market method would band all 2000 markets
		into [0.50,0.70) and see win_rate 0.60 vs a drift-inflated mean implied 0.616
		→ a fabricated −0.0152 edge with |z| ≈ 4 → EDGE_EXISTS. The NEW trade-price
		method conditions on the in-band 60¢ entries only → win_rate 0.60 at implied
		0.60 → calibrated → NO_EDGE, with the band STILL fully populated (n=2000).
		Per-day win counts wobble around 60% so the calibrated z is genuinely ~0.
		"""
		wins_by_day = [63, 59, 61, 57, 61, 59, 63, 59, 61, 57,
					   61, 59, 63, 59, 61, 57, 61, 59, 63, 59]
		per_day = 100
		markets = []
		trades = []
		mkt = 0
		for di, w in enumerate(wins_by_day):
			date = f"2026-02-{di + 1:02d}"
			for k in range(per_day):
				won = k < w
				ticker = f"C3A-{mkt}"
				mkt += 1
				markets.append({
					"ticker": ticker, "series_ticker": "SER_C3A",
					"result": "yes" if won else "no",
					"last_price": 60, "volume": 10,
					"close_time": f"{date}T12:00:00Z",
					"open_time": f"{date}T00:00:00Z",
				})
				# Fair entry at 60¢ (small count) — the only in-band trade.
				trades.append({
					"trade_id": f"c3a-e-{ticker}", "ticker": ticker,
					"yes_price": 60, "no_price": 40, "count": 3,
					"created_time": f"{date}T06:00:00Z",
				})
				# Drift toward outcome, out of [0.50,0.70) but light enough that the
				# whole-life VWAP stays in-band. Winners → 80¢, losers → 40¢.
				drift = 80 if won else 40
				trades.append({
					"trade_id": f"c3a-d-{ticker}", "ticker": ticker,
					"yes_price": drift, "no_price": 100 - drift, "count": 2,
					"created_time": f"{date}T11:00:00Z",
				})
		conn = _make_test_db(tmp_path, markets, trades)
		runner = TestRunner()
		result = runner.run(
			"price_bucket_bias", conn, "SER_C3A",
			params={"buckets": [[0.50, 0.70]], "min_n_per_bucket": 10, "fee_model": "zero"},
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": -1.0},
		)
		conn.close()
		# Artifact killed, not shuffled: the band is populated AND the verdict is NO_EDGE.
		assert result.verdict == NO_EDGE
		assert len(result.detail["buckets"]) == 1
		b = result.detail["buckets"][0]
		assert b["n_markets"] == 2000                       # band fully populated
		assert b["n_trades"] == 2000                        # one in-band trade ROW each
		assert b["mean_price"] == pytest.approx(0.60, abs=0.01)  # in-band entries only
		assert abs(b["edge"]) < 0.01                        # calibrated

	def test_transit_artifact_graded_no_edge(self, tmp_path):
		"""PER-TRADE control: fair-priced markets that merely TRANSIT a band must not
		fabricate an edge (the false 'favorites-overpriced' signature that survived
		PR #87's per-market-mean-in-band calibration).

		Fixture per day: 10 'resident' markets with 10 trades each at 70¢ (8 settle
		YES, 2 NO) + 20 'transit' markets with exactly 1 trade at 70¢ (4 YES, 16 NO).
		Pooled per-trade calibration is EXACTLY flat: 84 YES-market trades / 120
		trades = 0.70 realized at 0.70 mean price. But one-row-per-market grading sees
		30 markets, mean price 0.70, win rate 12/30 = 0.40 → a fabricated −0.30 gap.

		Three assertions with distinct roles:
		(a) fixture-validity precondition — the per-market gap really is huge
			(computed inline from the fixture, does not exercise production code);
		(b) the REAL mutation guard — the shipped test grades NO_EDGE (a revert to
			one-row-per-market aggregation flips this to EDGE_EXISTS);
		(c) direct aggregation-unit guard — n_trades > n_markets in the detail
			(proves the shipped code counts trades, not markets).
		"""
		markets = []
		trades = []
		n_days = 6
		mkt = 0
		for d in range(1, n_days + 1):
			date = f"2026-03-{d:02d}"
			# 10 residents × 10 trades @70¢; 8 YES / 2 NO.
			for r in range(10):
				won = r < 8
				ticker = f"TA-R-{mkt}"
				mkt += 1
				markets.append({
					"ticker": ticker, "series_ticker": "SER_TA",
					"result": "yes" if won else "no",
					"last_price": 70, "volume": 100,
					"close_time": f"{date}T12:00:00Z",
					"open_time": f"{date}T00:00:00Z",
				})
				for j in range(10):
					trades.append({
						"trade_id": f"ta-r-{ticker}-{j}", "ticker": ticker,
						"yes_price": 70, "no_price": 30, "count": 1,
						"created_time": f"{date}T06:{j:02d}:00Z",
					})
			# 20 transits × 1 trade @70¢; 4 YES / 16 NO (passing through en route to 0).
			for t in range(20):
				won = t < 4
				ticker = f"TA-T-{mkt}"
				mkt += 1
				markets.append({
					"ticker": ticker, "series_ticker": "SER_TA",
					"result": "yes" if won else "no",
					"last_price": 70 if won else 5, "volume": 10,
					"close_time": f"{date}T12:00:00Z",
					"open_time": f"{date}T00:00:00Z",
				})
				trades.append({
					"trade_id": f"ta-t-{ticker}", "ticker": ticker,
					"yes_price": 70, "no_price": 30, "count": 1,
					"created_time": f"{date}T07:00:00Z",
				})

		# (a) Fixture-validity precondition: inline per-market grading on this data
		# shows the fabricated gap (win rate 0.40 vs mean in-band price 0.70).
		in_band_markets = {t["ticker"] for t in trades if 60 <= t["yes_price"] < 80}
		results_by_ticker = {m["ticker"]: m["result"] for m in markets}
		per_market_win = sum(
			1 for tk in in_band_markets if results_by_ticker[tk] == "yes"
		) / len(in_band_markets)
		assert per_market_win == pytest.approx(0.40)
		assert abs(per_market_win - 0.70) > 0.1  # the trap is present in the fixture

		conn = _make_test_db(tmp_path, markets, trades)
		runner = TestRunner()
		result = runner.run(
			"price_bucket_bias", conn, "SER_TA",
			params={"buckets": [[0.60, 0.80]], "min_n_per_bucket": 30, "fee_model": "zero"},
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": -1.0},
		)
		conn.close()
		# (b) The real mutation guard: per-trade calibration is flat → NO_EDGE.
		assert result.verdict == NO_EDGE
		b = result.detail["buckets"][0]
		assert b["mean_price"] == pytest.approx(0.70, abs=0.005)
		assert b["win_rate"] == pytest.approx(0.70, abs=0.005)
		# (c) Aggregation-unit guard: 720 trades across 180 markets.
		assert b["n_trades"] == 720
		assert b["n_markets"] == 180
		assert b["n_trades"] > b["n_markets"]

	def test_void_results_excluded(self, tmp_path):
		"""Only 'yes'/'no' settlements count. A voided market's trades must be
		excluded entirely — counting void as NO fabricates an edge (here a calibrated
		50¢ series would read as 25% realized → spurious EDGE_EXISTS).
		"""
		markets = []
		trades = []
		for i in range(200):
			ticker = f"VD-{i}"
			day = (i % 20) + 1
			date = f"2026-03-{day:02d}"
			if i < 100:
				result = "yes" if i < 50 else "no"  # calibrated 50% at 50¢
			else:
				result = "void"
			markets.append({
				"ticker": ticker, "series_ticker": "SER_VD",
				"result": result,
				"last_price": 50, "volume": 10,
				"close_time": f"{date}T12:00:00Z",
				"open_time": f"{date}T00:00:00Z",
			})
			trades.append({
				"trade_id": f"vd-{i}", "ticker": ticker,
				"yes_price": 50, "no_price": 50, "count": 1,
				"created_time": f"{date}T06:00:00Z",
			})
		conn = _make_test_db(tmp_path, markets, trades)
		runner = TestRunner()
		result = runner.run(
			"price_bucket_bias", conn, "SER_VD",
			params={"buckets": [[0.40, 0.60]], "min_n_per_bucket": 10, "fee_model": "zero"},
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": -1.0},
		)
		conn.close()
		assert result.verdict == NO_EDGE
		b = result.detail["buckets"][0]
		# Void markets' trades are excluded from the observation set entirely.
		assert b["n_trades"] == 100
		assert b["n_markets"] == 100
		assert b["win_rate"] == pytest.approx(0.50)

	def test_zero_and_null_count_trades_excluded(self, tmp_path):
		"""Trades with count <= 0 or NULL are placeholder rows, not prints — they
		must not count as observations (the old count-weighted SQL excluded them
		incidentally; the per-trade rewrite must exclude them deliberately).

		100 calibrated markets at 50c + 60 zero/NULL-count rows at 50c placed only
		on YES-settlers: counted, they'd skew win_rate to ~0.69 and fabricate an
		edge; excluded, the band stays calibrated with n_trades == 100.
		"""
		markets = []
		trades = []
		for i in range(100):
			ticker = f"ZC-{i}"
			won = i < 50
			day = (i % 20) + 1
			date = f"2026-03-{day:02d}"
			markets.append({
				"ticker": ticker, "series_ticker": "SER_ZC",
				"result": "yes" if won else "no",
				"last_price": 50, "volume": 10,
				"close_time": f"{date}T12:00:00Z",
				"open_time": f"{date}T00:00:00Z",
			})
			trades.append({
				"trade_id": f"zc-{i}", "ticker": ticker,
				"yes_price": 50, "no_price": 50, "count": 1,
				"created_time": f"{date}T06:00:00Z",
			})
			if won:
				# Zero-size and NULL-size phantom prints on winners only.
				trades.append({
					"trade_id": f"zc-z-{i}", "ticker": ticker,
					"yes_price": 50, "no_price": 50, "count": 0,
					"created_time": f"{date}T07:00:00Z",
				})
				if i < 10:
					trades.append({
						"trade_id": f"zc-n-{i}", "ticker": ticker,
						"yes_price": 50, "no_price": 50, "count": None,
						"created_time": f"{date}T08:00:00Z",
					})
		conn = _make_test_db(tmp_path, markets, trades)
		runner = TestRunner()
		result = runner.run(
			"price_bucket_bias", conn, "SER_ZC",
			params={"buckets": [[0.40, 0.60]], "min_n_per_bucket": 10, "fee_model": "zero"},
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": -1.0},
		)
		conn.close()
		b = result.detail["buckets"][0]
		assert b["n_trades"] == 100
		assert b["win_rate"] == pytest.approx(0.50)
		assert result.verdict == NO_EDGE

	def test_min_n_zero_does_not_crash_on_empty_band(self, tmp_path):
		"""min_n_per_bucket <= 0 (config-generated) must not ZeroDivisionError on an
		empty band — the floor is clamped to at least 1 observation.
		"""
		markets = [{
			"ticker": "MN-1", "series_ticker": "SER_MN", "result": "yes",
			"last_price": 90, "volume": 10,
			"close_time": "2026-03-01T12:00:00Z", "open_time": "2026-03-01T00:00:00Z",
		}]
		trades = [{
			"trade_id": "mn-1", "ticker": "MN-1", "yes_price": 90, "no_price": 10,
			"count": 1, "created_time": "2026-03-01T06:00:00Z",
		}]
		conn = _make_test_db(tmp_path, markets, trades)
		runner = TestRunner()
		# The [0.40, 0.60) band is empty (only trade is at 90c); min_n=0 must not crash.
		result = runner.run(
			"price_bucket_bias", conn, "SER_MN",
			params={"buckets": [[0.40, 0.60]], "min_n_per_bucket": 0, "fee_model": "zero"},
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": -1.0},
		)
		conn.close()
		assert result.verdict == INSUFFICIENT_DATA

	def test_overlapping_buckets_populate_independently(self, tmp_path):
		"""Overlapping bands are legal config: a trade inside both bands counts in
		both (the single-pass CASE optimization must fall back to per-band queries
		rather than assigning each trade to only the first matching band).
		"""
		markets = []
		trades = []
		for i in range(120):
			ticker = f"OV-{i}"
			won = i < 66  # 55% win at 55c → calibrated
			day = (i % 20) + 1
			date = f"2026-03-{day:02d}"
			markets.append({
				"ticker": ticker, "series_ticker": "SER_OV",
				"result": "yes" if won else "no",
				"last_price": 55, "volume": 10,
				"close_time": f"{date}T12:00:00Z",
				"open_time": f"{date}T00:00:00Z",
			})
			trades.append({
				"trade_id": f"ov-{i}", "ticker": ticker,
				"yes_price": 55, "no_price": 45, "count": 1,
				"created_time": f"{date}T06:00:00Z",
			})
		conn = _make_test_db(tmp_path, markets, trades)
		runner = TestRunner()
		result = runner.run(
			"price_bucket_bias", conn, "SER_OV",
			params={
				# 55c falls inside BOTH bands.
				"buckets": [[0.40, 0.60], [0.50, 0.70]],
				"min_n_per_bucket": 10, "fee_model": "zero",
			},
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": -1.0},
		)
		conn.close()
		assert len(result.detail["buckets"]) == 2
		for b in result.detail["buckets"]:
			assert b["n_trades"] == 120

	def test_dual_min_n_floor_requires_markets_too(self, tmp_path):
		"""min_n_per_bucket floors BOTH n_trades AND n_markets. Two markets with
		5,000 in-band trades between them clear a trades-only floor but carry ~2
		independent observations — they must stay INSUFFICIENT_DATA (the OLD
		market-count floor is preserved, not weakened, by the per-trade switch).
		"""
		markets = []
		trades = []
		for d, ticker in ((1, "DF-A"), (2, "DF-B")):
			date = f"2026-03-{d:02d}"
			markets.append({
				"ticker": ticker, "series_ticker": "SER_DF",
				"result": "yes" if d == 1 else "no",
				"last_price": 50, "volume": 10,
				"close_time": f"{date}T12:00:00Z",
				"open_time": f"{date}T00:00:00Z",
			})
			for j in range(2500):
				trades.append({
					"trade_id": f"df-{ticker}-{j}", "ticker": ticker,
					"yes_price": 50, "no_price": 50, "count": 1,
					"created_time": f"{date}T06:00:00Z",
				})
		conn = _make_test_db(tmp_path, markets, trades)
		runner = TestRunner()
		result = runner.run(
			"price_bucket_bias", conn, "SER_DF",
			params={"buckets": [[0.40, 0.60]], "min_n_per_bucket": 30, "fee_model": "zero"},
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": -1.0},
		)
		conn.close()
		assert result.verdict == INSUFFICIENT_DATA

	@pytest.mark.filterwarnings("ignore:divide by zero encountered:RuntimeWarning")
	def test_c3b_in_band_drift_surfaces_as_real_signal(self, tmp_path):
		"""C3(b): pin CURRENT behavior when a large poison trade lands INSIDE a band.

		Winners (40% of markets) get a big count=100 drift trade at 75¢, which falls in
		[0.70,0.90) carrying the settled (won) outcome; losers never trade in-band. So
		the band is populated ONLY by winners → win_rate 1.0 vs implied 0.75 → a
		+0.25 real-looking edge → EDGE_EXISTS. This is NOT a per-trade-VWAP artifact the
		method can remove: the 75¢ trade genuinely happened at 75¢. It documents that
		per-trade calibration is unbiased only if price is a fair belief at trade time
		(systematic drift on inefficient markets surfaces as a real signal). See the
		matching caveat comment in _trade_price_cluster_rows.
		"""
		markets = []
		trades = []
		n = 400
		tid = 0
		for i in range(n):
			won = i < 160  # 40% win
			day = (i % 20) + 1
			date = f"2026-02-{day:02d}"
			ticker = f"C3B-{i}"
			markets.append({
				"ticker": ticker, "series_ticker": "SER_C3B",
				"result": "yes" if won else "no",
				"last_price": 40, "volume": 10,
				"close_time": f"{date}T12:00:00Z",
				"open_time": f"{date}T00:00:00Z",
			})
			# Fair 40¢ entry (out of [0.70,0.90)).
			trades.append({
				"trade_id": f"c3b-{tid}", "ticker": ticker,
				"yes_price": 40, "no_price": 60, "count": 1,
				"created_time": f"{date}T06:00:00Z",
			})
			tid += 1
			# Winners additionally drift to 75¢ (in [0.70,0.90)) with large count.
			if won:
				trades.append({
					"trade_id": f"c3b-{tid}", "ticker": ticker,
					"yes_price": 75, "no_price": 25, "count": 100,
					"created_time": f"{date}T11:00:00Z",
				})
				tid += 1
		conn = _make_test_db(tmp_path, markets, trades)
		runner = TestRunner()
		result = runner.run(
			"price_bucket_bias", conn, "SER_C3B",
			params={"buckets": [[0.70, 0.90]], "min_n_per_bucket": 10, "fee_model": "zero"},
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": -1.0},
		)
		conn.close()
		# Pinned current behavior: the in-band drift surfaces as a real-looking edge.
		assert result.verdict == EDGE_EXISTS
		b = result.detail["buckets"][0]
		assert b["n_markets"] == 160          # only winners traded in-band
		assert b["win_rate"] == pytest.approx(1.0)
		assert b["edge"] == pytest.approx(0.25, abs=0.01)


class TestSmallKTReference:
	"""Artifact class (e): small-k normal inflation.

	The day-clustered statistic is a mean/SE over k cluster excesses, which under
	the null is Student-t with k−1 df — NOT normal. Grading the verdict on the
	normal reference let findings clear the bar by hairs that t(k−1) rejects
	(proven by real adversarial refutations, including one with dozens of
	clusters that cleared its Bonferroni alpha under the normal reference and
	failed under t). The verdict threshold must be the t(k−1) p-value vs the
	Bonferroni-corrected alpha.
	"""

	def test_t_pvalue_reference_values(self):
		"""stats_utils.t_pvalue: two-sided Student-t p; df < 1 is unusable → 1.0."""
		from edge_catcher.research.stats_utils import t_pvalue

		# t = 3.5 at df = 7 → two-sided p ≈ 0.00997 (fails alpha 0.0027; the
		# normal reference would give 0.000465 and pass — the exact inflation).
		assert t_pvalue(3.5, 7) == pytest.approx(0.00997, abs=0.0005)
		assert t_pvalue(-3.5, 7) == t_pvalue(3.5, 7)
		# Large df converges to the normal reference.
		assert t_pvalue(3.5, 100_000) == pytest.approx(0.000465, abs=0.00002)
		# Degenerate df: no usable t reference → p = 1.0 (never significant).
		assert t_pvalue(3.5, 0) == 1.0
		assert t_pvalue(3.5, -1) == 1.0

	def test_small_k_fails_t_reference_large_k_passes(self):
		"""Direct verdict-helper guard: identical z, only k differs.

		z = 3.5 at base threshold 3.0 (alpha 0.0027, K=1): the normal reference
		passes (p = 4.65e-4) at ANY k — the pre-fix behavior. Under t(k−1) the
		k=8 bucket fails (p ≈ 0.00997 > 0.0027) while k=1000 passes. A revert to
		the normal reference makes both EDGE_EXISTS.
		"""
		from edge_catcher.research.test_runner import _bucket_bonferroni_verdict

		def bucket(k: int) -> dict:
			return {
				"bucket_lo": 0.20, "bucket_hi": 0.30,
				"z": 3.5, "fee_adj": 0.05, "edge": 0.05, "n_clusters": k,
			}

		small = _bucket_bonferroni_verdict([bucket(8)], 3.0, 0.0, True)
		large = _bucket_bonferroni_verdict([bucket(1000)], 3.0, 0.0, True)
		assert small[0] == NO_EDGE
		assert large[0] == EDGE_EXISTS

	def test_small_k_t_reference_end_to_end(self, tmp_path):
		"""Runner-level class (e) control: a 10-day signal tuned to z ≈ 3.30 clears
		the normal bar at threshold 3.0 (p = 9.7e-4 < 0.0027) but fails t(9)
		(p ≈ 0.0092) → NO_EDGE. Pre-fix this graded EDGE_EXISTS.
		"""
		per_day = 400
		base = per_day * 25 // 100  # 100 fair wins/day at 25¢
		noise = [30, -30, 20, -20, 10, -10, 5, -5, 15, -15]
		surplus = 20  # z = surplus·√10/sd(noise) ≈ 3.30
		markets = []
		trades = []
		mkt = 0
		for di, x in enumerate(noise):
			date = f"2026-02-{di + 1:02d}"
			w = base + surplus + x
			for k in range(per_day):
				won = k < w
				ticker = f"TK-{mkt}"
				mkt += 1
				markets.append({
					"ticker": ticker, "series_ticker": "SER_TK",
					"result": "yes" if won else "no",
					"last_price": 25, "volume": 10,
					"close_time": f"{date}T12:00:00Z",
					"open_time": f"{date}T00:00:00Z",
				})
				trades.append({
					"trade_id": f"tk-{ticker}", "ticker": ticker,
					"yes_price": 25, "no_price": 75, "count": 1,
					"created_time": f"{date}T06:00:00Z",
				})
		conn = _make_test_db(tmp_path, markets, trades)
		result = TestRunner().run(
			"price_bucket_bias", conn, "SER_TK",
			params={"buckets": [[0.20, 0.30]], "min_n_per_bucket": 10, "fee_model": "zero"},
			thresholds={"clustered_z_stat": 3.0, "min_fee_adjusted_edge": 0.0},
		)
		conn.close()
		assert result.verdict == NO_EDGE
		b = result.detail["buckets"][0]
		# The z itself DID clear the raw threshold — only the t reference kills it.
		assert abs(b["z"]) > 3.0
		assert b["p_t"] > 0.0027


class TestTakerSideCompositionGate:
	"""Artifact class (b): taker-side composition.

	A band signal carried entirely by aggressive prints on ONE side, while the
	taker-replicable side is flat or opposite, is bid-ask bounce / adverse
	selection — not mispricing a taker can capture (proven by real adversarial
	refutations where the entire graded signal sat in one side's prints and the
	replicable side showed nothing). EDGE_EXISTS must require the exploit side
	(taker='yes' prints for a positive edge, taker='no' for a negative one) to
	independently clear the BASE alpha with a matching sign.
	"""

	def _one_sided_db(self, tmp_path, series: str):
		"""Bid-ask-bounce structure: true prob 60%, asks at 65¢ (taker=yes,
		4 prints/market), bids at 55¢ (taker=no, 1 print/market). The pooled band
		[0.50,0.70) shows a composition-driven −0.03 edge (mean price 0.63 vs 60%
		win), but the taker-replicable NO side realizes edge +0.05 — the OPPOSITE
		sign. Nothing here is capturable by buying NO at the ask.
		"""
		markets = []
		trades = []
		mkt = 0
		for d in range(1, 29):
			date = f"2026-01-{d:02d}"
			wins_today = 11 if d % 2 else 13  # mean 12/20 = 60%
			for k in range(20):
				won = k < wins_today
				ticker = f"TS-{mkt}"
				mkt += 1
				markets.append({
					"ticker": ticker, "series_ticker": series,
					"result": "yes" if won else "no",
					"last_price": 60, "volume": 10,
					"close_time": f"{date}T12:00:00Z",
					"open_time": f"{date}T00:00:00Z",
				})
				for j in range(4):
					trades.append({
						"trade_id": f"ts-y-{ticker}-{j}", "ticker": ticker,
						"yes_price": 65, "no_price": 35, "count": 1,
						"taker_side": "yes",
						"created_time": f"{date}T06:{j:02d}:00Z",
					})
				trades.append({
					"trade_id": f"ts-n-{ticker}", "ticker": ticker,
					"yes_price": 55, "no_price": 45, "count": 1,
					"taker_side": "no",
					"created_time": f"{date}T07:00:00Z",
				})
		return _make_test_db(tmp_path, markets, trades)

	def test_one_sided_signal_downgraded_not_edge_exists(self, tmp_path):
		"""The composition artifact must not grade EDGE_EXISTS: the exploit (NO)
		side's edge is +0.05 against the pooled −0.03 → taker_side_fragile →
		EDGE_NOT_TRADEABLE. Pre-fix this graded EDGE_EXISTS.
		"""
		conn = self._one_sided_db(tmp_path, "SER_TS")
		result = TestRunner().run(
			"price_bucket_bias", conn, "SER_TS",
			params={"buckets": [[0.50, 0.70]], "min_n_per_bucket": 10, "fee_model": "zero"},
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": 0.0},
		)
		conn.close()
		assert result.verdict == EDGE_NOT_TRADEABLE
		driver = result.detail["driver_bucket"]
		assert driver["taker_side_fragile"] is True
		assert driver["exploit_side"] == "no"
		# Side detail exposes the decomposition: the yes side carries the negative
		# signal; the replicable no side is positive (bid-ask bounce signature).
		assert driver["taker_yes"]["n_trades"] == 2240
		assert driver["taker_no"]["n_trades"] == 560
		assert driver["taker_yes"]["edge"] == pytest.approx(-0.05, abs=0.01)
		assert driver["taker_no"]["edge"] == pytest.approx(+0.05, abs=0.01)

	def test_both_sided_signal_still_grades_edge_exists(self, tmp_path):
		"""Over-kill guard: a genuine mispricing printed on BOTH sides at the same
		price keeps EDGE_EXISTS (the exploit side independently clears base alpha
		with a matching sign).
		"""
		markets = []
		trades = []
		mkt = 0
		for d in range(1, 29):
			date = f"2026-01-{d:02d}"
			wins_today = 10 if d % 2 else 12  # mean 11/20 = 55% at 65¢ → edge −0.10
			for k in range(20):
				won = k < wins_today
				ticker = f"TB-{mkt}"
				mkt += 1
				markets.append({
					"ticker": ticker, "series_ticker": "SER_TB",
					"result": "yes" if won else "no",
					"last_price": 65, "volume": 10,
					"close_time": f"{date}T12:00:00Z",
					"open_time": f"{date}T00:00:00Z",
				})
				for side in ("yes", "no"):
					trades.append({
						"trade_id": f"tb-{side}-{ticker}", "ticker": ticker,
						"yes_price": 65, "no_price": 35, "count": 1,
						"taker_side": side,
						"created_time": f"{date}T06:00:00Z",
					})
		conn = _make_test_db(tmp_path, markets, trades)
		result = TestRunner().run(
			"price_bucket_bias", conn, "SER_TB",
			params={"buckets": [[0.50, 0.70]], "min_n_per_bucket": 10, "fee_model": "zero"},
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": 0.0},
		)
		conn.close()
		assert result.verdict == EDGE_EXISTS
		driver = result.detail["driver_bucket"]
		assert driver["exploit_side"] == "no"
		assert driver["taker_side_fragile"] is False

	def test_missing_side_data_flagged_unavailable_not_fragile(self, tmp_path):
		"""Prints whose taker_side is not 'yes'/'no' (adapter default '' when the
		API omits the field; 'unknown' on other venues) leave both side splits
		empty. The gate still refuses EDGE_EXISTS (replicability unverifiable),
		but the detail must say taker_side_unavailable — NOT taker_side_fragile,
		which asserts a diagnosed one-sided artifact that was never measured.
		"""
		markets = []
		trades = []
		for i in range(200):
			ticker = f"NS-{i}"
			won = i < 60  # genuine 30%-win mispricing at 50¢
			day = (i % 28) + 1
			date = f"2026-01-{day:02d}"
			markets.append({
				"ticker": ticker, "series_ticker": "SER_NS",
				"result": "yes" if won else "no",
				"last_price": 50, "volume": 10,
				"close_time": f"{date}T12:00:00Z",
				"open_time": f"{date}T00:00:00Z",
			})
			trades.append({
				"trade_id": f"ns-{i}", "ticker": ticker,
				"yes_price": 50, "no_price": 50, "count": 1,
				"taker_side": "",  # side metadata absent in the capture
				"created_time": f"{date}T06:00:00Z",
			})
		conn = _make_test_db(tmp_path, markets, trades)
		result = TestRunner().run(
			"price_bucket_bias", conn, "SER_NS",
			params={"buckets": [[0.40, 0.60]], "min_n_per_bucket": 10, "fee_model": "zero"},
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": 0.0},
		)
		conn.close()
		assert result.verdict == EDGE_NOT_TRADEABLE
		driver = result.detail["driver_bucket"]
		assert driver["taker_side_coverage"] == 0.0
		assert driver["taker_side_unavailable"] is True
		# NOT labeled as a diagnosed composition artifact.
		assert driver["taker_side_fragile"] is False

	def test_taker_gate_direct_verdict_logic(self):
		"""Direct helper guard: identical significant buckets, only the exploit-side
		stats differ. Corroborated exploit side → EDGE_EXISTS; opposite-sign or
		insignificant exploit side → EDGE_NOT_TRADEABLE with the fragile flag.
		"""
		from edge_catcher.research.test_runner import _bucket_bonferroni_verdict

		def bucket(exploit_edge: float, exploit_p_t: float) -> dict:
			return {
				"bucket_lo": 0.50, "bucket_hi": 0.70,
				"z": -6.0, "n_clusters": 30, "fee_adj": 0.03, "edge": -0.05,
				"exploit_side": "no", "exploit_n_trades": 500,
				"exploit_edge": exploit_edge, "exploit_p_t": exploit_p_t,
				"exploit_n_clusters": 30,
			}

		ok = bucket(-0.05, 1e-6)
		corroborated = _bucket_bonferroni_verdict([ok], 3.0, 0.0, True)
		assert corroborated[0] == EDGE_EXISTS
		assert ok["taker_side_fragile"] is False

		flat = bucket(-0.05, 0.5)  # right sign, but n.s. on the replicable side
		res_flat = _bucket_bonferroni_verdict([flat], 3.0, 0.0, True)
		assert res_flat[0] == EDGE_NOT_TRADEABLE
		assert flat["taker_side_fragile"] is True

		opposite = bucket(+0.05, 1e-6)  # significant but WRONG sign
		res_opp = _bucket_bonferroni_verdict([opposite], 3.0, 0.0, True)
		assert res_opp[0] == EDGE_NOT_TRADEABLE
		assert opposite["taker_side_fragile"] is True

		empty = bucket(-0.05, 1e-6)
		empty["exploit_n_trades"] = 0  # signal entirely on the other side
		res_empty = _bucket_bonferroni_verdict([empty], 3.0, 0.0, True)
		assert res_empty[0] == EDGE_NOT_TRADEABLE
		assert empty["taker_side_fragile"] is True


class TestDegenerateOutcomeGate:
	"""Artifact class (c): degenerate zero-win z.

	When a band's wins are 0 (or n) in every cluster, the clustered z over
	excesses measures only PRICE dispersion across days — a huge |z| can coexist
	with an entirely unremarkable exact-binomial p (proven by a real adversarial
	refutation on an extreme-price band). EDGE_EXISTS must require market-level
	expected wins AND losses ≥ ~5 (normal approximation valid), or an exact
	binomial cross-check on the market-level outcome count to independently
	clear the Bonferroni alpha.
	"""

	def test_exact_binom_pvalue_reference_values(self):
		from edge_catcher.research.stats_utils import exact_binom_pvalue

		# 0 wins in 60 markets at mean price 1.5¢: E[wins] = 0.9 — observing 0
		# is unremarkable (p ≈ 1), no matter what the dispersion z claims.
		assert exact_binom_pvalue(0, 60, 0.015) > 0.5
		# 12 wins in 200 markets at 1¢ (E = 2) IS a real anomaly (p ≪ 0.001).
		assert exact_binom_pvalue(12, 200, 0.01) < 1e-4
		# Guards: nothing observed / degenerate null probability → 1.0.
		assert exact_binom_pvalue(0, 0, 0.5) == 1.0
		assert exact_binom_pvalue(0, 10, 0.0) == 1.0
		assert exact_binom_pvalue(10, 10, 1.0) == 1.0

	def test_degenerate_gate_direct_verdict_logic(self):
		"""Direct helper guard for the three branches: E-floor pass, E-floor fail
		with an unremarkable exact p (kill), and E-floor fail rescued by a truly
		anomalous exact p (keep).
		"""
		from edge_catcher.research.test_runner import _bucket_bonferroni_verdict

		def bucket(n_markets: int, mean_price: float, market_wins: int, edge: float, z: float) -> dict:
			return {
				"bucket_lo": 0.0, "bucket_hi": 0.03,
				"z": z, "n_clusters": 10, "fee_adj": 0.01, "edge": edge,
				"n_markets": n_markets, "mean_price": mean_price,
				"market_wins": market_wins,
			}

		# Zero wins, E[wins] = 0.9: the −9 z is pure price dispersion → NO_EDGE.
		dead = bucket(60, 0.015, 0, -0.015, -9.0)
		assert _bucket_bonferroni_verdict([dead], 2.0, 0.0, True)[0] == NO_EDGE
		assert dead["degenerate_gate_ok"] is False
		assert dead["expected_market_wins"] < 5
		assert dead["exact_binom_p"] > 0.05

		# Mirror: all-win favorites band, E[losses] = 0.9 → NO_EDGE.
		all_win = bucket(60, 0.985, 60, +0.015, +9.0)
		assert _bucket_bonferroni_verdict([all_win], 2.0, 0.0, True)[0] == NO_EDGE
		assert all_win["degenerate_gate_ok"] is False

		# Healthy outcome counts (E both ≥ 5): gate passes without the cross-check.
		healthy = bucket(2000, 0.5, 800, -0.10, -9.0)
		assert _bucket_bonferroni_verdict([healthy], 2.0, 0.0, True)[0] == EDGE_EXISTS
		assert healthy["degenerate_gate_ok"] is True

		# E[wins] = 2 < 5 BUT 12 observed wins is a genuine exact-binomial anomaly
		# → rescued by the cross-check.
		rescued = bucket(200, 0.01, 12, +0.05, +9.0)
		assert _bucket_bonferroni_verdict([rescued], 2.0, 0.0, True)[0] == EDGE_EXISTS
		assert rescued["degenerate_gate_ok"] is True
		assert rescued["exact_binom_p"] < 0.01

	@pytest.mark.filterwarnings("ignore:invalid value encountered:RuntimeWarning")
	@pytest.mark.filterwarnings("ignore:divide by zero encountered:RuntimeWarning")
	def test_zero_win_dispersion_z_graded_no_edge(self, tmp_path):
		"""End-to-end class (c) control: a 0–3¢ band where EVERY market settles NO.
		Day mean prices alternate 1¢/2¢, so the clustered z ≈ −9 measures price
		dispersion only; the exact market-level read (0 wins, E = 0.9) is p ≈ 1.
		Pre-fix this graded EDGE_EXISTS.
		"""
		markets = []
		trades = []
		mkt = 0
		for d in range(1, 11):
			date = f"2026-02-{d:02d}"
			price = 1 if d % 2 else 2
			for k in range(6):
				ticker = f"ZW-{mkt}"
				mkt += 1
				markets.append({
					"ticker": ticker, "series_ticker": "SER_ZW",
					"result": "no",
					"last_price": price, "volume": 10,
					"close_time": f"{date}T12:00:00Z",
					"open_time": f"{date}T00:00:00Z",
				})
				for side in ("yes", "no"):
					trades.append({
						"trade_id": f"zw-{side}-{ticker}", "ticker": ticker,
						"yes_price": price, "no_price": 100 - price, "count": 1,
						"taker_side": side,
						"created_time": f"{date}T06:00:00Z",
					})
		conn = _make_test_db(tmp_path, markets, trades)
		result = TestRunner().run(
			"price_bucket_bias", conn, "SER_ZW",
			params={"buckets": [[0.0, 0.03]], "min_n_per_bucket": 10, "fee_model": "zero"},
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": 0.0},
		)
		conn.close()
		assert result.verdict == NO_EDGE
		b = result.detail["buckets"][0]
		# The dispersion z is huge — only the outcome-count gate kills it.
		assert abs(b["z"]) > 5.0
		assert b["degenerate_gate_ok"] is False
		assert b["market_wins"] == 0
		assert b["expected_market_wins"] < 5


class TestMcNullGate:
	"""Artifact class (d): rare-event null inflation.

	When E[outcome-flips] is small (few clusters, extreme prices, lumpy per-day
	counts), the clustered z's nominal p overstates by orders of magnitude
	(proven by real adversarial refutations where the honest MC p sat many
	orders of magnitude above the nominal claim, and the nominal method fired
	frequently under a simulated H0). The honest reference is a Monte Carlo
	null: redraw each market's outcome ~ Bernoulli(its in-band traded price),
	keep the day-cluster structure, recompute the same statistic. EDGE_EXISTS
	requires the MC p to clear the Bonferroni alpha.
	"""

	def test_mc_null_pvalue_rare_event_inflation(self):
		"""The rare-event mechanism: 6 days × 5 markets at 95¢. The most likely
		null outcome (all 30 win, P ≈ 0.21) already produces the zero-variance
		sentinel |z| = 100, so ANY huge observed z has an honest MC p ≳ 0.2 —
		while its nominal normal p claims < 1e-10.
		"""
		from edge_catcher.research.stats_utils import mc_null_pvalue

		market_stats = [
			(f"2026-03-{d:02d}", 1, 0.95)
			for d in range(1, 7)
			for _ in range(5)
		]
		p = mc_null_pvalue(market_stats, z_obs=8.93)
		assert p > 0.05  # orders of magnitude above the nominal normal claim

	def test_mc_null_pvalue_null_calibration(self):
		"""Under a well-behaved null (20 days × 10 markets at 50¢), a modest
		z_obs = 1.0 is unremarkable: MC p lands near the t(19) two-sided value
		(≈ 0.33), nowhere near significance.
		"""
		from edge_catcher.research.stats_utils import mc_null_pvalue

		market_stats = [
			(f"2026-03-{d:02d}", 1, 0.50)
			for d in range(1, 21)
			for _ in range(10)
		]
		p = mc_null_pvalue(market_stats, z_obs=1.0)
		assert 0.15 < p < 0.6

	def test_mc_null_pvalue_strong_signal_stays_significant(self):
		"""Over-kill guard: a z_obs = 8 over 28 well-populated days IS honestly
		extreme — MC p must stay at the floor (no sim reaches it).
		"""
		from edge_catcher.research.stats_utils import mc_null_pvalue

		market_stats = [
			(f"2026-03-{d:02d}", 1, 0.50)
			for d in range(1, 29)
			for _ in range(20)
		]
		p = mc_null_pvalue(market_stats, z_obs=8.0)
		assert p < 0.001

	def test_mc_null_pvalue_determinism_and_guards(self):
		from edge_catcher.research.stats_utils import mc_null_pvalue

		market_stats = [("2026-03-01", 1, 0.5)] * 5 + [("2026-03-02", 1, 0.5)] * 5
		assert mc_null_pvalue(market_stats, 2.0) == mc_null_pvalue(market_stats, 2.0)
		# Fewer than 2 day-clusters: no null to simulate → 1.0.
		assert mc_null_pvalue([("2026-03-01", 1, 0.5)] * 10, 5.0) == 1.0
		assert mc_null_pvalue([], 5.0) == 1.0
		# Add-one correction: p is never exactly 0.
		assert mc_null_pvalue(market_stats, 500.0) > 0.0

	def test_mc_gate_direct_verdict_logic(self):
		"""Direct helper guard: an otherwise-qualifying bucket must survive only
		if the MC null clears the Bonferroni alpha. The rare-event structure
		(6 lumpy days at 95¢) fails; the well-populated structure passes.
		"""
		from edge_catcher.research.test_runner import _bucket_bonferroni_verdict

		def bucket() -> dict:
			return {
				"bucket_lo": 0.93, "bucket_hi": 0.97,
				"z": 8.93, "n_clusters": 6, "fee_adj": 0.03, "edge": 0.04,
			}

		rare = [
			(f"2026-03-{d:02d}", 1, 0.95)
			for d in range(1, 7)
			for _ in range(5)
		]
		b_rare = bucket()
		res = _bucket_bonferroni_verdict([b_rare], 2.0, 0.0, True, mc_rows_fn=lambda b: rare)
		assert res[0] == NO_EDGE
		assert b_rare["mc_p"] > 0.0455
		assert b_rare["mc_gate_ok"] is False

		healthy = [
			(f"2026-03-{d:02d}", 1, 0.50)
			for d in range(1, 29)
			for _ in range(20)
		]
		b_ok = bucket()
		b_ok["n_clusters"] = 28
		res_ok = _bucket_bonferroni_verdict([b_ok], 2.0, 0.0, True, mc_rows_fn=lambda b: healthy)
		assert res_ok[0] == EDGE_EXISTS
		assert b_ok["mc_gate_ok"] is True

	def test_mc_resolution_scales_to_bonferroni_alpha(self):
		"""The MC p floor is 1/(n_sims+1); with a FIXED sim count the gate goes
		mathematically unpassable once alpha_corr < that floor (e.g. a z=3.5
		threshold with K=5 evaluated buckets → alpha_corr ≈ 9.3e-5 < 1e-4).
		Sims must scale with alpha so a genuinely extreme bucket can still
		qualify. Pre-fix this graded NO_EDGE with mc_p == 9.999e-5 > alpha_corr
		despite ZERO extreme sims.
		"""
		from edge_catcher.research.test_runner import _bucket_bonferroni_verdict

		strong = {
			"bucket_lo": 0.20, "bucket_hi": 0.30,
			"z": 12.0, "n_clusters": 28, "fee_adj": 0.05, "edge": 0.05,
		}
		duds = [
			{
				"bucket_lo": lo, "bucket_hi": lo + 0.10,
				"z": 0.1, "n_clusters": 28, "fee_adj": 0.01, "edge": 0.01,
			}
			for lo in (0.30, 0.40, 0.50, 0.60)
		]
		healthy = [
			(f"2026-03-{d:02d}", 1, 0.50)
			for d in range(1, 29)
			for _ in range(20)
		]
		res = _bucket_bonferroni_verdict(
			[strong, *duds], 3.5, 0.0, True, mc_rows_fn=lambda b: healthy,
		)
		# alpha_corr = 2(1−Φ(3.5))/5 ≈ 9.3e-5 — below the fixed-10k floor.
		assert res[0] == EDGE_EXISTS
		assert strong["mc_gate_ok"] is True
		assert strong["mc_p"] <= 9.31e-5
		assert strong["mc_n_sims"] > 100_000  # sims scaled, not fixed

	def test_rare_event_inflation_end_to_end(self, tmp_path):
		"""Runner-level class (d) control: 6 days × 25 markets at 95¢ with wins
		[24,24,25,24,24,25]. The day-excess spread is small by luck → z = +2.77,
		t(5) p = 0.0395 ≤ alpha 0.0455 — every pre-MC gate passes (E[losses] =
		7.5 clears class (c); both taker sides print). But the honest MC null
		(lumpy skewed day excesses at an extreme price) gives p ≈ 0.074 → NO_EDGE.
		The normal reference claimed p = 0.006 — the rare-event inflation.
		"""
		wins_by_day = [24, 24, 25, 24, 24, 25]
		markets = []
		trades = []
		mkt = 0
		for d, w in enumerate(wins_by_day, start=1):
			date = f"2026-03-{d:02d}"
			for k in range(25):
				won = k < w
				ticker = f"RE-{mkt}"
				mkt += 1
				markets.append({
					"ticker": ticker, "series_ticker": "SER_RE",
					"result": "yes" if won else "no",
					"last_price": 95, "volume": 10,
					"close_time": f"{date}T12:00:00Z",
					"open_time": f"{date}T00:00:00Z",
				})
				for side in ("yes", "no"):
					trades.append({
						"trade_id": f"re-{side}-{ticker}", "ticker": ticker,
						"yes_price": 95, "no_price": 5, "count": 1,
						"taker_side": side,
						"created_time": f"{date}T06:00:00Z",
					})
		conn = _make_test_db(tmp_path, markets, trades)
		result = TestRunner().run(
			"price_bucket_bias", conn, "SER_RE",
			params={"buckets": [[0.93, 0.97]], "min_n_per_bucket": 10, "fee_model": "zero"},
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": 0.0},
		)
		conn.close()
		assert result.verdict == NO_EDGE
		b = result.detail["buckets"][0]
		# Every pre-MC gate passed — only the honest null kills it.
		assert b["significant"] is True
		assert b["taker_gate_ok"] is True
		assert b["degenerate_gate_ok"] is True
		assert b["mc_gate_ok"] is False
		assert b["mc_p"] > 0.0455
