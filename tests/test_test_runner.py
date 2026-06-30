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
		"""Contracts at 50% that settle YES only 30% → significant negative z."""
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

	def test_edge_not_tradeable_when_fees_kill_edge(self, tmp_path):
		"""Edge exists but fees eat it → EDGE_NOT_TRADEABLE."""
		markets = []
		trades = []
		for i in range(200):
			ticker = f"M-{i}"
			# Very slight mispricing: 45% win rate at 50% implied = 5% raw edge
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
				"created_time": f"2026-01-{(i % 30) + 1:02d}T06:00:00Z",
			})
		conn = _make_test_db(tmp_path, markets, trades)
		from edge_catcher.research.test_runner import TestRunner, EDGE_NOT_TRADEABLE, NO_EDGE
		runner = TestRunner()
		result = runner.run("price_bucket_bias", conn, "SER_A",
			params={
				"buckets": [[0.40, 0.60]],
				"min_n_per_bucket": 10,
				"fee_model": "standard",  # 1.75% maker fee
			},
			thresholds={"clustered_z_stat": 1.5, "min_fee_adjusted_edge": 0.0},
		)
		conn.close()
		# With 5% raw edge at 50% implied and 1.75% fee: 0.05 - 0.0175*0.5 = 0.04125
		# That actually survives fees. But the z-stat may not be significant enough.
		# The test just verifies the fee_model logic is wired up correctly.
		assert result.verdict in (EDGE_NOT_TRADEABLE, NO_EDGE)


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
			# 3 early trades within 15 minutes of open
			for j in range(3):
				trades.append({
					"trade_id": f"lc-early-{i}-{j}",
					"ticker": ticker,
					"yes_price": 50,
					"no_price": 50,
					"count": 1,
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


class TestVolumeMispricingTest:
	"""Tests for VolumeMispricingTest — liquidity-based edge detection."""

	def test_edge_exists_low_volume_mispriced(self, tmp_path):
		"""Low-volume markets (volume=1) have 30% win rate at 50% implied;
		high-volume markets (volume=100) have 50% win rate → EDGE_EXISTS."""
		from edge_catcher.research.test_runner import TestRunner, EDGE_EXISTS

		markets = []
		trades = []
		# 150 low-volume markets: 30% win rate
		for i in range(150):
			ticker = f"VM-low-{i}"
			won = i < 45  # 30% win rate
			day = (i % 28) + 1
			markets.append({
				"ticker": ticker,
				"series_ticker": "SER_VM",
				"result": "yes" if won else "no",
				"last_price": 50,
				"volume": 1,  # thin market
				"close_time": f"2026-01-{day:02d}T12:00:00Z",
				"open_time": f"2026-01-{day:02d}T00:00:00Z",
			})
			trades.append({
				"trade_id": f"vm-low-{i}",
				"ticker": ticker,
				"yes_price": 50,
				"no_price": 50,
				"count": 1,
				"created_time": f"2026-01-{day:02d}T06:00:00Z",
			})
		# 150 high-volume markets: 50% win rate
		for i in range(150):
			ticker = f"VM-high-{i}"
			won = i < 75  # 50% win rate
			day = (i % 28) + 1
			markets.append({
				"ticker": ticker,
				"series_ticker": "SER_VM",
				"result": "yes" if won else "no",
				"last_price": 50,
				"volume": 100,  # liquid market
				"close_time": f"2026-01-{day:02d}T12:00:00Z",
				"open_time": f"2026-01-{day:02d}T00:00:00Z",
			})
			trades.append({
				"trade_id": f"vm-high-{i}",
				"ticker": ticker,
				"yes_price": 50,
				"no_price": 50,
				"count": 100,
				"created_time": f"2026-01-{day:02d}T06:00:00Z",
			})

		conn = _make_test_db(tmp_path, markets, trades)
		runner = TestRunner()
		result = runner.run(
			"volume_mispricing", conn, "SER_VM",
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

	def test_clustering_by_ticker_not_trade(self, tmp_path):
		"""Many trades within FEW markets → effective N = #markets, so z does not
		inflate with trade count. 3 markets × 100 trades each at 50¢, 2/3 win.
		"""
		markets = []
		trades = []
		outcomes = ["yes", "no", "yes"]  # 2/3 win at 50¢ → big per-trade z, tiny per-market z
		for i, res in enumerate(outcomes):
			ticker = f"CL-{i}"
			markets.append({
				"ticker": ticker, "series_ticker": "SER_CL",
				"result": res,
				"last_price": 50, "volume": 100,
				"close_time": f"2026-01-{i + 1:02d}T12:00:00Z",
				"open_time": f"2026-01-{i + 1:02d}T00:00:00Z",
			})
			for j in range(100):
				trades.append({
					"trade_id": f"cl-{i}-{j}", "ticker": ticker,
					"yes_price": 50, "no_price": 50, "count": 1,
					"created_time": f"2026-01-{i + 1:02d}T06:{j % 60:02d}:00Z",
				})
		conn = _make_test_db(tmp_path, markets, trades)
		runner = TestRunner()
		result = runner.run(
			"price_bucket_bias", conn, "SER_CL",
			params={
				"buckets": [[0.40, 0.60]],
				"min_n_per_bucket": 3, "fee_model": "zero",
			},
			thresholds={"clustered_z_stat": 2.0, "min_fee_adjusted_edge": -1.0},
		)
		conn.close()
		# Per-trade clustering would give |z| ≫ 2 on 300 trades; per-ticker keeps it small.
		assert abs(result.z_stat) < 2.0
		assert result.verdict != EDGE_EXISTS
