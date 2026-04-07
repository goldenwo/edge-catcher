"""Tests for the Context Engine series profiler."""

import math
import sqlite3
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def sample_db(tmp_path):
	"""Create a minimal markets DB with 2 series."""
	db_path = str(tmp_path / "test.db")
	conn = sqlite3.connect(db_path)
	conn.execute("""
		CREATE TABLE markets (
			ticker TEXT, event_ticker TEXT, series_ticker TEXT,
			title TEXT, status TEXT, result TEXT,
			yes_bid INT, yes_ask INT, last_price INT,
			open_interest INT, volume INT,
			expiration_time TEXT, close_time TEXT, created_time TEXT,
			settled_time TEXT, open_time TEXT,
			notional_value REAL, floor_strike REAL, cap_strike REAL,
			raw_data TEXT, updated_at TEXT
		)
	""")
	# Insert hourly series — 100 markets, 1 hour apart across multiple days
	for i in range(100):
		day = 1 + i // 24
		hour = i % 24
		open_t = f"2025-01-{day:02d}T{hour:02d}:00:00+00:00"
		close_t = f"2025-01-{day:02d}T{hour + 1:02d}:00:00+00:00" if hour < 23 else f"2025-01-{day + 1:02d}T00:00:00+00:00"
		conn.execute(
			"INSERT INTO markets (ticker, series_ticker, title, status, result, "
			"last_price, volume, open_time, close_time, floor_strike, cap_strike) "
			"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			(f"KXTEST-{i}", "KXTEST", "Test price range on Jan 1?",
			 "settled", "yes" if i % 3 == 0 else "no",
			 5 if i % 2 == 0 else 95, i * 10,
			 open_t, close_t, 100.0, 200.0),
		)
	conn.commit()
	conn.close()
	return db_path


class TestSeriesProfile:
	def test_profile_returns_correct_fields(self, sample_db):
		from edge_catcher.research.context_engine import ContextEngine

		engine = ContextEngine()
		profiles = engine.profile_all([sample_db])

		assert len(profiles) == 1
		p = profiles[0]
		assert p.series_ticker == "KXTEST"
		assert p.db_path == sample_db
		assert p.market_count == 100
		assert "Test" in p.description
		assert p.settlement_frequency in ("hourly", "unknown")
		assert p.volume_stats["mean"] > 0
		assert "extreme" in p.price_distribution or "mid" in p.price_distribution
		assert "yes" in p.result_distribution
		assert p.date_range[0] is not None
		assert p.date_range[1] is not None

	def test_profile_skips_bad_db(self, tmp_path):
		from edge_catcher.research.context_engine import ContextEngine

		bad_db = str(tmp_path / "bad.db")
		conn = sqlite3.connect(bad_db)
		conn.execute("CREATE TABLE not_markets (id INT)")
		conn.close()

		engine = ContextEngine()
		profiles = engine.profile_all([bad_db])
		assert profiles == []

	def test_profile_multiple_series(self, sample_db):
		"""DB with multiple series returns one profile per series."""
		from edge_catcher.research.context_engine import ContextEngine

		# Add a second series to the same DB
		conn = sqlite3.connect(sample_db)
		for i in range(50):
			conn.execute(
				"INSERT INTO markets (ticker, series_ticker, title, status, result, "
				"last_price, volume, open_time, close_time) "
				"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
				(f"KXOTHER-{i}", "KXOTHER", "Other series?",
				 "settled", "yes", 50, 20,
				 "2025-01-01T00:00:00+00:00", "2025-01-02T00:00:00+00:00"),
			)
		conn.commit()
		conn.close()

		engine = ContextEngine()
		profiles = engine.profile_all([sample_db])
		tickers = {p.series_ticker for p in profiles}
		assert tickers == {"KXTEST", "KXOTHER"}


class TestBuildContextBlock:
	def test_empty_profiles_returns_empty(self):
		from edge_catcher.research.context_engine import ContextEngine

		engine = ContextEngine()
		assert engine.build_context_block([]) == ""

	def test_context_block_contains_series_info(self, sample_db):
		from edge_catcher.research.context_engine import ContextEngine

		engine = ContextEngine()
		profiles = engine.profile_all([sample_db])
		block = engine.build_context_block(profiles)

		assert "KXTEST" in block
		assert "Market Profiles" in block
		assert "Volume:" in block

	def test_context_block_groups_by_asset_class(self, sample_db):
		from edge_catcher.research.context_engine import ContextEngine

		engine = ContextEngine()
		profiles = engine.profile_all([sample_db])
		# Override asset class for testing
		profiles[0].asset_class = "Crypto"
		block = engine.build_context_block(profiles)
		assert "Crypto" in block


class TestFindRelatedSeries:
	def test_finds_same_asset_class(self):
		from edge_catcher.research.context_engine import ContextEngine, SeriesProfile

		profiles = [
			SeriesProfile(
				series_ticker="KXXRP", db_path="data/alt.db",
				description="XRP hourly", settlement_frequency="hourly",
				market_count=1000, date_range=("2025-01-01", "2025-12-31"),
				volume_stats={"median": 10, "mean": 20, "p90": 50},
				price_distribution={"extreme": 0.5, "mid": 0.2, "moderate": 0.3},
				result_distribution={"yes": 0.5, "no": 0.5},
				asset_class="Crypto", external_asset="xrp",
			),
			SeriesProfile(
				series_ticker="KXXRPD", db_path="data/alt.db",
				description="XRP daily", settlement_frequency="daily",
				market_count=500, date_range=("2025-01-01", "2025-12-31"),
				volume_stats={"median": 50, "mean": 80, "p90": 200},
				price_distribution={"extreme": 0.4, "mid": 0.3, "moderate": 0.3},
				result_distribution={"yes": 0.5, "no": 0.5},
				asset_class="Crypto", external_asset="xrp",
			),
			SeriesProfile(
				series_ticker="KXNBA", db_path="data/sports.db",
				description="NBA spread", settlement_frequency="daily",
				market_count=200, date_range=("2025-01-01", "2025-12-31"),
				volume_stats={"median": 30, "mean": 40, "p90": 100},
				price_distribution={"extreme": 0.3, "mid": 0.4, "moderate": 0.3},
				result_distribution={"yes": 0.5, "no": 0.5},
				asset_class="Sports",
			),
		]

		engine = ContextEngine()
		related = engine.find_related_series("KXXRP", profiles, same_asset_class=True)

		# Same asset (XRP daily) should be first
		assert related[0][0] == "KXXRPD"
		# Sports should not appear (different asset class)
		assert all(t != "KXNBA" for t, _ in related)

	def test_returns_empty_for_unknown_series(self):
		from edge_catcher.research.context_engine import ContextEngine

		engine = ContextEngine()
		assert engine.find_related_series("UNKNOWN", [], same_asset_class=True) == []


class TestIdeatorContextIntegration:
	def test_ideate_accepts_context_block(self):
		"""ideate() should accept context_block parameter."""
		import inspect
		from edge_catcher.research.llm_ideator import LLMIdeator

		sig = inspect.signature(LLMIdeator.ideate)
		assert "context_block" in sig.parameters

	def test_build_prompt_includes_context_block(self):
		"""build_ideation_prompt should include context block when provided."""
		from unittest.mock import MagicMock
		from edge_catcher.research.llm_ideator import LLMIdeator

		tracker = MagicMock()
		tracker.list_results.return_value = []
		ideator = LLMIdeator(tracker=tracker, audit=MagicMock(), client=MagicMock())

		prompt = ideator.build_ideation_prompt(
			available_strategies=["example"],
			series_map={"data/test.db": ["KXTEST"]},
			context_block="## Market Profiles\nKXTEST: test series",
		)
		assert "## Market Profiles" in prompt
		# "Available Data" section should be removed when context_block is present
		assert "## Available Data" not in prompt

	def test_build_prompt_without_context_block_unchanged(self):
		"""Without context_block, prompt should still have Available Data."""
		from unittest.mock import MagicMock
		from edge_catcher.research.llm_ideator import LLMIdeator

		tracker = MagicMock()
		tracker.list_results.return_value = []
		ideator = LLMIdeator(tracker=tracker, audit=MagicMock(), client=MagicMock())

		prompt = ideator.build_ideation_prompt(
			available_strategies=["example"],
			series_map={"data/test.db": ["KXTEST"]},
		)
		assert "## Available Data" in prompt

	def test_cold_start_bypasses_min_results(self):
		"""With context_block, ideate should not raise for < 10 results."""
		from unittest.mock import MagicMock
		from edge_catcher.research.llm_ideator import LLMIdeator

		tracker = MagicMock()
		tracker.list_results.return_value = []  # 0 results

		client = MagicMock()
		client.complete.return_value = '{"analysis":"test","existing_strategy_hypotheses":[],"novel_strategy_proposals":[]}'
		client._resolve_model.return_value = "test-model"
		client.last_usage = {"input_tokens": 0, "output_tokens": 0}

		ideator = LLMIdeator(
			tracker=tracker, audit=MagicMock(), client=client,
		)
		# Should NOT raise ValueError with context_block
		hypotheses, proposals = ideator.ideate(
			available_strategies=["example"],
			series_map={"data/test.db": ["KXTEST"]},
			context_block="## Market Profiles\ntest",
			start_date="2025-01-01",
			end_date="2025-12-31",
		)
		assert isinstance(hypotheses, list)


class TestSteeringDirectives:
	def test_steering_includes_cross_series_notes(self):
		"""Steering directives should mention cross-series relationships when profiles are available."""
		from unittest.mock import MagicMock
		from edge_catcher.research.context_engine import SeriesProfile
		from edge_catcher.research.llm_ideator import LLMIdeator

		tracker = MagicMock()
		tracker.list_results.return_value = []
		ideator = LLMIdeator(tracker=tracker, audit=MagicMock(), client=MagicMock())

		profiles = [
			SeriesProfile(
				series_ticker="KXXRP", db_path="data/alt.db",
				description="XRP hourly", settlement_frequency="hourly",
				market_count=1000, date_range=("2025-01-01", "2025-12-31"),
				volume_stats={"median": 10, "mean": 20, "p90": 50},
				price_distribution={"extreme": 0.5, "mid": 0.2, "moderate": 0.3},
				result_distribution={"yes": 0.5, "no": 0.5},
				asset_class="Crypto", external_asset="xrp",
			),
			SeriesProfile(
				series_ticker="KXXRPD", db_path="data/alt.db",
				description="XRP daily", settlement_frequency="daily",
				market_count=500, date_range=("2025-01-01", "2025-12-31"),
				volume_stats={"median": 50, "mean": 80, "p90": 200},
				price_distribution={"extreme": 0.4, "mid": 0.3, "moderate": 0.3},
				result_distribution={"yes": 0.5, "no": 0.5},
				asset_class="Crypto", external_asset="xrp",
			),
		]

		directives = ideator._build_context_directives(profiles)
		assert "KXXRP" in directives
		assert "KXXRPD" in directives
		assert "same asset" in directives.lower() or "xrp" in directives.lower()
