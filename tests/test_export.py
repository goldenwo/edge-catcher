"""Tests for edge_catcher.research.export."""

from __future__ import annotations

import argparse
import json
import zipfile
from pathlib import Path
from unittest.mock import patch

import yaml

from edge_catcher.research.export import ExportCollector
from edge_catcher.research.tracker import Tracker
from edge_catcher.research.data_source_config import make_ds
from edge_catcher.research.hypothesis import Hypothesis, HypothesisResult


def _make_collector(tmp_path: Path) -> ExportCollector:
	db_path = str(tmp_path / "research.db")
	return ExportCollector(db_path=db_path)


def _insert_result(tracker: Tracker, strategy: str = "test-strat", series: str = "SERIES_A",
                   verdict: str = "promote", sharpe: float = 5.0) -> str:
	"""Insert a hypothesis + result into tracker, return hypothesis ID."""
	h = Hypothesis(
		strategy=strategy,
		data_sources=make_ds(db="test.db", series=series),
		start_date="2025-01-01",
		end_date="2025-12-31",
		fee_pct=1.0,
	)
	result = HypothesisResult(
		hypothesis=h, status="ok", total_trades=100, wins=60, losses=40,
		win_rate=0.6, net_pnl_cents=500.0, sharpe=sharpe, max_drawdown_pct=10.0,
		fees_paid_cents=50.0, avg_win_cents=15.0, avg_loss_cents=-10.0,
		per_strategy={strategy: {"sharpe": sharpe}},
		verdict=verdict, verdict_reason="test", raw_json={},
	)
	tracker.save_result(result)
	return h.id


class TestCollectResults:
	def test_empty_db_returns_empty_strategies(self, tmp_path):
		collector = _make_collector(tmp_path)
		bundle = collector.collect()
		assert bundle["version"] == 1
		assert bundle["strategies"] == {}
		assert "exported_at" in bundle
		assert bundle["filter"]["verdicts"] == ["promote", "review"]

	def test_collects_promotes_grouped_by_strategy(self, tmp_path):
		collector = _make_collector(tmp_path)
		_insert_result(collector.tracker, strategy="alpha", series="S1", verdict="promote")
		_insert_result(collector.tracker, strategy="alpha", series="S2", verdict="promote")
		_insert_result(collector.tracker, strategy="beta", series="S1", verdict="review")
		_insert_result(collector.tracker, strategy="gamma", series="S1", verdict="kill")

		bundle = collector.collect()
		assert set(bundle["strategies"].keys()) == {"alpha", "beta"}
		assert len(bundle["strategies"]["alpha"]["results"]) == 2
		assert len(bundle["strategies"]["beta"]["results"]) == 1
		assert bundle["strategies"]["beta"]["results"][0]["verdict"] == "review"

	def test_result_fields_complete(self, tmp_path):
		collector = _make_collector(tmp_path)
		_insert_result(collector.tracker, strategy="alpha", series="S1",
		               verdict="promote", sharpe=5.5)
		bundle = collector.collect()
		result = bundle["strategies"]["alpha"]["results"][0]
		required_fields = [
			"hypothesis_id", "strategy", "series", "db_path", "start_date",
			"end_date", "fee_pct", "verdict", "status", "sharpe", "win_rate",
			"net_pnl_cents", "max_drawdown_pct", "total_trades", "wins",
			"losses", "fees_paid_cents", "avg_win_cents", "avg_loss_cents",
			"verdict_reason", "validation_details", "completed_at", "audit",
		]
		for field in required_fields:
			assert field in result, f"Missing field: {field}"
		assert result["sharpe"] == 5.5

	def test_custom_verdicts_filter(self, tmp_path):
		collector = _make_collector(tmp_path)
		_insert_result(collector.tracker, strategy="alpha", series="S1", verdict="promote")
		_insert_result(collector.tracker, strategy="beta", series="S1", verdict="kill")

		with patch("edge_catcher.research.export.ResearchAgent.read_strategy_code",
		           return_value=None):
			bundle = collector.collect(verdicts=["kill"])
		assert set(bundle["strategies"].keys()) == {"beta"}
		assert bundle["filter"]["verdicts"] == ["kill"]


class TestCollectSource:
	def test_attaches_strategy_source(self, tmp_path):
		collector = _make_collector(tmp_path)
		_insert_result(collector.tracker, strategy="my-strat", series="S1", verdict="promote")

		fake_code = 'class MyStrat(Strategy):\n\tname = "my-strat"\n\tdef evaluate(self): pass'
		with patch("edge_catcher.research.export.ResearchAgent.read_strategy_code",
		           return_value=fake_code):
			bundle = collector.collect()
		assert bundle["strategies"]["my-strat"]["source"] == fake_code

	def test_missing_source_is_none(self, tmp_path):
		collector = _make_collector(tmp_path)
		_insert_result(collector.tracker, strategy="gone-strat", series="S1", verdict="promote")

		with patch("edge_catcher.research.export.ResearchAgent.read_strategy_code",
		           return_value=None):
			bundle = collector.collect()
		assert bundle["strategies"]["gone-strat"]["source"] is None


class TestCollectJournalAndAudit:
	def test_attaches_journal_entries(self, tmp_path):
		collector = _make_collector(tmp_path)
		_insert_result(collector.tracker, strategy="alpha", series="S1", verdict="promote")

		from edge_catcher.research.journal import ResearchJournal
		journal = ResearchJournal(db_path=str(tmp_path / "research.db"))
		journal.write_entry("run-1", "outcome", {
			"phase": "grid", "strategy": "alpha",
			"series": ["S1"], "best_sharpe": 5.0,
		})

		with patch("edge_catcher.research.export.ResearchAgent.read_strategy_code",
		           return_value=None):
			bundle = collector.collect()
		entries = bundle["strategies"]["alpha"]["journal_entries"]
		assert len(entries) >= 1
		assert entries[0]["entry_type"] == "outcome"

	def test_journal_skips_entries_without_strategy(self, tmp_path):
		collector = _make_collector(tmp_path)
		_insert_result(collector.tracker, strategy="alpha", series="S1", verdict="promote")

		from edge_catcher.research.journal import ResearchJournal
		journal = ResearchJournal(db_path=str(tmp_path / "research.db"))
		journal.write_entry("run-1", "trajectory", {"status": "stuck", "total_sessions": 3})

		with patch("edge_catcher.research.export.ResearchAgent.read_strategy_code",
		           return_value=None):
			bundle = collector.collect()
		assert bundle["strategies"]["alpha"]["journal_entries"] == []

	def test_attaches_audit_records(self, tmp_path):
		collector = _make_collector(tmp_path)
		hid = _insert_result(collector.tracker, strategy="alpha", series="S1", verdict="promote")

		from edge_catcher.research.audit import AuditLog
		audit = AuditLog(str(tmp_path / "research.db"))
		audit.record_execution(
			hypothesis_id=hid, phase="grid", queue_position=0,
			verdict="promote", status="ok",
		)

		with patch("edge_catcher.research.export.ResearchAgent.read_strategy_code",
		           return_value=None):
			bundle = collector.collect()
		result = bundle["strategies"]["alpha"]["results"][0]
		assert len(result["audit"]) == 1
		assert result["audit"][0]["phase"] == "grid"
		assert result["audit"][0]["verdict"] == "promote"


class TestCollectConfig:
	def test_attaches_series_mapping(self, tmp_path):
		collector = _make_collector(tmp_path)
		_insert_result(collector.tracker, strategy="alpha", series="SERIES_A", verdict="promote")

		mapping = {"series_to_asset": {"SERIES_A": ["xrp", "ohlc.db", "xrp_ohlc"]}}
		mapping_path = tmp_path / "series_mapping.yaml"
		mapping_path.write_text(yaml.dump(mapping))

		with patch("edge_catcher.research.export.ResearchAgent.read_strategy_code",
		           return_value=None):
			bundle = collector.collect(series_mapping_path=mapping_path)
		assert "SERIES_A" in bundle["series_mapping"]
		assert bundle["series_mapping"]["SERIES_A"]["asset"] == "xrp"

	def test_attaches_hypothesis_config(self, tmp_path):
		collector = _make_collector(tmp_path)
		_insert_result(collector.tracker, strategy="alpha", series="S1", verdict="promote")

		hyp_config = {"hypotheses": {"alpha_edge": {
			"name": "Alpha Edge", "module": "edge_catcher.hypotheses.alpha",
			"status": "exploratory",
		}}}
		hyp_path = tmp_path / "hypotheses.yaml"
		hyp_path.write_text(yaml.dump(hyp_config))

		with patch("edge_catcher.research.export.ResearchAgent.read_strategy_code",
		           return_value=None):
			bundle = collector.collect(hypotheses_path=hyp_path)
		assert bundle["strategies"]["alpha"]["hypothesis_config"] is not None
		assert bundle["strategies"]["alpha"]["hypothesis_config"]["status"] == "exploratory"

	def test_missing_config_files_graceful(self, tmp_path):
		collector = _make_collector(tmp_path)
		_insert_result(collector.tracker, strategy="alpha", series="S1", verdict="promote")

		with patch("edge_catcher.research.export.ResearchAgent.read_strategy_code",
		           return_value=None):
			bundle = collector.collect(
				series_mapping_path=tmp_path / "nonexistent.yaml",
				hypotheses_path=tmp_path / "also_nonexistent.yaml",
			)
		assert bundle["series_mapping"] == {}
		assert bundle["strategies"]["alpha"]["hypothesis_config"] is None


class TestWriteZip:
	def test_creates_zip_with_manifest(self, tmp_path):
		collector = _make_collector(tmp_path)
		_insert_result(collector.tracker, strategy="alpha", series="S1", verdict="promote")

		with patch("edge_catcher.research.export.ResearchAgent.read_strategy_code",
		           return_value=None):
			bundle = collector.collect()
		output_dir = tmp_path / "exports"
		zip_path = collector.write_zip(bundle, output_dir=str(output_dir))

		assert zip_path.exists()
		assert zip_path.suffix == ".zip"
		with zipfile.ZipFile(zip_path) as zf:
			assert "manifest.json" in zf.namelist()
			manifest = json.loads(zf.read("manifest.json"))
			assert manifest["version"] == 1
			assert "alpha" in manifest["strategies"]

	def test_creates_output_dir_if_missing(self, tmp_path):
		collector = _make_collector(tmp_path)
		bundle = collector.collect()
		output_dir = tmp_path / "new" / "nested" / "dir"
		zip_path = collector.write_zip(bundle, output_dir=str(output_dir))
		assert zip_path.exists()

	def test_avoids_overwriting_existing_zip(self, tmp_path):
		collector = _make_collector(tmp_path)
		bundle = collector.collect()
		output_dir = str(tmp_path / "exports")
		zip1 = collector.write_zip(bundle, output_dir=output_dir)
		zip2 = collector.write_zip(bundle, output_dir=output_dir)
		assert zip1 != zip2
		assert zip1.exists()
		assert zip2.exists()


class TestExportCLI:
	def test_export_handler_creates_zip(self, tmp_path):
		"""Test the CLI handler function directly."""
		from edge_catcher.cli.research import _run_export

		db_path = str(tmp_path / "research.db")
		tracker = Tracker(db_path)
		_insert_result(tracker, strategy="alpha", series="S1", verdict="promote")

		output_dir = str(tmp_path / "exports")
		args = argparse.Namespace(
			research_db=db_path,
			export_output_dir=output_dir,
		)

		with patch("edge_catcher.research.export.ResearchAgent.read_strategy_code",
		           return_value=None):
			_run_export(args)

		exports = list(Path(output_dir).glob("*.zip"))
		assert len(exports) == 1


class TestIntegration:
	def test_full_export_roundtrip(self, tmp_path):
		"""Insert multiple strategies, export, unzip, verify manifest."""
		collector = _make_collector(tmp_path)

		# Insert diverse results
		_insert_result(collector.tracker, strategy="alpha", series="S1", verdict="promote", sharpe=5.0)
		_insert_result(collector.tracker, strategy="alpha", series="S2", verdict="review", sharpe=2.5)
		_insert_result(collector.tracker, strategy="beta", series="S1", verdict="promote", sharpe=3.0)
		_insert_result(collector.tracker, strategy="gamma", series="S1", verdict="kill", sharpe=0.5)

		with patch("edge_catcher.research.export.ResearchAgent.read_strategy_code",
		           side_effect=lambda name: f"class {name.title()}(Strategy): pass" if name != "gamma" else None):
			bundle = collector.collect()

		zip_path = collector.write_zip(bundle, output_dir=str(tmp_path / "out"))

		# Roundtrip: unzip and parse
		with zipfile.ZipFile(zip_path) as zf:
			assert zf.namelist() == ["manifest.json"]
			manifest = json.loads(zf.read("manifest.json"))

		assert manifest["version"] == 1
		assert set(manifest["strategies"].keys()) == {"alpha", "beta"}
		assert len(manifest["strategies"]["alpha"]["results"]) == 2
		assert len(manifest["strategies"]["beta"]["results"]) == 1
		# gamma (kill) excluded
		assert "gamma" not in manifest["strategies"]
		# Source attached
		assert manifest["strategies"]["alpha"]["source"] is not None
		assert manifest["strategies"]["beta"]["source"] is not None