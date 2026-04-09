# tests/test_audit.py
"""Tests for edge_catcher.research.audit module."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from edge_catcher.research.audit import AuditLog


class TestAuditLogInit:
    def test_creates_tables(self, tmp_path):
        audit = AuditLog(tmp_path / "research.db")
        conn = sqlite3.connect(str(tmp_path / "research.db"))
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        conn.close()
        assert "audit_decisions" in tables
        assert "audit_executions" in tables
        assert "audit_integrity" in tables

    def test_coexists_with_existing_db(self, tmp_path):
        """AuditLog should not break an existing research.db."""
        db_path = tmp_path / "research.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE hypotheses (id TEXT PRIMARY KEY)")
        conn.commit()
        conn.close()
        audit = AuditLog(db_path)
        conn = sqlite3.connect(str(db_path))
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        conn.close()
        assert "hypotheses" in tables
        assert "audit_decisions" in tables


class TestAuditLogDecisions:
    def test_record_decision(self, tmp_path):
        audit = AuditLog(tmp_path / "research.db")
        audit.record_decision(
            prompt_hash="abc123",
            prompt_text="test prompt",
            response_text="test response",
            parsed_output={"hypotheses": ["h1"]},
            model="claude-sonnet-4-20250514",
            token_count=100,
        )
        rows = audit.list_decisions()
        assert len(rows) == 1
        assert rows[0]["prompt_hash"] == "abc123"
        assert rows[0]["model"] == "claude-sonnet-4-20250514"


class TestAuditLogExecutions:
    def test_record_execution(self, tmp_path):
        audit = AuditLog(tmp_path / "research.db")
        audit.record_execution(
            hypothesis_id="h-001",
            phase="grid",
            queue_position=0,
            verdict="promote",
            status="ok",
        )
        rows = audit.list_executions()
        assert len(rows) == 1
        assert rows[0]["hypothesis_id"] == "h-001"
        assert rows[0]["phase"] == "grid"

    def test_record_execution_with_error(self, tmp_path):
        audit = AuditLog(tmp_path / "research.db")
        audit.record_execution(
            hypothesis_id="h-002",
            phase="llm",
            queue_position=3,
            verdict="kill",
            status="error",
        )
        rows = audit.list_executions()
        assert len(rows) == 1
        assert rows[0]["status"] == "error"


class TestAuditLogIntegrity:
    def test_record_and_verify_integrity(self, tmp_path):
        audit = AuditLog(tmp_path / "research.db")
        audit.record_integrity(
            checkpoint="pre_llm_phase",
            result_hash="sha256_abc",
            result_count=42,
        )
        rows = audit.list_integrity_checks()
        assert len(rows) == 1
        assert rows[0]["checkpoint"] == "pre_llm_phase"
        assert rows[0]["result_hash"] == "sha256_abc"
        assert rows[0]["result_count"] == 42

    def test_compute_result_hash_deterministic(self, tmp_path):
        audit = AuditLog(tmp_path / "research.db")
        rows = [
            {"hypothesis_id": "a", "verdict": "promote", "sharpe": 2.5},
            {"hypothesis_id": "b", "verdict": "kill", "sharpe": 0.1},
        ]
        h1 = audit.compute_result_hash(rows)
        h2 = audit.compute_result_hash(rows)
        assert h1 == h2

    def test_compute_result_hash_order_independent(self, tmp_path):
        audit = AuditLog(tmp_path / "research.db")
        rows_a = [
            {"hypothesis_id": "b", "verdict": "kill"},
            {"hypothesis_id": "a", "verdict": "promote"},
        ]
        rows_b = [
            {"hypothesis_id": "a", "verdict": "promote"},
            {"hypothesis_id": "b", "verdict": "kill"},
        ]
        assert audit.compute_result_hash(rows_a) == audit.compute_result_hash(rows_b)


class TestAuditLogValidation:
    def test_validate_hypothesis_ok(self, tmp_path):
        # Create a fake db file so the "db exists" check passes
        fake_db = tmp_path / "data" / "kalshi.db"
        fake_db.parent.mkdir(parents=True)
        fake_db.touch()
        audit = AuditLog(tmp_path / "research.db")
        ok, err = audit.validate_hypothesis(
            strategy="C",
            series="SERIES_A",
            db_path=str(fake_db),
            start_date="2025-01-01",
            end_date="2025-12-31",
        )
        assert ok is True
        assert err is None

    def test_validate_hypothesis_missing_db(self, tmp_path):
        audit = AuditLog(tmp_path / "research.db")
        ok, err = audit.validate_hypothesis(
            strategy="C",
            series="SERIES_A",
            db_path="/nonexistent/path.db",
            start_date="2025-01-01",
            end_date="2025-12-31",
        )
        assert ok is False
        assert "db_path" in err.lower() or "not found" in err.lower()

    def test_validate_hypothesis_missing_fields(self, tmp_path):
        audit = AuditLog(tmp_path / "research.db")
        ok, err = audit.validate_hypothesis(
            strategy="",
            series="SERIES_A",
            db_path="data/kalshi.db",
            start_date="2025-01-01",
            end_date="2025-12-31",
        )
        assert ok is False

    def test_validate_result_consistency(self, tmp_path):
        audit = AuditLog(tmp_path / "research.db")
        ok, err = audit.validate_result_consistency(
            total_trades=100, wins=90, losses=10, status="ok"
        )
        assert ok is True

    def test_validate_result_inconsistent_counts(self, tmp_path):
        audit = AuditLog(tmp_path / "research.db")
        ok, err = audit.validate_result_consistency(
            total_trades=100, wins=90, losses=20, status="ok"
        )
        assert ok is False
        assert "wins + losses" in err.lower() or "inconsistent" in err.lower()
