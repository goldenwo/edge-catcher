"""Tests for hypothesis registry: discovery and run tracking."""

import pytest
from pathlib import Path

from edge_catcher.hypotheses.registry import discover, run_all, run_hypothesis
from edge_catcher.storage.db import get_connection
from edge_catcher.storage.models import HypothesisResult

CONFIG = Path("config")


# ---------------------------------------------------------------------------
# discover()
# ---------------------------------------------------------------------------

def test_discover_finds_example_hypothesis():
    modules = discover(CONFIG)
    assert "example_hypothesis" in modules


def test_discover_module_has_run_function():
    modules = discover(CONFIG)
    assert hasattr(modules["example_hypothesis"], "run")


# ---------------------------------------------------------------------------
# run_hypothesis()
# ---------------------------------------------------------------------------

def test_run_hypothesis_returns_hypothesis_result(tmp_db_path):
    conn = get_connection(tmp_db_path)
    result = run_hypothesis("example_hypothesis", conn, CONFIG)
    conn.close()
    assert isinstance(result, HypothesisResult)


def test_run_hypothesis_unknown_id_raises(tmp_db_path):
    conn = get_connection(tmp_db_path)
    with pytest.raises(ValueError, match="not found"):
        run_hypothesis("nonexistent_hypothesis", conn, CONFIG)
    conn.close()


def test_run_hypothesis_result_has_verdict(tmp_db_path):
    conn = get_connection(tmp_db_path)
    result = run_hypothesis("example_hypothesis", conn, CONFIG)
    conn.close()
    assert result.verdict in (
        "INSUFFICIENT_DATA",
        "NO_EDGE",
        "INCONCLUSIVE",
        "EDGE_EXISTS",
        "EDGE_NOT_TRADEABLE",
    )


# ---------------------------------------------------------------------------
# run_all()
# ---------------------------------------------------------------------------

def test_run_all_returns_dict(tmp_db_path):
    conn = get_connection(tmp_db_path)
    results = run_all(conn, CONFIG)
    conn.close()
    assert isinstance(results, dict)
    assert len(results) >= 1


def test_run_all_includes_example_hypothesis(tmp_db_path):
    conn = get_connection(tmp_db_path)
    results = run_all(conn, CONFIG)
    conn.close()
    assert "example_hypothesis" in results
    assert isinstance(results["example_hypothesis"], HypothesisResult)
