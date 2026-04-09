"""Tests for model settings API endpoints."""
from __future__ import annotations

import os
from unittest.mock import patch

import pytest
pytest.importorskip("fastapi", reason="fastapi not installed")
from fastapi.testclient import TestClient

from api.main import app


@pytest.fixture
def client():
    return TestClient(app)


def _clean_env() -> dict:
    clean = os.environ.copy()
    for key in [
        "ANTHROPIC_API_KEY", "OPENAI_API_KEY", "OPENROUTER_API_KEY",
        "EDGE_CATCHER_LLM_PROVIDER", "EDGE_CATCHER_LLM_MODEL",
    ]:
        clean.pop(key, None)
    return clean


def test_get_models_anthropic(client):
    """GET /api/settings/ai/models returns Anthropic models when key is set."""
    env = {**_clean_env(), "ANTHROPIC_API_KEY": "sk-ant-test"}
    with patch.dict(os.environ, env, clear=True):
        resp = client.get("/api/settings/ai/models")
    assert resp.status_code == 200
    data = resp.json()
    assert data["provider"] == "anthropic"
    assert data["current_model"] is None
    assert len(data["models"]) == 3
    ids = [m["id"] for m in data["models"]]
    assert "claude-haiku-4-5-20251001" in ids


def test_get_models_no_provider(client):
    """GET /api/settings/ai/models returns empty when no provider configured and no CLI on PATH."""
    import shutil
    with patch.dict(os.environ, _clean_env(), clear=True), \
            patch.object(shutil, "which", return_value=None):
        resp = client.get("/api/settings/ai/models")
    assert resp.status_code == 200
    data = resp.json()
    assert data["provider"] is None
    assert data["models"] == []


def test_get_models_with_current(client):
    """GET /api/settings/ai/models returns current_model from env."""
    env = {
        **_clean_env(),
        "ANTHROPIC_API_KEY": "sk-ant-test",
        "EDGE_CATCHER_LLM_MODEL": "claude-sonnet-4-20250514",
    }
    with patch.dict(os.environ, env, clear=True):
        resp = client.get("/api/settings/ai/models")
    assert resp.json()["current_model"] == "claude-sonnet-4-20250514"


def test_save_model_valid(client, tmp_path, monkeypatch):
    """POST /api/settings/ai/model with valid model succeeds."""
    env_file = tmp_path / ".env"
    env_file.write_text("")
    monkeypatch.chdir(tmp_path)
    env = {**_clean_env(), "ANTHROPIC_API_KEY": "sk-ant-test"}
    with patch.dict(os.environ, env, clear=True):
        resp = client.post("/api/settings/ai/model", json={"model": "claude-sonnet-4-20250514"})
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}


def test_save_model_invalid_returns_400(client):
    """POST /api/settings/ai/model with invalid model returns 400."""
    env = {**_clean_env(), "ANTHROPIC_API_KEY": "sk-ant-test"}
    with patch.dict(os.environ, env, clear=True):
        resp = client.post("/api/settings/ai/model", json={"model": "invalid-model"})
    assert resp.status_code == 400


def test_save_model_null_clears(client, tmp_path, monkeypatch):
    """POST /api/settings/ai/model with null clears the override."""
    env_file = tmp_path / ".env"
    env_file.write_text("EDGE_CATCHER_LLM_MODEL=claude-sonnet-4-20250514\n")
    monkeypatch.chdir(tmp_path)
    env = {
        **_clean_env(),
        "ANTHROPIC_API_KEY": "sk-ant-test",
        "EDGE_CATCHER_LLM_MODEL": "claude-sonnet-4-20250514",
    }
    with patch.dict(os.environ, env, clear=True):
        resp = client.post("/api/settings/ai/model", json={"model": None})
    assert resp.status_code == 200
    assert "EDGE_CATCHER_LLM_MODEL" not in os.environ
