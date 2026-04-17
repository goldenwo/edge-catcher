"""Tests for api/config_helpers and edge_catcher/ai/client.detect_active_provider."""
from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest


# ── detect_active_provider ────────────────────────────────────────────────────

def test_detect_active_provider_env_var():
	"""EDGE_CATCHER_LLM_PROVIDER env var takes highest priority."""
	from edge_catcher.ai.client import detect_active_provider
	env = {
		"EDGE_CATCHER_LLM_PROVIDER": "openai",
		"ANTHROPIC_API_KEY": "sk-ant-test",
	}
	with patch.dict(os.environ, env, clear=True):
		assert detect_active_provider() == "openai"


def test_detect_active_provider_anthropic_key():
	"""ANTHROPIC_API_KEY detected before other keys."""
	from edge_catcher.ai.client import detect_active_provider
	env = {
		"ANTHROPIC_API_KEY": "sk-ant-test",
		"OPENAI_API_KEY": "sk-openai-test",
	}
	with patch.dict(os.environ, env, clear=True):
		assert detect_active_provider() == "anthropic"


def test_detect_active_provider_openai_key():
	"""OPENAI_API_KEY detected when no Anthropic key."""
	from edge_catcher.ai.client import detect_active_provider
	env = {"OPENAI_API_KEY": "sk-openai-test"}
	with patch.dict(os.environ, env, clear=True):
		assert detect_active_provider() == "openai"


def test_detect_active_provider_openrouter_key():
	"""OPENROUTER_API_KEY detected when no other keys."""
	from edge_catcher.ai.client import detect_active_provider
	env = {"OPENROUTER_API_KEY": "or-test"}
	with patch.dict(os.environ, env, clear=True):
		assert detect_active_provider() == "openrouter"


def test_detect_active_provider_none_no_cli(monkeypatch):
	"""Returns None when no keys set and CLI tools not found."""
	from edge_catcher.ai.client import detect_active_provider
	import shutil
	monkeypatch.setattr(shutil, "which", lambda _cmd: None)
	with patch.dict(os.environ, {}, clear=True):
		assert detect_active_provider() is None


def test_detect_active_provider_claude_code_cli(monkeypatch):
	"""Returns 'claude-code' when claude CLI is on PATH and no keys set."""
	from edge_catcher.ai.client import detect_active_provider
	import shutil
	# Simulate 'claude' found on PATH but no 'npx'
	monkeypatch.setattr(shutil, "which", lambda cmd: "/usr/bin/claude" if cmd == "claude" else None)
	with patch.dict(os.environ, {}, clear=True):
		assert detect_active_provider() == "claude-code"


def test_llm_client_uses_detect_active_provider():
	"""LLMClient._resolve_provider delegates to detect_active_provider when no explicit provider."""
	from edge_catcher.ai.client import LLMClient
	env = {"ANTHROPIC_API_KEY": "sk-ant-test"}
	with patch.dict(os.environ, env, clear=True):
		client = LLMClient()
	assert client.provider == "anthropic"


# ── config_helpers ────────────────────────────────────────────────────────────

def test_config_path_default():
	"""config_path() returns Path('config') by default."""
	from api.config_helpers import config_path
	with patch.dict(os.environ, {}, clear=True):
		assert config_path() == Path("config")


def test_config_path_env_override(tmp_path):
	"""config_path() honours CONFIG_PATH env var."""
	from api.config_helpers import config_path
	with patch.dict(os.environ, {"CONFIG_PATH": str(tmp_path)}, clear=True):
		assert config_path() == tmp_path


def test_markets_yaml():
	"""markets_yaml() always points to config/markets-btc.yaml (Kalshi BTC default)."""
	from api.config_helpers import markets_yaml
	assert markets_yaml() == Path("config") / "markets-btc.yaml"


def test_research_db_path_default():
	"""research_db_path() returns default path when env var absent."""
	from api.config_helpers import research_db_path
	with patch.dict(os.environ, {}, clear=True):
		assert research_db_path() == Path("data/research.db")


def test_research_db_path_env_override(tmp_path):
	"""research_db_path() honours RESEARCH_DB env var."""
	from api.config_helpers import research_db_path
	custom = str(tmp_path / "my_research.db")
	with patch.dict(os.environ, {"RESEARCH_DB": custom}, clear=True):
		assert research_db_path() == Path(custom)


def test_load_merged_hypotheses_empty(tmp_path, monkeypatch):
	"""load_merged_hypotheses() returns empty dict when no hypothesis files exist."""
	from api.config_helpers import load_merged_hypotheses
	monkeypatch.setattr("api.config_helpers.config_path", lambda: tmp_path / "config")
	# config.local doesn't exist in tmp_path either
	monkeypatch.chdir(tmp_path)
	assert load_merged_hypotheses() == {}


def test_load_merged_hypotheses_public_only(tmp_path, monkeypatch):
	"""load_merged_hypotheses() reads from config/ when config.local/ absent."""
	import yaml
	from api.config_helpers import load_merged_hypotheses

	cfg_dir = tmp_path / "config"
	cfg_dir.mkdir()
	(cfg_dir / "hypotheses.yaml").write_text(yaml.dump({
		"hypotheses": {
			"hyp_a": {"name": "Alpha", "market": "kalshi", "status": "active"},
		}
	}))

	monkeypatch.setattr("api.config_helpers.config_path", lambda: cfg_dir)
	monkeypatch.chdir(tmp_path)

	result = load_merged_hypotheses()
	assert "hyp_a" in result
	assert result["hyp_a"]["name"] == "Alpha"


def test_load_merged_hypotheses_local_overrides(tmp_path, monkeypatch):
	"""load_merged_hypotheses() lets config.local/ override config/."""
	import yaml
	from api.config_helpers import load_merged_hypotheses

	cfg_dir = tmp_path / "config"
	cfg_dir.mkdir()
	(cfg_dir / "hypotheses.yaml").write_text(yaml.dump({
		"hypotheses": {
			"hyp_a": {"name": "Alpha Public", "market": "kalshi", "status": "exploratory"},
			"hyp_b": {"name": "Beta", "market": "kalshi", "status": "active"},
		}
	}))

	local_dir = tmp_path / "config.local"
	local_dir.mkdir()
	(local_dir / "hypotheses.yaml").write_text(yaml.dump({
		"hypotheses": {
			"hyp_a": {"name": "Alpha Local", "market": "kalshi", "status": "active"},
		}
	}))

	monkeypatch.setattr("api.config_helpers.config_path", lambda: cfg_dir)
	monkeypatch.chdir(tmp_path)

	result = load_merged_hypotheses()
	# local override takes effect
	assert result["hyp_a"]["name"] == "Alpha Local"
	# public-only entry still present
	assert result["hyp_b"]["name"] == "Beta"


def test_validate_db_valid():
	"""validate_db() returns Path('data') / name for a valid db name."""
	from api.config_helpers import validate_db
	from dataclasses import dataclass

	@dataclass
	class MockAdapter:
		db_file: str

	# Mock ADAPTERS with a test database
	mock_adapters = [MockAdapter(db_file="data/kalshi-btc.db")]

	with patch("api.adapter_registry.ADAPTERS", mock_adapters):
		result = validate_db("kalshi-btc.db")
		assert result == Path("data") / "kalshi-btc.db"


def test_validate_db_invalid():
	"""validate_db() raises ValueError for an invalid db name."""
	from api.config_helpers import validate_db
	from dataclasses import dataclass

	@dataclass
	class MockAdapter:
		db_file: str

	# Mock ADAPTERS with a test database
	mock_adapters = [MockAdapter(db_file="data/kalshi-btc.db")]

	with patch("api.adapter_registry.ADAPTERS", mock_adapters):
		with pytest.raises(ValueError, match="Unknown database: invalid.db"):
			validate_db("invalid.db")
