"""Tests for AI integration modules. No actual API calls are made."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from edge_catcher.ai.client import LLMClient, LLMError
from edge_catcher.ai.formalizer import (
    _build_user_prompt as formalizer_user_prompt,
    _parse_llm_response,
    formalize,
)
from edge_catcher.ai.interpreter import (
    _build_user_prompt as interpreter_user_prompt,
    interpret,
)

# ── shared fixtures ───────────────────────────────────────────────────────────

SAMPLE_FORMALIZER_RESPONSE = """\
Here is the formalized hypothesis config:

```yaml
hypotheses:
  test_hypothesis:
    name: "Test Hypothesis"
    module: "edge_catcher.hypotheses.custom.test_hypothesis"
    market: kalshi
    status: exploratory
    rationale: >
      A test hypothesis used in unit tests.
    thresholds:
      t_stat: 3.0
      min_n_per_bucket: 30
      min_independent_obs: 80
      min_fee_adjusted_edge: 0.0
    fee_model: kalshi
    buckets:
      - [0.01, 0.10]
      - [0.10, 0.25]
    pass_criteria:
      clustered_z_stat: 3.0
      clustered_n: 80
      fee_adjusted_edge_positive: true
```

Economic intuition: this is only used for testing.
"""


def _clean_env() -> dict:
    """Return a copy of os.environ with all AI-related keys removed."""
    clean = os.environ.copy()
    for key in [
        "ANTHROPIC_API_KEY",
        "OPENAI_API_KEY",
        "OPENROUTER_API_KEY",
        "EDGE_CATCHER_LLM_PROVIDER",
        "EDGE_CATCHER_LLM_MODEL",
    ]:
        clean.pop(key, None)
    return clean


# ── LLMClient: provider auto-detection ───────────────────────────────────────

def test_client_no_api_key_raises():
    """complete() raises LLMError with a helpful message when no key and no CLI."""
    with patch.dict(os.environ, _clean_env(), clear=True), \
         patch("edge_catcher.ai.client.shutil.which", return_value=None):
        client = LLMClient()
        with pytest.raises(LLMError, match="API key"):
            client.complete("system", "user")


def test_client_auto_detect_anthropic():
    """ANTHROPIC_API_KEY present → provider resolves to 'anthropic'."""
    env = {**_clean_env(), "ANTHROPIC_API_KEY": "sk-ant-test"}
    with patch.dict(os.environ, env, clear=True):
        client = LLMClient()
    assert client.provider == "anthropic"


def test_client_auto_detect_openai_when_no_anthropic():
    """OPENAI_API_KEY present, ANTHROPIC absent → provider is 'openai'."""
    env = {**_clean_env(), "OPENAI_API_KEY": "sk-oai-test"}
    with patch.dict(os.environ, env, clear=True):
        client = LLMClient()
    assert client.provider == "openai"


def test_client_auto_detect_openrouter_last():
    """Only OPENROUTER_API_KEY set → provider is 'openrouter'."""
    env = {**_clean_env(), "OPENROUTER_API_KEY": "sk-or-test"}
    with patch.dict(os.environ, env, clear=True):
        client = LLMClient()
    assert client.provider == "openrouter"


def test_client_explicit_provider_overrides_env():
    """Explicit provider= argument overrides auto-detection."""
    env = {
        **_clean_env(),
        "ANTHROPIC_API_KEY": "sk-ant-test",
        "OPENAI_API_KEY": "sk-oai-test",
    }
    with patch.dict(os.environ, env, clear=True):
        client = LLMClient(provider="openai", api_key="sk-oai-test")
    assert client.provider == "openai"


def test_client_env_provider_var_overrides_key_detection():
    """EDGE_CATCHER_LLM_PROVIDER env var overrides API-key auto-detection."""
    env = {
        **_clean_env(),
        "ANTHROPIC_API_KEY": "sk-ant-test",
        "OPENAI_API_KEY": "sk-oai-test",
        "EDGE_CATCHER_LLM_PROVIDER": "openai",
    }
    with patch.dict(os.environ, env, clear=True):
        client = LLMClient()
    assert client.provider == "openai"


def test_client_resolve_model_with_no_provider():
    """_resolve_model does not crash when provider is None."""
    with patch.dict(os.environ, _clean_env(), clear=True), \
         patch("edge_catcher.ai.client.shutil.which", return_value=None):
        client = LLMClient()
    assert client._resolve_model("interpreter") is None


def test_client_missing_anthropic_package_raises():
    """ImportError on anthropic package → LLMError with install hint."""
    env = {**_clean_env(), "ANTHROPIC_API_KEY": "sk-ant-test"}
    with patch.dict(os.environ, env, clear=True):
        client = LLMClient()
    # Simulate package not installed by injecting None into sys.modules
    with patch.dict(sys.modules, {"anthropic": None}):
        with pytest.raises(LLMError, match="pip install"):
            client.complete("sys", "user")


# ── Formalizer: prompt construction ──────────────────────────────────────────

def test_formalizer_user_prompt_includes_description():
    desc = "NFL underdogs cover the spread more often than the market expects"
    prompt = formalizer_user_prompt(desc)
    assert desc in prompt


# ── Formalizer: response parsing ──────────────────────────────────────────────

def test_formalizer_parse_valid_response():
    hyp_id, config = _parse_llm_response(SAMPLE_FORMALIZER_RESPONSE)
    assert hyp_id == "test_hypothesis"
    assert config["status"] == "exploratory"
    assert config["market"] == "kalshi"
    assert "thresholds" in config
    assert config["thresholds"]["t_stat"] == 3.0


def test_formalizer_parse_accepts_bare_dict():
    """YAML block without a top-level 'hypotheses:' key is also accepted."""
    response = """\
```yaml
bare_hyp:
  name: "Bare Dict"
  module: "edge_catcher.hypotheses.custom.bare_hyp"
  market: kalshi
  status: exploratory
  rationale: "bare dict test"
  thresholds:
    t_stat: 3.0
  fee_model: kalshi
  buckets: []
```
"""
    hyp_id, config = _parse_llm_response(response)
    assert hyp_id == "bare_hyp"
    assert config["name"] == "Bare Dict"


def test_formalizer_parse_missing_yaml_raises():
    with pytest.raises(ValueError, match="No YAML block"):
        _parse_llm_response("No YAML here, just plain text without fences.")


# ── Formalizer: file creation ─────────────────────────────────────────────────

def test_formalize_creates_config_entry_and_stub(tmp_path):
    """With a mocked LLM, verify the config is updated and a stub is created."""
    config_path = tmp_path / "hypotheses.yaml"
    config_path.write_text("hypotheses: {}\n")
    hypotheses_base = tmp_path / "hypotheses"
    hypotheses_base.mkdir()

    mock_client = MagicMock()
    mock_client.complete.return_value = SAMPLE_FORMALIZER_RESPONSE

    result = formalize(
        "A test hypothesis",
        mock_client,
        config_path=config_path,
        hypotheses_base=hypotheses_base,
    )

    assert not result.get("error")
    assert result["hypothesis_id"] == "test_hypothesis"

    # Config entry written correctly
    written = yaml.safe_load(config_path.read_text())
    assert "test_hypothesis" in written["hypotheses"]
    assert written["hypotheses"]["test_hypothesis"]["status"] == "exploratory"

    # Stub file created with required content
    stub_path = Path(result["module_path"])
    assert stub_path.exists()
    content = stub_path.read_text()
    assert 'HYPOTHESIS_ID = "test_hypothesis"' in content
    assert "def run(db_conn" in content


def test_formalize_stub_contains_hypothesis_id(tmp_path):
    """Stub module's HYPOTHESIS_ID constant matches the generated id."""
    config_path = tmp_path / "hypotheses.yaml"
    config_path.write_text("hypotheses: {}\n")
    hypotheses_base = tmp_path / "hypotheses"
    hypotheses_base.mkdir()

    mock_client = MagicMock()
    mock_client.complete.return_value = SAMPLE_FORMALIZER_RESPONSE

    result = formalize(
        "A test hypothesis",
        mock_client,
        config_path=config_path,
        hypotheses_base=hypotheses_base,
    )

    content = Path(result["module_path"]).read_text()
    assert f'HYPOTHESIS_ID = "{result["hypothesis_id"]}"' in content


def test_formalize_handles_malformed_llm_response(tmp_path, capsys):
    """Malformed LLM response prints a message and returns an error dict."""
    config_path = tmp_path / "hypotheses.yaml"
    config_path.write_text("hypotheses: {}\n")

    mock_client = MagicMock()
    mock_client.complete.return_value = "Sorry, I cannot formalize that right now."

    result = formalize("bad input", mock_client, config_path=config_path)

    assert result.get("error") is True
    assert "raw_response" in result
    captured = capsys.readouterr()
    assert "parse" in captured.out.lower() or "raw" in captured.out.lower()


def test_formalize_appends_to_existing_hypotheses(tmp_path):
    """formalize() appends to an existing yaml without clobbering other entries."""
    config_path = tmp_path / "hypotheses.yaml"
    existing_config = {
        "hypotheses": {
            "existing_hyp": {
                "name": "Existing",
                "module": "edge_catcher.hypotheses.custom.existing_hyp",
                "market": "kalshi",
                "status": "exploratory",
            }
        }
    }
    config_path.write_text(yaml.dump(existing_config))
    hypotheses_base = tmp_path / "hypotheses"
    hypotheses_base.mkdir()

    mock_client = MagicMock()
    mock_client.complete.return_value = SAMPLE_FORMALIZER_RESPONSE

    formalize(
        "A second hypothesis",
        mock_client,
        config_path=config_path,
        hypotheses_base=hypotheses_base,
    )

    written = yaml.safe_load(config_path.read_text())
    assert "existing_hyp" in written["hypotheses"]
    assert "test_hypothesis" in written["hypotheses"]


# ── Interpreter: prompt construction ─────────────────────────────────────────

def test_interpreter_user_prompt_includes_report_fields():
    report = {
        "verdict": "EDGE_EXISTS",
        "hypothesis_id": "kalshi_hypothesis",
        "clustered_z_stat": 3.5,
    }
    prompt = interpreter_user_prompt(report)
    assert "EDGE_EXISTS" in prompt
    assert "kalshi_hypothesis" in prompt
    assert "3.5" in prompt


# ── Interpreter: file handling ────────────────────────────────────────────────

def test_interpret_missing_file_raises(tmp_path):
    mock_client = MagicMock()
    with pytest.raises(FileNotFoundError):
        interpret(tmp_path / "nonexistent.json", mock_client)


def test_interpret_returns_llm_summary(tmp_path):
    """interpret() passes the report JSON to the LLM and returns its response."""
    report = {
        "hypothesis_id": "kalshi_hypothesis",
        "verdict": "NO_EDGE",
        "clustered_z_stat": 1.1,
        "naive_n": 150,
    }
    report_path = tmp_path / "analysis.json"
    report_path.write_text(json.dumps(report))

    expected_summary = (
        "This analysis found NO_EDGE. Sample size was too small for a conclusion."
    )
    mock_client = MagicMock()
    mock_client.complete.return_value = expected_summary

    summary = interpret(report_path, mock_client)

    assert summary == expected_summary
    mock_client.complete.assert_called_once()
    # Verify task='interpreter' was passed as a keyword argument
    call_kwargs = mock_client.complete.call_args.kwargs
    assert call_kwargs.get("task") == "interpreter"


def test_interpret_passes_full_report_to_llm(tmp_path):
    """The user prompt sent to the LLM contains the report's JSON content."""
    report = {"verdict": "INCONCLUSIVE", "hypothesis_id": "test", "naive_n": 45}
    report_path = tmp_path / "analysis.json"
    report_path.write_text(json.dumps(report))

    mock_client = MagicMock()
    mock_client.complete.return_value = "This analysis found INCONCLUSIVE."

    interpret(report_path, mock_client)

    call_args = mock_client.complete.call_args
    user_prompt = call_args.args[1]  # second positional arg
    assert "INCONCLUSIVE" in user_prompt
    assert "test" in user_prompt


# ── LLMClient: model override ─────────────────────────────────────────────────

def test_client_explicit_model_overrides_default():
    """Explicit model= kwarg is used for all tasks regardless of provider defaults."""
    env = {**_clean_env(), "ANTHROPIC_API_KEY": "sk-ant-test"}
    with patch.dict(os.environ, env, clear=True):
        client = LLMClient(model="claude-opus-4-20250514")
    assert client._resolve_model("interpreter") == "claude-opus-4-20250514"
    assert client._resolve_model("formalizer") == "claude-opus-4-20250514"


def test_client_env_model_override():
    """EDGE_CATCHER_LLM_MODEL env var is passed through by callers."""
    env = {
        **_clean_env(),
        "ANTHROPIC_API_KEY": "sk-ant-test",
        "EDGE_CATCHER_LLM_MODEL": "claude-opus-4-20250514",
    }
    with patch.dict(os.environ, env, clear=True):
        model_override = os.getenv("EDGE_CATCHER_LLM_MODEL") or None
        client = LLMClient(model=model_override)
    assert client._resolve_model("interpreter") == "claude-opus-4-20250514"
    assert client._resolve_model("strategizer") == "claude-opus-4-20250514"
