"""Tests for edge_catcher.live.config."""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from edge_catcher.live.config import (
	ABSOLUTE_MAX_ORDER_DOLLARS,
	CLI_CAP_FLOOR_DOLLARS,
	LiveConfig,
	load_config,
)


def test_constants_match_spec():
	assert ABSOLUTE_MAX_ORDER_DOLLARS == 50.0
	assert CLI_CAP_FLOOR_DOLLARS == 1.0


def test_default_config_validates():
	cfg = LiveConfig()
	assert cfg.cli_max_order_dollars == 1.0
	assert cfg.audit_log_path == Path("data/live_audit.jsonl")
	assert cfg.kalshi_rest_base == "https://api.elections.kalshi.com"
	assert cfg.http_timeout_seconds == 30.0
	assert cfg.max_retries == 5


def test_cli_max_below_floor_rejected():
	with pytest.raises(Exception, match="below floor"):
		LiveConfig(cli_max_order_dollars=0.5)


def test_cli_max_above_absolute_max_rejected():
	with pytest.raises(Exception, match="exceeds ABSOLUTE_MAX"):
		LiveConfig(cli_max_order_dollars=51.0)


def test_cli_max_within_range_accepted():
	cfg = LiveConfig(cli_max_order_dollars=5.0)
	assert cfg.cli_max_order_dollars == 5.0


def test_extra_keys_forbidden():
	with pytest.raises(Exception):
		LiveConfig(unknown_field=42)


def test_load_config_missing_file_returns_defaults(tmp_path):
	cfg = load_config(tmp_path / "nonexistent.yaml")
	assert cfg.cli_max_order_dollars == 1.0


def test_load_config_reads_yaml(tmp_path):
	p = tmp_path / "live-trader.yaml"
	p.write_text(yaml.safe_dump({
		"cli_max_order_dollars": 3.0,
		"audit_log_path": "tmp/audit.jsonl",
		"max_retries": 2,
	}))
	cfg = load_config(p)
	assert cfg.cli_max_order_dollars == 3.0
	assert cfg.audit_log_path == Path("tmp/audit.jsonl")
	assert cfg.max_retries == 2


def test_load_config_invalid_yaml_field_raises(tmp_path):
	p = tmp_path / "live-trader.yaml"
	p.write_text(yaml.safe_dump({"cli_max_order_dollars": 100.0}))
	with pytest.raises(Exception, match="exceeds"):
		load_config(p)
