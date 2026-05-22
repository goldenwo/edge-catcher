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


def test_extra_keys_ignored():
	"""SC-E3a (spec §10 / §8): the LOCKED §8 design is ONE combined
	``config.local/live-trader.yaml`` that ALSO carries the engine's
	executor/risk/execution/strategies/db_path/notifications sibling blocks.
	``LiveConfig`` was deliberately relaxed from ``extra: forbid`` to
	``extra: ignore`` so the composition root can load ONLY its 5-field client
	subset from that same file without ``extra: forbid`` hard-rejecting the
	engine's sibling keys (which would make the combined file structurally
	unloadable).

	Contract under test: constructing ``LiveConfig`` with an unknown/extra key
	SUCCEEDS and the extra key is IGNORED (dropped — not present on the model,
	not in ``model_dump()``; with pydantic-v2 ``extra: ignore`` it is not even
	retained in ``model_extra``).

	CRITICAL — the relaxation is scoped to UNDECLARED keys ONLY: the 5 declared
	client fields stay STRICTLY validated. This test pins that directly (a
	declared field with a bad value STILL raises even under ``extra: ignore``)
	so funds-config safety coverage is preserved/improved, not reduced. The
	per-field strictness of ``cli_max_order_dollars`` (the funds cap) is
	additionally covered by the sibling tests ``test_cli_max_below_floor_
	rejected``, ``test_cli_max_above_absolute_max_rejected`` and
	``test_load_config_invalid_yaml_field_raises``.
	"""
	# Extra/undeclared keys (incl. the engine's sibling `db_path`) are
	# accepted and silently dropped.
	cfg = LiveConfig(unknown_field=42, db_path="data/live_trades.db")
	assert not hasattr(cfg, "unknown_field"), (
		"an undeclared key must NOT become a model attribute (extra: ignore)"
	)
	assert not hasattr(cfg, "db_path"), (
		"the engine's sibling `db_path` key must be ignored by the client "
		"LiveConfig subset, not surfaced on the model"
	)
	assert "unknown_field" not in cfg.model_dump(), (
		"ignored keys must not leak into model_dump()"
	)
	assert cfg.model_extra in (None, {}), (
		"extra: ignore must DROP extras (not retain them in model_extra; "
		"that would be extra: allow)"
	)
	# The 5 declared fields are unaffected — defaults intact.
	assert cfg.cli_max_order_dollars == 1.0
	assert set(cfg.model_dump()) == {
		"cli_max_order_dollars",
		"audit_log_path",
		"kalshi_rest_base",
		"http_timeout_seconds",
		"max_retries",
	}

	# Funds-config safety is NOT relaxed: a DECLARED field with an
	# out-of-range value STILL raises even though extras are ignored. This
	# proves `extra: ignore` did not weaken declared-field validation.
	with pytest.raises(Exception, match="exceeds ABSOLUTE_MAX"):
		LiveConfig(unknown_field=42, cli_max_order_dollars=999.0)


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
