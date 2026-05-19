"""Live trader configuration — Pydantic v2 Settings + yaml loader."""

from __future__ import annotations
from pathlib import Path
from typing import Annotated

import yaml
from pydantic import BaseModel, Field, field_validator

# Hardcoded floor — never lowered by config. Defense against catastrophic bug.
ABSOLUTE_MAX_ORDER_DOLLARS: float = 50.0

# Hardcoded floor for the CLI cap — config can raise but not lower below this.
CLI_CAP_FLOOR_DOLLARS: float = 1.0

DEFAULT_CONFIG_PATH = Path("config.local/live-trader.yaml")
DEFAULT_AUDIT_LOG_PATH = Path("data/live_audit.jsonl")


class LiveConfig(BaseModel):
	"""Loaded from `config.local/live-trader.yaml`."""

	# SC-E3a (spec §10 / §8): the LOCKED §8 design is ONE combined
	# `config.local/live-trader.yaml` that also carries the engine's
	# executor/risk/execution/strategies/db_path/notifications blocks. The
	# live-trader composition root (`engine._compose_live`) loads ONLY this
	# `LiveConfig` 5-field client subset from that same file; `extra: forbid`
	# would hard-reject the engine's sibling keys (`extra_forbidden`) and make
	# the combined file structurally unloadable. `ignore` relaxes ONLY
	# UNDECLARED keys — the 5 declared client fields below stay strictly
	# validated (type coercion + the `cli_max_order_dollars` field_validator
	# still enforce the ABSOLUTE_MAX / floor). A's `live/cli.py` is unchanged
	# (it already `getattr`s `db_path` with a default — an ignored key simply
	# stays absent, exactly its prior behaviour).
	model_config = {"extra": "ignore"}

	cli_max_order_dollars: Annotated[float, Field(gt=0)] = 1.0
	audit_log_path: Path = DEFAULT_AUDIT_LOG_PATH
	# Kalshi API host — overridable for tests; defaults to prod.
	# NOTE: this is the HOST only. The /trade-api/v2 prefix is owned by client.py
	# and prepended to every path so that the path used for signing exactly
	# matches the path sent on the wire. See client._KALSHI_REST_PREFIX.
	kalshi_rest_base: str = "https://api.elections.kalshi.com"
	http_timeout_seconds: float = 30.0
	max_retries: int = 5

	@field_validator("cli_max_order_dollars")
	@classmethod
	def _validate_cap(cls, v: float) -> float:
		if v < CLI_CAP_FLOOR_DOLLARS:
			raise ValueError(
				f"cli_max_order_dollars={v} below floor ${CLI_CAP_FLOOR_DOLLARS}"
			)
		if v > ABSOLUTE_MAX_ORDER_DOLLARS:
			raise ValueError(
				f"cli_max_order_dollars={v} exceeds ABSOLUTE_MAX ${ABSOLUTE_MAX_ORDER_DOLLARS}"
			)
		return v


def load_config(path: Path = DEFAULT_CONFIG_PATH) -> LiveConfig:
	"""Read and validate live-trader.yaml. Returns LiveConfig with defaults filled in."""
	if not path.exists():
		return LiveConfig()
	with path.open("r") as fh:
		data = yaml.safe_load(fh) or {}
	return LiveConfig.model_validate(data)
