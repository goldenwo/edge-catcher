"""J1 — `config/live-trader.example.yaml` is a coherence-PASSING skeleton.

The live trader boots from ONE combined config (`config.local/live-trader.yaml`,
the operator's gitignored real values). `config/live-trader.example.yaml` is the
TRACKED template the operator copies. It must be a faithful, self-documenting
skeleton that PASSES the fail-closed boot coherence gate
(`_assert_mode_coherence` in `edge_catcher/engine/engine.py`) for
`executor: live`, carrying the locked Phase-1 caps (spec §8) — otherwise an
operator who copies it gets a config that aborts at boot.

This is the coherence-acceptance test for J1:

  * POSITIVE — the real tracked example file, loaded through the SAME
    production loader `run_engine` uses (`engine.discovery.load_config`),
    with the trade-scope creds env vars stubbed (throwaway RSA-2048 keypair,
    mirroring the `signing_env` idiom in tests/test_live_client.py) and
    `notifications.config_path` pointed at the tracked
    `config/notifications.example.yaml` (whose channels the example names) —
    PASSES `_assert_mode_coherence` (does not raise) AND passes
    `validate_exec_cfg` on its `execution:` block (the other boot-time config
    gate `_compose_live` runs at T0).

  * NEGATIVE — three deep-copies of that same loaded config, each with
    exactly ONE coherence dimension broken (a Phase-1 cap removed; the
    dedicated risk channel removed; executor flipped to paper while the DB
    stays live) — each must raise RuntimeError (the `_coherence_fail`).

No real network, no real Kalshi keys, no real secrets.
"""
from __future__ import annotations

import copy
from pathlib import Path

import pytest

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa


# Repo-root-anchored paths to the TRACKED example files (this test must
# exercise the real shipped templates, not a synthetic copy).
_REPO_ROOT = Path(__file__).resolve().parent.parent
_LIVE_EXAMPLE = _REPO_ROOT / "config" / "live-trader.example.yaml"
_NOTIFY_EXAMPLE = _REPO_ROOT / "config" / "notifications.example.yaml"

# Default live trade-scope signing env-var names (auth.py:
# KALSHI_LIVE_KEY_ID_ENV / KALSHI_LIVE_PRIVATE_KEY_ENV — the
# `_DEFAULT_LIVE_KEY_ID_ENV` / `_DEFAULT_LIVE_PRIVATE_KEY_ENV` the §2.3
# coherence check resolves when the config names no override). The example
# deliberately uses these defaults (documents them in a comment, adds no
# `live_key_id_env` key) so this test stubs exactly them.
_LIVE_KEY_ID_ENV = "KALSHI_LIVE_KEY_ID"
_LIVE_PRIVATE_KEY_ENV = "KALSHI_LIVE_PRIVATE_KEY"

# `config/notifications.example.yaml` references these env vars in its
# webhook/smtp channels. `load_channels` pre-interpolates EVERY ${VAR} in the
# file before YAML parse and raises if any is unset — so to point the
# coherence check's `config_path` at the real notifications example, all of
# them must be stubbed (throwaway test values; no real secrets).
_NOTIFY_ENV_STUBS = {
	"DISCORD_PNL_WEBHOOK_URL": "https://example.invalid/discord-webhook",
	"SLACK_OPS_WEBHOOK_URL": "https://example.invalid/slack-webhook",
	"SMTP_USER": "test-smtp-user",
	"SMTP_PASSWORD": "test-smtp-password",
}


@pytest.fixture
def coherence_env(monkeypatch: pytest.MonkeyPatch) -> None:
	"""Stub the side state the live example's coherence checks resolve:

	  * the trade-scope signing creds (Check 3) — a throwaway RSA-2048
	    keypair in the DEFAULT live env vars (the example uses the defaults);
	  * the notifications-example env vars (so Check 4/4b can `load_channels`
	    the real `config/notifications.example.yaml` the example points at).

	No real network, no real Kalshi keys, no real secrets.
	"""
	key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
	pem = key.private_bytes(
		encoding=serialization.Encoding.PEM,
		format=serialization.PrivateFormat.PKCS8,
		encryption_algorithm=serialization.NoEncryption(),
	)
	monkeypatch.setenv(_LIVE_KEY_ID_ENV, "test-live-key")
	monkeypatch.setenv(_LIVE_PRIVATE_KEY_ENV, pem.decode())
	for var, val in _NOTIFY_ENV_STUBS.items():
		monkeypatch.setenv(var, val)


def _load_example_config() -> dict:
	"""Load `config/live-trader.example.yaml` through the SAME loader
	`run_engine` uses (`engine.discovery.load_config`), then redirect
	`notifications.config_path` from the operator's gitignored
	`config.local/notifications.yaml` (absent in CI) to the tracked
	`config/notifications.example.yaml` — the example NAMES channels that
	exist in that tracked notifications template, so this is the faithful
	resolution, not a synthetic stand-in.
	"""
	from edge_catcher.engine.discovery import load_config

	cfg = load_config(_LIVE_EXAMPLE)
	assert isinstance(cfg, dict) and cfg, "example yaml loaded empty"
	cfg.setdefault("notifications", {})
	cfg["notifications"]["config_path"] = str(_NOTIFY_EXAMPLE)
	return cfg


def test_example_yaml_files_exist() -> None:
	"""Guard: both tracked example files exist (a missing file would make
	the coherence assertions vacuously pass / fail for the wrong reason)."""
	assert _LIVE_EXAMPLE.is_file(), f"missing tracked example: {_LIVE_EXAMPLE}"
	assert _NOTIFY_EXAMPLE.is_file(), f"missing tracked example: {_NOTIFY_EXAMPLE}"


def test_live_example_passes_mode_coherence(coherence_env: None) -> None:
	"""POSITIVE — the real tracked `config/live-trader.example.yaml`, loaded
	via the production loader with creds/notify side state stubbed, PASSES
	`_assert_mode_coherence` (the fail-closed boot gate) for executor: live.

	If this raises, the shipped example is NOT a coherent live skeleton: an
	operator copying it would hit a boot abort.
	"""
	from edge_catcher.engine.engine import _assert_mode_coherence

	cfg = _load_example_config()
	# Must NOT raise — executor/db/creds/channel/risk-channel/caps all cohere.
	_assert_mode_coherence(cfg)


def test_live_example_execution_block_validates(coherence_env: None) -> None:
	"""POSITIVE — the example's `execution:` block passes `validate_exec_cfg`,
	the OTHER boot-time config gate `_compose_live` runs at T0 (a faithful
	template must clear every boot-time config check, not only coherence).
	"""
	from edge_catcher.engine.execution import validate_exec_cfg

	cfg = _load_example_config()
	# Raises TypeError/ValueError on a malformed/missing execution block.
	validate_exec_cfg(cfg.get("execution", {}))


def test_live_example_risk_block_is_exact_phase1(coherence_env: None) -> None:
	"""The example's `risk:` keys parse via the SAME `RiskConfig.from_dict`
	the live risk module uses, and carry the LOCKED Phase-1 caps (spec §8).
	Belt-and-braces over the coherence check so a silent value drift (e.g.
	sizing_pct bumped) is caught explicitly, not just structurally.
	"""
	from edge_catcher.engine.risk import RiskConfig

	cfg = _load_example_config()
	rc = RiskConfig.from_dict(cfg["risk"])
	assert rc.sizing_pct == 0.005
	assert rc.daily_loss_pct == 0.02
	assert rc.drawdown_pct == 0.05
	assert rc.max_open == 5
	assert rc.min_fill_contracts == 3
	assert rc.absolute_panic_floor_cents == 3000  # $30 static floor
	assert rc.absolute_max_cents == 5000  # $50 per-order cap


def test_live_example_channels_resolve_in_notifications_example(
	coherence_env: None,
) -> None:
	"""The example's `live_channel` / `live_risk_channel` name channels that
	actually exist in the tracked `config/notifications.example.yaml`, and
	they are DISTINCT (the risk channel is dedicated — §2.4/§6 G3)."""
	from edge_catcher.notifications import load_channels

	cfg = _load_example_config()
	notif = cfg["notifications"]
	channels = load_channels(_NOTIFY_EXAMPLE)
	assert notif["live_channel"] in channels, (
		f"example live_channel {notif['live_channel']!r} not in "
		f"notifications.example.yaml channels {sorted(channels)}"
	)
	assert notif["live_risk_channel"] in channels, (
		f"example live_risk_channel {notif['live_risk_channel']!r} not in "
		f"notifications.example.yaml channels {sorted(channels)}"
	)
	assert notif["live_channel"] != notif["live_risk_channel"], (
		"the dedicated risk channel must be distinct from the general "
		"live channel (§2.4/§6 G3)"
	)


def test_live_example_uses_default_trade_scope_env_vars() -> None:
	"""The example deliberately relies on the DEFAULT trade-scope env-var
	names (documenting them in a comment) rather than pinning non-default
	`live_key_id_env`/`live_private_key_env` keys — keeps the skeleton lean
	and matches the `signing_env` idiom. Assert it adds no override keys."""
	from edge_catcher.engine.discovery import load_config

	cfg = load_config(_LIVE_EXAMPLE)
	notif = cfg.get("notifications", {}) or {}
	assert "live_key_id_env" not in notif, (
		"example pins a non-default live_key_id_env; J1 prefers the default "
		"+ a documenting comment"
	)
	assert "live_private_key_env" not in notif


# ---------------------------------------------------------------------------
# NEGATIVE — break exactly one coherence dimension on a deep-copy of the
# loaded example; each must raise the `_coherence_fail` RuntimeError. Proves
# the positive pass is REAL (the gate genuinely rejects) and that the
# example's coherence is load-bearing, not coincidental.
# ---------------------------------------------------------------------------


def test_negative_missing_phase1_cap_is_rejected(coherence_env: None) -> None:
	"""(a) Remove a Phase-1 cap from the loaded example's risk: block →
	RuntimeError (Check 5)."""
	from edge_catcher.engine.engine import _assert_mode_coherence

	cfg = copy.deepcopy(_load_example_config())
	del cfg["risk"]["absolute_max_cents"]
	with pytest.raises(RuntimeError, match="coherence"):
		_assert_mode_coherence(cfg)


def test_negative_missing_live_risk_channel_is_rejected(
	coherence_env: None,
) -> None:
	"""(b) Remove `notifications.live_risk_channel` → RuntimeError (Check 4b:
	the dedicated kill-switch channel is mandatory, fail-closed)."""
	from edge_catcher.engine.engine import _assert_mode_coherence

	cfg = copy.deepcopy(_load_example_config())
	del cfg["notifications"]["live_risk_channel"]
	with pytest.raises(RuntimeError, match="coherence"):
		_assert_mode_coherence(cfg)


def test_negative_executor_paper_with_live_db_is_rejected(
	coherence_env: None,
) -> None:
	"""(c) Flip `executor` to paper while db_path stays the live_trades DB →
	RuntimeError (Check 2: a paper run must never touch the real-money DB)."""
	from edge_catcher.engine.engine import _assert_mode_coherence

	cfg = copy.deepcopy(_load_example_config())
	cfg["executor"] = "paper"
	# db_path is still data/live_trades.db from the example → incoherent.
	with pytest.raises(RuntimeError, match="coherence"):
		_assert_mode_coherence(cfg)
