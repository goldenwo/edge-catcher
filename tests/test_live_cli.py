"""Tests for edge_catcher.live.cli — argparse + confirm prompts + CLI cap."""
from __future__ import annotations

import httpx
import pytest

from edge_catcher.live.cli import main


def _load_cfg_from(path):
	"""Helper: zero-arg loader that reads from a specific path — used for monkeypatching."""
	from edge_catcher.live.config import load_config
	return load_config(path)


def test_no_args_prints_help_returns_2(capsys):
	rc = main([])
	assert rc == 2
	out = capsys.readouterr().out
	assert "place" in out and "cancel" in out


def test_place_yes_skip_calls_client(monkeypatch, tmp_path, signing_env_cli):
	"""--yes skips the prompt and calls client.place."""
	cfg_path = tmp_path / "live-trader.yaml"
	audit_path = tmp_path / "a.jsonl"
	cfg_path.write_text(
		f"cli_max_order_dollars: 1.00\naudit_log_path: {audit_path}\n"
	)
	monkeypatch.setattr(
		"edge_catcher.live.cli.load_config",
		lambda: _load_cfg_from(cfg_path),
	)

	captured = []

	def handler(request: httpx.Request) -> httpx.Response:
		captured.append(request)
		return httpx.Response(201, json={"order": {
			"order_id": "ord-1", "status": "resting", "count": 1, "yes_price": 1,
			"side": "yes", "action": "buy", "time_in_force": "gtc",
		}})

	# Patch httpx.Client to inject a MockTransport without touching other kwargs.
	original = httpx.Client

	def mock_client(*args, **kwargs):
		kwargs_clean = {k: v for k, v in kwargs.items() if k != "transport"}
		return original(*args, transport=httpx.MockTransport(handler), **kwargs_clean)

	monkeypatch.setattr("httpx.Client", mock_client)
	rc = main([
		"place", "--ticker", "X", "--side", "yes",
		"--price", "1", "--count", "1", "--yes",
	])
	assert rc == 0
	assert len(captured) == 1


def test_place_exposure_above_cli_cap_exits_3(
	monkeypatch, tmp_path, capsys, signing_env_cli
):
	cfg_path = tmp_path / "live-trader.yaml"
	cfg_path.write_text("cli_max_order_dollars: 1.00\n")
	monkeypatch.setattr(
		"edge_catcher.live.cli.load_config",
		lambda: _load_cfg_from(cfg_path),
	)
	# 100 contracts × 5¢ = $5 > $1 CLI cap — rejected before any HTTP call
	rc = main([
		"place", "--ticker", "X", "--side", "yes",
		"--price", "5", "--count", "100", "--yes",
	])
	assert rc == 3
	out = capsys.readouterr().err
	assert "REJECTED" in out and "CLI cap" in out


def test_place_no_yes_prompt_n_aborts(
	monkeypatch, tmp_path, capsys, signing_env_cli
):
	cfg_path = tmp_path / "live-trader.yaml"
	cfg_path.write_text("cli_max_order_dollars: 1.00\n")
	monkeypatch.setattr(
		"edge_catcher.live.cli.load_config",
		lambda: _load_cfg_from(cfg_path),
	)
	monkeypatch.setattr("builtins.input", lambda prompt: "n")
	rc = main(["place", "--ticker", "X", "--side", "yes", "--price", "1", "--count", "1"])
	assert rc == 1
	assert "Cancelled" in capsys.readouterr().out


def test_cancel_yes_skip_calls_client(monkeypatch, tmp_path, capsys, signing_env_cli):
	"""cancel <id> --yes calls client.cancel, exit 0, prints Cancelled line."""
	cfg_path = tmp_path / "live-trader.yaml"
	audit_path = tmp_path / "a.jsonl"
	cfg_path.write_text(
		f"cli_max_order_dollars: 1.00\naudit_log_path: {audit_path}\n"
	)
	monkeypatch.setattr(
		"edge_catcher.live.cli.load_config",
		lambda: _load_cfg_from(cfg_path),
	)

	def handler(request: httpx.Request) -> httpx.Response:
		return httpx.Response(200, json={"order": {"order_id": "ord-x", "status": "canceled"}})

	original = httpx.Client

	def mock_client(*args, **kwargs):
		kwargs_clean = {k: v for k, v in kwargs.items() if k != "transport"}
		return original(*args, transport=httpx.MockTransport(handler), **kwargs_clean)

	monkeypatch.setattr("httpx.Client", mock_client)
	rc = main(["cancel", "ord-x", "--yes"])
	assert rc == 0
	out = capsys.readouterr().out
	assert "Cancelled order_id=ord-x status=canceled" in out


def test_cancel_no_yes_n_aborts(monkeypatch, tmp_path, capsys, signing_env_cli):
	"""cancel without --yes, user answers 'n' → exit 1, 'Aborted.' printed."""
	cfg_path = tmp_path / "live-trader.yaml"
	audit_path = tmp_path / "a.jsonl"
	cfg_path.write_text(
		f"cli_max_order_dollars: 1.00\naudit_log_path: {audit_path}\n"
	)
	monkeypatch.setattr(
		"edge_catcher.live.cli.load_config",
		lambda: _load_cfg_from(cfg_path),
	)
	monkeypatch.setattr("builtins.input", lambda prompt: "n")
	rc = main(["cancel", "ord-x"])
	assert rc == 1
	assert "Aborted." in capsys.readouterr().out


def test_status_prints_order_details(monkeypatch, tmp_path, capsys, signing_env_cli):
	"""status <id> calls client.status and prints all order fields."""
	cfg_path = tmp_path / "live-trader.yaml"
	audit_path = tmp_path / "a.jsonl"
	cfg_path.write_text(
		f"cli_max_order_dollars: 1.00\naudit_log_path: {audit_path}\n"
	)
	monkeypatch.setattr(
		"edge_catcher.live.cli.load_config",
		lambda: _load_cfg_from(cfg_path),
	)

	def handler(request: httpx.Request) -> httpx.Response:
		return httpx.Response(200, json={"order": {
			"order_id": "ord-x", "ticker": "X", "side": "yes", "action": "buy",
			"count": 10, "yes_price": 5, "time_in_force": "gtc",
			"status": "resting", "filled_count": 3,
		}})

	original = httpx.Client

	def mock_client(*args, **kwargs):
		kwargs_clean = {k: v for k, v in kwargs.items() if k != "transport"}
		return original(*args, transport=httpx.MockTransport(handler), **kwargs_clean)

	monkeypatch.setattr("httpx.Client", mock_client)
	rc = main(["status", "ord-x"])
	assert rc == 0
	out = capsys.readouterr().out
	assert "order_id=ord-x" in out
	assert "ticker" in out and "X" in out
	assert "side" in out and "yes" in out
	assert "count" in out and "10" in out
	assert "price" in out and "5c" in out
	assert "tif" in out and "gtc" in out
	assert "status" in out and "resting" in out


def test_balance_prints_dollar_amount(monkeypatch, tmp_path, capsys, signing_env_cli):
	"""balance prints 'balance: $XXX.YY' from cents value."""
	cfg_path = tmp_path / "live-trader.yaml"
	audit_path = tmp_path / "a.jsonl"
	cfg_path.write_text(
		f"cli_max_order_dollars: 1.00\naudit_log_path: {audit_path}\n"
	)
	monkeypatch.setattr(
		"edge_catcher.live.cli.load_config",
		lambda: _load_cfg_from(cfg_path),
	)

	def handler(request: httpx.Request) -> httpx.Response:
		return httpx.Response(200, json={"balance": 19500})

	original = httpx.Client

	def mock_client(*args, **kwargs):
		kwargs_clean = {k: v for k, v in kwargs.items() if k != "transport"}
		return original(*args, transport=httpx.MockTransport(handler), **kwargs_clean)

	monkeypatch.setattr("httpx.Client", mock_client)
	rc = main(["balance"])
	assert rc == 0
	out = capsys.readouterr().out
	assert "balance: $195.00" in out


def test_positions_empty_prints_no_open(monkeypatch, tmp_path, capsys, signing_env_cli):
	"""positions with empty result prints '(no open positions)'."""
	cfg_path = tmp_path / "live-trader.yaml"
	audit_path = tmp_path / "a.jsonl"
	cfg_path.write_text(
		f"cli_max_order_dollars: 1.00\naudit_log_path: {audit_path}\n"
	)
	monkeypatch.setattr(
		"edge_catcher.live.cli.load_config",
		lambda: _load_cfg_from(cfg_path),
	)

	def handler(request: httpx.Request) -> httpx.Response:
		return httpx.Response(200, json={"market_positions": []})

	original = httpx.Client

	def mock_client(*args, **kwargs):
		kwargs_clean = {k: v for k, v in kwargs.items() if k != "transport"}
		return original(*args, transport=httpx.MockTransport(handler), **kwargs_clean)

	monkeypatch.setattr("httpx.Client", mock_client)
	rc = main(["positions"])
	assert rc == 0
	assert "(no open positions)" in capsys.readouterr().out


def test_positions_non_empty_prints_each(monkeypatch, tmp_path, capsys, signing_env_cli):
	"""positions with one or two entries prints one row per position."""
	cfg_path = tmp_path / "live-trader.yaml"
	audit_path = tmp_path / "a.jsonl"
	cfg_path.write_text(
		f"cli_max_order_dollars: 1.00\naudit_log_path: {audit_path}\n"
	)
	monkeypatch.setattr(
		"edge_catcher.live.cli.load_config",
		lambda: _load_cfg_from(cfg_path),
	)

	def handler(request: httpx.Request) -> httpx.Response:
		return httpx.Response(200, json={"market_positions": [
			{"ticker": "KXETH", "position": 10, "average_position_cost": 5},
			{"ticker": "KXBTC", "position": 5, "average_position_cost": 20},
		]})

	original = httpx.Client

	def mock_client(*args, **kwargs):
		kwargs_clean = {k: v for k, v in kwargs.items() if k != "transport"}
		return original(*args, transport=httpx.MockTransport(handler), **kwargs_clean)

	monkeypatch.setattr("httpx.Client", mock_client)
	rc = main(["positions"])
	assert rc == 0
	out = capsys.readouterr().out
	assert "KXETH" in out
	assert "KXBTC" in out


@pytest.fixture
def signing_env_cli(monkeypatch):
	from cryptography.hazmat.primitives import serialization
	from cryptography.hazmat.primitives.asymmetric import rsa
	key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
	pem = key.private_bytes(
		encoding=serialization.Encoding.PEM,
		format=serialization.PrivateFormat.PKCS8,
		encryption_algorithm=serialization.NoEncryption(),
	)
	monkeypatch.setenv("KALSHI_KEY_ID", "test")
	monkeypatch.setenv("KALSHI_PRIVATE_KEY", pem.decode())
