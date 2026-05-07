"""Tests for edge_catcher.adapters.kalshi.auth — RSA-PSS-SHA256 signing."""
from __future__ import annotations

import base64
from unittest.mock import patch

import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from edge_catcher.adapters.kalshi.auth import make_auth_headers


@pytest.fixture
def rsa_keypair_in_env(monkeypatch):
	"""Generate a test RSA key, encode as PEM, set into env for one test."""
	key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
	pem = key.private_bytes(
		encoding=serialization.Encoding.PEM,
		format=serialization.PrivateFormat.PKCS8,
		encryption_algorithm=serialization.NoEncryption(),
	)
	monkeypatch.setenv("KALSHI_KEY_ID", "test-key-id")
	monkeypatch.setenv("KALSHI_PRIVATE_KEY", pem.decode())
	return key


def test_default_signs_get_ws_path(rsa_keypair_in_env):
	"""Backwards-compat: no args returns headers signed for GET WS_PATH."""
	headers = make_auth_headers()
	assert headers["KALSHI-ACCESS-KEY"] == "test-key-id"
	assert "KALSHI-ACCESS-SIGNATURE" in headers
	assert "KALSHI-ACCESS-TIMESTAMP" in headers


def test_explicit_post_signs_with_method_in_message(rsa_keypair_in_env):
	"""POST with custom path signs ts + 'POST' + path; verify sig matches."""
	with patch("edge_catcher.adapters.kalshi.auth.time.time", return_value=1700000000.0):
		headers = make_auth_headers("POST", "/trade-api/v2/portfolio/orders")
	expected_ts = str(int(1700000000.0 * 1000))
	expected_msg = expected_ts + "POST" + "/trade-api/v2/portfolio/orders"
	# Verify signature against the public key
	sig = base64.b64decode(headers["KALSHI-ACCESS-SIGNATURE"])
	rsa_keypair_in_env.public_key().verify(
		sig,
		expected_msg.encode(),
		padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
		hashes.SHA256(),
	)


def test_delete_method_supported(rsa_keypair_in_env):
	"""DELETE on an order id path signs successfully."""
	headers = make_auth_headers("DELETE", "/trade-api/v2/portfolio/orders/abc-123")
	assert headers["KALSHI-ACCESS-KEY"] == "test-key-id"


def test_non_rsa_key_raises_valueerror(monkeypatch):
	"""Ed25519 key in env should raise ValueError, not silently break sig verification."""
	from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
	ed_key = Ed25519PrivateKey.generate()
	pem = ed_key.private_bytes(
		encoding=serialization.Encoding.PEM,
		format=serialization.PrivateFormat.PKCS8,
		encryption_algorithm=serialization.NoEncryption(),
	)
	monkeypatch.setenv("KALSHI_KEY_ID", "test")
	monkeypatch.setenv("KALSHI_PRIVATE_KEY", pem.decode())
	with pytest.raises(ValueError, match="must be an RSA private key"):
		make_auth_headers()
