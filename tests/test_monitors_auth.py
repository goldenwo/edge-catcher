"""Tests for Kalshi RSA auth header generation."""

import os
import time
from unittest.mock import patch

import pytest


class TestMakeAuthHeaders:
	def test_returns_required_kalshi_headers(self):
		"""Auth headers must contain KEY, SIGNATURE, and TIMESTAMP."""
		from edge_catcher.monitors.auth import make_auth_headers
		from cryptography.hazmat.primitives.asymmetric import rsa
		from cryptography.hazmat.primitives import serialization

		private_key = rsa.generate_private_key(
			public_exponent=65537, key_size=2048
		)
		pem = private_key.private_bytes(
			encoding=serialization.Encoding.PEM,
			format=serialization.PrivateFormat.PKCS8,
			encryption_algorithm=serialization.NoEncryption(),
		).decode()

		with patch.dict(os.environ, {
			"KALSHI_KEY_ID": "test-key-id",
			"KALSHI_PRIVATE_KEY": pem,
		}):
			headers = make_auth_headers("/trade-api/ws/v2")

		assert "KALSHI-ACCESS-KEY" in headers
		assert headers["KALSHI-ACCESS-KEY"] == "test-key-id"
		assert "KALSHI-ACCESS-SIGNATURE" in headers
		assert "KALSHI-ACCESS-TIMESTAMP" in headers
		ts = int(headers["KALSHI-ACCESS-TIMESTAMP"])
		assert abs(ts - int(time.time() * 1000)) < 5000

	def test_missing_env_raises(self):
		"""Missing KALSHI_KEY_ID should raise KeyError."""
		from edge_catcher.monitors.auth import make_auth_headers
		with patch.dict(os.environ, {}, clear=True):
			with pytest.raises(KeyError):
				make_auth_headers("/trade-api/ws/v2")
