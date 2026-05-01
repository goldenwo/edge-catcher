"""Kalshi RSA auth header generation for WebSocket and REST."""

import base64
import os
import time

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

KALSHI_REST_BASE = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
WS_PATH = "/trade-api/ws/v2"


def make_auth_headers(path: str = WS_PATH) -> dict[str, str]:
	"""Generate signed Kalshi API headers for the given path.

	Kalshi mandates RSA keys for API auth; we narrow `load_pem_private_key`'s
	cross-algorithm return union (Ed25519 / DSA / EC / post-quantum types in
	newer cryptography releases) to `RSAPrivateKey` via isinstance so the
	subsequent `.sign(...)` call matches RSA's signature on every supported
	library version.
	"""
	key_id = os.environ["KALSHI_KEY_ID"]
	private_key_pem = os.environ["KALSHI_PRIVATE_KEY"].encode()
	private_key = serialization.load_pem_private_key(
		private_key_pem, password=None, backend=default_backend()
	)
	if not isinstance(private_key, RSAPrivateKey):
		raise ValueError(
			f"KALSHI_PRIVATE_KEY must be an RSA private key (got {type(private_key).__name__}). "
			"Kalshi API auth requires RSA-PSS-SHA256 signatures."
		)
	ts_ms = str(int(time.time() * 1000))
	msg = ts_ms + "GET" + path
	sig = private_key.sign(
		msg.encode(),
		padding.PSS(
			mgf=padding.MGF1(hashes.SHA256()),
			salt_length=padding.PSS.MAX_LENGTH,
		),
		hashes.SHA256(),
	)
	sig_b64 = base64.b64encode(sig).decode()
	return {
		"KALSHI-ACCESS-KEY": key_id,
		"KALSHI-ACCESS-SIGNATURE": sig_b64,
		"KALSHI-ACCESS-TIMESTAMP": ts_ms,
	}
