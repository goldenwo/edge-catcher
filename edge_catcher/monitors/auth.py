"""Kalshi RSA auth header generation for WebSocket and REST."""

import base64
import os
import time

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

KALSHI_REST_BASE = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
WS_PATH = "/trade-api/ws/v2"


def make_auth_headers(path: str = WS_PATH) -> dict[str, str]:
	"""Generate signed Kalshi API headers for the given path."""
	key_id = os.environ["KALSHI_KEY_ID"]
	private_key_pem = os.environ["KALSHI_PRIVATE_KEY"].encode()
	private_key = serialization.load_pem_private_key(
		private_key_pem, password=None, backend=default_backend()
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
