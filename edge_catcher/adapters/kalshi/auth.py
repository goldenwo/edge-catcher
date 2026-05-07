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


def make_auth_headers(method: str = "GET", path: str = WS_PATH) -> dict[str, str]:
	"""Sign request and return Kalshi auth headers.

	Signs `ts_ms + method + path` with RSA-PSS-SHA256 using the env-loaded
	private key. Method and path default to GET + WS_PATH for backwards
	compatibility with the paper trader's WebSocket auth path.
	"""
	key_id = os.environ["KALSHI_KEY_ID"]
	private_key_pem = os.environ["KALSHI_PRIVATE_KEY"].encode()
	private_key = serialization.load_pem_private_key(
		private_key_pem, password=None, backend=default_backend()
	)
	if not isinstance(private_key, RSAPrivateKey):
		raise ValueError(
			f"KALSHI_PRIVATE_KEY must be an RSA private key (got "
			f"{type(private_key).__name__}). Kalshi API auth requires RSA-PSS-SHA256."
		)
	ts_ms = str(int(time.time() * 1000))
	msg = ts_ms + method + path
	sig = private_key.sign(
		msg.encode(),
		padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
		hashes.SHA256(),
	)
	return {
		"KALSHI-ACCESS-KEY": key_id,
		"KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
		"KALSHI-ACCESS-TIMESTAMP": ts_ms,
	}
