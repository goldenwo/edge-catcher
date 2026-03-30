"""Bearer token auth dependency. Skips auth when API_KEY env var is unset."""
from __future__ import annotations

import os

from fastapi import HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

_bearer = HTTPBearer(auto_error=False)


async def check_auth(
    credentials: HTTPAuthorizationCredentials = Security(_bearer),
) -> None:
    api_key = os.getenv("API_KEY")
    if not api_key:
        return  # dev mode: auth disabled
    if not credentials or credentials.credentials != api_key:
        raise HTTPException(status_code=401, detail="Unauthorized")
