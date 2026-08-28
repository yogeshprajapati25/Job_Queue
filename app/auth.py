import os
import secrets
from fastapi import Security, HTTPException, status
from fastapi.security import APIKeyHeader

# Read the API key from environment variable
# In production this is set in Render dashboard / .env file
API_KEY = os.getenv("API_KEY", "")

# This tells FastAPI to look for the key in the X-API-Key header
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def verify_api_key(api_key: str = Security(api_key_header)) -> str:
    """
    FastAPI dependency — inject into any route to protect it.
    Returns the key if valid, raises 401 if missing or wrong.
    Uses secrets.compare_digest to prevent timing attacks.
    """
    if not API_KEY:
        # If no API_KEY env var is set, fail loudly — don't silently allow all traffic
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Server misconfiguration: API_KEY environment variable is not set",
        )

    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing API key. Pass it in the X-API-Key header.",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    # secrets.compare_digest prevents timing attacks where an attacker
    # measures response time to guess the key character by character
    if not secrets.compare_digest(api_key, API_KEY):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid API key.",
        )

    return api_key
