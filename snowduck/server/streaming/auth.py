"""Authentication and token management for Snowpipe Streaming API.

This module handles:
- Scoped token generation and validation
- OAuth token exchange
- LRU cache for token storage
"""

from __future__ import annotations

import base64
import datetime
import json
import secrets
from collections import OrderedDict
from typing import Any


class LRUTokenCache(OrderedDict):
    """LRU cache for scoped authentication tokens.
    
    Limits memory usage by evicting oldest tokens when capacity is reached.
    """
    
    def __init__(self, maxsize: int = 10_000) -> None:
        super().__init__()
        self.maxsize = maxsize
    
    def __setitem__(self, key: str, value: str) -> None:
        if key in self:
            self.move_to_end(key)
        super().__setitem__(key, value)
        if len(self) > self.maxsize:
            self.popitem(last=False)
    
    def get_original_auth(self, scoped_token: str) -> str | None:
        """Get the original authorization for a scoped token."""
        return self.get(scoped_token)


# Backwards compatibility alias
_LRUCache = LRUTokenCache


# Global token cache instance
_token_cache = LRUTokenCache()


def generate_scoped_token(scope: str | None = None) -> str:
    """Generate a scoped JWT-like token.
    
    Creates a minimal JWT structure that the SDK can parse to extract
    expiration time. This is not a cryptographically valid JWT, but
    provides the format the SDK expects.
    
    Args:
        scope: Optional scope string for the token
    
    Returns:
        JWT-format token string
    """
    exp_time = int(
        (datetime.datetime.now(datetime.timezone.utc) + 
         datetime.timedelta(hours=1)).timestamp()
    )
    
    header = base64.urlsafe_b64encode(
        b'{"alg":"HS256","typ":"JWT"}'
    ).decode().rstrip('=')
    
    payload_data = {
        "exp": exp_time,
        "scope": scope or "streaming",
        "iat": int(datetime.datetime.now(datetime.timezone.utc).timestamp()),
    }
    payload = base64.urlsafe_b64encode(
        json.dumps(payload_data).encode()
    ).decode().rstrip('=')
    
    signature = base64.urlsafe_b64encode(
        secrets.token_bytes(32)
    ).decode().rstrip('=')
    
    return f"{header}.{payload}.{signature}"


def store_scoped_token(scoped_token: str, original_auth: str) -> None:
    """Store mapping from scoped token to original authorization."""
    _token_cache[scoped_token] = original_auth


def validate_scoped_token(scoped_token: str) -> bool:
    """Validate that a scoped token exists in the cache."""
    return scoped_token in _token_cache


def parse_form_urlencoded(body: str) -> dict[str, Any]:
    """Parse application/x-www-form-urlencoded body."""
    from urllib.parse import parse_qs
    parsed = parse_qs(body)
    # Get first value for each key
    return {k: v[0] if v else None for k, v in parsed.items()}
