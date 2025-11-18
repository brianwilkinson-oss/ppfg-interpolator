"""Authentication helpers for the Corva CLI."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class AuthError(RuntimeError):
    """Raised when authentication cannot be resolved."""


class AuthMethod(str, Enum):
    """Supported authentication mechanisms."""

    API_KEY = "api_key"
    JWT = "jwt"


@dataclass(slots=True)
class AuthContext:
    """Represents resolved authentication credentials for a command run."""

    method: AuthMethod
    token: str


def resolve_auth(api_key: Optional[str], jwt: Optional[str]) -> AuthContext:
    """Return an :class:`AuthContext` based on provided credentials.

    Args:
        api_key: API key supplied via CLI or environment.
        jwt: JWT supplied via CLI or environment.

    Raises:
        AuthError: If the credentials are ambiguous or missing.
    """

    if api_key and jwt:
        raise AuthError("Provide either an API key or a JWT, not both.")
    if api_key:
        return AuthContext(method=AuthMethod.API_KEY, token=api_key)
    if jwt:
        return AuthContext(method=AuthMethod.JWT, token=jwt)

    raise AuthError("Authentication required: supply --api-key or --jwt.")


__all__ = ["AuthContext", "AuthError", "AuthMethod", "resolve_auth"]
