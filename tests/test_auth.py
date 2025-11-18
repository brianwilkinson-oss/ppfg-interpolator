import pytest

from corva_cli.auth import AuthError, AuthMethod, resolve_auth


def test_resolve_api_key():
    ctx = resolve_auth("abc", None)
    assert ctx.method == AuthMethod.API_KEY
    assert ctx.token == "abc"


def test_resolve_jwt():
    ctx = resolve_auth(None, "jwt-token")
    assert ctx.method == AuthMethod.JWT


def test_resolve_conflict():
    with pytest.raises(AuthError):
        resolve_auth("key", "jwt")


def test_resolve_missing():
    with pytest.raises(AuthError):
        resolve_auth(None, None)
