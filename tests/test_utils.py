import json

import httpx
import pytest

from corva_cli import settings as settings_module
from corva_cli.auth import AuthContext, AuthMethod
from corva_cli.utils import (
    MQLNormalizationError,
    _ensure_mql_is_array_of_dicts,
    build_auth_headers,
    execute_data_api_pipeline,
)


def test_ensure_mql_variants():
    assert _ensure_mql_is_array_of_dicts({"$match": {}}) == [{"$match": {}}]
    assert _ensure_mql_is_array_of_dicts([
        {"$match": {}},
        {"$sort": {"ts": 1}},
    ]) == [{"$match": {}}, {"$sort": {"ts": 1}}]
    with pytest.raises(MQLNormalizationError):
        _ensure_mql_is_array_of_dicts("bad")


def test_build_auth_headers():
    api_headers = build_auth_headers(AuthContext(method=AuthMethod.API_KEY, token="key"))
    assert api_headers["X-API-Key"] == "key"
    jwt_headers = build_auth_headers(AuthContext(method=AuthMethod.JWT, token="jwt"))
    assert jwt_headers["Authorization"] == "Bearer jwt"


@pytest.mark.asyncio
async def test_execute_data_api_pipeline(monkeypatch):
    monkeypatch.setenv("CORVA_DATA_API_ROOT_URL", "https://example.com")
    settings_module.reload_settings()

    captured = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["json"] = json.loads(request.content.decode())
        return httpx.Response(200, json={"data": [1, 2, 3]})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        data = await execute_data_api_pipeline(
            provider="provider",
            dataset_name="dataset",
            mql={"$match": {"foo": "bar"}},
            headers={"X-API-Key": "token"},
            client=client,
        )

    assert data == {"data": [1, 2, 3]}
    assert captured["url"].endswith("/api/v1/data/provider/dataset/aggregate/pipeline/")
    assert captured["json"] == {"stages": [{"$match": {"foo": "bar"}}]}
