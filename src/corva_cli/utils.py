"""Shared utility helpers."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
import asyncio
from typing import Any, Dict, Iterable, List, Optional

import httpx

from corva_cli.auth import AuthContext, AuthMethod
from corva_cli.settings import get_settings


class MQLNormalizationError(ValueError):
    """Raised when an MQL pipeline cannot be coerced into a list of dicts."""


def _ensure_mql_is_array_of_dicts(mql: Any) -> List[Dict[str, Any]]:
    """Return a normalized list of mapping stages."""

    if isinstance(mql, Mapping):
        return [dict(mql)]
    if isinstance(mql, Sequence) and not isinstance(mql, (str, bytes)):
        stages: List[Dict[str, Any]] = []
        for stage in mql:
            if not isinstance(stage, Mapping):
                raise MQLNormalizationError("Each MQL stage must be a mapping/dict.")
            stages.append(dict(stage))
        if not stages:
            raise MQLNormalizationError("MQL pipeline must contain at least one stage.")
        return stages
    raise MQLNormalizationError("MQL must be a dict or a sequence of dicts.")


def build_auth_headers(auth: AuthContext) -> Dict[str, str]:
    """Return HTTP headers corresponding to the provided auth token."""

    if auth.method is AuthMethod.API_KEY:
        return {"X-API-Key": auth.token}
    return {"Authorization": f"Bearer {auth.token}"}


async def execute_data_api_pipeline(
    provider: str,
    dataset_name: str,
    mql: Any,
    headers: Dict[str, str],
    *,
    timeout: Optional[float] = None,
    client: Optional[httpx.AsyncClient] = None,
) -> Any:
    """Execute an aggregate pipeline against the Data API and return JSON."""

    stages_payload = _ensure_mql_is_array_of_dicts(mql)
    settings = get_settings()
    timeout_seconds = timeout if timeout is not None else settings.data_api_timeout_seconds
    base_url = settings.data_api_root_url.rstrip("/")
    url = f"{base_url}/api/v1/data/{provider}/{dataset_name}/aggregate/pipeline/"

    own_client = client is None
    request_client = client or httpx.AsyncClient()
    try:
        response = await request_client.post(
            url,
            json={"stages": stages_payload},
            headers=headers,
            timeout=timeout_seconds,
        )
        response.raise_for_status()
        return response.json()
    finally:
        if own_client:
            await request_client.aclose()


def run_data_api_pipeline(
    provider: str,
    dataset_name: str,
    mql: Any,
    headers: Dict[str, str],
    *,
    timeout: Optional[float] = None,
) -> Any:
    """Convenience sync wrapper that runs :func:`execute_data_api_pipeline`."""

    return asyncio.run(
        execute_data_api_pipeline(
            provider,
            dataset_name,
            mql,
            headers,
            timeout=timeout,
        )
    )


__all__ = [
    "MQLNormalizationError",
    "_ensure_mql_is_array_of_dicts",
    "build_auth_headers",
    "execute_data_api_pipeline",
    "run_data_api_pipeline",
]
