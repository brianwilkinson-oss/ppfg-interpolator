#!/usr/bin/env python3
"""Refresh docs/dataset.json with detailed dataset metadata."""
from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import httpx

DATASET_LIST_URL = "https://data.corva.ai/api/v1/dataset/"
DATASET_DETAIL_URL = "https://data.corva.ai/api/v1/dataset/{path}/"
 
 
def _dataset_path(name: str) -> str:
    if "#" in name:
        provider, dataset = name.split("#", 1)
        return f"{provider}/{dataset}"
    return name
DEFAULT_OUTPUT = Path("docs/dataset.json")


async def fetch_detail(client: httpx.AsyncClient, name: str) -> Optional[Dict[str, Any]]:
    path = _dataset_path(name)
    url = DATASET_DETAIL_URL.format(path=path)
    try:
        resp = await client.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPError as exc:  # pragma: no cover - networking
        print(f"Failed to fetch {name}: {exc}")
        return None


def merge_records(base: Dict[str, Any], detail: Dict[str, Any]) -> Dict[str, Any]:
    merged = base.copy()
    merged.update(detail)
    return merged


async def main(
    provider: Optional[str],
    company_id: Optional[int],
    token: str,
    output: Path,
    *,
    use_jwt: bool = False,
) -> None:
    scheme = "Bearer" if use_jwt else "API"
    headers = {"Authorization": f"{scheme} {token}"}
    async with httpx.AsyncClient(headers=headers) as client:
        list_resp = await client.get(DATASET_LIST_URL, timeout=30)
        list_resp.raise_for_status()
        datasets = list_resp.json()
        filtered: Iterable[Dict[str, Any]] = datasets
        if provider:
            filtered = [item for item in filtered if item.get("provider") == provider]
        if company_id is not None:
            filtered = [item for item in filtered if item.get("company_id") == company_id]

        updated: List[Dict[str, Any]] = []
        for item in filtered:
            detail = await fetch_detail(client, item["name"])
            updated.append(merge_records(item, detail) if detail else item)

    output.write_text(json.dumps(updated, indent=2))
    print(f"Wrote {len(updated)} records to {output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Refresh docs/dataset.json from Data API")
    parser.add_argument("--provider")
    parser.add_argument("--company-id", type=int)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--token",
        default=os.getenv("CORVA_API_KEY"),
        help="API key token",
    )
    parser.add_argument(
        "--jwt",
        default=os.getenv("CORVA_JWT"),
        help="JWT credential",
    )
    args = parser.parse_args()
    if not (args.token or args.jwt):
        parser.error("Provide --token / --jwt or set CORVA_API_KEY / CORVA_JWT")
    token = args.token or args.jwt
    asyncio.run(
        main(
            args.provider,
            args.company_id,
            token,
            args.output,
            use_jwt=bool(args.jwt),
        )
    )
