"""Timelog sample tool used for CLI scaffolding."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from corva_cli.settings import get_settings
from corva_cli.timewindow import resolve_auto_window
from corva_cli.tools.base import ParameterSpec, ToolContext, ToolResult
from corva_cli.tools.registry import registry
from corva_cli import utils


def _to_unix_seconds(dt: datetime) -> int:
    if dt.tzinfo is None:
        aware = dt.replace(tzinfo=timezone.utc)
    else:
        aware = dt.astimezone(timezone.utc)
    return int(aware.timestamp())


def _build_timelog_pipeline(
    assets: List[int],
    start_dt: Optional[datetime],
    end_dt: Optional[datetime],
    statuses: List[str],
    step_minutes: int,
    limit: int,
    skip: int,
) -> List[Dict[str, Any]]:
    asset_filter: Any
    if len(assets) == 1:
        asset_filter = assets[0]
    else:
        asset_filter = {"$in": assets}

    window_filter: Dict[str, Any] = {"asset_id": asset_filter}
    if start_dt and end_dt:
        start_epoch = _to_unix_seconds(start_dt)
        end_epoch = _to_unix_seconds(end_dt)
        window_filter["data.start_time"] = {"$gte": start_epoch, "$lte": end_epoch}

    stages: List[Dict[str, Any]] = [{"$match": window_filter}]
    if skip > 0:
        stages.append({"$skip": skip})
    stages.append({"$limit": limit})
    stages.append({"$sort": {"data.start_time": -1}})
    stages.append(
        {
            "$addFields": {
                "data.start_time_iso": {
                    "$dateToString": {
                        "date": {
                            "$toDate": {
                                "$multiply": ["$data.start_time", 1000],
                            }
                        },
                        "format": "%Y-%m-%dT%H:%M:%S.000Z",
                    }
                },
                "data.end_time_iso": {
                    "$dateToString": {
                        "date": {
                            "$toDate": {
                                "$multiply": ["$data.end_time", 1000],
                            }
                        },
                        "format": "%Y-%m-%dT%H:%M:%S.000Z",
                    }
                },
            }
        }
    )
    return stages


@registry.tool(
    name="timelog",
    description="Return synthetic timelog data for one or more assets.",
    parameters=[
        ParameterSpec(
            "asset_ids",
            help="Comma-separated integer asset IDs",
        ),
        ParameterSpec(
            "start_time",
            help="Window start in auto_* syntax (e.g. auto_2h30m)",
            required=False,
            default=None,
        ),
        ParameterSpec(
            "end_time",
            help="Window end in auto_* syntax (e.g. auto_0d)",
            required=False,
            default=None,
        ),
        ParameterSpec(
            "step_minutes",
            type=int,
            help="Override step size in minutes (defaults to settings)",
            required=False,
            default=None,
        ),
        ParameterSpec(
            "statuses",
            help="Override comma-separated statuses (defaults to settings)",
            required=False,
            default=None,
        ),
        ParameterSpec(
            "limit",
            type=int,
            help="Max documents returned when no time window is provided (default 1000)",
            required=False,
            default=1000,
        ),
        ParameterSpec(
            "skip",
            type=int,
            help="Number of documents to skip before collecting results",
            required=False,
            default=0,
        ),
    ],
)
def get_timelog_data(
    context: ToolContext,
    asset_ids: str,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    step_minutes: Optional[int] = None,
    statuses: Optional[str] = None,
    limit: Optional[int] = 1000,
    skip: Optional[int] = 0,
) -> ToolResult:
    raw_assets = [asset.strip() for asset in asset_ids.split(",") if asset.strip()]
    if not raw_assets:
        raise ValueError("Provide at least one asset id.")
    try:
        assets = [int(asset) for asset in raw_assets]
    except ValueError as exc:
        raise ValueError("Asset ids must be integers.") from exc

    settings = get_settings()
    effective_step = max(step_minutes or settings.timelog_step_minutes, 1)
    effective_limit = max(limit or 1000, 1)
    effective_skip = max(skip or 0, 0)
    status_choices: List[str]
    if statuses:
        status_choices = [item.strip() for item in statuses.split(",") if item.strip()]
        if not status_choices:
            status_choices = settings.timelog_statuses
    else:
        status_choices = settings.timelog_statuses

    if bool(start_time) ^ bool(end_time):
        raise ValueError("Provide both start_time and end_time, or omit both.")

    start_dt: Optional[datetime] = None
    end_dt: Optional[datetime] = None
    window_duration: Optional[float] = None
    if start_time and end_time:
        start_dt, end_dt = resolve_auto_window(start_time, end_time)
        window_duration = (end_dt - start_dt).total_seconds() / 3600
    headers = utils.build_auth_headers(context.auth)
    pipeline = _build_timelog_pipeline(
        assets,
        start_dt,
        end_dt,
        status_choices,
        effective_step,
        effective_limit,
        effective_skip,
    )

    (api_result, api_debug) = asyncio.run(
        utils.execute_data_api_pipeline(
            settings.timelog_provider,
            settings.timelog_dataset,
            pipeline,
            headers,
        )
    )

    if context.verbose:
        query_payload = {
            "assets": assets,
            "step_minutes": effective_step,
            "statuses": status_choices,
            "limit": effective_limit,
            "provider": settings.timelog_provider,
            "dataset": settings.timelog_dataset,
            "window": (
                {
                    "start": start_dt.isoformat(),
                    "end": end_dt.isoformat(),
                    "duration_hours": round(window_duration or 0, 2),
                }
                if start_dt and end_dt
                else None
            ),
        }
        payload: Any = {
            "query": query_payload,
            "result": api_result,
            "api_debug": api_debug,
        }
    else:
        payload = api_result
    return ToolResult(payload=payload, metadata={"assets": len(assets)})
