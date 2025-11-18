"""Timelog sample tool used for CLI scaffolding."""
from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional

from corva_cli.settings import get_settings
from corva_cli.timewindow import resolve_auto_window
from corva_cli.tools.base import ParameterSpec, ToolContext, ToolResult
from corva_cli.tools.registry import registry
from corva_cli import utils


def _build_timelog_pipeline(
    assets: List[str],
    start_dt: datetime,
    end_dt: datetime,
    statuses: List[str],
    step_minutes: int,
) -> List[Dict[str, Any]]:
    window_filter = {
        "asset_id": {"$in": assets},
        "timestamp": {"$gte": start_dt.isoformat(), "$lt": end_dt.isoformat()},
    }
    return [
        {"$match": window_filter},
        {"$sort": {"timestamp": 1}},
        {
            "$addFields": {
                "_requested_step_minutes": step_minutes,
                "_requested_statuses": statuses,
                "_requested_window": {"start": start_dt.isoformat(), "end": end_dt.isoformat()},
            }
        },
    ]


@registry.tool(
    name="get-timelog-data",
    description="Return synthetic timelog data for one or more assets.",
    parameters=[
        ParameterSpec(
            "asset_ids",
            help="Comma-separated asset identifiers",
        ),
        ParameterSpec(
            "start_time",
            help="Window start in auto_* syntax (e.g. auto_2h30m)",
        ),
        ParameterSpec(
            "end_time",
            help="Window end in auto_* syntax (e.g. auto_0d)",
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
    ],
)
def get_timelog_data(
    context: ToolContext,
    asset_ids: str,
    start_time: str,
    end_time: str,
    step_minutes: Optional[int] = None,
    statuses: Optional[str] = None,
) -> ToolResult:
    assets = [asset.strip() for asset in asset_ids.split(",") if asset.strip()]
    if not assets:
        raise ValueError("Provide at least one asset id.")

    settings = get_settings()
    effective_step = max(step_minutes or settings.timelog_step_minutes, 1)
    status_choices: List[str]
    if statuses:
        status_choices = [item.strip() for item in statuses.split(",") if item.strip()]
        if not status_choices:
            status_choices = settings.timelog_statuses
    else:
        status_choices = settings.timelog_statuses

    start_dt, end_dt = resolve_auto_window(start_time, end_time)
    window_duration = end_dt - start_dt
    headers = utils.build_auth_headers(context.auth)
    pipeline = _build_timelog_pipeline(assets, start_dt, end_dt, status_choices, effective_step)

    api_result = asyncio.run(
        utils.execute_data_api_pipeline(
            settings.timelog_provider,
            settings.timelog_dataset,
            pipeline,
            headers,
        )
    )

    payload = {
        "query": {
            "assets": assets,
            "window": {
                "start": start_dt.isoformat(),
                "end": end_dt.isoformat(),
                "duration_hours": round(window_duration.total_seconds() / 3600, 2),
            },
            "step_minutes": effective_step,
            "statuses": status_choices,
            "provider": settings.timelog_provider,
            "dataset": settings.timelog_dataset,
        },
        "result": api_result,
    }
    return ToolResult(payload=payload, metadata={"assets": len(assets)})
