"""Timelog sample tool used for CLI scaffolding."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from functools import lru_cache
import inspect
from typing import Any, Dict, FrozenSet, Iterable, List, Optional, Set, Tuple

import typer

from corva_cli.datasets import DatasetMeta, load_corva_company_datasets
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
    company_id: Optional[int],
    start_dt: Optional[datetime],
    end_dt: Optional[datetime],
    depth_range: Optional[Tuple[float, float]],
    limit: int,
    skip: int,
    time_field: Optional[str] = None,
    depth_field: Optional[str] = None,
    extra_filters: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    effective_time_field = time_field
    effective_depth_field = depth_field
    window_filter: Dict[str, Any] = {}
    if assets:
        asset_filter: Any
        if len(assets) == 1:
            asset_filter = assets[0]
        else:
            asset_filter = {"$in": assets}
        window_filter["asset_id"] = asset_filter
    if company_id is not None:
        window_filter["company_id"] = company_id
    if effective_time_field and start_dt and end_dt:
        start_epoch = _to_unix_seconds(start_dt)
        end_epoch = _to_unix_seconds(end_dt)
        window_filter[effective_time_field] = {"$gte": start_epoch, "$lte": end_epoch}
    if depth_range and effective_depth_field:
        depth_start, depth_end = depth_range
        window_filter[effective_depth_field] = {"$gte": depth_start, "$lte": depth_end}
    if extra_filters:
        for key, value in extra_filters.items():
            window_filter[key] = value

    stages: List[Dict[str, Any]] = [{"$match": window_filter}]
    if skip > 0:
        stages.append({"$skip": skip})
    stages.append({"$limit": limit})
    if effective_time_field:
        stages.append({"$sort": {effective_time_field: -1}})
        iso_field_name = f"{effective_time_field}_iso"
        add_fields: Dict[str, Any] = {
            iso_field_name: {
                "$dateToString": {
                    "date": {
                        "$toDate": {
                            "$multiply": [f"${effective_time_field}", 1000],
                        }
                    },
                    "format": "%Y-%m-%dT%H:%M:%S.000Z",
                }
            }
        }
        if effective_time_field.endswith("start_time"):
            add_fields["data.end_time_iso"] = {
                "$dateToString": {
                    "date": {
                        "$toDate": {
                            "$multiply": ["$data.end_time", 1000],
                        }
                    },
                    "format": "%Y-%m-%dT%H:%M:%S.000Z",
                }
            }
        stages.append({"$addFields": add_fields})
    return stages


def _common_optionals(company_spec: ParameterSpec) -> List[ParameterSpec]:
    return [
        company_spec,
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
    ]


ASSET_ID_REQUIRED = ParameterSpec(
    "asset_ids",
    help="Comma-separated integer asset IDs",
)

ASSET_ID_OPTIONAL = ParameterSpec(
    "asset_ids",
    help="Comma-separated integer asset IDs",
    required=False,
    default="",
)

DEPTH_START_OPTIONAL = ParameterSpec(
    "depth_start",
    type=float,
    help="Depth range start",
    required=False,
    default=None,
)

DEPTH_END_OPTIONAL = ParameterSpec(
    "depth_end",
    type=float,
    help="Depth range end",
    required=False,
    default=None,
)


def _require_company_when_no_assets(ctx, param, value):
    asset_ids = ctx.params.get("asset_ids", "")
    if not asset_ids and value is None:
        raise typer.BadParameter("Provide --company-id when --asset-ids is omitted.")
    return value


TIMELOG_PARAMETERS = [
    ASSET_ID_REQUIRED,
    *_common_optionals(
        ParameterSpec(
            "company_id",
            type=int,
            help="Single company identifier",
            required=False,
            default=None,
        )
    ),
    DEPTH_START_OPTIONAL,
    DEPTH_END_OPTIONAL,
]

ASSETS_PARAMETERS = [
    ASSET_ID_OPTIONAL,
    *_common_optionals(
        ParameterSpec(
            "company_id",
            type=int,
            help="Single company identifier",
            required=False,
            default=None,
            callback=_require_company_when_no_assets,
        )
    ),
    DEPTH_START_OPTIONAL,
    DEPTH_END_OPTIONAL,
]


TIME_FIELD_OVERRIDES_BY_DATASET: Dict[str, Optional[str]] = {
    "activity-groups": "data.start_time",
    "well.design_optimization": "data.start_time",
    "well.design_optimization.timelog": "data.start_time",
    "assets": None,
}

TIME_FIELD_OVERRIDES_BY_SLUG: Dict[str, Optional[str]] = {
    "activity-groups": "data.start_time",
    "well-design-optimization": "data.start_time",
    "well-design-optimization-timelog": "data.start_time",
    "assets": None,
}


METRICS_PARAMETERS = [
    ASSET_ID_OPTIONAL,
    *_common_optionals(
        ParameterSpec(
            "company_id",
            type=int,
            help="Single company identifier",
            required=False,
            default=None,
            callback=_require_company_when_no_assets,
        )
    ),
    ParameterSpec(
        "metric_type",
        help="Metric type (maps to data.type)",
    ),
    ParameterSpec(
        "metric_keys",
        help="Comma-separated metric keys (maps to data.key)",
    ),
]


@lru_cache()
def _get_dataset_meta_by_name(dataset_name: str) -> Optional[DatasetMeta]:
    metas = load_corva_company_datasets()
    for meta in metas:
        if meta.dataset == dataset_name:
            return meta
    return None


RequirementToken = str


TOKEN_ASSET_IDS: RequirementToken = "asset_ids"
TOKEN_COMPANY_ID: RequirementToken = "company_id"
TOKEN_TIME_WINDOW: RequirementToken = "time_window"
TOKEN_DEPTH_RANGE: RequirementToken = "depth_range"


_INDEX_FIELD_TOKEN_MAP: Dict[str, Set[RequirementToken]] = {
    "asset_id": {TOKEN_ASSET_IDS},
    "company_id": {TOKEN_COMPANY_ID},
    "timestamp": {TOKEN_TIME_WINDOW},
    "start_time": {TOKEN_TIME_WINDOW},
    "end_time": {TOKEN_TIME_WINDOW},
    "depth": {TOKEN_DEPTH_RANGE},
}

_TOKEN_LABELS: Dict[RequirementToken, str] = {
    TOKEN_ASSET_IDS: "--asset-ids",
    TOKEN_COMPANY_ID: "--company-id",
    TOKEN_TIME_WINDOW: "--start-time/--end-time",
    TOKEN_DEPTH_RANGE: "--depth-start/--depth-end",
}

_TOKEN_SORT_ORDER: Dict[RequirementToken, int] = {
    TOKEN_ASSET_IDS: 0,
    TOKEN_COMPANY_ID: 1,
    TOKEN_TIME_WINDOW: 2,
    TOKEN_DEPTH_RANGE: 3,
}


def _normalize_index_field_name(field_name: str) -> Optional[str]:
    lowered = field_name.lower()
    tail = lowered.split(".")[-1]
    if tail in {"asset_id", "company_id", "timestamp", "start_time", "end_time"}:
        return tail
    if "depth" in tail or "depth" in lowered:
        return "depth"
    return None


def _iter_index_fields(meta: DatasetMeta) -> Iterable[str]:
    for index_fields in meta.indexes:
        for field_name in index_fields:
            yield field_name


def _resolve_time_field(meta: DatasetMeta) -> Optional[str]:
    if meta.dataset in TIME_FIELD_OVERRIDES_BY_DATASET:
        return TIME_FIELD_OVERRIDES_BY_DATASET[meta.dataset]
    slug = meta.slug
    if slug and slug in TIME_FIELD_OVERRIDES_BY_SLUG:
        return TIME_FIELD_OVERRIDES_BY_SLUG[slug]

    fields = list(_iter_index_fields(meta))
    for field_name in fields:
        if "timestamp" in field_name.lower():
            return field_name
    for field_name in fields:
        if "start_time" in field_name.lower():
            return field_name
    for field_name in fields:
        if "time" in field_name.lower():
            return field_name
    return None


def _resolve_depth_field(meta: DatasetMeta) -> Optional[str]:
    fields = list(_iter_index_fields(meta))
    priority = ["measured_depth", "hole_depth", "bit_depth", "depth"]
    for keyword in priority:
        for field_name in fields:
            if keyword in field_name.lower():
                return field_name
    return None


def _dataset_requirement_groups(meta: DatasetMeta) -> List[FrozenSet[RequirementToken]]:
    groups: List[FrozenSet[RequirementToken]] = []
    for index_fields in meta.indexes:
        tokens: Set[RequirementToken] = set()
        for field_name in index_fields:
            normalized = _normalize_index_field_name(field_name)
            if not normalized:
                continue
            tokens.update(_INDEX_FIELD_TOKEN_MAP.get(normalized, set()))
        if tokens:
            groups.append(frozenset(tokens))
    deduped: List[FrozenSet[RequirementToken]] = []
    seen: Set[FrozenSet[RequirementToken]] = set()
    for group in groups:
        if group not in seen:
            seen.add(group)
            deduped.append(group)
    return deduped


def _has_asset_ids(asset_ids: Optional[str]) -> bool:
    if not asset_ids:
        return False
    return any(part.strip() for part in asset_ids.split(","))


def _format_requirement_group(group: FrozenSet[RequirementToken]) -> str:
    ordered_tokens = sorted(group, key=lambda token: _TOKEN_SORT_ORDER.get(token, 99))
    return " + ".join(_TOKEN_LABELS[token] for token in ordered_tokens)


def _group_satisfied(
    group: FrozenSet[RequirementToken],
    asset_ids: Optional[str],
    company_id: Optional[int],
    start_time: Optional[str],
    end_time: Optional[str],
    depth_start: Optional[float],
    depth_end: Optional[float],
) -> bool:
    for token in group:
        if token == TOKEN_ASSET_IDS and not _has_asset_ids(asset_ids):
            return False
        if token == TOKEN_COMPANY_ID and company_id is None:
            return False
        if token == TOKEN_TIME_WINDOW and (not start_time or not end_time):
            return False
        if token == TOKEN_DEPTH_RANGE and (depth_start is None or depth_end is None):
            return False
    return True


def _ensure_dataset_requirements(
    meta: DatasetMeta,
    requirement_groups: List[FrozenSet[RequirementToken]],
    asset_ids: Optional[str],
    company_id: Optional[int],
    start_time: Optional[str],
    end_time: Optional[str],
    depth_start: Optional[float],
    depth_end: Optional[float],
) -> None:
    if not requirement_groups:
        return
    for group in requirement_groups:
        if _group_satisfied(group, asset_ids, company_id, start_time, end_time, depth_start, depth_end):
            return
    options = ", ".join(_format_requirement_group(group) for group in requirement_groups)
    raise ValueError(f"{meta.friendly_name} requires filters matching one of: {options}.")


def _run_dataset_query(
    dataset_name: Optional[str],
    context: ToolContext,
    asset_ids: Optional[str],
    company_id: Optional[int] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    limit: Optional[int] = 1000,
    skip: Optional[int] = 0,
    require_assets: bool = True,
    depth_start: Optional[float] = None,
    depth_end: Optional[float] = None,
    provider_override: Optional[str] = None,
    time_field: Optional[str] = None,
    depth_field: Optional[str] = None,
    extra_match: Optional[Dict[str, Any]] = None,
) -> ToolResult:
    asset_ids = asset_ids or ""
    raw_assets = [asset.strip() for asset in asset_ids.split(",") if asset.strip()]
    if not raw_assets:
        if require_assets:
            raise ValueError("Provide at least one asset id.")
        assets: List[int] = []
    else:
        try:
            assets = [int(asset) for asset in raw_assets]
        except ValueError as exc:
            raise ValueError("Asset ids must be integers.") from exc

    settings = get_settings()
    dataset = dataset_name or "drilling.timelog.data"
    provider = provider_override or "corva"
    effective_time_field = time_field
    effective_depth_field = depth_field
    if effective_time_field is None or effective_depth_field is None:
        meta = _get_dataset_meta_by_name(dataset)
        if meta:
            if effective_time_field is None:
                effective_time_field = _resolve_time_field(meta)
            if effective_depth_field is None:
                effective_depth_field = _resolve_depth_field(meta)
    effective_limit = max(limit or 1000, 1)
    effective_skip = max(skip or 0, 0)
    if bool(start_time) ^ bool(end_time):
        raise ValueError("Provide both start_time and end_time, or omit both.")

    start_dt: Optional[datetime] = None
    end_dt: Optional[datetime] = None
    window_duration: Optional[float] = None
    if start_time and end_time:
        start_dt, end_dt = resolve_auto_window(start_time, end_time)
        window_duration = (end_dt - start_dt).total_seconds() / 3600
    headers = utils.build_auth_headers(context.auth)
    depth_range = None
    if depth_start is not None and depth_end is not None:
        depth_range = (float(depth_start), float(depth_end))
    if extra_match:
        for key, value in extra_match.items():
            window_filter_key = key  # placeholder for readability
    # Wait: we need window_filter, but we only have it inside pipeline builder earlier. need restructure.

    pipeline = _build_timelog_pipeline(
        assets,
        company_id,
        start_dt,
        end_dt,
        depth_range,
        effective_limit,
        effective_skip,
        time_field=effective_time_field,
        depth_field=effective_depth_field,
        extra_filters=extra_match,
    )

    (api_result, api_debug) = asyncio.run(
        utils.execute_data_api_pipeline(
            provider,
            dataset,
            pipeline,
            headers,
        )
    )

    if context.verbose:
        query_payload = {
            "assets": assets,
            "company_id": company_id,
            "limit": effective_limit,
            "provider": provider,
            "dataset": dataset,
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


@registry.tool(
    name="timelog",
    description="Return timelog data for one or more assets.",
    parameters=TIMELOG_PARAMETERS,
)
def get_timelog_data(
    context: ToolContext,
    asset_ids: str,
    company_id: Optional[int] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    limit: Optional[int] = 1000,
    skip: Optional[int] = 0,
    depth_start: Optional[float] = None,
    depth_end: Optional[float] = None,
) -> ToolResult:
    return _run_dataset_query(
        None,
        context,
        asset_ids,
        company_id,
        start_time,
        end_time,
        limit,
        skip,
        depth_start=depth_start,
        depth_end=depth_end,
    )


@registry.tool(
    name="assets",
    description="Return asset metadata for one or more assets.",
    parameters=ASSETS_PARAMETERS,
)
def get_assets(
    context: ToolContext,
    asset_ids: str = "",
    company_id: Optional[int] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    limit: Optional[int] = 1000,
    skip: Optional[int] = 0,
    depth_start: Optional[float] = None,
    depth_end: Optional[float] = None,
) -> ToolResult:
    return _run_dataset_query(
        "assets",
        context,
        asset_ids,
        company_id,
        start_time,
        end_time,
        limit,
        skip,
        require_assets=False,
        depth_start=depth_start,
        depth_end=depth_end,
    )


# ---------------------------------------------------------------------------
# Dataset-driven commands
# ---------------------------------------------------------------------------

def _build_dataset_parameters(meta: DatasetMeta) -> List[ParameterSpec]:
    params: List[ParameterSpec] = [
        ParameterSpec(
            "asset_ids",
            help="Comma-separated integer asset IDs",
            required=False,
            default="",
        ),
        ParameterSpec(
            "company_id",
            type=int,
            help="Company identifier",
            required=False,
            default=None,
        ),
    ]
    params.extend(
        [
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
                "depth_start",
                type=float,
                help="Depth range start",
                required=False,
                default=None,
            ),
            ParameterSpec(
                "depth_end",
                type=float,
                help="Depth range end",
                required=False,
                default=None,
            ),
        ]
    )
    params.append(
        ParameterSpec(
            "limit",
            type=int,
            help="Max documents returned",
            required=False,
            default=1000,
        )
    )
    params.append(
        ParameterSpec(
            "skip",
            type=int,
            help="Number of documents to skip before collecting results",
            required=False,
            default=0,
        )
    )
    return params


def _register_dataset_tools() -> None:
    metas = load_corva_company_datasets()
    if not metas:
        return
    used: Set[str] = set()
    metrics_meta: Optional[DatasetMeta] = None

    for meta in metas:
        if meta.dataset == "data.metrics":
            metrics_meta = meta
            continue
        slug = meta.slug or meta.name.split("#")[-1]
        command_name = f"dataset-{slug}"
        if command_name in used:
            command_name = f"{command_name}-{meta.company_id}"
        used.add(command_name)
        params = _build_dataset_parameters(meta)

        requirement_groups = _dataset_requirement_groups(meta)
        time_field = _resolve_time_field(meta)
        depth_field = _resolve_depth_field(meta)

        def make_callback(
            dataset: DatasetMeta,
            requirement_groups: List[FrozenSet[RequirementToken]],
            time_field: Optional[str],
            depth_field: Optional[str],
        ):
            def dataset_tool(
                context: ToolContext,
                asset_ids: str = "",
                company_id: Optional[int] = None,
                start_time: Optional[str] = None,
                end_time: Optional[str] = None,
                depth_start: Optional[float] = None,
                depth_end: Optional[float] = None,
                limit: Optional[int] = 1000,
                skip: Optional[int] = 0,
            ) -> ToolResult:
                _ensure_dataset_requirements(
                    dataset,
                    requirement_groups,
                    asset_ids,
                    company_id,
                    start_time,
                    end_time,
                    depth_start,
                    depth_end,
                )
                return _run_dataset_query(
                    dataset.dataset,
                    context,
                    asset_ids,
                    company_id,
                    start_time,
                    end_time,
                    limit=limit,
                    skip=skip,
                    depth_start=depth_start,
                    depth_end=depth_end,
                    provider_override=dataset.provider,
                    require_assets=False,
                    time_field=time_field,
                    depth_field=depth_field,
                )

            return dataset_tool

        registry.tool(
            name=command_name,
            description=meta.description or f"Corva dataset {meta.friendly_name}",
            parameters=params,
        )(make_callback(meta, requirement_groups, time_field, depth_field))

    _register_metrics_tool(metrics_meta)


def _register_metrics_tool(meta: Optional[DatasetMeta]) -> None:
    dataset_name = meta.dataset if meta else "metrics"
    provider = meta.provider if meta else "corva"
    requirement_groups = _dataset_requirement_groups(meta) if meta else []
    time_field = _resolve_time_field(meta) if meta else None
    depth_field = _resolve_depth_field(meta) if meta else None
    description = meta.description if meta else "Corva dataset data.metrics"

    @registry.tool(
        name="dataset-data-metrics",
        description=description,
        parameters=METRICS_PARAMETERS,
    )
    def dataset_data_metrics(
        context: ToolContext,
        asset_ids: str = "",
        company_id: Optional[int] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        metric_type: Optional[str] = None,
        metric_keys: Optional[str] = None,
        limit: Optional[int] = 1000,
        skip: Optional[int] = 0,
        depth_start: Optional[float] = None,
        depth_end: Optional[float] = None,
    ) -> ToolResult:
        if meta:
            _ensure_dataset_requirements(
                meta,
                requirement_groups,
                asset_ids,
                company_id,
                start_time,
                end_time,
                depth_start,
                depth_end,
            )
        if not metric_type:
            raise ValueError("Provide --metric-type for metrics.")
        if not metric_keys:
            raise ValueError("Provide --metric-keys for metrics.")
        keys_list = [key.strip() for key in metric_keys.split(",") if key.strip()]
        if not keys_list:
            raise ValueError("Provide at least one metric key.")
        extra_match = {
            "data.type": metric_type,
            "data.key": {"$in": keys_list},
        }
        return _run_dataset_query(
            dataset_name,
            context,
            asset_ids,
            company_id,
            start_time,
            end_time,
            limit=limit,
            skip=skip,
            depth_start=depth_start,
            depth_end=depth_end,
            provider_override=provider,
            require_assets=False,
            time_field=time_field,
            depth_field=depth_field,
            extra_match=extra_match,
        )


_register_dataset_tools()
