"""Typer application entrypoint."""
from __future__ import annotations

import inspect
import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import httpx
import typer

from corva_cli import __version__
from corva_cli.auth import AuthError, resolve_auth
from corva_cli.datasets import DatasetMeta, load_corva_company_datasets
from corva_cli.grouping import GroupConfigError, GroupItem, GroupSpec, load_groups
from corva_cli.output import format_result, preview_plot
from corva_cli.tools.base import OutputFormat, ParameterSpec, ToolContext, ToolResult
from corva_cli.tools.timelog import _dataset_requirement_groups, _ensure_dataset_requirements
from corva_cli.tools.registry import load_builtin_tools, registry
from corva_cli.utils import build_auth_headers

app = typer.Typer(help="Corva pluggable CLI")
group_app = typer.Typer(help="Run grouped tool definitions")
app.add_typer(group_app, name="group")

# Ensure built-in tools are registered on import
load_builtin_tools()


def _common_parameters() -> List[inspect.Parameter]:
    return [
        inspect.Parameter(
            "api_key",
            inspect.Parameter.KEYWORD_ONLY,
            default=typer.Option(
                None,
                "--api-key",
                metavar="TOKEN",
                help="Corva API key",
                envvar="CORVA_API_KEY",
            ),
        ),
        inspect.Parameter(
            "jwt",
            inspect.Parameter.KEYWORD_ONLY,
            default=typer.Option(
                None,
                "--jwt",
                metavar="TOKEN",
                help="JWT credential",
                envvar="CORVA_JWT",
            ),
        ),
        inspect.Parameter(
            "output",
            inspect.Parameter.KEYWORD_ONLY,
            annotation=OutputFormat,
            default=typer.Option(OutputFormat.JSON, "--output", case_sensitive=False, help="json or markdown"),
        ),
        inspect.Parameter(
            "show_plot",
            inspect.Parameter.KEYWORD_ONLY,
            annotation=bool,
            default=typer.Option(False, "--show-plot", help="Attempt a plotext preview when supported"),
        ),
        inspect.Parameter(
            "verbose",
            inspect.Parameter.KEYWORD_ONLY,
            annotation=bool,
            default=typer.Option(False, "--verbose", help="Include query/debug metadata in output"),
        ),
    ]


def _parameter_to_option(spec: ParameterSpec) -> inspect.Parameter:
    default_value = spec.default
    option_default = ... if spec.required and default_value is inspect._empty else (
        None if default_value is inspect._empty else default_value
    )
    return inspect.Parameter(
        spec.name,
        inspect.Parameter.KEYWORD_ONLY,
        annotation=spec.type,
        default=typer.Option(
            option_default,
            help=spec.help,
            autocompletion=spec.completion,
            callback=spec.callback,
        ),
    )


def _build_signature(specs: List[ParameterSpec]) -> inspect.Signature:
    params = _common_parameters()
    params.extend(_parameter_to_option(spec) for spec in specs)
    return inspect.Signature(parameters=params)


def _execute_tool_by_name(
    tool_name: str,
    api_key: Optional[str],
    jwt: Optional[str],
    output: OutputFormat,
    show_plot: bool,
    verbose: bool,
    **tool_kwargs: Any,
):
    tool = registry.get(tool_name)
    auth_ctx = resolve_auth(api_key, jwt)
    context = ToolContext(auth=auth_ctx, output_format=output, verbose=verbose)
    result = tool.callback(context, **tool_kwargs)
    typer.echo(format_result(result, output))
    if show_plot:
        preview = preview_plot(result)
        if preview:
            typer.echo(preview)
    return result


def _register_tool_command() -> None:
    for tool in registry.all():
        def command_factory(current_tool):
            def command(**kwargs: Any):
                try:
                    return _execute_tool_by_name(current_tool.name, **kwargs)
                except AuthError as exc:
                    raise typer.BadParameter(str(exc)) from exc
                except Exception as exc:
                    typer.secho(f"Error running {current_tool.name}: {exc}", fg=typer.colors.RED)
                    raise typer.Exit(code=1) from exc

            command.__doc__ = current_tool.description
            command.__signature__ = _build_signature(current_tool.parameters)
            return command

        cmd = command_factory(tool)
        app.command(tool.name)(cmd)


REGISTERED_GROUP_COMMANDS: Set[str] = set()


def _parse_group_file(path: Path):
    try:
        return load_groups(path)
    except GroupConfigError as exc:
        typer.secho(str(exc), fg=typer.colors.RED)
        raise typer.Exit(code=1) from exc


DEFAULT_GROUPS_FILE = Path("groups/generated_groups.json")
DATASET_FILTER_URL = "https://api.corva.ai/v2/datasets/filtered_by_apps"


def _fetch_app_dataset_names(headers: Dict[str, str], app_id: int) -> List[str]:
    response = httpx.get(
        DATASET_FILTER_URL,
        headers=headers,
        params={"app_ids[]": app_id},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    entries = payload.get("data") or []
    names: List[str] = []
    for entry in entries:
        attributes = entry.get("attributes") or {}
        name = attributes.get("name")
        if name:
            names.append(name)
    return names


def _dataset_meta_lookup() -> Tuple[Dict[str, DatasetMeta], Dict[str, DatasetMeta]]:
    metas = load_corva_company_datasets()
    by_full_name = {meta.name: meta for meta in metas}
    by_dataset = {meta.dataset: meta for meta in metas}
    return by_full_name, by_dataset


def _map_dataset_names_to_commands(dataset_names: Iterable[str]) -> Tuple[List[Tuple[str, DatasetMeta]], List[str]]:
    by_full_name, by_dataset = _dataset_meta_lookup()
    commands: List[Tuple[str, DatasetMeta]] = []
    missing: List[str] = []
    seen: Set[str] = set()
    for dataset_name in dataset_names:
        meta = by_full_name.get(dataset_name)
        if meta is None and "#" in dataset_name:
            _, dataset_suffix = dataset_name.split("#", 1)
            meta = by_dataset.get(dataset_suffix)
        if meta is None:
            missing.append(dataset_name)
            continue
        command_name = f"dataset-{meta.slug}"
        if command_name not in seen:
            commands.append((command_name, meta))
            seen.add(command_name)
    return commands, missing


def _build_shared_params(
    asset_ids: Optional[str],
    company_id: Optional[int],
    start_time: Optional[str],
    end_time: Optional[str],
    depth_start: Optional[float],
    depth_end: Optional[float],
    limit: Optional[int],
    skip: Optional[int],
) -> Dict[str, Any]:
    params: Dict[str, Any] = {}
    if asset_ids is not None:
        params["asset_ids"] = asset_ids
    if company_id is not None:
        params["company_id"] = company_id
    if start_time is not None:
        params["start_time"] = start_time
    if end_time is not None:
        params["end_time"] = end_time
    if depth_start is not None:
        params["depth_start"] = depth_start
    if depth_end is not None:
        params["depth_end"] = depth_end
    if limit is not None:
        params["limit"] = limit
    if skip is not None:
        params["skip"] = skip
    return params


def _collect_requirement_errors(
    command_entries: List[Tuple[str, DatasetMeta]],
    shared_params: Dict[str, Any],
) -> List[str]:
    errors: List[str] = []
    asset_ids = shared_params.get("asset_ids")
    company_id = shared_params.get("company_id")
    start_time = shared_params.get("start_time")
    end_time = shared_params.get("end_time")
    depth_start = shared_params.get("depth_start")
    depth_end = shared_params.get("depth_end")

    for command_name, meta in command_entries:
        requirement_groups = _dataset_requirement_groups(meta)
        try:
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
        except ValueError as exc:
            errors.append(f"{command_name}: {exc}")
    return errors


def _load_or_init_groups_file(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"groups": []}
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError as exc:  # pragma: no cover - invalid user file
        raise GroupConfigError(f"Invalid groups file {path}: {exc}") from exc
    groups_value = data.get("groups")
    if groups_value is None:
        data["groups"] = []
        return data
    if not isinstance(groups_value, list):
        raise GroupConfigError(f"Group file {path} must contain a 'groups' list.")
    return data


def _write_groups_file(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2) + "\n")


def _command_name_for_group(name: str) -> str:
    slug = re.sub(r"[^0-9a-zA-Z_-]+", "-", name).strip("-").lower()
    return slug or "generated-group"


def _execute_group_spec(
    spec: GroupSpec,
    auth_ctx,
    output: OutputFormat,
    show_plot: bool,
    verbose: bool,
    runtime_overrides: Dict[str, Any],
) -> ToolResult:
    combined_payload: Dict[str, Any] = {}
    combined_metadata: Dict[str, Any] = {}

    def _run_item(item: GroupItem) -> ToolResult:
        tool = registry.get(item.name)
        effective_params = dict(item.params)
        effective_params.update(runtime_overrides)
        context = ToolContext(auth=auth_ctx, output_format=output, verbose=verbose)
        return tool.callback(context, **effective_params)

    tools = spec.tools
    if spec.ordered:
        ordered_items = tools
    else:
        ordered_items = tools  # Simplified sequential execution

    for item in ordered_items:
        result = _run_item(item)
        combined_payload[item.name] = result.payload
        if result.metadata is not None:
            combined_metadata[item.name] = result.metadata

    payload: Dict[str, Any] = {
        "group": spec.name,
        "results": combined_payload,
    }
    if verbose:
        payload["debug"] = {"metadata": combined_metadata}

    result = ToolResult(payload=payload, metadata={"tools": len(spec.tools)})
    typer.echo(format_result(result, output))
    if show_plot:
        preview = preview_plot(result)
        if preview:
            typer.echo(preview)
    return result


def _register_generated_group_commands() -> None:
    path = DEFAULT_GROUPS_FILE
    if not path.exists():
        return
    try:
        groups = load_groups(path)
    except GroupConfigError as exc:  # pragma: no cover - user file issue
        typer.secho(f"Skipping generated groups: {exc}", fg=typer.colors.YELLOW)
        return

    for group_name, spec in groups.items():
        command_name = _command_name_for_group(group_name)
        if command_name in REGISTERED_GROUP_COMMANDS:
            continue

        def command_factory(current_spec: GroupSpec, display_name: str):
            def generated_group_command(
                api_key: Optional[str] = typer.Option(None, envvar="CORVA_API_KEY"),
                jwt: Optional[str] = typer.Option(None, envvar="CORVA_JWT"),
                output: OutputFormat = typer.Option(OutputFormat.JSON, case_sensitive=False),
                show_plot: bool = typer.Option(False, help="Attempt a plot preview"),
                verbose: bool = typer.Option(False, "--verbose", help="Include query/debug metadata"),
                asset_ids: Optional[str] = typer.Option(None, "--asset-ids", help="Override shared asset ids."),
                company_id: Optional[int] = typer.Option(None, "--company-id", help="Override shared company id."),
                start_time: Optional[str] = typer.Option(None, "--start-time", help="Override shared window start."),
                end_time: Optional[str] = typer.Option(None, "--end-time", help="Override shared window end."),
                depth_start: Optional[float] = typer.Option(None, "--depth-start", help="Override depth start."),
                depth_end: Optional[float] = typer.Option(None, "--depth-end", help="Override depth end."),
                limit: Optional[int] = typer.Option(None, "--limit", help="Override limit."),
                skip: Optional[int] = typer.Option(None, "--skip", help="Override skip."),
            ):
                try:
                    auth_ctx = resolve_auth(api_key, jwt)
                except AuthError as exc:
                    raise typer.BadParameter(str(exc)) from exc

                overrides = _build_shared_params(
                    asset_ids,
                    company_id,
                    start_time,
                    end_time,
                    depth_start,
                    depth_end,
                    limit,
                    skip,
                )
                _execute_group_spec(
                    current_spec,
                    auth_ctx,
                    output,
                    show_plot,
                    verbose,
                    overrides,
                )

            generated_group_command.__name__ = f"group_{command_name}"
            generated_group_command.__doc__ = f"Generated group '{display_name}'."
            return generated_group_command

        cmd = command_factory(spec, group_name)
        app.command(command_name)(cmd)
        REGISTERED_GROUP_COMMANDS.add(command_name)


@group_app.command("run")
def run_group(
    groups_file: Path = typer.Argument(..., help="Path to group definition JSON"),
    name: str = typer.Option(..., "--name", help="Group name to execute"),
    api_key: Optional[str] = typer.Option(None, envvar="CORVA_API_KEY"),
    jwt: Optional[str] = typer.Option(None, envvar="CORVA_JWT"),
    output: OutputFormat = typer.Option(OutputFormat.JSON, case_sensitive=False),
    show_plot: bool = typer.Option(False, help="Attempt a plot preview"),
    verbose: bool = typer.Option(False, "--verbose", help="Include query/debug metadata"),
    asset_ids: Optional[str] = typer.Option(None, "--asset-ids", help="Override shared asset ids."),
    company_id: Optional[int] = typer.Option(None, "--company-id", help="Override shared company id."),
    start_time: Optional[str] = typer.Option(None, "--start-time", help="Override shared window start."),
    end_time: Optional[str] = typer.Option(None, "--end-time", help="Override shared window end."),
    depth_start: Optional[float] = typer.Option(None, "--depth-start", help="Override depth start."),
    depth_end: Optional[float] = typer.Option(None, "--depth-end", help="Override depth end."),
    limit: Optional[int] = typer.Option(None, "--limit", help="Override limit."),
    skip: Optional[int] = typer.Option(None, "--skip", help="Override skip."),
) -> None:
    groups = _parse_group_file(groups_file)
    if name not in groups:
        typer.secho(f"Group '{name}' not found in {groups_file}", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    auth_ctx = resolve_auth(api_key, jwt)
    runtime_overrides = _build_shared_params(
        asset_ids,
        company_id,
        start_time,
        end_time,
        depth_start,
        depth_end,
        limit,
        skip,
    )

    spec = groups[name]
    _execute_group_spec(
        spec,
        auth_ctx,
        output,
        show_plot,
        verbose,
        runtime_overrides,
    )


@group_app.command("create")
def create_group(
    app_id: int = typer.Option(..., "--app-id", "-a", help="Application identifier."),
    group_name: str = typer.Option(..., "--group-name", "-g", help="Name for the generated group."),
    groups_file: Path = typer.Option(
        DEFAULT_GROUPS_FILE,
        "--groups-file",
        "-f",
        help="Group definition JSON to create or update.",
    ),
    token: Optional[str] = typer.Option(None, "--token", "-t", help="Corva API token"),
    jwt: Optional[str] = typer.Option(None, "--jwt", "-j", help="JWT credential"),
    asset_ids: Optional[str] = typer.Option(None, "--asset-ids", help="Shared comma-separated asset IDs."),
    company_id: Optional[int] = typer.Option(None, "--company-id", help="Shared company id."),
    start_time: Optional[str] = typer.Option(None, "--start-time", help="Shared start time (auto_* syntax)."),
    end_time: Optional[str] = typer.Option(None, "--end-time", help="Shared end time (auto_* syntax)."),
    depth_start: Optional[float] = typer.Option(None, "--depth-start", help="Shared depth start."),
    depth_end: Optional[float] = typer.Option(None, "--depth-end", help="Shared depth end."),
    limit: Optional[int] = typer.Option(None, "--limit", help="Shared limit override."),
    skip: Optional[int] = typer.Option(None, "--skip", help="Shared skip override."),
) -> None:
    """Create/update a group using datasets assigned to an app."""

    try:
        auth_ctx = resolve_auth(token, jwt)
    except AuthError as exc:
        typer.secho(str(exc), fg=typer.colors.RED)
        raise typer.Exit(code=1) from exc

    headers = build_auth_headers(auth_ctx)
    try:
        dataset_names = _fetch_app_dataset_names(headers, app_id)
    except httpx.HTTPError as exc:
        typer.secho(f"Failed to fetch datasets for app {app_id}: {exc}", fg=typer.colors.RED)
        raise typer.Exit(code=1) from exc

    if not dataset_names:
        typer.secho(f"No datasets returned for app {app_id}.", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    command_entries, missing = _map_dataset_names_to_commands(dataset_names)
    if not command_entries:
        typer.secho(
            "None of the app datasets matched known CLI dataset commands. "
            "Update docs/dataset.json or request different app permissions.",
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=1)

    shared_params = _build_shared_params(
        asset_ids,
        company_id,
        start_time,
        end_time,
        depth_start,
        depth_end,
        limit,
        skip,
    )
    if not shared_params:
        typer.secho(
            "No shared dataset parameters were provided. Generated group will rely on command defaults.",
            fg=typer.colors.YELLOW,
        )

    requirement_errors = _collect_requirement_errors(command_entries, shared_params)
    if requirement_errors:
        typer.secho(
            "Shared parameters may not satisfy every dataset requirement. "
            "Missing filters will need to be supplied when running the group:",
            fg=typer.colors.YELLOW,
        )
        for error in requirement_errors:
            typer.secho(f"- {error}", fg=typer.colors.YELLOW)

    try:
        groups_doc = _load_or_init_groups_file(groups_file)
    except GroupConfigError as exc:
        typer.secho(str(exc), fg=typer.colors.RED)
        raise typer.Exit(code=1) from exc

    existing_groups = [
        entry for entry in groups_doc["groups"] if entry.get("name") != group_name
    ]
    group_entry = {
        "name": group_name,
        "ordered": True,
        "tools": [
            {"name": command, "params": dict(shared_params)}
            for command, _ in command_entries
        ],
    }
    existing_groups.append(group_entry)
    groups_doc["groups"] = existing_groups
    _write_groups_file(groups_file, groups_doc)

    typer.secho(
        f"Group '{group_name}' with {len(command_entries)} dataset command(s) saved to {groups_file}",
        fg=typer.colors.GREEN,
    )
    if missing:
        typer.secho(
            "Skipped datasets without CLI mappings: " + ", ".join(missing),
            fg=typer.colors.YELLOW,
        )
    typer.echo(
        "Run it directly via: "
        f"uv run corva {_command_name_for_group(group_name)} --token/--jwt ... "
        "or rerun group create to refresh."
    )


@app.callback()
def main(ctx: typer.Context, version: bool = typer.Option(False, "--version", help="Show version and exit")):
    if version:
        typer.echo(__version__)
        raise typer.Exit()


_register_tool_command()
_register_generated_group_commands()


__all__ = ["app"]
