"""Typer application entrypoint."""
from __future__ import annotations

import inspect
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import httpx
import typer

from corva_cli import __version__
from corva_cli.auth import AuthError, resolve_auth
from corva_cli.datasets import DatasetMeta, load_corva_company_datasets
from corva_cli.grouping import GroupConfigError, GroupRunner, load_groups
from corva_cli.output import format_result, preview_plot
from corva_cli.tools.base import OutputFormat, ParameterSpec, ToolContext
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


def _map_dataset_names_to_commands(dataset_names: Iterable[str]) -> Tuple[List[str], List[str]]:
    by_full_name, by_dataset = _dataset_meta_lookup()
    commands: List[str] = []
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
            commands.append(command_name)
            seen.add(command_name)
    return commands, missing


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


@group_app.command("run")
def run_group(
    groups_file: Path = typer.Argument(..., help="Path to group definition JSON"),
    name: str = typer.Option(..., "--name", help="Group name to execute"),
    api_key: Optional[str] = typer.Option(None, envvar="CORVA_API_KEY"),
    jwt: Optional[str] = typer.Option(None, envvar="CORVA_JWT"),
    output: OutputFormat = typer.Option(OutputFormat.JSON, case_sensitive=False),
    show_plot: bool = typer.Option(False, help="Attempt a plot preview"),
    verbose: bool = typer.Option(False, "--verbose", help="Include query/debug metadata"),
) -> None:
    groups = _parse_group_file(groups_file)
    if name not in groups:
        typer.secho(f"Group '{name}' not found in {groups_file}", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    auth_ctx = resolve_auth(api_key, jwt)

    def executor(tool_name: str, params: Dict[str, Any]):
        tool = registry.get(tool_name)
        context = ToolContext(auth=auth_ctx, output_format=output, verbose=verbose)
        result = tool.callback(context, **params)
        typer.echo(format_result(result, output))
        if show_plot:
            preview = preview_plot(result)
            if preview:
                typer.echo(preview)
        return result

    runner = GroupRunner(executor)
    runner.run(groups[name])


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

    commands, missing = _map_dataset_names_to_commands(dataset_names)
    if not commands:
        typer.secho(
            "None of the app datasets matched known CLI dataset commands. "
            "Update docs/dataset.json or request different app permissions.",
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=1)

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
        "tools": [{"name": command} for command in commands],
    }
    existing_groups.append(group_entry)
    groups_doc["groups"] = existing_groups
    _write_groups_file(groups_file, groups_doc)

    typer.secho(
        f"Group '{group_name}' with {len(commands)} dataset command(s) saved to {groups_file}",
        fg=typer.colors.GREEN,
    )
    if missing:
        typer.secho(
            "Skipped datasets without CLI mappings: " + ", ".join(missing),
            fg=typer.colors.YELLOW,
        )
    typer.echo(
        f"Run it via: uv run corva group run {groups_file} --name {group_name} "
        "and supply --token/--jwt again."
    )


@app.callback()
def main(ctx: typer.Context, version: bool = typer.Option(False, "--version", help="Show version and exit")):
    if version:
        typer.echo(__version__)
        raise typer.Exit()


_register_tool_command()


__all__ = ["app"]
