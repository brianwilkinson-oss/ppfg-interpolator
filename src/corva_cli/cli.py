"""Typer application entrypoint."""
from __future__ import annotations

import inspect
from pathlib import Path
from typing import Any, Dict, List, Optional

import typer

from corva_cli import __version__
from corva_cli.auth import AuthError, resolve_auth
from corva_cli.grouping import GroupConfigError, GroupRunner, load_groups
from corva_cli.output import format_result, preview_plot
from corva_cli.tools.base import OutputFormat, ParameterSpec, ToolContext
from corva_cli.tools.registry import load_builtin_tools, registry

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
    **tool_kwargs: Any,
):
    tool = registry.get(tool_name)
    auth_ctx = resolve_auth(api_key, jwt)
    context = ToolContext(auth=auth_ctx, output_format=output)
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


@group_app.command("run")
def run_group(
    groups_file: Path = typer.Argument(..., help="Path to group definition JSON"),
    name: str = typer.Option(..., "--name", help="Group name to execute"),
    api_key: Optional[str] = typer.Option(None, envvar="CORVA_API_KEY"),
    jwt: Optional[str] = typer.Option(None, envvar="CORVA_JWT"),
    output: OutputFormat = typer.Option(OutputFormat.JSON, case_sensitive=False),
    show_plot: bool = typer.Option(False, help="Attempt a plot preview"),
) -> None:
    groups = _parse_group_file(groups_file)
    if name not in groups:
        typer.secho(f"Group '{name}' not found in {groups_file}", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    auth_ctx = resolve_auth(api_key, jwt)

    def executor(tool_name: str, params: Dict[str, Any]):
        tool = registry.get(tool_name)
        context = ToolContext(auth=auth_ctx, output_format=output)
        result = tool.callback(context, **params)
        typer.echo(format_result(result, output))
        if show_plot:
            preview = preview_plot(result)
            if preview:
                typer.echo(preview)
        return result

    runner = GroupRunner(executor)
    runner.run(groups[name])


@app.callback()
def main(ctx: typer.Context, version: bool = typer.Option(False, "--version", help="Show version and exit")):
    if version:
        typer.echo(__version__)
        raise typer.Exit()


_register_tool_command()


__all__ = ["app"]
