"""Output rendering utilities."""
from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List

try:  # pragma: no cover - optional dependency
    import plotext as plt  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    plt = None

from corva_cli.tools.base import OutputFormat, ToolResult


def _dict_to_markdown_table(data: Dict[str, Any]) -> str:
    lines = ["| Field | Value |", "| --- | --- |"]
    for key, value in data.items():
        lines.append(f"| {key} | `{value}` |")
    return "\n".join(lines)


def _list_to_markdown(items: Iterable[Any]) -> str:
    rendered: List[str] = []
    for item in items:
        if isinstance(item, dict):
            rendered.append(_dict_to_markdown_table(item))
        else:
            rendered.append(f"- `{item}`")
    return "\n\n".join(rendered)


def format_result(result: ToolResult, output: OutputFormat) -> str:
    """Return the rendered payload string."""

    if output is OutputFormat.JSON:
        return json.dumps(result.payload, indent=2)
    if output is OutputFormat.MARKDOWN:
        payload = result.payload
        if isinstance(payload, dict):
            return _dict_to_markdown_table(payload)
        if isinstance(payload, list):
            return _list_to_markdown(payload)
        return f"`{payload}`"

    raise ValueError(f"Unsupported output format: {output}")


def preview_plot(result: ToolResult) -> str:
    """Render a quick plotext preview when numeric samples are available.

    This is future-looking glue so that when Plotly support is added it will
    only require extending this function.
    """

    payload = result.payload
    samples = None
    if isinstance(payload, dict) and "samples" in payload:
        samples = [entry.get("volume") for entry in payload["samples"] if "volume" in entry]

    if not samples or plt is None:
        return ""

    plt.clt()
    plt.plot(samples)
    plt.title("Sample preview")
    plt_output = plt.build()
    plt.cld()
    return plt_output


__all__ = ["format_result", "preview_plot"]
