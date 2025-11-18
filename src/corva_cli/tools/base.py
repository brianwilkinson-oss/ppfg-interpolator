"""Base structures for pluggable tools."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import inspect
from typing import Any, Callable, Dict, Iterable, List, Optional, Protocol

from corva_cli.auth import AuthContext


class OutputFormat(str, Enum):
    """Supported output renderers."""

    JSON = "json"
    MARKDOWN = "markdown"


class CompletionFn(Protocol):
    """Signature for option completion callbacks (Typer compatible)."""

    def __call__(self, ctx: Any, param: Any, incomplete: str) -> Iterable[str]:
        """Return a list of completion candidates."""


@dataclass(slots=True)
class ParameterSpec:
    """Metadata describing a CLI parameter for a tool."""

    name: str
    type: type[Any] = str
    help: str = ""
    required: bool = True
    default: Any = inspect._empty
    completion: Optional[CompletionFn] = None


@dataclass(slots=True)
class ToolContext:
    """Runtime context shared with every tool call."""

    auth: AuthContext
    output_format: OutputFormat
    verbose: bool = False
    invoked_at: datetime = field(default_factory=datetime.utcnow)


@dataclass(slots=True)
class ToolResult:
    """Represents a structured result that can be rendered by the CLI."""

    payload: Any
    metadata: Optional[Dict[str, Any]] = None


ToolCallable = Callable[..., ToolResult]


@dataclass(slots=True)
class ToolDefinition:
    """Registered tool metadata and execution hook."""

    name: str
    description: str
    callback: ToolCallable
    parameters: List[ParameterSpec]
    tags: List[str] = field(default_factory=list)


__all__ = [
    "CompletionFn",
    "OutputFormat",
    "ParameterSpec",
    "ToolCallable",
    "ToolContext",
    "ToolDefinition",
    "ToolResult",
]
