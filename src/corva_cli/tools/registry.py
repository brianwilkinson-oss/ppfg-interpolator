"""Tool registry and decorators."""
from __future__ import annotations

from typing import Dict, Iterable, List, Optional

from corva_cli.tools.base import ParameterSpec, ToolCallable, ToolDefinition


class ToolRegistry:
    """Simple in-memory registry for CLI tools."""

    def __init__(self) -> None:
        self._tools: Dict[str, ToolDefinition] = {}

    def register(self, definition: ToolDefinition) -> ToolDefinition:
        if definition.name in self._tools:
            raise ValueError(f"Tool '{definition.name}' is already registered")
        self._tools[definition.name] = definition
        return definition

    def get(self, name: str) -> ToolDefinition:
        try:
            return self._tools[name]
        except KeyError as exc:  # pragma: no cover - defensive
            raise KeyError(f"Unknown tool '{name}'") from exc

    def all(self) -> Iterable[ToolDefinition]:
        return self._tools.values()

    # Decorator helper -----------------------------------------------------
    def tool(
        self,
        *,
        name: str,
        description: str,
        parameters: List[ParameterSpec],
        tags: Optional[List[str]] = None,
    ):
        tags = tags or []

        def decorator(func: ToolCallable) -> ToolCallable:
            definition = ToolDefinition(
                name=name,
                description=description,
                callback=func,
                parameters=parameters,
                tags=tags,
            )
            self.register(definition)
            return func

        return decorator


registry = ToolRegistry()


def load_builtin_tools() -> None:
    """Import modules that register built-in tools."""

    # Importing registers via decorators.
    from corva_cli.tools import timelog  # noqa: F401  # pragma: no cover


__all__ = ["ParameterSpec", "ToolRegistry", "load_builtin_tools", "registry"]
