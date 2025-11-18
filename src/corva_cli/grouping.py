"""Group execution primitives."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
import json
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List

from corva_cli.tools.base import ToolResult


@dataclass(slots=True)
class GroupItem:
    name: str
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class GroupSpec:
    name: str
    ordered: bool
    tools: List[GroupItem]


class GroupConfigError(RuntimeError):
    """Raised when a group configuration file is invalid."""


ExecuteToolFn = Callable[[str, Dict[str, Any]], ToolResult]


def load_groups(path: Path) -> Dict[str, GroupSpec]:
    data = json.loads(path.read_text())
    groups_raw = data.get("groups")
    if not isinstance(groups_raw, list):
        raise GroupConfigError("Group file must contain a 'groups' list.")

    specs: Dict[str, GroupSpec] = {}
    for entry in groups_raw:
        name = entry.get("name")
        if not name:
            raise GroupConfigError("Each group requires a name.")
        ordered = bool(entry.get("ordered", True))
        tools_data = entry.get("tools", [])
        if not isinstance(tools_data, list) or not tools_data:
            raise GroupConfigError(f"Group '{name}' must declare tools.")
        items = []
        for tool_entry in tools_data:
            if isinstance(tool_entry, str):
                items.append(GroupItem(name=tool_entry))
                continue
            if not isinstance(tool_entry, dict) or "name" not in tool_entry:
                raise GroupConfigError(
                    f"Tools for group '{name}' must be a string or object with a name."
                )
            params = tool_entry.get("params", {})
            if params is None:
                params = {}
            if not isinstance(params, dict):
                raise GroupConfigError(
                    f"Params for tool '{tool_entry['name']}' in group '{name}' must be an object."
                )
            items.append(GroupItem(name=tool_entry["name"], params=params))
        specs[name] = GroupSpec(name=name, ordered=ordered, tools=items)
    return specs


class GroupRunner:
    """Executes groups either sequentially or concurrently."""

    def __init__(self, executor: ExecuteToolFn) -> None:
        self._executor = executor

    def run(self, spec: GroupSpec) -> List[ToolResult]:
        if spec.ordered:
            return [self._executor(item.name, item.params) for item in spec.tools]
        return self._run_unordered(spec)

    def _run_unordered(self, spec: GroupSpec) -> List[ToolResult]:
        results: List[ToolResult] = []
        with ThreadPoolExecutor(max_workers=len(spec.tools)) as pool:
            futures = {pool.submit(self._executor, item.name, item.params): item for item in spec.tools}
            for future in as_completed(futures):
                results.append(future.result())
        return results


__all__ = ["GroupConfigError", "GroupItem", "GroupRunner", "GroupSpec", "load_groups"]
