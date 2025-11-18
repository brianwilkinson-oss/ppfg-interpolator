from pathlib import Path

import pytest

from corva_cli.grouping import GroupConfigError, GroupItem, GroupRunner, GroupSpec, load_groups
from corva_cli.tools.base import ToolResult


def test_load_groups(tmp_path):
    groups_json = {
        "groups": [
            {
                "name": "demo",
                "ordered": True,
                "tools": [
                    {"name": "tool-a", "params": {"foo": 1}},
                    "tool-b",
                ],
            }
        ]
    }
    path = tmp_path / "groups.json"
    path.write_text(__import__("json").dumps(groups_json))

    specs = load_groups(path)
    assert "demo" in specs
    group = specs["demo"]
    assert group.ordered is True
    assert len(group.tools) == 2
    assert group.tools[0].params == {"foo": 1}


def test_load_groups_requires_list(tmp_path):
    path = tmp_path / "bad.json"
    path.write_text("{}")
    with pytest.raises(GroupConfigError):
        load_groups(path)


def test_group_runner_ordered():
    seen = []

    def executor(name, params):
        seen.append(name)
        return ToolResult(payload={"name": name})

    spec = GroupSpec(name="demo", ordered=True, tools=[GroupItem(name="a"), GroupItem(name="b")])
    runner = GroupRunner(executor)
    runner.run(spec)
    assert seen == ["a", "b"]


def test_group_runner_unordered():
    seen = []

    def executor(name, params):
        seen.append(name)
        return ToolResult(payload={"name": name})

    spec = GroupSpec(name="demo", ordered=False, tools=[GroupItem(name="a"), GroupItem(name="b")])
    runner = GroupRunner(executor)
    results = runner.run(spec)
    assert len(results) == 2
    assert sorted(item.payload["name"] for item in results) == ["a", "b"]
