from corva_cli.output import format_result
from corva_cli.tools.base import OutputFormat, ToolResult


def test_format_json_dict():
    result = ToolResult(payload={"foo": "bar"})
    rendered = format_result(result, OutputFormat.JSON)
    assert "\n" in rendered
    assert "foo" in rendered


def test_format_markdown_list():
    result = ToolResult(payload=[{"foo": "bar"}, "baz"])
    rendered = format_result(result, OutputFormat.MARKDOWN)
    assert "| Field |" in rendered
    assert "- `baz`" in rendered


def test_format_markdown_list_of_dicts_flattens():
    payload = [
        {"foo": "bar", "nested": {"id": 1}},
        {"foo": "baz", "nested": {"id": 2}, "extra": "x"},
    ]
    result = ToolResult(payload=payload)
    rendered = format_result(result, OutputFormat.MARKDOWN)
    assert "| foo | nested.id | extra |" in rendered
    assert "`bar`" in rendered
    assert "| `baz` | 2 | `x` |" in rendered
