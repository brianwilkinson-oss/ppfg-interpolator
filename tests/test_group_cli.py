import json
from pathlib import Path

from typer.testing import CliRunner

from corva_cli.cli import app

runner = CliRunner()


class _DummyResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def test_group_create_generates_config(tmp_path, monkeypatch):
    called = {}

    def fake_get(url, headers, params, timeout):
        called["params"] = params
        return _DummyResponse(
            {
                "data": [
                    {"attributes": {"name": "corva#activities"}},
                    {"attributes": {"name": "corva#missing"}},
                ]
            }
        )

    monkeypatch.setattr("corva_cli.cli.httpx.get", fake_get)

    groups_path = tmp_path / "groups.json"
    result = runner.invoke(
        app,
        [
            "group",
            "create",
            "--token",
            "demo",
            "--app-id",
            "7",
            "--group-name",
            "demo-group",
            "--groups-file",
            str(groups_path),
        ],
    )

    assert result.exit_code == 0, result.stdout
    assert called["params"] == {"app_ids[]": 7}
    data = json.loads(groups_path.read_text())
    assert data["groups"][0]["name"] == "demo-group"
    assert data["groups"][0]["tools"] == [{"name": "dataset-activities"}]


def test_group_create_errors_when_no_datasets_match(tmp_path, monkeypatch):
    def fake_get(url, headers, params, timeout):
        return _DummyResponse({"data": [{"attributes": {"name": "corva#missing"}}]})

    monkeypatch.setattr("corva_cli.cli.httpx.get", fake_get)

    groups_path = tmp_path / "groups.json"
    result = runner.invoke(
        app,
        [
            "group",
            "create",
            "--token",
            "demo",
            "--app-id",
            "8",
            "--group-name",
            "missing-group",
            "--groups-file",
            str(groups_path),
        ],
    )

    assert result.exit_code != 0
    assert not groups_path.exists()
