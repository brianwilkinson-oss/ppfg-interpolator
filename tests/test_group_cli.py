import json
from pathlib import Path
from uuid import uuid4

from typer.testing import CliRunner

from corva_cli.cli import app, _register_generated_group_commands

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
            "--jwt",
            "demo",
            "--app-id",
            "7",
            "--group-name",
            "demo-group",
            "--groups-file",
            str(groups_path),
            "--asset-ids",
            "101,202",
            "--start-time",
            "auto_1h",
            "--end-time",
            "auto_0d",
            "--company-id",
            "3",
        ],
    )

    assert result.exit_code == 0, result.stdout
    assert called["params"] == {"app_ids[]": 7}
    data = json.loads(groups_path.read_text())
    assert data["groups"][0]["name"] == "demo-group"
    assert data["groups"][0]["tools"] == [
        {
            "name": "dataset-activities",
            "params": {
                "asset_ids": "101,202",
                "company_id": 3,
                "start_time": "auto_1h",
                "end_time": "auto_0d",
            },
        }
    ]


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


def test_group_run_accepts_overrides(tmp_path, monkeypatch):
    groups_path = tmp_path / "groups.json"
    groups_path.write_text(
        json.dumps(
            {
                "groups": [
                    {
                        "name": "demo",
                        "ordered": True,
                        "tools": [
                            {"name": "dataset-activities", "params": {}},
                        ],
                    }
                ]
            }
        )
    )

    async def fake_execute(provider, dataset_name, mql, headers, **kwargs):
        assert dataset_name == "activities"
        match_stage = mql[0]["$match"]
        assert match_stage["asset_id"] == 101
        assert "timestamp" in match_stage
        return ({"rows": []}, {"status_code": 200})

    monkeypatch.setattr("corva_cli.utils.execute_data_api_pipeline", fake_execute)

    result = runner.invoke(
        app,
        [
            "group",
            "run",
            str(groups_path),
            "--name",
            "demo",
            "--jwt",
            "demo",
            "--asset-ids",
            "101",
            "--start-time",
            "auto_1h",
            "--end-time",
            "auto_0d",
        ],
    )

    assert result.exit_code == 0, result.stdout
    payload = json.loads(result.stdout)
    assert payload["group"] == "demo"
    assert payload["results"]["dataset-activities"]["rows"] == []


def test_generated_group_command_available(tmp_path, monkeypatch):
    unique_name = f"demo-{uuid4().hex[:6]}"
    groups_path = tmp_path / "groups.json"
    groups_path.write_text(
        json.dumps(
            {
                "groups": [
                    {
                        "name": unique_name,
                        "ordered": True,
                        "tools": [
                            {"name": "dataset-activities", "params": {}},
                        ],
                    }
                ]
            }
        )
    )

    monkeypatch.setattr("corva_cli.cli.DEFAULT_GROUPS_FILE", groups_path)
    monkeypatch.setattr("corva_cli.cli.REGISTERED_GROUP_COMMANDS", set())
    _register_generated_group_commands()

    async def fake_execute(provider, dataset_name, mql, headers, **kwargs):
        assert dataset_name == "activities"
        return ({"rows": []}, {"status_code": 200})

    monkeypatch.setattr("corva_cli.utils.execute_data_api_pipeline", fake_execute)

    result = runner.invoke(
        app,
        [
            unique_name,
            "--jwt",
            "demo",
            "--asset-ids",
            "101",
            "--start-time",
            "auto_1h",
            "--end-time",
            "auto_0d",
        ],
    )
    assert result.exit_code == 0, result.stdout


def test_generated_dvd_group_command(tmp_path, monkeypatch):
    groups_path = tmp_path / "groups.json"
    groups_path.write_text(
        json.dumps(
            {
                "groups": [
                    {
                        "name": "dvd",
                        "ordered": True,
                        "tools": [
                            {"name": "dataset-activities", "params": {}},
                            {
                                "name": "dataset-data-metrics",
                                "params": {
                                    "metric_type": "bha",
                                    "metric_keys": "on_bottom_percentage,rop",
                                },
                            },
                        ],
                    }
                ]
            }
        )
    )

    monkeypatch.setattr("corva_cli.cli.DEFAULT_GROUPS_FILE", groups_path)
    monkeypatch.setattr("corva_cli.cli.REGISTERED_GROUP_COMMANDS", set())
    _register_generated_group_commands()

    executions = []

    async def fake_execute(provider, dataset_name, mql, headers, **kwargs):
        executions.append((dataset_name, mql))
        return ({"rows": [dataset_name]}, {"status_code": 200})

    monkeypatch.setattr("corva_cli.utils.execute_data_api_pipeline", fake_execute)

    result = runner.invoke(
        app,
        [
            "dvd",
            "--jwt",
            "demo",
            "--asset-ids",
            "101",
            "--company-id",
            "3",
            "--start-time",
            "auto_1h",
            "--end-time",
            "auto_0d",
        ],
    )

    assert result.exit_code == 0, result.stdout
    payload = json.loads(result.stdout)
    assert payload["group"] == "dvd"
    assert "dataset-data-metrics" in payload["results"]
    metric_match = executions[-1][1][0]["$match"]
    assert metric_match["data.type"] == "bha"
    assert payload["results"]["dataset-activities"]["rows"] == ["activities"]
