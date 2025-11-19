import json

from typer.testing import CliRunner

from corva_cli.cli import app

runner = CliRunner()


def test_timelog_auto_window_default(monkeypatch):
    captured = {}

    async def fake_execute(provider, dataset_name, mql, headers, **kwargs):
        captured["mql"] = mql
        captured["provider"] = provider
        captured["dataset"] = dataset_name
        captured["headers"] = headers
        return {"documents": len(mql)}, {"status_code": 200, "url": "https://example.com", "request_body": {"stages": mql}}

    monkeypatch.setattr("corva_cli.utils.execute_data_api_pipeline", fake_execute)

    result = runner.invoke(
        app,
        [
            "timelog",
            "--api-key",
            "demo",
            "--asset-ids",
            "101,202",
            "--company-id",
            "12",
            "--start-time",
            "auto_2h",
            "--end-time",
            "auto_0d",
        ],
    )
    assert result.exit_code == 0, result.stdout
    output = json.loads(result.stdout)
    assert output["documents"] == len(captured["mql"])
    assert captured["provider"] == "corva"
    assert captured["dataset"] == "drilling.timelog.data"
    pipeline = captured["mql"]
    assert len(pipeline) == 4
    match_stage = pipeline[0]["$match"]
    assert match_stage["asset_id"]["$in"] == [101, 202]
    assert match_stage["company_id"] == 12
    range_filter = match_stage["data.start_time"]
    assert isinstance(range_filter["$gte"], int)
    assert isinstance(range_filter["$lte"], int)
    assert pipeline[1] == {"$limit": 1000}
    assert pipeline[2] == {"$sort": {"data.start_time": -1}}
    add_fields = pipeline[3]["$addFields"]
    assert "data.start_time_iso" in add_fields
    assert captured["headers"]["Authorization"] == "API demo"
    # Default output omits metadata
    assert "api_debug" not in result.stdout


def test_timelog_verbose_includes_metadata(monkeypatch):
    async def fake_execute(provider, dataset_name, mql, headers, **kwargs):
        return (
            {"mql": mql, "headers": headers, "provider": provider, "dataset": dataset_name},
            {"status_code": 200, "request_body": {"stages": mql}, "url": "https://example.com"},
        )

    monkeypatch.setattr("corva_cli.utils.execute_data_api_pipeline", fake_execute)

    result = runner.invoke(
        app,
        [
            "timelog",
            "--jwt",
            "demo-jwt",
            "--asset-ids",
            "303",
            "--company-id",
            "99",
            "--start-time",
            "auto_1h",
            "--end-time",
            "auto_0d",
            "--step-minutes",
            "15",
            "--statuses",
            "idle,run",
            "--verbose",
        ],
    )
    assert result.exit_code == 0, result.stdout
    output = json.loads(result.stdout)
    assert output["query"]["step_minutes"] == 15
    assert output["query"]["statuses"] == ["idle", "run"]
    assert output["query"]["provider"] == "corva"
    assert output["query"]["dataset"] == "drilling.timelog.data"
    assert output["query"]["limit"] == 1000
    assert output["query"]["company_id"] == 99
    pipeline = output["result"]["mql"]
    assert pipeline[0]["$match"]["asset_id"] == 303
    assert output["result"]["headers"]["Authorization"] == "Bearer demo-jwt"


def test_timelog_without_window_uses_limit(monkeypatch):
    captured = {}

    async def fake_execute(provider, dataset_name, mql, headers, **kwargs):
        captured["mql"] = mql
        return ({"docs": []}, {"status_code": 200, "request_body": {"stages": mql}})

    monkeypatch.setattr("corva_cli.utils.execute_data_api_pipeline", fake_execute)

    result = runner.invoke(
        app,
        [
            "timelog",
            "--api-key",
            "demo",
            "--asset-ids",
            "404",
            "--limit",
            "200",
            "--skip",
            "25",
        ],
    )
    assert result.exit_code == 0, result.stdout
    payload = json.loads(result.stdout)
    match_stage = captured["mql"][0]["$match"]
    assert "data.start_time" not in match_stage
    assert captured["mql"][1] == {"$skip": 25}
    assert captured["mql"][2] == {"$limit": 200}
    assert payload == {"docs": []}


def test_assets_command(monkeypatch):
    captured = {}

    async def fake_execute(provider, dataset_name, mql, headers, **kwargs):
        captured["provider"] = provider
        captured["dataset"] = dataset_name
        captured["mql"] = mql
        return ({"assets": []}, {"status_code": 200, "request_body": {"stages": mql}})

    monkeypatch.setattr("corva_cli.utils.execute_data_api_pipeline", fake_execute)

    result = runner.invoke(
        app,
        [
            "assets",
            "--api-key",
            "demo",
            "--asset-ids",
            "909",
            "--company-id",
            "77",
            "--limit",
            "5",
        ],
    )
    assert result.exit_code == 0, result.stdout
    assert captured["dataset"] == "assets"
    assert captured["provider"] == "corva"
    match_stage = captured["mql"][0]["$match"]
    assert match_stage["company_id"] == 77
    assert captured["mql"][1] == {"$limit": 5}
    assert json.loads(result.stdout) == {"assets": []}


def test_assets_command_without_asset_ids(monkeypatch):
    captured = {}

    async def fake_execute(provider, dataset_name, mql, headers, **kwargs):
        captured["mql"] = mql
        return ({"assets": []}, {"status_code": 200, "request_body": {"stages": mql}})

    monkeypatch.setattr("corva_cli.utils.execute_data_api_pipeline", fake_execute)

    result = runner.invoke(
        app,
        [
            "assets",
            "--api-key",
            "demo",
            "--company-id",
            "55",
            "--limit",
            "10",
        ],
    )
    assert result.exit_code == 0, result.stdout
    match_stage = captured["mql"][0]["$match"]
    assert "asset_id" not in match_stage
    assert match_stage["company_id"] == 55


def test_assets_requires_company_when_no_assets(monkeypatch):
    async def fake_execute(*args, **kwargs):
        return {}, {}

    monkeypatch.setattr("corva_cli.utils.execute_data_api_pipeline", fake_execute)

    result = runner.invoke(
        app,
        [
            "assets",
            "--api-key",
            "demo",
            "--limit",
            "10",
        ],
    )
    assert result.exit_code != 0
    assert "--company-id" in result.stdout.lower()


def test_dvd_returns_both_payloads(monkeypatch):
    async def fake_execute(provider, dataset_name, mql, headers, **kwargs):
        if dataset_name == "assets":
            return (["asset-entry"], {"status_code": 200})
        return (["timelog-entry"], {"status_code": 200})

    monkeypatch.setattr("corva_cli.utils.execute_data_api_pipeline", fake_execute)

    result = runner.invoke(
        app,
        [
            "dvd",
            "--api-key",
            "demo",
            "--asset-ids",
            "909",
            "--company-id",
            "77",
        ],
    )
    assert result.exit_code == 0, result.stdout
    payload = json.loads(result.stdout)
    assert payload["assets"] == ["asset-entry"]
    assert payload["timelog"] == ["timelog-entry"]


def test_dataset_time_command(monkeypatch):
    async def fake_execute(provider, dataset_name, mql, headers, **kwargs):
        assert dataset_name == "activities"
        return ({"rows": []}, {"status_code": 200, "request_body": {"stages": mql}})

    monkeypatch.setattr("corva_cli.utils.execute_data_api_pipeline", fake_execute)

    result = runner.invoke(
        app,
        [
            "dataset-activities",
            "--api-key",
            "demo",
            "--asset-ids",
            "123",
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
    assert "rows" in payload


def test_dataset_depth_requires_depth(monkeypatch):
    async def fake_execute(*args, **kwargs):
        return ({}, {})

    monkeypatch.setattr("corva_cli.utils.execute_data_api_pipeline", fake_execute)

    # Missing depth parameters should trigger error
    result = runner.invoke(
        app,
        [
            "dataset-directional-tool-face",
            "--api-key",
            "demo",
            "--asset-ids",
            "123",
        ],
    )
    assert result.exit_code != 0
    assert "depth-start" in result.stdout.lower()

    # Providing depth parameters succeeds
    result_ok = runner.invoke(
        app,
        [
            "dataset-directional-tool-face",
            "--api-key",
            "demo",
            "--asset-ids",
            "123",
            "--depth-start",
            "1000",
            "--depth-end",
            "1500",
        ],
    )
    assert result_ok.exit_code == 0, result_ok.stdout


def test_dataset_time_optional_limit_only(monkeypatch):
    async def fake_execute(provider, dataset_name, mql, headers, **kwargs):
        assert dataset_name == "drilling.timelog.data"
        return ({"rows": []}, {"status_code": 200})

    monkeypatch.setattr("corva_cli.utils.execute_data_api_pipeline", fake_execute)

    result = runner.invoke(
        app,
        [
            "dataset-drilling-timelog-data",
            "--api-key",
            "demo",
            "--asset-ids",
            "101",
            "--company-id",
            "3",
            "--limit",
            "5",
        ],
    )
    assert result.exit_code == 0, result.stdout
