import json

from typer.testing import CliRunner

from corva_cli.cli import app
from corva_cli.datasets import DatasetMeta
from corva_cli.tools.timelog import DVD_DATASET_COMMANDS

runner = CliRunner()


def _fake_dvd_metas():
    entries = []
    for command in DVD_DATASET_COMMANDS:
        slug = command[len("dataset-") :]
        dataset_name = slug.replace("-", ".")
        entries.append(
            (
                command,
                DatasetMeta(
                    name=f"corva#{dataset_name}",
                    friendly_name=slug,
                    provider="corva",
                    company_id=3,
                    data_type="time",
                    description="",
                    dataset=dataset_name,
                    indexes=(("asset_id", "timestamp"),),
                ),
            )
        )
    return entries


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
    assert "timestamp" in match_stage
    range_filter = match_stage["timestamp"]
    assert isinstance(range_filter["$gte"], int)
    assert isinstance(range_filter["$lte"], int)
    assert pipeline[1] == {"$limit": 1000}
    assert pipeline[2] == {"$sort": {"timestamp": -1}}
    add_fields = pipeline[3]["$addFields"]
    assert "timestamp_iso" in add_fields
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
            "--verbose",
        ],
    )
    assert result.exit_code == 0, result.stdout
    output = json.loads(result.stdout)
    assert output["query"]["provider"] == "corva"
    assert output["query"]["dataset"] == "drilling.timelog.data"
    assert output["query"]["limit"] == 1000
    assert output["query"]["company_id"] == 99
    pipeline = output["result"]["mql"]
    assert pipeline[0]["$match"]["asset_id"] == 303
    assert "timestamp" in pipeline[0]["$match"]
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
    monkeypatch.setattr("corva_cli.tools.timelog._dvd_dataset_metas", _fake_dvd_metas)
    calls = []

    async def fake_execute(provider, dataset_name, mql, headers, **kwargs):
        calls.append(dataset_name)
        return ([dataset_name], {"status_code": 200})

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
            "--start-time",
            "auto_1h",
            "--end-time",
            "auto_0d",
        ],
    )
    assert result.exit_code == 0, result.stdout
    payload = json.loads(result.stdout)
    assert set(payload["datasets"].keys()) == set(DVD_DATASET_COMMANDS)
    first_key = DVD_DATASET_COMMANDS[0]
    assert first_key in payload["datasets"]
    assert len(calls) == len(DVD_DATASET_COMMANDS)


def test_dvd_requires_time_window(monkeypatch):
    monkeypatch.setattr("corva_cli.tools.timelog._dvd_dataset_metas", _fake_dvd_metas)
    calls = []

    async def fake_execute(provider, dataset_name, mql, headers, **kwargs):
        calls.append(dataset_name)
        return ([dataset_name], {"status_code": 200})

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
    assert result.exit_code != 0
    assert "--start-time/--end-time" in result.stdout
    assert calls == []


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


def test_dataset_wits_summary_uses_timestamp(monkeypatch):
    captured = {}

    async def fake_execute(provider, dataset_name, mql, headers, **kwargs):
        captured["mql"] = mql
        return ({"rows": []}, {"status_code": 200})

    monkeypatch.setattr("corva_cli.utils.execute_data_api_pipeline", fake_execute)

    result = runner.invoke(
        app,
        [
            "dataset-wits-summary-1m",
            "--api-key",
            "demo",
            "--asset-ids",
            "321",
            "--company-id",
            "3",
            "--start-time",
            "auto_1h",
            "--end-time",
            "auto_0d",
        ],
    )
    assert result.exit_code == 0, result.stdout
    match_stage = captured["mql"][0]["$match"]
    assert "timestamp" in match_stage


def test_dataset_time_requires_window(monkeypatch):
    calls = []

    async def fake_execute(provider, dataset_name, mql, headers, **kwargs):
        calls.append(dataset_name)
        return ({}, {})

    monkeypatch.setattr("corva_cli.utils.execute_data_api_pipeline", fake_execute)

    result = runner.invoke(
        app,
        [
            "dataset-activities",
            "--api-key",
            "demo",
            "--asset-ids",
            "123",
        ],
    )
    assert result.exit_code != 0
    assert "--start-time/--end-time" in result.stdout
    assert calls == []


def test_dataset_depth_requires_depth_filters(monkeypatch):
    calls = []

    async def fake_execute(provider, dataset_name, mql, headers, **kwargs):
        calls.append(dataset_name)
        return ({}, {})

    monkeypatch.setattr("corva_cli.utils.execute_data_api_pipeline", fake_execute)

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
    assert "--depth-start/--depth-end" in result.stdout
    assert calls == []


def test_dataset_depth_uses_depth_filters(monkeypatch):
    captured = {}

    async def fake_execute(provider, dataset_name, mql, headers, **kwargs):
        captured["dataset"] = dataset_name
        captured["mql"] = mql
        return ({}, {})

    monkeypatch.setattr("corva_cli.utils.execute_data_api_pipeline", fake_execute)

    result = runner.invoke(
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
            "1200",
        ],
    )
    assert result.exit_code == 0, result.stdout
    assert captured["dataset"] == "directional.tool_face"
    match_stage = captured["mql"][0]["$match"]
    assert "measured_depth" in match_stage


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
