import json

from typer.testing import CliRunner

from corva_cli.cli import app

runner = CliRunner()


def test_get_timelog_data_auto_window(monkeypatch):
    captured = {}

    async def fake_execute(provider, dataset_name, mql, headers, **kwargs):
        captured["mql"] = mql
        captured["provider"] = provider
        return {"documents": len(mql)}

    monkeypatch.setattr("corva_cli.utils.execute_data_api_pipeline", fake_execute)

    result = runner.invoke(
        app,
        [
            "get-timelog-data",
            "--api-key",
            "demo",
            "--asset-ids",
            "asset-1,asset-2",
            "--start-time",
            "auto_2h",
            "--end-time",
            "auto_0d",
        ],
    )
    assert result.exit_code == 0, result.stdout
    output = json.loads(result.stdout)
    assert output["result"]["documents"] == len(captured["mql"])
    assert captured["provider"]


def test_get_timelog_data_with_overrides(monkeypatch):
    async def fake_execute(provider, dataset_name, mql, headers, **kwargs):
        return {"mql": mql, "headers": headers, "provider": provider, "dataset": dataset_name}

    monkeypatch.setattr("corva_cli.utils.execute_data_api_pipeline", fake_execute)

    result = runner.invoke(
        app,
        [
            "get-timelog-data",
            "--jwt",
            "demo-jwt",
            "--asset-ids",
            "asset-1",
            "--start-time",
            "auto_1h",
            "--end-time",
            "auto_0d",
            "--step-minutes",
            "15",
            "--statuses",
            "idle,run",
        ],
    )
    assert result.exit_code == 0, result.stdout
    output = json.loads(result.stdout)
    assert output["query"]["step_minutes"] == 15
    assert output["query"]["statuses"] == ["idle", "run"]
