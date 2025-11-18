from corva_cli import settings as settings_module


def test_settings_defaults(monkeypatch):
    monkeypatch.delenv("CORVA_TIMELOG_STEP_MINUTES", raising=False)
    monkeypatch.delenv("CORVA_TIMELOG_STATUSES", raising=False)
    monkeypatch.delenv("CORVA_DATA_API_ROOT_URL", raising=False)
    monkeypatch.delenv("CORVA_DATA_API_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("CORVA_TIMELOG_PROVIDER", raising=False)
    monkeypatch.delenv("CORVA_TIMELOG_DATASET", raising=False)
    settings_module.reload_settings()
    cfg = settings_module.get_settings()
    assert cfg.timelog_step_minutes == 60
    assert cfg.timelog_statuses == ["online", "maintenance", "offline"]
    assert cfg.data_api_root_url == "https://data.example.com"
    assert cfg.data_api_timeout_seconds == 30
    assert cfg.timelog_provider == "timelog"
    assert cfg.timelog_dataset == "entries"


def test_settings_env_override(monkeypatch):
    monkeypatch.setenv("CORVA_TIMELOG_STEP_MINUTES", "15")
    monkeypatch.setenv("CORVA_TIMELOG_STATUSES", "idle , run ")
    monkeypatch.setenv("CORVA_DATA_API_ROOT_URL", "https://override")
    monkeypatch.setenv("CORVA_DATA_API_TIMEOUT_SECONDS", "45.5")
    monkeypatch.setenv("CORVA_TIMELOG_PROVIDER", "provider")
    monkeypatch.setenv("CORVA_TIMELOG_DATASET", "dataset")
    settings_module.reload_settings()
    cfg = settings_module.get_settings()
    assert cfg.timelog_step_minutes == 15
    assert cfg.timelog_statuses == ["idle", "run"]
    assert cfg.data_api_root_url == "https://override"
    assert cfg.data_api_timeout_seconds == 45.5
    assert cfg.timelog_provider == "provider"
    assert cfg.timelog_dataset == "dataset"
