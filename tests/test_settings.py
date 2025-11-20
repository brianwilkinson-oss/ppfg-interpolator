from corva_cli import settings as settings_module


def test_settings_defaults(monkeypatch):
    monkeypatch.delenv("CORVA_DATA_API_ROOT_URL", raising=False)
    monkeypatch.delenv("CORVA_DATA_API_TIMEOUT_SECONDS", raising=False)
    settings_module.reload_settings()
    cfg = settings_module.get_settings()
    assert cfg.data_api_root_url == "https://data.corva.ai"
    assert cfg.data_api_timeout_seconds == 30.0


def test_settings_env_override(monkeypatch):
    monkeypatch.setenv("CORVA_DATA_API_ROOT_URL", "https://override")
    monkeypatch.setenv("CORVA_DATA_API_TIMEOUT_SECONDS", "45.5")
    settings_module.reload_settings()
    cfg = settings_module.get_settings()
    assert cfg.data_api_root_url == "https://override"
    assert cfg.data_api_timeout_seconds == 45.5
