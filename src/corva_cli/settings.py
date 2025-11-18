"""Application configuration helpers."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import List

from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[1]
_ENV_PATH = _ROOT / ".env"
if _ENV_PATH.exists():
    load_dotenv(_ENV_PATH, override=False)
else:  # pragma: no cover - optional env file
    load_dotenv(override=False)

_DEFAULT_STATUSES = ["online", "maintenance", "offline"]
_DEFAULT_STEP_MINUTES = 60
_DEFAULT_DATA_API_ROOT_URL = "https://data.example.com"
_DEFAULT_DATA_API_TIMEOUT = 30.0
_DEFAULT_TIMELOG_PROVIDER = "corva"
_DEFAULT_TIMELOG_DATASET = "drilling.timelog.data"


def _parse_statuses(raw: str | None) -> List[str]:
    if not raw:
        return _DEFAULT_STATUSES.copy()
    values = [item.strip() for item in raw.split(",") if item.strip()]
    return values or _DEFAULT_STATUSES.copy()


@dataclass(slots=True)
class Settings:
    """Simple settings container."""

    timelog_step_minutes: int = _DEFAULT_STEP_MINUTES
    timelog_statuses: List[str] = field(default_factory=lambda: _DEFAULT_STATUSES.copy())
    data_api_root_url: str = _DEFAULT_DATA_API_ROOT_URL
    data_api_timeout_seconds: float = _DEFAULT_DATA_API_TIMEOUT
    timelog_provider: str = _DEFAULT_TIMELOG_PROVIDER
    timelog_dataset: str = _DEFAULT_TIMELOG_DATASET

    @classmethod
    def from_env(cls) -> "Settings":
        step_raw = os.getenv("CORVA_TIMELOG_STEP_MINUTES")
        statuses_raw = os.getenv("CORVA_TIMELOG_STATUSES")
        data_api_root = os.getenv("CORVA_DATA_API_ROOT_URL", _DEFAULT_DATA_API_ROOT_URL)
        data_api_timeout_raw = os.getenv("CORVA_DATA_API_TIMEOUT_SECONDS")
        timelog_provider = os.getenv("CORVA_TIMELOG_PROVIDER", _DEFAULT_TIMELOG_PROVIDER)
        timelog_dataset = os.getenv("CORVA_TIMELOG_DATASET", _DEFAULT_TIMELOG_DATASET)

        step_minutes = int(step_raw) if step_raw else _DEFAULT_STEP_MINUTES
        data_api_timeout = float(data_api_timeout_raw) if data_api_timeout_raw else _DEFAULT_DATA_API_TIMEOUT
        statuses = _parse_statuses(statuses_raw)
        return cls(
            timelog_step_minutes=step_minutes,
            timelog_statuses=statuses,
            data_api_root_url=data_api_root,
            data_api_timeout_seconds=data_api_timeout,
            timelog_provider=timelog_provider,
            timelog_dataset=timelog_dataset,
        )


@lru_cache()
def _cached_settings() -> Settings:
    return Settings.from_env()


def get_settings() -> Settings:
    """Return cached settings resolved from defaults/env."""

    return _cached_settings()


def reload_settings() -> Settings:
    """Clear cache and reload settings (useful in tests)."""

    _cached_settings.cache_clear()
    return _cached_settings()


__all__ = ["Settings", "get_settings", "reload_settings"]
