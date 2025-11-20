"""Application configuration helpers."""
from __future__ import annotations

import os
from dataclasses import dataclass
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

_DEFAULT_DATA_API_ROOT_URL = "https://data.corva.ai"
_DEFAULT_DATA_API_TIMEOUT = 30.0


@dataclass(slots=True)
class Settings:
    """Simple settings container."""

    data_api_root_url: str = _DEFAULT_DATA_API_ROOT_URL
    data_api_timeout_seconds: float = _DEFAULT_DATA_API_TIMEOUT

    @classmethod
    def from_env(cls) -> "Settings":
        data_api_root = os.getenv("CORVA_DATA_API_ROOT_URL", _DEFAULT_DATA_API_ROOT_URL)
        data_api_timeout_raw = os.getenv("CORVA_DATA_API_TIMEOUT_SECONDS")

        data_api_timeout = float(data_api_timeout_raw) if data_api_timeout_raw else _DEFAULT_DATA_API_TIMEOUT
        return cls(
            data_api_root_url=data_api_root,
            data_api_timeout_seconds=data_api_timeout,
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
