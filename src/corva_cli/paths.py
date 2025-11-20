"""Utilities for locating project resources."""
from __future__ import annotations

from pathlib import Path
import sys


def get_project_root() -> Path:
    """Return the root directory containing bundled assets."""

    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return Path(sys._MEIPASS)
    return Path(__file__).resolve().parents[2]


__all__ = ["get_project_root"]
