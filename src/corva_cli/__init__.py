"""Corva CLI package."""

from importlib.metadata import version, PackageNotFoundError

try:  # pragma: no cover
    __version__ = version("corva-cli")
except PackageNotFoundError:  # pragma: no cover
    __version__ = "0.0.0"

__all__ = ["__version__"]
