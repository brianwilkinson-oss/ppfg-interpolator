"""Entry point for PyInstaller/Python -m invocations."""
from __future__ import annotations

from corva_cli.cli import app


def main() -> None:
    app()


if __name__ == "__main__":
    main()
