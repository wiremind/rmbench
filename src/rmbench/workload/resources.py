from __future__ import annotations

from pathlib import Path

import yaml

RESOURCES_FILE = Path("resources.yaml")
COMPOSE_ENV = Path(".compose.env")


def resources(path: Path = RESOURCES_FILE) -> dict[str, dict[str, str]]:
    return yaml.safe_load(path.read_text())


def duckdb_budget(path: Path = RESOURCES_FILE) -> tuple[int, str]:
    budget = resources(path)["duckdb"]
    return budget["threads"], budget["memory"]


def write_compose_env(path: Path = RESOURCES_FILE, target: Path = COMPOSE_ENV) -> Path:
    """Render the budget as `SERVICE_KEY=value` for compose to interpolate."""
    target.write_text(
        "".join(
            f"{service.upper().replace('-', '_')}_{key.upper()}={value}\n"
            for service, values in resources(path).items()
            for key, value in values.items()
        )
    )
    return target
